"""Strict JSON contracts, deliberately independent of model-training imports."""
import json
import math
import re
from datetime import date, datetime, timezone
from analysis.forecast.contracts import FEATURE_NAMES, FEATURE_VERSION, TARGET
from analysis.forecast.continuity import validate_evidence

PARAMS = dict(objective='reg:squarederror', n_estimators=80, max_depth=3,
              learning_rate=.05, subsample=1., colsample_bytree=1.,
              random_state=42, n_jobs=1, tree_method='hist', device='cpu')
METHODS = dict(zero_return={}, ridge=dict(alpha=1.0, solver='svd',
    imputation='train_median_all_missing_zero', scaling='standard_scaler_train_only'), xgboost=PARAMS)
COMMON = 'schema_version report_id ts_code target horizon units protocol_id evaluation_kind history_previously_observed prospective_validation created_at source feature_version folds overall_metrics'.split()
REPORT_FIELDS = COMMON + 'source_continuity feature_names methods dependency_versions series'.split()
SUMMARY_FIELDS = COMMON + 'evaluation report_sha256 report_bytes'.split()


def _keys(value, names):
    if type(value) is not dict or set(value) != set(names):
        raise ValueError('Invalid contract fields')


def _int(value, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError('Invalid integer')


def _number(value):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError('Invalid finite number')


def _day(value):
    if type(value) is not str or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise ValueError('Invalid date')
    date.fromisoformat(value)
    return value


def _hash(value):
    if type(value) is not str or not re.fullmatch('[0-9a-f]{64}', value):
        raise ValueError('Invalid digest')


def validate_report_id(value):
    if type(value) is not str or not re.fullmatch('eval_[0-9a-f]{32}', value):
        raise ValueError('Invalid report ID')
    return value


def _metrics(value):
    _keys(value, METHODS)
    for metric in value.values():
        _keys(metric, ('rmse', 'mae', 'r2'))
        for name, number in metric.items():
            if name == 'r2' and number is None:
                continue
            _number(number)
            if (name != 'r2' and number < 0) or (name == 'r2' and number > 1):
                raise ValueError('Invalid metric range')


def _range(value, fields, low, high):
    _keys(value, fields)
    for key in fields:
        if key.endswith('start') or key.endswith('end'):
            _day(value[key])
    _int(value['count'], low, high)
    if value['signal_start'] > value['signal_end']:
        raise ValueError('Invalid date range')


def _common(report):
    fixed = dict(schema_version=1, ts_code='000001.SZ', target=TARGET, horizon=1,
        units='fractional_return', protocol_id='v08_expanding_3x100_1',
        evaluation_kind='retrospective_walk_forward', history_previously_observed=True,
        prospective_validation=False, feature_version=FEATURE_VERSION)
    for key, value in fixed.items():
        if type(report[key]) is not type(value) or report[key] != value:
            raise ValueError('Invalid fixed contract value: '+key)
    validate_report_id(report['report_id'])
    if type(report['created_at']) is not str:
        raise ValueError('Invalid timestamp')
    stamp = datetime.fromisoformat(report['created_at'])
    if stamp.tzinfo is None or stamp.utcoffset() != timezone.utc.utcoffset(stamp):
        raise ValueError('Timestamp requires UTC offset')
    source = report['source']
    _keys(source, ('signal_start', 'signal_end', 'row_count', 'data_hash'))
    _day(source['signal_start']); _day(source['signal_end'])
    _int(source['row_count'], 300, 2500); _hash(source['data_hash'])
    if source['signal_start'] >= source['signal_end']:
        raise ValueError('Invalid source range')
    folds = report['folds']
    if type(folds) is not list or len(folds) != 3:
        raise ValueError('Expected three folds')
    for i, fold in enumerate(folds, 1):
        _keys(fold, ('fold_id', 'train', 'evaluation', 'metrics'))
        _int(fold['fold_id'], i, i)
        train, evaluation = fold['train'], fold['evaluation']
        _range(train, ('signal_start','signal_end','label_end','count','purged_count'), 300, 2500)
        _int(train['purged_count'], 1, 1)
        _range(evaluation, ('signal_start','signal_end','target_start','target_end','count'), 100, 100)
        if not (source['signal_start'] <= train['signal_start'] <= train['signal_end']
                < train['label_end'] < evaluation['signal_start'] <= evaluation['signal_end']
                < evaluation['target_end'] <= source['signal_end']):
            raise ValueError('Invalid causal fold boundaries')
        if not evaluation['signal_start'] < evaluation['target_start'] <= evaluation['target_end']:
            raise ValueError('Invalid evaluation targets')
        if i > 1:
            previous = folds[i-2]
            if (previous['evaluation']['target_end'] != evaluation['signal_start']
                    or train['count'] != previous['train']['count'] + 100
                    or train['signal_start'] != previous['train']['signal_start']):
                raise ValueError('Invalid expanding windows')
        _metrics(fold['metrics'])
    _metrics(report['overall_metrics'])


def _evaluation(report):
    first, last = report['folds'][0]['evaluation'], report['folds'][-1]['evaluation']
    return dict(signal_start=first['signal_start'], signal_end=last['signal_end'],
        target_start=first['target_start'], target_end=last['target_end'], count=300)


def _computed(actual, predicted):
    residual = [a-p for a,p in zip(actual, predicted)]
    constant = len(actual) < 2 or all(a == actual[0] for a in actual)
    mean = sum(actual)/len(actual) if not constant else actual[0]
    denominator = 0. if constant else sum((a-mean)**2 for a in actual)
    return dict(rmse=math.sqrt(sum(r*r for r in residual)/len(actual)),
        mae=sum(abs(r) for r in residual)/len(actual),
        r2=1-sum(r*r for r in residual)/denominator if denominator else None)


def _check_metrics(rows, supplied):
    for method in METHODS:
        expected = _computed([r['actual_return'] for r in rows], [r['predicted_returns'][method] for r in rows])
        for key, number in expected.items():
            actual = supplied[method][key]
            if (number is None and actual is not None) or (number is not None and
                    (actual is None or not math.isclose(number, actual, rel_tol=1e-10, abs_tol=1e-12))):
                raise ValueError('Metric does not match series')


def validate_report(report):
    try:
        _keys(report, REPORT_FIELDS); _common(report)
        if report['feature_names'] != list(FEATURE_NAMES):
            raise ValueError('Invalid fixed features')
        # Canonical JSON distinguishes true from 1 and fixed parameter types.
        if json.dumps(report['methods'], sort_keys=True, allow_nan=False) != json.dumps(METHODS, sort_keys=True):
            raise ValueError('Invalid fixed methods')
        versions = report['dependency_versions']
        _keys(versions, ('numpy','pandas','sklearn','xgboost','threadpoolctl'))
        if any(type(v) is not str or not v or len(v) > 100 for v in versions.values()):
            raise ValueError('Invalid dependency versions')
        _keys(report['source_continuity'], ('source','exchange','calendar_start','calendar_end',
            'source_start','source_end','session_count','open_dates','source_dates_hash'))
        days = validate_evidence(report['source_continuity'], report['ts_code'])
        source = report['source']
        if (source['row_count'] != len(days) or source['signal_start'] != days[0]
                or source['signal_end'] != days[-1]):
            raise ValueError('Source evidence mismatch')
        rows = report['series']
        if type(rows) is not list or len(rows) != 300:
            raise ValueError('Expected 300 evaluation observations')
        for i, row in enumerate(rows):
            _keys(row, ('signal_date','target_date','fold_id','actual_return','predicted_returns'))
            _int(row['fold_id'], i//100+1, i//100+1)
            if row['signal_date'] != days[len(days)-301+i] or row['target_date'] != days[len(days)-300+i]:
                raise ValueError('Invalid signal/adjacent target session')
            _number(row['actual_return']); _keys(row['predicted_returns'], METHODS)
            for value in row['predicted_returns'].values(): _number(value)
            if row['predicted_returns']['zero_return'] != 0:
                raise ValueError('Invalid zero-return baseline')
        for i, fold in enumerate(report['folds']):
            part = rows[i*100:(i+1)*100]
            expected = dict(signal_start=part[0]['signal_date'], signal_end=part[-1]['signal_date'],
                target_start=part[0]['target_date'], target_end=part[-1]['target_date'], count=100)
            if fold['evaluation'] != expected: raise ValueError('Fold series mismatch')
            train = fold['train']
            begin, end = days.index(train['signal_start']), days.index(train['signal_end'])
            if end-begin+1 != train['count'] or days[end+1] != train['label_end'] or end != len(days)-303+i*100:
                raise ValueError('Training range mismatch')
            _check_metrics(part, fold['metrics'])
        _check_metrics(rows, report['overall_metrics'])
    except (KeyError, TypeError, IndexError, OverflowError) as exc:
        raise ValueError('Invalid report') from exc
    return report


def validate_summary(summary):
    try:
        _keys(summary, SUMMARY_FIELDS); _common(summary)
        _hash(summary['report_sha256']); _int(summary['report_bytes'], 1, 2*1024*1024)
        _range(summary['evaluation'], ('signal_start','signal_end','target_start','target_end','count'), 300, 300)
        if summary['evaluation'] != _evaluation(summary):
            raise ValueError('Summary evaluation mismatch')
        if len(json.dumps(summary, ensure_ascii=False, allow_nan=False).encode('utf-8')) > 65535:
            raise ValueError('Summary exceeds byte limit')
    except (KeyError, TypeError, IndexError, OverflowError) as exc:
        raise ValueError('Invalid summary') from exc
    return summary


def make_summary(report, digest, size):
    validate_report(report)
    import copy
    summary = {key: copy.deepcopy(report[key]) for key in COMMON}
    summary.update(evaluation=_evaluation(report), report_sha256=digest, report_bytes=size)
    return validate_summary(summary)


def _parse(raw, limit):
    if type(raw) not in (str, bytes) or len(raw.encode('utf-8') if type(raw) is str else raw) > limit:
        raise ValueError('Invalid JSON size/type')
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result: raise ValueError('Duplicate JSON key')
            result[key] = value
        return result
    def reject(value):
        raise ValueError('Nonfinite JSON value')
    try:
        return json.loads(raw, object_pairs_hook=unique, parse_constant=reject)
    except (UnicodeError, RecursionError) as exc:
        raise ValueError('Invalid JSON') from exc


def parse_report(raw):
    return validate_report(_parse(raw, 2*1024*1024))


def parse_summary(raw):
    return validate_summary(_parse(raw, 65535))
