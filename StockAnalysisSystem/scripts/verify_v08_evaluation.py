"""Read-only Decimal/calendar reconciliation; never imports training code.

PASS proves reported observations and arithmetic, not estimator fit provenance,
point-in-time vendor availability, predictive usefulness or a pristine holdout.
"""
import argparse
from decimal import Decimal, InvalidOperation
import json
import math
from pathlib import Path
import re
import sys
from urllib.parse import urlsplit

from verify_v07_prediction import (VerificationError, require, day, number,
    equal_number, metrics, evidence_days, calendar_sessions, read_json, fetch,
    load_source, MAX_JSON)

METHODS = ('zero_return', 'ridge', 'xgboost')


def verify(source, report, calendar):
    """Independently derive the raw final 300 adjacent-session return pairs."""
    try:
        fixed = dict(schema_version=1, ts_code='000001.SZ', horizon=1,
            target='next_trading_day_close_return', units='fractional_return',
            protocol_id='v08_expanding_3x100_1', feature_version='v07_fixed_1',
            evaluation_kind='retrospective_walk_forward', history_previously_observed=True,
            prospective_validation=False)
        for key, expected in fixed.items():
            require(type(report[key]) is type(expected) and report[key] == expected, 'Unsupported evaluation contract')
        require(type(report['report_id']) is str and bool(re.fullmatch(r'eval_[0-9a-f]{32}', report['report_id'])), 'Invalid report ID')
        require(type(source) is list and 660 <= len(source) <= 2500, 'Invalid source count')
        days = [day(r['trade_date']) for r in source]
        require(days == sorted(set(days)), 'Invalid source dates')
        require(days == evidence_days(report['source_continuity'], calendar, report['ts_code']), 'Source calendar mismatch')
        origin = report['source']
        require(type(origin['row_count']) is int and origin['row_count'] == len(days) and
            origin['signal_start'] == days[0] and origin['signal_end'] == days[-1], 'Source bounds mismatch')
        closes = [number(r['close']) for r in source]
        require(all(c > 0 for c in closes), 'Invalid close')
        rows, folds = report['series'], report['folds']
        require(type(rows) is list and len(rows) == 300 and type(folds) is list and len(folds) == 3, 'Invalid evaluation count')
        actual, predictions = [], {m: [] for m in METHODS}
        for offset, row in enumerate(rows):
            i = len(days)-301+offset
            require(row['signal_date'] == days[i] and row['target_date'] == days[i+1] and
                type(row['fold_id']) is int and row['fold_id'] == offset//100+1, 'Evaluation dates/labels mismatch')
            expected = closes[i+1]/closes[i]-1
            equal_number(row['actual_return'], expected, 'Actual decimal-return mismatch')
            actual.append(expected)
            require(type(row['predicted_returns']) is dict and set(row['predicted_returns']) == set(METHODS), 'Invalid methods')
            for method in METHODS:
                predictions[method].append(number(row['predicted_returns'][method]))
            require(predictions['zero_return'][-1] == 0, 'Nonzero zero-return baseline')
        for offset, fold in enumerate(folds):
            begin = len(days)-301+offset*100
            require(type(fold['fold_id']) is int and fold['fold_id'] == offset+1, 'Invalid fold ID')
            expected = dict(signal_start=days[begin], signal_end=days[begin+99],
                target_start=days[begin+1], target_end=days[begin+100], count=100)
            require(fold['evaluation'] == expected and type(fold['evaluation']['count']) is int, 'Evaluation fold bounds mismatch')
            train = fold['train']
            # Fixed ma60 warmup produces the first eligible signal at raw index59.
            # One immediately preceding signal is purged (its label equals eval start).
            expected_train = dict(signal_start=days[59], signal_end=days[begin-2],
                label_end=days[begin-1], count=begin-60, purged_count=1)
            require(train == expected_train and type(train['count']) is int and
                type(train['purged_count']) is int and train['count'] >= 300, 'Training purge/range mismatch')
            _check_metrics(fold['metrics'], actual[offset*100:(offset+1)*100],
                {m: predictions[m][offset*100:(offset+1)*100] for m in METHODS})
        _check_metrics(report['overall_metrics'], actual, predictions)
        return dict(status='PASS', report_id=report['report_id'], ts_code=report['ts_code'], checked_rows=300,
            source_rows=len(days), source_start=days[0], source_end=days[-1],
            exchange_calendar_completeness_verified=True, training_fit_provenance_verified=False,
            scope='raw adjacent-session observations, fixed date ranges and Decimal metrics',
            numeric_tolerance='absolute 1e-10 + relative 1e-8; fractional-return units',
            overall_metrics={m: {k:float(v) if v is not None else None for k,v in metrics(actual,predictions[m]).items()} for m in METHODS})
    except (KeyError, TypeError, ValueError, IndexError, InvalidOperation, OverflowError):
        raise VerificationError('Invalid verification input') from None


def _check_metrics(supplied, actual, predicted):
    require(type(supplied) is dict and set(supplied) == set(METHODS), 'Invalid metric methods')
    for method in METHODS:
        require(type(supplied[method]) is dict and set(supplied[method]) == {'rmse','mae','r2'}, 'Invalid metric fields')
        for name, expected in metrics(actual, predicted[method]).items():
            equal_number(supplied[method][name], expected, 'Decimal metric mismatch')


def run(args):
    require(args.stock == '000001.SZ', 'Unsupported stock')
    require(bool(re.fullmatch(r'eval_[0-9a-f]{32}', args.report)), 'Invalid report ID')
    first, last = day(args.start), day(args.end)
    require(first <= last, 'Inverted source bounds')
    require(math.isfinite(args.timeout) and 0 < args.timeout < 30, 'Timeout must be 0 < seconds < 30')
    try:
        parts = urlsplit(args.api_base)
        port = parts.port
    except ValueError:
        raise VerificationError('Invalid API base') from None
    require(parts.scheme in ('http','https') and parts.hostname and not parts.username and not parts.password and
        not parts.query and not parts.fragment and parts.path in ('','/') and (port is None or port > 0), 'Invalid API base')
    try:
        with Path(args.calendar).open('rb') as file:
            calendar = read_json(file.read(MAX_JSON+1))
    except OSError:
        raise VerificationError('Trusted local calendar unavailable') from None
    calendar_sessions(calendar, args.stock)
    source = load_source(args.stock, first, last, args.timeout)
    report = fetch(args.api_base.rstrip('/'), '/api/analysis/evaluations/'+args.stock+'/'+args.report, args.timeout)
    require(type(report) is dict and report.get('report_id') == args.report and report.get('ts_code') == args.stock, 'Request/report binding mismatch')
    require(report.get('source',{}).get('signal_start') == first and report.get('source',{}).get('signal_end') == last, 'Requested source bounds mismatch')
    return verify(source, report, calendar)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('stock','report','start','end','calendar'):
        parser.add_argument('--'+name, required=True)
    parser.add_argument('--api-base',default='http://127.0.0.1:8084')
    parser.add_argument('--timeout',type=float,default=15)
    args = parser.parse_args(argv)
    try:
        print(json.dumps(run(args), allow_nan=False, ensure_ascii=False))
        return 0
    except VerificationError as exc:
        print('Verification failed: '+str(exc), file=sys.stderr)
        return 1
    except Exception:
        print('Verification failed: dependency/input unavailable (details suppressed)', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
