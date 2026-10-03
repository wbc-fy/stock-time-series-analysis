"""Bounded, read-only historical reconciliation, independent of training/metrics code.

PASS verifies scoped stored observations against an explicit trusted local exchange
calendar, not historical fit selection, model performance or vendor revisions.
"""
import argparse
import hashlib
from datetime import date
from decimal import Decimal, InvalidOperation
import json
import math
import os
from pathlib import Path
import re
import sys
from urllib.parse import urlencode, urlsplit
from urllib.request import urlopen

APP = Path(__file__).resolve().parents[1] / 'python-services' / 'stock-analysis-app'
MAX_JSON = 2 * 1024 * 1024
ABS_TOL = Decimal('1e-10')
REL_TOL = Decimal('1e-8')


class VerificationError(Exception):
    """Only sanitized messages should escape to the command line."""


def require(condition, message):
    if not condition:
        raise VerificationError(message)


def day(value):
    value = str(value)
    require(bool(re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value)), 'Invalid ISO date')
    try:
        date.fromisoformat(value)
    except ValueError:
        raise VerificationError('Invalid ISO date') from None
    return value


def number(value):
    require(not isinstance(value, (bool, str)) and value is not None, 'Invalid numeric value')
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise VerificationError('Invalid numeric value') from None
    require(result.is_finite(), 'Nonfinite numeric value')
    return result


def equal_number(actual, expected, message):
    if expected is None:
        require(actual is None, message)
    else:
        a, b = number(actual), number(expected)
        require(abs(a-b) <= ABS_TOL + REL_TOL * abs(b), message)


def metrics(actual, predicted):
    """Decimal oracle: no production forecast, numpy or metrics helpers."""
    count = Decimal(len(actual))
    errors = [a-p for a, p in zip(actual, predicted)]
    squared_error = sum((e*e for e in errors), Decimal(0))
    mean = sum(actual, Decimal(0)) / count
    variation = sum(((a-mean)**2 for a in actual), Decimal(0))
    return dict(rmse=(squared_error/count).sqrt(),
                mae=sum((abs(e) for e in errors), Decimal(0))/count,
                r2=1-squared_error/variation if len(actual) >= 2 and variation else None)


def calendar_sessions(calendar, stock):
    """Independent calendar oracle; never imports production continuity helpers."""
    require(type(calendar) is dict, 'Trusted local calendar required')
    exchange = 'BSE' if stock.endswith('.BJ') else 'SSE'
    require(calendar['source'] == 'tushare.trade_cal' and calendar['exchange'] == exchange,
            'Incompatible calendar origin/exchange')
    start, end = day(calendar['start']), day(calendar['end'])
    require(0 <= (date.fromisoformat(end)-date.fromisoformat(start)).days <= 10000,
            'Invalid calendar coverage bound')
    sessions = calendar['open_dates']
    require(type(sessions) is list and 0 < len(sessions) <= 7500, 'Invalid calendar session bound')
    require(all(type(d) is str and day(d) == d for d in sessions) and sessions == sorted(set(sessions)),
            'Invalid calendar sessions')
    require(start <= sessions[0] <= sessions[-1] <= end, 'Invalid calendar coverage')
    return sessions


def evidence_days(proof, calendar, stock):
    require(type(proof) is dict, 'Missing continuity evidence')
    sessions = calendar_sessions(calendar, stock)
    require(proof['source'] == calendar['source'] and proof['exchange'] == calendar['exchange'],
            'Continuity origin/exchange mismatch')
    start, end = day(proof['source_start']), day(proof['source_end'])
    coverage_start, coverage_end = day(proof['calendar_start']), day(proof['calendar_end'])
    require(coverage_start <= start <= end <= coverage_end and
            0 <= (date.fromisoformat(coverage_end)-date.fromisoformat(coverage_start)).days <= 10000 and
            calendar['start'] <= start <= end <= calendar['end'], 'Incomplete calendar coverage')
    days = proof['open_dates']
    require(type(days) is list and type(proof['session_count']) is int and
            0 < proof['session_count'] == len(days) <= 2500, 'Invalid continuity session count')
    require(days == [d for d in sessions if start <= d <= end] and days and days[0] == start and days[-1] == end,
            'Source calendar continuity mismatch')
    digest = hashlib.sha256('\n'.join(days).encode('ascii')).hexdigest()
    require(proof['source_dates_hash'] == digest, 'Source date hash mismatch')
    return days


def verify(source, dto, frozen, stock, model, calendar=None):
    """Require full frozen test series; never accept a truncated API tail."""
    try:
        require(isinstance(dto, dict) and set(dto) == {'metadata', 'test_series', 'latest'}, 'Invalid prediction DTO')
        metadata, series, latest = dto['metadata'], dto['test_series'], dto['latest']
        require(metadata['model_id'] == frozen['model_id'] == model and
                metadata['ts_code'] == frozen['ts_code'] == stock, 'Stock/model binding mismatch')
        for key, expected in (('schema_version', 1), ('model_type', 'xgboost'),
                              ('model_name', 'xgboost_regressor'), ('horizon', 1),
                              ('target', 'next_trading_day_close_return')):
            require(type(metadata[key]) is type(expected) and metadata[key] == expected, 'Unsupported forecast contract')
        immutable = {k: v for k, v in frozen.items() if k not in
                     ('test_series', 'latest', 'data_cutoff', 'feature_importance', 'inference_continuity')}
        require(set(metadata) == set(immutable) | {'data_cutoff', 'inference_continuity'}, 'Incomplete model metadata')
        for key, expected in immutable.items():
            require(metadata[key] == expected, 'Frozen metadata mismatch: '+key)
        require(series == frozen['test_series'], 'Frozen test series mismatch')
        require(0 < len(source) <= 2500, 'Source must contain 1..2500 rows')
        days = [day(row['trade_date']) for row in source]
        require(days == sorted(set(days)), 'Source dates must be ordered and unique')
        original_days = evidence_days(frozen['source_continuity'], calendar, stock)
        require(frozen['inference_continuity'] == frozen['source_continuity'], 'Invalid frozen inference evidence')
        current_days = evidence_days(metadata['inference_continuity'], calendar, stock)
        require(days == current_days and days[:len(original_days)] == original_days,
                'Current source must retain frozen start and historical date prefix')
        closes = [number(row['close']) for row in source]
        require(all(c > 0 for c in closes), 'Invalid source close')
        cutoff, original, seen = (day(metadata[k]) for k in ('data_cutoff', 'source_cutoff', 'seen_through'))
        require(seen < original <= cutoff == days[-1] and original == day(frozen['data_cutoff']),
                'Cutoff/seen boundary mismatch')
        require(original_days[-1] == original, 'Frozen source evidence cutoff mismatch')
        splits = metadata['splits']
        for name in ('train', 'val', 'test'):
            split = splits[name]
            start, end, label_end = (day(split[k]) for k in ('signal_start', 'signal_end', 'label_end'))
            require(start <= end < label_end and all(d in days for d in (start, end, label_end)), 'Invalid split dates')
            require(type(split['count']) is int and 0 < split['count'] <= 2500, 'Invalid split count')
        require(splits['train']['label_end'] < splits['val']['signal_start'] and
                splits['val']['label_end'] < splits['test']['signal_start'] and
                seen == splits['val']['label_end'], 'Split/seen ordering mismatch')
        require(bool(series) and len(series) == splits['test']['count'], 'Incomplete test series')
        signals = [day(row['signal_date']) for row in series]
        require(signals == sorted(set(signals)), 'Test dates must be ordered and unique')
        test = splits['test']
        require(signals[0] == test['signal_start'] and signals[-1] == test['signal_end'] and
                series[-1]['target_date'] == test['label_end'] <= original, 'Test bounds mismatch')
        index = {d: i for i, d in enumerate(days)}
        actual, predicted = [], []
        for row in series:
            signal, target = row['signal_date'], day(row['target_date'])
            require(signal in index and signal > seen, 'Missing/seen test signal')
            i = index[signal]
            require(i+1 < len(days) and target == days[i+1], 'Label is not next proven open exchange session')
            expected = closes[i+1]/closes[i]-1
            equal_number(row['actual_return'], expected, 'Actual decimal-return mismatch')
            actual.append(expected)
            predicted.append(number(row['predicted_return']))
        for group, values in (('metrics', metrics(actual, predicted)),
                              ('baseline_metrics', metrics(actual, [Decimal(0)]*len(actual)))):
            require(set(metadata[group]) == {'rmse', 'mae', 'r2'}, 'Incomplete metrics')
            for name, expected in values.items():
                equal_number(metadata[group][name], expected, group+' metric mismatch: '+name)
        require(day(latest['signal_date']) == cutoff and type(latest['horizon']) is int and latest['horizon'] == 1 and
                latest['target_date'] is None and latest['actual_return'] is None, 'Invalid latest unknown target')
        number(latest['predicted_return'])
        if cutoff == original:
            require(latest == frozen['latest'], 'Frozen latest prediction mismatch')
        gaps = [dict(from_date=a, to_date=b, calendar_days=(date.fromisoformat(b)-date.fromisoformat(a)).days)
                for a, b in zip(days, days[1:]) if (date.fromisoformat(b)-date.fromisoformat(a)).days > 7]
        return dict(status='PASS', scope='frozen source and current inference exchange-calendar continuity', ts_code=stock, model_id=model,
                    source_rows=len(source), source_start=days[0], source_end=days[-1],
                    frozen_source_rows=len(original_days), frozen_source_start=original_days[0], frozen_source_end=original_days[-1],
                    checked_test_rows=len(actual), test_count=test['count'], latest_cutoff=cutoff,
                    original_model_seen_through=seen, training_label_provenance_verified=False,
                    exchange_calendar_completeness_verified=True, observed_gaps_over_7_days=gaps,
                    metrics={k: float(v) if v is not None else None for k, v in metrics(actual, predicted).items()},
                    baseline_metrics={k: float(v) if v is not None else None for k, v in metrics(actual, [Decimal(0)]*len(actual)).items()},
                    numeric_tolerance='absolute 1e-10 + relative 1e-8; decimal-return units')
    except (KeyError, TypeError, ValueError, InvalidOperation, OverflowError, IndexError):
        raise VerificationError('Invalid verification input') from None


def read_json(data):
    require(len(data) <= MAX_JSON, 'JSON exceeds size bound')
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, 'Duplicate JSON key')
            result[key] = value
        return result
    def reject(value):
        raise VerificationError('Nonfinite JSON number')
    try:
        return json.loads(data, object_pairs_hook=pairs, parse_constant=reject)
    except (ValueError, TypeError, RecursionError):
        raise VerificationError('Invalid JSON') from None


def fetch(base, path, timeout):
    try:
        with urlopen(base+path, timeout=timeout) as response:
            return read_json(response.read(MAX_JSON+1))
    except VerificationError:
        raise
    except Exception:
        raise VerificationError('HTTP dependency unavailable (details suppressed)') from None


def load_source(stock, start, end, timeout):
    """Only a bound parameterized SELECT; never DDL, publication or cache writes."""
    try:
        import pymysql
        from dotenv import load_dotenv
        env = APP / '.env'
        load_dotenv(env if env.is_file() else APP.parents[1] / '.env')
        with pymysql.connect(host=os.getenv('DB_HOST', 'localhost'), port=int(os.getenv('DB_PORT', '3306')),
                             user=os.environ['DB_USER'], password=os.environ['DB_PASSWORD'],
                             database=os.environ['DB_NAME'], charset='utf8mb4',
                             connect_timeout=max(1, math.ceil(timeout)), read_timeout=timeout, write_timeout=timeout,
                             cursorclass=pymysql.cursors.DictCursor) as db:
            with db.cursor() as cursor:
                cursor.execute('SELECT /*+ MAX_EXECUTION_TIME(15000) */ trade_date, close FROM stock_daily '
                               'WHERE ts_code=%s AND trade_date BETWEEN %s AND %s ORDER BY trade_date ASC LIMIT %s',
                               (stock, start, end, 2501))
                return cursor.fetchall()
    except Exception:
        raise VerificationError('MySQL dependency unavailable (details suppressed)') from None


def run(args):
    require(bool(re.fullmatch(r'[0-9]{6}\.(SH|SZ|BJ)', args.stock)), 'Invalid stock')
    require(bool(re.fullmatch(r'v07_[0-9a-f]{32}', args.model)), 'Invalid model ID')
    first, last = day(args.start), day(args.end)
    require(first <= last, 'Inverted source bounds')
    require(math.isfinite(args.timeout) and 0 < args.timeout <= 30, 'Timeout must be 0 < seconds <= 30')
    parts = urlsplit(args.api_base)
    require(parts.scheme in ('http', 'https') and parts.hostname and not parts.username and not parts.password
            and not parts.query and not parts.fragment and parts.path in ('', '/'), 'Invalid API base')
    try:
        with Path(args.calendar).open('rb') as file:
            raw = file.read(256 * 1024 + 1)
        require(len(raw) <= 256 * 1024, 'Calendar exceeds size bound')
        calendar = read_json(raw)
        calendar_sessions(calendar, args.stock)
    except OSError:
        raise VerificationError('Trusted local calendar unavailable') from None
    root = APP / 'models' / 'v07'
    directory = root / args.model
    metadata_path = directory / 'metadata.json'
    require(not root.is_symlink() and not directory.is_symlink() and not metadata_path.is_symlink()
            and directory.resolve().parent == root.resolve(), 'Unsafe controlled metadata path')
    try:
        with metadata_path.open('rb') as file:
            frozen = read_json(file.read(MAX_JSON+1))
    except OSError:
        raise VerificationError('Controlled metadata unavailable') from None
    source = load_source(args.stock, first, last, args.timeout)
    dto = fetch(args.api_base.rstrip('/'), '/api/analysis/prediction/'+args.stock+'?'+
                urlencode(dict(model=args.model, limit=500)), args.timeout)
    return verify(source, dto, frozen, args.stock, args.model, calendar)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stock', required=True)
    parser.add_argument('--model', required=True)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--calendar', required=True, help='Trusted local tushare.trade_cal JSON')
    parser.add_argument('--api-base', default='http://127.0.0.1:8084')
    parser.add_argument('--timeout', type=float, default=15)
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
