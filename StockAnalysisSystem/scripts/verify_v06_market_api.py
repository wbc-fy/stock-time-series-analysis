"""Opt-in, bounded, read-only MySQL / V0.6 API historical reconciliation."""
import argparse
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
import json
import os
from pathlib import Path
import re
from urllib.parse import urlencode, urlsplit
from urllib.request import urlopen

FIELDS = ('ma5', 'ma10', 'ma20', 'vol_ma5', 'vol_ma10', 'volume_ratio')
SCALE = Decimal('0.000001')

class VerificationError(Exception):
    pass

def require(condition, message):
    if not condition:
        raise VerificationError(message)

def scope(code, start, end):
    require(bool(re.fullmatch(r'[0-9]{6}\.(SH|SZ|BJ)', code)), 'Invalid stock code')
    try:
        first, last = date.fromisoformat(start), date.fromisoformat(end)
    except ValueError:
        raise VerificationError('Dates must be valid ISO dates') from None
    require(0 <= (last-first).days <= 2000, 'Scope must be ordered and at most 2000 calendar days')
    return first, last

def decimal(value):
    return None if value is None else Decimal(str(value))

def scaled(value):
    return None if value is None else value.quantize(SCALE, rounding=ROUND_HALF_UP)

def oracle(rows, index):
    def mean(column, count):
        if index+1 < count:
            return None
        return scaled(sum((decimal(r[column]) for r in rows[index-count+1:index+1]), Decimal(0))/count)
    v5 = mean('vol', 5)
    return dict(ma5=mean('close',5), ma10=mean('close',10), ma20=mean('close',20),
                vol_ma5=v5, vol_ma10=mean('vol',10),
                volume_ratio=scaled(decimal(rows[index]['vol'])/v5) if v5 else None)

def ordered(rows):
    days = [str(r['trade_date']) for r in rows]
    require(days == sorted(set(days)), 'API dates must be strictly ordered and unique')
    return days

def check_indicators(source, actual, prior, fresh):
    require(ordered(actual) == ordered(source), 'Indicator dates differ from MySQL')
    history = ([] if fresh else prior) + source
    checked, unchecked = [], []
    for i, (row, got) in enumerate(zip(source, actual)):
        day = str(row['trade_date'])
        size = got['window_size']
        require(type(size) is int and 1 <= size <= 20, 'Invalid window_size')
        require(type(got['is_warmup']) is bool and got['is_warmup'] == (size < 20), 'Invalid window warmup')
        if fresh:
            require(size == min(i+1,20), 'Fresh window_size does not match empty-state assumption')
        for source_key, api_key in [('close','close'), ('vol','volume'), ('pct_chg','pct_chg')]:
            require(decimal(got[api_key]) == scaled(decimal(row[source_key])), f'{day}: source {api_key} mismatch')
        index = i + (0 if fresh else len(prior))
        if not fresh and (size != 20 or index < 19):
            unchecked.append(day)
            continue
        for key, expected in oracle(history,index).items():
            require(decimal(got[key]) == expected, f'{day}: {key} mismatch')
        checked.append(day)
    require(bool(checked), 'No full-window result to verify (or empty fresh scope)')
    return checked, unchecked

def fetch(base, path, timeout):
    try:
        with urlopen(base+path, timeout=timeout) as response:
            return json.load(response, parse_float=Decimal)
    except Exception:
        raise VerificationError('HTTP request failed or returned invalid JSON') from None

def run(args):
    first, last = scope(args.ts_code,args.start,args.end)
    parts = urlsplit(args.api_base)
    require(parts.scheme in ('http','https') and bool(parts.hostname) and not parts.username and not parts.password
            and not parts.query and not parts.fragment, 'API base must be an HTTP URL without credentials/query')
    require(0 < args.timeout <= 30, 'Timeout must be 0 < seconds <= 30')
    from dotenv import load_dotenv
    import pymysql
    load_dotenv(Path(__file__).resolve().parents[1]/'.env')
    try:
        db = pymysql.connect(host=os.getenv('DB_HOST','localhost'),port=int(os.getenv('DB_PORT','3306')),
                             user=os.environ['DB_USER'],password=os.environ['DB_PASSWORD'],database=os.environ['DB_NAME'],
                             connect_timeout=max(1,int(args.timeout)),read_timeout=args.timeout,write_timeout=args.timeout,
                             cursorclass=pymysql.cursors.DictCursor)
        with db:
            with db.cursor() as cursor:
                cursor.execute('SELECT trade_date,open,high,low,close,pre_close,`change`,pct_chg,vol,amount FROM stock_daily WHERE ts_code=%s AND trade_date BETWEEN %s AND %s ORDER BY trade_date ASC LIMIT %s', (args.ts_code,first,last,2001))
                source = cursor.fetchall()
                require(0 < len(source) <= 2000, 'Source range must contain 1..2000 rows')
                prior = []
                if not args.fresh_window:
                    cursor.execute('SELECT trade_date,close,vol FROM stock_daily WHERE ts_code=%s AND trade_date<%s ORDER BY trade_date DESC LIMIT %s', (args.ts_code,first,19))
                    prior = list(reversed(cursor.fetchall()))
    except VerificationError:
        raise
    except Exception:
        raise VerificationError('MySQL connection or read failed (details suppressed)') from None
    base = args.api_base.rstrip('/')
    require(fetch(base,'/actuator/health',args.timeout).get('status') == 'UP', 'API health is not UP')
    stocks = fetch(base,'/api/analysis/stocks?limit=6000',args.timeout)
    require(any(row.get('ts_code') == args.ts_code for row in stocks), 'Stock absent from bounded API stock pool')
    query = '?' + urlencode(dict(start=args.start,end=args.end,limit=2000))
    bars = fetch(base,'/api/analysis/kline/'+args.ts_code+query,args.timeout)['bars']
    require(ordered(bars) == ordered(source), 'Kline dates differ from MySQL')
    for row, got in zip(source,bars):
        for key, value in row.items():
            if key != 'trade_date':
                require(decimal(got[key]) == decimal(value), f'Kline {key} mismatch')
    actual = fetch(base,'/api/analysis/indicators/'+args.ts_code+query,args.timeout)['indicators']
    checked, unchecked = check_indicators(source,actual,prior,args.fresh_window)
    return dict(status='PASS',data='MySQL historical data; not Tushare real-time',rows=len(source),
                latest_date=str(source[-1]['trade_date']),matched_indicator_fields=len(checked)*6,
                checked_dates=checked,unchecked_warmup_or_context_dates=unchecked,
                mode='fresh-window (explicit empty-state assumption)' if args.fresh_window else 'normal (19 prior MySQL rows)')

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--api-base',default='http://127.0.0.1:8083')
    parser.add_argument('--ts-code',required=True)
    parser.add_argument('--end',default=date.today().isoformat())
    parser.add_argument('--start',default=(date.today()-timedelta(days=365)).isoformat())
    parser.add_argument('--timeout',type=float,default=10)
    parser.add_argument('--fresh-window',action='store_true',help='Assert first date began with empty Flink state; never resets anything')
    args = parser.parse_args()
    try:
        print(json.dumps(run(args),ensure_ascii=False))
        return 0
    except VerificationError as error:
        print(json.dumps(dict(status='FAIL',reason=str(error))))
        return 1
    except Exception:
        print(json.dumps(dict(status='FAIL',reason='Unexpected verification failure (details suppressed)')))
        return 1

if __name__ == '__main__':
    raise SystemExit(main())
