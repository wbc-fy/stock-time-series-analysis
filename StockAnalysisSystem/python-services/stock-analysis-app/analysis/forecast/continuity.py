"""Bounded offline exchange-calendar gates; no network or weekday inference."""
import hashlib
import json
from datetime import date
from pathlib import Path
import re

MAX_CALENDAR_BYTES = 256 * 1024


def _day(value):
    if type(value) is not str or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value):
        raise ValueError('Invalid calendar ISO date')
    date.fromisoformat(value)
    return value


def dates_hash(days):
    return hashlib.sha256('\n'.join(days).encode('ascii')).hexdigest()


def _sessions(days):
    if type(days) is not list or not 1 <= len(days) <= 7500:
        raise ValueError('Invalid calendar session bound')
    for day in days:
        _day(day)
    if days != sorted(set(days)):
        raise ValueError('Invalid calendar session ordering or duplicates')


def validate_calendar(calendar, ts_code):
    if type(calendar) is not dict:
        raise ValueError('Trusted local calendar is required')
    try:
        exchange = 'BSE' if ts_code.endswith('.BJ') else 'SSE'
        if calendar['source'] != 'tushare.trade_cal' or calendar['exchange'] != exchange:
            raise ValueError('Incompatible calendar origin or exchange')
        start, end = _day(calendar['start']), _day(calendar['end'])
        span = (date.fromisoformat(end) - date.fromisoformat(start)).days
        if not 0 <= span <= 10000:
            raise ValueError('Invalid calendar coverage bound')
        days = calendar['open_dates']
        _sessions(days)
        if not start <= days[0] <= days[-1] <= end:
            raise ValueError('Invalid calendar coverage')
    except (KeyError, TypeError) as exc:
        raise ValueError('Invalid calendar evidence') from exc
    return calendar


def load_calendar(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('Duplicate calendar JSON key')
            result[key] = value
        return result
    with Path(path).open('rb') as stream:
        raw = stream.read(MAX_CALENDAR_BYTES + 1)
    if len(raw) > MAX_CALENDAR_BYTES:
        raise ValueError('Calendar exceeds size bound')
    return json.loads(raw, object_pairs_hook=unique,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError('Invalid calendar number')))


def verify_dates(days, calendar, ts_code):
    validate_calendar(calendar, ts_code)
    _sessions(days)
    if not 300 <= len(days) <= 2500:
        raise ValueError('Source continuity requires 300..2500 sessions')
    if not calendar['start'] <= days[0] <= days[-1] <= calendar['end']:
        raise ValueError('Incomplete calendar coverage for source')
    expected = [d for d in calendar['open_dates'] if days[0] <= d <= days[-1]]
    if days != expected:
        raise ValueError('Source continuity does not match every open calendar session')
    return dict(source=calendar['source'], exchange=calendar['exchange'],
                calendar_start=calendar['start'], calendar_end=calendar['end'],
                source_start=days[0], source_end=days[-1], session_count=len(days),
                open_dates=list(days), source_dates_hash=dates_hash(days))


def validate_evidence(proof, ts_code):
    if type(proof) is not dict:
        raise ValueError('Missing calendar continuity evidence')
    try:
        calendar = dict(source=proof['source'], exchange=proof['exchange'],
                        start=proof['calendar_start'], end=proof['calendar_end'], open_dates=proof['open_dates'])
        validate_calendar(calendar, ts_code)
        days = proof['open_dates']
        if (type(proof['session_count']) is not int or not 300 <= proof['session_count'] <= 2500
                or proof['session_count'] != len(days) or proof['source_start'] != days[0]
                or proof['source_end'] != days[-1] or proof['source_dates_hash'] != dates_hash(days)):
            raise ValueError('Invalid calendar continuity evidence')
    except (KeyError, TypeError) as exc:
        raise ValueError('Invalid calendar continuity evidence') from exc
    return days


def require_prefix(frozen, current):
    if current[:len(frozen)] != frozen:
        raise ValueError('Inference must retain frozen source start and historical date prefix')
