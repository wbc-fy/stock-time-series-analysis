"""Causal feature rows, with verified next-trading-session labels."""
import numpy as np
import pandas as pd
from data_processor.feature_engineer import FeatureEngineer
from .contracts import validate_stock, FEATURE_NAMES, FEATURE_VERSION
from .continuity import verify_dates


def prepare_causal_rows(frame, ts_code, calendar=None):
    validate_stock(ts_code)
    data = frame.copy(deep=True)
    if not 300 <= len(data) <= 2500:
        raise ValueError('History requires 300..2500 rows')
    required = ['trade_date', 'ts_code', 'open', 'high', 'low', 'close', 'vol']
    if any(name not in data for name in required):
        raise ValueError('Missing market columns')
    if not data.ts_code.eq(ts_code).all():
        raise ValueError('Market stock mismatch')
    if pd.api.types.is_numeric_dtype(data.trade_date.dtype):
        raise ValueError('Invalid daily market dates')
    try:
        data['trade_date'] = pd.to_datetime(data.trade_date, errors='raise')
    except (ValueError, TypeError) as exc:
        raise ValueError('Invalid market dates') from exc
    if data.trade_date.isna().any() or data.trade_date.duplicated().any():
        raise ValueError('Invalid or duplicate market dates')
    if data.trade_date.dt.tz is not None or not data.trade_date.eq(data.trade_date.dt.normalize()).all():
        raise ValueError('Invalid daily market dates')
    data = data.sort_values('trade_date').reset_index(drop=True)
    evidence = verify_dates(data.trade_date.dt.strftime('%Y-%m-%d').tolist(), calendar, ts_code)
    for name in ['open', 'high', 'low', 'close', 'vol']:
        data[name] = pd.to_numeric(data[name], errors='coerce')
        if not np.isfinite(data[name]).all():
            raise ValueError('Invalid market values')
    prices = data[['open', 'high', 'low', 'close']]
    if ((prices <= 0).any().any() or (data.vol < 0).any()
            or (data.high < prices.max(axis=1)).any()
            or (data.low > prices.min(axis=1)).any()):
        raise ValueError('Invalid OHLC bounds')
    for name in ['turnover_rate', 'pe_ttm', 'pb']:
        data[name] = pd.to_numeric(data[name], errors='coerce') if name in data else np.nan
    rows = FeatureEngineer().calculate_all_features(data)
    rows['signal_date'] = data.trade_date
    rows['target_date'] = data.trade_date.shift(-1)
    rows['actual_return'] = data.close.shift(-1) / data.close - 1
    rows[FEATURE_NAMES] = rows[FEATURE_NAMES].replace([np.inf, -np.inf], np.nan)
    return dict(frame=rows, source_continuity=evidence, feature_names=list(FEATURE_NAMES))


def build_samples(frame, ts_code, calendar=None):
    prepared = prepare_causal_rows(frame, ts_code, calendar)
    rows = prepared['frame']
    evidence = prepared['source_continuity']
    # Boundaries are fixed BEFORE warmup/label filtering.
    n = len(rows)
    val_start, test_start = rows.signal_date.iloc[int(n*.6)], rows.signal_date.iloc[int(n*.8)]
    eligible = rows.dropna(subset=['return_10d', 'ma60', 'rsi'])
    labelled = eligible.dropna(subset=['actual_return', 'target_date'])
    train = labelled[(labelled.signal_date < val_start) & (labelled.target_date < val_start)]
    val = labelled[(labelled.signal_date >= val_start) & (labelled.signal_date < test_start)
                   & (labelled.target_date < test_start)]
    test = labelled[labelled.signal_date >= test_start]
    latest = eligible[eligible.signal_date == rows.signal_date.iloc[-1]]
    if min(len(train), len(val), len(test), len(latest)) == 0:
        raise ValueError('Insufficient valid causal features')
    seen = max(train.target_date.max(), val.target_date.max())
    if test.signal_date.min() <= seen or latest.signal_date.iloc[0] <= seen:
        raise ValueError('Prediction must be unseen')
    split = {}
    for name, part in [('train', train), ('val', val), ('test', test)]:
        split[name] = dict(signal_start=part.signal_date.min().strftime('%Y-%m-%d'),
                           signal_end=part.signal_date.max().strftime('%Y-%m-%d'),
                           label_end=part.target_date.max().strftime('%Y-%m-%d'), count=len(part))
    return dict(source_continuity=evidence, feature_names=list(FEATURE_NAMES), frame=rows, train=train, val=val,
                test=test, latest=latest, splits=split,
                seen_through=seen.strftime('%Y-%m-%d'),
                data_cutoff=rows.signal_date.iloc[-1].strftime('%Y-%m-%d'))
