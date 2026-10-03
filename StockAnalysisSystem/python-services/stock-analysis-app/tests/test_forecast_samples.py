import importlib.util
import numpy as np
import pandas as pd
import pytest


def history():
    close = 10 + np.arange(400) * .01 + np.sin(np.arange(400)) * .05
    return pd.DataFrame(dict(ts_code='000001.SZ', trade_date=pd.bdate_range('2023-01-01', periods=400), open=close, high=close+.2, low=close-.2, close=close, vol=1000.))


def calendar(frame=None):
    """Declared deterministic synthetic sessions, not production weekday inference."""
    frame = history() if frame is None else frame
    dates = sorted(frame.trade_date.dt.strftime('%Y-%m-%d').tolist())
    return dict(source='tushare.trade_cal', exchange='SSE', start=dates[0], end=dates[-1], open_dates=dates)


def test_causal_boundaries_and_unlabelled_latest():
    assert importlib.util.find_spec('analysis.forecast') is not None, 'causal forecast module is required'
    from analysis.forecast.samples import build_samples
    data = history()
    samples = build_samples(data, '000001.SZ', calendar())
    assert samples['train'].target_date.max() < samples['val'].signal_date.min()
    assert samples['val'].target_date.max() < samples['test'].signal_date.min()
    assert samples['latest'].signal_date.iloc[0] == data.trade_date.max()
    assert samples['latest'].actual_return.isna().all()
    assert all('future' not in name for name in samples['feature_names'])


@pytest.mark.parametrize('bad', ['duplicate', 'stock', 'ohlc', 'overflow'])
def test_rejects_invalid_market(bad):
    from analysis.forecast.samples import build_samples
    data = history()
    if bad == 'duplicate': data.loc[1, 'trade_date'] = data.loc[0, 'trade_date']
    if bad == 'stock': data.loc[1, 'ts_code'] = '600000.SH'
    if bad == 'ohlc': data.loc[1, 'close'] = 0
    if bad == 'overflow': data = pd.concat([data]*7)
    with pytest.raises(ValueError): build_samples(data, '000001.SZ', calendar())


def test_intraday_timestamps_are_not_daily_observations():
    from analysis.forecast.samples import build_samples
    data = history()
    data['trade_date'] += pd.Timedelta(hours=1)
    with pytest.raises(ValueError, match='daily'): build_samples(data, '000001.SZ', calendar())


def test_optional_valuation_missing_does_not_drop_latest():
    from analysis.forecast.samples import build_samples
    data = history()
    data['pe_ttm'] = np.inf
    samples = build_samples(data, '000001.SZ', calendar())
    assert len(samples['latest']) == 1
    assert samples['latest']['pe_ttm'].isna().all()
