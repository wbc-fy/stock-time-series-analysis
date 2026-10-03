import numpy as np
import pandas as pd
import pytest


def history(n=900):
    dates = pd.bdate_range('2022-01-03', periods=n)
    close = 10 + np.arange(n) * .002 + np.sin(np.arange(n)) * .05
    frame = pd.DataFrame(dict(ts_code='000001.SZ', trade_date=dates,
        open=close, high=close+.2, low=close-.2, close=close, vol=1000.))
    days = dates.strftime('%Y-%m-%d').tolist()
    return frame, dict(source='tushare.trade_cal', exchange='SSE', start=days[0], end=days[-1], open_dates=days)


def test_raw_boundaries_and_strict_purge():
    from analysis.evaluation.protocol import build_folds
    frame, calendar = history()
    parts = build_folds(frame, '000001.SZ', calendar)['folds']
    assert len(parts) == 3
    for index, part in enumerate(parts):
        test, train = part['evaluation'], part['train']
        assert len(test) == 100 and len(train) >= 300
        assert test.signal_date.iloc[0] == frame.trade_date.iloc[599 + 100*index]
        assert train.target_date.max() < test.signal_date.iloc[0]
        assert part['purged_count'] == 1
    assert parts[-1]['evaluation'].target_date.iloc[-1] == frame.trade_date.iloc[-1]


@pytest.mark.parametrize('n', [299, 600, 2501])
def test_invalid_history(n):
    from analysis.evaluation.protocol import build_folds
    frame, calendar = history(n)
    with pytest.raises(ValueError):
        build_folds(frame, '000001.SZ', calendar)


def test_missing_session_precedes_features(monkeypatch):
    from analysis.evaluation.protocol import build_folds
    from analysis.forecast import samples
    frame, calendar = history()
    monkeypatch.setattr(samples.FeatureEngineer, 'calculate_all_features', lambda *args: pytest.fail('features reached'))
    with pytest.raises(ValueError, match='continuity'):
        build_folds(frame.drop(index=400), '000001.SZ', calendar)


def test_invalid_evaluation_features(monkeypatch):
    from analysis.evaluation.protocol import build_folds
    from analysis.forecast import samples
    frame, calendar = history()
    original = samples.FeatureEngineer.calculate_all_features
    def invalid(self, data):
        rows = original(self, data)
        rows.loc[650, 'ma60'] = np.nan
        return rows
    monkeypatch.setattr(samples.FeatureEngineer, 'calculate_all_features', invalid)
    with pytest.raises(ValueError, match='evaluation features'):
        build_folds(frame, '000001.SZ', calendar)
