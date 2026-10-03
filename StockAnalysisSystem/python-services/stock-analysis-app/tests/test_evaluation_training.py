import copy
import numpy as np
import pandas as pd
import pytest
from tests.test_evaluation_protocol import history


def test_isolated_six_fits_and_determinism(monkeypatch):
    from analysis.evaluation import training
    from analysis.forecast.contracts import FEATURE_NAMES
    fits = []
    boosted_inputs = []
    scaler_inputs = []
    original_scaler_fit = training.StandardScaler.fit
    def scaler_spy(self, x, *args, **kwargs):
        assert x.shape[1] == 23
        assert not x.isna().any().any()
        assert (x[['turnover_rate', 'pe_ttm', 'pb']] == 0).all().all()
        scaler_inputs.append(x.index.tolist())
        return original_scaler_fit(self, x, *args, **kwargs)
    monkeypatch.setattr(training.StandardScaler, 'fit', scaler_spy)
    for cls in (training.Ridge, training.xgb.XGBRegressor):
        original = cls.fit
        def spy(self, x, y, *args, _original=original, **kwargs):
            assert 'eval_set' not in kwargs
            assert x.shape[1] == 23
            fits.append((type(self).__name__, len(x)))
            if isinstance(self, training.xgb.XGBRegressor):
                assert x.columns.tolist() == FEATURE_NAMES
                boosted_inputs.append((x.copy(deep=True), y.copy(deep=True)))
            return _original(self, x, y, *args, **kwargs)
        monkeypatch.setattr(cls, 'fit', spy)
    frame, calendar = history()
    report = training.evaluate(frame, '000001.SZ', calendar)
    assert len(fits) == 6
    expected = training.build_folds(frame, '000001.SZ', calendar)
    assert scaler_inputs == [part['train'].index.tolist() for part in expected['folds']]
    assert len(boosted_inputs) == len(expected['folds'])
    for (x, y), part in zip(boosted_inputs, expected['folds']):
        pd.testing.assert_frame_equal(x, part['train'][FEATURE_NAMES])
        pd.testing.assert_series_equal(y, part['train'].actual_return)
    assert report['methods']['xgboost'] == training.PARAMS
    assert report['feature_names'] == FEATURE_NAMES
    second = training.evaluate(frame, '000001.SZ', calendar)
    for key in ('report_id', 'created_at'):
        report.pop(key); second.pop(key)
    assert report == second
    changed = frame.copy()
    changed.loc[800:, ['open', 'high', 'low', 'close']] *= 1.1
    future = training.evaluate(changed, '000001.SZ', calendar)
    assert future['series'][:100] == report['series'][:100]


def test_metrics_reject_invalid_and_constant_r2():
    from analysis.evaluation.training import metrics
    assert metrics([1, 1], [0, 0])['r2'] is None
    for a, p in (([], []), ([0], [np.nan]), ([np.inf], [0]), ([1], [0, 1])):
        with pytest.raises(ValueError):
            metrics(a, p)


@pytest.mark.parametrize('value,count', [(.01, 100), (.1, 300), (.01, 1)])
@pytest.mark.parametrize('perfect', [True, False])
def test_exact_constant_metrics_have_null_r2(value, count, perfect):
    from analysis.evaluation.training import metrics
    result = metrics([value]*count, [value if perfect else 0.]*count)
    assert result['r2'] is None
    assert result['rmse'] == pytest.approx(0. if perfect else value)
    assert result['mae'] == pytest.approx(0. if perfect else value)


def test_small_genuinely_varying_metrics_keep_r2():
    from analysis.evaluation.training import metrics
    actual = [.01, np.nextafter(.01, np.inf)]
    assert metrics(actual, actual)['r2'] == 1.
