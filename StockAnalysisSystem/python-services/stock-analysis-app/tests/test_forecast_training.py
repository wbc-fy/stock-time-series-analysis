import numpy as np
from tests.test_forecast_samples import history, calendar


def test_frozen_training_and_metric_alignment():
    from analysis.forecast.training import train_forecast
    model, payload = train_forecast(history(), '000001.SZ', calendar())
    assert model is not None, 'real trained XGBoost model required'
    model2, repeated = train_forecast(history(), '000001.SZ', calendar())
    assert payload['test_series'] == repeated['test_series']
    changed = history()
    changed.loc[350:, 'close'] *= 1.01
    changed.loc[350:, 'high'] += .3
    model3, _ = train_forecast(changed, '000001.SZ', calendar())
    assert model.get_booster().save_raw() == model3.get_booster().save_raw()
    actual = np.array([r['actual_return'] for r in payload['test_series']])
    pred = np.array([r['predicted_return'] for r in payload['test_series']])
    assert np.isclose(payload['metrics']['rmse'], np.sqrt(np.mean((actual-pred)**2)))
    assert np.isclose(payload['metrics']['mae'], np.mean(abs(actual-pred)))
    assert np.isclose(payload['metrics']['r2'], 1-np.sum((actual-pred)**2)/np.sum((actual-actual.mean())**2))
    assert np.isclose(payload['baseline_metrics']['rmse'], np.sqrt(np.mean(actual**2)))
    assert all(r['signal_date'] > payload['seen_through'] for r in payload['test_series'])


def test_loaded_prediction_does_not_fit(tmp_path, monkeypatch):
    from analysis.forecast.training import train_forecast, predict_latest
    from analysis.forecast.artifacts import ArtifactStore
    model, payload = train_forecast(history(), '000001.SZ', calendar())
    store = ArtifactStore(tmp_path)
    store.save(model, payload)
    loaded, metadata = store.load(payload['model_id'])
    monkeypatch.setattr(type(loaded), 'fit', lambda *a, **k: (_ for _ in ()).throw(AssertionError('fit called')))
    revision = predict_latest(loaded, metadata, history(), '000001.SZ', calendar())
    assert revision['test_series'] == payload['test_series']
    assert revision['latest'] == payload['latest']


def test_new_history_keeps_frozen_independent_test(tmp_path):
    import pandas as pd
    from analysis.forecast.training import train_forecast, predict_latest
    from analysis.forecast.artifacts import ArtifactStore
    original = history()
    model, payload = train_forecast(original, '000001.SZ', calendar())
    store = ArtifactStore(tmp_path)
    store.save(model, payload)
    loaded, metadata = store.load(payload['model_id'])
    extra = original.tail(1).copy()
    extra['trade_date'] = pd.bdate_range(original.trade_date.max(), periods=2)[-1]
    extended = pd.concat([original, extra])
    revised = predict_latest(loaded, metadata, extended, '000001.SZ', calendar(extended))
    assert revised['data_cutoff'] > payload['data_cutoff']
    assert revised['latest']['signal_date'] == revised['data_cutoff']
    for key in ('splits', 'seen_through', 'test_series', 'metrics', 'source_cutoff', 'data_hash', 'source_continuity'):
        assert revised[key] == payload[key]
