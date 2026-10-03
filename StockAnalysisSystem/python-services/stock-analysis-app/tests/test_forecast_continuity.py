"""Explicit synthetic exchange sessions, never an inferred weekday fallback."""
import copy
import pytest
from tests.test_forecast_samples import history, calendar


def test_missing_calendar_fails_before_features(monkeypatch):
    from analysis.forecast.samples import build_samples
    from data_processor.feature_engineer import FeatureEngineer
    monkeypatch.setattr(FeatureEngineer, 'calculate_all_features', lambda *a: pytest.fail('features ran'))
    with pytest.raises(ValueError, match='calendar'):
        build_samples(history(), '000001.SZ')


@pytest.mark.parametrize('fault', ['hole', 'coverage', 'nonopen', 'duplicate', 'exchange', 'origin'])
def test_strict_calendar_gate(fault, monkeypatch):
    from analysis.forecast.samples import build_samples
    from data_processor.feature_engineer import FeatureEngineer
    frame, proof = history(), calendar()
    if fault == 'hole': frame = frame.drop(200)
    if fault == 'coverage': proof['start'] = proof['open_dates'][1]
    if fault == 'nonopen': proof['open_dates'].pop(200)
    if fault == 'duplicate': proof['open_dates'].insert(1, proof['open_dates'][0])
    if fault == 'exchange': proof['exchange'] = 'BSE'
    if fault == 'origin': proof['source'] = 'weekdays'
    monkeypatch.setattr(FeatureEngineer, 'calculate_all_features', lambda *a: pytest.fail('features ran'))
    with pytest.raises(ValueError, match='calendar|session|continuity'):
        build_samples(frame, '000001.SZ', proof)


def test_holiday_is_proven_adjacent_not_calendar_day():
    from analysis.forecast.samples import build_samples
    frame = history().drop([10, 11, 12]).reset_index(drop=True)
    samples = build_samples(frame, '000001.SZ', calendar(frame))
    assert samples['frame'].target_date.iloc[9] == frame.trade_date.iloc[10]
    assert samples['source_continuity']['session_count'] == len(frame)


def test_calendar_file_duplicate_keys_and_bounded_read(tmp_path):
    from analysis.forecast.continuity import load_calendar
    path = tmp_path / 'calendar.json'
    path.write_text('{"source":"tushare.trade_cal","source":"weekdays"}')
    with pytest.raises(ValueError, match='Duplicate'): load_calendar(path)
    path.write_text(' ' * (256 * 1024 + 1))
    with pytest.raises(ValueError, match='bound'): load_calendar(path)


def test_frozen_proof_and_current_inference_prefix():
    import pandas as pd
    from analysis.forecast.training import train_forecast, predict_latest
    frame = history()
    model, payload = train_forecast(frame, '000001.SZ', calendar(frame))
    extra = frame.tail(1).copy()
    extra['trade_date'] += pd.Timedelta(days=3)
    extended = pd.concat([frame, extra], ignore_index=True)
    revised = predict_latest(model, payload, extended, '000001.SZ', calendar(extended))
    assert revised['source_continuity'] == payload['source_continuity']
    assert revised['inference_continuity']['session_count'] == len(extended)
    with pytest.raises(ValueError, match='prefix|start'):
        predict_latest(model, payload, extended.iloc[1:], '000001.SZ', calendar(extended))


def test_contract_rejects_unproven_old_and_cross_session_pairs():
    from analysis.forecast.training import train_forecast
    from analysis.forecast.contracts import validate_payload
    _, payload = train_forecast(history(), '000001.SZ', calendar())
    old = copy.deepcopy(payload)
    old.pop('source_continuity')
    with pytest.raises(ValueError): validate_payload(old)
    payload['test_series'][0]['target_date'] = payload['test_series'][2]['signal_date']
    with pytest.raises(ValueError, match='session'): validate_payload(payload)
