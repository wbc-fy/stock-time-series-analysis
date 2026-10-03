"""Independent oracle and adversarial checks; all history here is synthetic."""
import copy
import hashlib
import json
import sys
from types import SimpleNamespace
import importlib.util
from pathlib import Path
from decimal import Decimal

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / 'verify_v07_prediction.py'


def test_verifier_exists():
    assert SCRIPT.exists(), 'V0.7 read-only verifier is not implemented'


@pytest.fixture
def verifier():
    assert SCRIPT.exists(), 'V0.7 read-only verifier is not implemented'
    spec = importlib.util.spec_from_file_location('verify_v07', SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def sample():
    days = ['2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05',
            '2024-01-08', '2024-01-09', '2024-01-10', '2024-01-11',
            '2024-01-12', '2024-01-15']
    source = [dict(trade_date=d, close=Decimal(c)) for d, c in zip(
        days, ['10', '11', '12', '13', '14', '15', '16', '20', '10', '15'])]
    rows = [dict(signal_date=days[7], target_date=days[8], actual_return=-.5, predicted_return=0.),
            dict(signal_date=days[8], target_date=days[9], actual_return=.5, predicted_return=0.)]
    def split(a, b, c, n):
        return dict(signal_start=days[a], signal_end=days[b], label_end=days[c], count=n)
    frozen = dict(schema_version=1, model_id='v07_'+'a'*32, ts_code='000001.SZ',
                  model_type='xgboost', model_name='xgboost_regressor', horizon=1,
                  target='next_trading_day_close_return', data_cutoff=days[-1],
                  source_cutoff=days[-1], seen_through=days[6],
                  splits=dict(train=split(0, 1, 2, 2), val=split(3, 5, 6, 3), test=split(7, 8, 9, 2)),
                  metrics=dict(rmse=.5, mae=.5, r2=0.),
                  baseline_metrics=dict(rmse=.5, mae=.5, r2=0.), test_series=rows,
                  latest=dict(signal_date=days[-1], target_date=None, horizon=1, actual_return=None, predicted_return=.01),
                  feature_importance=[])
    proof = dict(source='tushare.trade_cal', exchange='SSE', calendar_start=days[0], calendar_end=days[-1],
                 source_start=days[0], source_end=days[-1], session_count=len(days), open_dates=days,
                 source_dates_hash=hashlib.sha256('\n'.join(days).encode('ascii')).hexdigest())
    frozen['source_continuity'] = copy.deepcopy(proof)
    frozen['inference_continuity'] = copy.deepcopy(proof)
    dto = dict(metadata=copy.deepcopy({k: v for k, v in frozen.items() if k not in ('test_series', 'latest', 'feature_importance')}),
               test_series=copy.deepcopy(rows), latest=copy.deepcopy(frozen['latest']))
    return source, dto, frozen


def check(verifier, sample):
    source, dto, frozen = sample
    proof = dto['metadata']['inference_continuity']
    calendar = dict(source=proof['source'], exchange=proof['exchange'], start=proof['calendar_start'],
                    end=proof['calendar_end'], open_dates=proof['open_dates'])
    return verifier.verify(source, dto, frozen, '000001.SZ', 'v07_'+'a'*32, calendar)


def test_verifies_full_test_metrics_and_bounds(verifier, sample):
    result = check(verifier, sample)
    assert result['status'] == 'PASS'
    assert result['checked_test_rows'] == result['test_count'] == 2
    assert result['source_rows'] == 10
    assert result['training_label_provenance_verified'] is False
    assert result['exchange_calendar_completeness_verified'] is True
    assert result['frozen_source_rows'] == 10


def test_rejects_unproven_long_observed_gap(verifier, sample):
    source, dto, frozen = sample
    source[-1]['trade_date'] = '2024-04-15'
    for payload in (dto['metadata'], frozen):
        payload['data_cutoff'] = payload['source_cutoff'] = '2024-04-15'
        payload['splits']['test']['label_end'] = '2024-04-15'
    dto['latest']['signal_date'] = frozen['latest']['signal_date'] = '2024-04-15'
    dto['test_series'][-1]['target_date'] = frozen['test_series'][-1]['target_date'] = '2024-04-15'
    with pytest.raises(verifier.VerificationError): check(verifier, sample)


def test_old_unverified_frozen_metadata_is_rejected(verifier, sample):
    source, dto, frozen = sample
    for payload in (dto['metadata'], frozen):
        payload.pop('source_continuity')
        payload.pop('inference_continuity')
    with pytest.raises(verifier.VerificationError):
        verifier.verify(source, dto, frozen, '000001.SZ', 'v07_'+'a'*32)


def test_external_calendar_missing_session_rejected(verifier, sample):
    source, dto, frozen = sample
    days = frozen['source_continuity']['open_dates']
    calendar = dict(source='tushare.trade_cal', exchange='SSE', start=days[0], end=days[-1],
                    open_dates=days[:4]+['2024-01-06']+days[4:])
    with pytest.raises(verifier.VerificationError):
        verifier.verify(source, dto, frozen, '000001.SZ', 'v07_'+'a'*32, calendar)


def test_extended_inference_has_separate_proof(verifier, sample):
    source, dto, frozen = sample
    source.append(dict(trade_date='2024-01-16', close=Decimal(15)))
    dto['metadata']['data_cutoff'] = dto['latest']['signal_date'] = '2024-01-16'
    proof = dto['metadata']['inference_continuity']
    proof['calendar_end'] = proof['source_end'] = '2024-01-16'
    proof['open_dates'].append('2024-01-16')
    proof['session_count'] += 1
    proof['source_dates_hash'] = hashlib.sha256('\n'.join(proof['open_dates']).encode('ascii')).hexdigest()
    result = check(verifier, sample)
    assert result['source_rows'] == 11 and result['frozen_source_rows'] == 10


@pytest.mark.parametrize('fault', ['next_date', 'duplicate_test', 'source_duplicate', 'source_unordered',
                                  'cutoff', 'seen', 'nonfinite', 'latest_actual', 'percent',
                                  'baseline', 'truncated', 'frozen', 'bad_close', 'missing_session'])
def test_rejects_faults(verifier, sample, fault):
    source, dto, frozen = sample
    if fault == 'next_date': dto['test_series'][0]['target_date'] = '2024-01-15'
    if fault == 'duplicate_test': dto['test_series'][1]['signal_date'] = dto['test_series'][0]['signal_date']
    if fault == 'source_duplicate': source[1]['trade_date'] = source[0]['trade_date']
    if fault == 'source_unordered': source.reverse()
    if fault == 'cutoff': dto['metadata']['source_cutoff'] = '2025-01-01'
    if fault == 'seen': dto['metadata']['seen_through'] = '2024-01-15'
    if fault == 'nonfinite': dto['metadata']['metrics']['rmse'] = float('nan')
    if fault == 'latest_actual': dto['latest']['actual_return'] = 0
    if fault == 'percent': dto['test_series'][0]['actual_return'] *= 100
    if fault == 'baseline': dto['metadata']['baseline_metrics']['mae'] = .01
    if fault == 'truncated': dto['test_series'].pop()
    if fault == 'frozen': frozen['test_series'][0]['predicted_return'] = .2
    if fault == 'bad_close': source[-1]['close'] = Decimal('Infinity')
    if fault == 'missing_session': source.pop(8)
    with pytest.raises(verifier.VerificationError):
        check(verifier, sample)


def test_checks_metrics_even_if_frozen_and_api_agree_on_wrong_value(verifier, sample):
    sample[1]['metadata']['metrics']['rmse'] = sample[2]['metrics']['rmse'] = .123
    with pytest.raises(verifier.VerificationError, match='metric'):
        check(verifier, sample)


def test_constant_target_r2_is_null(verifier, sample):
    source, dto, frozen = sample
    source[8]['close'] = source[9]['close'] = source[7]['close']
    for row in dto['test_series']: row['actual_return'] = 0.
    frozen['test_series'] = copy.deepcopy(dto['test_series'])
    for group in ('metrics', 'baseline_metrics'):
        dto['metadata'][group] = frozen[group] = dict(rmse=0., mae=0., r2=None)
    assert check(verifier, sample)['status'] == 'PASS'


def test_http_failure_is_sanitized(verifier, monkeypatch):
    def fail(*a, **k): raise RuntimeError('secret-password user@db')
    monkeypatch.setattr(verifier, 'urlopen', fail)
    with pytest.raises(verifier.VerificationError) as caught:
        verifier.fetch('http://127.0.0.1:8084', '/health', 5)
    assert 'secret-password' not in str(caught.value)


def test_cli_failure_is_sanitized(verifier, monkeypatch, capsys):
    def fail(*a, **k): raise RuntimeError('secret-password user@db')
    monkeypatch.setattr(verifier, 'run', fail)
    assert verifier.main(['--stock', '000001.SZ', '--model', 'v07_'+'a'*32,
                          '--start', '2024-01-02', '--end', '2024-01-15', '--calendar', 'calendar.json']) == 1
    assert 'secret-password' not in capsys.readouterr().err


def test_duplicate_json_keys_rejected(verifier):
    with pytest.raises(verifier.VerificationError):
        verifier.read_json('{"a": 1, "a": 2}')


def test_rejects_jointly_truncated_frozen_and_api_test(verifier, sample):
    source, dto, frozen = sample
    # Dropping the first signal from both sides must not turn partial validation into PASS.
    dto['test_series'].pop(0)
    frozen['test_series'].pop(0)
    for metadata in (dto['metadata'], frozen):
        metadata['splits']['test']['count'] = 1
    with pytest.raises(verifier.VerificationError):
        check(verifier, sample)


def test_rejects_boolean_horizon(verifier, sample):
    sample[1]['metadata']['horizon'] = sample[2]['horizon'] = True
    with pytest.raises(verifier.VerificationError):
        check(verifier, sample)


def test_http_read_and_timeout_are_bounded(verifier, monkeypatch):
    captured = {}
    class Response:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def read(self, bound):
            captured['bound'] = bound
            return b' ' * bound
    def open_url(url, timeout):
        captured.update(url=url, timeout=timeout)
        return Response()
    monkeypatch.setattr(verifier, 'urlopen', open_url)
    with pytest.raises(verifier.VerificationError, match='size bound'):
        verifier.fetch('http://127.0.0.1:8084', '/health', 7)
    assert captured == dict(url='http://127.0.0.1:8084/health', timeout=7, bound=verifier.MAX_JSON+1)


def test_mysql_select_bound_parameters_and_timeouts(verifier, monkeypatch):
    captured = {}
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def execute(self, sql, parameters): captured.update(sql=sql, parameters=parameters)
        def fetchall(self): return []
    class Connection:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def cursor(self): return Cursor()
    def connect(**kwargs):
        captured['connect'] = kwargs
        return Connection()
    monkeypatch.setitem(sys.modules, 'pymysql', SimpleNamespace(connect=connect, cursors=SimpleNamespace(DictCursor=object)))
    monkeypatch.setitem(sys.modules, 'dotenv', SimpleNamespace(load_dotenv=lambda *a: None))
    for key in ('DB_USER', 'DB_PASSWORD', 'DB_NAME'): monkeypatch.setenv(key, 'synthetic')
    assert verifier.load_source('000001.SZ', '2024-01-02', '2024-01-15', 7) == []
    assert captured['parameters'] == ('000001.SZ', '2024-01-02', '2024-01-15', 2501)
    assert captured['sql'].startswith('SELECT ') and 'LIMIT %s' in captured['sql']
    assert captured['connect']['connect_timeout'] == captured['connect']['read_timeout'] == captured['connect']['write_timeout'] == 7


def test_controlled_metadata_symlink_rejected_before_dependencies(verifier, sample, tmp_path, monkeypatch):
    proof = sample[2]['source_continuity']
    path = tmp_path / 'calendar.json'
    path.write_text(json.dumps(dict(source=proof['source'], exchange=proof['exchange'],
                                    start=proof['calendar_start'], end=proof['calendar_end'], open_dates=proof['open_dates'])))
    monkeypatch.setattr(verifier, 'APP', tmp_path)
    monkeypatch.setattr(Path, 'is_symlink', lambda self: self.name == 'v07_'+'a'*32)
    monkeypatch.setattr(verifier, 'load_source', lambda *a: pytest.fail('unexpected SQL'))
    args = SimpleNamespace(stock='000001.SZ', model='v07_'+'a'*32, start='2024-01-02', end='2024-01-15',
                           timeout=7, api_base='http://127.0.0.1:8084', calendar=str(path))
    with pytest.raises(verifier.VerificationError, match='Unsafe controlled'):
        verifier.run(args)
