import copy
import json
from contextlib import contextmanager
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from tests.test_forecast_samples import history, calendar


@pytest.fixture
def forecast():
    from analysis.forecast.training import train_forecast
    return train_forecast(history(), '000001.SZ', calendar())


@pytest.fixture
def connector():
    class Connector:
        engine = create_engine('sqlite://')
        Session = sessionmaker(bind=engine)

        @contextmanager
        def session_scope(self):
            session = self.Session()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()
    conn = Connector()
    with conn.engine.begin() as db:
        db.execute(text('CREATE TABLE analysis_result (id INTEGER PRIMARY KEY, ts_code TEXT, analysis_date TEXT, analysis_type TEXT, result TEXT, prediction REAL, confidence REAL, created_at TEXT)'))
    return conn


def test_artifact_roundtrip_immutable_and_safe(tmp_path, forecast):
    from analysis.forecast.artifacts import ArtifactStore
    model, payload = forecast
    store = ArtifactStore(tmp_path)
    assert store.save(model, payload) is not None, 'native artifact must be saved'
    loaded, metadata = store.load(payload['model_id'])
    assert metadata == payload
    assert loaded.get_booster().feature_names == payload['feature_names']
    with pytest.raises(ValueError): store.save(model, payload)
    with pytest.raises(ValueError): store.load('../unsafe')
    path = tmp_path/payload['model_id']/'metadata.json'
    bad = copy.deepcopy(payload)
    bad['ts_code'] = '600000.SH'
    path.write_text(json.dumps(bad))
    with pytest.raises(ValueError): store.load(payload['model_id'])


def test_transactional_stock_bound_publications(connector, tmp_path, forecast):
    from analysis.forecast.artifacts import ArtifactStore
    from analysis.forecast.repository import ForecastRepository
    model, payload = forecast
    ArtifactStore(tmp_path).save(model, payload)
    repo = ForecastRepository(connector, ArtifactStore(tmp_path))
    repo.publish(payload)
    assert len(repo.models('000001.SZ')) == 1
    assert repo.get('600000.SH', payload['model_id']) is None
    assert repo.get('000001.SZ', payload['model_id']) == payload
    with connector.engine.begin() as db:
        db.execute(text("INSERT INTO analysis_result (ts_code, analysis_type, result) VALUES ('000001.SZ','old', '{}')"))
        db.execute(text("INSERT INTO analysis_result (ts_code, analysis_type, result) VALUES ('000001.SZ','v07_xgboost_regression', '{}')"))
    assert len(repo.results('000001.SZ')) == 1
    assert repo.health() is True
    broken = copy.deepcopy(payload)
    broken['latest']['predicted_return'] = float('nan')
    with pytest.raises(ValueError): repo.publish(broken)
    assert len(repo.results('000001.SZ')) == 1


@pytest.mark.parametrize('change', ['schema', 'bool', 'order', 'stock', 'split', 'feature', 'infinity'])
def test_strict_contract(forecast, change):
    from analysis.forecast.contracts import validate_payload
    _, payload = forecast
    bad = copy.deepcopy(payload)
    if change == 'schema': bad['schema_version'] = True
    if change == 'bool': bad['latest']['predicted_return'] = True
    if change == 'order': bad['test_series'].reverse()
    if change == 'stock': bad['ts_code'] = '０００００１.SZ'
    if change == 'split': bad['splits']['train']['label_end'] = bad['splits']['val']['signal_start']
    if change == 'feature': bad['feature_names'].reverse()
    if change == 'infinity': bad['metrics']['rmse'] = float('inf')
    with pytest.raises(ValueError): validate_payload(bad)


def test_commit_failure_has_no_success(connector, tmp_path, forecast):
    from analysis.forecast.repository import ForecastRepository, DependencyError
    from analysis.forecast.artifacts import ArtifactStore
    model, payload = forecast
    ArtifactStore(tmp_path).save(model, payload)
    original = connector.session_scope
    @contextmanager
    def broken_scope():
        session = connector.Session()
        try:
            yield session
            raise RuntimeError('password secret')
        finally:
            session.rollback()
            session.close()
    connector.session_scope = broken_scope
    with pytest.raises(DependencyError, match='Forecast database unavailable'):
        ForecastRepository(connector, ArtifactStore(tmp_path)).publish(payload)
    connector.session_scope = original
    assert ForecastRepository(connector).models('000001.SZ') == []


def test_missing_files_never_publish(connector, tmp_path, forecast):
    from analysis.forecast.repository import ForecastRepository
    from analysis.forecast.artifacts import ArtifactStore
    _, payload = forecast
    repo = ForecastRepository(connector, ArtifactStore(tmp_path))
    with pytest.raises(ValueError): repo.publish(payload)
    assert repo.results('000001.SZ') == []


def test_connector_url_preserves_special_password_and_scoped_timeouts(monkeypatch):
    from database import db_connector
    captured = {}
    def create_engine(url, **kwargs):
        captured.update(url=url, kwargs=kwargs)
        return object()
    monkeypatch.setattr(db_connector, 'create_engine', create_engine)
    monkeypatch.setattr(db_connector, 'sessionmaker', lambda **kwargs: lambda: None)
    config = dict(host='localhost', port='3306', user='tester', password='p@ss:/?#', database='test',
                  charset='utf8mb4', read_timeout=7, write_timeout=8)
    db_connector.DatabaseConnector(config)
    assert not isinstance(captured['url'], str), 'SQLAlchemy URL object must preserve password verbatim'
    assert captured['url'].password == config['password']
    assert captured['kwargs']['connect_args']['read_timeout'] == 7
    assert captured['kwargs']['connect_args']['write_timeout'] == 8


def test_cli_invalid_inputs_fail_without_connection(monkeypatch, capsys):
    from scripts.forecast import main
    from database import db_connector
    monkeypatch.setattr(db_connector, 'DatabaseConnector', lambda *a: pytest.fail('unexpected connection'))
    assert main(['train', '--stock', 'unsafe', '--start', '2023-01-01', '--end', '2024-01-01', '--calendar', 'missing.json']) == 1
    assert 'Forecast failed' in capsys.readouterr().err


def test_market_read_is_bounded_and_left_joined(connector):
    from analysis.forecast.repository import ForecastRepository
    with connector.engine.begin() as db:
        db.execute(text('CREATE TABLE stock_daily (ts_code TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL, vol REAL, amount REAL)'))
        db.execute(text('CREATE TABLE stock_daily_basic (ts_code TEXT, trade_date TEXT, turnover_rate REAL, pe_ttm REAL, pb REAL)'))
        values = history()
        values['trade_date'] = values.trade_date.dt.strftime('%Y-%m-%d')
        values['amount'] = 10.
        db.execute(text('INSERT INTO stock_daily VALUES (:ts_code, :trade_date, :open, :high, :low, :close, :vol, :amount)'), values.to_dict('records'))
    repo = ForecastRepository(connector)
    loaded = repo.load_market('000001.SZ', '2023-01-01', '2025-01-01')
    assert len(loaded) == 400
    assert loaded.pe_ttm.isna().all()
    assert loaded.trade_date.is_monotonic_increasing
    with connector.engine.begin() as db:
        db.execute(text('INSERT INTO stock_daily SELECT * FROM stock_daily'))
        db.execute(text('INSERT INTO stock_daily SELECT * FROM stock_daily'))
        db.execute(text('INSERT INTO stock_daily SELECT * FROM stock_daily'))
    with pytest.raises(ValueError, match='2500'):
        repo.load_market('000001.SZ', '2023-01-01', '2025-01-01')


def test_duplicate_json_keys_rejected(forecast):
    from analysis.forecast.contracts import parse_payload
    _, payload = forecast
    raw = json.dumps(payload)
    with pytest.raises(ValueError, match='Duplicate'):
        parse_payload('{"schema_version": 1, '+raw[1:])


@pytest.mark.parametrize('field,value', [
    ('model_name', None), ('model_name', 'classifier'),
    ('model_type', None), ('model_type', 'pickle'),
    ('model_version', None), ('model_version', 7), ('model_version', '1.0.0'),
    ('created_at', None), ('created_at', 'yesterday'), ('created_at', '2026-10-03T12:00:00'),
    ('dependency_versions', {'unknown': '1'}), ('dependency_versions', {'xgboost': True, 'pandas': '2', 'numpy': '2'}),
])
def test_complete_model_metadata_required(forecast, field, value):
    from analysis.forecast.contracts import validate_payload
    _, payload = forecast
    broken = copy.deepcopy(payload)
    if value is None:
        broken.pop(field)
    else:
        broken[field] = value
    with pytest.raises(ValueError): validate_payload(broken)


def test_malformed_metadata_list_exclusion_and_direct_dependency_failure(connector, forecast):
    from analysis.forecast.repository import ForecastRepository, DependencyError
    _, payload = forecast
    broken = copy.deepcopy(payload)
    broken.pop('model_type')
    with connector.engine.begin() as db:
        db.execute(text('INSERT INTO analysis_result (ts_code, analysis_type, result) VALUES (:stock, :type, :result)'),
                   dict(stock=payload['ts_code'], type='v07_xgboost_regression', result=json.dumps(broken)))
    repo = ForecastRepository(connector)
    assert repo.models(payload['ts_code']) == []
    assert repo.results(payload['ts_code']) == []
    with pytest.raises(DependencyError, match='Forecast publication unavailable'):
        repo.get(payload['ts_code'], payload['model_id'])
    with pytest.raises(DependencyError, match='Forecast publication unavailable'):
        repo.get_model(payload['model_id'])
    assert repo.get('600000.SH', payload['model_id']) is None


def test_direct_corrupt_matching_publication_is_not_unknown(connector, forecast):
    from analysis.forecast.repository import ForecastRepository, DependencyError
    _, payload = forecast
    broken = copy.deepcopy(payload)
    broken['schema_version'] = 99
    with connector.engine.begin() as db:
        db.execute(text('INSERT INTO analysis_result (ts_code, analysis_type, result) VALUES (:stock, :type, :result)'),
                   dict(stock=payload['ts_code'], type='v07_xgboost_regression', result=json.dumps(broken)))
    repo = ForecastRepository(connector)
    with pytest.raises(DependencyError, match='Forecast publication unavailable'):
        repo.get(payload['ts_code'], payload['model_id'])
    with pytest.raises(DependencyError, match='Forecast publication unavailable'):
        repo.get_model(payload['model_id'])


def test_publish_extension_keeps_original_calendar_proof(connector, tmp_path, forecast):
    import pandas as pd
    from analysis.forecast.training import predict_latest
    from analysis.forecast.artifacts import ArtifactStore
    from analysis.forecast.repository import ForecastRepository
    model, payload = forecast
    store = ArtifactStore(tmp_path)
    store.save(model, payload)
    frame = history()
    extra = frame.tail(1).copy()
    extra['trade_date'] += pd.Timedelta(days=3)
    extended = pd.concat([frame, extra])
    revised = predict_latest(model, payload, extended, '000001.SZ', calendar(extended))
    repo = ForecastRepository(connector, store)
    repo.publish(revised)
    assert repo.get('000001.SZ', payload['model_id'])['source_continuity'] == payload['source_continuity']


def test_long_verified_history_publishes_lossless_compact_json_within_text_budget(connector):
    import pandas as pd
    from types import SimpleNamespace
    from analysis.forecast.training import train_forecast
    from analysis.forecast.repository import ForecastRepository
    frame = pd.concat([history()] * 3, ignore_index=True).iloc[:1150].copy()
    frame['trade_date'] = pd.bdate_range('2022-01-04', periods=len(frame))
    _, payload = train_forecast(frame, '000001.SZ', calendar(frame))
    compact = json.dumps(payload, allow_nan=False, separators=(',', ':'))
    assert len(json.dumps(payload).encode('utf-8')) > 65535
    assert len(compact.encode('utf-8')) <= 65535
    store = SimpleNamespace(load=lambda model_id: (None, payload))
    ForecastRepository(connector, store).publish(payload)
    with connector.engine.connect() as db:
        stored = db.execute(text('SELECT result FROM analysis_result')).scalar_one()
    assert stored == compact
    assert len(stored.encode('utf-8')) <= 65535
    assert json.loads(stored) == payload


def test_publication_text_utf8_bound_rejects_before_sql_and_preserves_rows(connector, forecast, monkeypatch):
    from types import SimpleNamespace
    from analysis.forecast.repository import ForecastRepository
    _, payload = forecast
    payload = copy.deepcopy(payload)
    payload['storage_note'] = '界' * 12000
    assert len(json.dumps(payload, separators=(',', ':')).encode('utf-8')) > 65535
    store = SimpleNamespace(load=lambda model_id: (None, payload))
    with monkeypatch.context() as isolated:
        isolated.setattr(connector, 'session_scope', lambda: pytest.fail('oversize publication reached SQL'))
        with pytest.raises(ValueError, match='65535.*UTF-8|UTF-8.*65535'):
            ForecastRepository(connector, store).publish(payload)
    with connector.engine.connect() as db:
        assert db.execute(text('SELECT COUNT(*) FROM analysis_result')).scalar_one() == 0
