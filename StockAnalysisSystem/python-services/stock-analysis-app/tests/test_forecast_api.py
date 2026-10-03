"""Read-only HTTP boundaries exercised against real SQLite publications."""
import copy
import json
import os
import subprocess
import sys
from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from tests.test_forecast_publication import forecast


@pytest.fixture
def repo():
    from analysis.forecast.repository import ForecastRepository
    class Connector:
        engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
        Session = sessionmaker(bind=engine)

        @contextmanager
        def session_scope(self):
            with self.Session() as session:
                yield session
    conn = Connector()
    with conn.engine.begin() as db:
        db.execute(text('CREATE TABLE stock_basic (ts_code TEXT PRIMARY KEY)'))
        db.execute(text("INSERT INTO stock_basic VALUES ('000001.SZ'), ('000002.SZ')"))
        db.execute(text('CREATE TABLE analysis_result (id INTEGER PRIMARY KEY, ts_code TEXT, analysis_type TEXT, result TEXT)'))
    return ForecastRepository(conn)


@pytest.fixture
def client(repo):
    from api.forecast import create_app
    with TestClient(create_app(repo)) as client:
        yield client


def insert(repo, payload):
    with repo.connector.engine.begin() as db:
        db.execute(text('INSERT INTO analysis_result (ts_code, analysis_type, result) VALUES (:stock, :type, :result)'),
                   dict(stock=payload['ts_code'], type='v07_xgboost_regression', result=json.dumps(payload)))


def test_known_stock_without_model_is_empty(client, repo):
    assert repo.stock_exists('000002.SZ') is True
    assert repo.stock_exists('999999.SZ') is False
    for path in ('models?ts_code=000002.SZ', 'results/000002.SZ'):
        response = client.get('/api/analysis/'+path)
        assert response.status_code == 200
        assert response.json() == []
    assert client.get('/api/analysis/models?ts_code=999999.SZ').status_code == 404
    assert client.get('/api/analysis/results/999999.SZ').status_code == 404


def test_results_distinguish_publications_of_same_frozen_model(client, repo, forecast):
    _, payload = forecast
    insert(repo, payload)
    insert(repo, payload)
    with repo.connector.engine.connect() as db:
        actual_ids = list(db.execute(text('SELECT id FROM analysis_result ORDER BY id DESC')).scalars())
    results = client.get('/api/analysis/results/000001.SZ').json()
    assert [row.get('publication_id') for row in results] == actual_ids
    assert len(set(row['publication_id'] for row in results)) == 2
    assert all(type(row['publication_id']) is int and row['publication_id'] > 0 for row in results)
    assert [row['model_id'] for row in results] == [payload['model_id']] * 2
    models = client.get('/api/analysis/models?ts_code=000001.SZ').json()
    assert len(models) == 1
    assert 'publication_id' not in models[0]
    assert 'publication_id' not in repo.get('000001.SZ', payload['model_id'])


def test_publication_snapshot_and_read_only(client, repo, forecast, monkeypatch):
    _, payload = forecast
    insert(repo, payload)
    monkeypatch.setattr(repo, 'publish', lambda *a: pytest.fail('GET must not publish'))
    monkeypatch.setattr(repo, 'load_market', lambda *a: pytest.fail('GET must not compute features'))
    metadata = {k: v for k, v in payload.items() if k not in ('test_series', 'latest', 'feature_importance')}
    assert client.get('/api/analysis/models?ts_code=000001.SZ').json() == [metadata]
    response = client.get(f"/api/analysis/prediction/000001.SZ?model={payload['model_id']}&limit=2")
    assert response.status_code == 200
    assert response.json() == dict(metadata=metadata, test_series=payload['test_series'][-2:], latest=payload['latest'])
    importance = client.get(f"/api/analysis/models/{payload['model_id']}/importance?top=2").json()
    assert importance == dict(model_id=payload['model_id'], ts_code='000001.SZ', importance_method='gain',
                             feature_importance=sorted(payload['feature_importance'], key=lambda x: x['importance'], reverse=True)[:2])
    assert client.get('/api/analysis/results/000001.SZ').json() == repo.results('000001.SZ')
    assert client.get(f"/api/analysis/prediction/000002.SZ?model={payload['model_id']}").status_code == 404
    assert client.get('/api/analysis/prediction/000001.SZ?model=v07_'+'f'*32).status_code == 404
    assert client.post('/api/analysis/models?ts_code=000001.SZ').status_code == 405
    assert client.get('/health').json() == {'status': 'UP', 'database': 'UP'}


@pytest.mark.parametrize('path', [
    'models', 'models?ts_code=000001.US', 'models?ts_code=０００００１.SZ',
    'models?ts_code=000001.SZ&limit=0', 'models?ts_code=000001.SZ&limit=101',
    'models?ts_code=000001.SZ&limit=1.5', 'models?ts_code=000001.SZ&limit=abc',
    'models?ts_code=000001.SZ&date=2024-02-30',
    'models/invalid/importance', 'models/v07_'+'a'*32+'/importance?top=101',
    'prediction/000001.SZ', 'prediction/000001.SZ?model=../unsafe',
    'prediction/000001.SZ?model=v07_'+'a'*32+'&limit=501',
    'results/000001.SHX', 'results/000001.SZ?limit=-1',
])
def test_invalid_requests_are_400(client, path):
    assert client.get('/api/analysis/'+path).status_code == 400


def test_corrupt_matching_publication_is_503(client, repo, forecast):
    _, payload = forecast
    broken = copy.deepcopy(payload)
    broken['latest']['predicted_return'] = float('nan')
    insert(repo, broken)
    for path in (f"prediction/000001.SZ?model={payload['model_id']}", f"models/{payload['model_id']}/importance"):
        response = client.get('/api/analysis/'+path)
        assert response.status_code == 503
        assert response.json() == {'detail': 'Forecast dependency unavailable'}


def test_old_unverified_publication_hidden_and_direct_503_preserves_row(client, repo, forecast):
    _, payload = forecast
    old = copy.deepcopy(payload)
    old.pop('source_continuity')
    old.pop('inference_continuity')
    insert(repo, old)
    for path in ('models?ts_code=000001.SZ', 'results/000001.SZ'):
        assert client.get('/api/analysis/'+path).json() == []
    response = client.get(f"/api/analysis/prediction/000001.SZ?model={payload['model_id']}")
    assert response.status_code == 503
    assert response.json() == {'detail': 'Forecast dependency unavailable'}
    with repo.connector.engine.connect() as db:
        assert db.execute(text('SELECT COUNT(*) FROM analysis_result')).scalar() == 1


def test_dependency_errors_are_sanitized(client, repo, monkeypatch):
    def fail(*args):
        raise RuntimeError('mysql://user:password-secret@private-host/db')
    monkeypatch.setattr(repo, 'stock_exists', fail)
    response = client.get('/api/analysis/models?ts_code=000001.SZ')
    assert response.status_code == 503
    assert response.json() == {'detail': 'Forecast dependency unavailable'}
    monkeypatch.setattr(repo, 'health', fail)
    assert client.get('/health').status_code == 503
    assert client.get('/health').json() == {'status': 'DOWN', 'database': 'DOWN'}


def test_import_and_injected_lifespan_need_no_config_or_training():
    code = """
import sys
from fastapi.testclient import TestClient
from api.forecast import create_app
class Repository:
    def health(self): return True
with TestClient(create_app(Repository())) as client:
    assert client.get('/health').status_code == 200
assert 'config.settings' not in sys.modules
assert 'database.db_connector' not in sys.modules
assert 'xgboost' not in sys.modules
assert 'analysis.forecast.training' not in sys.modules
"""
    env = dict(os.environ)
    env.pop('DB_PASSWORD', None)
    result = subprocess.run([sys.executable, '-c', code], env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


def test_repository_reads_do_not_import_training():
    result = subprocess.run([sys.executable, '-c',
        "import sys; from analysis.forecast.repository import ForecastRepository; assert 'xgboost' not in sys.modules"],
        capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


def test_stock_exists_is_bounded_parameterized_select(repo):
    statements = []
    def capture(conn, cursor, statement, parameters, context, executemany):
        statements.append((statement, parameters))
    event.listen(repo.connector.engine, 'before_cursor_execute', capture)
    assert repo.stock_exists('000001.SZ') is True
    assert statements == [('SELECT 1 FROM stock_basic WHERE ts_code=? LIMIT 1', ('000001.SZ',))]
    with pytest.raises(ValueError):
        repo.stock_exists("000001.SZ' OR 1=1")
    assert len(statements) == 1


def test_production_factory_has_scoped_timeouts_and_closes(monkeypatch):
    from api import forecast as api
    from config import settings
    from database import db_connector
    captured = {}
    class Connector:
        def __init__(self, config):
            captured['config'] = config
        def close(self):
            captured['closed'] = True
    monkeypatch.setattr(db_connector, 'DatabaseConnector', Connector)
    defaults = copy.deepcopy(settings.DATABASE_CONFIG)
    with TestClient(api.create_app()) as client:
        assert captured['config']['connect_timeout'] == 5
        assert captured['config']['pool_timeout'] == 5
        assert captured['config']['read_timeout'] == 5
        assert captured['config']['write_timeout'] == 5
    assert captured['closed'] is True
    assert settings.DATABASE_CONFIG == defaults


def test_failed_startup_serves_dependency_down(monkeypatch):
    from api import forecast as api
    def fail():
        raise RuntimeError('password-secret')
    monkeypatch.setattr(api, '_production_repository', fail)
    with TestClient(api.create_app()) as client:
        assert client.get('/health').status_code == 503
        response = client.get('/api/analysis/results/000001.SZ')
        assert response.status_code == 503
        assert 'password-secret' not in response.text


def test_undefined_r2_is_null_and_bad_dates_are_dependency_failure(client, repo, forecast):
    _, payload = forecast
    payload = copy.deepcopy(payload)
    payload['metrics']['r2'] = None
    insert(repo, payload)
    response = client.get(f"/api/analysis/prediction/000001.SZ?model={payload['model_id']}")
    assert response.json()['metadata']['metrics']['r2'] is None
    payload['latest']['signal_date'] = '2024-02-30'
    insert(repo, payload)
    assert client.get(f"/api/analysis/prediction/000001.SZ?model={payload['model_id']}").status_code == 503


def test_oversized_publication_is_not_served(client, repo, forecast):
    _, payload = forecast
    payload = copy.deepcopy(payload)
    payload['unused_padding'] = 'x' * (2 * 1024 * 1024)
    insert(repo, payload)
    response = client.get(f"/api/analysis/prediction/000001.SZ?model={payload['model_id']}")
    assert response.status_code == 503
    assert client.get('/api/analysis/models?ts_code=000001.SZ').json() == []
