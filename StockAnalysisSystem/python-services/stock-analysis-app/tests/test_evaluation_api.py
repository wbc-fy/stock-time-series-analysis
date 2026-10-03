"""Read-only evaluation HTTP trust boundary and isolated dependency lifecycle."""
import copy
import subprocess
import sys

import pytest
from fastapi.testclient import TestClient
from tests.test_evaluation_contracts import report


class KnownStock:
    def stock_exists(self, stock): return stock in ('000001.SZ', '000002.SZ')
    def health(self): return True


class Evaluations:
    def __init__(self, report=None):
        self.report = report
        self.calls = []

    def list(self, stock, limit=20):
        from analysis.evaluation.contracts import make_summary
        self.calls.append(('list', stock, limit))
        return [] if self.report is None else [make_summary(self.report, 'a'*64, 1234)]

    def get(self, stock, report_id):
        self.calls.append(('get', stock, report_id))
        return self.report


def client(repo):
    from api.forecast import create_app
    return TestClient(create_app(KnownStock(), repo))


def test_empty_and_limits():
    repo = Evaluations()
    with client(repo) as http:
        for suffix, bound in (('', 20), ('&limit=1', 1), ('&limit=100', 100)):
            response = http.get('/api/analysis/evaluations?ts_code=000001.SZ'+suffix)
            assert response.status_code == 200
            assert response.json() == []
            assert repo.calls[-1] == ('list', '000001.SZ', bound)
        assert http.get('/health').json() == {'status': 'UP', 'database': 'UP'}


@pytest.mark.parametrize('path', [
    '', '?ts_code=000001.US', '?ts_code=000001.SZ&limit=0',
    '?ts_code=000001.SZ&limit=101', '?ts_code=000001.SZ&limit=1.5',
    '?ts_code=000001.SZ&limit=-1', '?ts_code=000001.SZ&limit=abc',
    '?ts_code=000001.SZ&limit=20&limit=20', '?ts_code=000001.SZ&train=true',
    '?ts_code=000001.SZ&ts_code=000001.SZ', '/000001.SZ/invalid',
    '/000001.SZ/eval_'+'a'*32+'?limit=1',
])
def test_strict_requests(path):
    repo = Evaluations()
    with client(repo) as http:
        assert http.get('/api/analysis/evaluations'+path).status_code == 400
        assert repo.calls == []


def test_detail_complete_bound_and_readonly(report):
    repo = Evaluations(report)
    def forbidden(*args): pytest.fail('GET must not mutate or compute')
    repo.publish = repo.save = repo.train = repo.load_market = forbidden
    path = '/api/analysis/evaluations/000001.SZ/'+report['report_id']
    with client(repo) as http:
        assert http.get('/api/analysis/evaluations?ts_code=000001.SZ').json() == repo.list('000001.SZ')
        response = http.get(path)
        assert response.status_code == 200
        assert response.json() == report
        assert len(response.json()['series']) == 300
        assert http.get(path.replace('000001.SZ', '000002.SZ')).status_code == 404
        assert http.get(path[:-32]+'f'*32).status_code == 404
        assert http.get(path.replace('000001.SZ', '999999.SZ')).status_code == 404
        assert http.post(path).status_code == 405


def test_missing_report():
    with client(Evaluations()) as http:
        assert http.get('/api/analysis/evaluations/000001.SZ/eval_'+'f'*32).status_code == 404


@pytest.mark.parametrize('method', ['stock_exists', 'list', 'get'])
def test_dependency_failure_sanitized(method):
    stock, repo = KnownStock(), Evaluations()
    def fail(*args): raise RuntimeError('mysql://password-secret@private-host/root/report.json')
    setattr(stock if method == 'stock_exists' else repo, method, fail)
    from api.forecast import create_app
    with TestClient(create_app(stock, repo)) as http:
        path = '/api/analysis/evaluations?ts_code=000001.SZ' if method != 'get' else '/api/analysis/evaluations/000001.SZ/eval_'+'a'*32
        response = http.get(path)
        assert response.status_code == 503
        assert response.json() == {'detail': 'Evaluation dependency unavailable'}


def test_corrupt_injected_payload_sanitized(report):
    bad = copy.deepcopy(report)
    bad['series'][0]['actual_return'] = float('nan')
    repo = Evaluations(bad)
    repo.list = lambda *a: [{'password-secret': 'private path'}]
    with client(repo) as http:
        for path in ('?ts_code=000001.SZ', '/000001.SZ/'+report['report_id']):
            response = http.get('/api/analysis/evaluations'+path)
            assert response.status_code == 503
            assert response.json() == {'detail': 'Evaluation dependency unavailable'}


def test_import_and_get_no_training_config_features_or_writes():
    code = '''
import sys
from fastapi.testclient import TestClient
from api.forecast import create_app
class Stock:
    def stock_exists(self, stock): return True
class Repo:
    def list(self, *args): return []
    def get(self, *args): return None
    def __getattr__(self, name): raise AssertionError(name)
with TestClient(create_app(Stock(), Repo())) as http:
    assert http.get('/api/analysis/evaluations?ts_code=000001.SZ').json() == []
    assert http.get('/api/analysis/evaluations/000001.SZ/eval_'+'a'*32).status_code == 404
for module in ('config.settings', 'database.db_connector', 'sklearn', 'xgboost',
               'analysis.evaluation.training', 'analysis.forecast.training', 'analysis.forecast.features'):
    assert module not in sys.modules, module
'''
    result = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


def test_shared_production_connector_owned_once(monkeypatch):
    from api import forecast
    from analysis.evaluation import repository
    class Connector:
        closes = 0
        def close(self): self.closes += 1
    stock = KnownStock()
    stock.connector = Connector()
    monkeypatch.setattr(forecast, '_production_repository', lambda: stock)
    with TestClient(forecast.create_app()) as http:
        assert isinstance(http.app.state.evaluation_repository, repository.EvaluationRepository)
        assert http.app.state.evaluation_repository.connector is stock.connector
    assert stock.connector.closes == 1
    with TestClient(forecast.create_app(stock, Evaluations())):
        pass
    assert stock.connector.closes == 1


def test_failed_evaluation_creation_preserves_forecast(monkeypatch):
    from api import forecast
    from analysis.evaluation import repository
    stock = KnownStock()
    stock.connector = object()
    def fail(*args): raise RuntimeError('private path/password-secret')
    monkeypatch.setattr(repository, 'EvaluationRepository', fail)
    with TestClient(forecast.create_app(stock)) as http:
        assert http.app.state.evaluation_repository is None
        assert http.get('/health').status_code == 200
        assert http.get('/api/analysis/evaluations?ts_code=000001.SZ').json() == {
            'detail': 'Evaluation dependency unavailable'}
