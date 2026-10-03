import json
from contextlib import contextmanager
from types import SimpleNamespace
import pytest
from sqlalchemy import event, text
from tests.test_forecast_publication import connector as base_connector
from tests.test_evaluation_reports import report


@pytest.fixture
def connector(base_connector):
    # SQLite's JSON_EXTRACT returns an unquoted scalar; MySQL additionally needs JSON_UNQUOTE.
    with base_connector.engine.connect() as connection:
        connection.connection.driver_connection.create_function('JSON_UNQUOTE', 1, lambda v: v)
    return base_connector


def test_publish_file_first_and_exact_read(connector, tmp_path, report):
    from analysis.evaluation.reports import ReportStore
    from analysis.evaluation.repository import EvaluationRepository
    store = ReportStore(tmp_path)
    statements = []
    def inspect(conn, cursor, statement, parameters, context, many):
        statements.append(statement)
        if statement.startswith('INSERT'):
            assert (tmp_path / report['report_id'] / 'report.json').is_file()
    event.listen(connector.engine, 'before_cursor_execute', inspect)
    repo = EvaluationRepository(connector, store)
    assert repo.publish(report) == report['report_id']
    assert repo.get('000001.SZ', report['report_id']) == report
    assert len(repo.list('000001.SZ')) == 1
    assert repo.get('000001.SZ', 'eval_'+'0'*32) is None
    with connector.engine.connect() as db:
        row = db.execute(text('SELECT * FROM analysis_result')).mappings().one()
        assert row['prediction'] is None and row['confidence'] is None
        assert row['analysis_date'] == report['source']['signal_end']
        assert len(row['result'].encode('utf-8')) <= 65535
    assert all('UPDATE' not in s and 'DELETE' not in s for s in statements)


@pytest.mark.parametrize('phase', ['insert', 'commit'])
def test_database_failure_preserves_unlisted_orphan(connector, tmp_path, report, phase):
    from analysis.evaluation.reports import ReportStore
    from analysis.evaluation.repository import EvaluationRepository, DependencyError
    original = connector.session_scope
    @contextmanager
    def broken():
        with original() as session:
            if phase == 'insert':
                session.execute = lambda *a, **kw: (_ for _ in ()).throw(RuntimeError('password'))
            yield session
            if phase == 'commit': raise RuntimeError('password')
    connector.session_scope = broken
    repo = EvaluationRepository(connector, ReportStore(tmp_path))
    with pytest.raises(DependencyError, match='Evaluation database unavailable'): repo.publish(report)
    assert (tmp_path / report['report_id'] / 'report.json').is_file()
    connector.session_scope = original
    assert repo.list('000001.SZ') == []


def test_utf8_summary_bound_before_sql(connector, report, monkeypatch):
    from analysis.evaluation.repository import EvaluationRepository
    import analysis.evaluation.repository as module
    import analysis.evaluation.reports as reports
    summary = {'note': '界'*22000}
    monkeypatch.setattr(module, 'validate_summary', lambda value: value)
    monkeypatch.setattr(reports, 'validate_summary', lambda value: value)
    monkeypatch.setattr(connector, 'session_scope', lambda: pytest.fail('oversize reached SQL'))
    with pytest.raises(ValueError, match='byte bound'):
        EvaluationRepository(connector, SimpleNamespace(save=lambda r: summary)).publish(report)


def test_corruption_excluded_and_exact_lookup_fail_closed(connector, tmp_path, report):
    from analysis.evaluation.repository import EvaluationRepository, DependencyError
    from analysis.evaluation.reports import ReportStore
    store = ReportStore(tmp_path)
    summary = store.save(report)
    summary['schema_version'] = 99
    with connector.engine.begin() as db:
        db.execute(text('INSERT INTO analysis_result (ts_code,analysis_type,result) VALUES (:stock,:kind,:result)'),
            dict(stock='000001.SZ', kind='v08_walk_forward_evaluation', result=json.dumps(summary)))
        db.execute(text("INSERT INTO analysis_result (ts_code,analysis_type,result) VALUES ('000001.SZ','old','{}')"))
    repo = EvaluationRepository(connector, store)
    assert repo.list('000001.SZ') == []
    with pytest.raises(DependencyError): repo.get('000001.SZ', report['report_id'])
    with connector.engine.begin() as db:
        db.execute(text("INSERT INTO analysis_result (ts_code,analysis_type,result) VALUES ('000001.SZ','v08_walk_forward_evaluation','not json')"))
    with pytest.raises(DependencyError): repo.get('000001.SZ', report['report_id'])


def test_queries_bounded_parameterized_and_invalid_inputs(connector, tmp_path, report):
    from analysis.evaluation.repository import EvaluationRepository
    from analysis.evaluation.reports import ReportStore
    statements = []
    event.listen(connector.engine, 'before_cursor_execute', lambda c,u,s,p,x,m: statements.append((s,p)))
    repo = EvaluationRepository(connector, ReportStore(tmp_path))
    repo.list('000001.SZ', 20); repo.get('000001.SZ', report['report_id'])
    assert all('ORDER BY id DESC LIMIT' in s for s,p in statements)
    assert 'JSON_UNQUOTE(JSON_EXTRACT' in statements[-1][0]
    assert report['report_id'] not in statements[-1][0]
    for limit in (True, 0, 101, '1'):
        with pytest.raises(ValueError): repo.list('000001.SZ', limit)
    with pytest.raises(ValueError): repo.get('000001.SZ', '../unsafe')


def test_row_stock_binding_before_file_load(tmp_path, report):
    from analysis.evaluation.reports import ReportStore
    from analysis.evaluation.repository import EvaluationRepository, DependencyError
    store = ReportStore(tmp_path)
    summary = store.save(report)
    @contextmanager
    def scope():
        yield SimpleNamespace(execute=lambda *args: SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda:
            [{'ts_code': '600000.SH', 'result': json.dumps(summary)}])))
    repo = EvaluationRepository(SimpleNamespace(session_scope=scope), SimpleNamespace(load=lambda *a: pytest.fail('unbound file load')))
    assert repo.list('000001.SZ') == []
    with pytest.raises(DependencyError): repo.get('000001.SZ', report['report_id'])
