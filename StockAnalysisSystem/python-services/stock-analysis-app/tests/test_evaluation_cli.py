import sys
import logging
from types import SimpleNamespace
import pytest


def test_import_help_no_connector(monkeypatch, capsys):
    from database import db_connector
    monkeypatch.setattr(db_connector, 'DatabaseConnector', lambda *a: pytest.fail('unexpected connection'))
    from scripts.evaluate import main
    with pytest.raises(SystemExit) as exit: main(['--help'])
    assert exit.value.code == 0
    assert '--calendar' in capsys.readouterr().out


@pytest.mark.parametrize('stock,start,end,calendar', [
    ('600000.SH','2020-01-01','2024-01-01','missing'),
    ('000001.SZ','bad','2024-01-01','missing'),
    ('000001.SZ','2024-01-01','2020-01-01','missing'),
    ('000001.SZ','2020-01-01','2024-01-01','missing')])
def test_invalid_arguments_fail_without_configuration(stock,start,end,calendar,monkeypatch,capsys):
    from scripts.evaluate import main
    monkeypatch.setitem(sys.modules, 'config.settings', None)
    assert main(['--stock',stock,'--start',start,'--end',end,'--calendar',calendar]) == 1
    assert capsys.readouterr().err == 'Evaluation failed: check inputs, history, reports and database availability.\n'


def test_cli_fixed_pipeline_and_sanitized_failure(monkeypatch,capsys):
    from scripts import evaluate as cli
    calls = []
    class Connector:
        def __init__(self, config):
            assert all(config[k] == 5 for k in ('connect_timeout','pool_timeout','read_timeout','write_timeout'))
        def __enter__(self): return self
        def __exit__(self,*args): pass
    from database import db_connector
    monkeypatch.setattr(db_connector,'DatabaseConnector',Connector)
    monkeypatch.setitem(sys.modules,'config.settings',SimpleNamespace(DATABASE_CONFIG={}))
    monkeypatch.setattr(cli,'load_calendar',lambda p: {'calendar': True})
    monkeypatch.setattr(cli,'validate_calendar',lambda c,s: calls.append('calendar'))
    monkeypatch.setattr(cli.ForecastRepository,'load_market',lambda self,*args: calls.append('select') or 'frame')
    monkeypatch.setattr(cli,'evaluate',lambda *args: calls.append('evaluate') or dict(report_id='eval_'+'a'*32,ts_code='000001.SZ'))
    monkeypatch.setattr(cli.EvaluationRepository,'publish',lambda self,r: calls.append('publish') or r['report_id'])
    args=['--stock','000001.SZ','--start','2020-01-01','--end','2024-01-01','--calendar','calendar.json']
    assert cli.main(args) == 0
    assert calls == ['calendar','select','evaluate','publish']
    assert capsys.readouterr().out == 'Published eval_'+'a'*32+' for 000001.SZ\n'
    monkeypatch.setattr(cli.EvaluationRepository,'publish',lambda *a: (_ for _ in ()).throw(RuntimeError('secret password SQL')))
    assert cli.main(args) == 1
    output = capsys.readouterr()
    assert output.out == '' and 'secret' not in output.err


@pytest.mark.parametrize('failure', [None, 'connect', 'training', 'close'])
@pytest.mark.parametrize('previous_disable', [logging.NOTSET, logging.DEBUG])
def test_real_connector_logs_are_suppressed_and_logging_restored(
        failure, previous_disable, monkeypatch, capsys):
    from scripts import evaluate as cli
    from database import db_connector
    from sqlalchemy import create_engine

    # Exercise the actual constructor/_connect/close, replacing only external IO.
    config = dict(user='private-user', password='private-password',
                  host='private-host', port=3306, database='private-db', charset='utf8mb4')
    monkeypatch.setitem(sys.modules, 'config.settings', SimpleNamespace(DATABASE_CONFIG=config))
    console = logging.StreamHandler(sys.stderr)
    monkeypatch.setattr(db_connector.logger, 'handlers', [console])
    monkeypatch.setattr(db_connector.logger, 'level', logging.INFO)
    training_logger = logging.getLogger('evaluation_cli_training_test')
    monkeypatch.setattr(training_logger, 'handlers', [console])
    monkeypatch.setattr(training_logger, 'level', logging.INFO)
    monkeypatch.setattr(training_logger, 'propagate', False)
    engine = create_engine('sqlite:///:memory:')
    original_dispose = engine.dispose

    def dispose():
        original_dispose()
        if failure == 'close':
            db_connector.logger.error('private close diagnostic')
            raise RuntimeError('private close diagnostic')

    monkeypatch.setattr(engine, 'dispose', dispose)

    def create_test_engine(*args, **kwargs):
        if failure == 'connect':
            raise RuntimeError('private constructor diagnostic')
        return engine

    monkeypatch.setattr(db_connector, 'create_engine', create_test_engine)
    monkeypatch.setattr(cli, 'load_calendar', lambda p: {})
    monkeypatch.setattr(cli, 'validate_calendar', lambda *a: None)
    monkeypatch.setattr(cli.ForecastRepository, 'load_market', lambda *a: 'frame')

    def train(*args):
        training_logger.info('private training diagnostic')
        if failure == 'training':
            training_logger.error('private training failure')
            raise RuntimeError('private training failure')
        return {'report_id': 'eval_' + 'a' * 32}

    monkeypatch.setattr(cli, 'evaluate', train)
    monkeypatch.setattr(cli.EvaluationRepository, 'publish', lambda self, r: r['report_id'])
    initial_disable = logging.root.manager.disable
    logging.disable(previous_disable)
    try:
        result = cli.main(['--stock', '000001.SZ', '--start', '2020-01-01',
                           '--end', '2024-01-01', '--calendar', 'calendar.json'])
        assert logging.root.manager.disable == previous_disable
        output = capsys.readouterr()
        assert result == (1 if failure else 0)
        assert output.out == ('' if failure else 'Published eval_' + 'a' * 32 + ' for 000001.SZ\n')
        assert output.err == ('Evaluation failed: check inputs, history, reports and database availability.\n'
                              if failure else '')
        training_logger.warning('logging restored')
        assert capsys.readouterr().err == 'logging restored\n'
    finally:
        logging.disable(initial_disable)
        original_dispose()
