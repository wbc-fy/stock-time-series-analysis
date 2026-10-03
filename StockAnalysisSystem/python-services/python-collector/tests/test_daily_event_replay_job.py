from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from app.models.stock_daily import StockDailyRecord


START = date(2026, 8, 1)
END = date(2026, 8, 28)


def record(code, day=START):
    return StockDailyRecord(
        ts_code=code, trade_date=day,
        open=Decimal('10.1234'), high=Decimal('11.0000'),
        low=Decimal('9.0000'), close=Decimal('10.5000'),
        pre_close=Decimal('10.0000'), change=Decimal('0.5000'),
        pct_chg=Decimal('5.0000'), vol=Decimal('100.0000'),
        amount=Decimal('1050.0000'), source='MYSQL_REPLAY',
    )


def test_replay_creates_real_events_and_flushes_each_batch():
    from app.jobs.daily_event_replay_job import DailyEventReplayJob

    repository = MagicMock()
    repository.iter_daily_records.return_value = [
        [record('000001.SZ'), record('000002.SZ')],
        [record('000001.SZ', date(2026, 8, 2))],
    ]
    producer = MagicMock()
    calls = []
    producer.send_event.side_effect = lambda event: calls.append(event)
    producer.flush.side_effect = lambda: calls.append('flush')

    result = DailyEventReplayJob(repository, producer).execute(
        START, END, batch_size=2
    )

    repository.iter_daily_records.assert_called_once_with(START, END, 2, None)
    assert result == {
        'success': True, 'published_count': 3, 'batch_count': 2,
        'start_date': START, 'end_date': END, 'ts_code': None,
    }
    assert [item if item == 'flush' else item.ts_code for item in calls] == [
        '000001.SZ', '000002.SZ', 'flush', '000001.SZ', 'flush',
    ]
    first = calls[0]
    assert first.event_id == 'MYSQL_REPLAY:000001.SZ:20260801'
    assert first.trace_id == 'replay-20260801-20260828'
    assert first.trade_date == START
    assert first.open == Decimal('10.1234')
    assert first.source == 'MYSQL_REPLAY'


@pytest.mark.parametrize('failure_at', ['send', 'flush'])
def test_replay_stops_on_first_kafka_error_with_confirmed_counts(failure_at):
    from app.jobs.daily_event_replay_job import DailyEventReplayJob, ReplayFailure

    repository = MagicMock()
    repository.iter_daily_records.return_value = [
        [record('000001.SZ')], [record('000002.SZ')], [record('000003.SZ')],
    ]
    producer = MagicMock()
    failing_method = producer.send_event if failure_at == 'send' else producer.flush
    failing_method.side_effect = [None, RuntimeError('broker failed')]

    with pytest.raises(ReplayFailure) as error:
        DailyEventReplayJob(repository, producer).execute(START, END, 1)

    assert error.value.published_count == 1
    assert error.value.batch_count == 1
    assert producer.send_event.call_count == 2
    assert producer.flush.call_count == 2


@pytest.mark.parametrize('settlement_fails', [False, True])
def test_replay_settles_partial_batch_once_but_keeps_enqueue_error_primary(
    settlement_fails
):
    from app.jobs.daily_event_replay_job import DailyEventReplayJob, ReplayFailure

    repository = MagicMock()
    repository.iter_daily_records.return_value = [[
        record('000001.SZ'), record('000002.SZ'), record('000003.SZ')
    ]]
    producer = MagicMock()
    enqueue_error = RuntimeError('enqueue failed')
    producer.send_event.side_effect = [None, enqueue_error]
    if settlement_fails:
        producer.flush.side_effect = RuntimeError('secret payload')

    with patch('app.jobs.daily_event_replay_job.logger', create=True) as logger:
        with pytest.raises(ReplayFailure) as error:
            DailyEventReplayJob(repository, producer).execute(START, END, 3)

    assert error.value.__cause__ is enqueue_error
    assert error.value.published_count == 0
    assert error.value.batch_count == 0
    assert producer.send_event.call_count == 2
    producer.flush.assert_called_once_with()
    if settlement_fails:
        assert logger.warning.called
        assert 'secret payload' not in str(logger.method_calls)


def test_replay_rejects_reversed_range_before_reading():
    from app.jobs.daily_event_replay_job import DailyEventReplayJob

    repository = MagicMock()
    producer = MagicMock()
    with pytest.raises(ValueError, match='start_date'):
        DailyEventReplayJob(repository, producer).execute(END, START)
    repository.iter_daily_records.assert_not_called()
    producer.send_event.assert_not_called()


def test_cli_parses_replay_arguments():
    from app.main import build_parser

    args = build_parser().parse_args([
        'replay-daily-events', '--start', '20260801', '--end', '2026-08-28',
        '--batch-size', '7', '--ts-code', '000001.sz',
    ])
    assert (args.command, args.start, args.end, args.batch_size, args.ts_code) == (
        'replay-daily-events', '20260801', '2026-08-28', 7, '000001.sz',
    )


def test_cli_ts_code_defaults_to_none():
    from app.main import build_parser

    args = build_parser().parse_args([
        'replay-daily-events', '--start', '20260801', '--end', '20260828',
    ])
    assert args.ts_code is None


@pytest.mark.parametrize('start,end,size', [
    ('bad-date', '20260828', 1),
    ('20260829', '20260828', 1),
    ('20260801', '20260828', 0),
])
def test_cli_rejects_invalid_arguments_before_opening_resources(start, end, size):
    from app.main import cmd_replay_daily_events

    args = MagicMock(start=start, end=end, batch_size=size)
    with patch('database.db_connector.DatabaseConnector') as connector:
        with pytest.raises(ValueError):
            cmd_replay_daily_events(args)
    connector.assert_not_called()


def test_cli_uses_direct_producer_and_closes_both_resources():
    from app.main import cmd_replay_daily_events

    db = MagicMock()
    session = MagicMock()

    @contextmanager
    def session_scope():
        yield session

    db.session_scope.side_effect = session_scope
    producer = MagicMock()
    with patch('database.db_connector.DatabaseConnector', return_value=db), \
         patch('app.kafka.producer.StockKafkaProducer', return_value=producer), \
         patch('app.jobs.daily_event_replay_job.DailyEventReplayJob') as job, \
         patch('app.main.logger') as logger:
        job.return_value.execute.return_value = {
            'success': True, 'published_count': 3, 'batch_count': 2,
        }
        code = cmd_replay_daily_events(MagicMock(
            start='20260801', end='20260828', batch_size=2,
            ts_code=' 000001.sz ',
        ))

    assert code == 0
    job.return_value.execute.assert_called_once_with(START, END, 2, '000001.SZ')
    assert logger.info.call_args.args[1:4] == (START, END, '000001.SZ')
    producer.close.assert_called_once_with(flush=False)
    db.close.assert_called_once_with()
    db.create_tables.assert_not_called()


def test_cli_reports_replay_failure_without_logging_broker_details():
    from app.main import cmd_replay_daily_events
    from app.jobs.daily_event_replay_job import ReplayFailure

    db = MagicMock()
    session = MagicMock()

    @contextmanager
    def session_scope():
        yield session

    db.session_scope.side_effect = session_scope
    producer = MagicMock()
    with patch('database.db_connector.DatabaseConnector', return_value=db), \
         patch('app.kafka.producer.StockKafkaProducer', return_value=producer), \
         patch('app.jobs.daily_event_replay_job.DailyEventReplayJob') as job, \
         patch('app.main.logger') as logger:
        job.return_value.execute.side_effect = ReplayFailure(1, 1)
        code = cmd_replay_daily_events(MagicMock(
            start='20260801', end='20260828', batch_size=2, ts_code=None
        ))

    assert code == 1
    assert 'broker' not in str(logger.method_calls).lower()
    producer.close.assert_called_once_with(flush=False)


@pytest.mark.parametrize('record_count,expected_flushes', [
    (0, 0), (1, 1), (3, 2),
])
def test_cli_flushes_exactly_once_per_nonempty_database_batch(
    record_count, expected_flushes
):
    from app.kafka.producer import StockKafkaProducer
    from app.main import cmd_replay_daily_events

    db = MagicMock()
    session = MagicMock()
    rows = [record(f'{index:06d}.SZ').to_dict()
            for index in range(record_count)]
    for row in rows:
        del row['source']
    session.execute.return_value.mappings.return_value = iter(rows)

    @contextmanager
    def session_scope():
        yield session

    db.session_scope.side_effect = session_scope
    client = MagicMock()
    client.flush.return_value = 0
    client.close.side_effect = client.flush  # Mimic native implicit flush.
    client.produce.side_effect = lambda **kwargs: kwargs['on_delivery'](
        None, MagicMock()
    )
    producer = StockKafkaProducer(
        client=client,
        producer_config={'bootstrap.servers': 'localhost:9092'},
        daily_topic='stock.ods.daily.v1',
    )
    with patch('database.db_connector.DatabaseConnector', return_value=db), \
         patch('app.kafka.producer.StockKafkaProducer', return_value=producer):
        code = cmd_replay_daily_events(MagicMock(
            start='20260801', end='20260828', batch_size=2, ts_code=None
        ))

    assert code == 0
    assert client.produce.call_count == record_count
    assert client.flush.call_count == expected_flushes
    client.close.assert_not_called()
    assert producer._client is None
    db.close.assert_called_once_with()


@pytest.mark.parametrize('ts_code', ['', '   ', 'not-a-stock', '12345.SZ', '000001.XX', '０００００１.SZ', '٠٠٠٠٠١.SZ'])
def test_cli_rejects_invalid_ts_code_before_opening_resources(ts_code):
    from app.main import cmd_replay_daily_events

    args = MagicMock(
        start='20260801', end='20260828', batch_size=5000, ts_code=ts_code,
    )
    with patch('database.db_connector.DatabaseConnector') as connector, \
         patch('app.kafka.producer.StockKafkaProducer') as producer:
        with pytest.raises(ValueError, match='ts-code'):
            cmd_replay_daily_events(args)

    connector.assert_not_called()
    producer.assert_not_called()


def test_single_stock_twenty_day_replay_preserves_event_semantics():
    from app.jobs.daily_event_replay_job import DailyEventReplayJob

    repository = MagicMock()
    records = [
        record('000001.SZ', date(2026, 8, day)) for day in range(1, 21)
    ]
    repository.iter_daily_records.return_value = [records[:7], records[7:14], records[14:]]
    producer = MagicMock()

    result = DailyEventReplayJob(repository, producer).execute(
        date(2026, 8, 1), date(2026, 8, 20), batch_size=7,
        ts_code='000001.SZ',
    )

    repository.iter_daily_records.assert_called_once_with(
        date(2026, 8, 1), date(2026, 8, 20), 7, '000001.SZ'
    )
    assert producer.send_event.call_count == 20
    assert producer.flush.call_count == 3
    events = [call.args[0] for call in producer.send_event.call_args_list]
    assert [event.trade_date for event in events] == [
        date(2026, 8, day) for day in range(1, 21)
    ]
    assert {event.ts_code for event in events} == {'000001.SZ'}
    assert result == {
        'success': True, 'published_count': 20, 'batch_count': 3,
        'start_date': date(2026, 8, 1), 'end_date': date(2026, 8, 20),
        'ts_code': '000001.SZ',
    }
