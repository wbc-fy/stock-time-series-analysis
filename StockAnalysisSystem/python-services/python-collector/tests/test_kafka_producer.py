from datetime import date, datetime
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from app.events.event_factory import EventFactory
from app.kafka.producer import KafkaProducerError, StockKafkaProducer
from app.models.stock_daily import StockDailyRecord


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def create_event():
    record = StockDailyRecord(
        ts_code="000001.SZ",
        trade_date=date(2026, 7, 3),
        open=10.25,
        high=10.68,
        low=10.12,
        close=10.55,
        pre_close=10.20,
        change=0.35,
        pct_chg=3.4314,
        vol=1250345.00,
        amount=13054890.25,
        source="tushare",
    )
    return EventFactory.create_daily_event(
        record=record,
        trace_id="daily-tushare-20260703-10001",
        ingest_time=datetime(2026, 7, 3, 16, 20, 30, tzinfo=SHANGHAI_TZ),
    )


def create_producer(client):
    return StockKafkaProducer(
        client=client,
        producer_config={"bootstrap.servers": "localhost:9092"},
        daily_topic="stock.ods.daily.v1",
    )


def test_send_event_uses_ts_code_as_key_and_serialized_json_as_value():
    client = MagicMock()
    producer = create_producer(client)

    producer.send_event(create_event())

    call = client.produce.call_args
    assert call.kwargs["topic"] == "stock.ods.daily.v1"
    assert call.kwargs["key"] == "000001.SZ"
    assert isinstance(call.kwargs["value"], bytes)
    assert b'"schemaVersion":1' in call.kwargs["value"]
    assert callable(call.kwargs["on_delivery"])
    client.poll.assert_called_once_with(0)


def test_delivery_callback_records_success():
    client = MagicMock()
    producer = create_producer(client)
    producer.send_event(create_event())

    callback = client.produce.call_args.kwargs["on_delivery"]
    callback(None, MagicMock())

    assert producer.statistics == {
        "successCount": 1,
        "failureCount": 0,
        "errors": [],
    }


def test_delivery_callback_records_failure_and_flush_raises():
    client = MagicMock()
    client.flush.return_value = 0
    producer = create_producer(client)
    producer.send_event(create_event())

    callback = client.produce.call_args.kwargs["on_delivery"]
    callback(RuntimeError("broker unavailable"), MagicMock())

    with pytest.raises(KafkaProducerError, match="broker unavailable"):
        producer.flush()

    assert producer.statistics["failureCount"] == 1


def test_synchronous_produce_failure_is_counted_and_raised():
    client = MagicMock()
    client.produce.side_effect = BufferError("local queue full")
    producer = create_producer(client)

    with pytest.raises(KafkaProducerError, match="local queue full"):
        producer.send_event(create_event())

    assert producer.statistics["failureCount"] == 1


def test_flush_raises_when_messages_remain_undelivered():
    client = MagicMock()
    client.flush.return_value = 2
    producer = create_producer(client)

    with pytest.raises(KafkaProducerError, match="2"):
        producer.flush(timeout=3.0)

    client.flush.assert_called_once_with(3.0)


def test_close_flushes_messages():
    client = MagicMock()
    client.flush.return_value = 0
    client.close.side_effect = client.flush  # Native close can flush again.
    producer = create_producer(client)

    producer.close()

    client.flush.assert_called_once()
    client.close.assert_not_called()
    assert producer._client is None


def test_close_without_flush_releases_client_without_extra_flush():
    client = MagicMock()
    client.close.side_effect = client.flush  # Detect an implicit native flush.
    producer = create_producer(client)

    producer.close(flush=False)

    client.flush.assert_not_called()
    client.close.assert_not_called()
    assert producer._client is None


def test_close_is_idempotent_and_does_not_flush_again():
    client = MagicMock()
    client.flush.return_value = 0
    producer = create_producer(client)

    producer.close()
    producer.close()

    client.flush.assert_called_once_with()
    client.close.assert_not_called()
    assert producer._client is None


def test_close_releases_client_even_when_flush_fails():
    client = MagicMock()
    client.flush.return_value = 2
    producer = create_producer(client)

    with pytest.raises(KafkaProducerError, match='undelivered'):
        producer.close()

    client.flush.assert_called_once_with()
    client.close.assert_not_called()
    assert producer._client is None


def test_send_and_flush_fail_clearly_after_close():
    client = MagicMock()
    producer = create_producer(client)
    producer.close(flush=False)

    with pytest.raises(KafkaProducerError, match='closed'):
        producer.send_event(create_event())
    with pytest.raises(KafkaProducerError, match='closed'):
        producer.flush()

    client.produce.assert_not_called()
    client.flush.assert_not_called()


def test_successful_batch_is_not_affected_by_previous_failure():
    client = MagicMock()
    client.flush.return_value = 0
    producer = create_producer(client)

    producer.send_event(create_event())
    first_callback = client.produce.call_args.kwargs["on_delivery"]
    first_callback(RuntimeError("first batch failed"), MagicMock())

    with pytest.raises(KafkaProducerError, match="first batch failed"):
        producer.flush()

    producer.send_event(create_event())
    second_callback = client.produce.call_args.kwargs["on_delivery"]
    second_callback(None, MagicMock())

    result = producer.flush()

    assert result["successCount"] == 1
    assert result["failureCount"] == 1
