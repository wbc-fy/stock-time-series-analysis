from collections.abc import Mapping

try:
    from confluent_kafka import Producer as ConfluentProducer
except ImportError:  # pragma: no cover - exercised only without dependency
    ConfluentProducer = None

from app.events.stock_daily_event import StockDailyEvent
from app.kafka.delivery_callback import DeliveryCallback
from app.kafka.serializer import KafkaJsonSerializer


class KafkaProducerError(RuntimeError):
    """Kafka producer operation failed."""


class StockKafkaProducer:
    """Asynchronous producer for V0.3 stock events."""

    def __init__(
        self,
        client=None,
        producer_config: Mapping[str, object] | None = None,
        daily_topic: str | None = None,
    ):
        if producer_config is None or daily_topic is None:
            from app.config import KAFKA_CONFIG, KAFKA_TOPICS

        config = dict(
            producer_config if producer_config is not None else KAFKA_CONFIG
        )
        self._daily_topic = (
            daily_topic if daily_topic is not None else KAFKA_TOPICS['daily']
        )
        self._delivery_callback = DeliveryCallback()
        self._reported_failure_count = 0

        if client is not None:
            self._client = client
        elif ConfluentProducer is not None:
            self._client = ConfluentProducer(config)
        else:
            raise KafkaProducerError(
                'confluent-kafka is not installed; install project requirements'
            )

    def send_event(
        self,
        event: StockDailyEvent,
        topic: str | None = None,
    ) -> None:
        client = self._client
        if client is None:
            raise KafkaProducerError('Kafka producer is closed')
        try:
            client.produce(
                topic=topic or self._daily_topic,
                key=event.ts_code,
                value=KafkaJsonSerializer.serialize(event),
                on_delivery=self._delivery_callback,
            )
            client.poll(0)
        except Exception as exc:
            self._delivery_callback.record_failure(exc)
            raise KafkaProducerError(
                f'Kafka message enqueue failed: {exc}'
            ) from exc

    def flush(self, timeout: float | None = None) -> dict[str, object]:
        client = self._client
        if client is None:
            raise KafkaProducerError('Kafka producer is closed')
        if timeout is None:
            remaining = client.flush()
        else:
            remaining = client.flush(timeout)

        if remaining:
            raise KafkaProducerError(
                f'{remaining} Kafka message(s) remained undelivered after flush'
            )

        statistics = self.statistics
        failure_count = int(statistics['failureCount'])
        new_failure_count = failure_count - self._reported_failure_count
        if new_failure_count > 0:
            errors = statistics['errors']
            new_errors = errors[self._reported_failure_count:failure_count]
            self._reported_failure_count = failure_count
            last_error = (
                new_errors[-1] if new_errors else 'unknown delivery error'
            )
            raise KafkaProducerError(f'Kafka delivery failed: {last_error}')

        return statistics

    def close(self, flush: bool = True) -> dict[str, object]:
        """Optionally flush once, then release the native client reference."""
        if self._client is None:
            return self.statistics
        try:
            return self.flush() if flush else self.statistics
        finally:
            # Native close() may flush internally, and is absent in older clients.
            self._client = None

    @property
    def statistics(self) -> dict[str, object]:
        return self._delivery_callback.statistics
