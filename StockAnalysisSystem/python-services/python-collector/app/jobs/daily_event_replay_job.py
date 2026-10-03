"""Read-only MySQL daily history replay into the existing Kafka input topic."""

import re

from app.events.event_factory import EventFactory
from app.market_data.codes import to_ts_code
from app.utils.logger import get_logger


logger = get_logger(__name__)

_TS_CODE_PATTERN = re.compile(r'^[0-9]{6}\.(?:SZ|SH|BJ)$')


def normalize_replay_ts_code(ts_code):
    """Normalize an optional A-share code and reject ambiguous filter input."""
    if ts_code is None:
        return None
    raw_code = str(ts_code).strip()
    if not raw_code:
        raise ValueError('ts-code 不能为空')
    normalized = to_ts_code(raw_code)
    if not _TS_CODE_PATTERN.fullmatch(normalized):
        raise ValueError('ts-code 必须是 6 位代码，可带 .SZ、.SH 或 .BJ 后缀')
    return normalized


class ReplayFailure(RuntimeError):
    """Replay stopped; counts include only batches confirmed by flush."""

    def __init__(self, published_count: int, batch_count: int):
        super().__init__('Kafka 历史重放失败')
        self.published_count = published_count
        self.batch_count = batch_count


class DailyEventReplayJob:
    def __init__(self, repository, producer):
        self.repository = repository
        self.producer = producer

    def execute(self, start_date, end_date, batch_size=5000, ts_code=None):
        if start_date > end_date:
            raise ValueError('start_date 不能晚于 end_date')
        if batch_size <= 0:
            raise ValueError('batch_size 必须大于 0')
        ts_code = normalize_replay_ts_code(ts_code)

        trace_id = f'replay-{start_date:%Y%m%d}-{end_date:%Y%m%d}'
        published_count = 0
        batch_count = 0
        try:
            for batch in self.repository.iter_daily_records(
                start_date, end_date, batch_size, ts_code
            ):
                for record in batch:
                    try:
                        event = EventFactory.create_daily_event(
                            record, trace_id=trace_id
                        )
                        self.producer.send_event(event)
                    except Exception:
                        # Settle earlier accepted records, but keep the record
                        # error primary and do not count this incomplete batch.
                        try:
                            self.producer.flush()
                        except Exception as settle_exc:
                            logger.warning(
                                'Kafka replay partial-batch settlement failed: %s',
                                type(settle_exc).__name__,
                            )
                        raise
                self.producer.flush()
                published_count += len(batch)
                batch_count += 1
        except Exception as exc:
            raise ReplayFailure(published_count, batch_count) from exc

        return {
            'success': True,
            'published_count': published_count,
            'batch_count': batch_count,
            'start_date': start_date,
            'end_date': end_date,
            'ts_code': ts_code,
        }
