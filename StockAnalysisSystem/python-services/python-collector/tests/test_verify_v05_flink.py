import importlib.util
import json
import sys
import subprocess
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
import simplejson


SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "verify_v05_flink.py"
SPEC = importlib.util.spec_from_file_location("verify_v05_flink", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_optimized_verifier_refuses_to_run_before_external_resources():
    result = subprocess.run(
        [sys.executable, '-O', str(SCRIPT), '--scenario', 'valid'],
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload['status'] == 'FAIL'
    assert 'optimized' in payload['error']


def test_inflight_proof_requires_uncommitted_output_and_no_committed_output():
    expected = MODULE.expected_indicators(fixture_rows(2))
    actual = [result_record(item) for item in expected]
    MODULE.validate_inflight_visibility(actual, [], expected)
    with pytest.raises(AssertionError, match='already committed'):
        MODULE.validate_inflight_visibility(actual, actual[:1], expected)
    with pytest.raises(AssertionError, match='result count'):
        MODULE.validate_inflight_visibility(actual[:1], [], expected)


def test_isolated_fault_guard_refuses_any_existing_job():
    spec = importlib.util.spec_from_file_location('isolated', SCRIPT.with_name('verify_v05_isolated.py'))
    isolated = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(isolated)
    isolated.require_empty_cluster([])
    with pytest.raises(RuntimeError, match='existing jobs'):
        isolated.require_empty_cluster([{'jid': 'main', 'state': 'RUNNING'}])
    with pytest.raises(RuntimeError, match='existing jobs'):
        isolated.require_empty_cluster([{'jid': 'main', 'state': 'RESTARTING'}])


def test_lag_validation_requires_every_partition_caught_up():
    spec = importlib.util.spec_from_file_location('isolated', SCRIPT.with_name('verify_v05_isolated.py'))
    isolated = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(isolated)
    isolated.validate_partition_lag([
        {'partition': 0, 'high': 20, 'committed': 20},
        {'partition': 1, 'high': 0, 'committed': -1001},
        {'partition': 2, 'high': 0, 'committed': -1001},
    ])
    with pytest.raises(AssertionError, match='lag'):
        isolated.validate_partition_lag([{'partition': 0, 'high': 20, 'committed': 19}])


def test_restore_requires_matching_id_and_normalized_checkpoint_path():
    spec = importlib.util.spec_from_file_location('isolated', SCRIPT.with_name('verify_v05_isolated.py'))
    isolated = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(isolated)
    pointer = 'file:///opt/flink/checkpoints/isolated/job/chk-2'
    isolated.validate_restored_checkpoint(
        {'id': 2, 'external_path': 'file:/opt/flink/checkpoints/isolated/job/chk-2'}, 2, pointer,
    )
    with pytest.raises(AssertionError, match='checkpoint path'):
        isolated.validate_restored_checkpoint(
            {'id': 2, 'external_path': 'file:/opt/flink/checkpoints/another/job/chk-2'}, 2, pointer,
        )
    with pytest.raises(AssertionError, match='checkpoint path'):
        isolated.validate_restored_checkpoint({'id': 2}, 2, pointer)
    with pytest.raises(AssertionError, match='retained checkpoint'):
        isolated.validate_restored_checkpoint({'id': 3, 'external_path': pointer}, 2, pointer)


def fixture_rows(count=20, ts_code="V05TEST.SZ"):
    return [
        {
            "eventId": f"V05:{ts_code}:{day}:original",
            "traceId": "v05-unit",
            "tsCode": ts_code,
            "tradeDate": (date(2026, 8, 1) + timedelta(days=day - 1)).isoformat(),
            "open": Decimal(day),
            "high": Decimal(day),
            "low": Decimal(day),
            "close": Decimal(day),
            "preClose": Decimal(day),
            "change": Decimal(0),
            "pctChg": Decimal("1.2345675"),
            "volume": Decimal(day * 10),
            "amount": Decimal(day * 100),
            "source": "V05_VERIFIER",
            "eventTime": "2026-08-01T15:00:00+08:00",
            "ingestTime": "2026-08-01T16:00:00+08:00",
            "schemaVersion": 1,
        }
        for day in range(1, count + 1)
    ]


def result_record(expected):
    return {
        "key": expected["tsCode"],
        "value": {**expected, "calculationTime": "2026-09-30T10:00:00+08:00"},
    }


def test_expected_indicators_use_decimal_half_up_and_full_window():
    rows = fixture_rows(20)
    result = MODULE.expected_indicators(rows)[-1]

    assert result["windowSize"] == 20
    assert result["isWarmup"] is False
    assert result["ma5"] == Decimal("18.000000")
    assert result["ma10"] == Decimal("15.500000")
    assert result["ma20"] == Decimal("10.500000")
    assert result["volMa5"] == Decimal("180.000000")
    assert result["volMa10"] == Decimal("155.000000")
    assert result["volumeRatio"] == Decimal("1.111111")
    assert result["pctChg"] == Decimal("1.234568")


def test_expected_indicators_do_not_expose_partial_averages():
    results = MODULE.expected_indicators(fixture_rows(10))

    assert results[3]["ma5"] is None
    assert results[4]["ma5"] == Decimal("3.000000")
    assert results[8]["ma10"] is None
    assert results[9]["ma10"] == Decimal("5.500000")
    assert results[9]["ma20"] is None
    assert all(item["isWarmup"] for item in results)


def test_same_day_overwrite_replaces_last_point_without_growing_window():
    rows = fixture_rows(20)
    overwrite = {**rows[-1], "eventId": "V05:overwrite", "close": Decimal(40), "volume": Decimal(400)}
    results = MODULE.expected_indicators([*rows, overwrite])

    assert results[-1]["windowSize"] == 20
    assert results[-1]["ma5"] == Decimal("22.000000")
    assert results[-1]["volMa5"] == Decimal("220.000000")
    assert results[-1]["sourceEventId"] == "V05:overwrite"


def test_validate_results_rejects_duplicate_event_id():
    event = result_record(MODULE.expected_indicators(fixture_rows(1))[0])

    with pytest.raises(AssertionError, match="duplicate eventId"):
        MODULE.validate_results([event, event])


def test_validate_results_checks_key_and_every_expected_field():
    expected = MODULE.expected_indicators(fixture_rows(1))[0]
    record = result_record(expected)
    MODULE.validate_results([record], [expected])

    with pytest.raises(AssertionError, match="Kafka key"):
        MODULE.validate_results([{**record, "key": "wrong"}], [expected])
    with pytest.raises(AssertionError, match="close"):
        MODULE.validate_results([{**record, "value": {**record["value"], "close": Decimal(999)}}], [expected])


def test_validate_results_rejects_missing_and_unexpected_source_events():
    expected = MODULE.expected_indicators(fixture_rows(2))

    with pytest.raises(AssertionError, match="count"):
        MODULE.validate_results([result_record(expected[0])], expected)
    with pytest.raises(AssertionError, match="sourceEventId"):
        MODULE.validate_results([result_record(expected[0]), result_record({**expected[1], "sourceEventId": "other"})], expected)


def test_validate_dead_letter_preserves_original_record_and_error_type():
    source = {"key": "V05TEST.SZ", "raw": b'{"eventId":', "topic": "stock.ods.daily.v1", "partition": 2, "offset": 7}
    record = {"key": source["key"], "value": {
        "originalTopic": source["topic"], "originalPartition": 2, "originalOffset": 7,
        "originalKey": source["key"], "originalPayload": source["raw"].decode(),
        "errorType": "JSON_PARSE", "errorMessage": "invalid StockDailyEvent JSON",
        "failedAt": "2026-09-30T10:00:00+08:00", "schemaVersion": 1,
    }}

    MODULE.validate_dead_letter(record, source, "JSON_PARSE")
    with pytest.raises(AssertionError, match="originalPayload"):
        MODULE.validate_dead_letter({**record, "value": {**record["value"], "originalPayload": "changed"}}, source, "JSON_PARSE")


def test_consumer_config_isolation_and_unique_group():
    first = MODULE.consumer_config("localhost:9092")
    second = MODULE.consumer_config("localhost:9092")

    assert first["isolation.level"] == "read_committed"
    assert first["auto.offset.reset"] == "earliest"
    assert first["group.id"].startswith("v05-verifier-")
    assert first["group.id"] != second["group.id"]


def test_build_rows_use_unique_source_ids_and_numeric_decimals():
    rows = MODULE.build_rows("V05TEST.SZ", "test-run", 2)

    assert [row["tradeDate"] for row in rows] == ["2026-08-01", "2026-08-02"]
    assert len({row["eventId"] for row in rows}) == 2
    assert all(row["tsCode"] == "V05TEST.SZ" for row in rows)
    assert rows[1]["volume"] == Decimal(20)


def test_find_running_job_rejects_missing_or_ambiguous_job():
    jobs = [
        {"jid": "old", "name": "stock-daily-indicator-v1", "state": "FAILED"},
        {"jid": "current", "name": "stock-daily-indicator-v1", "state": "RUNNING"},
    ]

    assert MODULE.find_running_job(jobs) == "current"
    with pytest.raises(AssertionError, match="exactly one"):
        MODULE.find_running_job(jobs[:1])
    with pytest.raises(AssertionError, match="exactly one"):
        MODULE.find_running_job([jobs[1], {**jobs[1], "jid": "second"}])


def test_wait_for_job_running_retries_during_taskmanager_restart(monkeypatch):
    attempts = iter([AssertionError("RESTARTING"), OSError("connection reset"), {"state": "RUNNING"}])

    def check(*_args):
        outcome = next(attempts)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(MODULE, "job_state", check)
    monkeypatch.setattr(MODULE.time, "sleep", lambda _seconds: None)

    assert MODULE.wait_for_job_running("http://flink", "job", 5) == {"state": "RUNNING"}


def test_recovery_after_uses_saved_job_id_during_restart(monkeypatch):
    manifest = {"jobId": "saved-job"}
    monkeypatch.setattr(MODULE, "resolve_job_id", lambda *_args: pytest.fail("discovery during restart"))

    assert MODULE.choose_job_id(None, "recovery-after", manifest, "http://flink", 5) == "saved-job"
    assert MODULE.choose_job_id("explicit", "recovery-after", manifest, "http://flink", 5) == "explicit"


def test_result_validation_rejects_numerically_equal_wrong_decimal_scale():
    expected = MODULE.expected_indicators(fixture_rows(20))[-1]
    decimal_fields = ("close", "volume", "pctChg", "ma5", "ma10", "ma20", "volMa5", "volMa10", "volumeRatio")

    for field in decimal_fields:
        record = result_record(expected)
        record["value"][field] = expected[field].quantize(Decimal("0.0000001"))
        assert record["value"][field] == expected[field]
        with pytest.raises(AssertionError, match=rf"{field}.*scale"):
            MODULE.validate_results([record], [expected])

    record = result_record(expected)
    record["value"]["close"] = int(expected["close"])
    with pytest.raises(AssertionError, match="close.*scale"):
        MODULE.validate_results([record], [expected])


def taskmanager_fixture(started_at="2026-09-30T10:00:00Z"):
    return {
        "containerId": "stable-container-id", "containerName": "/stock-flink-taskmanager",
        "composeProject": "infrastructure", "composeService": "flink-taskmanager",
        "startedAt": started_at, "running": True,
    }


def test_restart_evidence_requires_same_container_and_newer_started_at():
    before = taskmanager_fixture()
    MODULE.validate_restart_evidence(before, taskmanager_fixture("2026-09-30T10:01:00Z"))

    with pytest.raises(AssertionError, match="StartedAt"):
        MODULE.validate_restart_evidence(before, taskmanager_fixture())
    with pytest.raises(AssertionError, match="containerId"):
        MODULE.validate_restart_evidence(before, {**taskmanager_fixture("2026-09-30T10:01:00Z"), "containerId": "replacement"})
    with pytest.raises(AssertionError, match="composeService"):
        MODULE.validate_restart_evidence(before, {**taskmanager_fixture("2026-09-30T10:01:00Z"), "composeService": "kafka"})


def test_checkpoint_must_be_completed_and_triggered_after_observation():
    checkpoint = {"id": 22, "status": "COMPLETED", "trigger_timestamp": 1501}
    assert MODULE.checkpoint_is_after_barrier(checkpoint, after_id=20, barrier_ms=1500)
    assert not MODULE.checkpoint_is_after_barrier({**checkpoint, "trigger_timestamp": 1500}, 20, 1500)
    assert not MODULE.checkpoint_is_after_barrier({**checkpoint, "trigger_timestamp": 1499}, 20, 1500)
    assert not MODULE.checkpoint_is_after_barrier({**checkpoint, "id": 20}, 20, 1500)
    assert not MODULE.checkpoint_is_after_barrier({**checkpoint, "status": "IN_PROGRESS"}, 20, 1500)


def test_wait_for_checkpoint_ignores_new_id_triggered_before_barrier(monkeypatch):
    stale = {"id": 21, "status": "COMPLETED", "trigger_timestamp": 1499}
    fresh = {"id": 22, "status": "COMPLETED", "trigger_timestamp": 1501}
    checks = iter([stale, fresh])
    monkeypatch.setattr(MODULE, "completed_checkpoint", lambda *_args: next(checks))
    monkeypatch.setattr(MODULE, "job_state", lambda *_args: {"state": "RUNNING"})
    monkeypatch.setattr(MODULE.time, "sleep", lambda _seconds: None)

    assert MODULE.wait_for_checkpoint("http://flink", "job", 20, 1500, 5) == fresh


def test_wait_for_checkpoint_ignores_equal_trigger_timestamp(monkeypatch):
    equal = {"id": 21, "status": "COMPLETED", "trigger_timestamp": 1500}
    later = {"id": 22, "status": "COMPLETED", "trigger_timestamp": 1501}
    checks = iter([equal, later])
    monkeypatch.setattr(MODULE, "completed_checkpoint", lambda *_args: next(checks))
    monkeypatch.setattr(MODULE, "job_state", lambda *_args: {"state": "RUNNING"})
    monkeypatch.setattr(MODULE.time, "sleep", lambda _seconds: None)

    assert MODULE.wait_for_checkpoint("http://flink", "job", 20, 1500, 5) == later


def test_recovery_manifest_keeps_restart_and_checkpoint_barriers():
    row = fixture_rows(1)[0]
    actual = [result_record(MODULE.expected_indicators([row])[0])]
    checkpoint = {"id": 22, "status": "COMPLETED", "trigger_timestamp": 1500}

    manifest = MODULE.recovery_manifest("run", row["tsCode"], "job", taskmanager_fixture(), checkpoint, 1400, actual, [row])

    assert manifest["taskmanager"] == taskmanager_fixture()
    assert manifest["observationBarrierMs"] == 1400
    assert manifest["checkpointId"] == 22
    assert manifest["checkpointTriggerTimestamp"] == 1500


class FakeKafkaMessage:
    def __init__(self, record):
        self.record = record

    def error(self):
        return None

    def key(self):
        return self.record["key"].encode("utf-8")

    def value(self):
        return simplejson.dumps(self.record["value"], use_decimal=True).encode("utf-8")

    def topic(self):
        return "stock.dws.daily-indicator.v1"

    def partition(self):
        return 1

    def offset(self):
        return 7


def test_post_checkpoint_reconsume_detects_delayed_committed_duplicate(monkeypatch):
    expected = MODULE.expected_indicators(fixture_rows(1))[0]
    message = FakeKafkaMessage(result_record(expected))
    messages = iter([message, None, None, message])
    consumer = SimpleNamespace(poll=lambda _timeout: next(messages, None), close=lambda: None)
    opened = []

    def open_fresh(*args, **kwargs):
        opened.append(kwargs)
        return consumer

    monkeypatch.setattr(MODULE, "open_consumer", open_fresh)

    with pytest.raises(AssertionError, match="extra committed output"):
        MODULE.reconsume_committed("localhost:9092", "stock.dws.daily-indicator.v1", expected["tsCode"], [expected], 5, quiet=1)
    assert opened == [{"from_end": False}]


def test_open_consumer_seeks_each_partition_to_high_only_for_live_tail(monkeypatch):
    class FakeConsumer:
        def __init__(self, config):
            self.config = config
            self.seeks = []

        def subscribe(self, topics):
            self.topics = topics

        def assignment(self):
            return [SimpleNamespace(topic="out", partition=0), SimpleNamespace(topic="out", partition=1)]

        def get_watermark_offsets(self, partition, timeout):
            return 3 + partition.partition, 12 + partition.partition

        def seek(self, partition):
            self.seeks.append((partition.topic, partition.partition, partition.offset))

    monkeypatch.setitem(sys.modules, "confluent_kafka", SimpleNamespace(
        Consumer=FakeConsumer,
        TopicPartition=lambda topic, partition, offset: SimpleNamespace(topic=topic, partition=partition, offset=offset),
    ))

    tail = MODULE.open_consumer("localhost:9092", "out", 5, from_end=True)
    rewind = MODULE.open_consumer("localhost:9092", "out", 5, from_end=False)

    assert tail.config["isolation.level"] == "read_committed"
    assert tail.seeks == [("out", 0, 12), ("out", 1, 13)]
    assert rewind.seeks == [("out", 0, 3), ("out", 1, 4)]
    assert tail.config["group.id"] != rewind.config["group.id"]


def test_fresh_consumer_rewinds_assignment_time_polled_message(monkeypatch):
    first = SimpleNamespace(offset=lambda: 5)
    second = SimpleNamespace(offset=lambda: 6)

    class RaceConsumer:
        def __init__(self, _config):
            self.assigned = False
            self.position = 0
            self.seeks = []

        def subscribe(self, _topics):
            pass

        def assignment(self):
            return [SimpleNamespace(topic="out", partition=0)] if self.assigned else []

        def poll(self, _timeout):
            if not self.assigned:
                self.assigned = True
            message = (first, second)[self.position]
            self.position += 1
            return message

        def get_watermark_offsets(self, _partition, timeout):
            return 5, 7

        def seek(self, partition):
            self.seeks.append(partition.offset)
            self.position = partition.offset - 5

    monkeypatch.setitem(sys.modules, "confluent_kafka", SimpleNamespace(
        Consumer=RaceConsumer,
        TopicPartition=lambda topic, partition, offset: SimpleNamespace(topic=topic, partition=partition, offset=offset),
    ))

    consumer = MODULE.open_consumer("localhost:9092", "out", 5, from_end=False)

    assert consumer.seeks == [5]
    assert consumer.poll(0).offset() == 5


def test_recovery_before_records_post_observation_checkpoint_and_container(monkeypatch):
    row = fixture_rows(1)[0]
    actual = [result_record(MODULE.expected_indicators([row])[0])]
    saved = {}
    events = []
    file = SimpleNamespace(
        exists=lambda: False,
        parent=SimpleNamespace(mkdir=lambda **_kwargs: None),
        write_text=lambda contents, **_kwargs: saved.update(json.loads(contents)),
    )
    monkeypatch.setattr(MODULE, "RECOVERY_FILE", file)
    monkeypatch.setattr(MODULE, "new_run", lambda: ("run", row["tsCode"], "other"))
    monkeypatch.setattr(MODULE, "build_rows", lambda *_args: [row])
    monkeypatch.setattr(MODULE, "open_consumer", lambda *_args, **_kwargs: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(MODULE, "make_producer", lambda *_args: object())
    monkeypatch.setattr(MODULE, "send_rows", lambda *_args: None)
    monkeypatch.setattr(MODULE, "collect", lambda *_args, **_kwargs: events.append("observed") or actual)
    monkeypatch.setattr(MODULE, "inspect_taskmanager", lambda *_args: taskmanager_fixture())
    monkeypatch.setattr(MODULE, "completed_checkpoint", lambda *_args: {"id": 20})
    monkeypatch.setattr(MODULE, "observation_epoch_ms", lambda: 1400, raising=False)
    monkeypatch.setattr(MODULE, "wait_for_checkpoint", lambda _url, _job, after_id, barrier, _timeout: (
        events.append(("checkpoint", after_id, barrier)) or
        {"id": 22, "status": "COMPLETED", "trigger_timestamp": 1500}
    ))
    monkeypatch.setattr(MODULE, "reconsume_committed", lambda *_args, **_kwargs: events.append("reconsumed") or actual, raising=False)
    args = SimpleNamespace(timeout=5, flink_url="http://flink", job_id="job",
                           bootstrap_servers="localhost:9092", output_topic="out")

    MODULE.scenario_recovery_before(args)

    assert events == ["observed", ("checkpoint", 20, 1400), "reconsumed"]
    assert saved["taskmanager"] == taskmanager_fixture()
    assert saved["checkpointTriggerTimestamp"] == 1500
    assert saved["observationBarrierMs"] == 1400


def test_recovery_after_rejects_no_restart_before_publishing(monkeypatch):
    before = taskmanager_fixture()
    manifest = {
        "runId": "run", "tsCode": "V05TEST.SZ", "jobId": "job", "taskmanager": before,
        "eventIds": [f"id-{n}" for n in range(10)], "checkpointId": 20,
        "observationBarrierMs": 1400, "checkpointTriggerTimestamp": 1500,
    }
    monkeypatch.setattr(MODULE, "RECOVERY_FILE", SimpleNamespace(read_text=lambda **_kwargs: json.dumps(manifest)))
    monkeypatch.setattr(MODULE, "inspect_taskmanager", lambda *_args: taskmanager_fixture())
    monkeypatch.setattr(MODULE, "wait_for_job_running", lambda *_args: None)
    monkeypatch.setattr(MODULE, "open_consumer", lambda *_args, **_kwargs: pytest.fail("must not read/publish before restart proof"))
    args = SimpleNamespace(timeout=5, flink_url="http://flink", job_id="job",
                           bootstrap_servers="localhost:9092", output_topic="out")

    with pytest.raises(AssertionError, match="StartedAt"):
        MODULE.scenario_recovery_after(args)
