#!/usr/bin/env python3
"""Verify committed V0.5 Flink indicator, side-output, and recovery behavior."""

import argparse
import json
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from urllib.request import Request, urlopen
from uuid import uuid4


SCALE = Decimal("0.000001")
OUTPUT_FIELDS = {
    "eventId", "sourceEventId", "traceId", "tsCode", "tradeDate", "source",
    "sourceEventTime", "close", "volume", "pctChg", "ma5", "ma10", "ma20",
    "volMa5", "volMa10", "volumeRatio", "windowSize", "isWarmup",
    "calculationTime", "schemaVersion",
}
DECIMAL_FIELDS = ("close", "volume", "pctChg", "ma5", "ma10", "ma20", "volMa5", "volMa10", "volumeRatio")
RECOVERY_FILE = Path(__file__).resolve().parents[1] / ".tmp" / "v05-recovery.json"


def decimal(value):
    return Decimal(str(value))


def scaled(value):
    return None if value is None else decimal(value).quantize(SCALE, rounding=ROUND_HALF_UP)


def trailing_average(points, field, count):
    if len(points) < count:
        return None
    return scaled(sum((decimal(point[field]) for point in points[-count:]), Decimal(0)) / count)


def expected_indicators(rows):
    """Independent decimal oracle for the keyed, 20-date Flink state machine."""
    windows = {}
    results = []
    for row in rows:
        code, trade_date = row["tsCode"], row["tradeDate"]
        points = windows.setdefault(code, [])
        if points and trade_date < points[-1]["tradeDate"]:
            raise ValueError("expected_indicators accepts only current or newer dates")
        if points and trade_date == points[-1]["tradeDate"]:
            points[-1] = row
        else:
            points.append(row)
            del points[:-20]
        vol_ma5 = trailing_average(points, "volume", 5)
        ratio = None if vol_ma5 is None or vol_ma5 == 0 else scaled(decimal(row["volume"]) / vol_ma5)
        results.append({
            "eventId": f"FLINK_INDICATOR:{code}:{trade_date}:v1",
            "sourceEventId": row["eventId"],
            "traceId": row["traceId"],
            "tsCode": code,
            "tradeDate": trade_date,
            "source": row["source"],
            "sourceEventTime": row["eventTime"],
            "close": scaled(row["close"]),
            "volume": scaled(row["volume"]),
            "pctChg": scaled(row.get("pctChg")),
            "ma5": trailing_average(points, "close", 5),
            "ma10": trailing_average(points, "close", 10),
            "ma20": trailing_average(points, "close", 20),
            "volMa5": vol_ma5,
            "volMa10": trailing_average(points, "volume", 10),
            "volumeRatio": ratio,
            "windowSize": len(points),
            "isWarmup": len(points) < 20,
            "schemaVersion": 1,
        })
    return results


def validate_results(records, expected=None, allowed_duplicate_ids=frozenset()):
    """Assert unique committed IDs and exact event/key/JSON contract."""
    seen = set()
    by_source = {}
    for record in records:
        value = record["value"]
        event_id = value["eventId"]
        if event_id in seen and event_id not in allowed_duplicate_ids:
            raise AssertionError(f"duplicate eventId: {event_id}")
        seen.add(event_id)
        assert record["key"] == value["tsCode"], f"Kafka key mismatch for {event_id}"
        assert set(value) == OUTPUT_FIELDS, f"JSON fields mismatch for {event_id}: {set(value) ^ OUTPUT_FIELDS}"
        for field in DECIMAL_FIELDS:
            number = value[field]
            if number is not None:
                assert isinstance(number, Decimal) and number.as_tuple().exponent == -6, (
                    f"{field} decimal scale must be 6 for {event_id}; got {number!r}"
                )
        assert value["schemaVersion"] == 1, f"schemaVersion mismatch for {event_id}"
        timestamp = datetime.fromisoformat(value["calculationTime"])
        assert timestamp.utcoffset() is not None, f"calculationTime lacks timezone for {event_id}"
        source_id = value["sourceEventId"]
        assert source_id not in by_source, f"duplicate sourceEventId: {source_id}"
        by_source[source_id] = record
    if expected is None:
        return
    assert len(records) == len(expected), f"result count: expected {len(expected)}, got {len(records)}"
    assert len(by_source) == len(expected), "sourceEventId count mismatch"
    for item in expected:
        source_id = item["sourceEventId"]
        assert source_id in by_source, f"missing sourceEventId: {source_id}"
        value = by_source[source_id]["value"]
        for field, wanted in item.items():
            assert value[field] == wanted, f"{field} mismatch for {source_id}: expected {wanted!r}, got {value[field]!r}"


def validate_dead_letter(record, source, error_type):
    value = record["value"]
    fields = {
        "originalTopic": source["topic"],
        "originalPartition": source["partition"],
        "originalOffset": source["offset"],
        "originalKey": source["key"],
        "originalPayload": source["raw"].decode("utf-8"),
        "errorType": error_type,
        "schemaVersion": 1,
    }
    assert record["key"] == source["key"], "DLT Kafka key mismatch"
    for field, wanted in fields.items():
        assert value[field] == wanted, f"DLT {field} mismatch: expected {wanted!r}, got {value[field]!r}"
    assert value["errorMessage"], "DLT errorMessage missing"
    assert datetime.fromisoformat(value["failedAt"]).utcoffset() is not None, "DLT failedAt lacks timezone"


def validate_inflight_visibility(uncommitted, committed, expected):
    """Fail closed unless the entire fault batch exists only in an open transaction."""
    if committed:
        raise AssertionError("fault batch already committed; in-flight fault proof invalid")
    validate_results(uncommitted, expected)


def consumer_config(bootstrap_servers, isolation="read_committed"):
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"v05-verifier-{uuid4().hex}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": isolation,
    }


def build_rows(ts_code, run_id, count=20, start=1, price_offset=0):
    rows = []
    for day in range(start, start + count):
        trade_date = date(2026, 8, 1) + timedelta(days=day - 1)
        close = Decimal(day + price_offset)
        rows.append({
            "eventId": f"V05:{run_id}:{ts_code}:{day}",
            "traceId": f"v05-{run_id}",
            "tsCode": ts_code,
            "tradeDate": trade_date.isoformat(),
            "open": close, "high": close, "low": close, "close": close,
            "preClose": close, "change": Decimal(0),
            "pctChg": Decimal("1.2345675"), "volume": close * 10,
            "amount": close * 100, "source": "V05_VERIFIER",
            "eventTime": f"{trade_date.isoformat()}T15:00:00+08:00",
            "ingestTime": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
            "schemaVersion": 1,
        })
    return rows


def new_run():
    token = uuid4().hex[:12].upper()
    return token, f"V05{token}A.SZ", f"V05{token}B.SZ"


def get_json(url, timeout):
    with urlopen(Request(url, headers={"Accept": "application/json"}), timeout=min(timeout, 5)) as response:
        return json.load(response)


def find_running_job(jobs):
    matches = [job["jid"] for job in jobs if job["name"] == "stock-daily-indicator-v1" and job["state"] == "RUNNING"]
    assert len(matches) == 1, f"expected exactly one RUNNING stock-daily-indicator-v1 job; got {len(matches)}"
    return matches[0]


def resolve_job_id(base_url, timeout):
    return find_running_job(get_json(f"{base_url}/jobs/overview", timeout)["jobs"])


def choose_job_id(explicit, scenario, manifest, base_url, timeout):
    if explicit:
        return explicit
    if scenario == "recovery-after":
        return manifest["jobId"]
    return resolve_job_id(base_url, timeout)


def inspect_taskmanager(timeout):
    try:
        process = subprocess.run(
            ["docker", "inspect", "--type", "container", "stock-flink-taskmanager"],
            capture_output=True, text=True, check=True, timeout=min(timeout, 10),
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        raise RuntimeError(f"could not inspect stock-flink-taskmanager: {error}") from error
    containers = json.loads(process.stdout)
    assert len(containers) == 1, f"expected one stock-flink-taskmanager container, got {len(containers)}"
    container = containers[0]
    labels = container["Config"]["Labels"]
    return {
        "containerId": container["Id"],
        "containerName": container["Name"],
        "composeProject": labels["com.docker.compose.project"],
        "composeService": labels["com.docker.compose.service"],
        "startedAt": container["State"]["StartedAt"],
        "running": container["State"]["Running"],
        "restartCount": container["RestartCount"],
    }


def validate_restart_evidence(before, after):
    assert before["composeService"] == "flink-taskmanager", "before composeService is not flink-taskmanager"
    for field in ("containerId", "containerName", "composeProject", "composeService"):
        assert after[field] == before[field], f"TaskManager {field} changed across restart"
    assert after["running"], "TaskManager is not running after restart"
    prior = datetime.fromisoformat(before["startedAt"].replace("Z", "+00:00"))
    current = datetime.fromisoformat(after["startedAt"].replace("Z", "+00:00"))
    assert prior.utcoffset() is not None and current.utcoffset() is not None, "TaskManager StartedAt lacks timezone"
    assert current > prior, f"TaskManager StartedAt did not advance: {before['startedAt']} -> {after['startedAt']}"


def job_state(base_url, job_id, timeout):
    job = get_json(f"{base_url}/jobs/{job_id}", timeout)
    assert job["state"] == "RUNNING", f"Flink job is {job['state']}"
    managers = get_json(f"{base_url}/taskmanagers", timeout)["taskmanagers"]
    assert len(managers) == 1 and managers[0]["slotsNumber"] >= 3, "TaskManager/slots not healthy"
    return job


def wait_for_job_running(base_url, job_id, timeout):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            return job_state(base_url, job_id, timeout)
        except (AssertionError, OSError, RuntimeError) as error:
            last_error = error
            time.sleep(1)
    raise TimeoutError(f"Flink job/TaskManager did not recover within {timeout}s: {last_error}")


def completed_checkpoint(base_url, job_id, timeout):
    data = get_json(f"{base_url}/jobs/{job_id}/checkpoints", timeout)
    return data.get("latest", {}).get("completed")


def checkpoint_is_after_barrier(checkpoint, after_id, barrier_ms):
    return (
        checkpoint is not None
        and checkpoint.get("status") == "COMPLETED"
        and (after_id is None or checkpoint["id"] > after_id)
        and checkpoint["trigger_timestamp"] > barrier_ms
    )


def wait_for_checkpoint(base_url, job_id, after_id, barrier_ms, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job_state(base_url, job_id, timeout)
        checkpoint = completed_checkpoint(base_url, job_id, timeout)
        if checkpoint_is_after_barrier(checkpoint, after_id, barrier_ms):
            return checkpoint
        time.sleep(1)
    raise TimeoutError(f"no completed checkpoint after ID {after_id} and observation {barrier_ms}ms within {timeout}s")


def open_consumer(bootstrap, topic, timeout, from_end=True, isolation="read_committed"):
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(consumer_config(bootstrap, isolation))
    consumer.subscribe([topic])
    deadline = time.monotonic() + min(timeout, 15)
    while not consumer.assignment() and time.monotonic() < deadline:
        consumer.poll(0.2)
    partitions = consumer.assignment()
    if not partitions:
        consumer.close()
        raise TimeoutError(f"no consumer assignment for {topic}")
    for partition in partitions:
        low, high = consumer.get_watermark_offsets(partition, timeout=min(timeout, 5))
        # Assignment polling may already have returned a record; rewind fresh readers explicitly.
        consumer.seek(TopicPartition(topic, partition.partition, high if from_end else low))
    return consumer


def read_record(message):
    if message.error():
        raise RuntimeError(f"Kafka consume error: {message.error()}")
    return {
        "key": None if message.key() is None else message.key().decode("utf-8"),
        "value": json.loads(message.value(), parse_float=Decimal),
        "topic": message.topic(), "partition": message.partition(), "offset": message.offset(),
    }


def collect(consumer, keys, count, timeout, quiet=12):
    records = []
    deadline = time.monotonic() + timeout
    quiet_deadline = None
    while time.monotonic() < deadline:
        message = consumer.poll(0.5)
        if message is not None:
            record = read_record(message)
            if record["key"] in keys:
                records.append(record)
                if len(records) > count:
                    raise AssertionError(f"unexpected extra committed output for {keys}")
        if len(records) == count:
            if quiet_deadline is None:
                quiet_deadline = time.monotonic() + quiet
            if time.monotonic() >= quiet_deadline:
                return records
    raise TimeoutError(f"expected {count} committed records for {keys}; received {len(records)}")


def publish(producer, topic, messages, timeout):
    import simplejson

    delivered = []
    errors = []

    def callback(error, message, source):
        if error is not None:
            errors.append(str(error))
        else:
            delivered.append({**source, "topic": message.topic(), "partition": message.partition(), "offset": message.offset()})

    for source in messages:
        raw = source.get("raw")
        if raw is None:
            raw = simplejson.dumps(source["payload"], use_decimal=True, separators=(",", ":")).encode("utf-8")
        source = {**source, "raw": raw}
        producer.produce(topic, key=source["key"].encode("utf-8"), value=raw,
                         on_delivery=lambda error, message, item=source: callback(error, message, item))
        producer.poll(0)
    remaining = producer.flush(timeout)
    if remaining or errors or len(delivered) != len(messages):
        raise RuntimeError(f"Kafka produce failed: remaining={remaining}, errors={errors}, delivered={len(delivered)}")
    return delivered


def make_producer(bootstrap):
    from confluent_kafka import Producer
    return Producer({"bootstrap.servers": bootstrap, "client.id": f"v05-verifier-{uuid4().hex}"})


def send_rows(producer, args, rows):
    return publish(producer, args.input_topic, [{"key": row["tsCode"], "payload": row} for row in rows], args.timeout)


def reconsume_committed(bootstrap, topic, code, expected, timeout, quiet=25):
    """Rewind a new read_committed group after a barrier checkpoint and catch late replays."""
    consumer = open_consumer(bootstrap, topic, timeout, from_end=False)
    try:
        actual = collect(consumer, {code}, len(expected), timeout, quiet=quiet)
        validate_results(actual, expected)
        return actual
    finally:
        consumer.close()


def observation_epoch_ms():
    return time.time_ns() // 1_000_000


def recovery_manifest(run_id, code, job_id, taskmanager, checkpoint, barrier_ms, actual, rows):
    return {
        "runId": run_id, "tsCode": code, "jobId": job_id,
        "taskmanager": taskmanager,
        "observationBarrierMs": barrier_ms,
        "checkpointId": checkpoint["id"],
        "checkpointTriggerTimestamp": checkpoint["trigger_timestamp"],
        "eventIds": [item["value"]["eventId"] for item in actual],
        "sourceEventIds": [row["eventId"] for row in rows],
        "writtenAt": datetime.now(timezone.utc).isoformat(),
    }


def scenario_valid(args):
    run_id, code_a, code_b = new_run()
    keys = {code_a, code_b}
    main = open_consumer(args.bootstrap_servers, args.output_topic, args.timeout)
    late = open_consumer(args.bootstrap_servers, args.late_topic, args.timeout)
    dlt = open_consumer(args.bootstrap_servers, args.dlt_topic, args.timeout)
    try:
        producer = make_producer(args.bootstrap_servers)
        rows_a = build_rows(code_a, run_id)
        rows_b = build_rows(code_b, run_id, price_offset=100)
        rows = [row for pair in zip(rows_a, rows_b) for row in pair]
        send_rows(producer, args, rows)
        initial = collect(main, keys, 40, args.timeout)
        validate_results(initial, expected_indicators(rows))
        overwrite = {**rows_a[-1], "eventId": f"V05:{run_id}:{code_a}:overwrite",
                     "close": Decimal(40), "volume": Decimal(400)}
        send_rows(producer, args, [overwrite])
        replacement = collect(main, keys, 1, args.timeout)
        expected = expected_indicators([*rows, overwrite])[-1]
        validate_results(replacement, [expected])
        assert replacement[0]["value"]["eventId"] == next(
            item["value"]["eventId"] for item in initial if item["value"]["sourceEventId"] == rows_a[-1]["eventId"]
        ), "same-day overwrite changed business eventId"
        collect(late, keys, 0, args.timeout, quiet=12)
        collect(dlt, keys, 0, args.timeout, quiet=12)
        return {"runId": run_id, "stocks": sorted(keys), "initial": len(initial), "overwrites": 1}
    finally:
        main.close()
        late.close()
        dlt.close()


def scenario_late(args):
    run_id, code, _ = new_run()
    main = open_consumer(args.bootstrap_servers, args.output_topic, args.timeout)
    late = open_consumer(args.bootstrap_servers, args.late_topic, args.timeout)
    dlt = open_consumer(args.bootstrap_servers, args.dlt_topic, args.timeout)
    try:
        producer = make_producer(args.bootstrap_servers)
        rows = build_rows(code, run_id)
        send_rows(producer, args, rows)
        validate_results(collect(main, {code}, 20, args.timeout), expected_indicators(rows))
        older = {**rows[-2], "eventId": f"V05:{run_id}:{code}:late", "close": Decimal(999)}
        send_rows(producer, args, [older])
        late_records = collect(late, {code}, 1, args.timeout)
        value = late_records[0]["value"]
        assert value["originalEvent"] == older, "late originalEvent mismatch"
        assert value["latestTradeDate"] == rows[-1]["tradeDate"], "late latestTradeDate mismatch"
        assert value["reason"] == "TRADE_DATE_BEFORE_LATEST", "late reason mismatch"
        assert value["schemaVersion"] == 1, "late schemaVersion mismatch"
        collect(main, {code}, 0, args.timeout, quiet=12)
        collect(dlt, {code}, 0, args.timeout, quiet=12)
        probe = {**rows[-1], "eventId": f"V05:{run_id}:{code}:probe", "close": Decimal(40)}
        send_rows(producer, args, [probe])
        validate_results(collect(main, {code}, 1, args.timeout), expected_indicators([*rows, probe])[-1:])
        return {"runId": run_id, "stock": code, "late": 1, "stateProbe": "PASS"}
    finally:
        main.close()
        late.close()
        dlt.close()


def scenario_invalid(args):
    run_id, code, other = new_run()
    main = open_consumer(args.bootstrap_servers, args.output_topic, args.timeout)
    dlt = open_consumer(args.bootstrap_servers, args.dlt_topic, args.timeout)
    late = open_consumer(args.bootstrap_servers, args.late_topic, args.timeout)
    try:
        producer = make_producer(args.bootstrap_servers)
        mismatch = build_rows(other, run_id, 1)[0]
        sources = publish(producer, args.input_topic, [
            {"key": code, "raw": b'{"eventId":'},
            {"key": code, "payload": mismatch},
        ], args.timeout)
        dead = collect(dlt, {code}, 2, args.timeout)
        by_offset = {(record["value"]["originalPartition"], record["value"]["originalOffset"]): record for record in dead}
        for source, error in zip(sources, ("JSON_PARSE", "KEY_MISMATCH")):
            coordinate = (source["partition"], source["offset"])
            assert coordinate in by_offset, f"missing DLT for {coordinate}"
            validate_dead_letter(by_offset[coordinate], source, error)
        collect(main, {code, other}, 0, args.timeout, quiet=12)
        collect(late, {code, other}, 0, args.timeout, quiet=12)
        return {"runId": run_id, "deadLetters": 2, "mainOutputs": 0}
    finally:
        main.close()
        dlt.close()
        late.close()


def scenario_recovery_before(args):
    recovery_file = getattr(args, "recovery_file", RECOVERY_FILE)
    assert not recovery_file.exists(), f"recovery manifest already exists: {recovery_file}; preserve/resolve it before a new run"
    run_id, code, _ = new_run()
    main = open_consumer(args.bootstrap_servers, args.output_topic, args.timeout)
    try:
        taskmanager = inspect_taskmanager(args.timeout)
        assert taskmanager["running"] and taskmanager["composeService"] == "flink-taskmanager", "TaskManager not running"
        baseline = completed_checkpoint(args.flink_url, args.job_id, args.timeout)
        producer = make_producer(args.bootstrap_servers)
        rows = build_rows(code, run_id, 10)
        send_rows(producer, args, rows)
        expected = expected_indicators(rows)
        validate_results(collect(main, {code}, 10, args.timeout, quiet=0), expected)
        barrier_ms = observation_epoch_ms()
        checkpoint = wait_for_checkpoint(
            args.flink_url, args.job_id, None if baseline is None else baseline["id"], barrier_ms, args.timeout)
        actual = reconsume_committed(args.bootstrap_servers, args.output_topic, code, expected, args.timeout)
        manifest = recovery_manifest(run_id, code, args.job_id, taskmanager, checkpoint, barrier_ms, actual, rows)
        recovery_file.parent.mkdir(parents=True, exist_ok=True)
        recovery_file.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        return {"manifest": str(recovery_file), "checkpointId": checkpoint["id"],
                "checkpointTriggerTimestamp": checkpoint["trigger_timestamp"], "stock": code, "committed": 10}
    finally:
        main.close()


def scenario_recovery_after(args):
    recovery_file = getattr(args, "recovery_file", RECOVERY_FILE)
    manifest = json.loads(recovery_file.read_text(encoding="utf-8"))
    run_id, code = manifest["runId"], manifest["tsCode"]
    assert args.job_id == manifest["jobId"], "recovery Job ID does not match manifest"
    assert len(manifest["eventIds"]) == 10 and len(set(manifest["eventIds"])) == 10, "invalid recovery manifest IDs"
    assert manifest["checkpointTriggerTimestamp"] >= manifest["observationBarrierMs"], "pre-restart checkpoint predates observation"
    wait_for_job_running(args.flink_url, args.job_id, args.timeout)
    taskmanager = inspect_taskmanager(args.timeout)
    validate_restart_evidence(manifest["taskmanager"], taskmanager)
    main = open_consumer(args.bootstrap_servers, args.output_topic, args.timeout, from_end=False)
    try:
        producer = make_producer(args.bootstrap_servers)
        send_rows(producer, args, build_rows(code, run_id, 10, start=11))
        expected = expected_indicators(build_rows(code, run_id, 20))
        validate_results(collect(main, {code}, 20, args.timeout, quiet=0), expected)
        barrier_ms = observation_epoch_ms()
        checkpoint = wait_for_checkpoint(args.flink_url, args.job_id, manifest["checkpointId"], barrier_ms, args.timeout)
        actual = reconsume_committed(args.bootstrap_servers, args.output_topic, code, expected, args.timeout)
        before_ids = [item["value"]["eventId"] for item in actual if item["value"]["tradeDate"] <= "2026-08-10"]
        assert set(before_ids) == set(manifest["eventIds"]), "pre-restart committed event IDs changed"
        assert len({item["value"]["tradeDate"] for item in actual}) == 20, "missing or duplicated recovery date"
        return {"runId": run_id, "stock": code, "committed": 20,
                "taskmanagerStartedAtBefore": manifest["taskmanager"]["startedAt"],
                "taskmanagerStartedAtAfter": taskmanager["startedAt"],
                "beforeCheckpointId": manifest["checkpointId"], "afterCheckpointId": checkpoint["id"],
                "afterCheckpointTriggerTimestamp": checkpoint["trigger_timestamp"]}
    finally:
        main.close()


SCENARIOS = {
    "valid": scenario_valid, "late": scenario_late, "invalid": scenario_invalid,
    "recovery-before": scenario_recovery_before, "recovery-after": scenario_recovery_after,
}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=SCENARIOS, required=True)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--input-topic", default="stock.ods.daily.v1")
    parser.add_argument("--output-topic", default="stock.dws.daily-indicator.v1")
    parser.add_argument("--late-topic", default="stock.late.daily.v1")
    parser.add_argument("--dlt-topic", default="stock.flink.dead-letter.v1")
    parser.add_argument("--flink-url", default="http://localhost:8082")
    parser.add_argument("--job-id", help="Flink Job ID; discovered by name when omitted")
    parser.add_argument("--recovery-file", type=Path, default=RECOVERY_FILE,
                        help="Recovery manifest path; use a new path for each recovery run")
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args(argv)
    args.flink_url = args.flink_url.rstrip("/")
    started = time.monotonic()
    try:
        if not __debug__:
            raise RuntimeError("optimized Python execution is unsupported: validation assertions must remain enabled")
        manifest = json.loads(args.recovery_file.read_text(encoding="utf-8")) if args.scenario == "recovery-after" else None
        args.job_id = choose_job_id(args.job_id, args.scenario, manifest, args.flink_url, args.timeout)
        if args.scenario == "recovery-after":
            wait_for_job_running(args.flink_url, args.job_id, args.timeout)
        else:
            job_state(args.flink_url, args.job_id, args.timeout)
        detail = SCENARIOS[args.scenario](args)
        job_state(args.flink_url, args.job_id, args.timeout)
        result = {"status": "PASS", "scenario": args.scenario, **detail}
    except (AssertionError, FileNotFoundError, ImportError, OSError, RuntimeError, TimeoutError, ValueError, KeyError) as error:
        result = {"status": "FAIL", "scenario": args.scenario, "error": str(error)}
    result["elapsedSeconds"] = round(time.monotonic() - started, 3)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
