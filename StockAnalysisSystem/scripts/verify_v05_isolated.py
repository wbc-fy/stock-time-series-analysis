#!/usr/bin/env python3
"""Opt-in isolated live tests. Requires an empty Session Cluster; preserves all data.

Recovery freezes the JobManager coordinator before publishing the fault batch,
proves Kafka read_uncommitted visibility/read_committed invisibility, kills that
still-frozen coordinator, and explicitly resubmits from a retained checkpoint.
No timing window is used as a substitute for the visibility proof.
"""
import argparse
import importlib.util
import json
import os
import re
import subprocess
import sys
import time
from datetime import date
from decimal import Decimal
from pathlib import Path
from uuid import uuid4
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('v05', ROOT / 'scripts/verify_v05_flink.py')
v05 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v05)
JAR = '/opt/flink/usrlib/flink-realtime-job-0.3.0-SNAPSHOT-all.jar'
URL = 'http://localhost:8082'


def require_empty_cluster(jobs):
    if jobs:
        raise RuntimeError('isolated fault verification refuses existing jobs; use an empty cluster')


def validate_restored_checkpoint(restored, checkpoint_id, pointer):
    if not restored or restored.get('id') != checkpoint_id:
        raise AssertionError('JobManager resubmit did not restore the retained checkpoint')
    path = restored.get('external_path')
    # URI splitting treats file:/ and file:/// alike without conflating different
    # paths, hosts, queries or fragments belonging to different checkpoints.
    if not path or urlsplit(path) != urlsplit(pointer):
        raise AssertionError('JobManager restored checkpoint path does not match retained pointer')


def validate_partition_lag(partitions):
    if {p['partition'] for p in partitions} != {0, 1, 2}:
        raise AssertionError('lag audit must cover all three input partitions')
    for partition in partitions:
        if partition['high'] and partition['committed'] != partition['high']:
            raise AssertionError(f'input partition lag: {partition}')


def audit_input_lag(topic, group):
    from confluent_kafka import Consumer, TopicPartition
    consumer = Consumer({**v05.consumer_config('localhost:9092'), 'group.id': group})
    try:
        partitions = [TopicPartition(topic, n) for n in range(3)]
        committed = consumer.committed(partitions, timeout=10)
        result = []
        for partition in committed:
            low, high = consumer.get_watermark_offsets(partition, timeout=10)
            result.append({'partition': partition.partition, 'low': low, 'high': high,
                           'committed': partition.offset, 'lag': 0 if high == 0 else high - partition.offset})
        validate_partition_lag(result)
        return result
    finally:
        consumer.close()


def audit_side_topics(topics, code):
    for field in ('late', 'dlt'):
        consumer = v05.open_consumer('localhost:9092', topics[field], 15, from_end=False)
        try:
            v05.collect(consumer, {code}, 0, 20, quiet=3)
        finally:
            consumer.close()
    return {'late': 0, 'dlt': 0}


def command(*args):
    result = subprocess.run(args, capture_output=True, text=True, timeout=120)
    if result.returncode:
        # Commands contain no database configuration or secret arguments.
        raise RuntimeError(f'command failed: {args}: {result.stderr[-1500:]}')
    return result.stdout


def wait_cluster():
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            managers = v05.get_json(f'{URL}/taskmanagers', 5)['taskmanagers']
            if len(managers) == 1:
                return
        except OSError:
            pass
        time.sleep(1)
    raise TimeoutError('isolated cluster did not become ready')


def submit(options, checkpoint=None):
    args = ['docker', 'exec', 'stock-flink-jobmanager', 'flink', 'run', '-d']
    if checkpoint:
        args += ['-s', checkpoint]
    output = command(*args, '-c', 'com.stock.flink.DailyIndicatorJob', JAR, *options)
    match = re.search(r'JobID ([0-9a-f]{32})', output)
    if not match:
        raise RuntimeError('submission returned no Job ID')
    job_id = match.group(1)
    v05.wait_for_job_running(URL, job_id, 120)
    return job_id


def isolated_config():
    token = 'v05-' + uuid4().hex[:12]
    topics = {name: f'{token}.{name}' for name in ('input', 'output', 'late', 'dlt')}
    for topic in topics.values():
        command('docker', 'exec', 'stock-kafka', '/opt/kafka/bin/kafka-topics.sh',
                '--bootstrap-server', 'kafka:29092', '--create', '--topic', topic,
                '--partitions', '3', '--replication-factor', '1')
    options = ['--deployment-namespace', token, '--group-id', token,
               '--checkpoint-uri', f'file:///opt/flink/checkpoints/{token}', '--parallelism', '1']
    for field, topic in topics.items():
        options += ['--' + ('dead-letter' if field == 'dlt' else field) + '-topic', topic]
    return token, topics, options


def recovery(topics, options):
    job_id = submit(options)
    run_id, code, _ = v05.new_run()
    rows = v05.build_rows(code, run_id, 20)
    args = argparse.Namespace(bootstrap_servers='localhost:9092', input_topic=topics['input'], timeout=180)
    producer = v05.make_producer(args.bootstrap_servers)
    v05.send_rows(producer, args, rows[:10])
    baseline = v05.reconsume_committed(args.bootstrap_servers, topics['output'], code,
                                     v05.expected_indicators(rows[:10]), 180, quiet=0)
    barrier = v05.observation_epoch_ms()
    checkpoint = v05.wait_for_checkpoint(URL, job_id, None, barrier, 180)
    pointer = checkpoint.get('external_path')
    if not pointer:
        raise RuntimeError('completed checkpoint missing retained external_path')
    before = json.loads(command('docker', 'inspect', 'stock-flink-jobmanager'))[0]['State']['StartedAt']
    ru = v05.open_consumer('localhost:9092', topics['output'], 15, isolation='read_uncommitted')
    rc = v05.open_consumer('localhost:9092', topics['output'], 15)
    frozen = False
    try:
        # SIGSTOP prevents coordinator-triggered barriers and transaction commit callbacks.
        command('docker', 'kill', '--signal=STOP', 'stock-flink-jobmanager')
        frozen = True
        v05.send_rows(producer, args, rows[10:15])
        uncommitted = v05.collect(ru, {code}, 5, 20, quiet=0)
        committed = v05.collect(rc, {code}, 0, 5, quiet=2)
        v05.validate_inflight_visibility(uncommitted, committed, v05.expected_indicators(rows[:15])[10:])
        # Kill while STILL frozen: no covering checkpoint can commit the batch.
        command('docker', 'kill', '--signal=KILL', 'stock-flink-jobmanager')
        frozen = False
    finally:
        ru.close()
        rc.close()
        if frozen:
            command('docker', 'kill', '--signal=CONT', 'stock-flink-jobmanager')
    command('docker', 'start', 'stock-flink-jobmanager')
    wait_cluster()
    restored = submit(options, pointer)
    restored_stats = v05.get_json(f'{URL}/jobs/{restored}/checkpoints', 5)['latest'].get('restored')
    validate_restored_checkpoint(restored_stats, checkpoint['id'], pointer)
    v05.send_rows(producer, args, rows[15:])
    expected = v05.expected_indicators(rows)
    actual = v05.reconsume_committed('localhost:9092', topics['output'], code, expected, 180, quiet=25)
    final = v05.wait_for_checkpoint(URL, restored, None, v05.observation_epoch_ms(), 180)
    audit_side_topics(topics, code)
    after = json.loads(command('docker', 'inspect', 'stock-flink-jobmanager'))[0]['State']['StartedAt']
    if after <= before:
        raise AssertionError('JobManager container StartedAt did not advance')
    command('docker', 'exec', 'stock-flink-jobmanager', 'flink', 'cancel', restored)
    return {'jobBefore': job_id, 'jobAfter': restored, 'checkpoint': checkpoint,
            'restore': restored_stats, 'finalCheckpoint': final, 'jobmanagerStartedAtBefore': before,
            'jobmanagerStartedAtAfter': after, 'stock': code, 'committedPrefix': len(baseline),
            'uncommittedFaultBatch': len(uncommitted), 'committedFaultBatchBeforeKill': len(committed),
            'committedFinal': len(actual), 'uniqueBusinessKeys': len({r['value']['eventId'] for r in actual}),
            'D20': actual[-1]['value'], 'late': 0, 'dlt': 0}


def real_replay(topics, options, env_file):
    from dotenv import load_dotenv
    load_dotenv(env_file, override=True)
    os.environ['KAFKA_BOOTSTRAP_SERVERS'] = 'localhost:9092'
    os.environ['KAFKA_DAILY_TOPIC'] = topics['input']
    os.environ['POLARS_SKIP_CPU_CHECK'] = '1'
    collector = ROOT / 'python-services/python-collector'
    analysis = ROOT / 'python-services/stock-analysis-app'
    sys.path[:0] = [str(collector), str(analysis)]
    import pandas as pd
    from app.main import cmd_replay_daily_events
    from app.config import DATABASE_CONFIG
    from database.db_connector import DatabaseConnector
    from app.repositories.stock_repository import StockRepository
    from app.events.event_factory import EventFactory
    from app.kafka.serializer import KafkaJsonSerializer
    from data_processor.feature_engineer import FeatureEngineer
    db = DatabaseConnector(DATABASE_CONFIG)
    try:
        with db.session_scope() as session:
            records = [record for batch in StockRepository(session).iter_daily_records(
                date(2026, 4, 29), date(2026, 5, 29), 7, '000001.SZ') for record in batch]
    finally:
        db.close()
    if len(records) != 20 or len({r.trade_date for r in records}) != 20:
        raise AssertionError('real replay needs exactly 20 actual distinct trading days')
    job_id = submit(options)
    code = cmd_replay_daily_events(argparse.Namespace(
        start='20260429', end='20260529', batch_size=7, ts_code='000001.SZ'))
    if code:
        raise RuntimeError('Collector real replay failed')
    trace = 'replay-20260429-20260529'
    rows = [json.loads(KafkaJsonSerializer.serialize(EventFactory.create_daily_event(r, trace)),
                       parse_float=Decimal) for r in records]
    actual = v05.reconsume_committed('localhost:9092', topics['output'], '000001.SZ',
                                    v05.expected_indicators(rows), 180, quiet=25)
    frame = pd.DataFrame([r.to_dict() for r in records])
    frame['trade_date'] = pd.to_datetime(frame['trade_date'])
    for name in ('open', 'high', 'low', 'close', 'vol'):
        frame[name] = frame[name].astype(float)
    offline = FeatureEngineer().calculate_all_features(frame)
    mappings = {'ma5': 'ma5', 'ma10': 'ma10', 'ma20': 'ma20',
                'volMa5': 'vol_ma5', 'volMa10': 'vol_ma10', 'volumeRatio': 'volume_ratio'}
    checked = 0
    for index, record in enumerate(actual):
        for live, field in mappings.items():
            number = offline.iloc[index][field]
            wanted = None if pd.isna(number) else v05.scaled(number)
            if record['value'][live] != wanted:
                raise AssertionError(f'FeatureEngineer {live} differs on {record["value"]["tradeDate"]}')
            checked += 1
    checkpoint = v05.wait_for_checkpoint(URL, job_id, None, v05.observation_epoch_ms(), 180)
    lag = command('docker', 'exec', 'stock-kafka', '/opt/kafka/bin/kafka-consumer-groups.sh',
                  '--bootstrap-server', 'kafka:29092', '--describe', '--group', options[3])
    partition_lag = audit_input_lag(topics['input'], options[3])
    sides = audit_side_topics(topics, '000001.SZ')
    command('docker', 'exec', 'stock-flink-jobmanager', 'flink', 'cancel', job_id)
    return {'jobId': job_id, 'stock': '000001.SZ', 'start': rows[0]['tradeDate'],
            'end': rows[-1]['tradeDate'], 'actualTradingDays': 20, 'input': 20, 'committed': len(actual),
            'featureEngineerComparisons': checked, 'checkpoint': checkpoint, 'lag': lag,
            'partitionLag': partition_lag, **sides,
            'boundaries': {f'D{n}': actual[n - 1]['value'] for n in (1, 5, 10, 20)}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', choices=('recovery', 'real-replay'), required=True)
    parser.add_argument('--env-file', type=Path, help='Existing main .env; never printed or copied')
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError('optimized execution is unsupported')
    if args.report.exists():
        raise RuntimeError('report already exists; preserve it and choose a new report path')
    if args.scenario == 'real-replay' and (args.env_file is None or not args.env_file.is_file()):
        raise RuntimeError('real replay requires an existing --env-file')
    require_empty_cluster([j for j in v05.get_json(f'{URL}/jobs/overview', 5)['jobs']
                           if j['state'] not in ('CANCELED', 'FINISHED', 'FAILED')])
    token, topics, options = isolated_config()
    result = {'namespace': token, 'topics': topics, 'scenario': args.scenario}
    # Persist safe resource identifiers immediately, even when a later test fails.
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, indent=2), encoding='utf-8')
    detail = recovery(topics, options) if args.scenario == 'recovery' else real_replay(topics, options, args.env_file)
    result.update(status='PASS', **detail)
    args.report.write_text(json.dumps(result, indent=2, default=str) + '\n', encoding='utf-8')
    print(json.dumps(result, indent=2, default=str))


if __name__ == '__main__':
    main()
