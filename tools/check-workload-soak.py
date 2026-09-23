#!/usr/bin/env python3
"""Bounded mixed-workload soak using an installed client and owned local services.

Closed-loop concurrency tests stability/correctness; use check-performance.py for
open-arrival throughput. No production or daily-capacity qualification is implied.
"""

import argparse
import asyncio
from collections import Counter
import importlib.metadata
import json
import math
import os
import platform
from pathlib import Path
import runpy
import re
import traceback
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.parse
import uuid

from http_acceptance.harness import Deployment
from http_acceptance.sqs import SqsDeployment
from workload_acceptance.observations import CENSUS, resources, summarize, process_tree
from workload_acceptance.programs import MODES, PROGRAM, expected_counts
from workload_acceptance.receiver import Receiver

PERFORMANCE = runpy.run_path(str(Path(__file__).with_name('check-performance.py')))
MAX_OPERATIONS = 100_000


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument('--self-test', action='store_true')
    result.add_argument('--disposable-postgres', action='store_true')
    result.add_argument('--binaries', type=Path, required=False, help='explicit prebuilt release binaries')
    result.add_argument('--evidence', type=Path)
    result.add_argument('--psql', default='psql')
    result.add_argument('--duration', type=float, default=900)
    result.add_argument('--clients', type=int, default=16)
    result.add_argument('--workers', type=int, default=2)
    result.add_argument('--concurrency', type=int, default=8, help='process/consumer slots per worker')
    result.add_argument('--max-operations', type=int, default=MAX_OPERATIONS)
    result.add_argument('--work-ms', type=int, default=25)
    result.add_argument('--payload-bytes', type=int, default=1024)
    result.add_argument('--sample-interval', type=float, default=5)
    result.add_argument('--rss-limit-mib', type=int, default=2048)
    result.add_argument('--rss-growth-limit-mib', type=int, default=128)
    result.add_argument('--operation-timeout', type=float, default=180)
    result.add_argument('--delivery-config', type=Path)
    result.add_argument('--disposable-queue', action='store_true')
    result.add_argument('--metrics-endpoint', help='explicit OTLP HTTP/protobuf metrics URL')
    return result


def validate(args):
    limits = dict(duration=(10, 3600), clients=(1, 64), workers=(1, 4), concurrency=(1, 32),
                  max_operations=(6, MAX_OPERATIONS), work_ms=(0, 1000), payload_bytes=(0, 16384),
                  sample_interval=(1, 10), rss_limit_mib=(64, 8192), rss_growth_limit_mib=(1, 2048),
                  operation_timeout=(10, 600))
    for name, (low, high) in limits.items():
        value = getattr(args, name)
        if not math.isfinite(value) or not low <= value <= high:
            raise ValueError(f'{name} must be finite and between {low} and {high}')
    if bool(args.delivery_config) != args.disposable_queue:
        raise ValueError('delivery config and explicit disposable queue confirmation are required together')
    if args.metrics_endpoint:
        PERFORMANCE['validate_metrics_endpoint'](args.metrics_endpoint)


def environment(deployment, args):
    deployment.environment = {key: value for key, value in deployment.environment.items()
                              if not key.startswith(('OTEL_', 'LEDGENCE_POSTGRES_NOTIFICATION'))}
    deployment.environment['RUST_LOG'] = 'warn'
    if args.metrics_endpoint:
        deployment.environment['OTEL_EXPORTER_OTLP_METRICS_ENDPOINT'] = args.metrics_endpoint


async def exercise(deployment, receiver, args, directory, report):
    import ledgence.client as installed
    from ledgence.client import AsyncClient, RetryPolicy, CompletionState

    assert not Path(installed.__file__).resolve().is_relative_to(deployment.root), 'run with installed SDK wheel'
    report['client'] = dict(version=importlib.metadata.version('ledgence-client'), module=str(installed.__file__),
                            module_sha256=PERFORMANCE['digest'](Path(installed.__file__)))
    modes, latencies = Counter(), {mode: PERFORMANCE['Histogram']() for mode in MODES}
    indices, samples = [], []
    next_index = 0
    started = time.monotonic()
    deadline = started + args.duration
    finished = asyncio.Event()
    sql = PERFORMANCE['sql_json']

    def healthy():
        receiver.check()
        for process in deployment.processes:
            if process.process.poll() is not None:
                raise RuntimeError(f'{process.label} exited during soak')

    async def sample(stream):
        while True:
            healthy()
            processes = await asyncio.to_thread(resources, deployment)
            census = await asyncio.to_thread(sql, deployment, CENSUS, 15)
            row = dict(elapsed_seconds=time.monotonic() - started, processes=processes, database=census)
            samples.append(row)
            stream.write(json.dumps(row) + '\n')
            stream.flush()
            for name, process in processes.items():
                if not process['present']:
                    raise AssertionError(f'{name} vanished while sampling')
                if name.startswith('worker-') and process['descendants'] > args.concurrency:
                    raise AssertionError('worker exceeded its global subprocess bound')
            if sum(p['rss_bytes'] for p in processes.values()) > args.rss_limit_mib * 1024**2:
                raise AssertionError('service RSS exceeded configured soak bound')
            if finished.is_set():
                break
            try:
                await asyncio.wait_for(finished.wait(), timeout=args.sample_interval)
            except TimeoutError:
                pass

    async with AsyncClient(deployment.server_url, tenant=deployment.scope['tenant_id'],
                           namespace=deployment.scope['namespace'], request_timeout=15) as client:
        async def operation(index):
            mode = MODES[index % len(MODES)]
            source = client.tasks if mode in ('task', 'retry') else client.workflows
            command = source.prepare(program='soak', version='1.0.0', queue=deployment.queue,
                data=dict(index=index, mode=mode, queue=deployment.queue, work_ms=args.work_ms,
                          padding='x' * args.payload_bytes),
                idempotency_key=f'soak-{index}', correlation_key=f'soak-{index}',
                retry_policy=RetryPolicy(max_attempts=2, retry_delay_ms=0))
            handle = await source.submit(command)
            if index % 13 == 0:
                assert (await source.submit(command)).id == handle.id, 'idempotent replay changed execution identity'
            subscription = None
            if index % 7 == 0:
                subscription = await handle.subscribe(destination='soak', idempotency_key='result')
            if mode == 'event':
                await handle.send_event(key='finish', event=dict(specversion='1.0', id=f'event-{index}',
                    source='urn:ledgence:workload-soak', type='soak.finish.v1', datacontenttype='application/json',
                    data=dict(index=index)))
            output = await handle.result(timeout=args.operation_timeout)
            assert output == dict(index=index, mode=mode), output
            if subscription:
                while True:
                    status = await subscription.status()
                    if status.state == CompletionState.DELIVERED:
                        receiver.verify(status)
                        break
                    assert status.state != CompletionState.EXHAUSTED, 'callback exhausted unexpectedly'
                    await asyncio.sleep(.25)
            return dict(index=index, mode=mode, execution_id=handle.id,
                        subscription_id=subscription.id if subscription else None)

        with (directory / 'operations.jsonl').open('w') as journal, (directory / 'samples.jsonl').open('w') as sample_stream:
            async def lane():
                nonlocal next_index
                while time.monotonic() < deadline and next_index < args.max_operations:
                    healthy()
                    index = next_index
                    next_index += 1
                    begin = time.monotonic()
                    async with asyncio.timeout(args.operation_timeout):
                        record = await operation(index)
                    elapsed = (time.monotonic() - begin) * 1000
                    modes[record['mode']] += 1
                    latencies[record['mode']].add(elapsed)
                    indices.append(index)
                    journal.write(json.dumps(dict(record, elapsed_ms=elapsed)) + '\n')
                    journal.flush()

            async def lanes():
                try:
                    async with asyncio.TaskGroup() as group:
                        for _ in range(args.clients):
                            group.create_task(lane())
                finally:
                    finished.set()

            async with asyncio.TaskGroup() as group:
                group.create_task(sample(sample_stream))
                group.create_task(lanes())
    report.update(elapsed_seconds=time.monotonic() - started, completed_operations=len(indices),
                  modes=dict(modes), observation_latency_ms={mode: h.summary() for mode,h in latencies.items()},
                  resources=summarize(samples, time.monotonic() - started), receiver=receiver.summary(),
                  operation_limit_reached=next_index == args.max_operations)
    assert set(modes) == set(MODES), 'every mixed workload mode must execute'
    assert report['resources']['median_growth_bytes'] <= args.rss_growth_limit_mib * 1024**2, 'service RSS grew beyond configured bound'
    report['expected_counts'] = expected_counts(indices)
    final = await asyncio.to_thread(sql, deployment, CENSUS, 30)
    report['final_census'] = final
    assert final['pending_workflow_work'] == final['dispatch_intents'] == final['active_attempts'] == 0, final
    for key, value in report['expected_counts'].items():
        assert final[key] == value, f'{key}: expected {value}, got {final[key]}'
    assert final['task_states'] == dict(succeeded=final['tasks']), final
    assert final['workflow_states'] == dict(succeeded=final['workflows']), final
    assert final['callback_states'] == dict(delivered=final['callbacks']), final
    assert report['receiver']['unique'] == final['callbacks'], 'receiver missed accepted callbacks'


def main():
    if sys.version_info < (3, 11):
        raise SystemExit('CPython >=3.11 is required')
    cli = parser()
    args = cli.parse_args()
    if args.self_test:
        return 0 if unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(SelfTests)).wasSuccessful() else 1
    try:
        validate(args)
        config_digest = PERFORMANCE['inspect_delivery_config'](args.delivery_config) if args.delivery_config else None
    except (ValueError, OSError) as error:
        cli.error(str(error))
    parent = os.environ.get('LEDGENCE_POSTGRES_URL')
    if not args.disposable_postgres or not parent or args.binaries is None:
        cli.error('--disposable-postgres, LEDGENCE_POSTGRES_URL, and --binaries are required')
    parsed = urllib.parse.urlsplit(parent)
    if parsed.scheme not in ('postgres', 'postgresql') or not parsed.hostname:
        cli.error('invalid PostgreSQL URL')
    if any(key.lower() in ('dbname', 'database') for key, _ in urllib.parse.parse_qsl(parsed.query)):
        cli.error('PostgreSQL URL must not override database in query')
    root = Path(__file__).resolve().parents[1]
    binaries = args.binaries.resolve()
    hashes = {name: PERFORMANCE['digest'](binaries / name) for name in ('ledgence', 'ledgence-worker', 'ledgence-orchestrator')}
    directory = args.evidence or Path(tempfile.mkdtemp(prefix='ledgence-workload-soak-'))
    if args.evidence:
        directory.mkdir(parents=True, exist_ok=False)
    directory = directory.resolve()
    report = dict(schema_version=1, result='failed', workload='bounded-concurrency mixed soak', daily_capacity_qualified=False,
        source_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
        source_dirty=bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=root, text=True).strip()),
        binaries=hashes, binary_source_verified=False, delivery_config_sha256=config_digest,
        platform=platform.platform(), architecture=platform.machine(), logical_cpus=os.cpu_count(),
        python=sys.version,
        settings={k: str(v) if isinstance(v, Path) else v for k,v in vars(args).items()},
        limitations=['Closed loop: offered work falls when latency rises; use open-arrival harness for throughput.',
                     'Process-family RSS excludes PostgreSQL, broker, OS page cache and harness.',
                     'Database grows during this test; retention correctness is a separate aged-data gate.',
                     'Short local run does not prove absence of leaks or production capacity.'])
    database = 'ledgence_soak_' + uuid.uuid4().hex
    database_url = urllib.parse.urlunsplit(parsed._replace(path='/' + database))
    d = receiver = None
    created = False
    failures = []
    stage = 'database_creation'

    def admin(statement):
        subprocess.run([args.psql, '--dbname', parent, '-X', '-q', '--set', 'ON_ERROR_STOP=1', '--command', statement],
                       env=dict(os.environ, PGCONNECT_TIMEOUT='5'), capture_output=True, check=True, timeout=40)

    def interrupt(_signal, _frame):
        raise KeyboardInterrupt

    previous = {sig: signal.signal(sig, interrupt) for sig in (signal.SIGINT, signal.SIGTERM)}
    print(f'Evidence: {directory}', flush=True)
    try:
        # Retain cleanup authority even if the create response is interrupted.
        created = True
        admin(f'CREATE DATABASE "{database}"')
        stage = 'deployment'
        kind = SqsDeployment if args.delivery_config else Deployment
        kwargs = dict(delivery_config=args.delivery_config) if args.delivery_config else {}
        d = kind(root, directory, binaries, os.environ.get('LEDGENCE_PYTHON', sys.executable), database_url, args.psql, **kwargs)
        environment(d, args)
        d.publish('soak', '1.0.0', program_source=PROGRAM, runtime_protocol=3)
        receiver = Receiver(directory / 'callbacks.jsonl', math.ceil(args.max_operations / 7))
        d.completion_config = directory / 'completions.json'
        d.completion_config.write_text(json.dumps(dict(destinations=[dict(scope=d.scope, destination='soak', url=receiver.url)])))
        subprocess.run([str(binaries / 'ledgence-orchestrator'), 'migrate'], env=d.environment,
                       check=True, capture_output=True, timeout=650)
        d.server, _ = d.start_server()
        workers = [d.start_worker(concurrency=args.concurrency, cache=f'cache-{index}') for index in range(args.workers)]
        stage = 'mixed_soak'
        asyncio.run(exercise(d, receiver, args, directory, report))
        stage = 'graceful_shutdown'
        for worker in workers:
            worker.stop()
        d.server.stop()
        report['artifact_downloads'] = d.artifacts.downloads()
    except KeyboardInterrupt:
        failures.append('interrupted')
    except BaseException as error:
        # Error messages may embed database credentials; store types and phase only.
        failures.append(f'{stage}: {type(error).__name__}')
        detail = ''.join(traceback.format_exception(error))
        detail = re.sub(r'postgres(?:ql)?://[^\s\"\']+', '<redacted-postgres-url>', detail)
        (directory / 'failure.log').write_text(detail)
        if isinstance(error, BaseExceptionGroup):
            report['exception_types'] = [type(value).__name__ for value in error.exceptions]
    finally:
        for label, cleanup in [('deployment', lambda: d.close() if d else None),
                               ('receiver', lambda: receiver.close() if receiver else None),
                               ('database', lambda: admin(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)') if created else None)]:
            try:
                cleanup()
            except BaseException as error:
                failures.append(f'{label}_cleanup: {type(error).__name__}')
        if args.delivery_config:
            try:
                if PERFORMANCE['inspect_delivery_config'](args.delivery_config) != config_digest:
                    failures.append('delivery_configuration_changed')
            except (ValueError, OSError):
                failures.append('delivery_configuration_unreadable')
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        passed = 'passed_bounded_mixed_run' if report.get('operation_limit_reached') else 'passed_bounded_soak'
        report.update(result=passed if not failures else 'failed', failures=failures)
        (directory / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(dict(result=report['result'], failures=failures, evidence=str(directory))), flush=True)
    return int(bool(failures))


class SelfTests(unittest.TestCase):
    def test_mixed_census_includes_owned_work_and_retry_attempt(self):
        self.assertEqual(expected_counts(range(6)), dict(tasks=12, attempts=13, workflows=5, local_results=4, callbacks=1))
        self.assertEqual(expected_counts([7]), dict(tasks=1, attempts=2, workflows=0, local_results=0, callbacks=1))

    def test_resource_tree_does_not_count_other_applications(self):
        rows = {1:(0,100), 2:(1,200), 3:(2,300), 4:(0,400)}
        self.assertEqual(process_tree(rows,1), dict(rss_bytes=600, descendants=2, present=True))
        self.assertFalse(process_tree(rows,9)['present'])

    def test_nonfinite_and_unbounded_inputs_are_rejected(self):
        for name, value in [('duration', float('nan')), ('duration', 3601), ('max_operations', MAX_OPERATIONS+1), ('clients', 65)]:
            args = parser().parse_args([])
            setattr(args,name,value)
            with self.assertRaises(ValueError):
                validate(args)

    def test_receiver_accepts_identical_duplicates_and_detects_changed_events(self):
        import urllib.request
        import urllib.error
        import http.client
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as temporary:
            receiver = Receiver(Path(temporary) / 'events.jsonl', 1)
            event = dict(specversion='1.0', id='event', source='urn:test', type='complete.v1')
            def send(value):
                request = urllib.request.Request(receiver.url, json.dumps(value).encode(),
                    headers={'ledgence-subscription-id': 'subscription'}, method='POST')
                return urllib.request.urlopen(request, timeout=5)
            try:
                for _ in range(2):
                    with send(event) as response:
                        self.assertEqual(response.status,204)
                receiver.verify(SimpleNamespace(subscription_id='subscription',event=event))
                self.assertEqual(receiver.summary(),dict(unique=1,received=2,duplicates=1))
                with self.assertRaises((http.client.RemoteDisconnected, urllib.error.URLError)):
                    send(dict(event,id='changed'))
                with self.assertRaises(AssertionError):
                    receiver.check()
            finally:
                receiver.close()

    def test_growth_assessment_requires_disjoint_mature_samples(self):
        for count in (1,2,5):
            samples = [dict(elapsed_seconds=10+i, processes={'worker-1':dict(rss_bytes=1024, descendants=1)}) for i in range(count)]
            with self.assertRaisesRegex(AssertionError,'six mature'):
                summarize(samples,10)

    def test_summary_uses_process_families_and_mature_medians(self):
        samples = [dict(elapsed_seconds=i, processes={'worker-1':dict(rss_bytes=i*1024, descendants=2)}) for i in range(1,10)]
        result = summarize(samples,9)
        self.assertEqual(result['peak_worker_descendants'],2)
        self.assertEqual(result['mature_samples'],7)
        self.assertEqual(result['median_growth_bytes'],5*1024)


if __name__ == '__main__':
    raise SystemExit(main())
