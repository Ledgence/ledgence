#!/usr/bin/env python3
"""Durable callbacks through installed SDK, real Rust processes and PostgreSQL.

Uses an owned disposable database. The receiver intentionally loses replies and
returns failures. Retry due times are accelerated explicitly in one exhaustion
scenario; persisted attempt counts, leases and acknowledgments are never faked.
"""

import argparse
import asyncio
import contextlib
import http.server
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import traceback
import time
import urllib.parse
import uuid

from http_acceptance.harness import Deployment, exchange

FLOW = """from ledgence.worker.workflow import workflow_context
def handle(event):
    ctx = workflow_context()
    if ctx.continuation == 'start':
        return ctx.wait_event('finish', continuation='complete', state={'checkpoint': True})
    return ctx.complete({'completed': True, 'checkpoint': ctx.state})
"""


class Receiver:
    def __init__(self):
        self.lock = threading.Lock()
        self.records = []
        self.errors = []
        self.mode = 'accept'
        self.release = threading.Event()
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get('Content-Length', '0'))
                assert 0 < length <= 16384
                body = self.rfile.read(length)
                with owner.lock:
                    mode = owner.mode
                    owner.records.append(
                        dict(
                            body=body,
                            headers={k.lower(): v for k, v in self.headers.items()},
                            mode=mode,
                        )
                    )
                    if mode == 'lose-once':
                        owner.mode = 'accept'
                if mode == 'hold':
                    owner.release.wait(timeout=45)
                if mode in ('hold', 'lose-once'):
                    self.close_connection = True
                    return
                self.send_response(503 if mode == 'fail' else 204)
                self.send_header('Content-Length', '0')
                self.end_headers()

            def log_message(self, *_):
                pass

        class Server(http.server.ThreadingHTTPServer):
            def handle_error(self, request, address):
                with owner.lock:
                    owner.errors.append(traceback.format_exc())

        self.server = Server(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}/results'

    def set(self, mode):
        with self.lock:
            self.mode = mode

    def for_subscription(self, identity):
        with self.lock:
            return [
                r
                for r in self.records
                if r['headers'].get('ledgence-subscription-id') == identity
            ]

    def assert_healthy(self):
        with self.lock:
            assert not self.errors, self.errors

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


async def until(operation, predicate, timeout=40, description='condition'):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        value = await operation()
        if predicate(value):
            return value
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f'timed out: {description}; last={value!r}')
        await asyncio.sleep(0.05)


async def scenarios(d, receiver, record):
    import ledgence.client as installed
    from ledgence.client import (
        AsyncClient,
        CompletionSubscriptionUncertain,
        CompletionRetryUncertain,
        CompletionState,
        Conflict,
        NotFound,
        TaskFailed,
    )

    assert not Path(installed.__file__).resolve().is_relative_to(d.root), (
        'use installed client wheel'
    )
    proxy = d.proxy()
    options = dict(tenant=d.scope['tenant_id'], namespace=d.scope['namespace'])
    async with AsyncClient(proxy.url, **options) as client:
        async def submit(key, mode='success', **data):
            return await client.tasks.submit(
                program='invoice',
                version='1.0.0',
                queue=d.queue,
                idempotency_key=key,
                correlation_key='invoice%20literal\U0010ffff',
                data=dict(mode=mode, marker=str(d.marker), **data),
            )

        async def delivered(handle, timeout=40):
            return await until(
                handle.status,
                lambda s: s.state == CompletionState.DELIVERED,
                timeout,
                'callback acknowledgment',
            )

        # Register before workers start, lose the committed registration reply,
        # explicitly replay its frozen operation, then lose the receiver's first ack.
        task = await submit('registered-before-execution')
        prepared = task.prepare_subscribe(
            destination='results', idempotency_key='business-results'
        )
        lost = proxy.lose_once('/v1/completion-subscriptions')
        try:
            await task.subscribe(prepared)
            raise AssertionError('lost registration acknowledgment must be uncertain')
        except CompletionSubscriptionUncertain as error:
            assert error.command is prepared
        assert lost.is_set()
        subscription = await task.subscribe(prepared)
        assert (await subscription.status()).state == CompletionState.WAITING
        receiver.set('lose-once')
        worker = d.start_worker(concurrency=2)
        output = await task.result(timeout=30)
        assert output['event']['data']['mode'] == 'success'
        result = await delivered(subscription)
        records = receiver.for_subscription(subscription.id)
        assert len(records) == 2 and records[0]['body'] == records[1]['body']
        assert result.attempts == 2 and result.total_attempts == 2
        event = json.loads(records[0]['body'])
        assert 'data' not in event and event['ldgstate'] == 'succeeded'
        assert (
            event['ldgtaskid'] == task.id
            and event['ldgcorrelationkeyencoding'] == 'percent'
        )
        assert (
            urllib.parse.unquote(event['ldgcorrelationkey'])
            == 'invoice%20literal\U0010ffff'
        )
        assert records[0]['headers']['content-type'] == 'application/cloudevents+json'
        assert len(d.invocations(task.id)) == 1
        status, _, reference = await asyncio.to_thread(
            exchange, d.server_url, 'GET', event['ldgresultref']
        )
        assert status == 200 and reference['task']['task_id'] == task.id
        assert reference['outcome']['output'] == output
        record('uncertain_registration_and_duplicate_webhook', task.id)

        # A completed task allows a new subscription with the identical event,
        # while replaying the original registration never initiates another send.
        late = await task.subscribe(destination='results', idempotency_key='late')
        await delivered(late)
        assert receiver.for_subscription(late.id)[0]['body'] == records[0]['body']
        assert (await task.subscribe(prepared)).id == subscription.id
        replayed = await subscription.status()
        assert (
            replayed.state,
            replayed.generation,
            replayed.attempts,
            replayed.total_attempts,
        ) == (CompletionState.DELIVERED, 1, 2, 2)
        await asyncio.sleep(1.2)
        assert len(receiver.for_subscription(subscription.id)) == 2
        async with AsyncClient(
            proxy.url, tenant=options['tenant'], namespace='other'
        ) as wrong:
            try:
                await wrong.completions.handle(subscription.id).status()
                raise AssertionError('scope isolation')
            except NotFound:
                pass
        record('late_registration_and_scope_isolation', late.id)

        # Eight genuine failed sends, accelerating only the due-time clock in the
        # owned database. Explicit redelivery must not rerun the application.
        receiver.set('fail')
        exhausted = await task.subscribe(
            destination='results', idempotency_key='receiver-outage'
        )
        for count in range(1, 9):
            state = await until(
                exhausted.status,
                lambda s: s.attempts == count
                and s.state in (CompletionState.RETRYING, CompletionState.EXHAUSTED),
                description=f'failed delivery {count}',
            )
            if count < 8:
                await asyncio.to_thread(
                    d.sql,
                    f"UPDATE completion_subscriptions SET next_attempt_at_ms="
                    f"(extract(epoch FROM clock_timestamp())*1000)::bigint "
                    f"WHERE subscription_id='{exhausted.id}' AND state='retrying'",
                )
        assert state.state == CompletionState.EXHAUSTED and state.generation == 1
        assert exchange(d.server_url, 'GET', '/health/ready')[0] == 200
        receiver.set('accept')
        retry = exhausted.prepare_retry(expected_generation=1)
        lost = proxy.lose_once('/v1/completion-subscriptions/retry')
        try:
            await exhausted.retry(retry)
            raise AssertionError('lost redelivery acknowledgment must be uncertain')
        except CompletionRetryUncertain as error:
            assert error.command is retry
        assert lost.is_set()
        await exhausted.retry(retry)
        state = await delivered(exhausted)
        assert (state.generation, state.attempts, state.total_attempts) == (2, 1, 9)
        assert len({r['body'] for r in receiver.for_subscription(exhausted.id)}) == 1
        replayed = await exhausted.retry(retry)
        assert (
            replayed.state,
            replayed.generation,
            replayed.attempts,
            replayed.total_attempts,
        ) == (CompletionState.DELIVERED, 2, 1, 9)
        await asyncio.sleep(1.2)
        assert len(receiver.for_subscription(exhausted.id)) == 9
        assert len(d.invocations(task.id)) == 1
        assert await task.result(timeout=1) == output
        record('exhaustion_redelivery_and_independent_execution', exhausted.id)

        # Kill the real sender while its receiver holds a request. A new server
        # must recover the persisted lease after its real 30-second expiry.
        receiver.set('hold')
        crash = await task.subscribe(
            destination='results', idempotency_key='sender-crash'
        )
        await until(crash.status, lambda s: s.state == CompletionState.DELIVERING)
        deadline = time.monotonic() + 10
        while not receiver.for_subscription(crash.id):
            assert time.monotonic() < deadline
            await asyncio.sleep(0.02)
        await asyncio.to_thread(d.server.kill)
        receiver.set('accept')
        receiver.release.set()
        d.server, _ = await asyncio.to_thread(d.start_server)
        recovered = await delivered(crash, timeout=45)
        assert recovered.attempts == 2
        records = receiver.for_subscription(crash.id)
        assert len(records) == 2 and records[0]['body'] == records[1]['body']
        assert len(d.invocations(task.id)) == 1
        record('sender_crash_and_expired_lease_recovery', crash.id)

        # Workflow activation completion is not workflow completion: no callback
        # while a checkpoint waits for a durable event, including after restart.
        workflow = await client.workflows.submit(
            program='callback-flow',
            version='1.0.0',
            queue=d.queue,
            data={},
            idempotency_key='workflow-notification',
        )
        notify = await workflow.subscribe(
            destination='results', idempotency_key='workflow-result'
        )
        await until(workflow.status, lambda s: s.state.value == 'waiting')
        assert (await notify.status()).state == CompletionState.WAITING
        assert not receiver.for_subscription(notify.id)
        await asyncio.to_thread(d.server.stop)
        d.server, _ = await asyncio.to_thread(d.start_server)
        assert (await workflow.status()).state.value == 'waiting'
        assert (await notify.status()).state == CompletionState.WAITING
        assert not receiver.for_subscription(notify.id)
        wake = {
            'specversion': '1.0',
            'id': 'finish',
            'source': 'urn:test',
            'type': 'test.finish',
            'datacontenttype': 'application/json',
            'data': None,
        }
        await workflow.send_event(key='finish', event=wake)
        assert await workflow.result(timeout=30) == {
            'completed': True,
            'checkpoint': {'checkpoint': True},
        }
        await delivered(notify)
        event = json.loads(receiver.for_subscription(notify.id)[0]['body'])
        assert event['ldgworkflowid'] == workflow.id and event['ldgstate'] == 'succeeded'
        assert not {'ldgtaskid', 'ldgactivationid', 'data'} & event.keys()
        status, _, reference = await asyncio.to_thread(
            exchange, d.server_url, 'GET', event['ldgresultref']
        )
        assert status == 200 and reference['workflow']['workflow_id'] == workflow.id
        assert reference['outcome']['output']['completed'] is True
        record('workflow_checkpoint_wait_then_terminal_callback', workflow.id)

        failed = await submit('failed-execution', mode='business')
        notify = await failed.subscribe(destination='results', idempotency_key='failed')
        try:
            await failed.result(timeout=30)
        except TaskFailed:
            pass
        else:
            raise AssertionError('business failure must be retained')
        await delivered(notify)
        assert (
            json.loads(receiver.for_subscription(notify.id)[0]['body'])['ldgstate']
            == 'failed'
        )
        # A deliberately unconsumed queue makes cancellation-before-acquisition deterministic.
        cancelled = await client.tasks.submit(
            program='invoice',
            version='1.0.0',
            queue='unconsumed',
            data={},
            idempotency_key='cancelled',
        )
        notify = await cancelled.subscribe(
            destination='results', idempotency_key='cancelled'
        )
        await cancelled.cancel()
        await delivered(notify)
        assert (
            json.loads(receiver.for_subscription(notify.id)[0]['body'])['ldgstate']
            == 'cancelled'
        )
        assert not d.invocations(cancelled.id)
        record('failure_and_cancellation_notifications', failed.id)
        await asyncio.to_thread(worker.stop)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--psql', default='psql')
    parser.add_argument('--binaries', type=Path, required=True)
    parser.add_argument('--evidence', type=Path)
    args = parser.parse_args()
    parent = os.environ.get('LEDGENCE_POSTGRES_URL')
    if not parent:
        parser.error('LEDGENCE_POSTGRES_URL must name an owned disposable server')

    root = Path(__file__).resolve().parents[1]
    directory = args.evidence or Path(tempfile.mkdtemp(prefix='ledgence-completions-'))
    if args.evidence:
        directory.mkdir(parents=True, exist_ok=False)
    directory = directory.resolve()
    database = 'ledgence_completions_' + uuid.uuid4().hex
    url = urllib.parse.urlunsplit(
        urllib.parse.urlsplit(parent)._replace(path='/' + database)
    )

    def admin(sql):
        subprocess.run(
            [
                args.psql,
                '--dbname',
                parent,
                '-X',
                '--set',
                'ON_ERROR_STOP=1',
                '--command',
                sql,
            ],
            check=True,
            capture_output=True,
            timeout=40,
        )

    d = None
    receiver = None
    created = False
    succeeded = False
    results = []

    def record(name, detail):
        receiver.assert_healthy()
        results.append(dict(scenario=name, result='passed', detail=detail))
        (directory / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
        print(f'PASS {name}', flush=True)

    try:
        admin(f'CREATE DATABASE "{database}"')
        created = True
        d = Deployment(
            root,
            directory,
            args.binaries.resolve(),
            os.environ.get('LEDGENCE_PYTHON', sys.executable),
            url,
            args.psql,
        )
        d.publish('callback-flow', '1.0.0', program_source=FLOW, runtime_protocol=3)
        receiver = Receiver()
        d.completion_config = directory / 'completions.json'
        d.completion_config.write_text(
            json.dumps(
                {
                    'destinations': [
                        {
                            'scope': d.scope,
                            'destination': 'results',
                            'url': receiver.url,
                        }
                    ]
                }
            )
        )
        subprocess.run(
            [str(d.binaries / 'ledgence-orchestrator'), 'migrate'],
            env=d.environment,
            check=True,
            capture_output=True,
            timeout=40,
        )
        d.server, _ = d.start_server()
        asyncio.run(scenarios(d, receiver, record))
        receiver.assert_healthy()
        succeeded = True
        print(f'Completion acceptance passed: {len(results)} scenarios')
        return 0
    finally:
        try:
            try:
                if receiver:
                    receiver.close()
            finally:
                if d:
                    d.close()
        finally:
            if created:
                admin(f'DROP DATABASE "{database}" WITH (FORCE)')
            if succeeded and not args.evidence:
                shutil.rmtree(directory)
            elif not succeeded:
                print(f'Failure evidence: {directory}', file=sys.stderr)


if __name__ == '__main__':
    sys.exit(main())
