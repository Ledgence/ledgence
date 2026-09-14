#!/usr/bin/env python3
"""Separate-process checkpoint workflow acceptance using an owned disposable DB.

Requires LEDGENCE_POSTGRES_URL, psql, CPython >=3.11 and the Python client's
runtime dependencies. Imports the client from this checkout; wheel installation
is covered by check-python-client-e2e.py. Optional --endpoint is an explicit
owned loopback ElasticMQ endpoint. This gate records functional timings, not a
production throughput or exactly-once-effects qualification.
"""
import argparse
import asyncio
import contextlib
import http.server
import hashlib
import ipaddress
import json
import os
import platform
from pathlib import Path
import runpy
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.parse
import uuid

from http_acceptance.harness import Deployment, Process, eventually, exchange
from http_acceptance.sqs import SqsDeployment


FIXTURE = r'''
import asyncio, hashlib, json, os, time
from pathlib import Path
from urllib.request import urlopen
from ledgence_worker.workflow import workflow_context

def mark(marker, tag, kind, **fields):
    with open(marker, 'a', encoding='utf-8') as stream:
        stream.write(json.dumps(dict(tag=tag, kind=kind, pid=os.getpid(), **fields)) + '\n')

async def local_io(*, url, marker, tag, index):
    started = time.monotonic_ns()
    mark(marker, tag, 'local_started', index=index)
    body = await fetch(url)
    finished = time.monotonic_ns()
    mark(marker, tag, 'local_finished', index=index)
    return dict(index=index, body=body, started_ns=started, finished_ns=finished)

async def deep_value(*, depth):
    value = 0
    for _ in range(depth):
        value = [value]
    return value

async def handle(event):
    ctx = workflow_context()
    data = event['data']
    tag, marker = data['tag'], data['marker']
    mark(marker, tag, 'activation', activation=ctx.activation_id, attempt=event['ldgattemptno'], continuation=ctx.continuation)
    if data['mode'] in ('event', 'timer'):
        if ctx.continuation == 'start':
            state = {'checkpoint': data['tag']}
            if data['mode'] == 'event':
                return ctx.wait_event(data['wait_key'], continuation='woken', state=state,
                    timeout_ms=data.get('timeout_ms'))
            return ctx.sleep(data['wait_key'], data['delay_ms'], continuation='woken', state=state)
        wake = ctx.wake
        mark(marker, tag, 'wake', activation=ctx.activation_id, attempt=event['ldgattemptno'], wake=wake)
        if data.get('retry_on_wake') and event['ldgattemptno'] == 1:
            raise RuntimeError('intentional first resumed activation failure')
        if data.get('summarize_wake'):
            source = wake['event']
            encoded = json.dumps(source, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
            return ctx.complete(dict(event_id=source['id'], time=source['time'], value=source['data']['value'],
                padding_bytes=len(source['data']['padding'].encode()), event_sha256=hashlib.sha256(encoded).hexdigest(),
                python_encoded_bytes=len(encoded)))
        return ctx.complete(dict(wake=wake, state=ctx.state, inputs=ctx.inputs))
    if data['mode'] == 'depth':
        return ctx.complete(await ctx.local('depth', deep_value, depth=64))
    if data['mode'] == 'distributed':
        state = ctx.state or {}
        offset = state.get('offset', 0)
        total = state.get('total', 0)
        if ctx.continuation == 'batch':
            for index in range(state['previous'], offset):
                total += ctx.get_result(f'item-{index}')['index']
        if offset >= data['count']:
            return ctx.complete(dict(count=data['count'], total=total))
        end = min(offset + 50, data['count'])
        children = [ctx.task(f'item-{index}', program='workflow-io', version='1.0.0', queue=data['queue'],
            data=dict(url=data['url'], marker=marker, tag=tag, index=index)) for index in range(offset, end)]
        return ctx.suspend(continuation='batch', state=dict(previous=offset,offset=end,total=total), until=children)
    if ctx.continuation == 'start':
        if data.get('controller_gate'):
            values = [await ctx.local('fetch-0', local_io, url=data['url'], marker=marker, tag=tag, index=0)]
            mark(marker, tag, 'after_ack', activation=ctx.activation_id, attempt=event['ldgattemptno'])
            if event['ldgattemptno'] == 1:
                parent = os.getppid()
                while not Path(data['controller_gate']).exists():
                    if os.getppid() != parent:
                        raise RuntimeError('owned test worker disappeared')
                    await asyncio.sleep(0.02)
        else:
            values = await ctx.gather(*[ctx.local(f'fetch-{index}', local_io,
                url=data['url'], marker=marker, tag=tag, index=index) for index in range(data['count'])])
        overlap = len(values) < 2 or max(v['started_ns'] for v in values) < min(v['finished_ns'] for v in values)
        state = dict(count=len(values), total=sum(v['index'] for v in values), overlap=overlap)
        if data['mode'] == 'local':
            return ctx.complete(state)
        child = ctx.task('child', program='workflow-io', version='1.0.0', queue=data['queue'],
            data=dict(url=data['url'], marker=marker, tag=tag, index=state['total'], gate=data.get('child_gate')))
        return ctx.suspend(continuation='collect', state=state, until=[child])
    return ctx.complete(dict(ctx.state, child=ctx.get_result('child')))
'''
CHILD = r'''
import json, os, time
from pathlib import Path
from urllib.request import urlopen

def handle(event):
    data = event['data']
    with open(data['marker'], 'a', encoding='utf-8') as stream:
        stream.write(json.dumps(dict(tag=data['tag'],kind='child',pid=os.getpid(),task=event['ldgtaskid'],workflow=event.get('ldgworkflowid'),index=data['index']))+'\n')
    parent = os.getppid()
    deadline = time.monotonic() + 150
    while data.get('gate') and not Path(data['gate']).exists():
        if os.getppid() != parent or time.monotonic() > deadline:
            raise RuntimeError('owned test child gate was not released')
        time.sleep(0.02)
    with urlopen(data['url'], timeout=10) as response:
        body = response.read(65537)
    if len(body) > 65536:
        raise RuntimeError('oversized test reply')
    return dict(index=data['index'],body=body.decode(),workflow=event.get('ldgworkflowid'))
'''


class DelayServer:
    def __init__(self):
        self.active = self.peak = 0
        self.lock = threading.Lock()
        owner = self
        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def do_GET(self):
                with owner.lock:
                    owner.active += 1
                    owner.peak = max(owner.peak, owner.active)
                try:
                    time.sleep(0.03)
                    body = b'local network I/O'
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                finally:
                    with owner.lock:
                        owner.active -= 1
            def log_message(self, *_):
                pass
        class Server(http.server.ThreadingHTTPServer):
            request_queue_size = 128
        self.server = Server(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}/delay'
    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def publish(d, name, source):
    directory = d.directory / ('source-' + name)
    info = d.command('ledgence-worker', ['example', '--directory', str(directory), '--python', d.python])
    package = Path(info['program'])
    manifest_file = package / 'ledgence-program.json'
    manifest = json.loads(manifest_file.read_text())
    manifest['program'] = {'id': name, 'version': '1.0.0'}
    manifest['runtime']['protocol'] = 3
    manifest_file.write_text(json.dumps(manifest))
    (package / 'program.py').write_text(source)
    return d.command('ledgence-worker', ['publish', '--source', str(package), '--store', str(d.store)])


def records(d, tag, kind=None):
    marker = d.directory / 'workflow-markers.jsonl'
    text = marker.read_text() if marker.exists() else ''
    rows = [json.loads(line) for line in text.splitlines(keepends=True) if line.endswith('\n')]
    return [row for row in rows if row['tag'] == tag and (kind is None or row['kind'] == kind)]


def snapshot(d, workflow_id):
    query = urllib.parse.urlencode(dict(d.scope, workflow_id=workflow_id))
    status, _, result = exchange(d.server_url, 'GET', '/v1/workflows/status?' + query)
    assert status == 200, result
    return result


def boundary_event():
    # Rust's JSON encoder omits the leading zero in this float exponent.
    # The event is exactly at the authoritative 64 KiB limit, although Python
    # encodes the same typed JSON one byte longer. Bypass outbound SDK preflight
    # only for this cross-language inbound compatibility regression.
    event = dict(specversion='1.0', id='evt-float-boundary', source='urn:workflow-acceptance:boundary',
        type='boundary.accepted.v1', time='2016-12-31T23:59:60Z', datacontenttype='application/json',
        data=dict(value=2.9802322387695312e-8, padding=''))
    event['data']['padding'] = 'x' * (65536-len(rust_boundary_bytes(event)))
    encoded = rust_boundary_bytes(event)
    assert len(encoded)==65536 and len(json.dumps(event,sort_keys=True,separators=(',', ':')).encode())==65537
    assert json.loads(encoded)==event
    return event, encoded


def rust_boundary_bytes(value):
    encoded = json.dumps(value,sort_keys=True,separators=(',', ':'),ensure_ascii=False).encode()
    assert encoded.count(b'2.9802322387695312e-08')==1
    return encoded.replace(b'2.9802322387695312e-08',b'2.9802322387695312e-8')


async def scenarios(d, delay, names, record, placement_iterations=3, capture=None):
    from ledgence.client import AsyncClient, RetryPolicy, WaitTimeout, WorkflowEventUncertain
    options = dict(tenant=d.scope['tenant_id'], namespace=d.scope['namespace'])

    def data(tag, mode='mixed', count=4, **extra):
        return dict(tag=tag, mode=mode, count=count, marker=str(d.directory/'workflow-markers.jsonl'),
                    queue=d.queue, url=delay.url, **extra)

    async def submit(client, tag, **kwargs):
        prepared = client.workflows.prepare(program='workflow-controller', version='1.0.0', queue=d.queue,
            data=data(tag, **kwargs), idempotency_key=tag, retry_policy=RetryPolicy(max_attempts=3,retry_delay_ms=0))
        return await client.workflows.submit(prepared)

    if 'examples' in names:
        worker = d.start_worker(concurrency=1)
        async with AsyncClient(d.server_url, **options) as client:
            prepared = client.workflows.prepare(program='workflow-pages',version='1.0.0',queue=d.queue,
                data={'urls':[delay.url+'?index='+str(index) for index in range(4)],'queue':d.queue},
                idempotency_key='public-example')
            handle = await client.workflows.submit(prepared)
            result = await handle.result(timeout=110)
            assert result=={'page_count':4,'summary':{'characters':4*len('local network I/O'),'pages':4}},result
            record('examples',dict(workflow_id=handle.id,concurrency=1,output=result,
                controller_source='examples/checkpoint-workflow/controller/program.py',child_source='examples/checkpoint-workflow/child/program.py'))
        await asyncio.to_thread(worker.stop)

    if 'resume' in names:
        proxy = d.proxy()
        worker = d.start_worker(server=proxy.url, concurrency=1)
        gate = d.directory/'child-resume-gate'
        d.gates.add(gate)
        async with AsyncClient(proxy.url, **options) as client:
            handle = await submit(client, 'resume', child_gate=str(gate))
            await asyncio.to_thread(eventually, lambda: records(d,'resume','child'), description='distributed child under N=1')
            waiting = await asyncio.to_thread(snapshot,d,handle.id)
            assert waiting['state']=='waiting' and waiting['activation_id'] is None, waiting
            # A running child under N=1 proves the suspended controller released
            # the shared consumer/process capacity before child execution.
            count = len(proxy.commands('/v1/workflows'))
            try:
                await handle.result(timeout=0.05)
                raise AssertionError('blocked workflow result must time out locally')
            except WaitTimeout:
                pass
            assert len(proxy.commands('/v1/workflows')) == count == 1
            await asyncio.to_thread(d.server.kill)
            d.server, _ = await asyncio.to_thread(d.start_server)
            assert (await asyncio.to_thread(snapshot,d,handle.id))['state'] == 'waiting'
            gate.touch()
            result = await handle.result(timeout=110)
            assert result['count']==4 and result['total']==6 and result['overlap'], result
            assert result['child']['workflow']==handle.id, result
            assert len(records(d,'resume','local_started'))==4
            assert len(records(d,'resume','activation'))==2
            assert len(proxy.commands('/v1/workflows'))==1
            record('resume',dict(workflow_id=handle.id,concurrency=1,orchestrator_restart=True,local_timeout_no_resubmit=True))
        await asyncio.to_thread(worker.stop)

    if 'lost-ack' in names:
        proxy = d.proxy()
        lost = proxy.lose_once('/v1/workflows/local-results', lambda body: body['record']['key']=='fetch-0')
        worker = d.start_worker(server=proxy.url, concurrency=1)
        async with AsyncClient(proxy.url, **options) as client:
            handle = await submit(client,'lost-ack',mode='local',count=1)
            result = await handle.result(timeout=110)
            assert result['count']==1 and lost.is_set(), result
            calls = proxy.commands('/v1/workflows/local-results',lambda body: body['record']['key']=='fetch-0')
            assert len(calls)>=2 and all(call['body']==calls[0]['body'] for call in calls), calls
            assert len(records(d,'lost-ack','local_started'))==1
            record('lost-ack',dict(workflow_id=handle.id,local_execution_count=1,commit_requests=len(calls)))
        await asyncio.to_thread(worker.stop)

    if 'depth' in names:
        proxy = d.proxy()
        worker = d.start_worker(server=proxy.url, concurrency=1)
        async with AsyncClient(proxy.url, **options) as client:
            handle = await submit(client,'depth',mode='depth',count=1)
            result = await handle.result(timeout=110)
            for _ in range(64):
                assert type(result) is list and len(result)==1
                result = result[0]
            assert type(result) is int and result==0
            calls = proxy.commands('/v1/workflows/local-results', lambda body: body['record']['key']=='depth')
            assert len(calls)==1
            record('depth',dict(workflow_id=handle.id,application_output_depth=64,local_commit_requests=1))
        await asyncio.to_thread(worker.stop)

    if 'crash' in names:
        gate = d.directory/'controller-crash-gate'
        d.gates.add(gate)
        worker = d.start_worker(concurrency=1)
        async with AsyncClient(d.server_url, **options) as client:
            handle = await submit(client,'crash',mode='local',count=1,controller_gate=str(gate))
            ack = await asyncio.to_thread(eventually,lambda: records(d,'crash','after_ack'),description='local durable ACK before controller decision')
            first = ack[0]
            await asyncio.to_thread(worker.kill)
            # This PID came from the unique test package's post-ACK marker;
            # stop the owned orphan too before starting a replacement worker.
            with contextlib.suppress(ProcessLookupError):
                os.kill(first['pid'],signal.SIGKILL)
            replacement = d.start_worker(concurrency=1)
            result = await handle.result(timeout=130)
            assert result['count']==1
            assert len(records(d,'crash','local_started'))==1
            activations = records(d,'crash','activation')
            assert len(activations)==2 and [row['attempt'] for row in activations]==[1,2], activations
            assert {row['activation'] for row in activations}=={first['activation']}
            record('crash',dict(workflow_id=handle.id,attempts=2,local_execution_count=1,lease_expiry='production 60 seconds'))
        await asyncio.to_thread(replacement.stop)

    if 'events' in names:
        proxy = d.proxy()
        lost = proxy.lose_once('/v1/workflows/events')
        async with AsyncClient(proxy.url, **options) as client:
            handle = await submit(client, 'early-event', mode='event', wait_key='approval:1', retry_on_wake=True)
            event = dict(specversion='1.0', id='evt-approval-1', source='urn:workflow-acceptance',
                type='approval.granted.v1', datacontenttype='application/json',
                traceparent='00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00',
                tracestate='original=event', businessid='INV-1042',
                data={'approved':True,'values':[None,1,1.0,-0.0,18446744073709551615]})
            prepared = handle.prepare_event('approval:1', event=event)
            try:
                await handle.send_event(prepared)
                raise AssertionError('lost event ACK must expose an uncertain outcome')
            except WorkflowEventUncertain as uncertain:
                assert uncertain.command is prepared
                assert len(proxy.commands('/v1/workflows/events'))==1, 'event was automatically retried'
                accepted = await handle.send_event(uncertain.command)
            assert lost.is_set(), 'event acceptance reply was not lost'
            calls = proxy.commands('/v1/workflows/events')
            assert len(calls)>=2 and all(call['body']==calls[0]['body'] for call in calls), calls
            assert accepted.already_accepted, 'lost ACK did not reconcile the original event'
            assert not records(d,'early-event','activation'), 'early event test started a worker too soon'
            if capture:
                await asyncio.to_thread(eventually, lambda: len([span for span in trace_rows(capture)
                    if span['name']=='ledgence.workflow.event.accept'
                    and span['attributes'].get('ledgence.workflow.id')==handle.id]) >= 2,
                    description='committed event acceptance span export before restart')
            await asyncio.to_thread(d.server.kill)
            d.server,_ = await asyncio.to_thread(d.start_server)
            worker = d.start_worker(server=proxy.url, concurrency=1)
            result = await handle.result(timeout=110)
            expected = dict(kind='event', key='approval:1', event=event, accepted_at=accepted.accepted_at)
            assert json.dumps(result['wake'],sort_keys=True)==json.dumps(expected,sort_keys=True), result
            assert result['state']=={'checkpoint':'early-event'} and result['inputs']=={}, result
            wakes = records(d,'early-event','wake')
            assert len(wakes)==2 and [row['attempt'] for row in wakes]==[1,2], wakes
            assert wakes[0]['activation']==wakes[1]['activation']
            assert all(json.dumps(row['wake'],sort_keys=True)==json.dumps(expected,sort_keys=True) for row in wakes), wakes
            reconciled = await handle.send_event(prepared)
            assert reconciled.already_accepted and reconciled.accepted_at==accepted.accepted_at
            if capture:
                await asyncio.to_thread(eventually, lambda: len([span for span in trace_rows(capture)
                    if span['name']=='ledgence.workflow.event.accept'
                    and span['attributes'].get('ledgence.workflow.id')==handle.id]) >= 3,
                    description='event reconciliation span export')
            record('events',dict(workflow_id=handle.id,early_before_worker=True,lost_acceptance_ack=True,
                event_requests=len(proxy.commands('/v1/workflows/events')),resumed_attempts=2,
                frozen_wake_preserved=True,inbox_survived_server_restart=True,receipt_reconciled_after_completion=True))
        await asyncio.to_thread(worker.stop)

    if 'event-boundaries' in names:
        worker = d.start_worker(concurrency=1)
        async with AsyncClient(d.server_url, **options) as client:
            handle = await submit(client,'event-boundaries',mode='event',wait_key='boundary:1',
                summarize_wake=True,retry_on_wake=True)
            event, encoded_event = boundary_event()
            command = dict(scope=d.scope,workflow_id=handle.id,key='boundary:1',event=event)
            rejected = ['2024-02-29 12:00:00Z','2024-01-15T12:00:60Z']
            for invalid_time in rejected:
                invalid = dict(command,event=dict(event,time=invalid_time))
                status,_,error = await asyncio.to_thread(exchange,d.server_url,'POST','/v1/workflows/events',
                    rust_boundary_bytes(invalid))
                assert status==400 and error['code']=='invalid_input',error
            encoded_command = rust_boundary_bytes(command)
            status,_,receipt = await asyncio.to_thread(exchange,d.server_url,'POST','/v1/workflows/events',encoded_command)
            assert status==200 and not receipt['already_accepted'],receipt
            assert receipt['workflow_id']==handle.id and receipt['key']=='boundary:1',receipt
            expected_encoding = json.dumps(event,sort_keys=True,separators=(',', ':'),ensure_ascii=False).encode()
            expected = dict(event_id=event['id'],time=event['time'],value=event['data']['value'],
                padding_bytes=len(event['data']['padding']),event_sha256=hashlib.sha256(expected_encoding).hexdigest(),
                python_encoded_bytes=len(expected_encoding))
            result = await handle.result(timeout=110)
            assert result==expected,result
            wakes = records(d,'event-boundaries','wake')
            assert len(wakes)==2 and [row['attempt'] for row in wakes]==[1,2], 'boundary wake was not retried'
            assert wakes[0]['activation']==wakes[1]['activation']
            for wake in wakes:
                assert json.dumps(wake['wake']['event'],sort_keys=True,separators=(',', ':')).encode()==expected_encoding
            status,_,replayed = await asyncio.to_thread(exchange,d.server_url,'POST','/v1/workflows/events',encoded_command)
            assert status==200 and replayed['already_accepted'] and replayed['accepted_at']==receipt['accepted_at'],replayed
            record('event-boundaries',dict(workflow_id=handle.id,rust_event_bytes=len(encoded_event),
                python_event_bytes=len(expected_encoding),valid_leap_second=event['time'],
                invalid_times_rejected=rejected,resumed_attempts=2,compact_output=result,receipt_reconciled_after_completion=True))
        await asyncio.to_thread(worker.stop)

    if 'timers' in names:
        worker = d.start_worker(concurrency=1)
        async with AsyncClient(d.server_url, **options) as client:
            handle = await submit(client,'timer-restart',mode='timer',wait_key='delay:1',delay_ms=5000)
            await asyncio.to_thread(eventually,lambda: snapshot(d,handle.id)['state']=='waiting',description='persisted timer wait')
            # This ordinary task can execute using the only consumer while the
            # workflow's timer remains asleep. No runtime slot belongs to it.
            task = await client.tasks.submit(program='workflow-io',version='1.0.0',queue=d.queue,
                data=dict(url=delay.url,marker=str(d.directory/'workflow-markers.jsonl'),tag='timer-capacity',index=7),
                idempotency_key='timer-capacity')
            assert (await task.result(timeout=30))['index']==7
            assert (await asyncio.to_thread(snapshot,d,handle.id))['state']=='waiting'
            await asyncio.to_thread(d.server.kill)
            await asyncio.sleep(5.25)
            restart_at = int(time.time()*1000)
            d.server,_ = await asyncio.to_thread(d.start_server)
            result = await handle.result(timeout=110)
            assert result['wake']['kind']=='timer' and result['wake']['key']=='delay:1',result
            assert result['wake']['deadline']<restart_at, 'timer deadline restarted after server recovery'
            assert len(records(d,'timer-restart','activation'))==2
            record('timers',dict(workflow_id=handle.id,concurrency=1,ordinary_task_while_waiting=True,
                server_down_when_due=True,original_deadline=result['wake']['deadline'],restart_at=restart_at,
                controller_activations=2))
        await asyncio.to_thread(worker.stop)

    if 'wait-cancellation' in names:
        worker = d.start_worker(concurrency=1)
        async with AsyncClient(d.server_url, **options) as client:
            handle = await submit(client,'event-timeout',mode='event',wait_key='approval:expired',timeout_ms=0)
            result = await handle.result(timeout=30)
            assert result['wake']['kind']=='timeout' and result['wake']['key']=='approval:expired',result
            def external(workflow_id,key,identifier):
                return dict(scope=d.scope,workflow_id=workflow_id,key=key,event=dict(
                    specversion='1.0',id=identifier,source='urn:workflow-acceptance',type='approval.granted.v1',
                    datacontenttype='application/json',data=None))
            status,_,error = await asyncio.to_thread(exchange,d.server_url,'POST','/v1/workflows/events',
                external(handle.id,'approval:expired','evt-too-late'))
            assert status==409 and error['code']=='obsolete_operation',error
            cancelled = []
            for mode in ('event','timer'):
                tag = 'cancel-'+mode
                extra = dict(timeout_ms=None) if mode=='event' else dict(delay_ms=3000)
                waiting = await submit(client,tag,mode=mode,wait_key='cancel:1',**extra)
                await asyncio.to_thread(eventually,lambda: snapshot(d,waiting.id)['state']=='waiting',description='cancellable durable wait')
                await waiting.cancel()
                await asyncio.to_thread(eventually,lambda: snapshot(d,waiting.id)['state']=='cancelled',description='cancelled durable wait')
                if mode=='event':
                    status,_,error = await asyncio.to_thread(exchange,d.server_url,'POST','/v1/workflows/events',
                        external(waiting.id,'cancel:1','evt-after-cancel'))
                    assert status==409 and error['code']=='obsolete_operation',error
                cancelled.append((tag,waiting.id))
            await asyncio.sleep(3.25)
            for tag,workflow_id in cancelled:
                assert (await asyncio.to_thread(snapshot,d,workflow_id))['state']=='cancelled'
                assert len(records(d,tag,'activation'))==1 and not records(d,tag,'wake')
            record('wait-cancellation',dict(timed_out_workflow=handle.id,late_event_rejected=True,
                cancelled_event_and_timer=[identifier for _,identifier in cancelled],no_resurrection=True))
        await asyncio.to_thread(worker.stop)

    if 'placement' in names:
        worker = d.start_worker(concurrency=1)
        samples = []
        async with AsyncClient(d.server_url, **options) as client:
            for iteration in range(placement_iterations):
                modes = ('local','distributed') if iteration % 2 == 0 else ('distributed','local')
                for mode in modes:
                    tag = f'placement-{iteration}-{mode}'
                    before_lsn = await asyncio.to_thread(d.sql, 'SELECT pg_current_wal_insert_lsn()::text')
                    started = time.monotonic()
                    handle = await submit(client,tag,mode=mode,count=100)
                    result = await handle.result(timeout=180)
                    elapsed = time.monotonic()-started
                    quote = lambda value: "'"+value.replace("'","''")+"'"
                    wal_bytes = int(await asyncio.to_thread(d.sql,
                        f'SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),{quote(before_lsn)})::bigint'))
                    workflow = quote(handle.id)
                    counts = json.loads(await asyncio.to_thread(d.sql,
                        "SELECT json_build_object('tasks',count(*),'attempts',sum(attempt_count),"
                        f"'workflow_committed_elapsed_ms',(SELECT terminal_at_ms-submitted_at_ms FROM workflow_runs WHERE workflow_id={workflow}),"
                        "'controller_tasks',count(*) FILTER (WHERE workflow_activation_id IS NOT NULL),"
                        "'local_records',(SELECT count(*) FROM workflow_local_results r JOIN workflow_activations a USING(activation_id) "
                        f"WHERE a.workflow_id={workflow}),'history_rows',(SELECT count(*) FROM workflow_history WHERE workflow_id={workflow})) "
                        f"FROM tasks WHERE workflow_id={workflow}"))
                    assert result['count']==100 and result['total']==4950,result
                    local_count = len(records(d,tag,'local_started'))
                    child_count = len(records(d,tag,'child'))
                    assert (local_count,child_count)==((100,0) if mode=='local' else (0,100))
                    assert counts['local_records']==local_count and counts['tasks']==child_count+counts['controller_tasks'],counts
                    samples.append(dict(iteration=iteration+1,mode=mode,items=100,elapsed_seconds=round(elapsed,6),
                        input_payload_bytes=len(json.dumps(data(tag,mode=mode,count=100),separators=(',',':')).encode()),
                        local_executions=local_count,distributed_tasks=child_count,database_counts=counts,
                        controller_invocations=len(records(d,tag,'activation')),server_wal_delta_bytes=wal_bytes))
        record('placement',dict(concurrency=1,iterations=placement_iterations,io_delay_seconds=0.03,result_poll_interval_seconds=1.0,
            response_payload_bytes=len(b'local network I/O'),distributed_batch_size=50,samples=samples,
            interpretation='Paired end-to-end functional measurements: local I/O overlaps, distributed tasks execute serially at N=1. elapsed_seconds includes 1-second client observation polling; workflow_committed_elapsed_ms uses server submission/terminal timestamps. Server-wide WAL deltas may include other databases and maintenance; these are not isolated workflow write costs or production throughput/tail benchmarks.'))
        await asyncio.to_thread(worker.stop)


def trace_rows(capture):
    return [json.loads(line) for line in capture.stdout_path.read_text().splitlines(keepends=True)
            if line.endswith('\n')]


def verify_event_traces(d, capture, results):
    detail = next(row['detail'] for row in results if row['scenario']=='events')
    workflow_id = detail['workflow_id']
    wake = records(d,'early-event','wake')[0]['wake']
    origin = wake['event']['traceparent'].split('-')
    original = (origin[1],origin[2])
    rows = trace_rows(capture)
    accepts = [span for span in rows if span['name']=='ledgence.workflow.event.accept'
        and span['attributes'].get('ledgence.workflow.id')==workflow_id]
    activations = [span for span in rows if span['name']=='ledgence.workflow.activation'
        and span['attributes'].get('ledgence.workflow.id')==workflow_id
        and span['attributes'].get('ledgence.workflow.wake')=='event']
    assert len(accepts)>=3, 'missing actual event acceptance and reconciliation spans'
    assert len(activations)==2, 'each resumed attempt must export a causal activation span'
    for span in accepts+activations:
        assert [(link['trace_id'],link['span_id']) for link in span['links']]==[original],span
        assert span['attributes']['cloudevents.event_id']==wake['event']['id'],span
        assert span['attributes']['cloudevents.event_source']==wake['event']['source'],span
        assert span['trace_id']!=original[0], 'the external event must be linked, not become the processing parent'
    for accepted in accepts:
        parent = [span for span in rows if (span['trace_id'],span['span_id'])==
            (accepted['trace_id'],accepted['parent_span_id'])]
        assert len(parent)==1 and parent[0]['attributes'].get('http.route')=='/v1/workflows/events',parent
    inspected = []
    for activation in activations:
        parents = [span for span in rows if (span['trace_id'],span['span_id'])==
            (activation['trace_id'],activation['parent_span_id'])]
        assert len(parents)==1 and parents[0]['name']=='ledgence.attempt.process',parents
        parent = parents[0]
        task_id = parent['attributes']['ledgence.task.id']
        attempt_id = parent['attributes']['ledgence.attempt.id']
        query = urllib.parse.urlencode(dict(d.scope,task_id=task_id,attempt_id=attempt_id))
        status,_,attempt = exchange(d.server_url,'GET','/v1/attempts/inspect?'+query)
        assert status==200,attempt
        processing = attempt['settlement']['command']['processing_trace']['traceparent'].split('-')
        assert (processing[1],processing[2])==(parent['trace_id'],parent['span_id']),attempt
        creation = attempt['event']['traceparent'].split('-')
        assert (creation[1],creation[2])==(parent['trace_id'],parent['parent_span_id']),attempt
        inspected.append(dict(task_id=task_id,attempt_id=attempt_id,processing_span=parent['span_id'],
            activation_span=activation['span_id']))
    return dict(workflow_id=workflow_id,acceptance_spans=len(accepts),resumed_activation_spans=len(activations),
        original_event_trace=original[0],original_event_span=original[1],processing_trace_unchanged=True,
        inspected_attempts=inspected,decoded_spans=len(rows),capture_file=capture.stdout_path.name)


def artifact_metadata(root, binaries, python, d):
    """Capture provenance before timed work; never infer production performance."""
    def command(args):
        try:
            result = subprocess.run(args,cwd=root,capture_output=True,text=True,timeout=10)
            return result.stdout.strip()[:4096] if result.returncode==0 else None
        except (OSError,subprocess.SubprocessError):
            return None
    def sha256(path):
        digest = hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        return digest.hexdigest()
    sources = set(root.glob('crates/*/src/**/*.rs'))
    sources.update(root.glob('crates/*/Cargo.toml'))
    sources.update(root.glob('crates/*/migrations/*.sql'))
    sources.update(root.glob('crates/*/queries/*.sql'))
    sources.update(root.glob('rust-toolchain.toml'))
    sources.update(root.glob('sdk/python/ledgence_worker/*.py'))
    sources.update(root.glob('sdk/python-client/src/**/*.py'))
    sources.update(root.glob('examples/checkpoint-workflow/**/*.py'))
    sources.update(root.glob('tools/http_acceptance/*.py'))
    sources.update(root/name for name in ('Cargo.toml','Cargo.lock','tools/check-workflows.py','tools/check-sqs.py'))
    fingerprints = {str(path.relative_to(root)):sha256(path) for path in sorted(sources)}
    encoded = json.dumps(fingerprints,sort_keys=True,separators=(',',':')).encode()
    (d.directory/'source-sha256.json').write_text(json.dumps(fingerprints,indent=2)+'\n')
    dirty = command(['git','status','--porcelain=v1','--untracked-files=normal'])
    memory = None
    if sys.platform=='darwin':
        raw = command(['sysctl','-n','hw.memsize'])
        memory = int(raw) if raw and raw.isdecimal() else None
        processor = command(['sysctl','-n','machdep.cpu.brand_string'])
        hardware = command(['sysctl','-n','hw.model'])
    else:
        processor, hardware = platform.processor(), None
        with contextlib.suppress(OSError,ValueError,AttributeError):
            memory = os.sysconf('SC_PAGE_SIZE')*os.sysconf('SC_PHYS_PAGES')
    return dict(
        source=dict(git_head=command(['git','rev-parse','HEAD']),git_branch=command(['git','branch','--show-current']),
            working_tree_dirty=bool(dirty) if dirty is not None else None,
            aggregate_sha256=hashlib.sha256(encoded).hexdigest(),file_hashes='source-sha256.json'),
        binaries=dict(directory=str(binaries),profile_hint=binaries.name if binaries.name in ('debug','release') else 'custom',
            profile_hint_basis='directory name only; compiler options are not inferred',
            sha256={name:sha256(binaries/name) for name in ('ledgence','ledgence-worker','ledgence-orchestrator')}),
        hardware=dict(os=platform.platform(),machine=platform.machine(),model=hardware,processor=processor,
            logical_cpus=os.cpu_count(),memory_bytes=memory),
        software=dict(rustc=command(['rustc','--version']),cargo=command(['cargo','--version']),
            worker_python=command([python,'--version']),worker_python_path=python,
            client_python=sys.version,client_python_path=sys.executable,postgres=d.sql('SHOW server_version')))


def self_test(root):
    sys.path.insert(0,str(root/'sdk/python'))
    controller = (root/'examples/checkpoint-workflow/controller/program.py').read_text()
    namespace = {'__name__': 'workflow_acceptance_example'}
    exec(compile(controller,'example.py','exec'),namespace)
    compile(FIXTURE,'workflow_fixture.py','exec')
    boundary_event()
    compile(CHILD,'child_fixture.py','exec')
    compile((root/'examples/checkpoint-workflow/child/program.py').read_text(),'example_child.py','exec')
    delay = DelayServer()
    async def fetch_all():
        return await asyncio.gather(*(namespace['fetch'](delay.url) for _ in range(4)))
    try:
        assert asyncio.run(fetch_all()) == ['local network I/O']*4
        assert delay.peak > 1, 'example I/O did not overlap'
    finally:
        delay.close()
    print('Workflow harness self-test passed: compiled fixtures and overlapping real loopback HTTP I/O')
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--self-test',action='store_true',help='validate fixtures and async I/O without PostgreSQL')
    parser.add_argument('--endpoint',help='explicit owned loopback ElasticMQ URL; omit for integrated delivery')
    parser.add_argument('--region',default='us-east-1')
    parser.add_argument('--psql',default='psql')
    parser.add_argument('--binaries',type=Path)
    parser.add_argument('--evidence',type=Path)
    parser.add_argument('--capture',type=Path,help='optional OTLP capture executable; requires the events scenario')
    parser.add_argument('--placement-iterations',type=int,default=3,help='paired local/distributed timing iterations (1..10; default 3)')
    parser.add_argument('--scenario',action='append',choices=['examples','resume','lost-ack','depth','crash','events','event-boundaries','timers','wait-cancellation','placement'])
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    if args.self_test:
        return self_test(root)
    if args.capture and (not args.capture.is_file() or (args.scenario and 'events' not in args.scenario)):
        parser.error('--capture requires an existing executable and the events scenario')
    if not 1 <= args.placement_iterations <= 10:
        parser.error('--placement-iterations must be 1..10')
    if args.endpoint:
        endpoint = urllib.parse.urlsplit(args.endpoint)
        try:
            loopback = endpoint.hostname=='localhost' or ipaddress.ip_address(endpoint.hostname or '').is_loopback
        except ValueError:
            loopback = False
        if endpoint.scheme not in ('http','https') or not loopback or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment:
            parser.error('--endpoint must be a loopback HTTP(S) URL without credentials/query/fragment')
    parent_url = os.environ.get('LEDGENCE_POSTGRES_URL')
    if not parent_url:
        parser.error('LEDGENCE_POSTGRES_URL must name an owned disposable PostgreSQL server')
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0,str(root/'sdk/python-client/src'))
    python = os.environ.get('LEDGENCE_PYTHON',sys.executable)
    binaries = args.binaries
    if binaries is None:
        command = ['cargo','build','--workspace','--bins','--locked']
        if args.endpoint:
            command.append('--all-features')
        subprocess.run(command,cwd=root,check=True)
        metadata = json.loads(subprocess.check_output(['cargo','metadata','--no-deps','--format-version','1','--locked'],cwd=root))
        binaries = Path(metadata['target_directory'])/'debug'
    binaries = binaries.resolve()
    for binary in ('ledgence','ledgence-worker','ledgence-orchestrator'):
        if not (binaries/binary).is_file():
            parser.error(f'missing executable {binaries/binary}')
    temporary = not args.evidence
    directory = args.evidence.resolve() if args.evidence else Path(tempfile.mkdtemp(prefix='ledgence-workflows-'))
    if args.evidence:
        directory.mkdir(parents=True,exist_ok=False)
    database = 'ledgence_workflow_'+uuid.uuid4().hex
    database_url = urllib.parse.urlunsplit(urllib.parse.urlsplit(parent_url)._replace(path='/'+database))
    deployment = delay = queue_admin = queue_url = capture = None
    queue_name = 'ledgence-test-workflow-'+uuid.uuid4().hex
    created = queue_started = succeeded = False
    results = []
    def admin(statement):
        result = subprocess.run([args.psql,'--dbname',parent_url,'-X','--set','ON_ERROR_STOP=1','--command',statement],capture_output=True,timeout=40)
        if result.returncode:
            raise RuntimeError('owned workflow test database administration failed')
    def record(name,detail):
        results.append(dict(scenario=name,result='passed',detail=detail))
        (directory/'results.json').write_text(json.dumps(results,indent=2)+'\n')
        print(f'PASS {name}: {json.dumps(detail)}',flush=True)
    try:
        if args.endpoint:
            QueueAdmin = runpy.run_path(str(root/'tools/check-sqs.py'))['QueueAdmin']
            queue_admin = QueueAdmin(args.endpoint,args.region,'aws')
            queue_started = True
            queue_url = queue_admin.queue_url(queue_admin.call('CreateQueue',{'QueueName':queue_name,'Attributes':{'DelaySeconds':'0','VisibilityTimeout':'60','MessageRetentionPeriod':'3600'}}))
        admin(f'CREATE DATABASE "{database}"')
        created = True
        cls = SqsDeployment if args.endpoint else Deployment
        extra = dict(queue_url=queue_url,endpoint=args.endpoint,region=args.region) if args.endpoint else {}
        deployment = cls(root,directory,binaries,python,database_url,args.psql,**extra)
        if args.capture:
            capture = Process([str(args.capture.resolve()), '127.0.0.1:0'], directory, 'workflow-capture', dict(os.environ))
            line = eventually(lambda: next((line for line in capture.stderr_path.read_text().splitlines()
                if line.startswith('OTLP_CAPTURE_ENDPOINT=')), None), description='workflow trace capture readiness')
            deployment.environment.update(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=line.split('=',1)[1],
                OTEL_SDK_DISABLED='false',OTEL_TRACES_SAMPLER='parentbased_always_on',OTEL_TRACES_SAMPLER_ARG='1')
        # Exercise the public example's exact async HTTP helper in the fixture.
        controller = (root/'examples/checkpoint-workflow/controller/program.py').read_text()
        fetch = controller[:controller.index('\n\nasync def handle(event):')]
        packages = {
            'workflow-controller':publish(deployment,'workflow-controller',fetch+'\n'+FIXTURE),
            'workflow-io':publish(deployment,'workflow-io',CHILD),
            'workflow-pages':publish(deployment,'workflow-pages',controller),
            'workflow-summary':publish(deployment,'workflow-summary',(root/'examples/checkpoint-workflow/child/program.py').read_text()),
        }
        migration = subprocess.run([str(binaries/'ledgence-orchestrator'),'migrate'],env=deployment.environment,capture_output=True,timeout=40)
        assert migration.returncode==0,migration.stderr.decode(errors='replace')[-3000:]
        deployment.server,_ = deployment.start_server()
        delay = DelayServer()
        provenance = artifact_metadata(root,binaries,python,deployment)
        provenance.update(mode='elasticmq' if args.endpoint else 'integrated',database=database,queue_url=queue_url,real_aws=False,published_programs=packages)
        (directory/'resources.json').write_text(json.dumps(provenance,indent=2)+'\n')
        asyncio.run(scenarios(deployment,delay,args.scenario or ['examples','resume','lost-ack','depth','crash','events','event-boundaries','timers','wait-cancellation','placement'],record,args.placement_iterations,capture))
        if capture:
            # Workers have drained their exporters, but the server must remain
            # available while durable attempt snapshots are checked.
            eventually(lambda: len([span for span in trace_rows(capture)
                if span['name']=='ledgence.workflow.event.accept']) >= 3,
                description='final event reconciliation span export')
            record('event-traces',verify_event_traces(deployment,capture,results))
        deployment.server.stop()
        succeeded = True
        print(f'Workflow acceptance passed: {len(results)} scenarios; evidence {directory}',flush=True)
        return 0
    except Exception as error:
        print(f'Workflow acceptance failed: {error}\nEvidence: {directory}',file=sys.stderr)
        (directory/'failure.txt').write_text(traceback.format_exc())
        return 1
    finally:
        try:
            if deployment:
                deployment.close()
            if delay:
                delay.close()
            if capture:
                capture.cleanup()
        finally:
            try:
                if created:
                    admin(f'DROP DATABASE "{database}" WITH (FORCE)')
            finally:
                if queue_started:
                    if queue_url is None:
                        queue_url = queue_admin.queue_url(queue_admin.call('GetQueueUrl',{'QueueName':queue_name}))
                    queue_admin.call('DeleteQueue',{'QueueUrl':queue_url})
                if temporary and succeeded:
                    shutil.rmtree(directory)


if __name__=='__main__':
    sys.exit(main())
