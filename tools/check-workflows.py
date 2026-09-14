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

from http_acceptance.harness import Deployment, eventually, exchange
from http_acceptance.sqs import SqsDeployment


FIXTURE = r'''
import asyncio, json, os, time
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


async def scenarios(d, delay, names, record, placement_iterations=3):
    from ledgence.client import AsyncClient, RetryPolicy, WaitTimeout
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
    parser.add_argument('--placement-iterations',type=int,default=3,help='paired local/distributed timing iterations (1..10; default 3)')
    parser.add_argument('--scenario',action='append',choices=['examples','resume','lost-ack','depth','crash','placement'])
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    if args.self_test:
        return self_test(root)
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
    deployment = delay = queue_admin = queue_url = None
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
        asyncio.run(scenarios(deployment,delay,args.scenario or ['examples','resume','lost-ack','depth','crash','placement'],record,args.placement_iterations))
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
