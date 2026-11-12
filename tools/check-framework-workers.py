#!/usr/bin/env python3
"""Qualify optional SDK profiles through real workers, HTTP and PostgreSQL.

Requires a successful fresh SDK environment from check-framework-integrations.py.
Only owned test databases/processes/files are changed. SDK calls are deterministic
fixtures; network and child-process calls inside programs are forbidden. This
qualifies host packages, not another platform's wheels or live model providers.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import threading
import traceback
import urllib.parse
import uuid

from http_acceptance.harness import Deployment, FaultProxy, eventually, exchange
from instance_access.harness import AccessDeployment
from postgres_fixture import create_owned_database, owned_database_url
from program_fixture import prepare_package

ROOT = Path(__file__).resolve().parents[1]
MODULES = {"pydantic_ai": "pydantic", "google_adk": "adk", "claude_agent_sdk": "claude"}
LOCAL_RESULTS = "/v1/workflows/local-results"

WRAPPER = '''import asyncio,json,os,sys
from pathlib import Path
from ledgence.worker.workflow import workflow_context
from ledgence.worker.integrations import AgentIntegrationError

blocked=[]
def audit(event,args):
    if event in ('socket.connect','socket.getaddrinfo','socket.gethostbyname','socket.gethostbyaddr','socket.sendto','socket.sendmsg','socket.getnameinfo') or event.startswith('subprocess.') or event in ('os.system','os.fork','os.forkpty','os.posix_spawn','os.exec','os.spawn'):
        blocked.append(event)
        raise RuntimeError('offline SDK program forbids network/process access')
sys.addaudithook(audit)
from fixture import handle as run_agent

def mark(path,kind,**fields):
    with open(path,'a',encoding='utf-8') as output:
        output.write(json.dumps(dict(kind=kind,pid=os.getpid(),**fields))+'\\n')

async def handle(event):
    ctx=workflow_context()
    data=event['data']
    mark(data['marker'],'activation',activation=ctx.activation_id,attempt=event['ldgattemptno'],attempt_id=event['ldgattemptid'])
    try:
        result=await run_agent(event)
    except AgentIntegrationError:
        if blocked or not data.get('fail_model'): raise
        mark(data['marker'],'handled_failure')
        return ctx.complete({'handled':True,'pid':os.getpid()})
    if blocked: raise RuntimeError('forbidden IO was attempted')
    mark(data['marker'],'adapter_returned',result=result)
    if data.get('fail_after') and event['ldgattemptno']==1:
        raise RuntimeError('fixture failure after all adapter records accepted')
    if data.get('after_gate'):
        while not Path(data['after_gate']).exists(): await asyncio.sleep(0.01)
    return ctx.complete(result)
'''


class SerialCommitProxy(FaultProxy):
    """Serialize only local-result commits to make the lost prefix deterministic."""
    def __init__(self, upstream):
        super().__init__(upstream)
        original = self.server.RequestHandlerClass
        serial = threading.Lock()
        class Handler(original):
            def do_POST(self):
                if self.path == LOCAL_RESULTS:
                    with serial: self.handle_request()
                else: self.handle_request()
        self.server.RequestHandlerClass = Handler


def records(path):
    if not path.exists(): return []
    return [json.loads(line) for line in path.read_text().splitlines(keepends=True) if line.endswith('\n')]


def counts(rows):
    return {kind: sum(row['kind'] == kind for row in rows) for kind in ('model', 'tool', 'factory', 'query')}


def quote(value): return "'" + value.replace("'", "''") + "'"


def journal(deployment, activation):
    return json.loads(deployment.sql("SELECT coalesce(json_agg(convert_from(record_bytes,'UTF8')::json ORDER BY step_key),'[]'::json) FROM workflow_local_results WHERE activation_id=" + quote(activation)))


def result(deployment, workflow):
    query = urllib.parse.urlencode(dict(deployment.scope, workflow_id=workflow))
    status, _, body = exchange(deployment.server_url, 'GET', '/v1/workflows/result?' + query)
    assert status == 200
    return body


def terminal(deployment, workflow, expected='succeeded'):
    body = eventually(lambda: (value if (value := result(deployment, workflow))['workflow']['state'] in ('succeeded', 'failed', 'cancelled') else None), timeout=150, description='framework workflow terminal result')
    assert body['workflow']['state'] == expected, body
    return body


def prepare(environment, python, directory, framework):
    probe = "import json,sysconfig;print(json.dumps(sysconfig.get_paths()['purelib']))"
    site = Path(json.loads(subprocess.check_output([python, '-I', '-c', probe], text=True)))
    assert site.is_relative_to(environment)
    package = prepare_package(directory, 'framework-' + framework.replace('_', '-'), '1.0.0', WRAPPER,
                              python=python, protocol=3)
    shutil.copytree(site, package, dirs_exist_ok=True,
        ignore=shutil.ignore_patterns('__pycache__', '*.pyc', 'pip', 'pip-*.dist-info', 'setuptools', 'setuptools-*.dist-info', '_distutils_hack', 'distutils-precedence.pth'))
    assert not (package / 'ledgence').exists(), 'SDK environment must not supply Ledgence'
    shutil.copy2(ROOT / 'sdk/python/tests/framework_fixtures' / (MODULES[framework] + '_program.py'), package / 'fixture.py')
    return package


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--framework', required=True, choices=MODULES)
    parser.add_argument('--sdk-evidence', required=True, type=Path)
    parser.add_argument('--binaries', required=True, type=Path)
    parser.add_argument('--runner', required=True, type=Path)
    parser.add_argument('--psql', default='psql')
    parser.add_argument('--evidence', required=True, type=Path)
    args = parser.parse_args()
    parent = os.environ.get('LEDGENCE_POSTGRES_URL')
    if not parent: parser.error('LEDGENCE_POSTGRES_URL must name a disposable test server')
    qualification = json.loads((args.sdk_evidence / 'summary.json').read_text())
    if not qualification['passed'] or qualification['framework'] != args.framework:
        parser.error('successful matching installed SDK qualification is required')
    environment = (args.sdk_evidence / 'runtime-env').resolve()
    python = str(environment / 'bin/python')
    args.evidence.mkdir(parents=True, exist_ok=False)
    directory = args.evidence.resolve(); directory.chmod(0o700)
    runner = args.runner.resolve()
    helper_root = runner.parents[2]
    def helper_hashes():
        return {str(path.relative_to(helper_root)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted((helper_root/'ledgence/worker').rglob('*.py'))}
    helper_before = helper_hashes()
    if qualification.get('helper_sha256') != helper_before:
        parser.error('SDK boundary qualification belongs to a different helper')
    def sdk_inventory():
        return subprocess.check_output([python, '-I', '-B', ROOT/'tools/framework_integrations/inventory.py'], timeout=60)
    inventory_before = sdk_inventory()
    inventory_digest = hashlib.sha256(inventory_before).hexdigest()
    if inventory_digest != qualification.get('installed_inventory_sha256'):
        parser.error('installed SDK bytes differ from boundary qualification')
    inventory = json.loads(inventory_before)
    binary = args.binaries.resolve() / 'ledgence'
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    db = 'ledgence_framework_' + uuid.uuid4().hex
    deployment = None
    checks = []
    passed = False
    program_digest = None
    cleanup = True
    failure = None
    active_case = {}
    def admin(statement):
        process = subprocess.run([args.psql, '--dbname', parent, '-X', '--set', 'ON_ERROR_STOP=1', '--command', statement], capture_output=True, timeout=40)
        if process.returncode: raise RuntimeError('owned database command failed')
    try:
        create_owned_database(admin, db)
        deployment = AccessDeployment(ROOT, directory, args.binaries.resolve(), python, owned_database_url(parent, db), args.psql)
        d = deployment
        d.command('ledgence', ['orchestrator', 'migrate'])
        d.start_server()
        package = prepare(environment, python, directory / 'prepared', args.framework)
        publication = d.command('ledgence', ['program', 'publish', '--source', str(package), '--server', d.server_url, '--register', '--kind', 'workflow'])
        assert publication['phase'] == 'registered'
        program_digest = publication['descriptor']['digest']
        program = publication['descriptor']['program']['id']
        checks.append({'case': 'prepared-sdk-package-published-registered', 'passed': True,
                       'files': sum(p.is_file() for p in package.rglob('*'))})
        def worker(server=None):
            return Deployment.start_worker(d, server=server, runner=args.runner.resolve())
        def submit(tag, **data):
            marker = directory / (tag + '.jsonl')
            active_case.clear(); active_case.update(case=tag, marker=marker)
            body = d.submission(tag, program=program, **data)
            body['input']['data']['marker'] = str(marker)
            body['input']['retry_policy']['max_attempts'] = 1 if data.get('nested_control') else 3
            status, _, reply = exchange(d.server_url, 'POST', '/v1/workflows', body)
            assert status in (200, 201), reply
            return reply['workflow_id'], marker
        def activation(marker):
            return eventually(lambda: next((r for r in records(marker) if r['kind']=='activation'), None), description='unique fixture activation')
        def kill_runtime(marker):
            first = activation(marker)
            # PID comes from this unique owned artifact/run, never a process-name match.
            os.kill(first['pid'], signal.SIGKILL)
            return first
        def check(tag, marker, expected_models, expected_tools, **extra):
            rows = records(marker); measured = counts(rows)
            assert measured['model'] == expected_models and measured['tool'] == expected_tools, measured
            checks.append(dict(case=tag, passed=True, calls=measured, **extra))
        inner_tools = 2 if args.framework == 'claude_agent_sdk' else 4
        work = worker()
        pids = []
        for tag, extra in (('fresh', {}), ('fresh-again', {}), ('accepted-replay', {'fail_after': True}), ('after-failure', {})):
            workflow, marker = submit(tag, **extra)
            outcome = terminal(d, workflow)
            check(tag, marker, 3, inner_tools)
            rows = records(marker)
            activations = [r for r in rows if r['kind']=='activation']
            assert len(activations) == (2 if extra.get('fail_after') else 1)
            pids.extend(r['pid'] for r in activations)
            outputs = [r['result'] for r in rows if r['kind']=='adapter_returned']
            assert all(output['status'] == 'succeeded' for output in outputs)
            if args.framework == 'google_adk':
                assert all(o['resource']['closed'] and o['resource']['open_task_id']==o['resource']['close_task_id'] for o in outputs)
        assert pids[0]==pids[1]==pids[2] and pids[2]!=pids[3] and pids[3]==pids[4], pids
        checks.append({'case': 'warm-reuse-and-failed-runtime-retirement', 'passed': True, 'invocations': len(pids)})
        workflow, marker = submit('handled-sdk-failure', fail_model=True)
        terminal(d, workflow)
        assert activation(marker)['pid']==pids[-1]
        assert any(row['kind']=='handled_failure' for row in records(marker))
        workflow, marker = submit('after-handled-sdk-failure')
        terminal(d, workflow)
        assert activation(marker)['pid']==pids[-1]
        check('warm-after-handled-sdk-failure', marker, 3, inner_tools)
        if args.framework != 'claude_agent_sdk':
            workflow, marker = submit('nested', nested_control=True)
            terminal(d, workflow, 'failed')
            assert not any(r['kind']=='adapter_returned' for r in records(marker))
            accepted = journal(d, activation(marker)['activation'])
            assert all('forbidden' not in r['key'] for r in accepted)
            assert not any(r['input'].get('kind')=='tool' for r in accepted), accepted
            checks.append({'case': 'caught-nested-control-still-fails', 'passed': True})
        after_gate = directory / 'accepted-release'; d.gates.add(after_gate)
        workflow, marker = submit('crash-accepted', after_gate=str(after_gate))
        eventually(lambda: any(r['kind']=='adapter_returned' for r in records(marker)), timeout=60)
        first = activation(marker)
        accepted_before = journal(d, first['activation'])
        kill_runtime(marker); after_gate.touch()
        terminal(d, workflow)
        check('crash-after-all-records-accepted', marker, 3, inner_tools,
              accepted_records=len(accepted_before))
        gate = directory / 'unaccepted-release'; d.gates.add(gate)
        workflow, marker = submit('crash-unaccepted', release_file=str(gate))
        eventually(lambda: any(r['kind']==('whole_run_ready' if args.framework=='claude_agent_sdk' else 'tool') for r in records(marker)), timeout=60)
        first = activation(marker)
        before = counts(records(marker)); accepted_before = journal(d, first['activation'])
        kill_runtime(marker); gate.touch()
        terminal(d, workflow)
        # Unaccepted tools may already have run; only accepted model records replay.
        check('crash-before-tool-or-run-acceptance', marker,
              6 if args.framework=='claude_agent_sdk' else 3,
              before['tool'] + inner_tools,
              calls_before_crash=before, accepted_records=len(accepted_before), external_effects_exactly_once=False)
        work.stop()
        if args.framework != 'claude_agent_sdk':
            proxy = SerialCommitProxy(d.server_url); d.proxies.append(proxy)
            lost = proxy.lose_once(LOCAL_RESULTS, lambda body: body['record']['input'].get('kind')=='tool', block_after=True)
            work = worker(proxy.url)
            workflow, marker = submit('partial-lost-ack')
            assert lost.wait(60), 'no accepted tool commit observed'
            first = activation(marker)
            # Stop the sender while blocked so it cannot later deliver additional
            # commit retries. Kill its separately owned Python subprocess too.
            work.kill(); kill_runtime(marker)
            accepted = journal(d, first['activation']); before = counts(records(marker))
            accepted_tools = [r for r in accepted if r['input'].get('kind')=='tool']
            assert len(accepted_tools)==1, accepted
            assert all(r['status']==200 for r in proxy.commands(LOCAL_RESULTS))
            replacement = worker()
            terminal(d, workflow)
            after = counts(records(marker))
            assert after['model'] - before['model']==2 and after['tool'] - before['tool']==3, (before, after)
            checks.append({'case': 'accepted-first-model-tool-lost-ack-replacement', 'passed': True,
                           'calls_before_crash': before, 'calls_after': after, 'accepted_records': len(accepted),
                           'replacement_model_calls': 2, 'replacement_tool_calls': 3})
            replacement.stop()
        assert hashlib.sha256(binary.read_bytes()).hexdigest()==digest
        assert helper_hashes()==helper_before, 'helper changed during qualification'
        assert sdk_inventory()==inventory_before, 'SDK bytes changed during qualification'
        passed = True
    except Exception as error:
        failure = {'type': type(error).__name__, 'case': active_case.get('case', 'setup'),
                   'calls': counts(records(active_case['marker'])) if active_case.get('marker') else {}, 'frames': [dict(file=Path(f.filename).name,line=f.lineno,function=f.name) for f in traceback.extract_tb(error.__traceback__)]}
    finally:
        if deployment:
            try: deployment.close()
            except Exception: cleanup=False
        try: admin(f'DROP DATABASE IF EXISTS "{db}" WITH (FORCE)')
        except Exception: cleanup=False
        report = dict(framework=args.framework, passed=passed and cleanup, cleanup_complete=cleanup,
                      binary_sha256=digest, program_artifact_digest=program_digest, scenarios=checks, failure=failure,
                      helper_sha256=helper_before, helper_sha256_after=helper_hashes(),
                      installed_inventory_sha256=inventory_digest, installed_distributions=inventory['packages'],
                      source_sha=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
                      source_dirty=bool(subprocess.check_output(['git','status','--porcelain'],cwd=ROOT)),
                      python=subprocess.check_output([python,'-I','-c','import platform;print(platform.python_version())'],text=True).strip(),
                      provider_calls='not_exercised', native_cli='not_exercised')
        (directory/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps(report))
    return 0 if passed and cleanup else 1


if __name__=='__main__': sys.exit(main())
