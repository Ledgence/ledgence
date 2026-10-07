#!/usr/bin/env python3
"""Qualify a pulled distribution in owned Compose projects outside the checkout."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent / 'release'))
from local_distribution import verify_directory


class Acceptance:
    def __init__(self, directory, evidence, cli=None):
        self.evidence = evidence
        evidence.mkdir(parents=True, exist_ok=False)
        self.manifest = verify_directory(directory)
        self.kit = evidence / 'kit'
        shutil.copytree(directory, self.kit)
        self.manifest = verify_directory(self.kit)
        self.cwd = evidence / 'unrelated-working-directory'
        self.cwd.mkdir()
        self.cli = cli
        self.env = {key: value for key, value in os.environ.items() if not key.startswith('COMPOSE_')}
        self.env['LEDGENCE_CONCURRENCY'] = '1'
        self.env['LEDGENCE_PORT'] = str(self.free_port())
        self.base = 'http://127.0.0.1:' + self.env['LEDGENCE_PORT']
        self.project = 'ledgence-distribution-' + uuid.uuid4().hex[:16]
        self.compose = ['docker', 'compose', '--project-directory', str(self.kit),
                        '--project-name', self.project, '--file', str(self.kit / 'compose.yaml')]
        self.examples = self.compose + ['--file', str(self.kit / 'compose.examples.yaml')]
        self.owned = [(self.project, self.examples)]
        self.steps = []
        self.report = {'passed': False, 'distribution': self.manifest, 'projects': [self.project],
                       'started_at_unix': time.time(), 'source_checkout_needed_for_execution': False,
                       'scope': 'Local Docker distribution correctness; no AWS or capacity claim.'}

    @staticmethod
    def free_port():
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            return probe.getsockname()[1]

    def run(self, args, timeout=180, *, check=True, expect_failure=False):
        args = list(map(str, args))
        number = len(self.steps) + 1
        print('+', ' '.join(args), flush=True)
        start = time.monotonic()
        try:
            result = subprocess.run(args, cwd=self.cwd, env=self.env, text=True,
                                    capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            (self.evidence / f'{number:03d}.log').write_bytes(
                (error.stdout or b'') + (error.stderr or b''))
            self.steps.append({'command': args, 'timed_out': True, 'duration_seconds': time.monotonic() - start})
            raise
        (self.evidence / f'{number:03d}.log').write_text(result.stdout + result.stderr)
        self.steps.append({'command': args, 'exit_code': result.returncode,
                           'duration_seconds': time.monotonic() - start})
        if expect_failure and result.returncode == 0:
            raise RuntimeError(f'command unexpectedly succeeded; inspect {number:03d}.log')
        if check and result.returncode:
            raise RuntimeError(f'command failed ({result.returncode}); inspect {number:03d}.log')
        return result.stdout

    def api(self, path, *, base=None, body=None):
        request = urllib.request.Request((base or self.base) + path,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.load(response)

    def workers(self, *, base=None):
        """Read every persisted session, including earlier boots and unreported workers."""
        workers, cursors = [], set()
        path = '/v1/console/workers'
        while True:
            page = self.api(path, base=base)
            workers.extend(page['items'])
            cursor = page['next_cursor']
            if cursor is None:
                return workers
            assert cursor not in cursors, 'worker listing repeated a cursor'
            cursors.add(cursor)
            path = '/v1/console/workers?' + urllib.parse.urlencode({'cursor': cursor})

    def worker_session_ids(self, *, base=None):
        return {worker['worker_session_id'] for worker in self.workers(base=base)}

    def wait_for_worker(self, *, base=None, excluded_sessions=(), timeout=30):
        excluded_sessions = set(excluded_sessions)
        deadline = time.monotonic() + timeout
        while True:
            active = [worker for worker in self.workers(base=base)
                      if worker['freshness'] == 'fresh' and worker['accepting']
                      and not worker['session_expired']
                      and worker['worker_session_id'] not in excluded_sessions]
            if active:
                # Session IDs define list order, not recency. Every candidate
                # must satisfy readiness independently of its position.
                return min(active, key=lambda worker: worker['worker_session_id'])
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                expected = ' with a new session' if excluded_sessions else ''
                raise AssertionError('no fresh accepting worker' + expected)
            time.sleep(min(.2, remaining))

    def console(self, expected_programs, *, base=None, excluded_sessions=()):
        base = base or self.base
        config = self.api('/v1/console/config', base=base)
        assert config['instance_id'] == 'ledgence-local' and 'scope' not in config
        for route in ('/console/', '/console/executions/deep-link', '/console/programs'):
            with urllib.request.urlopen(base + route, timeout=10) as response:
                assert response.headers['Content-Type'].startswith('text/html')
                html = response.read().decode()
                assert '/console/assets/' in html
        for asset in re.findall(r'(?:src|href)="(/console/assets/[^"]+)"', html):
            with urllib.request.urlopen(base + asset, timeout=10) as response:
                assert response.status == 200 and 'text/html' not in response.headers['Content-Type']
        with urllib.request.urlopen(base + '/console/notices/index.html', timeout=10) as response:
            assert response.status == 200 and response.read()
        catalog = self.api('/v1/console/programs', base=base)['items']
        assert {item['program_id'] for item in catalog} == set(expected_programs)
        worker = self.wait_for_worker(base=base, excluded_sessions=excluded_sessions)
        details = self.api('/v1/console/workers/inspect?' + urllib.parse.urlencode(
            {'worker_session_id': worker['worker_session_id']}), base=base)
        assert details['worker']['capacity'] == 1 and len(details['slots']['items']) == 1
        return {'catalog': catalog, 'worker_session_id': worker['worker_session_id']}

    def preserved(self, demo):
        query = urllib.parse.urlencode({'tenant_id': 'acme', 'namespace': 'demo',
                                        'workflow_id': demo['workflow_id']})
        workflow = self.api('/v1/workflows/result?' + query)
        subscriptions = []
        for identity in demo['subscriptions']:
            query = urllib.parse.urlencode({'tenant_id': 'acme', 'namespace': 'demo', 'subscription_id': identity})
            value = self.api('/v1/completion-subscriptions/status?' + query)
            assert value['state'] == 'delivered'
            subscriptions.append(value)
        return {'workflow': workflow, 'subscriptions': subscriptions}

    def verify_runtime(self):
        self.run(['docker', 'pull', self.manifest['image']], timeout=600)
        image = json.loads(self.run(['docker', 'image', 'inspect', self.manifest['image']]))[0]
        architecture = {'x86_64': 'amd64', 'aarch64': 'arm64'}.get(image['Architecture'], image['Architecture'])
        assert image['Os'] + '/' + architecture in self.manifest['platforms']
        assert any(ref.endswith('@' + self.manifest['image'].split('@', 1)[1]) for ref in image['RepoDigests'])
        version = self.run(['docker', 'run', '--rm', '--network', 'none', self.manifest['image'],
                            'ledgence', '--version']).strip()
        assert version == 'ledgence ' + self.manifest['version']
        labels = image['Config'].get('Labels') or {}
        assert labels.get('org.opencontainers.image.version') == self.manifest['version']
        runtime = json.loads(self.run(['docker', 'run', '--rm', '--network', 'none', self.manifest['image'],
                                       'cat', '/opt/ledgence/legal/image-runtime.json']))
        assert runtime['format'] == 1 and runtime['python']['version'].startswith(self.manifest['python'] + '.')
        assert runtime['packages'] and runtime['python_license_sha256']
        self.report['runtime'] = {'image_id': image['Id'], 'repository_digests': image['RepoDigests'],
                                  'platform': image['Os'] + '/' + architecture, 'version': version,
                                  'labels': labels, 'inventory': runtime}
        self.image_id = image['Id']

    def verify_compose(self):
        model = json.loads(self.run(self.compose + ['config', '--format', 'json']))
        assert set(model['services']) == {'postgres', 'migrate', 'orchestrator', 'worker'}
        for name, service in model['services'].items():
            assert 'build' not in service
            if name != 'postgres':
                assert service['image'] == self.manifest['image']
        assert model['services']['orchestrator']['ports'][0]['host_ip'] == '127.0.0.1'
        assert '--allow-program-publication' in model['services']['orchestrator']['command']
        for name, read_only in [('orchestrator', False), ('worker', True)]:
            program_mount = next(v for v in model['services'][name]['volumes'] if v['target'] == '/programs')
            assert program_mount.get('read_only', False) is read_only
        self.run(self.compose + ['pull'], timeout=600)
        self.run(self.compose + ['up', '--no-build', '--detach', '--wait', '--wait-timeout', '120'])
        initial = self.console([])
        ids = self.run(self.compose + ['ps', '--all', '--quiet']).split()
        containers = json.loads(self.run(['docker', 'inspect', *ids]))
        for container in containers:
            service = container['Config']['Labels']['com.docker.compose.service']
            if service != 'postgres':
                assert container['Image'] == self.image_id
        self.report['fresh_base'] = initial
        # This also proves that the base installation needs neither examples nor
        # the callback receiver, before explicitly selecting the optional kit.
        base_sessions = self.worker_session_ids()
        self.run(self.compose + ['down', '--timeout', '65'])
        self.run(self.examples + ['up', '--no-build', '--detach', '--wait', '--wait-timeout', '120'])
        self.run(self.examples + ['run', '--rm', '--no-deps', 'publish'])
        self.run(self.examples + ['run', '--rm', '--no-deps', 'publish'])
        expected = ['invoice-issuer', 'workflow-example', 'workflow-summary']
        before_console = self.console(expected, excluded_sessions=base_sessions)
        demo = json.loads(self.run(self.examples + ['run', '--rm', '--no-deps', 'demo'], timeout=330))
        assert demo['passed'] is True
        before = self.preserved(demo)
        # Last-known accepting observations can remain fresh after process exit.
        # Exclude every session seen before restart, not just the selected worker.
        before_sessions = self.worker_session_ids()
        self.run(self.examples + ['down', '--timeout', '65'])
        self.run(self.examples + ['up', '--no-build', '--detach', '--wait', '--wait-timeout', '120'])
        assert self.preserved(demo) == before
        after_console = self.console(expected, excluded_sessions=before_sessions)
        assert after_console['catalog'] == before_console['catalog']
        assert after_console['worker_session_id'] != before_console['worker_session_id']
        after = json.loads(self.run(self.examples + ['run', '--rm', '--no-deps', 'demo'], timeout=330))
        assert after['passed'] is True
        self.report['examples'] = {'fresh': demo, 'after_restart': after,
                                   'retained': before, 'catalog_preserved': True,
                                   'new_worker_session': after_console['worker_session_id']}

    def publication_result(self, base, workflow_id):
        query = urllib.parse.urlencode({'tenant_id': 'acme', 'namespace': 'demo',
                                        'workflow_id': workflow_id})
        deadline = time.monotonic() + 90
        while True:
            result = self.api('/v1/workflows/result?' + query, base=base)
            if result['outcome'] is not None:
                assert result['outcome']['kind'] == 'succeeded', result
                output = result['outcome']['output']
                assert output['html'] == '<h1>Ledgence &amp; Python</h1>', output
                assert output['platform'] == 'linux' and output['native_extension'].endswith('.so'), output
                expected = {'arm64': 'aarch64', 'amd64': 'x86_64'}[self.report['runtime']['platform'].split('/')[1]]
                assert output['architecture'] == expected, output
                return output
            if time.monotonic() >= deadline:
                raise AssertionError('published workflow did not finish within 90 seconds')
            time.sleep(0.2)

    def publication_execute(self, base, descriptor):
        command = {'idempotency_key': 'publication-' + uuid.uuid4().hex,
                   'input': {'tenant_id': 'acme', 'namespace': 'demo', 'queue': 'demo',
                             'program': descriptor['program'], 'data': {'title': 'Ledgence & Python'}}}
        workflow_id = self.api('/v1/workflows', base=base, body=command)['workflow_id']
        return workflow_id, self.publication_result(base, workflow_id)

    def verify_program_publication(self, binary, base):
        # The native executable builds the exact Linux worker target. The example
        # is copied as developer input; the installed stack still uses only the kit.
        project = self.evidence / 'program project with spaces'
        source = Path(__file__).resolve().parents[1] / 'examples/program-publication'
        shutil.copytree(source, project)
        config = project / 'ledgence.toml'
        config.write_text(config.read_text()
            .replace('REPLACE_WITH_WORKER_IMAGE_DIGEST', self.manifest['image'])
            .replace('linux/arm64', self.report['runtime']['platform']))
        built = []
        for name in ('prepared-one', 'prepared-two'):
            built.append(json.loads(self.run([binary, 'program', 'build', '--config', config,
                '--output', '.ledgence/' + name], timeout=660)))
        assert built[0]['descriptor'] == built[1]['descriptor'], 'wheel preparation must package identically'
        # Exercise pip's real refusal paths, not only a fake Docker process.
        requirements = project / 'requirements.txt'
        original_requirements = requirements.read_text()
        original_config = config.read_text()
        try:
            for label, pins, target_config in [
                ('wrong-hash', 'MarkupSafe==3.0.4 --hash=sha256:' + '0' * 64, original_config),
                # This historical release has no ordinary CPython 3.14 wheel.
                # Its source hash cannot bypass --only-binary; no source runs.
                ('no-wheel', 'MarkupSafe==2.1.5 --hash=sha256:d283d37a890ba4c1ae73ffadf8046435c76e7bc2247bbb63c00bd1a709c6544b', original_config),
                ('wrong-python', original_requirements, original_config.replace('python = "3.14"', 'python = "3.13"')),
            ]:
                requirements.write_text(pins + '\n')
                config.write_text(target_config)
                output = project / '.ledgence' / label
                self.run([binary, 'program', 'build', '--config', config, '--output', output],
                         timeout=660, check=False, expect_failure=True)
                assert not output.exists() and not output.with_name(output.name + '.build.json').exists()
        finally:
            requirements.write_text(original_requirements)
            config.write_text(original_config)
        prepared = Path(built[0]['prepared_directory'])
        assert not list(prepared.rglob('*.pyc'))
        license_file = prepared / 'markupsafe-3.0.4.dist-info/licenses/LICENSE.txt'
        assert license_file.read_bytes() == (project / 'third_party/MarkupSafe-LICENSE.txt').read_bytes()
        published = json.loads(self.run([binary, 'program', 'publish', '--source', prepared,
                                         '--server', base, '--register']))
        assert published['phase'] == 'registered' and published['error'] is None, published
        assert published['descriptor'] == built[0]['descriptor']
        assert published['publication']['already_published'] is False
        repeated = json.loads(self.run([binary, 'program', 'publish', '--source', prepared,
                                        '--server', base, '--register']))
        assert repeated['publication']['already_published'] is True
        assert repeated['registration']['already_registered'] is True
        resumed = json.loads(self.run([binary, 'program', 'publish', '--resume', published['receipt']]))
        assert resumed['phase'] == 'registered' and resumed['descriptor'] == published['descriptor']
        workflow_id, output = self.publication_execute(base, published['descriptor'])
        return {'descriptor': published['descriptor'], 'receipt': published['receipt'],
                'workflow_id': workflow_id, 'output': output, 'deterministic_build': True,
                'repeated_publication': True, 'wheel_license_retained': True,
                'rejected_wrong_hash_missing_wheel_and_runtime': True}

    def verify_cli(self):
        if self.cli is None:
            return
        # Copy only the native executable. Pass the relocated distribution
        # explicitly so neither bundle auto-discovery nor a checkout can help.
        binary = self.cwd / 'ledgence'
        shutil.copy2(self.cli, binary)
        state_dir = self.evidence / 'cli-state'
        port = self.free_port()
        command = [str(binary), 'local']
        try:
            self.run(command + ['up', '--directory', str(state_dir), '--distribution', str(self.kit),
                                '--port', str(port), '--concurrency', '1'], timeout=330)
        finally:
            path = state_dir / '.ledgence-state.json'
            if path.is_file():
                state = json.loads(path.read_text())
                # Register cleanup before checking expected CLI metadata: a
                # regression in persisted fields must not leak its own project.
                if not re.fullmatch(r'ledgence-[a-f0-9]{24}', state.get('project', '')):
                    raise AssertionError('CLI saved an invalid owned project identity')
                if state.get('context') != self.run(['docker', 'context', 'show']).strip():
                    raise AssertionError('CLI changed the Docker context unexpectedly')
                cli_compose = ['docker', '--context', state['context'], 'compose', '--project-name', state['project'],
                               '--project-directory', str(state_dir), '--file', str(state_dir / 'compose.yaml')]
                self.owned.append((state['project'], cli_compose))
                self.report['projects'].append(state['project'])
                assert state['directory'] == str(state_dir)
                assert state['distribution'] == self.manifest
        original = path.read_bytes()
        base = 'http://127.0.0.1:' + str(port)
        first = self.console([], base=base)
        publication = self.verify_program_publication(binary, base)
        self.run(command + ['up', '--directory', str(state_dir)], timeout=330)
        self.run(command + ['status', '--directory', str(state_dir)])
        self.run(command + ['logs', '--directory', str(state_dir), '--tail', '10'])
        before_sessions = self.worker_session_ids(base='http://127.0.0.1:' + str(port))
        self.run(command + ['down', '--directory', str(state_dir)])
        self.run(command + ['up', '--directory', str(state_dir)], timeout=330)
        second = self.console(['html-report'], base=base, excluded_sessions=before_sessions)
        assert self.publication_result(base, publication['workflow_id']) == publication['output']
        workflow_id, output = self.publication_execute(base, publication['descriptor'])
        assert output == publication['output']
        publication.update(after_restart_workflow_id=workflow_id, persisted_result=True)
        self.report['program_publication'] = publication
        assert path.read_bytes() == original
        assert second['worker_session_id'] != first['worker_session_id']
        self.run(command + ['down', '--directory', str(state_dir)])
        self.report['cli'] = {'passed': True, 'state_sha256': hashlib.sha256(original).hexdigest(),
                              'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                              'repeat_up_identity_preserved': True, 'restart_preserved_kit_and_settings': True}

    def cleanup(self):
        errors = []
        for project, command in reversed(self.owned):
            try:
                self.run(command + ['logs', '--no-color'], timeout=30, check=False)
                self.run(command + ['down', '--volumes', '--remove-orphans', '--timeout', '65'], timeout=120)
                assert not self.run(['docker', 'ps', '--all', '--quiet', '--filter', 'label=com.docker.compose.project=' + project]).strip()
                assert not self.run(['docker', 'volume', 'ls', '--quiet', '--filter', 'label=com.docker.compose.project=' + project]).strip()
            except Exception as error:
                errors.append(type(error).__name__ + ': ' + str(error))
        self.report['cleanup_complete'] = not errors
        if errors:
            self.report['cleanup_errors'] = errors
            raise RuntimeError('owned project cleanup failed; see report')

    def execute(self):
        try:
            self.verify_runtime()
            self.verify_compose()
            self.verify_cli()
            self.report['passed'] = True
        except Exception as error:
            self.report['failure_type'] = type(error).__name__
            self.report['failure'] = str(error)
            raise
        finally:
            try:
                self.cleanup()
            except Exception:
                self.report['passed'] = False
                raise
            finally:
                self.report['finished_at_unix'] = time.time()
                self.report['commands'] = self.steps
                (self.evidence / 'report.json').write_text(json.dumps(self.report, indent=2) + '\n')
        print(json.dumps({'passed': True, 'evidence': str(self.evidence),
                          'platform': self.report['runtime']['platform'], 'cli': self.cli is not None}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True, type=Path)
    parser.add_argument('--evidence', required=True, type=Path, help='new evidence directory')
    parser.add_argument('--cli', type=Path, help='matching host native CLI; exercise relocated local commands')
    args = parser.parse_args()
    Acceptance(args.directory.resolve(), args.evidence.resolve(), args.cli.resolve() if args.cli else None).execute()


if __name__ == '__main__':
    main()
