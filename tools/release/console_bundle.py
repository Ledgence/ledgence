"""Validate prepared Console bytes and exercise them with a disposable real DB.

No Node, downloads, existing database migration, or existing resource cleanup.
"""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate Console manifest field')
        result[key] = value
    return result


def validate(directory, *, source_commit=None, version=None, project=None):
    directory = directory.resolve()
    path = directory / 'console-manifest.json'
    if path.is_symlink() or path.stat().st_size > 1024 * 1024:
        raise ValueError('invalid Console manifest file')
    manifest = json.loads(path.read_bytes(), object_pairs_hook=unique)
    if (set(manifest) != {'schema_version', 'console_version', 'console_contract_version',
                         'source_revision', 'source_dirty', 'toolchain', 'lockfile_sha256', 'assets'}
            or type(manifest['schema_version']) is not int or manifest['schema_version'] != 1
            or type(manifest['console_contract_version']) is not int or manifest['console_contract_version'] != 4
            or not re.fullmatch('[a-f0-9]{40}', manifest['source_revision'])
            or manifest['source_dirty'] is not False
            or not re.fullmatch('[a-f0-9]{64}', manifest['lockfile_sha256'])
            or not isinstance(manifest['toolchain'], dict) or set(manifest['toolchain']) != {'node', 'pnpm'}
            or any(not isinstance(v, str) or not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+', v) for v in manifest['toolchain'].values())
            or not isinstance(manifest['console_version'], str) or not manifest['console_version']
            or not isinstance(manifest['assets'], list) or not 1 <= len(manifest['assets']) <= 4096):
        raise ValueError('invalid, dirty or incompatible Console distribution')
    if source_commit and manifest['source_revision'] != source_commit:
        raise ValueError('Console source revision differs from native source')
    if version and manifest['console_version'] != version:
        raise ValueError('Console version differs from native package')
    if project:
        metadata = json.loads((project / 'package.json').read_text())
        if (manifest['toolchain'] != metadata['engines']
                or manifest['lockfile_sha256'] != digest(project / 'pnpm-lock.yaml')):
            raise ValueError('Console toolchain or dependency lock differs from source')
    expected = {'console-manifest.json'}
    total = 0
    for asset in manifest['assets']:
        if set(asset) != {'path', 'sha256', 'size_bytes', 'content_type'}:
            raise ValueError('invalid Console asset record')
        name = asset['path']
        pure = PurePosixPath(name)
        if (pure.is_absolute() or any(p in ('.', '..', '') for p in name.split('/'))
                or any(c in name for c in '\\%?#:') or any(ord(c) < 32 or ord(c) == 127 for c in name) or name in expected
                or not re.fullmatch('[a-f0-9]{64}', asset['sha256'])
                or type(asset['size_bytes']) is not int or not 0 <= asset['size_bytes'] <= 32 * 1024 * 1024):
            raise ValueError('invalid Console asset path, size or hash')
        extension = pure.suffix
        mime = {'.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
                '.css': 'text/css; charset=utf-8', '.json': 'application/json',
                '.txt': 'text/plain; charset=utf-8', '.svg': 'image/svg+xml',
                '.png': 'image/png', '.ico': 'image/x-icon', '.woff2': 'font/woff2'}.get(extension)
        if mime is None and name.startswith('notices/'):
            mime = 'text/plain; charset=utf-8'
        if mime is None or asset['content_type'] != mime:
            raise ValueError('invalid Console asset content type')
        total += asset['size_bytes']
        if total > 64 * 1024 * 1024:
            raise ValueError('Console distribution exceeds supported size')
        expected.add(name)
        file = directory / name
        if not file.is_file() or file.is_symlink() or not file.resolve().is_relative_to(directory):
            raise ValueError('invalid Console asset file')
        if file.stat().st_size != asset['size_bytes'] or digest(file) != asset['sha256']:
            raise ValueError('Console asset checksum mismatch: ' + name)
    actual = set()
    for file in directory.rglob('*'):
        if file.is_symlink() or not (file.is_dir() or file.is_file()):
            raise ValueError('unsupported Console distribution entry')
        if file.is_file():
            actual.add(file.relative_to(directory).as_posix())
    if actual != expected or not {'index.html', 'notices/index.html', 'notices/LEDGENCE-LICENSE.txt'} <= expected:
        raise ValueError('Console asset inventory or notices missing')
    return manifest


def verify_bundle(bundle):
    provenance = json.loads((bundle / 'provenance.json').read_text())
    # Promotion retains the original console qualification and every asset byte.
    if (bundle / 'candidate-provenance.json').is_file():
        provenance = json.loads((bundle / 'candidate-provenance.json').read_text())
    record = provenance.get('console')
    directory = bundle / 'console'
    if record is None:  # Backward-compatible verification of older native bundles.
        if directory.exists():
            raise ValueError('Console payload lacks provenance')
        return None
    if record == {'mode': 'headless'}:
        if directory.exists():
            raise ValueError('headless bundle contains unexpected Console assets')
        return None
    if not isinstance(record, dict) or record.get('mode') != 'static':
        raise ValueError('invalid Console provenance')
    manifest = validate(directory, source_commit=provenance['source_commit'], version=provenance['package_version'])
    if digest(directory / 'console-manifest.json') != record.get('manifest_sha256'):
        raise ValueError('Console manifest differs from build provenance')
    return manifest


def smoke(bundle, store, temporary):
    manifest = verify_bundle(bundle)
    if manifest is None:
        return
    admin_url = os.environ.get('LEDGENCE_POSTGRES_URL')
    if not admin_url:
        raise RuntimeError('Console bundle verification requires LEDGENCE_POSTGRES_URL for a disposable database')
    psql = os.environ.get('LEDGENCE_PSQL', 'psql')
    database = 'ldg_console_bundle_' + uuid.uuid4().hex
    def admin(statement):
        result = subprocess.run([psql, '--dbname', admin_url, '-X', '-q', '-v', 'ON_ERROR_STOP=1', '-f', '-'],
                                input=statement, text=True, capture_output=True, timeout=30)
        if result.returncode:
            raise RuntimeError('disposable Console database administration failed')
    parsed = urllib.parse.urlsplit(admin_url)
    url = urllib.parse.urlunsplit(parsed._replace(path='/' + database))
    environment = dict(os.environ, DATABASE_URL=url)
    instance = temporary / 'instance.json'
    instance.write_text(json.dumps({'instance_id': database, 'name': 'Relocated Console'}))
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    base = f'http://127.0.0.1:{port}'
    executable = bundle / 'bin/ledgence-orchestrator'
    created = False
    process = None
    with tempfile.TemporaryFile() as log:
        try:
            admin(f'CREATE DATABASE "{database}"')
            created = True
            migrated = subprocess.run([executable, 'migrate'], cwd=temporary, env=environment,
                                      stdout=log, stderr=log, timeout=60)
            if migrated.returncode:
                raise RuntimeError('relocated Console migrations failed')
            process = subprocess.Popen([executable, 'serve', '--bind', f'127.0.0.1:{port}',
                                        '--store', store, '--instance-config', instance,
                                        '--console-dir', bundle / 'console'],
                                       cwd=temporary, env=environment, stdout=log, stderr=log)
            deadline = time.monotonic() + 60
            while True:
                if process.poll() is not None:
                    log.seek(0)
                    raise RuntimeError('relocated Console exited: ' + log.read(8192).decode(errors='replace'))
                try:
                    with urllib.request.urlopen(base + '/health/ready', timeout=1) as response:
                        if response.status == 200:
                            break
                except (OSError, urllib.error.URLError):
                    pass
                if time.monotonic() >= deadline:
                    raise RuntimeError('relocated Console readiness timed out')
                time.sleep(.1)
            for path in ['/console/', '/console/executions/relocated-id', '/console/agents/example']:
                with urllib.request.urlopen(base + path, timeout=5) as response:
                    assert response.headers['Content-Type'].startswith('text/html')
                    assert response.read() == (bundle / 'console/index.html').read_bytes()
            for asset in manifest['assets']:
                with urllib.request.urlopen(base + '/console/' + urllib.parse.quote(asset['path'], safe='/'), timeout=5) as response:
                    assert response.headers['Content-Type'] == asset['content_type'], asset['path']
                    assert hashlib.sha256(response.read()).hexdigest() == asset['sha256'], asset['path']
            for resource in ['config', 'tasks', 'workflows', 'programs', 'workers']:
                with urllib.request.urlopen(base + '/v1/console/' + resource, timeout=5) as response:
                    assert response.headers['ledgence-instance-id'] == database
                    value = json.load(response)
                    assert (value['instance_id'] == database if resource == 'config' else value['items'] == [])
            for path in ['/console/assets/missing.js', '/v1/missing']:
                try:
                    urllib.request.urlopen(base + path, timeout=5)
                    raise AssertionError('unknown path succeeded')
                except urllib.error.HTTPError as error:
                    assert error.code == 404 and 'text/html' not in error.headers.get('Content-Type', '')
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=40) == 0
        finally:
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=10)
            if created:
                admin(f'DROP DATABASE "{database}" WITH (FORCE)')
    print('Relocated Console manifest, all assets/notices, deep links and real PostgreSQL API passed')
