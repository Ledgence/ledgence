"""The Docker preparation adapter. Never imports the application handler."""
import json
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path


def main():
    control = json.loads(Path('/control/build.json').read_text())
    manifest = control['manifest']
    actual = {
        'os': sys.platform,
        'arch': {'amd64': 'x86_64', 'arm64': 'aarch64'}.get(platform.machine().lower(), platform.machine().lower()),
        'python': f'{sys.version_info.major}.{sys.version_info.minor}',
        'implementation': sys.implementation.name,
    }
    expected = {'os': manifest['platform']['os'], 'arch': manifest['platform']['arch'],
                'python': manifest['runtime']['python'], 'implementation': 'cpython'}
    if actual != expected:
        raise RuntimeError('builder runtime differs from requested OS, architecture or CPython version')
    dependencies = Path('/tmp/ledgence-dependencies')
    dependencies.mkdir()
    if control['requirements']:
        # Do not use --no-deps: pip hash mode must reject missing/unpinned
        # transitive dependencies as well as a hash mismatch or missing wheel.
        command = [sys.executable, '-I', '-B', '-m', 'pip', '--isolated', 'install',
                   '--require-hashes', '--only-binary=:all:', '--no-compile',
                   '--no-cache-dir', '--disable-pip-version-check', '--no-input',
                   '--index-url', 'https://pypi.org/simple', '--target', str(dependencies),
                   '-r', '/input/requirements.txt']
        result = subprocess.run(command, stdin=subprocess.DEVNULL,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1',
                                     'PIP_CONFIG_FILE': os.devnull})
        if result.returncode:
            raise RuntimeError('dependency installation failed: require complete == pins, matching SHA256 hashes and compatible wheels for the target')
    output = Path('/output/prepared')
    output.mkdir()
    paths = {}
    size = 0
    count = 0
    for root in (dependencies, Path('/input/application')):
        for source in sorted(root.rglob('*')):
            if source.is_symlink() or not (source.is_file() or source.is_dir()):
                raise RuntimeError('preparation produced a symlink or special file')
            relative = source.relative_to(root)
            spelling = relative.as_posix()
            key = spelling.lower()
            is_dir = source.is_dir()
            prior = paths.get(key)
            if prior is not None and (prior != (spelling, True) or not is_dir):
                raise RuntimeError('application and dependencies have colliding package paths')
            if prior is None:
                paths[key] = (spelling, is_dir)
                count += 1
                if count > 4095:
                    raise RuntimeError('prepared package exceeds entry limit')
            destination = output / relative
            if is_dir:
                destination.mkdir(exist_ok=True)
                destination.chmod(0o755)
                continue
            length = source.stat().st_size
            size += length
            if length > 64 * 1024 * 1024 or size > 256 * 1024 * 1024:
                raise RuntimeError('prepared package exceeds expansion limit')
            destination.parent.mkdir(parents=True, exist_ok=True)
            with source.open('rb') as incoming, destination.open('xb') as outgoing:
                shutil.copyfileobj(incoming, outgoing)
            destination.chmod(0o755 if source.stat().st_mode & 0o111 else 0o644)
    (output / 'ledgence-program.json').write_text(json.dumps(manifest, separators=(',', ':')) + '\n')
    (Path('/output') / 'runtime.json').write_text(json.dumps(actual))
    print(json.dumps({'prepared': True}))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        # Operational errors can contain filesystem paths or dependency URLs.
        # Only deliberate diagnostics above are suitable for this boundary.
        message = str(error) if type(error) is RuntimeError else 'builder could not prepare the selected package'
        print(json.dumps({'error': message}))
        sys.exit(1)
