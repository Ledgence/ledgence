#!/usr/bin/env python3
"""Package a standalone, digest-pinned Compose kit; never build or publish images."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = ROOT / 'deploy/distribution'
PLATFORMS = ['linux/amd64', 'linux/arm64']
FILES = {'compose.yaml', 'compose.examples.yaml', 'README.md', 'LICENSE', 'distribution.json'}
VERSION = re.compile(r'(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)')
IMAGE = re.compile(r'[a-z0-9][a-z0-9._:/-]*@sha256:[a-f0-9]{64}')


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_manifest(value: dict, version: str | None = None) -> dict:
    if not isinstance(value, dict) or set(value) != {'format', 'version', 'image', 'python', 'platforms'}:
        raise ValueError('unexpected distribution manifest fields')
    if type(value['format']) is not int or value['format'] != 1:
        raise ValueError('unsupported distribution format')
    if not isinstance(value['version'], str) or not VERSION.fullmatch(value['version']):
        raise ValueError('distribution version must be an exact stable version')
    if version is not None and value['version'] != version:
        raise ValueError('distribution version does not match the release')
    image = value['image']
    if not isinstance(image, str) or not IMAGE.fullmatch(image):
        raise ValueError('image must be a repository pinned by its sha256 digest')
    repository = image.split('@', 1)[0]
    parts = repository.split('/')
    if len(parts) < 2 or any(not part or part in ('.', '..') for part in parts):
        raise ValueError('image must include a repository namespace')
    if any(':' in part for part in parts[1:]):
        raise ValueError('pin the repository digest without a mutable image tag')
    if value['python'] != '3.14' or not isinstance(value['platforms'], list) or not value['platforms'] or value['platforms'] != [p for p in PLATFORMS if p in value['platforms']]:
        raise ValueError('unsupported Python runtime or platform set')
    return value


def verify_directory(directory: Path, version: str | None = None) -> dict:
    directory = Path(directory)
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError('distribution must be a real directory')
    entries = list(directory.iterdir())
    if {p.name for p in entries} != FILES | {'SHA256SUMS'}:
        raise ValueError('distribution file inventory does not match format 1')
    if any(p.is_symlink() or not p.is_file() for p in entries):
        raise ValueError('distribution entries must be regular files')
    checks = {}
    for line in (directory / 'SHA256SUMS').read_text().splitlines():
        match = re.fullmatch(r'([a-f0-9]{64})  ([A-Za-z0-9_.-]+)', line)
        if match is None or match[2] in checks:
            raise ValueError('invalid or duplicate checksum entry')
        checks[match[2]] = match[1]
    if set(checks) != FILES or any(digest(directory / name) != expected for name, expected in checks.items()):
        raise ValueError('distribution checksum inventory mismatch')
    manifest = validate_manifest(json.loads((directory / 'distribution.json').read_text()), version)
    # Digest pinning remains explicit in every service image reference. Compose
    # must never need a source checkout or fall back to a local image build.
    for name in ('compose.yaml', 'compose.examples.yaml'):
        text = (directory / name).read_text()
        if manifest['image'] not in text or '@LEDGENCE_IMAGE@' in text:
            raise ValueError('Compose image does not match the distribution manifest')
        if re.search(r'^\s*(?:build|include|extends)\s*:', text, re.MULTILINE):
            raise ValueError('distribution Compose cannot build or load external files')
    return manifest


def stage(directory: Path, *, version: str, image: str, platforms: list[str] | None = None, templates: Path = TEMPLATES) -> dict:
    manifest = validate_manifest({'format': 1, 'version': version, 'image': image,
                                  'python': '3.14', 'platforms': PLATFORMS.copy() if platforms is None else list(platforms)})
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    for name in ('compose.yaml', 'compose.examples.yaml', 'README.md'):
        source = Path(templates) / name
        if source.is_symlink() or not source.is_file():
            raise ValueError(f'missing regular distribution template: {name}')
        text = source.read_text()
        if name.endswith('.yaml') and text.count('@LEDGENCE_IMAGE@') != 1:
            raise ValueError(f'expected one immutable image placeholder in {name}')
        (directory / name).write_text(text.replace('@LEDGENCE_IMAGE@', image))
    shutil.copyfile(ROOT / 'LICENSE', directory / 'LICENSE')
    (directory / 'distribution.json').write_text(json.dumps(manifest, indent=2) + '\n')
    (directory / 'SHA256SUMS').write_text(''.join(f'{digest(directory / name)}  {name}\n' for name in sorted(FILES)))
    return verify_directory(directory, version=version)


def archive_tree(directory: Path, archive: Path, *, epoch: int) -> None:
    if epoch < 0:
        raise ValueError('source timestamp cannot be negative')
    verify_directory(directory)
    with archive.open('xb') as raw, gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0) as zipped:
        with tarfile.open(fileobj=zipped, mode='w', format=tarfile.PAX_FORMAT) as tar:
            for path in [directory, *sorted(directory.iterdir())]:
                info = tarfile.TarInfo(str(Path(directory.name) / path.relative_to(directory)))
                info.uid = info.gid = 0
                info.uname = info.gname = ''
                info.mtime = epoch
                info.mode = 0o755 if path.is_dir() else 0o644
                if path.is_dir():
                    info.type = tarfile.DIRTYPE
                    tar.addfile(info)
                else:
                    info.size = path.stat().st_size
                    with path.open('rb') as data:
                        tar.addfile(info, data)


def unpack(archive: Path, directory: Path, *, version: str | None = None) -> Path:
    """Extract the small explicit format-1 inventory without arbitrary tar paths."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    with tarfile.open(archive, 'r:gz') as tar:
        members = tar.getmembers()
        if len(members) != len(FILES) + 2:
            raise ValueError('unexpected local distribution archive inventory')
        root = members[0].name.rstrip('/')
        if not re.fullmatch(r'ledgence-(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)-local', root):
            raise ValueError('unexpected local distribution archive root')
        expected = {root} | {root + '/' + name for name in FILES | {'SHA256SUMS'}}
        if {m.name.rstrip('/') for m in members} != expected:
            raise ValueError('unexpected or duplicate archive member')
        for member in members:
            if member.name.rstrip('/') == root:
                if not member.isdir():
                    raise ValueError('archive root must be a directory')
                continue
            if not member.isfile() or member.size > 1024 * 1024:
                raise ValueError('archive entry must be a bounded regular file')
            data = tar.extractfile(member)
            assert data is not None
            (directory / member.name.split('/', 1)[1]).write_bytes(data.read())
    manifest = verify_directory(directory, version=version)
    if root != 'ledgence-' + manifest['version'] + '-local':
        raise ValueError('archive root version differs from manifest')
    return directory


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', required=True)
    parser.add_argument('--image', required=True, help='qualified repository@sha256:<index digest>')
    parser.add_argument('--platform', action='append', choices=PLATFORMS, help='repeat for qualified platforms; defaults to both')
    parser.add_argument('--output', type=Path, required=True, help='new directory outside checkout')
    parser.add_argument('--epoch', type=int, help='source commit timestamp; defaults to current commit')
    args = parser.parse_args()
    version = tomllib.loads((ROOT / 'Cargo.toml').read_text())['workspace']['package']['version']
    if args.version != version:
        parser.error('kit version must match the source workspace')
    output = args.output.resolve()
    if output.exists() or output.is_relative_to(ROOT):
        parser.error('output must be a new directory outside checkout')
    epoch = args.epoch if args.epoch is not None else int(subprocess.check_output(
        ['git', 'show', '-s', '--format=%ct', 'HEAD'], cwd=ROOT, text=True))
    validate_manifest({'format': 1, 'version': version, 'image': args.image, 'python': '3.14', 'platforms': args.platform or PLATFORMS})
    if epoch < 0:
        parser.error('source timestamp cannot be negative')
    output.mkdir(parents=True)
    directory = output / f'ledgence-{version}-local'
    manifest = stage(directory, version=version, image=args.image, platforms=args.platform)
    archive = output / (directory.name + '.tar.gz')
    archive_tree(directory, archive, epoch=epoch)
    with tempfile.TemporaryDirectory(prefix='ledgence-local-kit-verify-') as temporary:
        unpack(archive, Path(temporary) / 'local', version=version)
    (output / 'SHA256SUMS').write_text(f'{digest(archive)}  {archive.name}\n')
    print(json.dumps({'directory': str(directory), 'archive': str(archive),
                      'sha256': digest(archive), 'distribution': manifest}, indent=2))


if __name__ == '__main__':
    main()
