#!/usr/bin/env python3
"""Promote native-tested OCI archives and prepare their standalone local kit.

Requires a public GitHub release, standard Docker/Skopeo authentication and the
two complete artifacts produced by images.yml. No image is rebuilt here.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tempfile

import local_distribution
import oci
import registry

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('image_sources', ROOT / 'deploy/distribution/image_sources.py')
image_sources = importlib.util.module_from_spec(spec)
spec.loader.exec_module(image_sources)


def verify_checksums(directory):
    expected = {}
    for line in (directory / 'SHA256SUMS').read_text().splitlines():
        match = re.fullmatch(r'([a-f0-9]{64})  (.+)', line)
        if match is None:
            raise ValueError('invalid qualification checksum entry')
        name = match[2]
        path = PurePosixPath(name)
        if path.is_absolute() or '..' in path.parts or str(path) != name or name in expected:
            raise ValueError('invalid qualification checksum path')
        expected[name] = match[1]
    entries = list(directory.rglob('*'))
    if any(path.is_symlink() or not (path.is_dir() or path.is_file()) for path in entries):
        raise ValueError('qualification artifact contains nonregular entries')
    actual = {str(path.relative_to(directory)) for path in entries
              if path.is_file() and path != directory / 'SHA256SUMS'}
    if actual != set(expected):
        raise ValueError('qualification artifact inventory differs from checksums')
    for name, digest in expected.items():
        if oci.checksum(directory / name) != digest:
            raise ValueError('qualification artifact checksum mismatch: ' + name)


def verify_inputs(directory, version, source, namespace):
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,63}', namespace):
        raise ValueError('configure an explicit Docker Hub account or organization namespace')
    records = {}
    for arch in oci.PLATFORMS:
        artifact = directory / ('oci-linux-' + arch)
        verify_checksums(artifact)
        record = json.loads((artifact / 'qualification.json').read_text())
        if record != oci.inspect_archive(artifact / 'image.tar', version=version, source=source, architecture=arch):
            raise ValueError('image archive differs from its qualification record')
        evidence = json.loads((artifact / 'acceptance/report.json').read_text())
        reference = '127.0.0.1:5000/ledgence@' + record['index_digest']
        if (evidence.get('passed') is not True or evidence.get('cleanup_complete') is not True
                or evidence.get('distribution', {}).get('image') != reference
                or evidence.get('runtime', {}).get('platform') != 'linux/' + arch
                or evidence.get('cli', {}).get('passed') is not True):
            raise ValueError('standalone distribution acceptance is missing or identifies another image')
        sources = image_sources.verify_output(artifact / 'sources', image=reference, platform='linux/' + arch)
        # Classic Docker exposes the config digest as Id; the containerd image
        # store can expose the loaded manifest/index digest instead. All three
        # identities are verified against the same qualified OCI archive.
        identities = {record['runtime_config_digest'], record['runtime_digest'], record['index_digest']}
        if sources.get('image', {}).get('id') not in identities:
            raise ValueError('corresponding sources were collected from a different runtime image')
        records[arch] = record
    return records


def source_assets(directory, version):
    for arch in oci.PLATFORMS:
        prefix = f'ledgence-{version}-linux-{arch}-'
        for name in ('corresponding-source.tar.gz', 'source-manifest.json'):
            yield directory / ('oci-linux-' + arch) / 'sources' / name, prefix + name


def publish_assets(assets, version):
    """Upload missing bytes and reject attempts to change existing release assets."""
    tag = 'v' + version
    repository = 'Ledgence/ledgence'
    release = json.loads(subprocess.check_output(
        ['gh', 'release', 'view', tag, '--repo', repository, '--json', 'isDraft,tagName,assets'], text=True))
    if release.get('isDraft') is not False or release.get('tagName') != tag:
        raise ValueError('create the public versioned GitHub release before publishing its runtime images')
    existing = {asset['name'] for asset in release['assets']}
    with tempfile.TemporaryDirectory(prefix='ledgence-source-publication-') as temporary:
        temporary = Path(temporary)
        for source, name in assets:
            if name in existing:
                destination = temporary / name
                subprocess.run(['gh', 'release', 'download', tag, '--repo', repository, '--pattern', name,
                                '--dir', str(temporary)], check=True, timeout=900)
                if oci.checksum(destination) != oci.checksum(source):
                    raise ValueError('existing source release asset differs from qualified bytes: ' + name)
            else:
                destination = temporary / name
                shutil.copyfile(source, destination)
                subprocess.run(['gh', 'release', 'upload', tag, '--repo', repository, str(destination)],
                               check=True, timeout=900)


def verify_index(reference, expected):
    raw = subprocess.check_output(['skopeo', 'inspect', '--raw', 'docker://' + reference])
    manifest = json.loads(raw)
    key = lambda entry: entry['digest']
    descriptors = sorted(manifest.get('manifests', []), key=key)
    if (manifest.get('schemaVersion') != 2 or manifest.get('mediaType') != oci.INDEX
            or descriptors != sorted(expected, key=key)):
        raise ValueError('release index differs from the two qualified platform images and attestations')
    return 'sha256:' + hashlib.sha256(raw).hexdigest()


def publish(directory, version, source, namespace, output):
    if registry.check_source(publish=True) != version or registry.git('rev-parse', 'HEAD') != source:
        raise ValueError('image publication must use the matching clean release source')
    if output.exists():
        raise ValueError('output directory must be new; publication can resume with a new output path')
    records = verify_inputs(directory, version, source, namespace)
    # Source availability is established before any runtime image publication.
    publish_assets(source_assets(directory, version), version)
    repository = f'docker.io/{namespace}/ledgence'
    release = repository + ':' + version + '-python3.14'
    references = []
    expected = []
    for arch, record in records.items():
        destination = release + '-' + arch
        oci.copy_qualified(directory / ('oci-linux-' + arch) / 'image.tar', record, destination)
        references.append(repository + '@' + record['index_digest'])
        expected.extend(record['manifests'])
    existing = oci.inspect_remote(release)
    if existing is None:
        subprocess.run(['docker', 'buildx', 'imagetools', 'create', '--tag', release, *references],
                       check=True, timeout=900)
    digest = verify_index(release, expected)
    # Verify public availability without using the publisher's registry session.
    with tempfile.TemporaryDirectory(prefix='ledgence-anonymous-registry-') as temporary:
        auth = Path(temporary) / 'auth.json'
        auth.write_text('{"auths":{}}\n')
        subprocess.run(['skopeo', 'inspect', '--authfile', str(auth), '--raw',
                        'docker://' + repository + '@' + digest], check=True,
                       stdout=subprocess.DEVNULL, timeout=120)
    output.mkdir(parents=True)
    kit = output / f'ledgence-{version}-local'
    manifest = local_distribution.stage(kit, version=version, image=repository + '@' + digest)
    epoch = int(subprocess.check_output(['git', 'show', '-s', '--format=%ct', source], cwd=ROOT, text=True))
    local_distribution.archive_tree(kit, output / (kit.name + '.tar.gz'), epoch=epoch)
    shutil.copyfile(ROOT / 'install.sh', output / 'install.sh')
    public_files = [output / (kit.name + '.tar.gz'), output / 'install.sh']
    for path in public_files.copy():
        checks = path.with_name(path.name + '.sha256')
        checks.write_text(f'{oci.checksum(path)}  {path.name}\n')
        public_files.append(checks)
    for file, name in source_assets(directory, version):
        shutil.copyfile(file, output / name)
    (output / 'container-release.json').write_text(json.dumps({
        'format': 1, 'version': version, 'source_commit': source, 'image': repository + '@' + digest,
        'tag': release, 'platforms': records, 'distribution': manifest,
        'sources': 'https://github.com/Ledgence/ledgence/releases/tag/v' + version,
    }, indent=2) + '\n')
    with (output / 'SHA256SUMS').open('x') as checks:
        for path in sorted(output.iterdir()):
            if path.is_file() and path.name != 'SHA256SUMS':
                checks.write(f'{oci.checksum(path)}  {path.name}\n')
    publish_assets(((path, path.name) for path in public_files), version)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True, type=Path)
    parser.add_argument('--version', required=True)
    parser.add_argument('--source', required=True)
    parser.add_argument('--namespace', required=True)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--check-only', action='store_true')
    args = parser.parse_args()
    if args.check_only:
        verify_inputs(args.directory, args.version, args.source, args.namespace)
        print('Both native-tested OCI artifacts and their corresponding sources are complete')
    else:
        print(json.dumps(publish(args.directory, args.version, args.source, args.namespace, args.output)))


if __name__ == '__main__':
    main()
