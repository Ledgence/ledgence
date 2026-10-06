#!/usr/bin/env python3
"""Verify qualified OCI archives and copy their exact digests without rebuilding.

Skopeo and Docker Buildx are release tools only; neither enters the runtime image.
Registry authentication is supplied by their standard credential files.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile


DIGEST = re.compile(r'sha256:[a-f0-9]{64}')
VERSION = re.compile(r'(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)')
SOURCE = re.compile(r'[a-f0-9]{40}')
PLATFORMS = ('amd64', 'arm64')
INDEX = 'application/vnd.oci.image.index.v1+json'
MANIFEST = 'application/vnd.oci.image.manifest.v1+json'


def checksum(path):
    with Path(path).open('rb') as data:
        return hashlib.file_digest(data, 'sha256').hexdigest()


def inspect_archive(archive, *, version, source, architecture):
    """Check every blob, references, runtime identity and retained attestations."""
    if not VERSION.fullmatch(version) or not SOURCE.fullmatch(source) or architecture not in PLATFORMS:
        raise ValueError('invalid expected image identity')
    with tarfile.open(archive, 'r:*') as tar:
        files = {}
        total = 0
        for item in tar.getmembers():
            name = item.name.removeprefix('./').rstrip('/')
            path = PurePosixPath(name)
            if path.is_absolute() or '..' in path.parts or not name or (not item.isfile() and not item.isdir()):
                raise ValueError('unsupported OCI archive entry')
            if item.isdir():
                continue
            if name in files:
                raise ValueError('duplicate OCI archive file')
            total += item.size
            if len(files) >= 100000 or total > 20 * 1024 ** 3:
                raise ValueError('OCI archive exceeds supported bounds')
            if name not in ('oci-layout', 'index.json') and not re.fullmatch(r'blobs/sha256/[a-f0-9]{64}', name):
                raise ValueError('unexpected OCI archive file')
            files[name] = item
            if name.startswith('blobs/'):
                data = tar.extractfile(item)
                if data is None or hashlib.file_digest(data, 'sha256').hexdigest() != path.name:
                    raise ValueError('OCI blob digest mismatch')

        def read_json(name):
            item = files.get(name)
            if item is None or item.size > 32 * 1024 ** 2:
                raise ValueError('missing or oversized OCI JSON')
            data = tar.extractfile(item)
            if data is None:
                raise ValueError('missing OCI JSON data')
            return json.load(data)

        def descriptor(entry):
            if not isinstance(entry, dict) or not DIGEST.fullmatch(entry.get('digest', '')):
                raise ValueError('invalid OCI descriptor')
            name = 'blobs/sha256/' + entry['digest'].split(':')[1]
            if name not in files or type(entry.get('size')) is not int or files[name].size != entry['size']:
                raise ValueError('OCI descriptor size or reference mismatch')
            return name

        if read_json('oci-layout') != {'imageLayoutVersion': '1.0.0'}:
            raise ValueError('unsupported OCI layout')
        top = read_json('index.json')
        if top.get('schemaVersion') != 2 or len(top.get('manifests', [])) != 1:
            raise ValueError('expected exactly one exported OCI image')
        root = top['manifests'][0]
        index = read_json(descriptor(root))
        if root.get('mediaType') != INDEX or index.get('schemaVersion') != 2:
            raise ValueError('export must retain the image index and attestations')
        runtime = []
        runtime_configs = []
        attestations = []
        for entry in index.get('manifests', []):
            if entry.get('mediaType') != MANIFEST:
                raise ValueError('unexpected nested image descriptor')
            body = read_json(descriptor(entry))
            config = read_json(descriptor(body['config']))
            for layer in body.get('layers', []):
                descriptor(layer)
            platform = entry.get('platform', {})
            if platform == {'architecture': 'unknown', 'os': 'unknown'}:
                if entry.get('annotations', {}).get('vnd.docker.reference.type') != 'attestation-manifest':
                    raise ValueError('unexpected unknown-platform manifest')
                predicates = set()
                subjects = []
                if "subject" in body:
                    descriptor(body["subject"])
                for layer in body.get('layers', []):
                    if layer.get('mediaType') != 'application/vnd.in-toto+json':
                        raise ValueError('unexpected attestation layer')
                    statement = read_json(descriptor(layer))
                    if statement.get('_type') not in ('https://in-toto.io/Statement/v0.1', 'https://in-toto.io/Statement/v1'):
                        raise ValueError('unsupported in-toto statement')
                    subject_digests = {
                        'sha256:' + subject.get('digest', {}).get('sha256', '')
                        for subject in statement.get('subject', []) if isinstance(subject, dict)
                    }
                    subjects.append(subject_digests)
                    predicate = statement.get('predicateType')
                    if isinstance(predicate, str):
                        predicates.add(predicate)
                attestations.append((entry, predicates, subjects, body.get("subject")))
            else:
                if platform.get('os') != 'linux' or platform.get('architecture') != architecture:
                    raise ValueError('OCI archive contains an unexpected runtime platform')
                labels = config.get('config', {}).get('Labels', {})
                if (config.get('os') != 'linux' or config.get('architecture') != architecture
                        or labels.get('org.opencontainers.image.version') != version
                        or labels.get('org.opencontainers.image.revision') != source
                        or labels.get('org.opencontainers.image.source') != 'https://github.com/Ledgence/ledgence'):
                    raise ValueError('runtime image identity does not match the qualified source')
                runtime.append(entry['digest'])
                runtime_configs.append(body['config']['digest'])
        if len(runtime) != 1 or len(attestations) != 1:
            raise ValueError('expected one runtime image and its attestation manifest')
        attestation, predicates, subjects, artifact_subject = attestations[0]
        if (attestation.get('annotations', {}).get('vnd.docker.reference.digest') != runtime[0]
                or any((subject and runtime[0] not in subject) or
                       (not subject and artifact_subject is None) for subject in subjects)
                or (artifact_subject is not None and artifact_subject['digest'] != runtime[0])
                or 'https://spdx.dev/Document' not in predicates
                or not any(p.startswith('https://slsa.dev/provenance/') for p in predicates)):
            raise ValueError('image must retain matching SBOM and provenance attestations')
        return {'format': 1, 'version': version, 'source_commit': source,
                'architecture': architecture, 'index_digest': root['digest'],
                'runtime_digest': runtime[0], 'runtime_config_digest': runtime_configs[0],
                'manifest_digests': sorted(entry['digest'] for entry in index['manifests']),
                'manifests': sorted(index['manifests'], key=lambda entry: entry['digest']),
                'archive_sha256': checksum(archive)}


def inspect_remote(reference):
    result = subprocess.run(['skopeo', 'inspect', '--raw', 'docker://' + reference],
                            text=True, capture_output=True, timeout=120)
    if result.returncode == 0:
        return 'sha256:' + hashlib.sha256(result.stdout.encode()).hexdigest()
    # Authentication, rate limits and connection failures must not be interpreted
    # as an absent release tag. Skopeo surfaces the registry's standard codes.
    missing = re.search(r'\b(?:manifest unknown|name unknown|MANIFEST_UNKNOWN|NAME_UNKNOWN)\b', result.stderr)
    if missing is None:
        raise RuntimeError('cannot determine existing registry identity: ' + result.stderr[-2048:])
    return None


def copy_qualified(archive, record, destination):
    current = inspect_archive(archive, version=record['version'], source=record['source_commit'],
                              architecture=record['architecture'])
    if current != record:
        raise ValueError('archive no longer matches the selected qualification record')
    existing = inspect_remote(destination)
    if existing is not None:
        if existing != record['index_digest']:
            raise ValueError('existing immutable release tag identifies different bytes')
        return
    subprocess.run(['skopeo', 'copy', '--all', '--preserve-digests', 'oci-archive:' + str(archive),
                    'docker://' + destination], check=True, timeout=1800)
    if inspect_remote(destination) != record['index_digest']:
        raise ValueError('published image digest differs from the qualified archive')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--version', required=True)
    parser.add_argument('--source', required=True)
    parser.add_argument('--architecture', choices=PLATFORMS, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    record = inspect_archive(args.archive, version=args.version, source=args.source,
                             architecture=args.architecture)
    with args.output.open('x') as stream:
        json.dump(record, stream, indent=2)
        stream.write('\n')
    print(json.dumps(record))


if __name__ == '__main__':
    main()
