"""Release identity failures must be caught before images reach a registry."""
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import oci


class OciTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.archive = Path(self.temp.name) / 'image.tar'
        self.source = 'a' * 40

    def make_archive(self, *, version='0.4.0', architecture='arm64', sbom=True, corrupt=False,
                     attestation_target=None, statement_target=None, artifact_target=None, empty_statement_subject=False):
        files = {'oci-layout': b'{"imageLayoutVersion":"1.0.0"}'}

        def blob(value, media_type):
            data = json.dumps(value).encode()
            digest = hashlib.sha256(data).hexdigest()
            files['blobs/sha256/' + digest] = data
            return {'mediaType': media_type, 'size': len(data), 'digest': 'sha256:' + digest}

        config = blob({'os': 'linux', 'architecture': architecture, 'config': {'Labels': {
            'org.opencontainers.image.version': version,
            'org.opencontainers.image.revision': self.source,
            'org.opencontainers.image.source': 'https://github.com/Ledgence/ledgence',
        }}}, 'application/vnd.oci.image.config.v1+json')
        self.config_digest = config['digest']
        runtime = blob({'schemaVersion': 2, 'config': config, 'layers': []}, oci.MANIFEST)
        runtime['platform'] = {'os': 'linux', 'architecture': architecture}
        predicates = ['https://slsa.dev/provenance/v1'] + (['https://spdx.dev/Document'] if sbom else [])
        body = {'schemaVersion': 2, 'config': blob({}, 'application/vnd.oci.image.config.v1+json'),
                'layers': [blob({
                    '_type': 'https://in-toto.io/Statement/v1', 'predicateType': p,
                    'subject': [{'name': '_', 'digest': {'sha256': (
                        statement_target or runtime['digest']).split(':')[1]}}],
                }, 'application/vnd.in-toto+json') for p in predicates]}
        if empty_statement_subject:
            body['layers'] = [blob({'_type': 'https://in-toto.io/Statement/v1',
                                   'predicateType': p, 'subject': []},
                                  'application/vnd.in-toto+json') for p in predicates]
        if artifact_target is not None:
            body['subject'] = dict(config if artifact_target == 'config' else runtime)
        attestation = blob(body, oci.MANIFEST)
        attestation.update(platform={'os': 'unknown', 'architecture': 'unknown'}, annotations={
            'vnd.docker.reference.type': 'attestation-manifest',
            'vnd.docker.reference.digest': attestation_target or runtime['digest'],
        })
        root = blob({'schemaVersion': 2, 'manifests': [runtime, attestation]}, oci.INDEX)
        files['index.json'] = json.dumps({'schemaVersion': 2, 'manifests': [root]}).encode()
        if corrupt:
            files['blobs/sha256/' + config['digest'].split(':')[1]] = b'corrupt'
        with tarfile.open(self.archive, 'w') as tar:
            for name, data in files.items():
                item = tarfile.TarInfo(name)
                item.size = len(data)
                tar.addfile(item, io.BytesIO(data))
        return root['digest']

    def inspect(self):
        return oci.inspect_archive(self.archive, version='0.4.0', source=self.source, architecture='arm64')

    def test_complete_archive_retains_digest_and_source(self):
        expected = self.make_archive()
        record = self.inspect()
        self.assertEqual(record['index_digest'], expected)
        self.assertEqual(record['archive_sha256'], oci.checksum(self.archive))
        self.assertEqual(record['runtime_config_digest'], self.config_digest)
        self.assertEqual([entry['digest'] for entry in record['manifests']], record['manifest_digests'])
        self.assertEqual({entry['platform']['architecture'] for entry in record['manifests']}, {'arm64', 'unknown'})

    def test_wrong_identity_missing_sbom_and_corrupt_blob_are_rejected(self):
        for options in ({'version': '0.3.1'}, {'architecture': 'amd64'}, {'sbom': False},
                        {'corrupt': True}, {'attestation_target': 'sha256:' + 'b' * 64},
                        {'statement_target': 'sha256:' + 'b' * 64}, {'artifact_target': 'config'},
                        {'empty_statement_subject': True}):
            with self.subTest(options=options):
                self.make_archive(**options)
                with self.assertRaises(ValueError):
                    self.inspect()

    def test_copy_is_idempotent_only_for_identical_remote_digest(self):
        self.make_archive()
        record = self.inspect()
        with patch.object(oci, 'inspect_remote', return_value=record['index_digest']), \
                patch.object(oci.subprocess, 'run') as run:
            oci.copy_qualified(self.archive, record, 'docker.io/ledgence/ledgence:0.4.0-python3.14-arm64')
            run.assert_not_called()
        with patch.object(oci, 'inspect_remote', return_value='sha256:' + 'b' * 64), \
                patch.object(oci.subprocess, 'run') as run:
            with self.assertRaisesRegex(ValueError, 'existing immutable'):
                oci.copy_qualified(self.archive, record, 'docker.io/ledgence/ledgence:0.4.0-python3.14-arm64')
            run.assert_not_called()

    def test_copy_preserves_all_digests_and_verifies_result(self):
        self.make_archive()
        record = self.inspect()
        with patch.object(oci, 'inspect_remote', side_effect=[None, record['index_digest']]), \
                patch.object(oci.subprocess, 'run') as run:
            oci.copy_qualified(self.archive, record, 'docker.io/ledgence/ledgence:0.4.0-python3.14-arm64')
            self.assertIn('--preserve-digests', run.call_args.args[0])
            self.assertIn('--all', run.call_args.args[0])

    def test_oci_artifact_subject_matches_its_runtime(self):
        self.make_archive(artifact_target='runtime')
        self.inspect()

    def test_current_buildkit_binds_empty_statements_through_artifact_subject(self):
        self.make_archive(artifact_target='runtime', empty_statement_subject=True)
        self.inspect()

    def test_inspect_remote_hashes_exact_raw_manifest(self):
        raw = '{\n  "schemaVersion": 2, "manifests": []\n}'
        result = subprocess.CompletedProcess([], 0, raw, '')
        with patch.object(oci.subprocess, 'run', return_value=result):
            self.assertEqual(oci.inspect_remote('docker.io/ledgence/ledgence:0.4.0'),
                             'sha256:' + hashlib.sha256(raw.encode()).hexdigest())

    def test_registry_failure_is_not_treated_as_missing_image(self):
        for diagnostic in ('unauthorized', 'TOOMANYREQUESTS', 'connection refused'):
            result = subprocess.CompletedProcess([], 1, '', diagnostic)
            with self.subTest(diagnostic=diagnostic), patch.object(oci.subprocess, 'run', return_value=result):
                with self.assertRaises(RuntimeError):
                    oci.inspect_remote('docker.io/ledgence/ledgence:0.4.0')
        result = subprocess.CompletedProcess([], 1, '', 'reading manifest: manifest unknown')
        with patch.object(oci.subprocess, 'run', return_value=result):
            self.assertIsNone(oci.inspect_remote('docker.io/ledgence/ledgence:0.4.0'))


if __name__ == '__main__':
    unittest.main()
