"""Publication preflights preserve immutable releases and require full evidence."""
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import publish_images as publication


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def checks(self, directory):
        (directory / 'SHA256SUMS').write_text(''.join(
            f'{publication.oci.checksum(path)}  {path.relative_to(directory)}\n'
            for path in sorted(directory.rglob('*')) if path.is_file() and path != directory / 'SHA256SUMS'))

    def test_checksum_inventory_rejects_changed_extra_and_missing_files(self):
        artifact = self.root / 'artifact'
        artifact.mkdir()
        (artifact / 'image.tar').write_bytes(b'qualified bytes')
        self.checks(artifact)
        publication.verify_checksums(artifact)
        (artifact / 'image.tar').write_bytes(b'different bytes')
        with self.assertRaisesRegex(ValueError, 'checksum mismatch'):
            publication.verify_checksums(artifact)
        self.checks(artifact)
        (artifact / 'extra').touch()
        with self.assertRaisesRegex(ValueError, 'inventory differs'):
            publication.verify_checksums(artifact)

    def test_checksum_paths_cannot_leave_the_qualified_artifact(self):
        for name in ('../image.tar', '/image.tar', 'x/../image.tar', 'x//image.tar'):
            with self.subTest(name=name):
                (self.root / 'SHA256SUMS').write_text('a' * 64 + '  ' + name + '\n')
                with self.assertRaisesRegex(ValueError, 'checksum path'):
                    publication.verify_checksums(self.root)

    def evidence(self, *, passed=True, cleanup=True, cli=True, source_id=None):
        records = {}
        for arch in ('amd64', 'arm64'):
            directory = self.root / ('oci-linux-' + arch)
            directory.mkdir()
            (directory / 'acceptance').mkdir()
            (directory / 'sources').mkdir()
            (directory / 'image.tar').write_bytes(b'tested archive')
            record = {'index_digest': 'sha256:' + 'a' * 64, 'architecture': arch,
                      'runtime_config_digest': 'sha256:' + 'b' * 64,
                      'runtime_digest': 'sha256:' + 'e' * 64}
            records[arch] = record
            (directory / 'qualification.json').write_text(json.dumps(record))
            (directory / 'acceptance/report.json').write_text(json.dumps({
                'passed': passed, 'cleanup_complete': cleanup,
                'distribution': {'image': '127.0.0.1:5000/ledgence@' + record['index_digest']},
                'runtime': {'platform': 'linux/' + arch}, 'cli': {'passed': cli},
            }))
            self.checks(directory)
        return records, {'image': {'id': source_id or 'sha256:' + 'b' * 64}}

    def test_source_inventory_must_identify_actual_runtime_config(self):
        records, sources = self.evidence(source_id='sha256:' + 'c' * 64)
        with patch.object(publication.oci, 'inspect_archive', side_effect=lambda *a, **kw: records[kw['architecture']]), \
                patch.object(publication.image_sources, 'verify_output', return_value=sources):
            with self.assertRaisesRegex(ValueError, 'different runtime'):
                publication.verify_inputs(self.root, '0.4.0', 'd' * 40, 'ledgence')

    def test_cli_and_cleanup_are_required_gates(self):
        records, sources = self.evidence(cli=False)
        with patch.object(publication.oci, 'inspect_archive', side_effect=lambda *a, **kw: records[kw['architecture']]), \
                patch.object(publication.image_sources, 'verify_output', return_value=sources):
            with self.assertRaisesRegex(ValueError, 'acceptance'):
                publication.verify_inputs(self.root, '0.4.0', 'd' * 40, 'ledgence')
            path = self.root / 'oci-linux-amd64/acceptance/report.json'
            value = json.loads(path.read_text())
            value['cli']['passed'] = True
            value['cleanup_complete'] = False
            path.write_text(json.dumps(value))
            self.checks(path.parents[1])
            with self.assertRaisesRegex(ValueError, 'acceptance'):
                publication.verify_inputs(self.root, '0.4.0', 'd' * 40, 'ledgence')

    def test_classic_and_containerd_docker_image_ids_are_supported(self):
        records, sources = self.evidence()
        for digest in ('b', 'e', 'a'):
            with self.subTest(digest=digest), \
                    patch.object(publication.oci, 'inspect_archive', side_effect=lambda *a, **kw: records[kw['architecture']]), \
                    patch.object(publication.image_sources, 'verify_output', return_value={'image': {'id': 'sha256:' + digest * 64}}):
                self.assertEqual(publication.verify_inputs(self.root, '0.4.0', 'd' * 40, 'ledgence'), records)

    def test_sources_cannot_be_left_in_a_private_draft(self):
        release = {'isDraft': True, 'tagName': 'v0.4.0', 'assets': []}
        with patch.object(publication.subprocess, 'check_output', return_value=json.dumps(release)), \
                patch.object(publication.subprocess, 'run') as run:
            with self.assertRaisesRegex(ValueError, 'public versioned'):
                publication.publish_assets([], '0.4.0')
            run.assert_not_called()

    def test_index_checks_platform_metadata_as_well_as_content_digest(self):
        entry = {'digest': 'sha256:' + 'a' * 64, 'size': 123, 'mediaType': publication.oci.MANIFEST,
                 'platform': {'os': 'linux', 'architecture': 'amd64'}}
        document = {'schemaVersion': 2, 'mediaType': publication.oci.INDEX, 'manifests': [entry]}
        raw = json.dumps(document).encode()
        with patch.object(publication.subprocess, 'check_output', return_value=raw):
            self.assertEqual(publication.verify_index('example/image:0.4.0', [entry]),
                             'sha256:' + hashlib.sha256(raw).hexdigest())
            wrong = {**entry, 'platform': {'os': 'linux', 'architecture': 'arm64'}}
            with self.assertRaisesRegex(ValueError, 'release index differs'):
                publication.verify_index('example/image:0.4.0', [wrong])


if __name__ == '__main__':
    unittest.main()
