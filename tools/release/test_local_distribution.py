#!/usr/bin/env python3
"""Distribution identities, corruption detection, and deterministic archives."""
import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

import local_distribution as kit


class DistributionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='ledgence-kit-test-')
        self.root = Path(self.temporary.name)
        self.image = 'docker.io/ledgence/ledgence@sha256:' + 'a' * 64
        self.version = '0.3.1'

    def tearDown(self):
        self.temporary.cleanup()

    def stage(self, name='ledgence-0.3.1-local', **kwargs):
        directory = self.root / name
        kit.stage(directory, version=self.version, image=self.image, **kwargs)
        return directory

    def test_archive_is_reproducible_and_relocated_kit_is_self_contained(self):
        first = self.stage()
        with tempfile.TemporaryDirectory() as other:
            second = Path(other) / first.name
            kit.stage(second, version=self.version, image=self.image)
            a, b = self.root / 'a.tar.gz', self.root / 'b.tar.gz'
            kit.archive_tree(first, a, epoch=123456789)
            kit.archive_tree(second, b, epoch=123456789)
            self.assertEqual(a.read_bytes(), b.read_bytes())
        extracted = kit.unpack(a, self.root / 'relocated', version=self.version)
        self.assertEqual(kit.verify_directory(extracted)['image'], self.image)
        self.assertEqual({p.name for p in extracted.iterdir()}, kit.FILES | {'SHA256SUMS'})
        with tarfile.open(a) as archive:
            for member in archive:
                self.assertEqual((member.uid, member.gid, member.mtime), (0, 0, 123456789))
                self.assertEqual(member.mode, 0o755 if member.isdir() else 0o644)

    def test_digest_and_stable_version_required(self):
        for bad in ('ledgence:latest', 'ledgence/ledgence:0.3.1@sha256:' + 'a' * 64,
                    'ledgence/ledgence@sha256:' + 'a' * 63, '../ledgence@sha256:' + 'a' * 64,
                    'ledgence/ledgence@sha256:' + 'A' * 64,
                    'ledgence/ledgence@sha256:' + 'a' * 64 + '\n'):
            with self.subTest(image=bad), self.assertRaises(ValueError):
                kit.stage(self.root / 'bad', version=self.version, image=bad)
        with self.assertRaises(ValueError):
            kit.stage(self.root / 'bad', version='v0.3.1', image=self.image)
        directory = self.stage()
        with self.assertRaisesRegex(ValueError, 'version'):
            kit.verify_directory(directory, version='0.3.2')

    def test_platform_subset_is_explicit_and_exact(self):
        directory = self.stage(platforms=['linux/arm64'])
        self.assertEqual(kit.verify_directory(directory)['platforms'], ['linux/arm64'])
        for platforms in ([], ['linux/amd64', 'linux/amd64'], ['macos/arm64'],
                          ['linux/arm64', 'linux/amd64']):
            with self.subTest(platforms=platforms), self.assertRaises(ValueError):
                kit.stage(self.root / 'bad', version=self.version, image=self.image, platforms=platforms)

    def test_changed_missing_extra_and_symlink_files_are_rejected(self):
        for change in ('changed', 'missing', 'extra', 'symlink'):
            with self.subTest(change=change):
                directory = self.stage(change)
                path = directory / 'compose.yaml'
                if change == 'changed':
                    path.write_text(path.read_text().replace(self.image, 'docker.io/other/image:latest'))
                elif change == 'missing':
                    path.unlink()
                elif change == 'extra':
                    (directory / 'override.yaml').write_text('services: {}')
                else:
                    content = self.root / ('source-' + change)
                    content.write_bytes(path.read_bytes())
                    path.unlink()
                    path.symlink_to(content)
                with self.assertRaises(ValueError):
                    kit.verify_directory(directory)

    def test_duplicate_and_traversal_checksums_are_rejected(self):
        for change in ('duplicate', 'traversal'):
            directory = self.stage(change)
            sums = directory / 'SHA256SUMS'
            if change == 'duplicate':
                sums.write_text(sums.read_text() + sums.read_text().splitlines()[0] + '\n')
            else:
                sums.write_text(sums.read_text().replace('compose.yaml', '../compose.yaml'))
            with self.assertRaises(ValueError):
                kit.verify_directory(directory)

    def test_manifest_unknown_fields_and_image_disagreement_are_rejected(self):
        for change in ('unknown', 'image'):
            directory = self.stage(change)
            path = directory / 'distribution.json'
            manifest = json.loads(path.read_text())
            if change == 'unknown':
                manifest['install_command'] = 'arbitrary'
            else:
                manifest['image'] = self.image.replace('a' * 64, 'b' * 64)
            path.write_text(json.dumps(manifest))
            self.rehash(directory)
            with self.assertRaises(ValueError):
                kit.verify_directory(directory)

    def test_archive_rejects_extra_and_link_entries_without_extracting_them(self):
        directory = self.stage()
        original = self.root / 'original.tar.gz'
        kit.archive_tree(directory, original, epoch=1)
        for kind in ('traversal', 'symlink'):
            bad = self.root / (kind + '.tar.gz')
            with tarfile.open(original) as source, tarfile.open(bad, 'w:gz') as dest:
                for member in source:
                    if kind == 'symlink' and member.name.endswith('/README.md'):
                        member.type = tarfile.SYMTYPE
                        member.linkname = '/etc/passwd'
                        member.size = 0
                        dest.addfile(member)
                    else:
                        dest.addfile(member, source.extractfile(member) if member.isfile() else None)
                if kind == 'traversal':
                    member = tarfile.TarInfo('../escaped')
                    member.size = 1
                    dest.addfile(member, io.BytesIO(b'x'))
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                kit.unpack(bad, self.root / ('out-' + kind))
            self.assertFalse((self.root / 'escaped').exists())

    def test_checked_inventory_cannot_request_build_or_external_compose(self):
        directory = self.stage()
        path = directory / 'compose.yaml'
        path.write_text(path.read_text() + '\n  attacker:\n    build: /source\n')
        self.rehash(directory)
        with self.assertRaisesRegex(ValueError, 'build'):
            kit.verify_directory(directory)

    @staticmethod
    def rehash(directory):
        (directory / 'SHA256SUMS').write_text(''.join(
            f'{hashlib.sha256((directory / name).read_bytes()).hexdigest()}  {name}\n'
            for name in sorted(kit.FILES)))


if __name__ == '__main__':
    unittest.main()
