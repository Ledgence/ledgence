"""Negative checks for archive and publication-boundary mistakes."""
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('rust_packages', Path(__file__).with_name('check-rust-packages.py'))
packages = importlib.util.module_from_spec(spec)
spec.loader.exec_module(packages)


class RustPackageChecks(unittest.TestCase):
    def normalized(self):
        return {'package': {'name': packages.APIS[1], 'version': '0.1.1', 'publish': ['crates-io'],
                            'license': 'MIT', 'readme': 'README.md', 'description': 'Portable contracts',
                            'repository': 'https://github.com/Ledgence/ledgence',
                            'documentation': 'https://docs.rs/' + packages.APIS[1], 'rust-version': '1.98'},
                'dependencies': {packages.APIS[0]: {'version': '0.1.1'}}}

    def test_normalized_registry_dependency_is_accepted(self):
        packages.normalized_manifest(self.normalized(), packages.APIS[1], '0.1.1')

    def test_target_specific_path_dependency_is_rejected(self):
        data = self.normalized()
        data['target'] = {'cfg(unix)': {'dependencies': {'hidden': {'version': '1.0', 'path': '../hidden'}}}}
        with self.assertRaisesRegex(ValueError, 'independent crates.io'):
            packages.normalized_manifest(data, packages.APIS[1], '0.1.1')

    def test_workspace_patch_cannot_hide_missing_registry_package(self):
        data = self.normalized()
        data['patch'] = {'crates-io': {packages.APIS[0]: {'path': '../worker'}}}
        with self.assertRaisesRegex(ValueError, 'resolution override'):
            packages.normalized_manifest(data, packages.APIS[1], '0.1.1')

    def test_internal_link_requires_exact_worker_archive_checksum(self):
        data = {packages.APIS[0]: {'version': '0.1.1', 'sha256': 'a' * 64},
                packages.APIS[1]: {'lock': {'package': [{'name': packages.APIS[0], 'version': '0.1.1',
                                                       'source': packages.REGISTRY, 'checksum': 'b' * 64}]}}}
        with self.assertRaisesRegex(ValueError, 'exact verified worker archive'):
            packages.internal_archive_link(data)
        data[packages.APIS[1]]['lock']['package'][0]['checksum'] = 'a' * 64
        packages.internal_archive_link(data)

    def test_internal_link_rejects_hidden_path_dependency(self):
        data = {packages.APIS[0]: {'version': '0.1.1', 'sha256': 'a' * 64},
                packages.APIS[1]: {'lock': {'package': [{'name': packages.APIS[0], 'version': '0.1.1',
                                                       'source': 'path+file:///workspace', 'checksum': 'a' * 64}]}}}
        with self.assertRaises(ValueError):
            packages.internal_archive_link(data)

    def test_index_lookup_binds_name_version_and_checksum_in_both_cache_formats(self):
        wanted = {'name': 'dependency', 'version': '1.2.3', 'checksum': 'a' * 64}
        records = [
            {'name': 'other', 'vers': '1.2.3', 'cksum': 'a' * 64},
            {'name': 'dependency', 'vers': '1.2.2', 'cksum': 'a' * 64},
            {'name': 'dependency', 'vers': '1.2.3', 'cksum': 'b' * 64},
            {'name': 'dependency', 'vers': '1.2.3', 'cksum': 'a' * 64},
        ]
        encoded = [json.dumps(row).encode() for row in records]
        for content in (b'\n'.join(encoded), b'\x03\x02\x00\x00\x00etag: fixture\0' + b'\0'.join(encoded)):
            self.assertEqual(packages.cached_index_entry(content, wanted), records[-1])
            self.assertIsNone(packages.cached_index_entry(content, dict(wanted, checksum='c' * 64)))
        self.assertIsNone(packages.cached_index_entry(b'not an index', wanted))

    def write_archive(self, entries):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        path = Path(temporary.name) / 'fixture.crate'
        with tarfile.open(path, 'w:gz') as tar:
            for name, kind in entries:
                entry = tarfile.TarInfo(name)
                if kind == 'symlink':
                    entry.type = tarfile.SYMTYPE
                    entry.linkname = '/outside'
                    tar.addfile(entry)
                else:
                    entry.size = 1
                    tar.addfile(entry, io.BytesIO(b'x'))
        return path

    def test_archive_rejects_duplicate_members(self):
        archive = self.write_archive([('fixture-1.0/src/lib.rs', 'file')] * 2)
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            packages.archive_contents(archive, 'fixture-1.0')

    def test_archive_rejects_traversal(self):
        archive = self.write_archive([('fixture-1.0/../outside', 'file')])
        with self.assertRaisesRegex(ValueError, 'unsafe'):
            packages.archive_contents(archive, 'fixture-1.0')

    def test_archive_rejects_links(self):
        archive = self.write_archive([('fixture-1.0/src/lib.rs', 'symlink')])
        with self.assertRaisesRegex(ValueError, 'non-file'):
            packages.archive_contents(archive, 'fixture-1.0')

    def test_publication_allowlist_rejects_other_workspace_crates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'Cargo.toml').write_text('[workspace]\nmembers = ["private"]\n[workspace.package]\nversion = "0.1.1"\n')
            (root / 'private').mkdir()
            (root / 'private/Cargo.toml').write_text('[package]\nname = "ledgence-worker"\nversion.workspace = true\npublish = ["crates-io"]\n')
            with self.assertRaisesRegex(ValueError, 'publication policy'):
                packages.publication_policy(root)


if __name__ == '__main__':
    unittest.main()
