"""Negative checks for archive and publication-boundary mistakes."""
import importlib.util
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('rust_packages', Path(__file__).with_name('check-rust-packages.py'))
packages = importlib.util.module_from_spec(spec)
spec.loader.exec_module(packages)


class RustPackageChecks(unittest.TestCase):
    def registry_fixture(self, directory):
        output, target, cache = (directory / name for name in ('evidence', 'target', 'original-cache'))
        output.mkdir()
        temporary_registry = target / 'package/tmp-registry'
        temporary_registry.mkdir(parents=True)
        dependency = {'name': 'cross-target', 'version': '1.2.3', 'source': packages.REGISTRY,
                      'checksum': packages.digest(b'exact dependency archive')}
        qualified = {}
        for name in packages.APIS:
            content = name.encode()
            (temporary_registry / f'{name}-0.7.0.crate').write_bytes(content)
            qualified[name] = {'version': '0.7.0', 'sha256': packages.digest(content),
                               'lock': {'package': [dependency]}}
        return output, target, cache, qualified, dependency

    def cached_dependency(self, home, dependency, *, content=b'exact dependency archive', index=True,
                          checksum=None):
        archive = home / 'registry/cache/fixture' / f"{dependency['name']}-{dependency['version']}.crate"
        archive.parent.mkdir(parents=True, exist_ok=True)
        archive.write_bytes(content)
        if index:
            entry = home / 'registry/index/fixture/.cache' / packages.index_path(dependency['name'])
            entry.parent.mkdir(parents=True, exist_ok=True)
            entry.write_text(json.dumps({'name': dependency['name'], 'vers': dependency['version'],
                                         'cksum': checksum or dependency['checksum']}))

    def test_cached_archives_never_start_a_network_prefetch(self):
        with tempfile.TemporaryDirectory() as directory:
            output, target, cache, qualified, dependency = self.registry_fixture(Path(directory))
            self.cached_dependency(cache, dependency)
            with patch.dict(os.environ, {'CARGO_HOME': str(cache)}), patch.object(packages.subprocess, 'run') as run:
                registry, _, inventory, prefetch = packages.fixture_registry(output, target, qualified, '1.98.1')
            run.assert_not_called()
            self.assertEqual(prefetch['packages'], [])
            self.assertFalse((output / 'archive-prefetch').exists())
            self.assertEqual(inventory['cross-target-1.2.3.crate'], dependency['checksum'])
            self.assertEqual((registry / 'cross-target-1.2.3.crate').read_bytes(), b'exact dependency archive')

    def test_missing_cross_target_archive_uses_exact_isolated_cargo_prefetch(self):
        with tempfile.TemporaryDirectory() as directory:
            output, target, cache, qualified, dependency = self.registry_fixture(Path(directory))
            # An unpacked source is insufficient; the exact source archive is required.
            unpacked = cache / 'registry/src/fixture/cross-target-1.2.3'
            unpacked.mkdir(parents=True)
            (unpacked / 'Cargo.toml').write_text('cached source must not substitute for an archive')
            locked = target / 'Cargo.lock'
            locked.write_bytes(b'original packaged lock bytes')

            def fetch(command, **kwargs):
                self.assertEqual(command, ['cargo', '+1.98.1', 'info', 'cross-target@1.2.3',
                                           '--registry', 'crates-io', '--color', 'never'])
                self.assertEqual(kwargs['cwd'], output / 'archive-prefetch/empty')
                self.assertEqual(list(kwargs['cwd'].iterdir()), [])
                self.assertEqual(kwargs['timeout'], 120)
                self.assertTrue(kwargs['check'])
                fetched_home = Path(kwargs['env']['CARGO_HOME'])
                self.assertNotEqual(fetched_home, cache)
                self.cached_dependency(fetched_home, dependency)

            with patch.dict(os.environ, {'CARGO_HOME': str(cache)}), patch.object(packages.subprocess, 'run', side_effect=fetch) as run:
                registry, _, _, prefetch = packages.fixture_registry(output, target, qualified, '1.98.1')
            self.assertEqual(run.call_count, 1)
            self.assertEqual(prefetch['packages'][0]['checksum'], dependency['checksum'])
            self.assertEqual((registry / 'cross-target-1.2.3.crate').read_bytes(), b'exact dependency archive')
            self.assertEqual(locked.read_bytes(), b'original packaged lock bytes')
            self.assertEqual(list((output / 'archive-prefetch/empty').iterdir()), [])
            self.assertIn('cross-target@1.2.3', (output / 'cargo-archive-prefetch.log').read_text())

    def test_prefetched_archive_must_match_the_unchanged_packaged_checksum(self):
        with tempfile.TemporaryDirectory() as directory:
            output, target, cache, qualified, dependency = self.registry_fixture(Path(directory))
            def fetch(command, **kwargs):
                self.cached_dependency(Path(kwargs['env']['CARGO_HOME']), dependency, content=b'wrong bytes')
            with patch.dict(os.environ, {'CARGO_HOME': str(cache)}), patch.object(packages.subprocess, 'run', side_effect=fetch):
                with self.assertRaisesRegex(ValueError, 'exact locked archive missing.*after prefetch'):
                    packages.fixture_registry(output, target, qualified, '1.98.1')
            self.assertFalse((output / 'fixture-registry/cross-target-1.2.3.crate').exists())

    def test_cached_archive_without_matching_index_fails_without_network(self):
        for index, checksum in ((False, None), (True, 'b' * 64)):
            with self.subTest(index=index), tempfile.TemporaryDirectory() as directory:
                output, target, cache, qualified, dependency = self.registry_fixture(Path(directory))
                self.cached_dependency(cache, dependency, index=index, checksum=checksum)
                with patch.dict(os.environ, {'CARGO_HOME': str(cache)}), patch.object(packages.subprocess, 'run') as run:
                    with self.assertRaisesRegex(ValueError, 'exact locked index entry missing'):
                        packages.fixture_registry(output, target, qualified, '1.98.1')
                run.assert_not_called()

    def test_prefetched_archive_without_index_proof_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            output, target, cache, qualified, dependency = self.registry_fixture(Path(directory))
            def fetch(command, **kwargs):
                self.cached_dependency(Path(kwargs['env']['CARGO_HOME']), dependency, index=False)
            with patch.dict(os.environ, {'CARGO_HOME': str(cache)}), patch.object(packages.subprocess, 'run', side_effect=fetch):
                with self.assertRaisesRegex(ValueError, 'exact locked index entry missing'):
                    packages.fixture_registry(output, target, qualified, '1.98.1')

    def test_prefetch_cannot_inherit_a_cargo_project(self):
        with tempfile.TemporaryDirectory() as directory:
            output, target, cache, qualified, _ = self.registry_fixture(Path(directory))
            (Path(directory) / 'Cargo.toml').write_text('[workspace]\n')
            with patch.dict(os.environ, {'CARGO_HOME': str(cache)}), patch.object(packages.subprocess, 'run') as run:
                with self.assertRaisesRegex(ValueError, 'outside every Cargo workspace'):
                    packages.fixture_registry(output, target, qualified, '1.98.1')
            run.assert_not_called()

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

    def test_archive_requires_nested_historical_fixtures(self):
        name, version, commit = packages.APIS[1], '0.1.1', 'a' * 40
        historical = 'tests/fixtures/historical/console-v3.json'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'crates' / name
            source.mkdir(parents=True)
            (root / 'LICENSE').write_text('MIT fixture notice\n')
            files = {
                'src/lib.rs': b'pub fn contract() {}\n',
                'tests/contracts.rs': b'const HISTORICAL: &str = include_str!("fixtures/historical/console-v3.json");\n',
                historical: b'{"contract_version":3}\n',
                'README.md': b'Portable contract fixture\n',
                'LICENSE': (root / 'LICENSE').read_bytes(),
                'Cargo.toml': b'[package]\nname = "ledgence-orchestration-api"\n',
            }
            for relative, content in files.items():
                path = source / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
            contents = dict(files)
            contents['Cargo.toml.orig'] = contents['Cargo.toml']
            contents['Cargo.toml'] = b"""[package]
name = "ledgence-orchestration-api"
version = "0.1.1"
publish = ["crates-io"]
license = "MIT"
readme = "README.md"
description = "Portable contracts"
repository = "https://github.com/Ledgence/ledgence"
documentation = "https://docs.rs/ledgence-orchestration-api"
rust-version = "1.98"
[dependencies.ledgence-worker-api]
version = "0.1.1"
"""
            contents['Cargo.lock'] = b'[[package]]\nname = "ledgence-orchestration-api"\nversion = "0.1.1"\n'
            contents['.cargo_vcs_info.json'] = json.dumps({
                'git': {'sha1': commit}, 'path_in_vcs': 'crates/' + name,
            }).encode()
            archive = root / 'fixture.crate'

            def package(omit_historical):
                with tarfile.open(archive, 'w:gz') as tar:
                    for relative, content in contents.items():
                        if omit_historical and relative == historical:
                            continue
                        entry = tarfile.TarInfo(f'{name}-{version}/{relative}')
                        entry.size = len(content)
                        tar.addfile(entry, io.BytesIO(content))

            package(omit_historical=True)
            with self.assertRaisesRegex(ValueError, 'source archive inventory differs'):
                packages.inspect_archive(archive, root, name, version, commit, False)
            package(omit_historical=False)
            qualified = packages.inspect_archive(archive, root, name, version, commit, False)
            self.assertEqual(qualified['files'][historical], packages.digest(files[historical]))

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


    def test_publication_policy_supports_optional_private_adapters(self):
        # The publication boundary is the API allowlist, not a stale workspace
        # crate count. Test both small and expanded private implementation sets.
        for private_count in (1, 17):
            with self.subTest(private_count=private_count), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                names = [*packages.APIS, *[f"ledgence-private-{i}" for i in range(private_count)]]
                members = ", ".join(json.dumps(name) for name in names)
                dependencies = "".join(f'{name} = {{ version = "0.3.0", path = "{name}" }}\n' for name in names)
                (root / 'Cargo.toml').write_text(f'[workspace]\nmembers = [{members}]\n'
                    '[workspace.package]\nversion = "0.3.0"\n[workspace.dependencies]\n' + dependencies)
                (root / 'Cargo.lock').write_text("".join(
                    f'[[package]]\nname = "{name}"\nversion = "0.3.0"\n' for name in names))
                for name in names:
                    (root / name).mkdir()
                    policy = '["crates-io"]' if name in packages.APIS else 'false'
                    (root / name / 'Cargo.toml').write_text(
                        f'[package]\nname = "{name}"\nversion.workspace = true\npublish = {policy}\n')
                version, discovered = packages.publication_policy(root)
                self.assertEqual(version, '0.3.0')
                self.assertEqual(set(discovered), set(names))
                private = root / names[-1] / 'Cargo.toml'
                private.write_text(private.read_text().replace('publish = false', 'publish = ["crates-io"]'))
                with self.assertRaisesRegex(ValueError, 'publication policy'):
                    packages.publication_policy(root)

    def test_publication_policy_requires_both_api_crates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'Cargo.toml').write_text('[workspace]\nmembers = []\n[workspace.package]\nversion = "0.3.0"\n')
            with self.assertRaisesRegex(ValueError, 'missing public API'):
                packages.publication_policy(root)


if __name__ == '__main__':
    unittest.main()
