"""Regress source identity, complete static inventory and retained legal bytes."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from console_bundle import validate, verify_bundle, digest


def distribution(directory, source='a' * 40):
    files = {'index.html': (b'<main>Console</main>', 'text/html; charset=utf-8'),
             'assets/app-12345678.js': (b'"use strict";', 'text/javascript; charset=utf-8'),
             'notices/index.html': (b'<main>Notices</main>', 'text/html; charset=utf-8'),
             'notices/LEDGENCE-LICENSE.txt': (b'MIT retained notice', 'text/plain; charset=utf-8')}
    assets = []
    for name, (data, mime) in files.items():
        file = directory / name
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(data)
        assets.append({'path': name, 'sha256': hashlib.sha256(data).hexdigest(),
                       'size_bytes': len(data), 'content_type': mime})
    manifest = {'schema_version': 1, 'console_contract_version': 3, 'console_version': '0.1.0',
                'source_revision': source, 'source_dirty': False,
                'toolchain': {'node': '24.21.0', 'pnpm': '11.27.1'}, 'lockfile_sha256': 'b' * 64, 'assets': assets}
    (directory / 'console-manifest.json').write_text(json.dumps(manifest))
    return manifest


class ConsoleBundleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.dist = self.root / 'console'
        self.manifest = distribution(self.dist)

    def test_clean_identity_and_each_asset_required(self):
        self.assertEqual(validate(self.dist, source_commit='a' * 40, version='0.1.0'), self.manifest)
        with self.assertRaisesRegex(ValueError, 'revision'):
            validate(self.dist, source_commit='c' * 40)
        (self.dist / 'assets/app-12345678.js').write_bytes(b'tampered')
        with self.assertRaisesRegex(ValueError, 'checksum'):
            validate(self.dist)

    def test_incompatible_console_contracts_are_rejected(self):
        for contract in [1, 2, 4, True, '3']:
            with self.subTest(contract=contract):
                changed = copy.deepcopy(self.manifest)
                changed['console_contract_version'] = contract
                (self.dist / 'console-manifest.json').write_text(json.dumps(changed))
                with self.assertRaisesRegex(ValueError, 'incompatible'):
                    validate(self.dist)

    def test_dirty_extra_and_legal_inventory_fail(self):
        for change in ['dirty', 'extra', 'notice']:
            with self.subTest(change=change):
                manifest = distribution(self.dist)
                if change == 'dirty':
                    manifest['source_dirty'] = True
                    (self.dist / 'console-manifest.json').write_text(json.dumps(manifest))
                elif change == 'extra':
                    (self.dist / 'unrecorded.js').write_bytes(b'unknown')
                else:
                    (self.dist / 'notices/LEDGENCE-LICENSE.txt').unlink()
                with self.assertRaises(ValueError):
                    validate(self.dist)
                (self.dist / 'unrecorded.js').unlink(missing_ok=True)

    def test_duplicate_manifest_and_symlink_fail(self):
        path = self.dist / 'console-manifest.json'
        value = path.read_text()
        path.write_text(value[:-1] + ',"source_dirty":false}')
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            validate(self.dist)
        path.write_text(value)
        asset = self.dist / 'assets/app-12345678.js'
        destination = self.root / 'outside.js'
        asset.rename(destination)
        asset.symlink_to(destination)
        with self.assertRaisesRegex(ValueError, 'asset file'):
            validate(self.dist)

    def test_bundle_requires_matching_provenance(self):
        provenance = {'source_commit': 'a' * 40, 'package_version': '0.1.0', 'console': {
            'mode': 'static', 'manifest_sha256': digest(self.dist / 'console-manifest.json')}}
        path = self.root / 'provenance.json'
        path.write_text(json.dumps(provenance))
        self.assertEqual(verify_bundle(self.root), self.manifest)
        for console in [None, {'mode': 'headless'}, {'mode': 'static', 'manifest_sha256': 'c' * 64}]:
            changed = copy.deepcopy(provenance)
            changed['console'] = console
            path.write_text(json.dumps(changed))
            with self.assertRaises(ValueError):
                verify_bundle(self.root)


if __name__ == '__main__':
    unittest.main()
