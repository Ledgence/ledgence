"""Offline regression tests for the demo's fail-closed dependency verification."""

import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

SPEC = importlib.util.spec_from_file_location("support_dependency_verify", Path(__file__).with_name("verify.py"))
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class VerificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.review = self.root / "third_party"
        self.review.mkdir()
        self.wheels = self.root / "wheels"
        self.wheels.mkdir()
        self.installed = self.root / "installed"
        self.installed.mkdir()
        self.target = "test-cp313"
        self.filename = "sample-1.0-py3-none-any.whl"
        self.metadata = b"Name: sample\nVersion: 1.0\nLicense-Expression: MIT\nRequires-Python: >=3.13\n\n"
        self.contents = {"sample/__init__.py": b"VALUE = 1\n",
                         "sample-1.0.dist-info/METADATA": self.metadata,
                         "sample-1.0.dist-info/LICENSE": b"Original MIT notice\n",
                         "sample-1.0.dist-info/RECORD": b""}
        with zipfile.ZipFile(self.wheels / self.filename, "w") as wheel:
            for name, content in self.contents.items():
                wheel.writestr(name, content)
                path = self.installed / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
        artifact = {"filename": self.filename, "sha256": self.hash((self.wheels / self.filename).read_bytes()),
                    "size": (self.wheels / self.filename).stat().st_size, "targets": [self.target],
                    "requires_python": ">=3.13", "requires_dist": [], "license_expression": "MIT",
                    "license": None, "native_files": [], "license_files": {
                        "sample-1.0.dist-info/LICENSE": {"path": "LICENSE", "sha256": self.hash(b"Original MIT notice\n")}}}
        (self.review / "LICENSE").write_bytes(b"Original MIT notice\n")
        (self.review / "SUPPLEMENTAL-LICENSES.txt").write_bytes(b"Supplement\n")
        self.inventory = {"schema_version": 1, "targets": [self.target], "roots": ["sample"],
                          "packages": [{"name": "sample", "version": "1.0", "artifacts": [artifact],
                                        "source": {"license_files": {}}, "active_dependencies": {self.target: []}}],
                          "embedded_rust": [], "supplemental_sources": [], "certifi_source_equivalence": {},
                          "supplemental_bundle": {"path": "SUPPLEMENTAL-LICENSES.txt", "sha256": self.hash(b"Supplement\n")}}
        self.save()
        self.lock = self.root / f"requirements-{self.target}.txt"
        self.lock.write_text(f"sample==1.0 --hash=sha256:{artifact['sha256']}\n")

    @staticmethod
    def hash(data):
        return hashlib.sha256(data).hexdigest()

    def save(self):
        (self.review / "inventory.json").write_text(json.dumps(self.inventory))

    def verify(self):
        return MODULE.verify(self.target, self.wheels, self.installed, self.review)

    def test_verified_wheel_and_installed_source(self):
        self.assertTrue(self.verify()["installed_verified"])

    def test_changed_wheel_is_rejected_before_import(self):
        with (self.wheels / self.filename).open("ab") as stream:
            stream.write(b"changed")
        with self.assertRaisesRegex(ValueError, "wheel size/hash mismatch"):
            self.verify()

    def test_unreviewed_artifact_is_rejected(self):
        (self.wheels / "extra.whl").write_bytes(b"unreviewed")
        with self.assertRaisesRegex(ValueError, "exactly the reviewed"):
            self.verify()

    def test_modified_source_is_rejected(self):
        (self.installed / "sample/__init__.py").write_text("VALUE = 2\n")
        with self.assertRaisesRegex(ValueError, "upstream file missing or changed"):
            self.verify()

    def test_unreviewed_import_hook_is_rejected(self):
        (self.installed / "unexpected.pth").write_text("import something\n")
        with self.assertRaisesRegex(ValueError, "unexpected installed file"):
            self.verify()

    def test_missing_legal_text_is_rejected(self):
        (self.review / "LICENSE").unlink()
        with self.assertRaisesRegex(ValueError, "missing original legal"):
            self.verify()

    def test_changed_supplement_is_rejected(self):
        (self.review / "SUPPLEMENTAL-LICENSES.txt").write_text("changed")
        with self.assertRaisesRegex(ValueError, "supplemental notice bundle changed"):
            self.verify()

    def test_changed_pin_is_rejected(self):
        self.lock.write_text("sample==2.0\n")
        with self.assertRaisesRegex(ValueError, "requirements lock differs"):
            self.verify()

    def test_broken_dependency_closure_is_rejected(self):
        self.inventory["packages"][0]["active_dependencies"][self.target] = ["absent"]
        self.save()
        with self.assertRaisesRegex(ValueError, "missing dependency"):
            self.verify()

    def test_metadata_and_reviewed_edges_cannot_drift(self):
        self.inventory["packages"][0]["artifacts"][0]["requires_dist"] = ["unreviewed>=1"]
        self.save()
        with self.assertRaisesRegex(ValueError, "dependency metadata changed"):
            self.verify()

    def test_certifi_source_equivalence_is_enforced(self):
        self.inventory["certifi_source_equivalence"] = {"sample/__init__.py": "0" * 64}
        self.save()
        with self.assertRaisesRegex(ValueError, "certifi source equivalence changed"):
            self.verify()

    def test_installer_metadata_is_tolerated(self):
        (self.installed / "sample-1.0.dist-info/INSTALLER").write_text("pip\n")
        (self.installed / "sample-1.0.dist-info/REQUESTED").touch()
        self.assertTrue(self.verify()["installed_verified"])


if __name__ == "__main__":
    unittest.main()
