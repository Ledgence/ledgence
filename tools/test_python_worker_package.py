"""Mutation checks for worker artifact and dependency policy (MIT)."""
import copy
from email.message import EmailMessage
from email.policy import default
import importlib.util
import io
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

SPEC = importlib.util.spec_from_file_location("worker_package", Path(__file__).with_name("check-python-worker.py"))
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


class WorkerPackagePolicyTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.project = gate.check_project()
        self.prefix = f"ledgence_worker-{self.project['version']}.dist-info/"
        self.files = {"ledgence/worker/" + name: (gate.WORKER / "ledgence/worker" / name).read_bytes()
                      for name in gate.MODULES}
        self.files.update({self.prefix + "METADATA": self.metadata(),
                           self.prefix + "WHEEL": b"Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
                           self.prefix + "RECORD": b"",
                           self.prefix + "licenses/LICENSE": (gate.WORKER / "LICENSE").read_bytes()})

    def metadata(self, **changes):
        message = EmailMessage()
        values = {"Metadata-Version": "2.4", "Name": "ledgence-worker", "Version": self.project["version"],
                  "Requires-Python": ">=3.11", "License-Expression": "MIT", "License-File": "LICENSE",
                  "Description-Content-Type": "text/markdown", **changes}
        for name, value in values.items():
            message[name] = value
        for label, url in self.project["urls"].items():
            message["Project-URL"] = f"{label}, {url}"
        message.set_payload((gate.WORKER / "README.md").read_text())
        return message.as_bytes(policy=default.clone(max_line_length=0))

    def wheel(self, files):
        path = self.root / "worker.whl"
        with zipfile.ZipFile(path, "w") as archive:
            for name, content in files.items():
                archive.writestr(name, content)
        return path

    def test_accepts_complete_namespace_portion(self):
        gate.verify_distribution(self.wheel(self.files))

    def test_rejects_missing_helper_module_or_typing_marker(self):
        for name in gate.MODULES:
            with self.subTest(name=name):
                files = dict(self.files)
                del files["ledgence/worker/" + name]
                with self.assertRaisesRegex(ValueError, "omits helper/legal"):
                    gate.verify_distribution(self.wheel(files))

    def test_rejects_different_runtime_or_license_bytes(self):
        for name in ("ledgence/worker/workflow.py", self.prefix + "licenses/LICENSE"):
            with self.subTest(name=name):
                files = dict(self.files)
                files[name] += b"changed"
                with self.assertRaisesRegex(ValueError, "changes source/legal"):
                    gate.verify_distribution(self.wheel(files))

    def test_rejects_root_namespace_and_unrelated_code_ownership(self):
        for name in ("ledgence/__init__.py", "ledgence/client/__init__.py", "aiohttp/__init__.py",
                     self.prefix + "entry_points.txt"):
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, "unexpected files"):
                    gate.verify_distribution(self.wheel(dict(self.files, **{name: b""})))

    def test_rejects_metadata_dependency_and_identity_drift(self):
        for changed in ({"Requires-Dist": "aiohttp>=3"}, {"Provides-Extra": "otel"},
                        {"Version": "99.0"}, {"Name": "ledgence-client"}, {"Requires-Python": ">=3.10"},
                        {"License-Expression": "Apache-2.0"}, {"License-File": "missing"}):
            with self.subTest(changed=changed):
                files = dict(self.files)
                files[self.prefix + "METADATA"] = self.metadata(**changed)
                with self.assertRaises(ValueError):
                    gate.verify_distribution(self.wheel(files))

    def test_rejects_nonportable_wheel(self):
        files = dict(self.files)
        files[self.prefix + "WHEEL"] = b"Root-Is-Purelib: false\nTag: cp313-cp313-macosx_11_0_arm64\n"
        with self.assertRaisesRegex(ValueError, "pure Python"):
            gate.verify_distribution(self.wheel(files))

    def sdist(self, files):
        path = self.root / "worker.tar.gz"
        with tarfile.open(path, "w:gz") as archive:
            for name, content in files.items():
                member = tarfile.TarInfo(f"ledgence_worker-{self.project['version']}/" + name)
                member.size = len(content)
                archive.addfile(member, io.BytesIO(content))
        return path

    def test_sdist_must_carry_self_contained_tests_and_legal_inputs(self):
        files = {name: content for name, content in self.files.items() if name.startswith("ledgence/")}
        files.update({name: (gate.WORKER / name).read_bytes()
                      for name in ("README.md", "LICENSE", "pyproject.toml", gate.SUITE)})
        files["PKG-INFO"] = self.metadata()
        gate.verify_distribution(self.sdist(files))
        for name in (gate.SUITE, "LICENSE", "pyproject.toml"):
            with self.subTest(name=name):
                missing = dict(files)
                del missing[name]
                with self.assertRaisesRegex(ValueError, "omits self-contained"):
                    gate.verify_distribution(self.sdist(missing))

    def test_rejects_unreviewed_build_runtime_and_entry_points(self):
        original = gate.tomllib.loads((gate.WORKER / "pyproject.toml").read_text())
        for section, key, value in (("project", "dependencies", ["requests"]),
                                    ("project", "optional-dependencies", {"otel": ["opentelemetry-api"]}),
                                    ("project", "scripts", {"ledgence-worker": "ledgence.worker:main"}),
                                    ("build-system", "requires", ["setuptools"])):
            with self.subTest(key=key):
                modified = copy.deepcopy(original)
                modified[section][key] = value
                with patch.object(gate.tomllib, "loads", side_effect=[modified, {"workspace": {"package": {"version": self.project["version"]}}}]):
                    with self.assertRaises(ValueError):
                        gate.check_project()


if __name__ == "__main__":
    unittest.main()
