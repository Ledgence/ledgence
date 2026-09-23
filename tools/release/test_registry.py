"""Failure-focused checks for the registry release gates; no external services."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("registry", Path(__file__).with_name("registry.py"))
registry = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(registry)


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.version = "0.1.1"
        (self.root / "Cargo.toml").write_text('[workspace.package]\nversion="0.1.1"\n')
        (self.root / "sdk/python-client").mkdir(parents=True)
        (self.root / "sdk/python-client/pyproject.toml").write_text(
            '[project]\nname="ledgence-client"\nversion="0.1.1"\n')
        self.git("init", "-b", "main")
        self.git("config", "user.email", "test@example.invalid")
        self.git("config", "user.name", "Registry test")
        self.git("add", ".")
        self.git("commit", "-m", "Fixture")
        self.git("tag", "-a", "v0.1.1", "-m", "Release")
        self.git("update-ref", "refs/remotes/origin/main", "HEAD")
        self.env = {"GITHUB_REPOSITORY": "Ledgence/ledgence", "GITHUB_REF": "refs/tags/v0.1.1"}

    def git(self, *args):
        subprocess.run(["git", *args], cwd=self.root, check=True, capture_output=True)

    def test_clean_tag_on_main_passes(self):
        self.assertEqual(registry.check_source(True, self.root, self.env), self.version)

    def test_dirty_tree_rejected_even_for_qualification(self):
        (self.root / "uncommitted").write_text("new")
        with self.assertRaisesRegex(ValueError, "clean"):
            registry.check_source(False, self.root)

    def test_branch_and_fork_cannot_publish(self):
        for env in ({**self.env, "GITHUB_REF": "refs/heads/develop"},
                    {**self.env, "GITHUB_REPOSITORY": "fork/ledgence"}):
            with self.subTest(env=env), self.assertRaises(ValueError):
                registry.check_source(True, self.root, env)

    def test_lightweight_tag_rejected(self):
        self.git("tag", "-d", "v0.1.1")
        self.git("tag", "v0.1.1")
        with self.assertRaisesRegex(ValueError, "annotated"):
            registry.check_source(True, self.root, self.env)

    def test_unpromoted_commit_rejected(self):
        self.git("commit", "--allow-empty", "-m", "Not on main")
        self.git("tag", "-fa", "v0.1.1", "-m", "Unpromoted")
        with self.assertRaises(subprocess.CalledProcessError):
            registry.check_source(True, self.root, self.env)

    def test_wrong_checkout_rejected(self):
        self.git("commit", "--allow-empty", "-m", "Later")
        with self.assertRaisesRegex(ValueError, "checkout"):
            registry.check_source(True, self.root, self.env)

    def test_version_mismatch_rejected(self):
        (self.root / "sdk/python-client/pyproject.toml").write_text(
            '[project]\nname="ledgence-client"\nversion="0.1.2"\n')
        with self.assertRaisesRegex(ValueError, "versions differ"):
            registry.release_version(self.root)

    def artifacts(self, name):
        directory = self.root / name
        directory.mkdir()
        for filename in ("ledgence_client-0.1.1-py3-none-any.whl", "ledgence_client-0.1.1.tar.gz"):
            (directory / filename).write_bytes(b"qualified bytes")
        return directory

    def test_changed_or_extra_artifacts_rejected(self):
        first, second = self.artifacts("a"), self.artifacts("b")
        registry.compare(first, second, self.version, "pypi")
        (second / "ledgence_client-0.1.1.tar.gz").write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "differ"):
            registry.compare(first, second, self.version, "pypi")
        (first / "unexpected.whl").touch()
        with self.assertRaisesRegex(ValueError, "file set"):
            registry.inventory(first, self.version, "pypi")

    def test_pypi_download_is_checked_beyond_registry_metadata(self):
        directory = self.artifacts("dist")
        hashes = registry.inventory(directory, self.version, "pypi")
        metadata = {"info": {"version": self.version}, "urls": [
            {"filename": name, "digests": {"sha256": digest},
             "url": f"https://files.pythonhosted.org/{name}", "yanked": False}
            for name, digest in hashes.items()]}
        with patch.object(registry, "get", side_effect=[json.dumps(metadata).encode(), b"wrong bytes"]):
            with self.assertRaisesRegex(ValueError, "downloaded bytes differ"):
                registry.verify_registry(directory, self.version, "pypi")

    def test_yanked_and_mismatched_crates_are_rejected(self):
        directory = self.root / "crates"
        directory.mkdir()
        for name in registry.CRATES:
            (directory / f"{name}-0.1.1.crate").write_bytes(b"qualified")
        for data in ({"num": self.version, "yanked": True, "checksum": "x"},
                     {"num": self.version, "yanked": False, "checksum": "x"}):
            with self.subTest(data=data), patch.object(registry, "get", return_value=json.dumps({"version": data}).encode()):
                with self.assertRaises(ValueError):
                    registry.verify_registry(directory, self.version, "crates")


    def pypi_metadata(self, directory, names=None):
        hashes = registry.inventory(directory, self.version, "pypi")
        return json.dumps({"info": {"version": self.version}, "urls": [
            {"filename": name, "digests": {"sha256": digest},
             "url": f"https://files.pythonhosted.org/{name}", "yanked": False}
            for name, digest in hashes.items() if names is None or name in names]}).encode()

    def test_first_publication_stages_all_files(self):
        directory = self.artifacts("dist")
        output = self.root / "pending"
        with patch.object(registry, "get", return_value=None):
            result = registry.prepare_publication(directory, output, self.version, "pypi")
        self.assertEqual(set(result["missing"]), set(registry.inventory(directory, self.version, "pypi")))
        self.assertEqual(registry.inventory(output, self.version, "pypi"),
                         registry.inventory(directory, self.version, "pypi"))
        self.assertEqual(registry.publication_outputs(result), "upload=true\npackages=\n")

    def test_partial_pypi_publication_only_stages_missing_sdist(self):
        directory = self.artifacts("dist")
        wheel = "ledgence_client-0.1.1-py3-none-any.whl"
        sdist = "ledgence_client-0.1.1.tar.gz"
        output = self.root / "pending"
        with patch.object(registry, "get", side_effect=[self.pypi_metadata(directory, [wheel]), b"qualified bytes"]):
            result = registry.prepare_publication(directory, output, self.version, "pypi")
        self.assertEqual(result["missing"], [sdist])
        self.assertEqual(list(result["existing_sha256"]), [wheel])
        self.assertEqual([path.name for path in output.iterdir()], [sdist])

    def test_complete_publication_skips_upload_but_rechecks_both_downloads(self):
        directory = self.artifacts("dist")
        output = self.root / "pending"
        with patch.object(registry, "get", side_effect=[self.pypi_metadata(directory), b"qualified bytes", b"qualified bytes"]) as get:
            result = registry.prepare_publication(directory, output, self.version, "pypi")
        self.assertEqual(get.call_count, 3)
        self.assertEqual(result["missing"], [])
        self.assertEqual(list(output.iterdir()), [])
        self.assertEqual(registry.publication_outputs(result), "upload=false\npackages=\n")

    def test_partial_publication_cannot_skip_mismatched_existing_bytes(self):
        directory = self.artifacts("dist")
        output = self.root / "pending"
        wheel = "ledgence_client-0.1.1-py3-none-any.whl"
        with patch.object(registry, "get", side_effect=[self.pypi_metadata(directory, [wheel]), b"different bytes"]):
            with self.assertRaisesRegex(ValueError, "downloaded bytes differ"):
                registry.prepare_publication(directory, output, self.version, "pypi")
        self.assertFalse(output.exists())

    def test_unknown_existing_file_prevents_resume(self):
        directory = self.artifacts("dist")
        metadata = json.loads(self.pypi_metadata(directory))
        metadata["urls"].append({"filename": "unexpected.whl"})
        with patch.object(registry, "get", return_value=json.dumps(metadata).encode()):
            with self.assertRaisesRegex(ValueError, "file set"):
                registry.prepare_publication(directory, self.root / "pending", self.version, "pypi")

    def test_partial_crates_publication_only_selects_missing_dependency_consumer(self):
        directory = self.root / "crates"
        directory.mkdir()
        for name in registry.CRATES:
            (directory / f"{name}-0.1.1.crate").write_bytes(b"qualified")
        hashes = registry.inventory(directory, self.version, "crates")
        metadata = json.dumps({"version": {"num": self.version, "yanked": False,
            "checksum": hashes["ledgence-worker-api-0.1.1.crate"]}}).encode()
        with patch.object(registry, "get", side_effect=[metadata, b"qualified", None]):
            result = registry.prepare_publication(directory, self.root / "pending", self.version, "crates")
        self.assertEqual(result["packages"], ["ledgence-orchestration-api"])
        self.assertEqual(registry.publication_outputs(result),
                         "upload=true\npackages=ledgence-orchestration-api\n")

    def test_missing_artifacts_are_rejected_by_final_verification(self):
        directory = self.artifacts("dist")
        wheel = "ledgence_client-0.1.1-py3-none-any.whl"
        with patch.object(registry, "get", side_effect=[self.pypi_metadata(directory, [wheel]), b"qualified bytes"]):
            with self.assertRaisesRegex(ValueError, "file set"):
                registry.verify_registry(directory, self.version, "pypi")

    def test_preflight_not_found_does_not_retry_or_hide_other_http_errors(self):
        url = "https://pypi.org/pypi/ledgence-client/0.1.1/json"
        missing = urllib.error.HTTPError(url, 404, "Not Found", {}, None)
        with patch.object(registry.urllib.request, "urlopen", side_effect=missing) as request:
            self.assertIsNone(registry.get(url, missing_ok=True))
            self.assertEqual(request.call_count, 1)
        denied = urllib.error.HTTPError(url, 403, "Forbidden", {}, None)
        with patch.object(registry.urllib.request, "urlopen", side_effect=denied):
            with self.assertRaises(urllib.error.HTTPError):
                registry.get(url, missing_ok=True)


    def test_fresh_cargo_install_checks_registry_version_and_qualified_checksum(self):
        directory = self.root / "qualified-crates"
        directory.mkdir()
        for name in registry.CRATES:
            (directory / f"{name}-0.1.1.crate").write_bytes(b"qualified")
        hashes = registry.inventory(directory, self.version, "crates")
        for fault in (None, "version", "checksum", "source"):
            def cargo(*command, cwd, env):
                self.assertEqual(command[:3], ("cargo", "+1.98.1", "run"))
                self.assertEqual(env["CARGO_REGISTRIES_CRATES_IO_PROTOCOL"], "sparse")
                self.assertEqual(env["CARGO_REGISTRIES_CRATES_IO_INDEX"], "sparse+https://index.crates.io/")
                self.assertNotIn("CARGO_NET_OFFLINE", env)
                packages = []
                for name in registry.CRATES:
                    version = "0.1.0" if fault == "version" else self.version
                    checksum = "changed" if fault == "checksum" else hashes[f"{name}-0.1.1.crate"]
                    source = ("registry+https://example.invalid/index" if fault == "source" else
                              "registry+https://github.com/rust-lang/crates.io-index")
                    packages.append("[[package]]\n" + "\n".join([
                        f'name = "{name}"', f'version = "{version}"',
                        f'checksum = "{checksum}"', f'source = "{source}"',
                    ]))
                (cwd / "Cargo.lock").write_text("\n".join(packages))
            with self.subTest(fault=fault), patch.dict(os.environ, {
                    "CARGO_REGISTRIES_CRATES_IO_PROTOCOL": "git",
                    "CARGO_REGISTRIES_CRATES_IO_INDEX": "https://example.invalid/index",
                    "CARGO_NET_OFFLINE": "true"}), patch.object(registry, "run", side_effect=cargo):
                if fault:
                    with self.assertRaises(ValueError):
                        registry.install_from_registry(self.version, "crates", directory)
                else:
                    registry.install_from_registry(self.version, "crates", directory)


if __name__ == "__main__":
    unittest.main()
