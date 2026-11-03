"""Acceptance evidence must never retain credentials after partial setup."""
import json
from pathlib import Path
import tempfile
import unittest

from instance_access.harness import AccessDeployment


class CredentialCleanup(unittest.TestCase):
    def test_failed_command_after_bundle_creation_is_still_scrubbed(self):
        with tempfile.TemporaryDirectory() as temporary:
            d = AccessDeployment.__new__(AccessDeployment)
            d.directory = Path(temporary)
            d.instance = d.directory / "instance.json"
            d.server_url, d.expiry = "http://127.0.0.1:1234", "2099-01-01T00:00:00Z"
            d.secret_files, d.secrets, d.bundle_directories = [], [], []
            token = "ldg1_" + "a" * 32 + "_" + "b" * 64

            def broken_command(*_args):
                bundle = d.directory / "initial-credentials"
                bundle.mkdir()
                (bundle / "operator.json").write_text(json.dumps({"token": token}))
                (d.directory / "unexpected.stdout").write_text(token)
                raise AssertionError("unexpected CLI exit after creating bundle")

            d.command = broken_command
            with self.assertRaises(AssertionError):
                d.provision()
            self.assertFalse(d.remove_credentials_and_scan())
            self.assertFalse((d.directory / "initial-credentials/operator.json").exists())
            self.assertNotIn(token, (d.directory / "unexpected.stdout").read_text())

    def test_interrupted_staging_and_read_only_artifacts_are_handled(self):
        with tempfile.TemporaryDirectory() as temporary:
            d = AccessDeployment.__new__(AccessDeployment)
            d.directory = Path(temporary)
            d.secret_files, d.secrets, d.bundle_directories = [], [], []
            bundle = d.directory / ".ledgence-access-test/bundle"
            bundle.mkdir(parents=True)
            (bundle / "worker.json").write_text(json.dumps({"token": "fixture-secret"}))
            artifact = d.directory / "immutable.zip"
            artifact.write_bytes(b"unchanged immutable artifact")
            artifact.chmod(0o444)
            self.assertTrue(d.remove_credentials_and_scan())
            self.assertFalse((bundle / "worker.json").exists())
            self.assertEqual(artifact.read_bytes(), b"unchanged immutable artifact")


if __name__ == "__main__":
    unittest.main()
