"""Exercise release entrypoint arguments without invoking builds or publication."""
import contextlib
import io
from pathlib import Path
import re
import shlex
import sys
import tempfile
import unittest
from unittest.mock import patch

import package


class SourceValidationReached(Exception):
    """Stop at the first build prerequisite, after argument validation."""


class PackageCliTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def assert_reaches_source_validation(self, arguments):
        with patch.object(sys, "argv", [str(package.ROOT / "tools/release/package.py"), *arguments]), \
                patch.object(package, "clean_source", side_effect=SourceValidationReached), \
                patch.object(package, "command") as build:
            with self.assertRaises(SourceValidationReached):
                package.main()
            build.assert_not_called()

    def test_candidate_workflow_invocations_reach_source_validation(self):
        workflow = (package.ROOT / ".github/workflows/candidate.yml").read_text()
        # Shell continuations and environment variables are expanded by Actions.
        workflow = workflow.replace("\\\n", "")
        invocations = re.findall(r"(?m)^\s*(?:run:\s*)?(python(?:3)?\s+tools/release/package\.py\s+[^\n]+)$", workflow)
        self.assertEqual(len(invocations), 2, "exercise both actual candidate packaging workflow commands")
        modes = []
        for invocation in invocations:
            command = shlex.split(invocation.replace("$RUNNER_TEMP", str(self.root)).replace("$CANDIDATE", "rc.1"))
            modes.append("--headless" if "--headless" in command else "--console-dist")
            with self.subTest(command=command):
                self.assert_reaches_source_validation(command[2:])
        self.assertCountEqual(modes, ["--headless", "--console-dist"])

    def test_both_explicit_distribution_modes_reach_source_validation(self):
        for mode in (["--headless"], ["--console-dist", str(self.root / "console")]):
            with self.subTest(mode=mode):
                self.assert_reaches_source_validation(["--output", str(self.root / "candidate"), *mode])

    def test_missing_or_ambiguous_distribution_mode_fails_before_source_io(self):
        for mode in ([], ["--headless", "--console-dist", str(self.root / "console")]):
            with self.subTest(mode=mode), \
                    patch.object(sys, "argv", ["package.py", "--output", str(self.root / "candidate"), *mode]), \
                    patch.object(package, "clean_source") as source, \
                    contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as raised:
                    package.main()
                self.assertEqual(raised.exception.code, 2)
                source.assert_not_called()


class PackageSourceVersionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "source"
        self.root.mkdir()
        self.output = Path(self.temporary.name) / "candidate"
        (self.root / "Cargo.toml").write_text('[workspace.package]\nversion = "0.4.1"\n')
        self.manifests = ("sdk/python-client/pyproject.toml", "sdk/python/pyproject.toml")
        for path in self.manifests:
            manifest = self.root / path
            manifest.parent.mkdir(parents=True)
            manifest.write_text('[project]\nversion = "0.4.1"\n')
        self.addCleanup(patch.stopall)
        patch.object(package, "ROOT", self.root).start()

    def test_matching_rust_client_and_worker_versions_are_accepted(self):
        self.assertEqual(package.source_version(), "0.4.1")

    def test_each_python_version_mismatch_fails_before_build_or_output(self):
        for path in self.manifests:
            with self.subTest(manifest=path):
                manifest = self.root / path
                manifest.write_text('[project]\nversion = "0.4.0"\n')
                self.assert_source_rejected("package versions must match")
                manifest.write_text('[project]\nversion = "0.4.1"\n')

    def test_missing_worker_manifest_fails_before_build_or_output(self):
        (self.root / "sdk/python/pyproject.toml").unlink()
        self.assert_source_rejected("lacks the ledgence-worker package manifest")

    def assert_source_rejected(self, message):
        error = io.StringIO()
        with patch.object(sys, "argv", ["package.py", "--output", str(self.output), "--headless"]), \
                patch.object(package, "clean_source", return_value="a" * 40), \
                patch.object(package, "read") as toolchain, \
                patch.object(package, "command") as build, contextlib.redirect_stderr(error):
            with self.assertRaises(SystemExit) as raised:
                package.main()
            self.assertEqual(raised.exception.code, 2)
            self.assertIn(message, error.getvalue())
            toolchain.assert_not_called()
            build.assert_not_called()
            self.assertFalse(self.output.exists())


if __name__ == "__main__":
    unittest.main()
