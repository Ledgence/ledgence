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


if __name__ == "__main__":
    unittest.main()
