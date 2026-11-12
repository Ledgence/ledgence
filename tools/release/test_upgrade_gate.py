"""Upgrade lifecycle regressions using inert bundles and simulated database ownership (MIT)."""
from contextlib import ExitStack, redirect_stdout
import io
import json
import os
from pathlib import Path
import runpy
import socket
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from release import upgrade_gate

ROOT = Path(__file__).resolve().parents[2]


class UpgradeGateCreationCleanupTests(unittest.TestCase):
    def uncertain_creation(self, predecessor, *, persistent_failure):
        attempted = "ldg_upgrade_" + "1" * 32
        unattempted = "ldg_upgrade_" + "2" * 32
        databases = {"operator_database"}
        statements = []
        drops = []
        deployment, exercise = Mock(), Mock()

        def admin(args, **_kwargs):
            statement = args[-1]
            statements.append(statement)
            if statement == f'CREATE DATABASE "{attempted}"':
                databases.add(attempted)
                raise subprocess.TimeoutExpired(args, 1)
            self.assertEqual(statement, f'DROP DATABASE IF EXISTS "{attempted}" WITH (FORCE)')
            drops.append(statement)
            if len(drops) == 1 or persistent_failure:
                return subprocess.CompletedProcess(args, 1)
            databases.discard(attempted)
            return subprocess.CompletedProcess(args, 0)

        with tempfile.TemporaryDirectory(prefix="ledgence-upgrade-gate-test-") as temporary, ExitStack() as stack:
            evidence = Path(temporary) / "evidence"
            stack.enter_context(patch.dict(os.environ, LEDGENCE_POSTGRES_URL="postgres://localhost/fixture"))
            stack.enter_context(patch.object(sys, "argv", ["upgrade", "--previous-bundle", temporary,
                "--binaries", temporary, "--console-dist", temporary,
                "--candidate-runner", str(Path(temporary) / "bootstrap.py"), "--evidence", str(evidence)]))
            stack.enter_context(patch.object(upgrade_gate.shutil, "which", return_value="/inert/bin/psql"))
            stack.enter_context(patch.object(upgrade_gate.uuid, "uuid4", side_effect=[
                SimpleNamespace(hex="1" * 32), SimpleNamespace(hex="2" * 32)]))
            inputs = stack.enter_context(patch.object(upgrade_gate, "inputs", return_value={"candidate": "same"}))
            stack.enter_context(patch.object(upgrade_gate.subprocess, "run", side_effect=admin))
            stack.enter_context(patch.object(subprocess, "Popen", side_effect=AssertionError("unexpected process")))
            stack.enter_context(patch.object(socket, "socket", side_effect=AssertionError("unexpected socket")))
            with redirect_stdout(io.StringIO()):
                status = upgrade_gate.main(predecessor=predecessor, deployment=deployment,
                                           exercise=exercise, description="inert uncertain-create fixture")
            report = json.loads((evidence / "summary.json").read_text())

        self.assertEqual(status, 1)
        self.assertFalse(report["passed"])
        deployment.assert_not_called()
        exercise.assert_not_called()
        self.assertEqual(inputs.call_count, 2)
        self.assertTrue(all(call.kwargs == {"predecessor": predecessor} for call in inputs.call_args_list))
        self.assertEqual(statements, [f'CREATE DATABASE "{attempted}"',
            f'DROP DATABASE IF EXISTS "{attempted}" WITH (FORCE)',
            f'DROP DATABASE IF EXISTS "{attempted}" WITH (FORCE)'])
        self.assertNotIn(unattempted, databases)
        self.assertNotIn(unattempted, "\n".join(statements))
        return report, databases, attempted

    def test_uncertain_create_and_failed_compensation_retries_exact_owned_name(self):
        for predecessor in ("0.8.0", "0.9.0"):
            with self.subTest(predecessor=predecessor):
                report, databases, _ = self.uncertain_creation(predecessor, persistent_failure=False)
                self.assertTrue(report["database_cleanup"])
                self.assertNotIn("cleanup_errors", report)
                self.assertEqual(databases, {"operator_database"})

    def test_persistently_failed_drop_reports_unresolved_owned_database(self):
        for predecessor in ("0.8.0", "0.9.0"):
            with self.subTest(predecessor=predecessor):
                report, databases, attempted = self.uncertain_creation(predecessor, persistent_failure=True)
                self.assertFalse(report["database_cleanup"])
                self.assertEqual(report["cleanup_errors"], [
                    {"database": attempted, "failure_type": "RuntimeError"}])
                self.assertEqual(databases, {"operator_database", attempted})


class UpgradeEntrypointTests(unittest.TestCase):
    def test_thin_wrappers_select_the_matching_predecessor_and_production_scenario(self):
        cases = (
            ("check-resource-upgrade.py", "0.8.0", "resource_admission.upgrade", "UpgradeDeployment"),
            ("check-artifact-upgrade.py", "0.9.0", "execution_artifacts.upgrade", "ArtifactUpgradeDeployment"),
        )
        for script, predecessor, scenario, deployment in cases:
            with self.subTest(script=script), \
                 patch.object(upgrade_gate, "main", return_value=0) as shared, \
                 patch.object(subprocess, "run", side_effect=AssertionError("unexpected process")), \
                 patch.object(subprocess, "Popen", side_effect=AssertionError("unexpected process")), \
                 patch.object(socket, "socket", side_effect=AssertionError("unexpected socket")), \
                 self.assertRaises(SystemExit) as raised:
                runpy.run_path(str(ROOT / "tools" / script), run_name="__main__")
            self.assertEqual(raised.exception.code, 0)
            shared.assert_called_once()
            self.assertEqual(shared.call_args.args, ())
            selected = shared.call_args.kwargs
            self.assertEqual(set(selected), {"predecessor", "deployment", "exercise", "description"})
            self.assertEqual(selected["predecessor"], predecessor)
            self.assertEqual(selected["deployment"].__module__, scenario)
            self.assertEqual(selected["deployment"].__name__, deployment)
            self.assertEqual(selected["exercise"].__module__, scenario)
            self.assertEqual(selected["exercise"].__name__, "exercise")
            self.assertTrue(selected["description"])


if __name__ == "__main__":
    unittest.main()
