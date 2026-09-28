"""No-network checks of live acceptance control flow and private evidence handling."""

import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

SPEC = importlib.util.spec_from_file_location(
    "support_demo_check", Path(__file__).resolve().parents[1] / "check.py"
)
check = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(check)


def draft():
    return {"ticket_id": "SUP-1042", "classification": "how_to", "model": "gemini-3.8-flash",
            "model_calls": 2, "tool_calls": 2,
            "http_attempts": 3, "http_retries": 1, "retry_wait_ms": 1500,
            "reply": "Un timeout solo limita la observación. No cancela la tarea ni la reenvía "
                     "automáticamente. La tarea continúa; recupera el resultado con el mismo task_id.",
            "sources": [{"id": "task-results", "title": "Task status and results",
                         "location": "docs/task-results.md"}]}


class FakeDeployment:
    def __init__(self, directory, *, changed_receipt=False, repeated_attempt=False, wrong_draft=False):
        self.evidence = check.Evidence(directory, "test-only-private-key")
        self.changed_receipt = changed_receipt
        self.repeated_attempt = repeated_attempt
        self.wrong_draft = wrong_draft
        self.server_url = "http://127.0.0.1:12345"
        self.artifacts = SimpleNamespace(url="http://127.0.0.1:12346")
        self.server = SimpleNamespace(process=SimpleNamespace(pid=11))
        self.worker = SimpleNamespace(process=SimpleNamespace(pid=12))
        self.submissions = self.probes = self.events = self.restarts = self.observed = 0
        self.finished = False
        self.token = None

    def workflow_wait(self, workflow_id):
        return {"workflow_id": workflow_id, "external_wait_key": "approval:1", "continuation": "finish"}

    def task_result(self, task_id):
        self.observed += 1
        output = draft() if task_id == "task-draft" else {"probe": "slot-released", "token": self.token}
        attempts = 2 if self.repeated_attempt and self.finished else 1
        return {"task": {"task_id": task_id, "state": "succeeded", "attempt_count": attempts},
                "outcome": {"kind": "succeeded", "output": output}, "observed_at": self.observed}

    def stop_services(self):
        self.restarts += 1

    def start_server(self):
        self.server.process.pid += 10

    def start_worker(self):
        self.worker.process.pid += 10

    def request(self, path, body=None, **query):
        if path == "/v1/console/workflows":
            self.submissions += 1
            return {"workflow": {"workflow_id": "workflow-demo"}}
        if path.endswith("/children"):
            return {"items": [{"kind": "task", "command_key": "draft", "target_id": "task-draft"}],
                    "next_cursor": None}
        if path.endswith("/waits"):
            return {"page": {"items": [{"wait_key": "approval:1", "deadline": 1234}], "next_cursor": None}}
        if path == "/v1/console/tasks":
            self.probes += 1
            self.token = body["input"]["data"]["token"]
            return {"task_id": "task-probe"}
        if path.endswith("/events"):
            self.events += 1
            receipt = {"already_accepted": self.events > 1, "accepted_at": 1,
                       "workflow_id": body["workflow_id"], "event_id": body["event"]["id"]}
            if self.changed_receipt and self.events > 1:
                receipt["accepted_at"] = 2
            return receipt
        if path == "/v1/console/workflows/result":
            self.finished = True
            output = {"ticket_id": "SUP-1042", "status": "approved", "draft_task_id": "task-draft", "draft": draft()}
            if self.wrong_draft:
                output["draft"]["reply"] = "Different reply"
            return {"workflow": {"state": "succeeded"}, "outcome": {"kind": "succeeded", "output": output}}
        if path.endswith("/attempts"):
            return {"items": [{"attempt_id": "attempt-draft"}], "next_cursor": None}
        raise AssertionError("unexpected endpoint")


class AcceptanceRunnerTests(unittest.TestCase):
    def terminal_deployment(self, directory, request):
        deployment = check.Deployment.__new__(check.Deployment)
        deployment.evidence = check.Evidence(directory, "test-only-private-key")
        deployment.request = Mock(side_effect=request)
        return deployment

    def test_terminal_workflow_retains_child_failure_and_all_attempt_pages_redacted(self):
        def request(path, **query):
            if path.endswith("/inspect"):
                return {"summary": {"workflow": {"state": "failed"}}}
            if path == "/v1/console/workflows/result":
                return {"outcome": {"kind": "failed", "error": {"kind": "draft_failed"}}}
            if path.endswith("/children"):
                if "cursor" not in query:
                    return {"items": [], "next_cursor": "children-2"}
                self.assertEqual(query["cursor"], "children-2")
                return {"items": [{"kind": "task", "target_id": "task-draft", "command_key": "draft"}],
                        "next_cursor": None}
            if path == "/v1/console/tasks/result":
                self.assertEqual(query["task_id"], "task-draft")
                return {"outcome": {"kind": "failed", "error": {"message": "provider test-only-private-key"}}}
            if path.endswith("/attempts"):
                self.assertEqual(query["task_id"], "task-draft")
                if "cursor" not in query:
                    return {"items": [{"attempt_id": "attempt-1"}], "next_cursor": "attempts-2"}
                self.assertEqual(query["cursor"], "attempts-2")
                return {"items": [{"attempt_id": "attempt-2"}], "next_cursor": None}
            raise AssertionError("unexpected endpoint")

        with tempfile.TemporaryDirectory() as directory:
            deployment = self.terminal_deployment(Path(directory), request)
            with self.assertRaisesRegex(check.CheckFailure, "workflow became terminal before approval"):
                deployment.workflow_wait("workflow-demo")
            report = json.loads((Path(directory) / "failure-diagnostics.json").read_text())
            self.assertEqual(report["errors"], [])
            self.assertEqual(len(report["children"]), 2)
            self.assertEqual(report["tasks"][0]["result"]["outcome"]["error"]["message"], "provider [REDACTED]")
            self.assertEqual([page["items"][0]["attempt_id"] for page in report["tasks"][0]["attempts"]],
                             ["attempt-1", "attempt-2"])
            self.assertEqual(json.loads((Path(directory) / "unexpected-workflow-result.json").read_text()),
                             report["workflow_result"])
            self.assertTrue(deployment.evidence.secret_detected)
            self.assertTrue(deployment.evidence.scan())

    def test_diagnostic_fetch_failures_preserve_original_failure_and_continue_collecting(self):
        def request(path, **query):
            if path.endswith("/inspect"):
                return {"summary": {"workflow": {"state": "failed"}}}
            if path == "/v1/console/workflows/result" or path == "/v1/console/tasks/result":
                raise RuntimeError("do not retain provider body or test-only-private-key")
            if path.endswith("/children"):
                return {"items": [{"kind": "task", "target_id": "task-draft"}], "next_cursor": None}
            if path.endswith("/attempts"):
                return {"items": [{"attempt_id": "attempt-1"}], "next_cursor": None}
            raise AssertionError("unexpected endpoint")

        with tempfile.TemporaryDirectory() as directory:
            deployment = self.terminal_deployment(Path(directory), request)
            with self.assertRaisesRegex(check.CheckFailure, "workflow became terminal before approval"):
                deployment.workflow_wait("workflow-demo")
            encoded = (Path(directory) / "failure-diagnostics.json").read_text()
            self.assertNotIn("provider body", encoded)
            self.assertNotIn("test-only-private-key", encoded)
            report = json.loads(encoded)
            self.assertEqual(report["errors"], [{"stage": "workflow_result", "failure_type": "RuntimeError"},
                                                 {"stage": "task_result", "failure_type": "RuntimeError"}])
            self.assertEqual(report["tasks"][0]["attempts"][0]["items"][0]["attempt_id"], "attempt-1")

    def test_broken_diagnostic_pagination_is_bounded_and_retains_partial_evidence(self):
        for broken in ("repeated", "unbounded", "malformed"):
            with self.subTest(broken=broken), tempfile.TemporaryDirectory() as directory:
                page_count = 0

                def request(path, **query):
                    nonlocal page_count
                    if path.endswith("/inspect"):
                        return {"summary": {"workflow": {"state": "failed"}}}
                    if path.endswith("/result"):
                        return {"outcome": {"kind": "failed"}}
                    if path.endswith("/children"):
                        page_count += 1
                        if broken == "malformed":
                            return {"items": None}
                        return {"items": [], "next_cursor": "same" if broken == "repeated" else str(page_count)}
                    raise AssertionError("unexpected endpoint")

                deployment = self.terminal_deployment(Path(directory), request)
                with self.assertRaisesRegex(check.CheckFailure, "workflow became terminal before approval"):
                    deployment.workflow_wait("workflow-demo")
                report = json.loads((Path(directory) / "failure-diagnostics.json").read_text())
                self.assertEqual(page_count, {"repeated": 2, "unbounded": 5, "malformed": 1}[broken])
                self.assertEqual(len(report["children"]), page_count)
                self.assertEqual(report["errors"][0]["failure_type"], {
                    "repeated": "InvalidCursor", "unbounded": "PageLimitExceeded", "malformed": "InvalidPage"}[broken])

    def test_diagnostic_storage_failure_does_not_replace_acceptance_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            deployment = self.terminal_deployment(Path(directory), lambda path, **query: {
                "summary": {"workflow": {"state": "failed"}}})
            with patch.object(deployment.evidence, "write", side_effect=OSError("disk full")), \
                    self.assertRaisesRegex(check.CheckFailure, "workflow became terminal before approval"):
                deployment.workflow_wait("workflow-demo")

    def test_console_failure_description_is_static_and_provider_exception_text_is_not_printed(self):
        for error in (check.CheckFailure("workflow became terminal before approval"),
                      RuntimeError("do not print provider body or test-only-private-key")):
            with self.subTest(error=type(error).__name__), contextlib.redirect_stdout(io.StringIO()) as output, \
                    patch.object(check, "require", side_effect=error):
                status = check.main(["--directory", "/unused", "--binaries", "/unused",
                                     "--evidence", "/unused", "--live-gemini"])
                public = json.loads(output.getvalue())
                self.assertEqual(status, 1)
                if isinstance(error, check.CheckFailure):
                    self.assertEqual(public["failed_check"], "workflow became terminal before approval")
                    self.assertIn("failure-diagnostics.json", public["next_step"])
                else:
                    self.assertNotIn("failed_check", public)
                self.assertNotIn("provider body", output.getvalue())
                self.assertNotIn("test-only-private-key", output.getvalue())

    def test_credential_configuration_failure_gives_safe_action_before_database_setup(self):
        for error in (ValueError("private credential file data"), OSError("private credential path")):
            with self.subTest(error=type(error).__name__), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                prepared, binaries, evidence = root / "prepared", root / "bin", root / "evidence"
                for kind in ("agent", "workflow"):
                    target = prepared / "packages" / kind
                    target.mkdir(parents=True)
                    (target / "program.py").write_bytes((check.HERE / kind / "program.py").read_bytes())
                (prepared / "prepared.json").write_text("{}")
                binaries.mkdir()
                for binary in ("ledgence-worker", "ledgence-orchestrator"):
                    (binaries / binary).touch()
                configuration = SimpleNamespace(worker_environment=Mock(side_effect=error))
                with patch.object(check, "load", return_value=configuration), \
                        patch.object(check.sys, "version_info", (3, 13)), \
                        patch.dict(check.os.environ, {"DATABASE_URL": "postgres://localhost/unused"}, clear=True), \
                        patch.object(check.subprocess, "run") as subprocess_run, \
                        contextlib.redirect_stdout(io.StringIO()) as output:
                    status = check.main(["--directory", str(prepared), "--binaries", str(binaries),
                                         "--evidence", str(evidence), "--live-gemini"])
                public = json.loads(output.getvalue())
                self.assertEqual(status, 1)
                self.assertEqual(public["phase"], "configuration")
                self.assertEqual(public["failure_type"], "CheckFailure")
                self.assertIn("GOOGLE_API_KEY", public["failed_check"])
                self.assertIn("--env-file", public["failed_check"])
                self.assertIn("GOOGLE_GENAI_USE_VERTEXAI=FALSE", public["failed_check"])
                self.assertNotIn("private credential", output.getvalue())
                self.assertFalse(evidence.exists())
                subprocess_run.assert_not_called()

    def test_paid_requests_require_explicit_flag_before_configuration_access(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            check.parser().parse_args(["--directory", "/unused", "--binaries", "/unused", "--evidence", "/unused"])
        self.assertEqual(error.exception.code, 2)
        args = check.parser().parse_args(["--directory", "/unused", "--binaries", "/unused",
                                          "--evidence", "/unused", "--live-gemini"])
        self.assertTrue(args.live_gemini)

    def test_owned_database_name_replaces_only_the_parent_database(self):
        name = "ldg_support_demo_" + "a" * 32
        self.assertEqual(check.database_url("postgres://user:password@127.0.0.1:1234/postgres?sslmode=disable", name),
                         f"postgres://user:password@127.0.0.1:1234/{name}?sslmode=disable")
        for parent, database in (("postgres://localhost/postgres?dbname=other", name),
                                 ("postgres://localhost/postgres?service=other", name),
                                 ("postgres://localhost/postgres", "user_database"),
                                 ("https://localhost/postgres", name)):
            with self.subTest(parent=parent), self.assertRaises(check.CheckFailure):
                check.database_url(parent, database)

    def test_server_and_admin_environments_exclude_provider_key_and_aliases(self):
        source = {"PATH": "/usr/bin", "GOOGLE_API_KEY": "secret-test", "GEMINI_API_KEY": "another",
                  "GOOGLE_APPLICATION_CREDENTIALS": "/private/file", "ALIAS": "prefix-secret-test",
                  "DATABASE_URL": "postgres://localhost/postgres"}
        before = dict(source)
        self.assertEqual(check.server_environment(source, "secret-test"), {
            "PATH": "/usr/bin", "DATABASE_URL": "postgres://localhost/postgres"})
        self.assertEqual(source, before)

    def test_logs_and_json_are_redacted_before_retention_and_scan_is_boolean(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence = check.Evidence(Path(directory), 'test-"private"-key')
            evidence.write("result.json", {"unexpected": 'test-"private"-key'})
            evidence.write_bytes("worker.stderr", b'provider error: test-"private"-key')
            self.assertTrue(evidence.secret_detected)
            self.assertTrue(evidence.scan())
            for path in Path(directory).iterdir():
                self.assertNotIn(b"private", path.read_bytes())
                self.assertIn(b"[REDACTED]", path.read_bytes())

    def test_grounded_ticket_check_rejects_cancel_and_resubmit_claims(self):
        self.assertTrue(all(check.timeout_answer_review_hints(draft()).values()))
        wrong = draft()
        wrong["reply"] = "El timeout cancela la tarea; reenvía la solicitud con un nuevo ID."
        checks = check.timeout_answer_review_hints(wrong)
        self.assertFalse(checks["does_not_cancel"])
        self.assertFalse(checks["does_not_resubmit"])
        self.assertFalse(checks["reuses_identity"])

    def test_scenario_submits_one_draft_one_probe_and_reconciles_one_event(self):
        with tempfile.TemporaryDirectory() as directory:
            deployment = FakeDeployment(Path(directory))
            with contextlib.redirect_stdout(io.StringIO()):
                result = check.scenario(deployment, "gemini-3.8-flash")
            self.assertEqual((deployment.submissions, deployment.probes, deployment.events, deployment.restarts), (1, 1, 2, 1))
            self.assertEqual(result["draft_attempt_count"], 1)
            for counter in ("model_calls", "tool_calls", "http_attempts", "http_retries", "retry_wait_ms"):
                self.assertEqual(result[counter], draft()[counter])
            self.assertNotEqual(result["original_pids"], result["restarted_pids"])
            summary = json.loads((Path(directory) / "workflow-result.json").read_text())
            self.assertEqual(summary["outcome"]["output"]["draft"], draft())

    def test_scenario_fails_changed_receipt_attempt_or_draft_without_resubmission(self):
        for changed in ("changed_receipt", "repeated_attempt", "wrong_draft"):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as directory:
                deployment = FakeDeployment(Path(directory), **{changed: True})
                with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(check.CheckFailure):
                    check.scenario(deployment, "gemini-3.8-flash")
                self.assertEqual(deployment.submissions, 1)

    def test_prepared_store_link_fallback_does_not_change_original_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            source, target = Path(directory) / "source", Path(directory) / "target"
            source.write_bytes(b"immutable package")
            with patch.object(check.os, "link", side_effect=OSError("different filesystem")):
                check.link_or_copy(source, target)
            self.assertEqual(target.read_bytes(), source.read_bytes())


if __name__ == "__main__":
    unittest.main()
