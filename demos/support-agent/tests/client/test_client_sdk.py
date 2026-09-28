"""Companion commands through the real SDK, with HTTP I/O substituted (MIT).

Run separately with the reviewed Python-client runtime requirements installed;
this suite deliberately fails import instead of silently skipping dependencies.
"""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "sdk/python-client/src"))
try:
    import ledgence.client.client as sdk_client
    from ledgence.client import codec
    from ledgence.client.transport import _decode_response
except ImportError as error:
    raise ImportError(
        "Install sdk/python-client/third_party/runtime-requirements.txt with --require-hashes "
        "in the separate client environment before running tests/client."
    ) from error

SPEC = importlib.util.spec_from_file_location(
    "support_demo_companion", ROOT / "demos/support-agent/client.py"
)
companion = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(companion)
SCOPE = {"tenant_id": "acme", "namespace": "demo"}


def workflow(state="running", *, correlation=None):
    terminal = state in {"succeeded", "failed", "cancelled"}
    return {"workflow_id": "wf-support", "scope": dict(SCOPE), "state": state,
            "revision": 3 if terminal else 1,
            "activation_id": "activation-start" if state == "running" else None,
            "submitted_at": 1, "terminal_at": 10 if terminal else None,
            "correlation_key": correlation}


def task_status():
    return {"scope": dict(SCOPE), "task_id": "task-draft", "run_id": "run-draft",
            "queue": "support-demo", "correlation_key": None, "state": "succeeded",
            "attempt_count": 1, "current_attempt_id": None, "latest_attempt_id": "attempt-draft",
            "submitted_at": 1, "available_at": 1, "terminal_at": 10, "cancel_requested_at": None}


def receipt(body, *, already_accepted=False):
    value = json.loads(body)
    return {"scope": value["scope"], "workflow_id": value["workflow_id"], "key": value["key"],
            "event_id": value["event"]["id"], "event_source": value["event"]["source"],
            "accepted_at": 7, "already_accepted": already_accepted}


class StubTransport:
    """Keep SDK request bytes, parsing, identity checks and exception mapping real."""
    def __init__(self, owner, base_url):
        self.owner, self.base_url, self.closed = owner, base_url, False

    def check_open(self):
        if self.closed:
            raise AssertionError("transport used after close")

    async def exchange(self, method, route, *, body=None, query=None, deadline, parser, limit=codec.RESPONSE_LIMIT):
        self.check_open()
        request = {"method": method, "route": route, "body": body, "query": query}
        self.owner.requests.append(request)
        status, payload = self.owner.respond(request)
        encoded = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode()
        return _decode_response(encoded, status, "stub-request", parser, limit)

    async def close(self):
        self.closed = True


class CompanionSdkTests(unittest.TestCase):
    def setUp(self):
        self.requests = []
        self.transports = []
        self.respond = self.accept
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.ticket = Path(self.temp.name) / "ticket.json"
        self.ticket.write_text(json.dumps({"ticket_id": "SUP-1042", "question": "¿Cómo recupero el resultado?"}))

        def create(base_url):
            transport = StubTransport(self, base_url)
            self.transports.append(transport)
            return transport
        transport_patch = patch.object(sdk_client, "_Transport", side_effect=create)
        transport_patch.start()
        self.addCleanup(transport_patch.stop)
        no_network = patch("aiohttp.ClientSession", side_effect=AssertionError("tests must not perform HTTP I/O"))
        no_network.start()
        self.addCleanup(no_network.stop)

    def accept(self, request):
        route = request["route"]
        if route == "/v1/workflows":
            command = json.loads(request["body"])
            return 200, workflow(correlation=command["input"]["correlation_key"])
        if route == "/v1/workflows/status":
            return 200, workflow("succeeded")
        if route == "/v1/workflows/result":
            return 200, {"workflow": workflow("succeeded"), "outcome": {
                "kind": "succeeded", "output": {"status": "approved", "draft_task_id": "task-draft"}}}
        if route == "/v1/workflows/cancel":
            return 200, workflow("cancelling")
        if route == "/v1/workflows/events":
            return 200, receipt(request["body"])
        if route == "/v1/tasks/status":
            return 200, task_status()
        if route == "/v1/tasks/result":
            return 200, {"task": task_status(), "outcome": {
                "kind": "succeeded", "attempt_id": "attempt-draft", "quiescence": "confirmed",
                "execution_may_have_started": True, "output": {"reply": "Retained draft"}}}
        raise AssertionError("unexpected HTTP route")

    def invoke(self, arguments):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = companion.main(["--server", "http://stub.invalid:8082", *arguments])
        self.assertTrue(all(transport.closed for transport in self.transports))
        return code, out.getvalue(), err.getvalue()

    def submit(self, *extra):
        return ["submit", "--ticket", str(self.ticket), "--idempotency-key", "support:SUP-1042:1", *extra]

    def review(self, decision="approve"):
        return ["review", "--workflow", "wf-support", "--ticket-id", "SUP-1042",
                "--draft-task", "task-draft", "--decision", decision, "--event-id", "review:SUP-1042:1"]

    def test_prepare_and_submit_preserve_pinned_program_scope_input_and_policy(self):
        code, output, error = self.invoke(self.submit())
        self.assertEqual((code, error), (0, ""))
        self.assertEqual(json.loads(output), {"workflow_id": "wf-support", "ticket_id": "SUP-1042"})
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(self.requests[0]["route"], "/v1/workflows")
        self.assertEqual(json.loads(self.requests[0]["body"]), {
            "idempotency_key": "support:SUP-1042:1", "input": {
                **SCOPE, "queue": "support-demo", "program": {"id": "support-workflow", "version": "1.0.1"},
                "data": {"ticket_id": "SUP-1042", "question": "¿Cómo recupero el resultado?",
                         "queue": "support-demo", "model": "gemini-3.8-flash", "approval_timeout_ms": 3_600_000},
                "correlation_key": "SUP-1042", "retry_policy": {"max_attempts": 3, "retry_delay_ms": 1000},
                "attempt_timeout_ms": 60_000,
            },
        })

    def test_submit_options_and_explicit_repeat_reconstruct_identical_request_bytes(self):
        arguments = self.submit("--queue", "review-queue", "--model", "gemini-example", "--approval-timeout-ms", "0")
        self.assertEqual(self.invoke(arguments)[0], 0)
        self.assertEqual(self.invoke(arguments)[0], 0)
        self.assertEqual(len(self.requests), 2)
        self.assertEqual(self.requests[0]["body"], self.requests[1]["body"])
        command = json.loads(self.requests[0]["body"])
        self.assertEqual(command["input"]["queue"], "review-queue")
        self.assertEqual(command["input"]["data"]["model"], "gemini-example")
        self.assertEqual(command["input"]["data"]["approval_timeout_ms"], 0)

    def test_status_is_a_scoped_read_and_cancel_is_a_scoped_mutation(self):
        code, output, _ = self.invoke(["status", "--workflow", "wf-support"])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output)["state"], "succeeded")
        self.assertEqual(self.requests[0], {"method": "GET", "route": "/v1/workflows/status", "body": None,
                                            "query": {**SCOPE, "workflow_id": "wf-support"}})
        code, output, _ = self.invoke(["cancel", "--workflow", "wf-support"])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output)["state"], "cancelling")
        self.assertEqual(self.requests[1]["method"], "POST")
        self.assertEqual(json.loads(self.requests[1]["body"]), {"scope": SCOPE, "workflow_id": "wf-support"})

    def test_observation_timeout_is_exit_three_and_same_workflow_remains_observable(self):
        self.respond = lambda request: (200, workflow("waiting"))
        code, output, error = self.invoke(["result", "--workflow", "wf-support", "--timeout", "0.01"])
        self.assertEqual((code, output), (3, ""))
        self.assertIn("remote work continues", error.lower())
        self.assertGreaterEqual(len(self.requests), 1)
        self.assertTrue(all(request["method"] == "GET" for request in self.requests))
        self.respond = self.accept
        code, output, error = self.invoke(["result", "--workflow", "wf-support", "--timeout", "1"])
        self.assertEqual((code, error), (0, ""))
        self.assertEqual(json.loads(output), {"status": "approved", "draft_task_id": "task-draft"})
        self.assertTrue(all(request["method"] == "GET" for request in self.requests))
        self.assertTrue(all(request["query"]["workflow_id"] == "wf-support" for request in self.requests))

    def test_draft_reads_existing_task_result_without_creating_work(self):
        code, output, error = self.invoke(["draft", "--task", "task-draft"])
        self.assertEqual((code, error), (0, ""))
        self.assertEqual(json.loads(output), {"reply": "Retained draft"})
        self.assertEqual([request["route"] for request in self.requests], ["/v1/tasks/status", "/v1/tasks/result"])
        self.assertTrue(all(request["method"] == "GET" and request["query"] ==
                            {**SCOPE, "task_id": "task-draft"} for request in self.requests))

    def test_review_uses_real_receipt_parser_and_preserves_exact_reconciliation_identity(self):
        self.respond = lambda request: (200, receipt(request["body"], already_accepted=len(self.requests) > 1))
        code, output, error = self.invoke(self.review())
        self.assertEqual((code, error), (0, ""))
        self.assertEqual(json.loads(output), {"scope": SCOPE, "workflow_id": "wf-support", "key": "approval:1",
            "event_id": "review:SUP-1042:1", "event_source": "urn:ledgence:demo:support-review",
            "accepted_at": 7, "already_accepted": False})
        code, output, _ = self.invoke(self.review())
        self.assertEqual(code, 0)
        self.assertTrue(json.loads(output)["already_accepted"])
        self.assertEqual(self.requests[0]["body"], self.requests[1]["body"])
        event = json.loads(self.requests[0]["body"])["event"]
        self.assertEqual(event["data"], {"ticket_id": "SUP-1042", "draft_task_id": "task-draft", "approved": True})
        self.assertNotIn("time", event)
        self.assertEqual(self.invoke(self.review("reject"))[0], 0)
        self.assertIs(json.loads(self.requests[-1]["body"])["event"]["data"]["approved"], False)

    def test_mismatched_receipt_is_uncertain_instead_of_a_false_success(self):
        self.respond = lambda request: (200, {**receipt(request["body"]), "event_id": "different-event"})
        code, output, error = self.invoke(self.review())
        self.assertEqual((code, output), (2, ""))
        self.assertIn("unchanged IDs", error)
        self.assertEqual(len(self.requests), 1)

    def test_all_uncertain_mutations_exit_two_without_resend_and_keep_exact_repeat_body(self):
        for arguments in (self.submit(), self.review(), ["cancel", "--workflow", "wf-support"]):
            with self.subTest(command=arguments[0]):
                self.requests.clear()
                self.respond = lambda request: (503, {"code": "unavailable", "message": "private-provider-error-body"})
                code, output, error = self.invoke(arguments)
                self.assertEqual((code, output), (2, ""))
                self.assertIn("reconcile", error)
                self.assertNotIn("private-provider-error-body", error)
                self.assertEqual(len(self.requests), 1, "mutations must not resend automatically")
                original = self.requests[0]["body"]
                self.respond = self.accept
                self.assertEqual(self.invoke(arguments)[0], 0)
                self.assertEqual(len(self.requests), 2)
                self.assertEqual(original, self.requests[1]["body"])

    def test_definitive_conflict_is_exit_one_and_does_not_retry(self):
        self.respond = lambda request: (409, {"code": "conflict"})
        code, output, error = self.invoke(self.submit())
        self.assertEqual((code, output), (1, ""))
        self.assertIn("Conflict", error)
        self.assertNotIn("Outcome uncertain", error)
        self.assertEqual(len(self.requests), 1)


if __name__ == "__main__":
    unittest.main()
