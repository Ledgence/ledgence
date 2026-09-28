"""Offline controller transitions and protocol-3 decision checks (MIT)."""

import asyncio
import copy
import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "sdk" / "python"))
SPEC = importlib.util.spec_from_file_location(
    "support_demo_workflow", ROOT / "examples" / "support-agent" / "workflow" / "program.py"
)
program = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(program)

from ledgence.worker.workflow import WorkflowContext, WorkflowError


def invocation(**changes):
    data = {"ticket_id": "SUP-1042", "question": "How do I resume an approval workflow?"}
    data.update(changes)
    return {"specversion": "1.0", "id": "invocation-1", "source": "urn:ledgence",
            "type": "ledgence.task", "data": data}


def draft(**changes):
    return {"ticket_id": "SUP-1042", "classification": "how_to",
            "reply": "Send a directly addressed event using the workflow ID and wait key.",
            "sources": [{"id": "workflow-events", "title": "Workflow events",
                         "location": "docs/workflow-events.md"}],
            "model": program.DEFAULT_MODEL, "model_calls": 2, "tool_calls": 2,
            "http_attempts": 3, "http_retries": 1, "retry_wait_ms": 1500, **changes}


def child(output=None):
    return {"task_id": "task-draft-1", "state": "succeeded", "outcome": {
        "kind": "succeeded", "attempt_id": "attempt-draft-1", "quiescence": "confirmed",
        "execution_may_have_started": True, "output": draft() if output is None else output,
    }}


def checkpoint(**changes):
    return {"draft_task_id": "task-draft-1", "draft": draft(), **changes}


def approval(approved=True, *, accepted_at=10, **changes):
    data = {"ticket_id": "SUP-1042", "draft_task_id": "task-draft-1", "approved": approved}
    data.update(changes)
    return {"kind": "event", "key": program.APPROVAL_KEY, "accepted_at": accepted_at,
            "event": {"specversion": "1.0", "id": "approval-1", "source": "urn:support:review",
                      "type": "support.approval.v1", "datacontenttype": "application/json",
                      "data": data}}


class FakeContext:
    def __init__(self, continuation="start", *, state=None, inputs=None, wake=None):
        self.continuation = continuation
        self._entry_registry = None
        self._entry_enum = None
        self.state = copy.deepcopy(state)
        self.inputs = copy.deepcopy(inputs or {})
        self.wake = copy.deepcopy(wake)
        self.commands = []

    def _active(self):
        pass

    def task(self, key, **options):
        self.commands.append(copy.deepcopy({"key": key, **options}))
        return key

    def suspend(self, *, continuation, state, until):
        return copy.deepcopy({"kind": "suspend", "continuation": WorkflowContext._target(self, continuation),
                              "state": state, "until": until, "commands": self.commands})

    def wait_event(self, key, *, continuation, state, timeout_ms):
        return copy.deepcopy({"kind": "wait", "continuation": WorkflowContext._target(self, continuation), "state": state,
                              "wait": {"kind": "event", "key": key, "timeout_ms": timeout_ms},
                              "commands": self.commands})

    def complete(self, output):
        return copy.deepcopy({"kind": "complete", "output": output})

    def fail(self, kind, message):
        return {"kind": "fail", "error": {"kind": kind, "message": message}}


async def dispatch(context, event=None):
    with patch("ledgence.worker.workflow.workflow_context", return_value=context):
        return await program.handle(invocation() if event is None else event)


def run(context, event=None):
    return asyncio.run(dispatch(context, event))


class WorkflowTests(unittest.TestCase):
    def assert_failure(self, context, kind, event=None):
        result = run(context, event)
        self.assertEqual(result["kind"], "fail")
        self.assertEqual(result["error"]["kind"], kind)
        self.assertEqual(context.commands, [])

    def test_start_stages_one_pinned_child_without_retries_or_input_mutation(self):
        event = invocation(extra_user_data={"nested": [1, None]})
        original = copy.deepcopy(event)
        result = run(FakeContext(), event)
        self.assertEqual(event, original)
        self.assertEqual(result["kind"], "suspend")
        self.assertEqual(result["continuation"], "review")
        self.assertEqual(result["until"], ["draft"])
        self.assertEqual(result["commands"], [{
            "key": "draft", "program": "support-agent", "version": "1.0.2",
            "queue": "support-demo", "data": {
                "ticket_id": "SUP-1042", "question": event["data"]["question"],
                "model": "gemini-3.8-flash",
            }, "retry_policy": {"max_attempts": 1, "retry_delay_ms": 0},
            "attempt_timeout_ms": 180_000,
        }])

    def test_explicit_configuration_and_utf8_byte_boundaries(self):
        event = invocation(ticket_id="T" * 128, question="é" * 4096,
                           queue="support-local", model="gemini-example", approval_timeout_ms=0)
        result = run(FakeContext(), event)
        self.assertEqual(result["commands"][0]["queue"], "support-local")
        self.assertEqual(result["commands"][0]["data"]["model"], "gemini-example")
        for duration in (0, program.MAX_APPROVAL_TIMEOUT_MS):
            result = run(FakeContext("review", inputs={"draft": child()}),
                         invocation(approval_timeout_ms=duration))
            self.assertEqual(result["wait"]["timeout_ms"], duration)

    def test_invalid_tickets_fail_before_child_dispatch(self):
        changes = [
            {"ticket_id": ""}, {"ticket_id": "two words"}, {"ticket_id": True},
            {"ticket_id": "T" * 129}, {"ticket_id": "é"}, {"question": " \n\t"},
            {"question": "é" * 4097}, {"question": None}, {"question": "bad\x00"},
            {"question": "\ud800"}, {"queue": ""}, {"queue": "q\n"},
            {"queue": "q" * 129}, {"model": ""}, {"model": "model name"},
            {"model": False}, {"model": "m" * 129},
        ]
        changes.extend({"approval_timeout_ms": value} for value in
                       (True, False, 0.0, "1000", None, -1, 86_400_001))
        for changed in changes:
            with self.subTest(changed=changed):
                self.assert_failure(FakeContext(), "invalid_ticket", invocation(**changed))
        for event in ({}, {"data": []}, {"data": {}}):
            with self.subTest(event=event):
                self.assert_failure(FakeContext(), "invalid_ticket", event)

    def test_review_checkpoints_accepted_draft_then_waits_without_new_child(self):
        context = FakeContext("review", inputs={"draft": child()})
        original = copy.deepcopy(context.inputs)
        event = invocation()
        original_event = copy.deepcopy(event)
        result = run(context, event)
        self.assertEqual(result["state"], checkpoint())
        self.assertEqual(result["wait"], {"kind": "event", "key": "approval:1",
                                          "timeout_ms": 3_600_000})
        self.assertEqual(result["continuation"], "finish")
        self.assertEqual(result["commands"], [])
        self.assertEqual(context.inputs, original)
        self.assertEqual(event, original_event)

    def test_failed_or_cancelled_child_fails_workflow_without_another_attempt(self):
        for state in ("failed", "cancelled"):
            with self.subTest(state=state):
                result = {"task_id": "task-draft-1", "state": state,
                          "outcome": {"kind": state}}
                self.assert_failure(FakeContext("review", inputs={"draft": result}), "draft_failed")

    def test_bad_child_binding_and_structured_drafts_are_rejected(self):
        invalid = [
            draft(ticket_id="OTHER"), draft(model="other-model"), draft(classification="unknown"),
            draft(reply=""), draft(reply="x" * 8193), draft(model_calls=0), draft(model_calls=True),
            draft(model_calls=7), draft(tool_calls=1), draft(tool_calls=9), draft(tool_calls=2.0),
            draft(http_attempts=0), draft(http_attempts=7), draft(http_attempts=True),
            draft(http_retries=-1), draft(http_retries=6), draft(http_retries=1.0),
            draft(retry_wait_ms=-1), draft(retry_wait_ms=120_001), draft(retry_wait_ms=True),
            draft(http_attempts=2), draft(http_retries=2),
            draft(model_calls=1, http_attempts=4, http_retries=3),
            draft(http_attempts=2, http_retries=0, retry_wait_ms=1500),
            draft(sources=[]), draft(sources=draft()["sources"] * 2),
            draft(sources=[{"id": "invented", "title": "Invented", "location": "docs/invented.md"}]),
            draft(sources=[{"id": "workflows", "title": "Workflows", "location": "https://elsewhere"}]),
            draft(sources=[{"id": "workflows", "title": "x" * 129, "location": "docs/workflows.md"}]),
            {"reply": "Unstructured"}, draft(unexpected=True),
        ]
        for output in invalid:
            with self.subTest(output=output):
                self.assert_failure(FakeContext("review", inputs={"draft": child(output)}), "invalid_draft")
        for result in (None, {}, {"state": "succeeded", "outcome": {}},
                       {**child(), "kind": "workflow"}, {**child(), "task_id": ""},
                       {**child(), "outcome": {"kind": "failed"}}):
            with self.subTest(result=result):
                self.assert_failure(FakeContext("review", inputs={"draft": result}), "invalid_draft")

    def test_valid_http_budgets_survive_the_approval_checkpoint(self):
        for counters in (
            {"model_calls": 2, "http_attempts": 2, "http_retries": 0, "retry_wait_ms": 0},
            {"model_calls": 2, "http_attempts": 6, "http_retries": 4, "retry_wait_ms": 8000},
            {"model_calls": 6, "http_attempts": 6, "http_retries": 0, "retry_wait_ms": 0},
        ):
            with self.subTest(counters=counters):
                value = draft(**counters)
                reviewed = run(FakeContext("review", inputs={"draft": child(value)}))
                self.assertEqual(reviewed["state"]["draft"], value)
                finished = run(FakeContext("finish", state=reviewed["state"], wake=approval()))
                self.assertEqual(finished["output"]["draft"], value)

    def test_new_workflow_and_client_sources_survive_approval(self):
        for source_id, location in (("workflow-entrypoints", "docs/workflow-entrypoints.md"),
                                    ("python-client", "sdk/python-client/README.md")):
            with self.subTest(source=source_id):
                value = draft(sources=[{"id": source_id, "title": "Bundled documentation", "location": location}])
                reviewed = run(FakeContext("review", inputs={"draft": child(value)}))
                self.assertEqual(reviewed["state"]["draft"], value)
                finished = run(FakeContext("finish", state=reviewed["state"], wake=approval()))
                self.assertEqual(finished["output"]["draft"], value)
                value["sources"][0]["location"] = "docs/not-the-recorded-source.md"
                self.assert_failure(FakeContext("review", inputs={"draft": child(value)}), "invalid_draft")

    def test_approval_and_rejection_complete_with_the_exact_accepted_draft(self):
        for approved, status in ((True, "approved"), (False, "rejected")):
            with self.subTest(status=status):
                context = FakeContext("finish", state=checkpoint(), wake=approval(approved))
                original = copy.deepcopy(context.wake)
                result = run(context)
                self.assertEqual(result, {"kind": "complete", "output": {
                    "ticket_id": "SUP-1042", "status": status, **checkpoint(),
                }})
                self.assertEqual(context.commands, [])
                self.assertEqual(context.wake, original)

    def test_timeout_completes_expired_without_waiting_again_or_launching_child(self):
        context = FakeContext("finish", state=checkpoint(),
                              wake={"kind": "timeout", "key": "approval:1", "deadline": 1})
        result = run(context)
        self.assertEqual(result["kind"], "complete")
        self.assertEqual(result["output"], {"ticket_id": "SUP-1042", "status": "expired", **checkpoint()})
        self.assertEqual(context.commands, [])

    def test_early_buffered_approval_is_bound_to_ticket_and_draft_not_arrival_time(self):
        result = run(FakeContext("finish", state=checkpoint(), wake=approval(accepted_at=0)))
        self.assertEqual(result["output"]["status"], "approved")
        for changes in ({"ticket_id": "OTHER"}, {"draft_task_id": "different-draft"}):
            with self.subTest(changes=changes):
                self.assert_failure(FakeContext("finish", state=checkpoint(),
                                                wake=approval(accepted_at=0, **changes)), "invalid_approval")

    def test_invalid_approval_shape_identity_and_boolean_fail_explicitly(self):
        wakes = [None, {"kind": "timer", "key": "approval:1"},
                 {**approval(), "key": "different-wait"}, {**approval(), "event": {}},
                 approval(ticket_id=None), approval(draft_task_id=None),
                 approval("true"), approval(1), approval(None)]
        missing_binding = approval()
        del missing_binding["event"]["data"]["draft_task_id"]
        wakes.append(missing_binding)
        for wake in wakes:
            with self.subTest(wake=wake):
                self.assert_failure(FakeContext("finish", state=checkpoint(), wake=wake), "invalid_approval")

    def test_missing_or_mismatched_checkpoint_fails_without_relaunch(self):
        for state in (None, {}, checkpoint(draft_task_id=""), checkpoint(draft=draft(ticket_id="OTHER"))):
            with self.subTest(state=state):
                self.assert_failure(FakeContext("finish", state=state, wake=approval()), "invalid_draft")

    def test_replayed_activations_return_identical_decisions(self):
        for continuation, options in (
            ("start", {}),
            ("review", {"inputs": {"draft": child()}}),
            ("finish", {"state": checkpoint(), "wake": approval()}),
        ):
            with self.subTest(continuation=continuation):
                first = run(FakeContext(continuation, **options))
                replay = run(FakeContext(continuation, **options))
                self.assertEqual(first, replay)
                if continuation != "start":
                    self.assertEqual(first.get("commands", []), [])

    def test_unknown_entrypoint_is_rejected_by_the_registry(self):
        with self.assertRaisesRegex(WorkflowError, "unknown workflow entrypoint"):
            run(FakeContext("unknown"))


class ProtocolDecisionTests(unittest.IsolatedAsyncioTestCase):
    async def test_decisions_validate_with_real_sdk_context_and_no_rpc(self):
        from ledgence.worker.workflow import WorkflowContext

        async def unexpected_rpc(*args):
            self.fail("the controller must not perform local RPC or provider work")

        state = None
        for revision, continuation in enumerate(("start", "review", "finish")):
            payload = {"v": 1, "workflow_id": "workflow-demo", "activation_id": f"activation-{revision}",
                       "revision": revision, "continuation": continuation, "state": state,
                       "inputs": {"draft": child()} if continuation == "review" else {},
                       "local_steps": [], "wake": approval() if continuation == "finish" else None}
            context = WorkflowContext(payload, unexpected_rpc)
            try:
                decision = await dispatch(context)
                self.assertIs(context.entrypoint, program.Entry(continuation))
                self.assertEqual(context._validate_decision(decision), decision)
                state = decision.get("state")
            finally:
                await context._finish()
        self.assertEqual(decision["output"]["status"], "approved")


if __name__ == "__main__":
    unittest.main()
