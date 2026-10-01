"""Controller durability and human decisions using the real SDK context (MIT)."""

import asyncio
import copy
import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT / "sdk/python"))
sys.path.insert(0, str(HERE))
SPEC = importlib.util.spec_from_file_location("change_review_workflow", HERE / "program.py")
program = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(program)

from change_review.candidate import BASE_SOURCE, make_candidate
from change_review.config import AGENT_QUEUE, APPROVAL_KEY, CONTROL_QUEUE, DEFAULT_APPROVAL_TIMEOUT_MS, VERSION
from ledgence.worker.workflow import WorkflowContext, WorkflowError, _workflow


CHECKS = ["tests:0", "review:0", "note:0"]


def child(output=None, state="succeeded", *, branch=False):
    outcome = {"kind": state}
    if not branch and state != "cancelled":
        outcome.update(attempt_id="attempt-child", quiescence="confirmed", execution_may_have_started=True)
    if state == "succeeded":
        outcome["output"] = output
    elif state == "failed":
        error = {"kind": "fixture_failure", "message": "deliberate infrastructure failure"}
        outcome.update(error=error) if branch else outcome.update(failure={"kind": "application", "error": error})
    identity = {"kind": "workflow", "workflow_id": "workflow-child"} if branch else {"task_id": "task-child"}
    return {**identity, "state": state, "outcome": outcome}


def candidate():
    execution = {"provider": "codex", "model": "fixture-model", "cli_version": "fixture",
                 "thread_id": "thread-fixture", "cli_invocations": 1,
                 "usage": {"input_tokens": 1, "cached_input_tokens": 0,
                           "output_tokens": 1, "reasoning_output_tokens": None}}
    return make_candidate("change-1", BASE_SOURCE.replace("total_cents > 10000", "total_cents >= 10000"),
                          "Include the free-shipping threshold.", execution)


def packet(*, status="waiting_for_approval"):
    # Controller tests stub the separately tested report validator. Native
    # acceptance tests exercise the complete workflow with real packet helpers.
    value = candidate()
    return {"workflow_id": "workflow-1", "change_id": "change-1", "status": status,
            "candidate": value, "comparison": {"candidate_sha256": value["sha256"]},
            "tests": {"candidate_sha256": value["sha256"], "passed": status != "needs_changes"},
            "review": {"candidate_sha256": value["sha256"], "verdict": "approve"},
            "note": {"candidate_sha256": value["sha256"], "text": "Shipping is free at $100."},
            "decision": None, "pull_request": {"title": "Fixture", "body": "Evidence", "url": None}}


def approval_wake(value, *, approved=True):
    return {"kind": "event", "key": APPROVAL_KEY, "accepted_at": 1,
            "event": {"specversion": "1.0", "id": "decision-1", "source": "urn:demo:test",
                      "type": "ledgence.change-review.decision", "datacontenttype": "application/json",
                      "data": {"workflow_id": value["workflow_id"],
                               "candidate_sha256": value["candidate"]["sha256"], "approved": approved}}}


class WorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.contexts, self.requests = [], []
        self.event = {"data": {"change_id": "change-1", "model": "fixture-model"}}

    async def asyncTearDown(self):
        for context in self.contexts:
            await context._finish(cancel=True)

    def context(self, continuation="start", *, inputs=None, state=None, local_steps=None, rpc=None, wake=None):
        async def commit(operation, request):
            self.requests.append((operation, copy.deepcopy(request)))
            return ({"committed": True, "key": request["key"],
                     "branch_keys": [branch["key"] for branch in request["branches"]]}
                    if operation == "workflow.fork" else {"committed": True})
        context = WorkflowContext({"v": 1, "workflow_id": "workflow-1", "activation_id": "activation-1",
                                   "revision": 0, "continuation": continuation, "state": state,
                                   "inputs": inputs or {}, "local_steps": local_steps or [], "wake": wake}, rpc or commit)
        self.contexts.append(context)
        return context

    async def invoke(self, context, event=None):
        token = _workflow.set(context)
        try:
            decision = await program.handle(self.event if event is None else event)
            await context._drain()
            return context._validate_decision(decision)
        finally:
            _workflow.reset(token)

    async def test_invalid_input_never_stages_an_agent(self):
        values = [None, {}, {"change_id": "bad name"}, {"change_id": "ok", "unknown": True}]
        values += [{"change_id": "ok", "approval_timeout_ms": timeout} for timeout in (True, -1, 86_400_001, "100")]
        for data in values:
            with self.subTest(data=data):
                result = await self.invoke(self.context(), {"data": data})
                self.assertEqual(result["error"]["kind"], "invalid_input")
                self.assertEqual(self.requests, [])

    async def test_start_stages_one_versioned_agent_task(self):
        result = await self.invoke(self.context())
        self.assertEqual(result["continuation"], "check_candidate")
        self.assertEqual(result["until"], ["implement:0"])
        self.assertEqual(len(result["commands"]), 1)
        command = result["commands"][0]
        self.assertEqual(command["program"]["version"], VERSION)
        self.assertEqual(command["queue"], AGENT_QUEUE)
        self.assertEqual(command["retry_policy"]["max_attempts"], 1)

    async def test_fork_ack_precedes_local_comparison_and_join_checkpoints_all_branches(self):
        registered, acknowledge, comparison_started = asyncio.Event(), asyncio.Event(), asyncio.Event()
        value = candidate()

        async def rpc(operation, request):
            self.requests.append((operation, copy.deepcopy(request)))
            if operation == "workflow.fork":
                registered.set()
                await acknowledge.wait()
                return {"committed": True, "key": request["key"], "branch_keys": CHECKS}
            return {"committed": True}

        async def comparison(*, candidate):
            comparison_started.set()
            return {"candidate_sha256": candidate["sha256"], "after": 0}

        context = self.context("check_candidate", inputs={"implement:0": child(value)}, rpc=rpc)
        with patch.object(program, "compare_candidate", comparison):
            task = asyncio.create_task(self.invoke(context))
            await asyncio.wait_for(registered.wait(), 1)
            self.assertFalse(comparison_started.is_set())
            self.assertFalse(task.done())
            acknowledge.set()
            decision = await asyncio.wait_for(task, 1)
        self.assertEqual(decision["kind"], "suspend")
        self.assertEqual(decision["continuation"], "prepare_review")
        self.assertEqual(decision["until"], CHECKS)
        self.assertEqual(decision["commands"], [])
        self.assertEqual(decision["state"]["candidate"], value)
        self.assertEqual([operation for operation, _ in self.requests], ["workflow.fork", "local_step.commit"])
        request = self.requests[0][1]
        self.assertEqual(request["key"], "checks:0")
        self.assertEqual([branch["entrypoint"] for branch in request["branches"]], ["run_tests", "review_code", "draft_note"])
        self.assertEqual([branch["queue"] for branch in request["branches"]], [CONTROL_QUEUE, AGENT_QUEUE, AGENT_QUEUE])
        for branch in request["branches"]:
            self.assertEqual(branch["data"]["candidate"], value)
            self.assertEqual(branch["retry_policy"]["max_attempts"], 1)

    async def test_invalid_candidate_cannot_start_a_fork(self):
        for change in ({"sha256": "0" * 64}, {"change_id": "another-change"}):
            result = await self.invoke(self.context("check_candidate", inputs={"implement:0": child(dict(candidate(), **change))}))
            self.assertEqual(result["error"]["kind"], "invalid_candidate")
            self.assertEqual(self.requests, [])

    async def test_lost_fork_ack_reconciles_same_binding_before_local_work(self):
        persisted, local_records = [], []
        calls, lose = 0, True

        async def comparison(*, candidate):
            nonlocal calls
            calls += 1
            return {"candidate_sha256": candidate["sha256"], "after": 0}

        async def rpc(operation, request):
            nonlocal lose
            if operation == "workflow.fork":
                if not persisted:
                    persisted.append(copy.deepcopy(request))
                self.assertEqual(request, persisted[0])
                if lose:
                    lose = False
                    raise OSError("lost accepted fork reply")
                return {"committed": True, "key": request["key"], "branch_keys": CHECKS}
            local_records.append(copy.deepcopy(request))
            return {"committed": True}

        inputs = {"implement:0": child(candidate())}
        with patch.object(program, "compare_candidate", comparison):
            failed = self.context("check_candidate", inputs=inputs, rpc=rpc)
            with self.assertRaisesRegex(OSError, "lost accepted fork reply"):
                await self.invoke(failed)
            self.assertEqual(calls, 0)
            with self.assertRaises(WorkflowError):
                failed.complete("cannot hide uncertainty")
            decision = await self.invoke(self.context("check_candidate", inputs=inputs, rpc=rpc))
        self.assertEqual(decision["continuation"], "prepare_review")
        self.assertEqual(calls, 1)
        self.assertEqual(len(persisted), 1)
        self.assertEqual(len(local_records), 1)

    async def test_accepted_local_result_replays_after_lost_ack_without_reexecution(self):
        records, calls = [], 0

        async def comparison(*, candidate):
            nonlocal calls
            calls += 1
            return {"candidate_sha256": candidate["sha256"], "after": 0}

        async def rpc(operation, request):
            if operation == "workflow.fork":
                return {"committed": True, "key": request["key"], "branch_keys": CHECKS}
            records.append(copy.deepcopy(request))
            raise OSError("lost accepted local-result reply")

        inputs = {"implement:0": child(candidate())}
        with patch.object(program, "compare_candidate", comparison):
            failed = self.context("check_candidate", inputs=inputs, rpc=rpc)
            with self.assertRaisesRegex(OSError, "lost accepted local-result reply"):
                await self.invoke(failed)
            with self.assertRaises(WorkflowError):
                failed.complete("cannot hide uncertainty")
            decision = await self.invoke(self.context("check_candidate", inputs=inputs, local_steps=records))
        self.assertEqual(calls, 1)
        self.assertEqual(decision["state"]["comparison"]["after"], 0)
        self.assertEqual(decision["until"], CHECKS)
        self.assertEqual([operation for operation, _ in self.requests], ["workflow.fork"])

    async def test_terminal_child_failures_do_not_schedule_more_work(self):
        cases = [("check_candidate", "implement:0", "implementation_failed", False),
                 ("await_decision", "prepare:0", "preparation_failed", False),
                 ("finish", "finalize:0", "finalization_failed", False)]
        cases += [("prepare_review", key, "check_failed", True) for key in CHECKS]
        for continuation, key, kind, branch in cases:
            for state in ("failed", "cancelled"):
                with self.subTest(continuation=continuation, key=key, state=state):
                    inputs = {name: child({}, branch=True) for name in CHECKS} if branch else {}
                    inputs[key] = child(state=state, branch=branch)
                    decision = await self.invoke(self.context(continuation, inputs=inputs))
                    self.assertEqual(decision["error"]["kind"], kind)
                    self.assertEqual(self.requests, [])

    async def test_reports_reach_public_packet_task_before_human_wait(self):
        for status in ("waiting_for_approval", "needs_changes"):
            bundle = packet(status=status)
            inputs = {key: child(bundle[key.split(":")[0]], branch=True) for key in CHECKS}
            with patch.object(program, "assemble_bundle", return_value=bundle) as validator:
                decision = await self.invoke(self.context("prepare_review", state={"candidate": bundle["candidate"],
                                                         "comparison": bundle["comparison"]}, inputs=inputs))
            self.assertEqual(decision["continuation"], "await_decision")
            command = decision["commands"][0]
            self.assertEqual(command["key"], "prepare:0")
            self.assertIsNone(command["data"]["decision"])
            self.assertNotIn("publication", command["data"])
            self.assertEqual(command["data"], validator.call_args.args[0])
            with patch.object(program, "assemble_bundle", return_value=bundle):
                result = await self.invoke(self.context("await_decision", inputs={"prepare:0": child(bundle)}))
            if status == "needs_changes":
                self.assertEqual(result["kind"], "complete")
                self.assertEqual(result["output"], bundle)
            else:
                self.assertEqual(result["kind"], "wait")
                self.assertEqual(result["continuation"], "on_decision")
                self.assertEqual(result["wait"], {"kind": "event", "key": APPROVAL_KEY,
                                                  "timeout_ms": DEFAULT_APPROVAL_TIMEOUT_MS})
                self.assertEqual(result["state"], {"bundle": bundle})
                self.assertEqual(result["commands"], [])

    async def test_human_wait_accepts_zero_expiry_and_rejects_tampered_packet(self):
        bundle = packet()
        with patch.object(program, "assemble_bundle", return_value=bundle):
            result = await self.invoke(self.context("await_decision", inputs={"prepare:0": child(bundle)}),
                                       {"data": {**self.event["data"], "approval_timeout_ms": 0}})
            self.assertEqual(result["wait"]["timeout_ms"], 0)
            for field, value in (("status", "approved"), ("change_id", "another"), ("workflow_id", "another"),
                                 ("decision", {"outcome": "approved"}), ("extra", True)):
                with self.subTest(field=field):
                    bad = dict(bundle, **{field: value})
                    result = await self.invoke(self.context("await_decision", inputs={"prepare:0": child(bad)}))
                    self.assertEqual(result["error"]["kind"], "invalid_packet")

    async def test_saved_packet_resumes_for_approve_reject_or_expiry_without_codex(self):
        bundle = packet()
        wakes = [(approval_wake(bundle), "approved"), (approval_wake(bundle, approved=False), "rejected"),
                 ({"kind": "timeout", "key": APPROVAL_KEY, "deadline": 1}, "expired")]
        for wake, outcome in wakes:
            with self.subTest(outcome=outcome), patch.object(program, "assemble_bundle", return_value=bundle), \
                    patch.object(program, "review_candidate") as review, patch.object(program, "draft_note") as note, \
                    patch.object(program, "compare_candidate") as comparison:
                result = await self.invoke(self.context("on_decision", state={"bundle": bundle}, wake=wake))
                replay = await self.invoke(self.context("on_decision", state={"bundle": bundle}, wake=wake))
                self.assertEqual(result, replay)
                self.assertEqual(result["continuation"], "finish")
                command = result["commands"][0]
                self.assertEqual(command["key"], "finalize:0")
                self.assertEqual(command["data"]["decision"], {
                    "workflow_id": "workflow-1", "candidate_sha256": bundle["candidate"]["sha256"],
                    "outcome": outcome, "event_id": None if outcome == "expired" else "decision-1"})
                self.assertNotIn("publication", command["data"])
                review.assert_not_called()
                note.assert_not_called()
                comparison.assert_not_called()
        self.assertEqual(self.requests, [])

    async def test_decision_must_match_exact_workflow_candidate_and_boolean(self):
        bundle = packet()
        changes = [{"workflow_id": "workflow-other"}, {"candidate_sha256": "0" * 64}, {"approved": 1},
                   {"approved": None}, {"extra": True}]
        for change in changes:
            wake = approval_wake(bundle)
            wake["event"]["data"].update(change)
            with self.subTest(change=change), patch.object(program, "assemble_bundle", return_value=bundle):
                result = await self.invoke(self.context("on_decision", state={"bundle": bundle}, wake=wake))
                self.assertEqual(result["error"]["kind"], "invalid_decision")
        for wake in (None, {"kind": "timeout", "key": "another", "deadline": 1},
                     {"kind": "timer", "key": APPROVAL_KEY, "deadline": 1}):
            with patch.object(program, "assemble_bundle", return_value=bundle):
                result = await self.invoke(self.context("on_decision", state={"bundle": bundle}, wake=wake))
                self.assertEqual(result["error"]["kind"], "invalid_decision")
        self.assertEqual(self.requests, [])

    async def test_every_branch_replays_accepted_output_without_running_again(self):
        for entrypoint, function, key in (("run_tests", "run_tests", "tests:0"),
                                          ("review_code", "review_candidate", "review:0"),
                                          ("draft_note", "draft_note", "note:0")):
            calls = 0
            async def operation(**kwargs):
                nonlocal calls
                calls += 1
                return {"candidate_sha256": kwargs["candidate"]["sha256"], "accepted": True}
            self.requests.clear()
            event = {"data": {"candidate": candidate(), "model": "fixture-model"}}
            with self.subTest(entrypoint=entrypoint), patch.object(program, function, operation):
                result = await self.invoke(self.context(entrypoint), event)
                records = [request for kind, request in self.requests if kind == "local_step.commit"]
                replay = await self.invoke(self.context(entrypoint, local_steps=records), event)
            self.assertEqual(replay, result)
            self.assertEqual(calls, 1)
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["key"], key)
            self.assertEqual(len(self.requests), 1)

    async def test_only_explicit_approval_passes_publication_to_finalizer(self):
        bundle = packet()
        data = {**self.event["data"], "publication": {"repository": "demo/fixture"}}
        # Publication target validation is covered by publishing tests; the
        # controller's responsibility is withholding it before human approval.
        with patch.object(program, "submission", return_value=data), \
                patch.object(program, "assemble_bundle", return_value=bundle):
            for approved in (False, True):
                result = await self.invoke(self.context("on_decision", state={"bundle": bundle},
                                                       wake=approval_wake(bundle, approved=approved)))
                self.assertEqual("publication" in result["commands"][0]["data"], approved)
            result = await self.invoke(self.context("on_decision", state={"bundle": bundle},
                                                   wake={"kind": "timeout", "key": APPROVAL_KEY, "deadline": 1}))
            self.assertNotIn("publication", result["commands"][0]["data"])

    async def test_finish_returns_final_task_output(self):
        final = {**packet(), "status": "approved"}
        result = await self.invoke(self.context("finish", inputs={"finalize:0": child(final)}))
        self.assertEqual(result["kind"], "complete")
        self.assertEqual(result["output"], final)


if __name__ == "__main__":
    unittest.main()
