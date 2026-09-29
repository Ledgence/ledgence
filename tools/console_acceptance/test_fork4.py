"""Exercise the packaged release workflow with the real SDK decision validator."""
import json
from pathlib import Path
import sys
import types
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk/python"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ledgence.worker.workflow import WorkflowContext, _workflow
from console_acceptance.fork4 import BRANCH_ENTRIES, BRANCH_KEYS, SEQUENCE, completed_attempt
from console_acceptance.test_explorer import task_result


def workflow_result(output=None, state="succeeded"):
    outcome = {"kind": state}
    if state == "succeeded":
        outcome["output"] = output
    elif state == "failed":
        outcome["error"] = {"kind": "check_failed", "message": "Check failed"}
    return {"kind": "workflow", "workflow_id": "check-workflow", "state": state, "outcome": outcome}


class AttemptEvidenceTests(unittest.TestCase):
    def setUp(self):
        # Unit metadata only; native acceptance still reads the real API. The
        # projection's durable completed report can lack process_instance_id.
        self.detail = {
            "attempt": {"task_id": "task-1", "attempt_id": "attempt-1", "state": "succeeded",
                        "finished_at": 1000, "execution_may_have_started": True,
                        "worker_session_id": "worker-1"},
            "process_id": 42, "process_instance_id": None,
            "reused_process": False, "worker_elapsed_ms": "0",
        }

    def test_completed_settlement_does_not_require_optional_process_identity(self):
        self.assertEqual(completed_attempt(self.detail, "task-1", "attempt-1"), "attempt-1")

    def test_missing_execution_or_terminal_evidence_still_fails(self):
        for field, value in (("state", "active"), ("finished_at", None),
                             ("execution_may_have_started", False), ("worker_session_id", ""),
                             ("task_id", "other"), ("attempt_id", "other")):
            with self.subTest(field=field):
                detail = {**self.detail, "attempt": {**self.detail["attempt"], field: value}}
                with self.assertRaises(AssertionError):
                    completed_attempt(detail, "task-1", "attempt-1")

    def test_missing_completed_report_evidence_still_fails(self):
        for field, value in (("process_id", None), ("process_id", 0),
                             ("reused_process", None), ("worker_elapsed_ms", None)):
            with self.subTest(field=field, value=value):
                with self.assertRaises(AssertionError):
                    completed_attempt({**self.detail, field: value}, "task-1", "attempt-1")


class ReleaseWorkflowFixtureTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.program = self.load("fork4_program.py")
        self.task = self.load("fork4_task.py")
        self.operations = []
        self.counter = 0
        self.data = {"queue": "test", "release": "2026.09", "sequence": SEQUENCE, "mode": "approved"}

    def load(self, name):
        module = types.ModuleType("console_" + name.removesuffix(".py"))
        source = Path(__file__).with_name(name).read_text()
        exec(compile(source, name, "exec"), module.__dict__)
        return module

    async def invoke(self, entrypoint, *, state=None, inputs=None, data=None):
        async def rpc(operation, payload):
            self.assertEqual(operation, "workflow.fork")
            self.operations.append((operation, json.loads(json.dumps(payload))))
            return {"committed": True, "key": payload["key"],
                    "branch_keys": [branch["key"] for branch in payload["branches"]]}

        self.counter += 1
        context = WorkflowContext({"v": 1, "workflow_id": "workflow-1",
            "activation_id": "activation-" + str(self.counter), "revision": self.counter,
            "continuation": entrypoint, "state": state, "inputs": inputs or {}, "local_steps": []}, rpc)
        token = _workflow.set(context)
        try:
            decision = await self.program.handle({"data": self.data if data is None else data})
            context._validate_decision(decision)
            return decision
        finally:
            await context._finish(cancel=True)
            _workflow.reset(token)

    def task_output(self, decision, key):
        self.assertEqual(len(decision["commands"]), 1)
        command, = decision["commands"]
        self.assertEqual(command["key"], key)
        self.assertEqual(command["program"], {"id": "explorer-release-task", "version": "1.0.0"})
        return self.task.handle({"data": command["data"]})

    async def prepared_join(self):
        start = await self.invoke("start")
        self.assertEqual(start["until"], ["prepare:0"])
        self.assertEqual(start["continuation"], "after_prepare")
        self.assertEqual(start["state"], {"branch_keys": BRANCH_KEYS})
        operation, fork = self.operations[-1]
        self.assertEqual(operation, "workflow.fork")
        self.assertEqual(fork["key"], "release-checks:0")
        self.assertEqual([branch["key"] for branch in fork["branches"]], BRANCH_KEYS)
        self.assertEqual({branch["key"]: branch["entrypoint"] for branch in fork["branches"]}, BRANCH_ENTRIES)
        prepared = self.task_output(start, "prepare:0")
        # JSON round-trip ensures the next activation receives plain durable
        # state, not a Python ForkRef that only existed in start's context.
        join = await self.invoke("after_prepare", state=json.loads(json.dumps(start["state"])),
                                 inputs={"prepare:0": task_result(prepared)})
        self.assertNotEqual(start["activation_id"], join["activation_id"])
        self.assertEqual(join["until"], BRANCH_KEYS)
        self.assertEqual(join["continuation"], "publish_draft")
        self.assertEqual(join["commands"], [])
        self.assertEqual(len(self.operations), 1, "join must not register a second fork")
        self.assertEqual(prepared["sequence"], SEQUENCE)
        inputs = {}
        for branch in fork["branches"]:
            result = await self.invoke(branch["entrypoint"], data=branch["data"])
            self.assertEqual(result["kind"], "complete")
            inputs[branch["key"]] = workflow_result(result["output"])
        return join, inputs

    async def test_full_fork_prepare_join_draft_review_final_cycle(self):
        join, inputs = await self.prepared_join()
        publish = await self.invoke("publish_draft", state=join["state"], inputs=inputs)
        draft = self.task_output(publish, "publish-draft:0")
        self.assertEqual(publish["until"], ["publish-draft:0"])
        self.assertEqual(publish["continuation"], "review")
        review = await self.invoke("review", inputs={"publish-draft:0": task_result(draft)})
        approval = self.task_output(review, "review-draft:0")
        self.assertEqual(review["until"], ["review-draft:0"])
        self.assertEqual(review["continuation"], "publish_report")
        report = await self.invoke("publish_report", state=review["state"],
                                   inputs={"review-draft:0": task_result(approval)})
        final = self.task_output(report, "publish-final:0")
        self.assertEqual(report["until"], ["publish-final:0"])
        self.assertEqual(report["continuation"], "finish")
        finished = await self.invoke("finish", inputs={"publish-final:0": task_result(final)})
        self.assertEqual(finished["kind"], "complete")
        self.assertEqual(finished["output"]["draft"], draft)
        self.assertEqual(finished["output"]["review"], approval)
        self.assertEqual(finished["output"]["draft"]["prepared"]["sequence"], SEQUENCE)

    async def test_rejected_review_never_stages_final_publication(self):
        decision = await self.invoke("publish_report", state={"draft": {}},
            inputs={"review-draft:0": task_result({"approved": False})})
        self.assertEqual(decision["kind"], "fail")
        self.assertEqual(decision["error"]["kind"], "review_rejected")
        self.assertNotIn("commands", decision)
        self.assertEqual(self.operations, [])

    async def test_approval_must_belong_to_the_draft_being_published(self):
        decision = await self.invoke("publish_report", state={"draft": {"draft_id": "current"}},
            inputs={"review-draft:0": task_result({"approved": True, "draft_id": "other"})})
        self.assertEqual(decision["error"]["kind"], "review_mismatch")
        self.assertNotIn("commands", decision)

    async def test_event_gate_reenters_after_prepare_then_joins_persisted_keys(self):
        self.data["mode"] = "join_satisfied"
        start = await self.invoke("start")
        prepared = self.task_output(start, "prepare:0")
        waiting = await self.invoke("after_prepare", state=start["state"],
                                    inputs={"prepare:0": task_result(prepared)})
        self.assertEqual(waiting["kind"], "wait")
        self.assertEqual(waiting["wait"]["key"], "join-ready:0")
        self.assertEqual(waiting["continuation"], "after_prepare")
        joined = await self.invoke("after_prepare", state=waiting["state"])
        self.assertNotEqual(waiting["activation_id"], joined["activation_id"])
        self.assertEqual(joined["until"], BRANCH_KEYS)
        self.assertEqual(joined["continuation"], "publish_draft")
        self.assertEqual(joined["commands"], [])
        self.assertEqual(len(self.operations), 1)

    async def test_satisfied_join_does_not_treat_failed_or_cancelled_branch_as_success(self):
        join, inputs = await self.prepared_join()
        for state in ("failed", "cancelled"):
            with self.subTest(state=state):
                inputs["security:0"] = workflow_result(state=state)
                decision = await self.invoke("publish_draft", state=join["state"], inputs=inputs)
                self.assertEqual(decision["kind"], "fail")
                self.assertEqual(decision["error"]["kind"], "branch_" + state)
                self.assertNotIn("commands", decision)

    async def test_business_rejection_is_not_a_successful_check(self):
        join, inputs = await self.prepared_join()
        inputs["security:0"] = workflow_result({"passed": False})
        decision = await self.invoke("publish_draft", state=join["state"], inputs=inputs)
        self.assertEqual(decision["kind"], "fail")
        self.assertEqual(decision["error"]["kind"], "checks_rejected")

    async def test_failed_or_cancelled_parent_task_prevents_next_side_effect(self):
        cases = [("after_prepare", "prepare:0"), ("review", "publish-draft:0"),
                 ("publish_report", "review-draft:0"), ("finish", "publish-final:0")]
        for entrypoint, key in cases:
            for state in ("failed", "cancelled"):
                with self.subTest(entrypoint=entrypoint, state=state):
                    outcome = {"kind": state}
                    if state == "failed":
                        outcome.update(attempt_id="attempt-1", quiescence="confirmed",
                            execution_may_have_started=True,
                            failure={"kind": "application", "error": {"kind": "failed", "message": "Failed"}})
                    decision = await self.invoke(entrypoint, state={}, inputs={key: {
                        "task_id": "task-1", "state": state, "outcome": outcome}})
                    self.assertEqual(decision["kind"], "fail")
                    self.assertEqual(decision["error"]["kind"], "task_" + state)
                    self.assertNotIn("commands", decision)

    async def test_branch_failure_and_wait_for_real_cancellation_are_explicit(self):
        failed = await self.invoke("security_review", data={**self.data, "mode": "branch_failed"})
        self.assertEqual(failed["error"]["kind"], "security_check_failed")
        waiting = await self.invoke("security_review", data={**self.data, "mode": "branch_cancelled"})
        self.assertEqual(waiting["kind"], "wait")
        self.assertEqual(waiting["wait"]["key"], "cancel-check:0")

    def test_final_task_defensively_requires_actual_approval(self):
        with self.assertRaisesRegex(ValueError, "requires approval"):
            self.task.handle({"data": {"operation": "publish_final", "review": {"approved": False}}})
        with self.assertRaisesRegex(ValueError, "different draft"):
            self.task.handle({"data": {"operation": "publish_final", "draft": {"draft_id": "current"},
                                      "review": {"approved": True, "draft_id": "other"}}})
        with self.assertRaisesRegex(ValueError, "Unknown release task"):
            self.task.handle({"data": {"operation": "unrecognized"}})


if __name__ == "__main__":
    unittest.main()
