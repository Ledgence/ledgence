"""Controller durability and terminal outcomes using the real SDK context (MIT)."""

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

from ledgence.worker.workflow import WorkflowContext, WorkflowError, _workflow


def child(output=None, state="succeeded", *, branch=False):
    outcome = {"kind": state}
    if not branch and state != "cancelled":
        outcome.update(attempt_id="attempt-child", quiescence="confirmed", execution_may_have_started=True)
    if state == "succeeded":
        outcome["output"] = output
    elif state == "failed":
        error = {"kind": "fixture_failure", "message": "deliberate infrastructure failure"}
        outcome.update(error=error) if branch else outcome.update(failure={"kind": "application", "error": error})
    identity = {"kind": "workflow", "workflow_id": "workflow-review"} if branch else {"task_id": "task-child"}
    return {**identity, "state": state, "outcome": outcome}


def candidate():
    # The controller transports candidate bytes; helper tests validate the source.
    return {"change_id": "change-1", "sha256": "a" * 64, "base_sha256": "b" * 64,
            "source": "def shipping_total(): return 0\n", "patch": "fixture patch",
            "summary": "Fixture candidate", "execution": {"provider": "offline-fixture"}}


class WorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.contexts, self.requests = [], []
        self.event = {"data": {"change_id": "change-1", "model": "fixture-model"}}

    async def asyncTearDown(self):
        for context in self.contexts:
            await context._finish(cancel=True)

    def context(self, continuation="start", *, inputs=None, state=None, local_steps=None, rpc=None):
        async def commit(operation, request):
            self.requests.append((operation, copy.deepcopy(request)))
            return ({"committed": True, "key": request["key"],
                     "branch_keys": [branch["key"] for branch in request["branches"]]}
                    if operation == "workflow.fork" else {"committed": True})
        context = WorkflowContext({"v": 1, "workflow_id": "workflow-1", "activation_id": "activation-1",
                                   "revision": 0, "continuation": continuation, "state": state,
                                   "inputs": inputs or {}, "local_steps": local_steps or []}, rpc or commit)
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
        for data in (None, {}, {"change_id": "bad name"}, {"change_id": "ok", "unknown": True}):
            context = self.context()
            result = await self.invoke(context, {"data": data})
            self.assertEqual(result["error"]["kind"], "invalid_input")
            self.assertEqual(self.requests, [])

    async def test_fork_ack_precedes_local_execution_and_join_does_not_wait_in_python(self):
        registered, acknowledge = asyncio.Event(), asyncio.Event()
        tests_started = asyncio.Event()
        value = candidate()

        async def rpc(operation, request):
            self.requests.append((operation, copy.deepcopy(request)))
            if operation == "workflow.fork":
                registered.set()
                await acknowledge.wait()
                return {"committed": True, "key": request["key"], "branch_keys": ["review:0"]}
            return {"committed": True}

        async def tests(*, candidate):
            tests_started.set()
            return {"candidate_sha256": candidate["sha256"], "passed": True}

        context = self.context("validate", inputs={"implement:0": child(value)}, rpc=rpc)
        with patch.object(program, "run_tests", tests):
            task = asyncio.create_task(self.invoke(context))
            await asyncio.wait_for(registered.wait(), 1)
            self.assertFalse(tests_started.is_set())
            self.assertFalse(task.done())
            acknowledge.set()
            decision = await asyncio.wait_for(task, 1)
        self.assertEqual(decision["kind"], "suspend")
        self.assertEqual(decision["continuation"], "collect")
        self.assertEqual(decision["until"], ["review:0"])
        self.assertEqual(decision["commands"], [])
        self.assertEqual(decision["state"]["candidate"], value)
        self.assertEqual([operation for operation, _ in self.requests], ["workflow.fork", "local_step.commit"])
        branch = self.requests[0][1]["branches"][0]
        self.assertEqual(branch["data"]["candidate"], decision["state"]["candidate"])
        self.assertEqual(branch["entrypoint"], "review")
        self.assertEqual(branch["retry_policy"]["max_attempts"], 1)

    async def test_lost_fork_ack_reconciles_same_binding_before_tests(self):
        persisted, local_records = [], []
        calls = 0
        lose = True

        async def tests(*, candidate):
            nonlocal calls
            calls += 1
            return {"candidate_sha256": candidate["sha256"], "passed": True}

        async def rpc(operation, request):
            nonlocal lose
            if operation == "workflow.fork":
                if not persisted:
                    persisted.append(copy.deepcopy(request))
                self.assertEqual(request, persisted[0])
                if lose:
                    lose = False
                    raise OSError("lost accepted fork reply")
                return {"committed": True, "key": request["key"], "branch_keys": ["review:0"]}
            local_records.append(copy.deepcopy(request))
            return {"committed": True}

        inputs = {"implement:0": child(candidate())}
        with patch.object(program, "run_tests", tests):
            failed = self.context("validate", inputs=inputs, rpc=rpc)
            with self.assertRaisesRegex(OSError, "lost accepted fork reply"):
                await self.invoke(failed)
            self.assertEqual(calls, 0)
            with self.assertRaises(WorkflowError):
                failed.complete("cannot hide uncertainty")
            decision = await self.invoke(self.context("validate", inputs=inputs, rpc=rpc))
        self.assertEqual(decision["continuation"], "collect")
        self.assertEqual(calls, 1)
        self.assertEqual(len(persisted), 1)
        self.assertEqual(len(local_records), 1)

    async def test_accepted_local_result_replays_after_lost_ack_without_running_tests_again(self):
        records = []
        calls = 0

        async def tests(*, candidate):
            nonlocal calls
            calls += 1
            return {"candidate_sha256": candidate["sha256"], "passed": False, "failures": 1}

        async def rpc(operation, request):
            if operation == "workflow.fork":
                return {"committed": True, "key": request["key"], "branch_keys": ["review:0"]}
            records.append(copy.deepcopy(request))
            raise OSError("lost accepted local-result reply")

        inputs = {"implement:0": child(candidate())}
        with patch.object(program, "run_tests", tests):
            failed = self.context("validate", inputs=inputs, rpc=rpc)
            with self.assertRaisesRegex(OSError, "lost accepted local-result reply"):
                await self.invoke(failed)
            with self.assertRaises(WorkflowError):
                failed.complete("cannot hide uncertainty")
            self.assertEqual(len(records), 1)
            decision = await self.invoke(self.context("validate", inputs=inputs, local_steps=records))
        self.assertEqual(calls, 1)
        self.assertFalse(decision["state"]["tests"]["passed"])
        self.assertEqual(decision["until"], ["review:0"])
        self.assertEqual([operation for operation, _ in self.requests], ["workflow.fork"])

    async def test_terminal_child_failures_do_not_schedule_more_work(self):
        for continuation, key, kind, branch in (("validate", "implement:0", "implementation_failed", False),
                                                ("collect", "review:0", "review_failed", True),
                                                ("finish", "finalize:0", "finalization_failed", False)):
            for state in ("failed", "cancelled"):
                with self.subTest(continuation=continuation, state=state):
                    decision = await self.invoke(self.context(continuation, inputs={key: child(state=state, branch=branch)}))
                    self.assertEqual(decision["error"]["kind"], kind)
                    self.assertEqual(self.requests, [])

    async def test_negative_review_and_test_reports_reach_finalizer_without_reexecution(self):
        value = candidate()
        tests = {"candidate_sha256": value["sha256"], "passed": False}
        review = {"candidate_sha256": value["sha256"], "verdict": "request_changes", "findings": [{"message": "fix"}]}
        decision = await self.invoke(self.context("collect", state={"candidate": value, "tests": tests},
                                                 inputs={"review:0": child(review, branch=True)}))
        self.assertEqual(decision["continuation"], "finish")
        command = decision["commands"][0]
        self.assertEqual(command["key"], "finalize:0")
        self.assertEqual(command["data"], {"candidate": value, "tests": tests, "review": review})
        bundle = {**command["data"], "change_id": "change-1", "status": "needs_changes"}
        finished = await self.invoke(self.context("finish", inputs={"finalize:0": child(bundle)}))
        self.assertEqual(finished["output"], bundle)
        self.assertEqual(self.requests, [])

    async def test_review_replay_uses_acknowledged_output_without_another_codex_call(self):
        calls = 0

        async def review(*, candidate, model):
            nonlocal calls
            calls += 1
            return {"candidate_sha256": candidate["sha256"], "verdict": "approve", "findings": []}

        event = {"data": {"candidate": candidate(), "model": "fixture-model"}}
        with patch.object(program, "review_candidate", review):
            decision = await self.invoke(self.context("review"), event)
            records = [request for operation, request in self.requests if operation == "local_step.commit"]
            replay = await self.invoke(self.context("review", local_steps=records), event)
        self.assertEqual(replay, decision)
        self.assertEqual(calls, 1)
        self.assertEqual(len(self.requests), 1)


if __name__ == "__main__":
    unittest.main()
