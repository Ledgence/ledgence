"""Workflow checkpoint, local durability, and interactive protocol tests (MIT)."""

import asyncio
import json
from pathlib import Path
import sys
import tracemalloc
import unittest

import test_bootstrap

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ledgence.worker.workflow import (
    SCHEMA, MAX_STATE_BYTES, WorkflowContext, WorkflowError, workflow_context,
)


def payload(**changes):
    return {"v": 1, "workflow_id": "workflow-1", "activation_id": "activation-1",
            "revision": 0, "continuation": "start", "state": None,
            "inputs": {}, "local_steps": [], **changes}


class WorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.commits = []
        self.contexts = []

    async def asyncTearDown(self):
        for context in self.contexts:
            await context._finish(cancel=True)

    def context(self, raw=None, rpc=None):
        async def commit(operation, value):
            self.assertEqual(operation, "local_step.commit")
            self.commits.append(json.loads(json.dumps(value)))
            return {"committed": True}
        context = WorkflowContext(payload() if raw is None else raw, rpc or commit)
        self.contexts.append(context)
        return context

    async def test_local_io_overlaps_and_waits_for_commit_ack(self):
        started = asyncio.Event()
        acknowledge = asyncio.Event()
        entered = 0
        async def operation(value):
            nonlocal entered
            entered += 1
            if entered == 2:
                started.set()
            await asyncio.wait_for(started.wait(), 1)
            return value
        async def commit(op, record):
            self.assertEqual(entered, 2)
            await acknowledge.wait()
            self.commits.append(record)
            return {"committed": True}
        context = self.context(rpc=commit)
        first = context.local("first", operation, value=1)
        second = context.local("second", operation, value=2)
        result = asyncio.create_task(context.gather(first, second))
        await asyncio.wait_for(started.wait(), 1)
        self.assertFalse(result.done(), "local results cannot precede durable acknowledgement")
        acknowledge.set()
        self.assertEqual(await asyncio.wait_for(result, 1), [1, 2])
        self.assertEqual(len(self.commits), 2)

    async def test_committed_local_record_survives_restart_and_returns_copies(self):
        calls = 0
        async def operation(value):
            nonlocal calls
            calls += 1
            return {"value": value}
        context = self.context()
        output = await context.local("read", operation, value=3)
        output["value"] = "changed"
        resumed = self.context(payload(local_steps=self.commits))
        again = await resumed.local("read", operation, value=3)
        again["value"] = "changed-again"
        self.assertEqual(await resumed.local("read", operation, value=3), {"value": 3})
        self.assertEqual(calls, 1)
        self.assertEqual(len(self.commits), 1)

    async def test_local_input_is_frozen_and_same_pending_key_shares_execution(self):
        calls = 0
        async def operation(value):
            nonlocal calls
            calls += 1
            await asyncio.sleep(0)
            return value
        context = self.context()
        data = {"items": [1]}
        first = context.local("read", operation, value=data)
        data["items"].append(2)
        second = context.local("read", operation, value={"items": [1]})
        self.assertEqual(await context.gather(first, second), [{"items": [1]}, {"items": [1]}])
        self.assertEqual(calls, 1)

    async def test_changed_input_or_callable_never_reexecutes_committed_key(self):
        def operation(value):
            return value
        def different(value):
            self.fail("different callable executed")
        for old, new in ((1, True), (-0.0, 0.0), ({"a": 1}, {"a": 2})):
            context = self.context()
            await context.local("key", operation, value=old)
            with self.assertRaises(WorkflowError):
                context.local("key", operation, value=new)
            with self.assertRaises(WorkflowError):
                context.local("key", different, value=old)

    async def test_acknowledgement_failure_cannot_be_swallowed_into_completion(self):
        for reply in ({"committed": False}, {"committed": 1}, None, {"committed": True, "extra": 1}):
            async def rpc(operation, record):
                return reply
            context = self.context(rpc=rpc)
            with self.assertRaises(WorkflowError):
                await context.local("read", lambda: 1)
            with self.assertRaises(WorkflowError):
                context.complete("incorrect-success")

    async def test_invalid_local_values_never_commit(self):
        for value in (float("nan"), 1 << 64, {1: "key"}, "x" * (128 * 1024)):
            context = self.context()
            with self.assertRaises(WorkflowError):
                await context.local("bad", lambda: value)
        self.assertEqual(self.commits, [])

    async def test_failed_local_sibling_does_not_abandon_successful_commit(self):
        finished = []
        async def failing():
            raise ValueError("failure")
        async def successful():
            await asyncio.sleep(.01)
            finished.append(True)
            return 42
        context = self.context()
        with self.assertRaisesRegex(ValueError, "failure"):
            await context.gather(context.local("a", failing), context.local("b", successful))
        self.assertEqual(finished, [True])
        self.assertEqual([record["key"] for record in self.commits], ["b"])

    async def test_cancelled_observer_does_not_abandon_local_operation(self):
        release = asyncio.Event()
        finished = []
        async def operation():
            await release.wait()
            finished.append(True)
            return 1
        context = self.context()
        local = context.local("key", operation)
        async def observe():
            return await local
        observer = asyncio.create_task(observe())
        await asyncio.sleep(0)
        observer.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await observer
        draining = asyncio.create_task(context._finish())
        await asyncio.sleep(0)
        self.assertFalse(draining.done())
        release.set()
        await draining
        self.assertEqual(finished, [True])
        self.assertEqual(len(self.commits), 1)
        with self.assertRaises(WorkflowError):
            context.local("late", operation)

    async def test_checkpoint_commands_are_frozen_and_no_dispatch_occurs(self):
        context = self.context()
        data, state = {"value": 3}, {"phase": "wait"}
        child = context.task("invoice", program="invoice-issuer", version="1.0.0",
                             queue="billing", data=data)
        decision = context.suspend(continuation="collect", state=state, until=[child])
        data["value"], state["phase"] = 99, "mutated"
        self.assertEqual(decision["commands"][0]["data"], {"value": 3})
        self.assertEqual(decision["state"], {"phase": "wait"})
        self.assertEqual(decision["until"], ["invoice"])
        self.assertEqual(decision["commands"][0]["retry_policy"],
                         {"max_attempts": 3, "retry_delay_ms": 5000})
        self.assertEqual(self.commits, [])
        with self.assertRaises(WorkflowError):
            context.complete(42)
        self.assertEqual(context.suspend(continuation="later", state=None,
                                         until=["previous-child"])["until"], ["previous-child"])
        with self.assertRaises(WorkflowError):
            self.context().suspend(continuation="bad", state=None, until=[child])

    async def test_resumed_context_exposes_child_results_without_mutable_aliases(self):
        context = self.context(payload(continuation="collect", state={"round": 1}, inputs={
            "child": {"task_id": "task-1", "state": "succeeded",
                      "outcome": {"kind": "succeeded", "attempt_id": "attempt-1",
                                  "quiescence": "confirmed", "execution_may_have_started": True,
                                  "output": {"amount": 3}}}}))
        state, inputs, result = context.state, context.inputs, context.get_result("child")
        state["round"] = 4
        inputs["child"]["state"] = "failed"
        result["amount"] = 7
        self.assertEqual(context.state, {"round": 1})
        self.assertEqual(context.get_result("child"), {"amount": 3})
        self.assertEqual(context.continue_(continuation="again", state=None)["kind"], "continue")
        self.assertEqual(context.fail("declined", "no approval")["kind"], "fail")

    async def test_json_depth_budget_is_separate_from_workflow_metadata(self):
        nested = None
        for _ in range(64):
            nested = [nested]
        context = self.context(payload(state=nested))
        self.assertEqual(context.complete(nested)["output"], nested)
        # Keyword arguments themselves are the persisted local input object.
        local_value = None
        for _ in range(63):
            local_value = [local_value]
        self.assertEqual(await context.local("deep", lambda value: nested, value=local_value), nested)
        restored = self.context(payload(local_steps=self.commits))
        self.assertEqual(restored._records["deep"]["output"], nested)
        with self.assertRaises(WorkflowError):
            context.complete([nested])
        with self.assertRaises(WorkflowError):
            context.local("too-deep", lambda value: value, value=nested)

    async def test_child_key_binding_replays_and_rejects_changed_input(self):
        context = self.context()
        args = dict(program="echo", version="1.0.0", queue="queue", data={"value": 1})
        first = context.task("child:0", **args)
        repeated = context.task("child:0", **args)
        self.assertEqual(first.key, repeated.key)
        decision = context.suspend(continuation="collect", state=None, until=[first])
        self.assertEqual(len(decision["commands"]), 1)
        with self.assertRaises(WorkflowError):
            context.task("child:0", **dict(args, data={"value": 1.0}))
        context.task("child:1", **args)
        self.assertEqual(len(context.continue_(continuation="next", state=None)["commands"]), 2)

    async def test_local_callable_context_blocks_nested_controls_and_thread_hops(self):
        context = self.context()
        operations = [
            lambda: context.local("nested", lambda: None),
            lambda: context.task("child", program="echo", version="1", queue="queue", data=None),
            lambda: context.workflow("nested", program="echo", version="1", queue="queue", data=None),
            lambda: context.complete(None),
            lambda: context.suspend(continuation="next", state=None),
            lambda: context.continue_(continuation="next", state=None),
            lambda: context.fail("failed", "failed"),
        ]
        for index, operation in enumerate(operations):
            async def local():
                return operation()
            with self.assertRaisesRegex(WorkflowError, "use the controller"):
                await context.local(str(index), local)
        async def thread_hop():
            return await asyncio.to_thread(operations[1])
        with self.assertRaisesRegex(WorkflowError, "use the controller"):
            await context.local("thread", thread_hop)
        with self.assertRaisesRegex(WorkflowError, "controller event loop"):
            await asyncio.to_thread(operations[1])
        self.assertEqual(self.commits, [])
        self.assertEqual(context._commands, [])

    async def test_stale_decision_cannot_drop_later_staged_children(self):
        for method in ("suspend", "continue_"):
            context = self.context()
            decision = getattr(context, method)(continuation="next", state=None)
            context.task("late", program="echo", version="1", queue="queue", data=None)
            with self.assertRaisesRegex(WorkflowError, "staged child commands"):
                context._validate_decision(decision)
            fresh = getattr(context, method)(continuation="next", state=None)
            self.assertEqual(len(context._validate_decision(fresh)["commands"]), 1)

    async def test_oversized_values_reject_before_large_serialization_allocations(self):
        from ledgence.worker.workflow import _encode
        value = {"blob": "x" * (8 * 1024 * 1024)}
        tracemalloc.start()
        try:
            with self.assertRaises(WorkflowError):
                _encode(value, 128 * 1024)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertLess(peak, 256 * 1024)
        self.assertEqual(_encode({"b": 1.0, "a": [1, -0.0]}, 128), b'{"a":[1,-0.0],"b":1.0}')

    async def test_context_and_decision_limits_and_identity(self):
        for changes in ({"v": True}, {"revision": True}, {"local_steps": [{}]},
                        {"state": "x" * MAX_STATE_BYTES}, {"inputs": []}):
            with self.assertRaises(WorkflowError):
                self.context(payload(**changes))
        context = self.context()
        decision = context.complete(None)
        decision["revision"] = 1
        with self.assertRaises(WorkflowError):
            context._validate_decision(decision)
        with self.assertRaises(WorkflowError):
            workflow_context()


class WorkflowProtocolTests(unittest.TestCase):
    launch = test_bootstrap.BootstrapTests.launch

    def invocation(self, identity="1", activation=None):
        message = test_bootstrap.BootstrapTests.invocation(self, "evt-" + identity, "attempt-" + identity)
        message.update(v=3, processing_context=None)
        frozen = payload() if activation is None else activation
        message["extension"] = {"schema": SCHEMA, "payload": frozen}
        message["event"].update(ldgtaskid=frozen["activation_id"],
                                ldgactivationid=frozen["activation_id"],
                                ldgworkflowid=frozen["workflow_id"])
        return message

    def ordinary(self, message):
        message.pop("extension")
        message["event"].pop("ldgactivationid", None)
        message["event"].pop("ldgworkflowid", None)

    def reply(self, number=1, identity="1", result=None):
        return {"v": 3, "type": "runtime_reply", "event_id": "evt-" + identity,
                "attempt_id": "attempt-" + identity, "id": number,
                "result": {"committed": True} if result is None else result}

    def test_async_workflow_commit_wire_context_cleanup_and_process_reuse(self):
        first, second = self.invocation(), self.invocation("2")
        first["event"].update(ldgworkflowid="workflow-1", ldgactivationid="activation-1")
        self.ordinary(second)
        source = '''import asyncio, os
from ledgence.worker import current_invocation
from ledgence.worker.workflow import workflow_context, WorkflowError
async def local(value):
    await asyncio.sleep(.001)
    return {"value": value}
async def handle(event):
    if event["id"] == "evt-2":
        try: workflow_context()
        except WorkflowError: return {"clean": True, "pid": os.getpid(), "event": event}
        raise AssertionError("workflow context leaked")
    ctx = workflow_context()
    assert current_invocation().workflow_id == ctx.workflow_id
    assert current_invocation().activation_id == ctx.activation_id
    result = await ctx.local("read", local, value=event["data"]["value"])
    return ctx.complete({"local": result, "pid": os.getpid(), "event": event})
'''
        process, frames = self.launch(source, [first, self.reply(), second,
                                              {"v": 3, "type": "shutdown"}], protocol=3)
        self.assertEqual(process.returncode, 0, process.stderr)
        controls = [frame for frame in frames if frame["type"] != "log"]
        self.assertEqual([frame["type"] for frame in controls],
                         ["ready", "runtime_request", "result", "result", "closing"])
        request = controls[1]
        self.assertEqual(request["id"], 1)
        self.assertEqual(request["operation"], "local_step.commit")
        self.assertEqual(request["payload"], {"key": "read", "callable": "program:local",
                                               "input": {"value": 42}, "output": {"value": 42}})
        completed = controls[2]["output"]["output"]
        self.assertEqual(completed["event"], first["event"])
        self.assertEqual(completed["pid"], controls[3]["output"]["pid"])
        self.assertTrue(controls[3]["output"]["clean"])

    def test_mismatched_activation_context_never_runs_controller(self):
        source = "def handle(event): raise AssertionError('must not run')\n"
        for field in ("ldgtaskid", "ldgactivationid", "ldgworkflowid"):
            invocation = self.invocation()
            invocation["event"][field] = "different"
            process, frames = self.launch(source, [invocation], protocol=3)
            self.assertEqual(process.returncode, 0, process.stderr)
            self.assertEqual(frames[-1]["status"], "runtime_error")
            self.assertIn("does not match", frames[-1]["error"]["message"])
            self.assertNotIn(b"must not run", process.stderr)

    def test_preloaded_local_step_does_not_execute_or_emit_commit(self):
        activation = payload(local_steps=[{"key": "read", "callable": "program:local",
                                           "input": {"value": 3}, "output": 42}])
        source = '''from ledgence.worker.workflow import workflow_context
async def local(value): raise AssertionError("committed operation executed again")
async def handle(event):
    ctx = workflow_context()
    return ctx.complete(await ctx.local("read", local, value=3))
'''
        process, frames = self.launch(source, [self.invocation(activation=activation)], protocol=3)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual([f["type"] for f in frames], ["ready", "result"])
        self.assertEqual(frames[-1]["output"]["output"], 42)

    def test_unobserved_local_failure_is_not_a_successful_checkpoint(self):
        source = """from ledgence.worker.workflow import workflow_context
async def fail(): raise ValueError("local failed")
async def handle(event):
    ctx = workflow_context()
    if event["id"] == "evt-1":
        ctx.local("forgotten", fail)
    else:
        try: await ctx.local("handled", fail)
        except ValueError: pass
    return ctx.complete("recovered")
"""
        process, frames = self.launch(source, [self.invocation(), self.invocation("2")], protocol=3)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(frames[1]["status"], "runtime_error")
        self.assertEqual(frames[2]["status"], "success")

    def test_local_callable_cannot_commit_hidden_child_work_before_a_retry(self):
        source = """from ledgence.worker.workflow import workflow_context
async def local():
    workflow_context().task("hidden", program="echo", version="1", queue="queue", data=None)
    return "must never be committed"
async def handle(event):
    ctx = workflow_context()
    await ctx.local("read", local)
    return ctx.complete("incorrect")
"""
        process, frames = self.launch(source, [self.invocation(), self.invocation("2")], protocol=3)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertFalse(any(frame["type"] == "runtime_request" for frame in frames))
        self.assertEqual([frame["status"] for frame in frames if frame["type"] == "result"],
                         ["runtime_error", "runtime_error"])

    def test_unexpected_error_is_runtime_error_but_explicit_fail_is_success(self):
        source = '''from ledgence.worker.workflow import workflow_context
def handle(event):
    if event["id"] == "evt-1": raise ValueError("retry me")
    return workflow_context().fail("rejected", "business outcome")
'''
        process, frames = self.launch(source, [self.invocation(), self.invocation("2")], protocol=3)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(frames[1]["status"], "runtime_error")
        self.assertEqual(frames[2]["status"], "success")
        self.assertEqual(frames[2]["output"]["kind"], "fail")

    def test_mismatched_commit_reply_prevents_checkpoint_success(self):
        source = '''from ledgence.worker.workflow import workflow_context
async def handle(event):
    ctx = workflow_context()
    try: await ctx.local("read", lambda: 3)
    except Exception: pass
    return ctx.complete("must not succeed")
'''
        for changed in ({"id": 2}, {"id": True}, {"attempt_id": "wrong"},
                        {"v": 2}, {"result": {"committed": False}}):
            with self.subTest(changed=changed):
                reply = {**self.reply(), **changed}
                process, frames = self.launch(source, [self.invocation(), reply], protocol=3)
                self.assertEqual(process.returncode, 0, process.stderr)
                self.assertEqual(frames[-1]["status"], "runtime_error")

    def test_ordinary_v3_async_task_preserves_user_output_and_v3_logs(self):
        invocation = self.invocation()
        self.ordinary(invocation)
        source = '''import asyncio
from ledgence.worker import get_logger
async def handle(event):
    get_logger("program").info("v3 log")
    await asyncio.sleep(.03)
    return {"kind": "suspend", "commands": "user-owned", "event": event}
'''
        process, frames = self.launch(source, [invocation], protocol=3)
        self.assertEqual(process.returncode, 0, process.stderr)
        logs = [frame for frame in frames if frame["type"] == "log"]
        self.assertTrue(logs)
        self.assertTrue(all(frame["v"] == 3 for frame in logs))
        result = [frame for frame in frames if frame["type"] == "result"][0]
        self.assertEqual(result["output"]["commands"], "user-owned")
        self.assertEqual(result["output"]["event"], invocation["event"])

    def test_v3_logs_include_workflow_identity_without_changing_v2_records(self):
        source = """import time
from ledgence.worker import get_logger
def handle(event):
    get_logger("program").info("correlated")
    time.sleep(.03)
    return None
"""
        for version in (2, 3):
            invocation = self.invocation()
            self.ordinary(invocation)
            invocation["v"] = version
            invocation["event"].update(ldgworkflowid="workflow-1", ldgactivationid="activation-1")
            process, frames = self.launch(source, [invocation], protocol=version)
            self.assertEqual(process.returncode, 0, process.stderr)
            logs = [frame for frame in frames if frame["type"] == "log"]
            self.assertTrue(logs)
            for frame in logs:
                if version == 3:
                    self.assertEqual(frame["invocation"]["workflow_id"], "workflow-1")
                    self.assertEqual(frame["invocation"]["activation_id"], "activation-1")
                else:
                    self.assertNotIn("workflow_id", frame["invocation"])
                    self.assertNotIn("activation_id", frame["invocation"])

    def test_async_background_tasks_are_drained_before_reuse(self):
        source = '''import asyncio
pending = []
async def background():
    try: await asyncio.sleep(100)
    finally: pending.append("drained")
async def handle(event):
    if event["id"] == "evt-1":
        asyncio.create_task(background())
        await asyncio.sleep(0)
        return "first"
    return pending
'''
        a, b = self.invocation(), self.invocation("2")
        self.ordinary(a)
        self.ordinary(b)
        process, frames = self.launch(source, [a, b], protocol=3)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(frames[-1]["output"], ["drained"])
        self.assertNotIn(b"Task was destroyed", process.stderr)


if __name__ == "__main__":
    unittest.main()
