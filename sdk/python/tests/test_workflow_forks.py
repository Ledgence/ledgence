"""Acknowledged owned branch registration and durable joins (MIT)."""
import asyncio
from dataclasses import FrozenInstanceError
from enum import StrEnum
import json
import unittest

import test_workflow
from ledgence.worker.workflow import (
    MAX_FORK_BYTES, BranchSpec, ForkRef, Workflow, WorkflowContext, WorkflowError, _workflow,
)


class Entry(StrEnum):
    START = "start"
    BRANCH = "branch"
    JOIN = "join"


class ForkTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.contexts = []
        self.requests = []

    async def asyncTearDown(self):
        for context in self.contexts:
            await context._finish(cancel=True)

    def context(self, rpc=None):
        async def register(operation, request):
            self.requests.append((operation, request))
            return {"committed": True, "key": request["key"],
                    "branch_keys": [branch["key"] for branch in request["branches"]]}
        context = WorkflowContext(test_workflow.payload(), register if rpc is None else rpc)
        self.contexts.append(context)
        return context

    def branch(self, ctx, key="branch", **changes):
        return ctx.branch(key, **{"entrypoint": "branch", "queue": "queue", "data": {"value": 1}, **changes})

    async def test_branch_is_pure_frozen_and_fork_registers_before_join(self):
        ctx = self.context()
        data = {"value": [1]}
        branch = self.branch(ctx, data=data)
        data["value"].append(2)
        self.assertEqual(self.requests, [])
        self.assertEqual(ctx.complete(None)["kind"], "complete")
        with self.assertRaises(FrozenInstanceError):
            branch._encoded = b"{}"
        fork = await ctx.fork("fork:0", branches=[branch])
        self.assertIs(type(fork), ForkRef)
        self.assertEqual(fork.branch_keys, ("branch",))
        with self.assertRaises(FrozenInstanceError):
            fork.branch_keys = ()
        operation, request = self.requests[0]
        self.assertEqual(operation, "workflow.fork")
        self.assertEqual(request, {"key": "fork:0", "branches": [{"key": "branch",
            "entrypoint": "branch", "queue": "queue", "data": {"value": [1]},
            "retry_policy": {"max_attempts": 3, "retry_delay_ms": 5000},
            "attempt_timeout_ms": 300000}]})
        decision = ctx.join(fork, resume="join", state={"local": 9})
        self.assertEqual(decision["kind"], "suspend")
        self.assertEqual(decision["until"], ["branch"])
        self.assertEqual(decision["commands"], [])
        self.assertEqual(ctx._validate_decision(decision), decision)

    async def test_registration_ack_is_awaited_without_waiting_for_child_completion(self):
        started, acknowledge = asyncio.Event(), asyncio.Event()
        async def register(operation, request):
            started.set()
            await acknowledge.wait()
            return {"committed": True, "key": "fork", "branch_keys": ["branch"]}
        ctx = self.context(register)
        pending = asyncio.create_task(ctx.fork("fork", branches=[self.branch(ctx)]))
        await started.wait()
        self.assertFalse(pending.done())
        acknowledge.set()
        fork = await asyncio.wait_for(pending, 1)
        self.assertEqual(fork.branch_keys, ("branch",))
        self.assertEqual(ctx.inputs, {})

    async def test_repeated_binding_shares_registration_and_changed_binding_rejects(self):
        ctx = self.context()
        first, second = await asyncio.gather(
            ctx.fork("fork", branches=[self.branch(ctx)]),
            ctx.fork("fork", branches=[self.branch(ctx)]))
        self.assertIs(first, second)
        self.assertEqual(len(self.requests), 1)
        with self.assertRaisesRegex(WorkflowError, "different binding"):
            await ctx.fork("fork", branches=[self.branch(ctx, data={"value": 2})])
        with self.assertRaisesRegex(WorkflowError, "already belongs"):
            await ctx.fork("other", branches=[self.branch(ctx)])
        with self.assertRaisesRegex(WorkflowError, "already belongs"):
            ctx.workflow("branch", program="program", version="1", queue="queue", data=None)
        with self.assertRaisesRegex(WorkflowError, "another activation"):
            self.context().join(first, resume="join", state=None)
        with self.assertRaisesRegex(WorkflowError, "not acknowledged"):
            ctx.join(ForkRef("fork", first.branch_keys, ctx), resume="join", state=None)

    async def test_staged_children_and_cross_activation_specs_reject_before_rpc(self):
        ctx = self.context()
        ctx.task("branch", program="program", version="1", queue="q", data=None)
        with self.assertRaisesRegex(WorkflowError, "staged child"):
            await ctx.fork("fork", branches=[self.branch(ctx)])
        other = self.context()
        with self.assertRaisesRegex(WorkflowError, "another activation"):
            await ctx.fork("fork", branches=[self.branch(other)])
        self.assertEqual(self.requests, [])

    async def test_malformed_receipts_and_transport_failures_poison_activation(self):
        good = {"committed": True, "key": "fork", "branch_keys": ["one", "two"]}
        invalid = [None, [], {**good, "committed": 1}, {**good, "key": "other"},
                   {**good, "branch_keys": ["two", "one"]}, {**good, "branch_keys": ["one"]},
                   {**good, "branch_keys": ["one", 2]}, {**good, "extra": True},
                   {**good, "branch_keys": ("one", "two")}, RuntimeError("lost acknowledgement")]
        for receipt in invalid:
            async def rpc(operation, request):
                if isinstance(receipt, Exception):
                    raise receipt
                return receipt
            ctx = self.context(rpc)
            with self.assertRaises((WorkflowError, RuntimeError)):
                await ctx.fork("fork", branches=[self.branch(ctx, "one"), self.branch(ctx, "two")])
            with self.assertRaisesRegex(WorkflowError, "acknowledgement failed"):
                ctx.complete("cannot hide uncertainty")

    async def test_cancelled_observer_retains_registration_until_drain(self):
        started, acknowledge = asyncio.Event(), asyncio.Event()
        async def rpc(operation, request):
            started.set()
            await acknowledge.wait()
            return {"committed": True, "key": "fork", "branch_keys": ["branch"]}
        ctx = self.context(rpc)
        observer = asyncio.create_task(ctx.fork("fork", branches=[self.branch(ctx)]))
        await started.wait()
        observer.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await observer
        drain = asyncio.create_task(ctx._drain())
        await asyncio.sleep(0)
        self.assertFalse(drain.done())
        acknowledge.set()
        await asyncio.wait_for(drain, 1)
        self.assertIsNotNone(await ctx.fork("fork", branches=[self.branch(ctx)]))

    async def test_fork_count_and_request_bounds_reject_before_network(self):
        ctx = self.context()
        for branches in ([], [self.branch(ctx)] * 2,
                         [self.branch(ctx, str(i)) for i in range(65)], [None]):
            with self.assertRaises(WorkflowError):
                await ctx.fork("fork", branches=branches)
        large = self.branch(ctx, data="x" * (MAX_FORK_BYTES // 2))
        large2 = self.branch(ctx, "second", data="x" * (MAX_FORK_BYTES // 2))
        with self.assertRaises(WorkflowError):
            await ctx.fork("large", branches=[large, large2])
        with self.assertRaises(WorkflowError):
            self.branch(ctx, data="x" * MAX_FORK_BYTES)
        self.assertEqual(self.requests, [])
        fork = await ctx.fork("full", branches=[self.branch(ctx, str(i)) for i in range(64)])
        self.assertEqual(len(fork.branch_keys), 64)

    async def test_branch_policy_bounds_and_registered_enum_family(self):
        ctx = self.context()
        for changes in ({"retry_policy": {"max_attempts": True, "retry_delay_ms": 0}},
                        {"attempt_timeout_ms": 1}, {"queue": ""}, {"entrypoint": ""}):
            with self.assertRaises(WorkflowError):
                self.branch(ctx, **changes)
        workflow = Workflow(Entry)
        for entry in Entry:
            workflow.entrypoint(entry, default=entry is Entry.START)(lambda event, ctx: ctx.complete(None))
        token = _workflow.set(ctx)
        try:
            await workflow.build()(None)
        finally:
            _workflow.reset(token)
        class Other(StrEnum):
            BRANCH = "branch"
            JOIN = "join"
        with self.assertRaisesRegex(WorkflowError, "enum family"):
            self.branch(ctx, entrypoint=Other.BRANCH)
        fork = await ctx.fork("fork", branches=[self.branch(ctx, entrypoint=Entry.BRANCH)])
        self.assertEqual(ctx.join(fork, resume=Entry.JOIN, state=None)["continuation"], "join")
        with self.assertRaisesRegex(WorkflowError, "enum family"):
            ctx.join(fork, resume=Other.JOIN, state=None)

    async def test_forged_spec_validation_and_failure_stop_queued_registrations(self):
        ctx = self.context()
        valid = json.loads(self.branch(ctx)._encoded)
        for value in ({}, {**valid, "entrypoint": ""}, {**valid, "attempt_timeout_ms": True}):
            with self.assertRaises(WorkflowError):
                await ctx.fork("bad", branches=[BranchSpec(json.dumps(value).encode(), ctx)])
        with self.assertRaises(WorkflowError):
            await ctx.fork("bad", branches=[BranchSpec(b"invalid-json", ctx)])
        self.assertEqual(self.requests, [])
        started, release = asyncio.Event(), asyncio.Event()
        calls = []
        async def lost(operation, request):
            calls.append(request["key"])
            started.set()
            await release.wait()
            raise RuntimeError("lost acknowledgement")
        failing = self.context(lost)
        first = asyncio.create_task(failing.fork("first", branches=[self.branch(failing, "a")]))
        await started.wait()
        second = asyncio.create_task(failing.fork("second", branches=[self.branch(failing, "b")]))
        await asyncio.sleep(0)
        release.set()
        results = await asyncio.gather(first, second, return_exceptions=True)
        self.assertTrue(all(isinstance(result, Exception) for result in results))
        self.assertEqual(calls, ["first"])

    async def test_branch_fork_and_join_are_forbidden_inside_durable_local(self):
        ctx = self.context()
        branch = self.branch(ctx)
        fork = await ctx.fork("fork", branches=[branch])
        for key, operation in (("branch", lambda: self.branch(ctx, "other")),
                               ("fork", lambda: ctx.fork("other", branches=[branch])),
                               ("join", lambda: ctx.join(fork, resume="join", state=None))):
            with self.assertRaisesRegex(WorkflowError, "use the controller"):
                await ctx.local(key, operation)
        self.assertEqual(len(self.requests), 1)


class ForkProtocolTests(unittest.TestCase):
    launch = test_workflow.WorkflowProtocolTests.launch
    invocation = test_workflow.WorkflowProtocolTests.invocation
    reply = test_workflow.WorkflowProtocolTests.reply

    def test_fork_then_local_work_then_checkpoint_through_real_bootstrap(self):
        source = '''from enum import StrEnum
from ledgence.worker.workflow import Workflow
class Entry(StrEnum):
    MAIN = "main"
    BRANCH = "branch"
    JOIN = "join"
workflow = Workflow(Entry)
def summarize(value): return value + 1
@workflow.entrypoint(Entry.MAIN, default=True)
async def main(event, ctx):
    fork = await ctx.fork("pair", branches=[
        ctx.branch("a", entrypoint=Entry.BRANCH, queue="q", data={"value": 1}),
        ctx.branch("b", entrypoint=Entry.BRANCH, queue="q", data={"value": 2})])
    result = await ctx.local("summary", summarize, value=event["data"]["value"])
    return ctx.join(fork, resume=Entry.JOIN, state={"summary": result})
@workflow.entrypoint(Entry.BRANCH)
def branch(event, ctx): raise AssertionError("branch must execute in its own workflow")
@workflow.entrypoint(Entry.JOIN)
def join(event, ctx): return ctx.complete(ctx.state)
handle = workflow.build()
'''
        receipt = {"committed": True, "key": "pair", "branch_keys": ["a", "b"]}
        messages = [self.invocation(), self.reply(result=receipt), self.reply(number=2)]
        process, frames = self.launch(source, messages, protocol=3)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual([frame["type"] for frame in frames], ["ready", "runtime_request", "runtime_request", "result"])
        self.assertEqual([frame["operation"] for frame in frames if frame["type"] == "runtime_request"],
                         ["workflow.fork", "local_step.commit"])
        self.assertEqual(frames[-1]["status"], "success")
        self.assertEqual(frames[-1]["output"]["until"], ["a", "b"])
        self.assertEqual(frames[-1]["output"]["state"], {"summary": 43})

    def test_public_example_routes_parent_and_independent_child_checkpoints(self):
        from pathlib import Path
        source = (Path(__file__).resolve().parents[3] / "examples/mixed-workflow/program.py").read_text()
        first = self.invocation()
        first["event"]["data"] = {"values": [1, 2, 3], "queue": "workflows"}
        receipt = {"committed": True, "key": "calculations:0", "branch_keys": ["double:0", "triple:0"]}
        double = self.invocation("2", test_workflow.payload(continuation="double"))
        double["event"]["data"] = {"values": [1, 2, 3]}
        ready = self.invocation("3", test_workflow.payload(continuation="double_ready", revision=1, state={"sum": 6}))
        triple = self.invocation("4", test_workflow.payload(continuation="triple"))
        triple["event"]["data"] = {"values": [1, 2, 3]}
        def result(value):
            return {"kind": "workflow", "workflow_id": "child" + str(value), "state": "succeeded",
                    "outcome": {"kind": "succeeded", "output": value}}
        collect = self.invocation("5", test_workflow.payload(continuation="collect", revision=1,
            state={"local": {"count": 3, "sum": 6}}, inputs={"double:0": result(12), "triple:0": result(18)}))
        messages = [first, self.reply(result=receipt), self.reply(number=2), double, ready, triple, collect]
        process, frames = self.launch(source, messages, protocol=3)
        self.assertEqual(process.returncode, 0, process.stderr)
        results = [frame for frame in frames if frame["type"] == "result"]
        self.assertEqual([frame["status"] for frame in results], ["success"] * 5)
        self.assertEqual(results[0]["output"]["kind"], "suspend")
        self.assertEqual(results[1]["output"]["kind"], "wait")
        self.assertEqual(results[2]["output"]["output"], 12)
        self.assertEqual(results[3]["output"]["output"], 18)
        self.assertEqual(results[4]["output"]["output"],
                         {"local": {"count": 3, "sum": 6}, "double": 12, "triple": 18})

    def test_uncertain_fork_cannot_be_caught_and_completed_in_bootstrap(self):
        source = '''from ledgence.worker.workflow import workflow_context
async def handle(event):
    ctx = workflow_context()
    try:
        await ctx.fork("fork", branches=[ctx.branch("branch", entrypoint="branch", queue="q", data=None)])
    except Exception: pass
    return ctx.complete("invalid")
'''
        process, frames = self.launch(source, [self.invocation(), self.reply(result={"committed": True})], protocol=3)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(frames[-1]["status"], "runtime_error")
