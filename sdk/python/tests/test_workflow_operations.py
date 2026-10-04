"""Vendor-neutral model/tool calls share the durable local journal (MIT)."""
import asyncio
from enum import StrEnum
import json
import unittest

import test_workflow
import test_workflow_inbound
from ledgence.worker.workflow import (
    MAX_RECORD_BYTES, MAX_STEPS, OperationKind, WorkflowError,
)


class OperationTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflow.WorkflowTests.asyncSetUp
    asyncTearDown = test_workflow.WorkflowTests.asyncTearDown
    context = test_workflow.WorkflowTests.context

    def call(self, context, key, fn, *, kind=OperationKind.MODEL, version="1", arguments=None):
        return context.operation(key, fn, kind=kind, version=version,
                                 arguments={} if arguments is None else arguments)

    async def test_effective_request_and_defaults_are_frozen_before_execution(self):
        seen = []
        default = {"temperature": 0}
        def model(messages, model="demo-2026-01", settings=default, **options):
            seen.append((messages, model, settings, options))
            messages.append("adapter mutation")
            settings["temperature"] = 99
            return {"text": "done"}
        context = self.context()
        supplied = {"messages": ["hello"], "max_tokens": 20}
        result = self.call(context, "turn:0:model", model, arguments=supplied)
        supplied["messages"].append("caller mutation")
        default["temperature"] = 5
        model.__defaults__ = ("changed-model", {"temperature": 10})
        self.assertEqual(await result, {"text": "done"})
        self.assertEqual(seen[0][1], "demo-2026-01")
        record = self.commits[0]
        self.assertEqual(record["callable"], "model:" + model.__module__ + ":" + model.__qualname__)
        self.assertEqual(record["input"], {
            "v": 1, "kind": "model", "version": "1", "arguments": {
                "messages": ["hello"], "model": "demo-2026-01",
                "settings": {"temperature": 0}, "max_tokens": 20,
            },
        })

    async def test_accepted_result_replays_without_execution_or_commit_and_returns_copies(self):
        calls = []
        async def tool(value=3):
            calls.append(value)
            return {"values": [value]}
        context = self.context()
        first = await self.call(context, "tool", tool, kind=OperationKind.TOOL)
        first["values"].append(99)
        resumed = self.context(test_workflow.payload(local_steps=self.commits))
        second = await self.call(resumed, "tool", tool, kind=OperationKind.TOOL,
                                 arguments={"value": 3})
        second["values"].append(42)
        self.assertEqual(await self.call(resumed, "tool", tool, kind=OperationKind.TOOL),
                         {"values": [3]})
        self.assertEqual(calls, [3])
        self.assertEqual(len(self.commits), 1)

    async def test_same_pending_key_and_effective_binding_share_one_execution(self):
        calls = []
        async def model(value=3):
            calls.append(value)
            await asyncio.sleep(0)
            return value
        context = self.context()
        first = self.call(context, "model", model)
        second = self.call(context, "model", model, arguments={"value": 3})
        self.assertEqual(await context.gather(first, second), [3, 3])
        self.assertEqual(calls, [3])
        self.assertEqual(len(self.commits), 1)

    async def test_changed_kind_version_callable_or_effective_arguments_reject_before_execution(self):
        calls = []
        def model(value=3):
            calls.append(value)
            return value
        def other(value=3):
            self.fail("a changed callable executed")
        for accepted in (False, True):
            context = self.context()
            operation = self.call(context, "same", model)
            if accepted:
                await operation
                context = self.context(test_workflow.payload(local_steps=[self.commits[-1]]))
            for fn, changed in ((model, {"kind": OperationKind.TOOL}),
                                (model, {"version": "2"}), (other, {}),
                                (model, {"arguments": {"value": 4}})):
                with self.subTest(accepted=accepted, changed=changed), self.assertRaisesRegex(
                        WorkflowError, "different binding"):
                    self.call(context, "same", fn, **changed)
            await operation
        self.assertEqual(calls, [3, 3])

    async def test_changing_a_default_changes_the_effective_binding(self):
        def model(value=3):
            return value
        context = self.context()
        await self.call(context, "model", model)
        model.__defaults__ = (4,)
        with self.assertRaisesRegex(WorkflowError, "different binding"):
            self.call(context, "model", model)
        self.assertEqual(await self.call(context, "model", model, arguments={"value": 3}), 3)

    async def test_numeric_identity_is_exact_and_object_order_is_irrelevant(self):
        def model(value):
            return value
        for old, changed in ((1, True), (1, 1.0), (-0.0, 0.0)):
            context = self.context()
            await self.call(context, "number", model, arguments={"value": old})
            resumed = self.context(test_workflow.payload(local_steps=[self.commits[-1]]))
            with self.assertRaisesRegex(WorkflowError, "different binding"):
                self.call(resumed, "number", model, arguments={"value": changed})
        context = self.context()
        await self.call(context, "object", model, arguments={"value": {"a": 1, "b": 2}})
        self.assertEqual(await self.call(context, "object", model,
                                        arguments={"value": {"b": 2, "a": 1}}), {"a": 1, "b": 2})

    async def test_rust_size_boundary_operation_replays_but_new_writes_remain_strict(self):
        def model(value):
            self.fail("accepted operation executed again")
        record = test_workflow_inbound.full_array(lambda value: {
            "key": "model", "callable": "model:" + model.__module__ + ":" + model.__qualname__,
            "input": {"v": 1, "kind": "model", "version": "1", "arguments": {"value": value}},
            "output": None}, MAX_RECORD_BYTES)
        context = self.context(test_workflow.payload(local_steps=[record]))
        self.assertIsNone(await self.call(context, "model", model,
                                         arguments=record["input"]["arguments"]))
        with self.assertRaises(WorkflowError):
            self.call(context, "new-model", model, arguments=record["input"]["arguments"])
        self.assertEqual(self.commits, [])

    async def test_plain_local_cannot_alias_an_operation_with_envelope_shaped_arguments(self):
        def adapter(**kwargs):
            return kwargs
        envelope = {"v": 1, "kind": "model", "version": "1", "arguments": {"value": 3}}
        context = self.context()
        await self.call(context, "same", adapter, arguments={"value": 3})
        with self.assertRaisesRegex(WorkflowError, "different binding"):
            context.local("same", adapter, **envelope)
        other = self.context()
        await other.local("same", adapter, **envelope)
        with self.assertRaisesRegex(WorkflowError, "different binding"):
            self.call(other, "same", adapter, arguments={"value": 3})

    async def test_strict_kind_version_key_and_json_argument_validation(self):
        class OtherKind(StrEnum):
            MODEL = "model"
        def model(**kwargs):
            self.fail("invalid input executed")
        context = self.context()
        for kind in ("model", OtherKind.MODEL, None, 1):
            with self.assertRaisesRegex(WorkflowError, "OperationKind"):
                self.call(context, "model", model, kind=kind)
        for version in (None, "", 1, "x" * 129, "bad\n"):
            with self.assertRaises(WorkflowError):
                self.call(context, "model", model, version=version)
        for key in (None, "", 1, "x" * 129, "_approval:fake"):
            with self.assertRaises(WorkflowError):
                self.call(context, key, model)
        for arguments in (None, [], "x", {1: 3}, {"value": object()},
                          {"value": float("nan")}, {"value": 1 << 64},
                          {"value": "x" * MAX_RECORD_BYTES}):
            with self.assertRaises(WorkflowError):
                context.operation("model", model, kind=OperationKind.MODEL,
                                  version="1", arguments=arguments)
        self.assertEqual(self.commits, [])
        self.assertEqual(context._pending, {})

    async def test_signature_binding_and_non_json_defaults_fail_before_scheduling(self):
        def required(value):
            self.fail("invalid signature executed")
        def positional(value, /):
            self.fail("positional-only callable executed")
        def bad_default(value=object()):
            self.fail("non-JSON default executed")
        for fn, arguments in ((required, {}), (required, {"other": 3}),
                              (positional, {"value": 3}), (bad_default, {}), (None, {})):
            context = self.context()
            with self.assertRaises(WorkflowError):
                self.call(context, "model", fn, arguments=arguments)
            self.assertEqual(context._pending, {})

    async def test_async_io_waits_for_durable_acknowledgment(self):
        returned, acknowledge = asyncio.Event(), asyncio.Event()
        async def model():
            returned.set()
            return 3
        async def rpc(operation, record):
            self.assertEqual(operation, "local_step.commit")
            await acknowledge.wait()
            self.commits.append(record)
            return {"committed": True}
        context = self.context(rpc=rpc)
        result = asyncio.ensure_future(self.call(context, "model", model))
        await asyncio.wait_for(returned.wait(), 1)
        self.assertFalse(result.done())
        acknowledge.set()
        self.assertEqual(await asyncio.wait_for(result, 1), 3)

    async def test_uncertain_acknowledgment_cannot_be_swallowed_and_retry_replays_accepted_record(self):
        calls = []
        def model():
            calls.append(True)
            return 3
        async def rpc(operation, record):
            self.commits.append(json.loads(json.dumps(record)))
            raise ConnectionError("reply lost after commit")
        context = self.context(rpc=rpc)
        with self.assertRaisesRegex(ConnectionError, "reply lost"):
            await self.call(context, "model", model)
        with self.assertRaisesRegex(WorkflowError, "acknowledgement"):
            context.complete("must not succeed")
        resumed = self.context(test_workflow.payload(local_steps=self.commits))
        self.assertEqual(await self.call(resumed, "model", model), 3)
        self.assertEqual(calls, [True])

    async def test_bad_acknowledgment_is_fatal(self):
        for reply in (None, {"committed": False}, {"committed": 1},
                      {"committed": True, "extra": True}):
            async def rpc(operation, record):
                return reply
            context = self.context(rpc=rpc)
            with self.assertRaisesRegex(WorkflowError, "acknowledgement"):
                await self.call(context, "model", lambda: 3)
            with self.assertRaisesRegex(WorkflowError, "acknowledgement"):
                context.continue_(continuation="next", state=None)

    async def test_effect_before_unaccepted_commit_may_repeat_and_application_idempotency_reconciles(self):
        invocations, effects = [], {}
        def tool(effect_key):
            invocations.append(effect_key)
            return effects.setdefault(effect_key, {"effect": len(effects) + 1})
        async def unaccepted(operation, record):
            raise ConnectionError("no accepted record")
        context = self.context(rpc=unaccepted)
        with self.assertRaises(ConnectionError):
            await self.call(context, "tool", tool, kind=OperationKind.TOOL,
                            arguments={"effect_key": "workflow:refund:0"})
        retried = self.context()
        result = await self.call(retried, "tool", tool, kind=OperationKind.TOOL,
                                 arguments={"effect_key": "workflow:refund:0"})
        self.assertEqual(result, {"effect": 1})
        self.assertEqual(len(invocations), 2)
        self.assertEqual(len(effects), 1)

    async def test_cancelled_observer_retains_operation_until_acknowledged(self):
        started, release = asyncio.Event(), asyncio.Event()
        async def model():
            started.set()
            await release.wait()
            return 3
        context = self.context()
        observer = asyncio.ensure_future(self.call(context, "model", model))
        await asyncio.wait_for(started.wait(), 1)
        observer.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await observer
        drain = asyncio.create_task(context._drain())
        await asyncio.sleep(0)
        self.assertFalse(drain.done())
        release.set()
        await asyncio.wait_for(drain, 1)
        self.assertEqual(len(self.commits), 1)

    async def test_nested_operation_and_workflow_controls_are_rejected(self):
        for nested in (lambda ctx: self.call(ctx, "nested", lambda: None),
                       lambda ctx: ctx.local("nested", lambda: None),
                       lambda ctx: ctx.complete(None),
                       lambda ctx: ctx.continue_(continuation="next", state=None)):
            context = self.context()
            async def model():
                return await asyncio.to_thread(nested, context)
            with self.assertRaisesRegex(WorkflowError, "use the controller"):
                await self.call(context, "model", model)
        self.assertEqual(self.commits, [])

    async def test_failures_and_partial_streams_are_not_committed_results(self):
        def failure():
            raise RuntimeError("provider failed")
        def stream():
            yield "partial"
        async def async_stream():
            yield "partial"
        for fn, error in ((failure, RuntimeError), (stream, WorkflowError),
                          (async_stream, WorkflowError), (lambda: object(), WorkflowError),
                          (lambda: "x" * MAX_RECORD_BYTES, WorkflowError)):
            context = self.context()
            with self.assertRaises(error):
                await self.call(context, "model", fn)
        self.assertEqual(self.commits, [])

    async def test_unobserved_failure_and_cancelled_owned_operation_fail_drain(self):
        def failure():
            raise RuntimeError("forgotten model failure")
        context = self.context()
        self.call(context, "model", failure)
        with self.assertRaisesRegex(RuntimeError, "forgotten model failure"):
            await context._drain()
        blocked = self.context()
        self.call(blocked, "model", lambda: None)
        blocked._pending["model"][1].cancel()
        with self.assertRaisesRegex(WorkflowError, "cancelled"):
            await blocked._drain()
        self.assertEqual(self.commits, [])

    async def test_journal_count_and_envelope_depth_are_checked_before_execution(self):
        context = self.context()
        for index in range(MAX_STEPS):
            self.call(context, str(index), lambda: None)
        with self.assertRaisesRegex(WorkflowError, "too many"):
            self.call(context, "overflow", lambda: None)
        await context._drain()
        nested = None
        for _ in range(63):
            nested = [nested]
        empty = self.context()
        with self.assertRaises(WorkflowError):
            self.call(empty, "deep", lambda value: self.fail("too-deep input executed"),
                      arguments={"value": nested})
        self.assertEqual(empty._pending, {})

    async def test_checkpoints_start_new_journals_and_carry_explicit_loop_state(self):
        calls = []
        def model(round):
            calls.append(round)
            return {"round": round, "answer": "continue"}
        context = self.context()
        result = await self.call(context, "model", model, arguments={"round": 0})
        decision = context.continue_(continuation="round", state={"round": 1, "previous": result})
        resumed = self.context(test_workflow.payload(
            activation_id="activation-2", revision=1, continuation=decision["continuation"],
            state=decision["state"]))
        self.assertEqual(resumed.state["previous"], result)
        await self.call(resumed, "model", model, arguments={"round": resumed.state["round"]})
        self.assertEqual(calls, [0, 1])


class OperationProtocolTests(unittest.TestCase):
    launch = test_workflow.WorkflowProtocolTests.launch
    invocation = test_workflow.WorkflowProtocolTests.invocation
    reply = test_workflow.WorkflowProtocolTests.reply

    def test_real_bootstrap_commits_semantic_binding_and_replays_it(self):
        source = '''from ledgence.worker.workflow import OperationKind, workflow_context
def model(messages, model="demo-v1", temperature=0):
    return {"text": messages[0], "model": model}
async def handle(event):
    ctx = workflow_context()
    result = await ctx.operation("turn:0:model", model, kind=OperationKind.MODEL,
                                 version="1", arguments={"messages": ["hello"]})
    return ctx.complete(result)
'''
        record = {"key": "turn:0:model", "callable": "model:program:model", "input": {
            "v": 1, "kind": "model", "version": "1", "arguments": {
                "messages": ["hello"], "model": "demo-v1", "temperature": 0}},
            "output": {"text": "hello", "model": "demo-v1"}}
        replay = self.invocation("2", test_workflow.payload(local_steps=[record]))
        process, frames = self.launch(source, [self.invocation(), self.reply(), replay], protocol=3)
        self.assertEqual(process.returncode, 0, process.stderr)
        controls = [frame for frame in frames if frame["type"] != "log"]
        self.assertEqual([frame["type"] for frame in controls],
                         ["ready", "runtime_request", "result", "result"])
        self.assertEqual(controls[1]["operation"], "local_step.commit")
        self.assertEqual(controls[1]["payload"], record)
        self.assertEqual([frame["output"]["output"] for frame in controls if frame["type"] == "result"],
                         [record["output"], record["output"]])


if __name__ == "__main__":
    unittest.main()
