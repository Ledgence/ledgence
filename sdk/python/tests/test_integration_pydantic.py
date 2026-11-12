"""Offline actual-SDK qualification for pydantic-ai-slim 2.66.0 (MIT)."""
import asyncio
from dataclasses import replace
from importlib import metadata
import json
import socket
import subprocess
import unittest
from unittest.mock import patch

import test_workflow
from ledgence.worker.workflow import WorkflowContext
from ledgence.worker.integrations import (
    AgentRunSpec, AgentLimits, AgentStatus, AgentProfileError, AgentBindingError,
    AgentExecutionError, AgentLimitError, UsageOrigin,
)
from ledgence.worker.integrations import pydantic_ai as adapter

try:
    INSTALLED = metadata.version("pydantic-ai-slim") == "2.66.0"
except metadata.PackageNotFoundError:
    INSTALLED = False
if INSTALLED:
    import anyio
    from pydantic_ai import Agent
    from pydantic_ai.capabilities import AbstractCapability
    from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart
    from pydantic_ai.models.function import FunctionModel
    from pydantic_ai.usage import RequestUsage
    from pydantic_ai.exceptions import ModelRetry, CallDeferred, ApprovalRequired

    class Transform(AbstractCapability):
        def __init__(self, delta): self.delta = delta
        async def before_tool_execute(self, ctx, *, call, tool_def, args):
            return {**args, "x": args["x"] + self.delta}
        async def after_tool_execute(self, ctx, *, call, tool_def, args, result):
            return {"wrapped": result}

SPEC = AgentRunSpec("pydantic-read", "1", {"policy": "read"})
SCHEMA = {"type": "object", "properties": {"x": {"type": "integer"}},
          "required": ["x"], "additionalProperties": False}


@unittest.skipUnless(INSTALLED, "optional exact pydantic-ai-slim 2.66.0 fixture environment")
class PydanticIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.contexts, self.commits, self.calls, self.blocked = [], [], [], []
        def forbidden(*args, **kwargs):
            self.blocked.append(True)
            raise AssertionError("network/process access forbidden in offline SDK test")
        self.guards = [patch.object(socket.socket, "connect", forbidden),
                       patch.object(socket.socket, "connect_ex", forbidden),
                       patch.object(socket, "getaddrinfo", forbidden),
                       patch.object(subprocess, "Popen", forbidden)]
        for guard in self.guards: guard.start()
    async def asyncTearDown(self):
        try:
            for context in self.contexts:
                await context._finish(cancel=True)
            self.assertEqual(self.blocked, [])
        finally:
            for guard in reversed(self.guards): guard.stop()
    def context(self, records=(), rpc=None):
        async def commit(operation, record):
            self.assertEqual(operation, "local_step.commit")
            self.commits.append(json.loads(json.dumps(record)))
            return {"committed": True}
        ctx = WorkflowContext(test_workflow.payload(local_steps=list(records)), rpc or commit)
        self.contexts.append(ctx)
        return ctx
    def run_agent(self, factory, *, records=(), spec=SPEC, prompt="hello", context=None):
        return adapter.run(context or self.context(records), spec=spec, prompt=prompt, factory=factory)
    def factory(self, *, delta=0, turns=1, duplicate=False, control=None, catalog=False,
                model_override=None, read_version="1", instructions=None):
        async def echo(x: int = 2) -> dict:
            self.calls.append(("tool", x))
            if control is not None: raise control("retry")
            await asyncio.sleep(0)
            return {"x": x, "timestamp": "user-owned"}
        async def execute(*, target, arguments):
            self.calls.append(("catalog-tool", target, arguments))
            return {"x": arguments["x"], "target": target}
        async def discover(*, configuration):
            self.calls.append(("discover", configuration))
            return [{"id": "echo", "name": "echo", "version": "1", "input_schema": SCHEMA,
                     "executor_id": "echo-v1", "target": {"index": "public"}}]
        async def model(messages, info):
            self.calls.append(("model", len([m for m in messages if isinstance(m, ModelResponse)])))
            previous = len([m for m in messages if isinstance(m, ModelResponse)])
            if previous < turns:
                return ModelResponse(parts=[ToolCallPart("echo", {"x": 1}, tool_call_id="a"),
                                            ToolCallPart("echo", {"x": 1}, tool_call_id="a" if duplicate else "b")],
                                     usage=RequestUsage(input_tokens=5, output_tokens=2))
            return ModelResponse(parts=[TextPart("done")], usage=RequestUsage(input_tokens=7, output_tokens=1))
        def factory(bindings):
            capabilities = [bindings.durability]
            if delta: capabilities.append(bindings.transform("add-x", Transform(delta), version="1"))
            if catalog:
                return Agent(model_override or FunctionModel(model, model_name="fixture"), name="reader", instructions=instructions,
                             toolsets=[bindings.catalog("lookup", version="1", discover=discover,
                                                        registry={"echo-v1": execute})], capabilities=capabilities)
            return Agent(model_override or FunctionModel(model, model_name="fixture"), name="reader", instructions=instructions,
                         tools=[bindings.read_tool("echo-read", echo, version=read_version)], capabilities=capabilities)
        return factory

    async def test_full_replay_immutable_causal_positions_usage_and_repeated_ids(self):
        factory = self.factory(turns=2)
        first = await self.run_agent(factory)
        self.assertEqual(first.output, "done")
        self.assertEqual(first.status, AgentStatus.SUCCEEDED)
        counts = list(self.calls)
        self.assertEqual(len([c for c in counts if c[0] == "tool"]), 4)
        self.assertEqual(len(first.usage), 3)
        self.assertEqual([u.input_tokens for u in first.usage], [5, 5, 7])
        self.assertTrue(first.lifecycle.transport_closed)
        records = list(self.commits)
        # Mutating the exposed native object cannot change the journal authority.
        first.native.all_messages()[1].parts.clear()
        second = await self.run_agent(factory, records=records)
        self.assertEqual(second.output, "done")
        self.assertEqual(self.calls, counts)
        self.assertTrue(all(u.origin is UsageOrigin.ACCEPTED_REPLAY for u in second.usage))
        self.assertEqual(len({r["key"] for r in records}), len(records))

    async def test_partial_prefix_recovery_only_dispatches_missing_suffix(self):
        factory = self.factory(turns=2)
        await self.run_agent(factory)
        records = list(self.commits)
        self.calls.clear()
        recovered = await self.run_agent(factory, records=records[:3])
        self.assertEqual(recovered.output, "done")
        self.assertEqual(len([c for c in self.calls if c[0] == "tool"]), 3)
        self.assertEqual(len([c for c in self.calls if c[0] == "model"]), 2)
        self.assertEqual([u.origin for u in recovered.usage],
                         [UsageOrigin.ACCEPTED_REPLAY, UsageOrigin.CURRENT_DISPATCH, UsageOrigin.CURRENT_DISPATCH])

    async def test_effective_transforms_bind_and_replay_without_repeat(self):
        await self.run_agent(self.factory(delta=1))
        self.assertEqual([c[1] for c in self.calls if c[0] == "tool"], [2, 2])
        records, calls = list(self.commits), list(self.calls)
        await self.run_agent(self.factory(delta=1), records=records)
        self.assertEqual(self.calls, calls)
        with self.assertRaises(AgentBindingError):
            await self.run_agent(self.factory(delta=3), records=records)
        self.assertEqual(self.calls, calls)

    async def test_duplicate_ids_rejected_before_any_tool_dispatch(self):
        with self.assertRaises(AgentProfileError):
            await self.run_agent(self.factory(duplicate=True))
        self.assertFalse(any(c[0] == "tool" for c in self.calls))
        self.assertEqual(len(self.commits), 1)

    async def test_dynamic_catalog_snapshot_replay_and_effective_schema(self):
        factory = self.factory(catalog=True, turns=2)
        result = await self.run_agent(factory)
        self.assertEqual(result.output, "done")
        calls, records = list(self.calls), list(self.commits)
        self.assertEqual(len([c for c in calls if c[0] == "catalog-tool"]), 4)
        self.assertEqual(len([c for c in calls if c[0] == "discover"]), 3)
        await self.run_agent(factory, records=records)
        self.assertEqual(self.calls, calls)

    async def test_model_retry_native_codec_replays_control_result(self):
        factory = self.factory(control=ModelRetry)
        await self.run_agent(factory)
        calls, records = list(self.calls), list(self.commits)
        await self.run_agent(factory, records=records)
        self.assertEqual(self.calls, calls)

    async def test_native_deferred_controls_poison_before_sdk_boxes_them(self):
        for control in (CallDeferred, ApprovalRequired):
            with self.subTest(control=control):
                with self.assertRaises(AgentProfileError):
                    await self.run_agent(self.factory(control=control))

    async def test_model_turn_limit_prevents_next_dispatch(self):
        with self.assertRaises(AgentLimitError):
            await self.run_agent(self.factory(turns=2), spec=replace(SPEC, limits=AgentLimits(max_model_turns=1)))
        self.assertEqual(len([c for c in self.calls if c[0] == "model"]), 1)

    async def test_native_unregistered_tools_rejected_before_model(self):
        async def tool(x: int): self.fail("unregistered tool ran")
        async def model(messages, info): self.fail("model ran")
        def factory(bindings):
            return Agent(FunctionModel(model), name="bad", tools=[tool], capabilities=[bindings.durability])
        with self.assertRaises(AgentProfileError): await self.run_agent(factory)

    async def test_owner_context_affinity_and_drain_before_close(self):
        entered, released = asyncio.Event(), asyncio.Event()
        events = []
        async def model(messages, info):
            events.append("request")
            entered.set()
            await released.wait()
            events.append("request-ended")
            return ModelResponse(parts=[TextPart("done")])
        class AffineModel(FunctionModel):
            async def __aenter__(self):
                self.owner = asyncio.current_task()
                self.scope = anyio.CancelScope()
                self.scope.__enter__()
                events.append("open")
                return self
            async def __aexit__(self, *args):
                self_test.assertIs(asyncio.current_task(), self.owner)
                events.append("close")
                self.scope.__exit__(*args)
        self_test = self
        ctx = self.context()
        handle = self.run_agent(self.factory(model_override=AffineModel(model)), context=ctx)
        async def observe(): return await handle
        observer = asyncio.create_task(observe())
        await asyncio.wait_for(entered.wait(), 2)
        observer.cancel()
        with self.assertRaises(asyncio.CancelledError): await observer
        await asyncio.sleep(0)
        self.assertNotIn("close", events)
        released.set()
        await ctx._finish(cancel=True)
        self.assertEqual(events, ["open", "request", "request-ended", "close"])

    async def test_failed_native_slot_poison_and_retained_handle(self):
        entered = []
        async def model(messages, info):
            entered.append(True)
            raise RuntimeError("ordinary provider failure must not retry")
        handle = self.run_agent(self.factory(model_override=FunctionModel(model)))
        with self.assertRaises(AgentExecutionError): await handle
        with self.assertRaises(AgentExecutionError): await handle
        self.assertEqual(entered, [True])
        self.assertEqual(len(self.commits), 1)

    async def test_unknown_usage_remains_unknown_and_replays(self):
        from ledgence.worker.integrations import UsageCoverage
        async def model(messages, info):
            return ModelResponse(parts=[TextPart("done")], usage=RequestUsage(unmeasured_requests=1))
        factory = self.factory(model_override=FunctionModel(model))
        result = await self.run_agent(factory)
        self.assertIs(result.usage[0].coverage, UsageCoverage.UNKNOWN)
        self.assertIsNone(result.usage[0].input_tokens)
        again = await self.run_agent(factory, records=list(self.commits))
        self.assertIs(again.usage[0].coverage, UsageCoverage.UNKNOWN)
        self.assertIsNone(again.usage[0].total_tokens)

    async def test_effective_normalized_request_keeps_user_json_timestamps(self):
        from pydantic_ai.messages import ModelRequest, ToolReturnPart
        seen = []
        async def model(messages, info):
            seen.extend(messages)
            if not any(isinstance(m, ModelResponse) for m in messages):
                return ModelResponse(parts=[ToolCallPart("echo", {"x": 3}, tool_call_id="timestamp")])
            return ModelResponse(parts=[TextPart("done")])
        factory = self.factory(model_override=FunctionModel(model, model_name="timestamps"))
        await self.run_agent(factory)
        requests = [m for m in seen if isinstance(m, ModelRequest)]
        self.assertEqual(len({m.timestamp for m in requests}), 1)
        tool_returns = [p for m in requests for p in m.parts if isinstance(p, ToolReturnPart)]
        self.assertEqual(tool_returns[0].content["timestamp"], "user-owned")
        records, count = list(self.commits), len(seen)
        await self.run_agent(factory, records=records)
        self.assertEqual(len(seen), count)

    async def test_native_validation_retry_is_reconstructed_without_tool_call(self):
        async def model(messages, info):
            self.calls.append(("model",))
            if not any(isinstance(m, ModelResponse) for m in messages):
                return ModelResponse(parts=[ToolCallPart("echo", {"x": "invalid-integer"}, tool_call_id="invalid")])
            return ModelResponse(parts=[TextPart("corrected")])
        factory = self.factory(model_override=FunctionModel(model, model_name="validation"))
        self.assertEqual((await self.run_agent(factory)).output, "corrected")
        self.assertFalse(any(c[0] == "tool" for c in self.calls))
        calls, records = list(self.calls), list(self.commits)
        await self.run_agent(factory, records=records)
        self.assertEqual(self.calls, calls)

    async def test_missing_default_is_bound_and_invalid_transformed_argument_rejected(self):
        async def model(messages, info):
            if not any(isinstance(m, ModelResponse) for m in messages):
                return ModelResponse(parts=[ToolCallPart("echo", {}, tool_call_id="default")])
            return ModelResponse(parts=[TextPart("done")])
        factory = self.factory(model_override=FunctionModel(model, model_name="defaults"))
        await self.run_agent(factory)
        self.assertEqual([c[1] for c in self.calls if c[0] == "tool"], [2])
        class InvalidTransform(AbstractCapability):
            async def before_tool_execute(self, ctx, *, call, tool_def, args):
                return {"x": True}
        original = self.factory()
        def invalid(bindings):
            agent = original(bindings)
            # Rebuild the normal factory with its registered tool and our declared transform.
            return Agent(agent.model, name="reader", tools=list(agent.toolsets[0].tools.values()),
                         capabilities=[bindings.durability,
                                       bindings.transform("invalid", InvalidTransform(), version="1")])
        before = len(self.calls)
        with self.assertRaises(AgentProfileError): await self.run_agent(invalid)
        self.assertEqual(len(self.calls), before + 1)

    async def test_profile_rejects_unregistered_capabilities_dynamic_toolsets_and_fallbacks(self):
        from pydantic_ai.toolsets import DynamicToolset
        from pydantic_ai.models.fallback import FallbackModel
        async def model(messages, info): self.fail("unsupported profile model dispatched")
        class UnsafeCapability(AbstractCapability):
            async def before_run(self, ctx): self.fail("unsupported hook ran")
        def dynamic(ctx): self.fail("unsupported discovery ran")
        for case in ("capability", "dynamic", "fallback", "missing-durability"):
            with self.subTest(case=case):
                def factory(bindings):
                    native = FunctionModel(model)
                    return Agent(FallbackModel(native, native) if case == "fallback" else native,
                                 name="bad", toolsets=[DynamicToolset(dynamic, id="bad")] if case == "dynamic" else [],
                                 capabilities=([bindings.durability] if case != "missing-durability" else [])
                                 + ([UnsafeCapability()] if case == "capability" else []))
                with self.assertRaises(AgentProfileError): await self.run_agent(factory)

    async def test_catalog_rejects_unsupported_and_invalid_effective_schemas(self):
        def make(schema, args):
            async def discover(*, configuration):
                return [{"id": "entry", "name": "lookup", "version": "1", "input_schema": schema,
                         "executor_id": "lookup-v1", "target": {}}]
            async def execute(*, target, arguments): self.fail("invalid catalog tool dispatched")
            async def model(messages, info):
                return ModelResponse(parts=[ToolCallPart("lookup", args, tool_call_id="bad-schema")])
            def factory(bindings):
                return Agent(FunctionModel(model), name="schema", capabilities=[bindings.durability],
                             toolsets=[bindings.catalog("lookup", version="1", discover=discover,
                                                        registry={"lookup-v1": execute})])
            return factory
        for schema, args in (({**SCHEMA, "$ref": "#bad"}, {"x": 1}),
                             (SCHEMA, {"x": True}), (SCHEMA, {"x": 1, "extra": 2})):
            with self.subTest(schema=schema, args=args):
                with self.assertRaises(AgentProfileError): await self.run_agent(make(schema, args))

    async def test_tool_and_discovery_limits_stop_admission(self):
        with self.assertRaises(AgentLimitError):
            await self.run_agent(self.factory(), spec=replace(SPEC, limits=AgentLimits(max_tool_calls=1)))
        self.assertLessEqual(len([c for c in self.calls if c[0] == "tool"]), 1)
        self.calls.clear()
        with self.assertRaises(AgentLimitError):
            await self.run_agent(self.factory(catalog=True),
                                 spec=replace(SPEC, limits=AgentLimits(max_discoveries=1)))
        self.assertEqual(len([c for c in self.calls if c[0] == "discover"]), 1)
        self.assertEqual(len([c for c in self.calls if c[0] == "model"]), 1)

    async def test_lost_ack_replays_server_accepted_model_without_dispatch(self):
        async def commit(operation, record):
            self.commits.append(json.loads(json.dumps(record)))
            if len(self.commits) == 2:
                raise RuntimeError("server accepted, acknowledgement lost")
            return {"committed": True}
        factory = self.factory()
        with self.assertRaises(AgentExecutionError):
            await self.run_agent(factory, context=self.context(rpc=commit))
        accepted = list(self.commits)
        self.calls.clear()
        result = await self.run_agent(factory, records=accepted)
        self.assertEqual(result.output, "done")
        self.assertEqual(len([c for c in self.calls if c[0] == "model"]), 1)
        self.assertEqual(len([c for c in self.calls if c[0] == "tool"]), 2)

    async def test_cleanup_failure_prevents_success_and_next_warm_run_is_fresh(self):
        events = []
        async def model(messages, info): return ModelResponse(parts=[TextPart("done")])
        class ClosingModel(FunctionModel):
            async def __aenter__(self):
                self.owner = asyncio.current_task()
                events.append("open")
                return self
            async def __aexit__(self, *args):
                self_test.assertIs(asyncio.current_task(), self.owner)
                events.append("close")
                raise RuntimeError("close failed")
        self_test = self
        with self.assertRaises(AgentExecutionError):
            await self.run_agent(self.factory(model_override=ClosingModel(model)))
        self.assertEqual(events, ["open", "close"])
        fresh = await self.run_agent(self.factory(turns=0))
        self.assertEqual(fresh.output, "done")
        self.assertTrue(fresh.lifecycle.transport_closed)

    async def test_cancel_during_sdk_open_retains_owner_and_starts_no_query(self):
        opening, release = asyncio.Event(), asyncio.Event()
        events = []
        async def model(messages, info): self.fail("query started after stop")
        class OpeningModel(FunctionModel):
            async def __aenter__(self):
                self.owner = asyncio.current_task()
                events.append("opening")
                opening.set()
                await release.wait()
                events.append("opened")
                return self
            async def __aexit__(self, *args):
                self_test.assertIs(asyncio.current_task(), self.owner)
                events.append("closed")
        self_test = self
        ctx = self.context()
        handle = self.run_agent(self.factory(model_override=OpeningModel(model)), context=ctx)
        async def observe(): return await handle
        observer = asyncio.create_task(observe())
        await asyncio.wait_for(opening.wait(), 2)
        observer.cancel()
        with self.assertRaises(asyncio.CancelledError): await observer
        finishing = asyncio.create_task(ctx._finish(cancel=True))
        await asyncio.sleep(0)
        finishing.cancel()
        await asyncio.sleep(0)
        self.assertEqual(events, ["opening"])
        release.set()
        try: await finishing
        except asyncio.CancelledError: pass
        await ctx._finish(cancel=True)
        self.assertEqual(events, ["opening", "opened", "closed"])

    async def test_oversize_native_model_wire_has_uncertainty_and_no_acceptance(self):
        async def model(messages, info):
            self.calls.append(("model",))
            return ModelResponse(parts=[TextPart("x" * 9000)])
        with self.assertRaises(AgentLimitError) as failed:
            await self.run_agent(self.factory(model_override=FunctionModel(model)))
        self.assertTrue(failed.exception.external_work_may_have_occurred)
        self.assertEqual(self.calls, [("model",)])
        self.assertEqual(len(self.commits), 1)

    async def test_registered_tool_identity_drift_rejects_before_callback(self):
        original = self.factory()
        await self.run_agent(original)
        records, calls = list(self.commits), list(self.calls)
        with self.assertRaises(AgentBindingError):
            await self.run_agent(self.factory(read_version="2"), records=records)
        self.assertEqual(self.calls, calls)

    async def test_reported_cost_does_not_forward_sdk_pricing_estimates(self):
        from decimal import Decimal
        async def model(messages, info):
            return ModelResponse(parts=[TextPart("done")],
                                 usage=RequestUsage(input_tokens=1, output_tokens=1, cost=Decimal("0.25")))
        result = await self.run_agent(self.factory(model_override=FunctionModel(model)))
        self.assertIsNone(result.usage[0].reported_cost)
        self.assertIsNone(result.usage[0].currency)

    async def test_static_instructions_are_bound_before_provider_dispatch(self):
        await self.run_agent(self.factory(turns=0, instructions="Read the declared catalog."))
        records, calls = list(self.commits), list(self.calls)
        await self.run_agent(self.factory(turns=0, instructions="Read the declared catalog."), records=records)
        with self.assertRaises(AgentBindingError):
            await self.run_agent(self.factory(turns=0, instructions="Use a different policy."), records=records)
        self.assertEqual(self.calls, calls)

    async def test_catalog_native_schema_conversion_preserves_nested_enum_and_numeric_wire(self):
        schema = {"type": "object", "additionalProperties": False, "required": ["n", "items"],
                  "properties": {"n": {"type": "integer"}, "items": {"type": "array", "items": {
                      "type": "object", "additionalProperties": False, "required": ["kind", "ready"],
                      "properties": {"kind": {"type": "string", "enum": ["read", "list"]},
                                     "ready": {"type": "boolean"}}}}}}
        args = {"n": 4.0, "items": [{"kind": "read", "ready": True}]}
        async def discover(*, configuration):
            return [{"id": "nested", "name": "lookup", "version": "1", "input_schema": schema,
                     "executor_id": "nested-v1", "target": {"timestamp": "user-owned"}}]
        async def execute(*, target, arguments):
            self.calls.append((target, arguments))
            self.assertIs(type(arguments["n"]), float)
            return arguments
        async def model(messages, info):
            if not any(isinstance(m, ModelResponse) for m in messages):
                self.assertEqual(info.function_tools[0].parameters_json_schema, schema)
                return ModelResponse(parts=[ToolCallPart("lookup", args, tool_call_id="nested")])
            return ModelResponse(parts=[TextPart("done")])
        def factory(bindings):
            return Agent(FunctionModel(model), name="nested", capabilities=[bindings.durability],
                         toolsets=[bindings.catalog("nested", version="1", discover=discover,
                                                    registry={"nested-v1": execute})])
        await self.run_agent(factory)
        records = list(self.commits)
        await self.run_agent(factory, records=records)
        self.assertEqual(self.calls, [({"timestamp": "user-owned"}, args)])

    async def test_native_tool_timeout_is_rejected_before_execution(self):
        original = self.factory()
        def factory(bindings):
            agent = original(bindings)
            return Agent(agent.model, name="reader", tools=list(agent.toolsets[0].tools.values()),
                         capabilities=[bindings.durability], tool_timeout=0.01)
        with self.assertRaises(AgentProfileError): await self.run_agent(factory)
        self.assertEqual(self.calls, [])

    async def test_deadline_retains_model_until_finished_then_closes(self):
        entered, release = asyncio.Event(), asyncio.Event()
        events = []
        async def model(messages, info):
            events.append("request")
            entered.set()
            await release.wait()
            events.append("returned")
            return ModelResponse(parts=[TextPart("done")])
        class ClosingModel(FunctionModel):
            async def __aexit__(self, *args): events.append("closed")
        handle = self.run_agent(self.factory(model_override=ClosingModel(model)),
                                spec=replace(SPEC, limits=AgentLimits(timeout_ms=50)))
        async def observe(): return await handle
        observer = asyncio.create_task(observe())
        await asyncio.wait_for(entered.wait(), 2)
        await asyncio.sleep(0.08)
        self.assertFalse(observer.done())
        self.assertNotIn("closed", events)
        release.set()
        with self.assertRaises(AgentExecutionError) as failure: await observer
        self.assertEqual(failure.exception.code, "deadline_exceeded")
        self.assertEqual(events, ["request", "returned", "closed"])

    async def test_native_suspended_response_cannot_start_hidden_continuation(self):
        async def model(messages, info):
            self.calls.append(("model",))
            return ModelResponse(parts=[TextPart("unfinished")], state="suspended")
        with self.assertRaises(AgentProfileError):
            await self.run_agent(self.factory(model_override=FunctionModel(model)))
        self.assertEqual(self.calls, [("model",)])
        self.assertEqual(len(self.commits), 1)

    async def test_portable_worker_fixture_replay_and_forbidden_nested_control(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory
        from framework_fixtures import pydantic_program
        with TemporaryDirectory() as directory:
            marker = Path(directory) / "dispatch.jsonl"
            event = {"data": {"marker": str(marker)}}
            with patch.object(pydantic_program, "workflow_context", return_value=self.context()):
                first = await pydantic_program.handle(event)
            rows = marker.read_text().splitlines()
            self.assertEqual(len(rows), 7)
            self.assertEqual(first["output"], "done")
            self.assertTrue(first["transport_closed"])
            with patch.object(pydantic_program, "workflow_context", return_value=self.context(list(self.commits))):
                again = await pydantic_program.handle(event)
            self.assertEqual(marker.read_text().splitlines(), rows)
            self.assertEqual(first["loop_id"], again["loop_id"])
            self.assertTrue(all(item["origin"] == "accepted_replay" for item in again["usage"]))
            nested = {"data": {"marker": str(marker), "nested_control": True}}
            with patch.object(pydantic_program, "workflow_context", return_value=self.context()):
                with self.assertRaises(AgentExecutionError):
                    await pydantic_program.handle(nested)
