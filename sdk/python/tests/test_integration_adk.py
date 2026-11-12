"""Offline qualification with actual google-adk 2.14.0 / google-genai 2.34.0 (MIT)."""
import asyncio
from dataclasses import replace
from importlib import metadata
import json
import socket
import subprocess
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import test_workflow
from ledgence.worker.workflow import WorkflowContext, WorkflowError
from ledgence.worker.integrations import (
    AgentRunSpec, AgentLimits, AgentStatus, AgentProfileError, AgentBindingError,
    AgentExecutionError, AgentLimitError, UsageOrigin,
)
from ledgence.worker.integrations import google_adk as adapter

try:
    INSTALLED = metadata.version("google-adk") == "2.14.0" and metadata.version("google-genai") == "2.34.0"
except metadata.PackageNotFoundError:
    INSTALLED = False
if INSTALLED:
    from google.adk.agents import LlmAgent
    from google.adk.models.base_llm import BaseLlm
    from google.adk.models.llm_response import LlmResponse
    from google.adk.runners import Runner
    from google.genai import types

SPEC = AgentRunSpec("adk-read", "1", {"policy": "read"})
SCHEMA = {"type": "object", "properties": {"x": {"type": "integer"}},
          "required": ["x"], "additionalProperties": False}


@unittest.skipUnless(INSTALLED, "optional exact google-adk 2.14.0 / google-genai 2.34.0 environment")
class AdkIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.contexts, self.commits, self.calls, self.blocked, self.owners = [], [], [], [], []
        def forbidden(*args, **kwargs):
            self.blocked.append(True)
            raise AssertionError("network/process access forbidden in offline SDK test")
        self.guards = [patch.object(socket.socket, "connect", forbidden),
                       patch.object(socket.socket, "connect_ex", forbidden),
                       patch.object(socket, "getaddrinfo", forbidden),
                       patch.object(subprocess, "Popen", forbidden)]
        for guard in self.guards:
            guard.start()
    async def asyncTearDown(self):
        try:
            for context in self.contexts:
                await context._finish(cancel=True)
            self.assertEqual(self.blocked, [])
        finally:
            for guard in reversed(self.guards):
                guard.stop()
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
    def factory(self, *, turns=1, parallel=1, missing_ids=True, before=None, after=None,
                catalog=False, model_override=None, tool_override=None, version="1"):
        test = self
        async def read(x: int = 2) -> dict:
            test.calls.append(("tool", x))
            if tool_override is not None:
                return await tool_override(x)
            await asyncio.sleep(0)
            return {"x": x}
        async def discover(*, configuration):
            test.calls.append(("discover", configuration))
            return [{"id": "read", "name": "read", "version": "1", "input_schema": SCHEMA,
                     "executor_id": "read-v1", "target": {"index": "public"}}]
        async def execute(*, target, arguments):
            test.calls.append(("catalog-tool", target, arguments))
            target["index"] = "mutated-owned-copy"
            return {"x": arguments["x"]}
        class Model(BaseLlm):
            async def generate_content_async(self, llm_request, stream=False):
                previous = len([c for c in llm_request.contents if c.role == "model"])
                test.calls.append(("model", previous, llm_request.model_dump(mode="json", exclude_none=True)))
                if model_override is not None:
                    response = await model_override(llm_request, previous)
                elif previous < turns:
                    response = LlmResponse(content=types.Content(role="model", parts=[types.Part(
                        function_call=types.FunctionCall(name="read", args={"x": 2 + i},
                            id=None if missing_ids else "provided-" + str(i))) for i in range(parallel)]))
                else:
                    response = LlmResponse(content=types.Content(role="model", parts=[types.Part(text="done")]),
                        usage_metadata=types.GenerateContentResponseUsageMetadata(
                            prompt_token_count=7, candidates_token_count=3, total_token_count=10))
                yield response
        async def close():
            test.owners.append(("close", asyncio.current_task()))
        def create(bindings):
            test.owners.append(("open", asyncio.current_task()))
            model = bindings.model(Model(model="offline"), identity="fixture", configuration={"version": version}, close=close)
            tools = ([bindings.catalog("catalog", version="1", discover=discover, registry={"read-v1": execute})]
                     if catalog else [bindings.read_tool("read", read, version="1")])
            return LlmAgent(name="reader", model=model, tools=tools,
                before_tool_callback=None if before is None else bindings.transform("before", before, version="1"),
                after_tool_callback=None if after is None else bindings.transform("after", after, version="1"))
        return create

    async def test_actual_runner_full_replay_missing_ids_and_usage(self):
        first = await self.run_agent(self.factory())
        records = list(self.commits)
        self.assertEqual(first.output, "done")
        self.assertEqual(first.status, AgentStatus.SUCCEEDED)
        self.assertEqual([c[0] for c in self.calls], ["model", "tool", "model"])
        self.assertEqual(len(records), 4)
        second = await self.run_agent(self.factory(), records=records)
        self.assertEqual(second.output, "done")
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(first.usage[0].origin, UsageOrigin.CURRENT_DISPATCH)
        self.assertEqual(second.usage[0].origin, UsageOrigin.ACCEPTED_REPLAY)
        self.assertEqual(second.usage[0].total_tokens, 10)
        for opened, closed in zip(self.owners[::2], self.owners[1::2]):
            self.assertIs(opened[1], closed[1])
        # Both the live runner and replay receive a real typed native Event.
        self.assertEqual(first.native.content.parts[0].text, "done")
        self.assertIsNot(first.native, second.native)

    async def test_partial_replay_only_unaccepted_continuation_dispatches(self):
        await self.run_agent(self.factory())
        records = list(self.commits)
        self.calls.clear()
        result = await self.run_agent(self.factory(), records=records[:-1])
        self.assertEqual(result.output, "done")
        self.assertEqual([c[:2] for c in self.calls], [("model", 1)])

    async def test_parallel_repeated_arguments_distinct_causal_slots_and_reused_ids(self):
        result = await self.run_agent(self.factory(turns=2, parallel=2, missing_ids=False))
        self.assertEqual(result.output, "done")
        self.assertEqual([c[1] for c in self.calls if c[0] == "tool"], [2, 3, 2, 3])
        self.assertEqual(len(self.commits), 8)
        calls = len(self.calls)
        await self.run_agent(self.factory(turns=2, parallel=2, missing_ids=False), records=list(self.commits))
        self.assertEqual(len(self.calls), calls)

    async def test_declared_transforms_bind_effective_arguments_and_next_request(self):
        async def before(*, tool, args, tool_context):
            args["x"] += 4
        async def after(*, tool, args, tool_context, tool_response):
            return {"wrapped": tool_response}
        await self.run_agent(self.factory(before=before, after=after))
        self.assertEqual([c[1] for c in self.calls if c[0] == "tool"], [6])
        records = list(self.commits)
        calls = len(self.calls)
        await self.run_agent(self.factory(before=before, after=after), records=records)
        self.assertEqual(len(self.calls), calls)
        async def changed(*, tool, args, tool_context, tool_response):
            return {"changed": tool_response}
        with self.assertRaises(AgentBindingError):
            await self.run_agent(self.factory(before=before, after=changed), records=records)
        self.assertEqual(len(self.calls), calls)

    async def test_catalog_once_start_snapshot_retains_owned_targets_on_replay(self):
        result = await self.run_agent(self.factory(turns=2, catalog=True))
        self.assertEqual(result.output, "done")
        self.assertEqual(len([c for c in self.calls if c[0] == "discover"]), 1)
        calls = len(self.calls)
        await self.run_agent(self.factory(turns=2, catalog=True), records=list(self.commits))
        self.assertEqual(len(self.calls), calls)

    async def test_unsupported_state_mutation_poisoned_even_when_callback_raises(self):
        async def before(*, tool, args, tool_context):
            tool_context.state["bad"] = {"value": 1}
            raise RuntimeError("callback failure")
        with self.assertRaises(AgentProfileError) as raised:
            await self.run_agent(self.factory(before=before))
        self.assertEqual(raised.exception.code, "adk_state_or_actions_unsupported")
        self.assertFalse(any(c[0] == "tool" for c in self.calls))

    async def test_duplicate_provider_ids_rejected_before_any_tool_dispatch(self):
        async def duplicate(request, previous):
            return LlmResponse(content=types.Content(role="model", parts=[types.Part(
                function_call=types.FunctionCall(name="read", args={"x": 2}, id="same")) for _ in range(2)]))
        with self.assertRaises(AgentProfileError) as raised:
            await self.run_agent(self.factory(model_override=duplicate))
        self.assertEqual(raised.exception.code, "adk_duplicate_function_call_id")
        self.assertEqual(len(self.commits), 1)
        self.assertFalse(any(c[0] == "tool" for c in self.calls))

    async def test_final_transformed_arguments_are_validated_before_read(self):
        for value in (True, "3", {"unsupported": 1}):
            async def invalid(*, tool, args, tool_context):
                args["x"] = value
            with self.assertRaises(AgentProfileError) as raised:
                await self.run_agent(self.factory(before=invalid))
            self.assertEqual(raised.exception.code, "tool_arguments_invalid")
        self.assertFalse(any(c[0] == "tool" for c in self.calls))

    async def test_before_short_circuit_and_actions_rejected(self):
        async def short_circuit(*, tool, args, tool_context):
            return {"skip": True}
        async def mutate_action(*, tool, args, tool_context):
            tool_context.actions.skip_summarization = True
        for callback, code in ((short_circuit, "adk_tool_short_circuit_unsupported"),
                               (mutate_action, "adk_state_or_actions_unsupported")):
            with self.assertRaises(AgentProfileError) as raised:
                await self.run_agent(self.factory(before=callback))
            self.assertEqual(raised.exception.code, code)
        self.assertFalse(any(c[0] == "tool" for c in self.calls))

    async def test_model_and_tool_failures_poison_when_sdk_emits_terminal_error_event(self):
        async def fail(*args):
            raise RuntimeError("private-provider-error")
        for options in ({"model_override": fail}, {"tool_override": fail}):
            with self.assertRaises(AgentExecutionError) as raised:
                await self.run_agent(self.factory(**options))
            self.assertNotIn("private-provider", str(raised.exception))
        self.assertEqual(len([c for c in self.calls if c[0] == "model"]), 2)

    async def test_input_output_and_operation_bounds(self):
        with self.assertRaises(AgentLimitError):
            await self.run_agent(self.factory(), prompt="x" * 4097)
        self.assertEqual(self.calls, [])
        async def large_model(request, previous):
            return LlmResponse(content=types.Content(role="model", parts=[types.Part(text="x" * 8192)]))
        with self.assertRaises(AgentLimitError):
            await self.run_agent(self.factory(model_override=large_model))
        async def large_tool(x):
            return {"text": "x" * 8192}
        with self.assertRaises(AgentLimitError):
            await self.run_agent(self.factory(tool_override=large_tool))
        self.calls.clear()
        with self.assertRaises(AgentLimitError):
            await self.run_agent(self.factory(), spec=replace(SPEC, limits=AgentLimits(max_model_turns=1)))
        self.assertEqual([c[0] for c in self.calls], ["model", "tool"])

    async def test_effective_model_configuration_conflict_precedes_external_dispatch(self):
        await self.run_agent(self.factory())
        calls = len(self.calls)
        with self.assertRaises(AgentBindingError):
            await self.run_agent(self.factory(version="changed"), records=list(self.commits))
        self.assertEqual(len(self.calls), calls)

    async def test_after_transform_output_drift_binds_next_request_even_with_same_identity(self):
        delta = 1
        async def after(*, tool, args, tool_context, tool_response):
            return {"x": tool_response["x"] + delta}
        factory = self.factory(after=after)
        await self.run_agent(factory)
        calls, records = len(self.calls), list(self.commits)
        delta = 2
        with self.assertRaises(AgentBindingError):
            await self.run_agent(factory, records=records)
        self.assertEqual(len(self.calls), calls)

    async def test_factory_failure_closes_all_models_in_reverse_order_on_owner(self):
        events = []
        class Model(BaseLlm):
            async def generate_content_async(self, llm_request, stream=False):
                raise AssertionError("no model dispatch")
                yield
        async def first():
            events.append(("first", asyncio.current_task()))
        async def second():
            events.append(("second", asyncio.current_task()))
            raise RuntimeError("closer fails")
        def factory(bindings):
            events.append(("factory", asyncio.current_task()))
            bindings.model(Model(model="offline"), identity="one", configuration={}, close=first)
            bindings.model(Model(model="offline"), identity="two", configuration={}, close=second)
            raise RuntimeError("factory fails")
        with self.assertRaises(AgentExecutionError):
            await self.run_agent(factory)
        self.assertEqual([event[0] for event in events], ["factory", "second", "first"])
        self.assertTrue(all(event[1] is events[0][1] for event in events))

    async def test_runner_close_precedes_explicit_model_close_on_owner(self):
        real_close = Runner.close
        async def close(runner):
            self.owners.append(("runner-close", asyncio.current_task()))
            await real_close(runner)
        with patch.object(Runner, "close", close):
            await self.run_agent(self.factory(turns=0))
        self.assertEqual([event[0] for event in self.owners], ["open", "runner-close", "close"])
        self.assertTrue(all(event[1] is self.owners[0][1] for event in self.owners))

    async def test_waiter_cancel_drains_inflight_model_before_resource_close(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def delayed(request, previous):
            entered.set()
            await release.wait()
            return LlmResponse(content=types.Content(role="model", parts=[types.Part(text="done")]))
        ctx = self.context()
        handle = self.run_agent(self.factory(model_override=delayed), context=ctx)
        async def wait():
            return await handle
        waiter = asyncio.create_task(wait())
        await asyncio.wait_for(entered.wait(), 2)
        waiter.cancel()
        await asyncio.sleep(0)
        self.assertFalse(any(event[0] == "close" for event in self.owners))
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        with self.assertRaises(WorkflowError):
            await ctx._drain()
        self.assertEqual([event[0] for event in self.owners], ["open", "close"])
        self.assertIs(self.owners[0][1], self.owners[1][1])

    async def test_declared_defaults_are_in_effective_tool_request(self):
        async def defaults(request, previous):
            if previous == 0:
                return LlmResponse(content=types.Content(role="model", parts=[types.Part(
                    function_call=types.FunctionCall(name="read", args={}))]))
            return LlmResponse(content=types.Content(role="model", parts=[types.Part(text="done")]))
        await self.run_agent(self.factory(model_override=defaults))
        self.assertEqual([c[1] for c in self.calls if c[0] == "tool"], [2])
        calls = len(self.calls)
        await self.run_agent(self.factory(model_override=defaults), records=list(self.commits))
        self.assertEqual(len(self.calls), calls)

    async def test_portable_program_fixture_full_replay_counts_and_owner_cleanup(self):
        from framework_fixtures import adk_program
        with tempfile.TemporaryDirectory() as directory:
            marker = str(Path(directory) / "markers.jsonl")
            event = {"data": {"marker": marker}}
            with patch.object(adk_program, "workflow_context", self.context):
                first = await adk_program.handle(event)
            records = list(self.commits)
            with patch.object(adk_program, "workflow_context", lambda: self.context(records)):
                second = await adk_program.handle(event)
            marks = [json.loads(line) for line in Path(marker).read_text().splitlines()]
        self.assertEqual([sum(row["kind"] == kind for row in marks) for kind in ("model", "tool")], [3, 4])
        self.assertEqual(len(records), 8)
        self.assertEqual(first["output"], second["output"])
        for result in (first, second):
            self.assertTrue(result["resource"]["closed"])
            self.assertEqual(result["resource"]["open_task_id"], result["resource"]["close_task_id"])
            self.assertIsNone(result["transport_closed"])
        self.assertTrue(all(item["origin"] == "accepted_replay" for item in second["usage"]))

    async def test_parallel_identical_calls_reverse_completion_and_partial_replay(self):
        both, release = asyncio.Event(), asyncio.Event()
        started, completed = [], []
        async def read(x):
            index = len(started)
            started.append(index)
            if index == 0:
                await release.wait()
            else:
                both.set()
                completed.append(index)
                release.set()
                return {"x": x}
            completed.append(index)
            return {"x": x}
        async def model(request, previous):
            if previous == 0:
                return LlmResponse(content=types.Content(role="model", parts=[types.Part(
                    function_call=types.FunctionCall(name="read", args={"x": 2})) for _ in range(2)]))
            return LlmResponse(content=types.Content(role="model", parts=[types.Part(text="done")]))
        await self.run_agent(self.factory(model_override=model, tool_override=read))
        self.assertTrue(both.is_set())
        self.assertEqual(completed, [1, 0])
        self.assertEqual(len(self.commits), 5)
        records = list(self.commits)
        # Commit order is second tool, first tool; drop one accepted read while
        # retaining the final model. Its identical sibling must still replay.
        records.pop(2)
        self.calls.clear()
        await self.run_agent(self.factory(model_override=model, tool_override=read), records=records)
        self.assertEqual([c[0] for c in self.calls], ["tool"])
        self.assertEqual(len(started), 3)

    async def test_sdk_receives_copied_effective_request(self):
        async def model(request, previous):
            request.contents[0].parts[0].text = "provider mutates its owned request"
            if previous == 0:
                return LlmResponse(content=types.Content(role="model", parts=[types.Part(
                    function_call=types.FunctionCall(name="read", args={"x": 2}))]))
            return LlmResponse(content=types.Content(role="model", parts=[types.Part(text="done")]))
        await self.run_agent(self.factory(model_override=model))
        self.assertEqual([c[2]["contents"][0]["parts"][0]["text"] for c in self.calls if c[0] == "model"],
                         ["hello", "hello"])
        calls = len(self.calls)
        await self.run_agent(self.factory(model_override=model), records=list(self.commits))
        self.assertEqual(len(self.calls), calls)

    async def test_unsupported_agent_settings_and_undeclared_hooks_fail_before_model(self):
        async def callback(**kwargs):
            return None
        for field, value in (("output_key", "state"), ("include_contents", "none"),
                             ("before_model_callback", callback), ("before_tool_callback", callback),
                             ("instruction", callback), ("model", "unbound-native-model")):
            with self.subTest(field=field):
                create = self.factory()
                def factory(bindings):
                    agent = create(bindings)
                    setattr(agent, field, value)
                    return agent
                with self.assertRaises(AgentProfileError):
                    await self.run_agent(factory)
        self.assertEqual(self.calls, [])
        self.assertEqual([event[0] for event in self.owners], ["open", "close"] * 6)

    async def test_multiple_nonstream_responses_close_generator_before_rejection(self):
        closed = []
        class Model(BaseLlm):
            async def generate_content_async(self, llm_request, stream=False):
                try:
                    for _ in range(2):
                        yield LlmResponse(content=types.Content(role="model", parts=[types.Part(text="done")]))
                finally:
                    closed.append(True)
        def factory(bindings):
            return LlmAgent(name="fixture", model=bindings.model(Model(model="offline"),
                identity="fixture", configuration={}, close=None))
        with self.assertRaises(AgentProfileError) as raised:
            await self.run_agent(factory)
        self.assertEqual(raised.exception.code, "adk_multiple_model_responses_unsupported")
        self.assertEqual(closed, [True])
        self.assertEqual(len(self.commits), 1)


if __name__ == "__main__":
    unittest.main()
