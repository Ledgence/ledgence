"""Installed source-only Claude SDK conformance; no CLI, network or credentials (MIT)."""
import asyncio
from contextlib import ExitStack
from dataclasses import replace
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import socket
import subprocess
import unittest
from unittest.mock import patch

import test_workflow
from ledgence.worker.workflow import WorkflowContext
from ledgence.worker.integrations import (
    AgentRunSpec, AgentLimits, AgentStatus, AgentCapability, AgentCapabilityError,
    AgentBindingError, AgentExecutionError, AgentIntegrationError, AgentLimitError,
    AgentProfileError, AgentRunConflict, UsageOrigin, UsageSource, UsageCoverage,
)
from ledgence.worker.integrations import claude_agent_sdk as integration

try:
    INSTALLED = importlib.metadata.version("claude-agent-sdk") == "0.2.180"
except importlib.metadata.PackageNotFoundError:
    INSTALLED = False

OPTIONS = {"model": "offline-model", "tools": []}
SPEC = AgentRunSpec("claude-fixture", "1", {"options": OPTIONS})


def terminal(**changes):
    value = {"type": "result", "subtype": "success", "duration_ms": 1,
             "duration_api_ms": 0, "is_error": False, "num_turns": 1,
             "session_id": "offline-session", "result": "done",
             "usage": {"input_tokens": 10, "output_tokens": 3},
             "total_cost_usd": 0.001, "terminal_reason": "completed"}
    value.update(changes)
    return value


def assistant(identifier="message-1", **changes):
    message = {"id": identifier, "model": "offline-model",
               "content": [{"type": "text", "text": "working"}],
               "usage": {"input_tokens": 10, "output_tokens": 2}}
    message.update(changes)
    return {"type": "assistant", "session_id": "offline-session", "message": message}


class PublicBoundaryTests(unittest.TestCase):
    def test_module_is_lazy_and_runtime_is_mandatory(self):
        self.assertFalse(any(name in integration.__dict__ for name in ("ClaudeSDKClient", "ClaudeAgentOptions", "Transport")))
        self.assertEqual(integration.PROFILE.capabilities, frozenset({
            AgentCapability.RESULT_REPLAY, AgentCapability.REPORTED_USAGE, AgentCapability.STOP_REQUEST}))
        for runtime in (None, integration.ExternalCli("relative", "runtime"), integration.CustomTransport(None, "runtime")):
            with self.assertRaises(AgentProfileError):
                integration.run(None, spec=SPEC, prompt="question", factory=lambda _: None, runtime=runtime)


@unittest.skipUnless(INSTALLED, "requires reviewed installed source-only claude-agent-sdk==0.2.180")
class ClaudeIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.contexts, self.commits, self.transports, self.attempts = [], [], [], []
        self.guards = ExitStack()
        self.addCleanup(self.guards.close)
        environment = {key: os.environ[key] for key in ("HOME", "PATH", "TMPDIR", "LANG") if key in os.environ}
        self.guards.enter_context(patch.dict(os.environ, environment, clear=True))
        original_connect, original_connect_ex = socket.socket.connect, socket.socket.connect_ex
        original_sendto = socket.socket.sendto
        def deny(*args, **kwargs):
            self.attempts.append("external I/O attempted")
            raise AssertionError("network/process activity is forbidden in this fixture")
        class NoProcess(subprocess.Popen):
            def __init__(self, *args, **kwargs): deny()
        def connect(sock, address):
            if sock.family in (socket.AF_INET, socket.AF_INET6): return deny()
            return original_connect(sock, address)
        def connect_ex(sock, address):
            if sock.family in (socket.AF_INET, socket.AF_INET6): return deny()
            return original_connect_ex(sock, address)
        def sendto(sock, *args):
            if sock.family in (socket.AF_INET, socket.AF_INET6): return deny()
            return original_sendto(sock, *args)
        for target, value in (("socket.socket.connect", connect), ("socket.socket.connect_ex", connect_ex),
                              ("socket.socket.sendto", sendto), ("socket.getaddrinfo", deny),
                              ("subprocess.Popen", NoProcess), ("os.system", deny),
                              ("asyncio.create_subprocess_exec", deny), ("asyncio.create_subprocess_shell", deny)):
            self.guards.enter_context(patch(target, value))
        for name in ("fork", "forkpty", "posix_spawn", "posix_spawnp"):
            if hasattr(os, name): self.guards.enter_context(patch.object(os, name, deny))
        self.sdk = importlib.import_module("claude_agent_sdk")
        import anyio
        self.guards.enter_context(patch.object(anyio, "open_process", deny))
        distribution = importlib.metadata.distribution("claude-agent-sdk")
        self.assertIn(Path(self.sdk.__file__).resolve(), {Path(distribution.locate_file(file)).resolve() for file in distribution.files})
        self.assertEqual(self.sdk.__version__, "0.2.180")
        self.assertFalse(any("claude_agent_sdk/_bundled/" in str(file) and Path(str(file)).name != ".gitignore" for file in distribution.files))

    async def asyncTearDown(self):
        try:
            for transport in self.transports:
                transport.allow_open.set()
                transport.allow_close.set()
            for context in self.contexts:
                await context._finish(cancel=True)
            self.assertEqual(self.attempts, [])
        finally:
            self.guards.close()

    def context(self, records=(), rpc=None):
        async def commit(operation, record):
            self.assertEqual(operation, "local_step.commit")
            self.commits.append(json.loads(json.dumps(record)))
            return {"committed": True}
        context = WorkflowContext(test_workflow.payload(local_steps=list(records)), rpc or commit)
        self.contexts.append(context)
        return context

    def transport(self, *, messages=None, blocked=False, open_blocked=False,
                  open_fails=False, close_blocked=False, close_fails=False):
        test = self
        class Scripted(self.sdk.Transport):
            def __init__(inner):
                inner.queue = asyncio.Queue()
                inner.events = []
                inner.ready = False
                inner.opens = inner.closes = inner.queries = 0
                inner.reader_closed = False
                inner.opening, inner.closing, inner.queried = asyncio.Event(), asyncio.Event(), asyncio.Event()
                inner.allow_open, inner.allow_close = asyncio.Event(), asyncio.Event()
                if not open_blocked: inner.allow_open.set()
                if not close_blocked: inner.allow_close.set()
            async def connect(inner):
                inner.opens += 1
                inner.events.append(("connect", asyncio.current_task()))
                inner.ready = True
                inner.opening.set()
                await inner.allow_open.wait()
                if open_fails: raise RuntimeError("private connect error")
            async def close(inner):
                inner.closes += 1
                inner.events.append(("close", asyncio.current_task()))
                inner.closing.set()
                await inner.allow_close.wait()
                inner.ready = False
                await inner.queue.put(None)
                if close_fails: raise RuntimeError("private close error")
            def is_ready(inner): return inner.ready
            async def end_input(inner): pass
            async def read_messages(inner):
                try:
                    while True:
                        value = await inner.queue.get()
                        if value is None: return
                        yield value
                finally:
                    inner.reader_closed = True
            async def write(inner, line):
                message = json.loads(line)
                if message["type"] == "control_request":
                    subtype = message["request"]["subtype"]
                    inner.events.append((subtype, asyncio.current_task()))
                    await inner.queue.put({"type": "control_response", "response": {
                        "subtype": "success", "request_id": message["request_id"], "response": {}}})
                elif message["type"] == "user":
                    inner.queries += 1
                    inner.events.append(("query", asyncio.current_task()))
                    inner.queried.set()
                    test.assertEqual(message["message"]["content"], "question")
                    if not blocked:
                        for value in messages if messages is not None else [assistant(), assistant(), terminal()]:
                            await inner.queue.put(value)
                        if messages == []: await inner.queue.put(None)
        value = Scripted()
        self.transports.append(value)
        return value

    def run_agent(self, context, *, spec=SPEC, factory=None, runtime=None, **transport_options):
        runtime = runtime or integration.CustomTransport(lambda: self.transport(**transport_options), "offline-transport-v1")
        return integration.run(context, spec=spec, prompt="question",
                               factory=factory or (lambda bindings: bindings.client()), runtime=runtime)

    async def test_actual_sdk_success_usage_dedup_and_full_replay_opens_no_client(self):
        calls = []
        def factory(bindings):
            calls.append(bindings.configuration)
            return bindings.client()
        result = await self.run_agent(self.context(), factory=factory)
        self.assertEqual(result.status, AgentStatus.SUCCEEDED)
        self.assertEqual(result.output, "done")
        self.assertIsInstance(result.native, self.sdk.ResultMessage)
        self.assertEqual(result.native.total_cost_usd, 0.001)
        self.assertEqual(len(result.usage), 2)
        total = next(item for item in result.usage if item.source_kind is UsageSource.RUN_TOTAL)
        self.assertEqual((total.input_tokens, total.output_tokens), (10, 3))
        self.assertIsNone(total.total_tokens)
        self.assertIsNone(total.reported_cost)
        self.assertIsNone(total.currency)
        self.assertIs(total.coverage, UsageCoverage.PARTIAL)
        self.assertTrue(result.lifecycle.transport_closed)
        self.assertIsNone(result.lifecycle.local_process_exit_observed)
        self.assertFalse(result.lifecycle.checkpoint_verified)
        transport = self.transports[0]
        self.assertEqual(transport.closes, 1)
        self.assertTrue(transport.reader_closed)
        self.assertIs(transport.events[0][1], transport.events[-1][1])
        records = list(self.commits)
        result.native.result = "mutated"
        replayed = await self.run_agent(self.context(records), factory=lambda _: self.fail("replay factory"))
        self.assertEqual(replayed.output, "done")
        self.assertEqual(replayed.native.result, "done")
        self.assertIsNot(replayed.native, result.native)
        self.assertTrue(all(item.origin is UsageOrigin.ACCEPTED_REPLAY for item in replayed.usage))
        self.assertIs(replayed.lifecycle.origin, UsageOrigin.ACCEPTED_REPLAY)
        self.assertEqual(len(self.transports), 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(records), 2)

    async def test_manifest_ack_precedes_client_and_unawaited_run_is_retained(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def commit(operation, record):
            if not self.commits:
                entered.set()
                await release.wait()
            self.commits.append(record)
            return {"committed": True}
        context = self.context(rpc=commit)
        handle = self.run_agent(context)
        await entered.wait()
        self.assertEqual(self.transports, [])
        draining = asyncio.create_task(context._drain())
        release.set()
        await draining
        self.assertEqual((await handle).output, "done")

    async def test_duplicate_capability_and_native_call_limits_fail_before_factory(self):
        context = self.context()
        handle = self.run_agent(context)
        with self.assertRaises(AgentRunConflict): self.run_agent(context)
        await handle
        for limits in (AgentLimits(max_model_turns=1), AgentLimits(max_tool_calls=1), AgentLimits(max_discoveries=1)):
            with self.assertRaises(AgentCapabilityError): self.run_agent(self.context(), spec=replace(SPEC, limits=limits))
        with self.assertRaises(AgentCapabilityError):
            integration.run(self.context(), spec=SPEC, prompt="question", factory=lambda _: self.fail("factory"),
                            runtime=integration.CustomTransport(lambda: self.fail("transport"), "offline"),
                            require=frozenset({AgentCapability.MODEL_REPLAY}))

    async def test_actual_options_mismatch_and_host_callbacks_reject_before_connect(self):
        changes = [{"model": "different"}, {"hooks": {"PreToolUse": [object()]}},
                   {"can_use_tool": lambda *_: None}, {"stderr": lambda *_: None},
                   {"mcp_servers": {"host": {"type": "sdk", "instance": object()}}},
                   {"session_store": object()}, {"resume": "old"}, {"continue_conversation": True},
                   {"fork_session": True}, {"enable_file_checkpointing": True},
                   {"settings": "{}"}, {"extra_args": {"debug": None}}, {"plugins": [{"type": "local", "path": "."}]}]
        for index, changed in enumerate(changes):
            def factory(bindings):
                options = self.sdk.ClaudeAgentOptions(model="offline-model", tools=[], permission_mode="dontAsk",
                                                     setting_sources=[], strict_mcp_config=True)
                for key, value in changed.items(): setattr(options, key, value)
                return bindings.client(options=options)
            with self.subTest(changed=list(changed)):
                with self.assertRaises((AgentProfileError, AgentBindingError)):
                    await self.run_agent(self.context(), factory=factory, spec=replace(SPEC, key=f"bad-{index}"))
        self.assertEqual(self.transports, [])

    async def test_mutation_after_bound_construction_is_rejected_and_allocated_transport_closed(self):
        def factory(bindings):
            client = bindings.client()
            client.options.model = "changed"
            return client
        with self.assertRaises(AgentBindingError): await self.run_agent(self.context(), factory=factory)
        transport = self.transports[0]
        self.assertEqual((transport.opens, transport.queries, transport.closes), (0, 0, 1))

    async def test_factory_failure_after_construction_closes_once(self):
        def factory(bindings):
            bindings.client()
            raise RuntimeError("private factory error")
        with self.assertRaises(AgentExecutionError): await self.run_agent(self.context(), factory=factory)
        self.assertEqual((self.transports[0].opens, self.transports[0].closes), (0, 1))

    async def test_reused_and_preconnected_transports_are_rejected_without_borrowed_close(self):
        transport = self.transport()
        runtime = integration.CustomTransport(lambda: transport, "offline-transport-v1")
        self.assertEqual((await self.run_agent(self.context(), runtime=runtime)).output, "done")
        with self.assertRaises(AgentProfileError):
            await self.run_agent(self.context(), runtime=runtime)
        self.assertEqual((transport.opens, transport.closes), (1, 1))
        borrowed = self.transport()
        borrowed.ready = True
        with self.assertRaises(AgentProfileError):
            await self.run_agent(self.context(), runtime=integration.CustomTransport(lambda: borrowed, "offline"))
        self.assertEqual((borrowed.opens, borrowed.closes), (0, 0))
        borrowed.ready = False  # The application retains its borrowed object.

    async def test_external_cli_constructs_public_client_with_explicit_path_only(self):
        clients = []
        def factory(bindings):
            client = bindings.client()
            clients.append(client)
            self.assertIs(type(client), self.sdk.ClaudeSDKClient)
            self.assertEqual(client.options.cli_path, "/not-executed/claude")
            raise RuntimeError("stop fixture before unqualified CLI connect")
        with self.assertRaises(AgentExecutionError):
            await self.run_agent(self.context(), factory=factory,
                                 runtime=integration.ExternalCli("/not-executed/claude", "explicit-cli-v1"))
        self.assertEqual(len(clients), 1)
        self.assertEqual(self.transports, [])
        self.assertEqual(len(self.commits), 1)
        self.assertEqual(self.attempts, [])

    async def test_malformed_semantic_types_fail_before_factory(self):
        for changed in ({"permission_mode": []}, {"mcp_servers": {"bad": {"type": []}}}):
            with self.assertRaises(AgentProfileError):
                self.run_agent(self.context(), spec=replace(SPEC, configuration={"options": {**OPTIONS, **changed}}))
        self.assertEqual(self.commits, [])
        self.assertEqual(self.transports, [])

    async def test_external_mcp_semantics_and_credential_names_are_bound_without_values(self):
        options = {"model": "offline-model", "tools": [], "mcp_servers": {
            "stdio": {"type": "stdio", "command": "/declared/mcp", "args": ["--read"]},
            "http": {"type": "http", "url": "https://example.invalid/mcp", "credential_headers": ["authorization"]},
            "sse": {"type": "sse", "url": "https://example.invalid/events"},
        }}
        def factory(bindings):
            actual = bindings.configuration["options"]
            for config in actual["mcp_servers"].values():
                names = config.pop("credential_headers", [])
                if names: config["headers"] = {name: "Bearer private-fixture-token" for name in names}
            return bindings.client(options=self.sdk.ClaudeAgentOptions(**actual))
        result = await self.run_agent(self.context(), spec=replace(SPEC, configuration={"options": options}), factory=factory)
        self.assertEqual(result.status, AgentStatus.SUCCEEDED)
        self.assertNotIn("private-fixture-token", json.dumps(self.commits))
        self.assertIn("authorization", json.dumps(self.commits))
        for server in ({"type": "http", "url": "https://user:secret@example.invalid/mcp"},
                       {"type": "http", "url": "https://example.invalid/mcp?token=secret"},
                       {"type": "http", "url": "https://example.invalid/mcp", "credential_headers": ["anthropic-version"]},
                       {"type": "stdio", "command": "mcp", "env": {"TOKEN": "secret"}}):
            with self.assertRaises(AgentProfileError):
                self.run_agent(self.context(), spec=replace(SPEC, configuration={"options": {**OPTIONS, "mcp_servers": {"bad": server}}}))

    async def test_terminal_classification_and_mirror_failure_are_conservative(self):
        for changes, expected in [({}, AgentStatus.SUCCEEDED),
                                  ({"terminal_reason": "aborted_tools"}, AgentStatus.ABORTED),
                                  ({"terminal_reason": "aborted_streaming"}, AgentStatus.ABORTED),
                                  ({"subtype": "error_max_turns", "is_error": True, "terminal_reason": "max_turns"}, AgentStatus.FAILED),
                                  ({"is_error": True, "terminal_reason": "api_error"}, AgentStatus.FAILED)]:
            result = await self.run_agent(self.context(), messages=[terminal(**changes)])
            self.assertIs(result.status, expected)
            if expected is not AgentStatus.SUCCEEDED: self.assertIsNone(result.output)
        for message in (terminal(terminal_reason="future_reason"), terminal(subtype="future_subtype"),
                        terminal(terminal_reason=[]), terminal(subtype=[])):
            before = len(self.commits)
            with self.assertRaises(AgentProfileError): await self.run_agent(self.context(), messages=[message])
            self.assertEqual(len(self.commits), before + 1)
        before = len(self.commits)
        with self.assertRaises(AgentExecutionError) as caught:
            await self.run_agent(self.context(), messages=[{"type": "system", "subtype": "mirror_error", "error": "secret"}, terminal()])
        self.assertEqual(caught.exception.code, "session_mirror_failed")
        self.assertNotIn("secret", str(caught.exception))
        self.assertEqual(len(self.commits), before + 1)

    async def test_missing_terminal_and_failed_close_never_commit_whole_result(self):
        for configuration in ({"messages": []}, {"close_fails": True}):
            before = len(self.commits)
            with self.assertRaises(AgentExecutionError): await self.run_agent(self.context(), **configuration)
            self.assertEqual(len(self.commits), before + 1)
            self.assertEqual(self.transports[-1].closes, 1)

    async def test_partial_connect_failure_closes_transport_when_sdk_has_no_query(self):
        with self.assertRaises(AgentExecutionError): await self.run_agent(self.context(), open_fails=True)
        transport = self.transports[0]
        self.assertEqual((transport.opens, transport.queries, transport.closes), (1, 0, 1))
        self.assertFalse(transport.ready)
        self.assertIs(transport.events[0][1], transport.events[-1][1])
        self.assertEqual(len(self.commits), 1)

    async def test_cancel_during_connect_retains_open_then_prevents_query_and_closes_same_task(self):
        context = self.context()
        observer = asyncio.ensure_future(self.run_agent(context, open_blocked=True))
        while not self.transports: await asyncio.sleep(0)
        transport = self.transports[0]
        await transport.opening.wait()
        observer.cancel()
        with self.assertRaises(asyncio.CancelledError): await observer
        finishing = asyncio.create_task(context._finish(cancel=True))
        await asyncio.sleep(0)
        self.assertFalse(finishing.done())
        self.assertEqual(transport.closes, 0)
        transport.allow_open.set()
        await finishing
        self.assertEqual((transport.queries, transport.closes), (0, 1))
        self.assertIs(transport.events[0][1], transport.events[-1][1])
        self.assertEqual(len(self.commits), 1)

    async def test_interrupt_ack_without_terminal_stays_unaccepted_and_cleanup_retained(self):
        context = self.context()
        observer = asyncio.ensure_future(self.run_agent(context, blocked=True, close_blocked=True))
        while not self.transports: await asyncio.sleep(0)
        transport = self.transports[0]
        await transport.queried.wait()
        observer.cancel()
        with self.assertRaises(asyncio.CancelledError): await observer
        await transport.closing.wait()
        self.assertIn("interrupt", [event[0] for event in transport.events])
        finishing = asyncio.create_task(context._finish(cancel=True))
        second = asyncio.create_task(context._finish())
        await asyncio.sleep(0)
        finishing.cancel()
        await asyncio.sleep(0)
        finishing.cancel()
        await asyncio.sleep(0)
        self.assertFalse(finishing.done())
        self.assertFalse(second.done())
        transport.allow_close.set()
        with self.assertRaises(asyncio.CancelledError): await finishing
        await second
        self.assertEqual(transport.closes, 1)
        self.assertEqual(len(self.commits), 1)
        self.assertIs(transport.events[0][1], transport.events[-1][1])

    async def test_deadline_requests_interrupt_without_accepting_a_terminal_result(self):
        with self.assertRaises(AgentExecutionError) as caught:
            await self.run_agent(self.context(), spec=replace(SPEC, limits=AgentLimits(timeout_ms=100)), blocked=True)
        self.assertEqual(caught.exception.code, "deadline_exceeded")
        self.assertEqual(self.transports[0].closes, 1)
        self.assertIn("interrupt", [event[0] for event in self.transports[0].events])
        self.assertEqual(len(self.commits), 1)

    async def test_message_count_bytes_and_final_envelope_bounds_are_honest(self):
        cases = [([assistant(str(index), usage=None) for index in range(256)] + [terminal()]),
                 [assistant("large", content=[{"type": "text", "text": "x" * (256 * 1024)}]), terminal()],
                 [terminal(result="x" * 8000)]]
        for messages in cases:
            before = len(self.commits)
            with self.assertRaises(AgentLimitError) as caught: await self.run_agent(self.context(), messages=messages)
            self.assertTrue(caught.exception.external_work_may_have_occurred)
            self.assertEqual(len(self.commits), before + 1)
            self.assertEqual(self.transports[-1].closes, 1)

    async def test_invalid_usage_and_conflicting_duplicate_observations(self):
        for value in (-1, True, float("nan")):
            with self.assertRaises((AgentProfileError, AgentLimitError)):
                await self.run_agent(self.context(), messages=[terminal(usage={"input_tokens": value, "output_tokens": 1})])
        for cost in (-1.0, True, float("inf")):
            with self.assertRaises((AgentProfileError, AgentLimitError)):
                await self.run_agent(self.context(), messages=[terminal(total_cost_usd=cost)])
        result = await self.run_agent(self.context(), messages=[assistant(), assistant(usage={"input_tokens": 99, "output_tokens": 1}), terminal()])
        observation = next(item for item in result.usage if item.source_kind is UsageSource.ASSISTANT_MESSAGE)
        self.assertIs(observation.coverage, UsageCoverage.PARTIAL)
        self.assertIsNone(observation.input_tokens)
        self.assertIsNone(observation.output_tokens)

    async def test_unaccepted_whole_run_retry_repeats_query_and_binding_drift_opens_nothing(self):
        first_context = self.context()
        with self.assertRaises(AgentExecutionError): await self.run_agent(first_context, messages=[])
        records = list(self.commits)
        result = await self.run_agent(self.context(records))
        self.assertEqual(result.output, "done")
        self.assertEqual(sum(transport.queries for transport in self.transports), 2)
        accepted = list(self.commits)
        before = len(self.transports)
        with self.assertRaises(AgentBindingError):
            await self.run_agent(self.context(accepted), spec=replace(SPEC, version="changed"))
        self.assertEqual(len(self.transports), before)

    async def test_structured_output_schema_and_public_value_are_bound(self):
        schema = {"type": "object", "properties": {"answer": {"type": "integer"}}, "required": ["answer"], "additionalProperties": False}
        spec = replace(SPEC, configuration={"options": {**OPTIONS, "output_format": {"type": "json_schema", "schema": schema}}})
        result = await self.run_agent(self.context(), spec=spec, messages=[terminal(structured_output={"answer": 42})])
        self.assertEqual(result.output, {"answer": 42})
        result.output["answer"] = 0
        replayed = await self.run_agent(self.context(list(self.commits)), spec=spec)
        self.assertEqual(replayed.output, {"answer": 42})
        with self.assertRaises(AgentProfileError):
            await self.run_agent(self.context(), spec=spec, messages=[terminal(structured_output={"answer": "wrong"})])


if __name__ == "__main__":
    unittest.main()
