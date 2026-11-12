"""SDK-free conformance for retained framework runs and journal boundaries (MIT)."""
import asyncio
import contextvars
from dataclasses import replace
import json
import unittest
from unittest.mock import patch

import test_workflow
from ledgence.worker.workflow import OperationKind, WorkflowError, WorkflowContext
from ledgence.worker.integrations import (
    AdapterProfile, AgentCapability, AgentLimits, AgentRunSpec, AgentStatus,
    AgentRunConflict, AgentBindingError, AgentCapabilityError, AgentProfileError,
    AgentLimitError, AgentExecutionError, UsageObservation, UsageSource, UsageOrigin, UsageCoverage,
)
from ledgence.worker.integrations._core import start_run, PreparedCall, JsonCodec
from ledgence.worker.integrations._schema import validate_schema, validate_arguments

PROFILE = AdapterProfile("fixture.bounded.v1", "1", frozenset({AgentCapability.MODEL_REPLAY}))
SPEC = AgentRunSpec("triage", "1", {"policy": "read"})


class Session:
    def __init__(self, state, factory):
        self.state, self.factory = state, factory
        self.tasks = []
        self.events = []
    async def open(self):
        self.tasks.append(asyncio.current_task())
        self.events.append("open")
        self.operation = self.factory(self.state)
    async def run(self):
        return await self.operation(self.state)
    async def request_stop(self): self.events.append("stop")
    async def close(self):
        self.tasks.append(asyncio.current_task())
        self.events.append("close")


async def one_model(state, executor):
    key = state.slot("MODEL")
    value = await state.execute(PreparedCall(key, OperationKind.MODEL, {"model": "fixture"},
                                           {"prompt": state.prompt}, "json-v1"), executor)
    return state.result(AgentStatus.SUCCEEDED, value)


class IntegrationCoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.contexts, self.commits, self.sessions = [], [], []
    async def asyncTearDown(self):
        for context in self.contexts:
            await context._finish(cancel=True)
    def context(self, records=(), rpc=None):
        async def commit(operation, record):
            self.assertEqual(operation, "local_step.commit")
            self.commits.append(json.loads(json.dumps(record)))
            return {"committed": True}
        context = WorkflowContext(test_workflow.payload(local_steps=list(records)), rpc or commit)
        self.contexts.append(context)
        return context
    def run_agent(self, context, factory, *, spec=SPEC, prompt="question", session=Session, **kwargs):
        def make(state, factory):
            result = session(state, factory)
            self.sessions.append(result)
            return result
        return start_run(context, spec=spec, prompt=prompt, factory=factory, profile=PROFILE,
                         dependencies=(), normalization="fixture-v1", session_factory=make, **kwargs)

    def run_agent_with_controlled_deadline(self, context, factory, **kwargs):
        # Capture the production timer without letting machine speed choose which
        # lifecycle phase it interrupts. No coroutine runs before cancellation.
        with patch.object(context._loop, "call_later", wraps=context._loop.call_later) as schedule:
            result = self.run_agent(context, factory, **kwargs)
            result._state._timer.cancel()
        schedule.assert_called_once()
        delay, expire = schedule.call_args.args
        self.assertEqual(delay, result._state.spec.limits.timeout_ms / 1000)
        self.assertEqual(schedule.call_args.kwargs, {})
        return result, expire

    async def test_manifest_ack_precedes_factory_and_never_awaited_runs_are_owned(self):
        acknowledged = asyncio.Event()
        calls = []
        async def rpc(operation, record):
            await acknowledged.wait()
            self.commits.append(record)
            return {"committed": True}
        async def execute(state): return state.result(AgentStatus.SUCCEEDED, state.configuration)
        def factory(state):
            calls.append(state.configuration)
            return execute
        context = self.context(rpc=rpc)
        config = {"policy": "read"}
        handle = self.run_agent(context, factory, spec=replace(SPEC, configuration=config))
        config["policy"] = "write"
        draining = asyncio.create_task(context._drain())
        await asyncio.sleep(0)
        self.assertEqual(calls, [])
        self.assertFalse(draining.done())
        acknowledged.set()
        await draining
        self.assertEqual(calls, [{"policy": "read"}])
        self.assertEqual((await handle).output, {"policy": "read"})
        self.assertIs(self.sessions[0].tasks[0], self.sessions[0].tasks[1])

    async def test_duplicate_occurrence_rejected_synchronously_and_branches_independent(self):
        async def execute(state): return state.result(AgentStatus.SUCCEEDED, None)
        context = self.context()
        handle = self.run_agent(context, lambda _: execute)
        with self.assertRaises(AgentRunConflict): self.run_agent(context, lambda _: execute)
        await handle
        with self.assertRaises(AgentRunConflict): self.run_agent(context, lambda _: execute)
        await self.run_agent(context, lambda _: execute, spec=replace(SPEC, branch="other"))
        self.assertEqual(len(self.sessions), 2)

    async def test_full_replay_and_manifest_drift_before_factory(self):
        calls = []
        async def external(*, request):
            calls.append(request)
            request["arguments"]["prompt"] = "mutated"
            return {"answer": 42}
        async def execute(state): return await one_model(state, external)
        first = self.context()
        result = await self.run_agent(first, lambda _: execute)
        result.output["answer"] = 99
        previous = list(self.commits)
        resumed = self.context(previous)
        second = await self.run_agent(resumed, lambda _: execute)
        self.assertEqual(second.output, {"answer": 42})
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.sessions[0].state.anchor, self.sessions[1].state.anchor)
        for changes in ({"prompt": "changed"}, {"spec": replace(SPEC, version="2")},
                        {"spec": replace(SPEC, configuration={"policy": "write"})}):
            with self.assertRaises(AgentBindingError):
                await self.run_agent(self.context(previous), lambda _: self.fail("factory dispatched"), **changes)

    async def test_effective_binding_conflict_does_not_dispatch(self):
        calls = []
        args = {"x": 1}
        async def execute(state):
            async def callback(*, request):
                calls.append(request)
                return request["arguments"]
            answer = await state.execute(PreparedCall(state.slot("MODEL"), OperationKind.MODEL,
                                         {"model": "fixture"}, args, "v1"), callback)
            return state.result(AgentStatus.SUCCEEDED, answer)
        await self.run_agent(self.context(), lambda _: execute)
        previous = list(self.commits)
        args["x"] = 2
        with self.assertRaises(AgentBindingError):
            await self.run_agent(self.context(previous), lambda _: execute)
        self.assertEqual(len(calls), 1)

    async def test_failed_slot_poison_cannot_be_swallowed_or_redispatched(self):
        calls = 0
        async def external(*, request):
            nonlocal calls
            calls += 1
            raise ValueError("sensitive provider message")
        async def execute(state):
            for _ in range(2):
                try: await one_model(state, external)
                except AgentExecutionError: pass
            return state.result(AgentStatus.SUCCEEDED, "wrong")
        with self.assertRaises(AgentExecutionError) as error:
            await self.run_agent(self.context(), lambda _: execute)
        self.assertEqual(calls, 1)
        self.assertEqual(str(error.exception), "callback_failed")
        self.assertTrue(error.exception.external_work_may_have_occurred)

    async def test_cancelling_observer_retains_late_operation_and_same_task_close(self):
        running, release = asyncio.Event(), asyncio.Event()
        async def external(*, request):
            running.set()
            await release.wait()
            return "accepted-late"
        async def execute(state): return await one_model(state, external)
        context = self.context()
        handle = self.run_agent(context, lambda _: execute)
        observer = asyncio.ensure_future(handle)
        await running.wait()
        observer.cancel()
        with self.assertRaises(asyncio.CancelledError): await observer
        finishing = asyncio.create_task(context._finish(cancel=True))
        second = asyncio.create_task(context._finish())
        await asyncio.sleep(0)
        finishing.cancel()
        await asyncio.sleep(0)
        finishing.cancel()
        await asyncio.sleep(0)
        self.assertFalse(finishing.done())
        self.assertFalse(second.done())
        self.assertNotIn("close", self.sessions[0].events)
        release.set()
        with self.assertRaises(asyncio.CancelledError): await finishing
        await second
        self.assertIn("close", self.sessions[0].events)
        self.assertIs(self.sessions[0].tasks[0], self.sessions[0].tasks[1])
        self.assertEqual(len(self.commits), 2)
        self.assertEqual(len(context._owned_tasks()), 1)

    async def test_stop_during_open_retains_acquisition_prevents_query_and_closes(self):
        opening, release = asyncio.Event(), asyncio.Event()
        class Blocked(Session):
            async def open(inner):
                inner.tasks.append(asyncio.current_task())
                opening.set()
                await release.wait()
            async def run(inner): self.fail("post-stop query")
        context = self.context()
        observer = asyncio.ensure_future(self.run_agent(context, lambda _: None, session=Blocked))
        await opening.wait()
        observer.cancel()
        with self.assertRaises(asyncio.CancelledError): await observer
        finishing = asyncio.create_task(context._finish(cancel=True))
        await asyncio.sleep(0)
        self.assertFalse(finishing.done())
        release.set()
        await finishing
        self.assertIs(self.sessions[0].tasks[0], self.sessions[0].tasks[1])
        with self.assertRaises(AgentExecutionError): await context._drain()

    async def test_cancel_before_manifest_ack_does_not_invoke_factory(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def rpc(operation, record):
            entered.set()
            await release.wait()
            return {"committed": True}
        context = self.context(rpc=rpc)
        observer = asyncio.ensure_future(self.run_agent(context, lambda _: self.fail("factory")))
        await entered.wait()
        observer.cancel()
        with self.assertRaises(asyncio.CancelledError): await observer
        closing = asyncio.create_task(context._finish(cancel=True))
        await asyncio.sleep(0)
        self.assertFalse(closing.done())
        release.set()
        await closing
        self.assertEqual(self.sessions, [])

    async def test_late_cleanup_failure_is_not_observed_by_cancelled_waiter(self):
        entered, release = asyncio.Event(), asyncio.Event()
        class FailingClose(Session):
            async def close(inner):
                entered.set()
                await release.wait()
                raise RuntimeError("close secret")
        async def execute(state): return state.result(AgentStatus.SUCCEEDED, None)
        context = self.context()
        observer = asyncio.ensure_future(self.run_agent(context, lambda _: execute, session=FailingClose))
        await entered.wait()
        observer.cancel()
        with self.assertRaises(asyncio.CancelledError): await observer
        release.set()
        with self.assertRaisesRegex(AgentExecutionError, "resource_close_failed"):
            await context._drain()

    async def test_normal_finish_allows_owned_subsequent_calls_and_drains_new_roots(self):
        started, release = asyncio.Event(), asyncio.Event()
        async def execute(state):
            started.set()
            await release.wait()
            async def external(*, request): return 7
            return await one_model(state, external)
        context = self.context()
        handle = self.run_agent(context, lambda _: execute)
        await started.wait()
        closing = asyncio.create_task(context._finish())
        await asyncio.sleep(0)
        release.set()
        await closing
        self.assertEqual((await handle).output, 7)
        self.assertEqual(len(self.commits), 2)

    async def test_driver_cannot_hide_nested_workflow_controls(self):
        async def execute(state):
            async def callback(*, request):
                self.run_agent(state.ctx, lambda _: None, spec=replace(SPEC, branch="nested"))
            return await one_model(state, callback)
        with self.assertRaises(AgentExecutionError) as raised:
            await self.run_agent(self.context(), lambda _: execute)
        self.assertIsInstance(raised.exception.__cause__, WorkflowError)
        self.assertEqual(len(self.sessions), 1)

    async def test_nested_controls_poison_even_when_tools_swallow_context_errors(self):
        from ledgence.worker.workflow import _local_owner
        for mode in ("inherited", "reset_local", "empty_direct", "empty_task", "alternate_context"):
            context = self.context()
            alternate = self.context()
            factories = []
            async def execute(state):
                async def callback(*, request):
                    def attempt():
                        target = alternate if mode == "alternate_context" else context
                        try:
                            self.run_agent(target, lambda _: factories.append("forbidden"),
                                           spec=replace(SPEC, branch="nested"))
                        except WorkflowError:
                            pass  # A framework may box or swallow a tool exception.
                    if mode == "reset_local":
                        token = _local_owner.set(None)
                        try: attempt()
                        finally: _local_owner.reset(token)
                    elif mode == "empty_direct":
                        contextvars.Context().run(attempt)
                    elif mode == "empty_task":
                        async def child(): attempt()
                        await asyncio.create_task(child(), context=contextvars.Context())
                    else:
                        attempt()
                    return "must not be accepted"
                try:
                    await one_model(state, callback)
                except AgentExecutionError:
                    pass  # The outer loop cannot recover this poisoned run.
                return state.result(AgentStatus.SUCCEEDED, "incorrect")
            before = len(self.commits)
            with self.subTest(mode=mode):
                with self.assertRaises(AgentExecutionError):
                    await self.run_agent(context, lambda _: execute)
                self.assertEqual(factories, [])
                self.assertEqual(len(self.commits), before + 1)  # Manifest only.
                self.assertIsNotNone(context._fatal)

    async def test_capability_limits_and_configuration_fail_before_factory(self):
        context = self.context()
        for kwargs in ({"require": frozenset({AgentCapability.SESSION_RESUME})},
                       {"spec": replace(SPEC, limits=AgentLimits(max_model_turns=5))},
                       {"spec": replace(SPEC, limits=AgentLimits(max_parallel_calls=True))},
                       {"prompt": "a" * 4097}):
            with self.assertRaises((AgentCapabilityError, AgentLimitError)):
                self.run_agent(context, lambda _: self.fail("factory"), **kwargs)
        self.assertEqual(context._owned_tasks(), [])

    async def test_oversized_output_is_uncertain_and_not_committed(self):
        async def external(*, request): return "x" * 8192
        async def execute(state): return await one_model(state, external)
        with self.assertRaises(AgentLimitError) as error:
            await self.run_agent(self.context(), lambda _: execute)
        self.assertTrue(error.exception.external_work_may_have_occurred)
        self.assertEqual(len(self.commits), 1)

    async def test_shared_parallel_bound_across_branches(self):
        running, release = asyncio.Event(), asyncio.Event()
        active = peak = 0
        async def external(*, request):
            nonlocal active, peak
            active += 1; peak = max(peak, active)
            if active == 4: running.set()
            await release.wait()
            active -= 1
            return 1
        async def execute(state):
            values = await asyncio.gather(*(state.execute(PreparedCall(state.slot("TOOL_PART", "model", i),
                OperationKind.TOOL, {"read": "fixture"}, {"x": i}, "v1"), external) for i in range(3)))
            return state.result(AgentStatus.SUCCEEDED, values)
        context = self.context()
        first = self.run_agent(context, lambda _: execute)
        second = self.run_agent(context, lambda _: execute, spec=replace(SPEC, branch="other"))
        await asyncio.wait_for(running.wait(), 1)
        await asyncio.sleep(0)
        self.assertEqual(peak, 4)
        release.set()
        self.assertEqual([r.output for r in await asyncio.gather(first, second)], [[1]*3, [1]*3])

    async def test_usage_deduplicates_conflicts_without_summing_cumulative_totals(self):
        async def execute(state):
            item = UsageObservation(state.slot("MODEL"), UsageSource.MODEL_RESPONSE, "response-1",
                                    UsageOrigin.CURRENT_DISPATCH, input_tokens=7, coverage=UsageCoverage.REPORTED)
            state.observe_usage(item); state.observe_usage(item)
            state.observe_usage(replace(item, input_tokens=8))
            state.observe_usage(replace(item, source_kind=UsageSource.RUN_TOTAL))
            return state.result(AgentStatus.SUCCEEDED, None)
        result = await self.run_agent(self.context(), lambda _: execute)
        self.assertEqual(len(result.usage), 2)
        self.assertIsNone(result.usage[0].input_tokens)
        self.assertEqual(result.usage[0].coverage, UsageCoverage.PARTIAL)
        self.assertEqual(result.usage[1].input_tokens, 7)

    async def test_swallowed_slot_budget_failure_still_prevents_success(self):
        async def execute(state):
            try:
                for index in range(129): state.slot("invented", position=index)
            except AgentLimitError:
                pass
            return state.result(AgentStatus.SUCCEEDED, "incorrect")
        with self.assertRaisesRegex(AgentLimitError, "causal_slot_bound_exceeded"):
            await self.run_agent(self.context(), lambda _: execute)

    async def test_foreign_slot_cannot_share_another_runs_executor_or_owner(self):
        owner_ready, release = asyncio.Event(), asyncio.Event()
        known = {}
        callbacks = []
        async def execute(state):
            if state.spec.branch == "main":
                key = state.slot("MODEL")
                known["key"] = key
            else:
                await owner_ready.wait()
                key = known["key"]
            async def external(*, request):
                callbacks.append(state.spec.branch)
                owner_ready.set()
                await release.wait()
                return 42
            answer = await state.execute(PreparedCall(key, OperationKind.MODEL,
                {"model": "fixture"}, {"same": True}, "v1"), external)
            return state.result(AgentStatus.SUCCEEDED, answer)
        context = self.context()
        first = self.run_agent(context, lambda _: execute)
        second = self.run_agent(context, lambda _: execute, spec=replace(SPEC, branch="other"))
        with self.assertRaises(AgentBindingError): await second
        self.assertEqual(callbacks, ["main"])
        self.assertFalse(self.sessions[0].state.task.done())
        release.set()
        self.assertEqual((await first).output, 42)
        self.assertFalse(self.sessions[0].state.children & self.sessions[1].state.children)

    async def test_partial_prefix_replay_and_lost_ack_use_server_history(self):
        server = []
        counts = []
        lose_ack = True
        async def rpc(operation, record):
            nonlocal lose_ack
            server.append(json.loads(json.dumps(record)))
            if len(server) == 2 and lose_ack:
                lose_ack = False
                raise ConnectionError("lost after durable write")
            return {"committed": True}
        async def execute(state):
            parent = "START"
            for turn in range(3):
                key = state.slot("MODEL", parent)
                async def external(*, request):
                    counts.append(request["arguments"]["turn"])
                    return {"turn": request["arguments"]["turn"]}
                await state.execute(PreparedCall(key, OperationKind.MODEL, {"model": "fixture"},
                                                {"turn": turn}, "v1"), external)
                parent = key
            return state.result(AgentStatus.SUCCEEDED, "done")
        with self.assertRaises(AgentExecutionError):
            await self.run_agent(self.context(rpc=rpc), lambda _: execute)
        self.assertEqual(counts, [0])
        result = await self.run_agent(self.context(server, rpc), lambda _: execute)
        self.assertEqual(result.output, "done")
        self.assertEqual(counts, [0, 1, 2])
        self.assertEqual(len(server), 4)

    async def test_core_aggregate_overflow_after_callback_reports_uncertainty(self):
        context = self.context()
        await context.local("ordinary-a", lambda: "x" * (120 * 1024))
        await context.local("ordinary-b", lambda: "x" * (120 * 1024))
        callbacks = []
        async def execute(state):
            async def external(*, request):
                callbacks.append(True)
                return "x" * 8000
            answer = await state.execute(PreparedCall(state.slot("MODEL"), OperationKind.MODEL,
                {"model": "fixture"}, {"large": "x" * 12000}, "v1"), external)
            return state.result(AgentStatus.SUCCEEDED, answer)
        with self.assertRaises(AgentExecutionError) as error:
            await self.run_agent(context, lambda _: execute)
        self.assertTrue(error.exception.external_work_may_have_occurred)
        self.assertEqual(callbacks, [True])
        self.assertEqual(len(self.commits), 3)  # ordinary entries + manifest only

    async def test_deadline_does_not_release_an_uncooperative_owned_call(self):
        running, release = asyncio.Event(), asyncio.Event()
        class EvidenceSession(Session):
            async def request_stop(inner):
                await super().request_stop()
                inner.state.lifecycle = replace(inner.state.lifecycle, stop_acknowledged=True)
            async def close(inner):
                await super().close()
                inner.state.lifecycle = replace(inner.state.lifecycle, transport_closed=True)
        async def external(*, request):
            running.set()
            await release.wait()
            return 1
        async def execute(state): return await one_model(state, external)
        context = self.context()
        result, expire = self.run_agent_with_controlled_deadline(
            context, lambda _: execute, session=EvidenceSession,
            spec=replace(SPEC, limits=AgentLimits(timeout_ms=5)))
        state = result._state
        try:
            await asyncio.wait_for(running.wait(), 1)
            self.assertFalse(state.stopped)
            context._loop.call_soon(expire)
            await asyncio.wait_for(state.stopping.wait(), 1)
            await asyncio.sleep(0)
            self.assertFalse(state.task.done())
            self.assertNotIn("close", self.sessions[0].events)
        finally:
            release.set()
        with self.assertRaisesRegex(AgentExecutionError, "deadline_exceeded") as caught:
            await asyncio.wait_for(asyncio.shield(result), 1)
        self.assertIn("close", self.sessions[0].events)
        self.assertEqual(caught.exception.lifecycle, state.lifecycle)
        self.assertTrue(caught.exception.external_work_may_have_occurred)
        self.assertTrue(caught.exception.lifecycle.stop_requested)
        self.assertTrue(caught.exception.lifecycle.stop_acknowledged)
        self.assertTrue(caught.exception.lifecycle.transport_closed)

    async def test_deadline_before_manifest_ack_retains_ack_and_prevents_dispatch(self):
        committing, acknowledge = asyncio.Event(), asyncio.Event()
        factories, callbacks = [], []
        async def rpc(operation, record):
            self.assertEqual(operation, "local_step.commit")
            committing.set()
            await acknowledge.wait()
            self.commits.append(json.loads(json.dumps(record)))
            return {"committed": True}
        async def external(*, request):
            callbacks.append(True)
            return 1
        async def execute(state): return await one_model(state, external)
        def factory(state):
            factories.append(True)
            return execute
        context = self.context(rpc=rpc)
        result, expire = self.run_agent_with_controlled_deadline(
            context, factory, spec=replace(SPEC, limits=AgentLimits(timeout_ms=5)))
        state = result._state
        try:
            await asyncio.wait_for(committing.wait(), 1)
            context._loop.call_soon(expire)
            await asyncio.wait_for(state.stopping.wait(), 1)
            await asyncio.sleep(0)
            self.assertFalse(state.task.done())
            self.assertTrue(any(not child.done() for child in state.children))
            self.assertEqual(self.commits, [])
            self.assertEqual(self.sessions, [])
            self.assertEqual(factories, [])
            self.assertEqual(callbacks, [])
        finally:
            acknowledge.set()
        with self.assertRaisesRegex(AgentExecutionError, "deadline_exceeded") as caught:
            await asyncio.wait_for(asyncio.shield(result), 1)
        self.assertEqual(len(self.commits), 1)
        self.assertTrue(all(child.done() for child in state.children))
        self.assertEqual(self.sessions, [])
        self.assertEqual(factories, [])
        self.assertEqual(callbacks, [])
        self.assertFalse(caught.exception.external_work_may_have_occurred)
        self.assertTrue(caught.exception.lifecycle.stop_requested)

    async def test_native_failure_delivers_final_cleanup_evidence(self):
        class EvidenceSession(Session):
            async def close(inner):
                await super().close()
                inner.state.lifecycle = replace(inner.state.lifecycle, transport_closed=True)
        async def execute(state):
            raise AgentProfileError("unsupported_native_result", lifecycle=state.lifecycle)
        with self.assertRaisesRegex(AgentProfileError, "unsupported_native_result") as caught:
            await self.run_agent(self.context(), lambda _: execute, session=EvidenceSession)
        self.assertEqual(caught.exception.lifecycle, self.sessions[0].state.lifecycle)
        self.assertTrue(caught.exception.lifecycle.transport_closed)

    async def test_run_registry_bound_rejects_before_unbounded_task_creation(self):
        async def execute(state): return state.result(AgentStatus.SUCCEEDED, None)
        context = self.context()
        handles = [self.run_agent(context, lambda _: execute, spec=replace(SPEC, key=str(i))) for i in range(64)]
        with self.assertRaisesRegex(AgentLimitError, "run_count_exceeded"):
            self.run_agent(context, lambda _: execute, spec=replace(SPEC, key="extra"))
        await asyncio.gather(*handles)
        self.assertEqual(len(context._owned_tasks()), 64)

    @unittest.skipUnless(hasattr(asyncio, "eager_task_factory"), "Python 3.12+ eager tasks")
    async def test_eager_task_factory_cannot_run_before_owner_and_child_registration(self):
        loop = asyncio.get_running_loop()
        original = loop.get_task_factory()
        loop.set_task_factory(asyncio.eager_task_factory)
        try:
            async def execute(state):
                self.assertIs(state.ctx._integration_owners[(SPEC.key, SPEC.branch)], state)
                self.assertIsNotNone(state.task)
                async def external(*, request):
                    self.assertIn(asyncio.current_task(), state.children)
                    return "ok"
                return await one_model(state, external)
            self.assertEqual((await self.run_agent(self.context(), lambda _: execute)).output, "ok")
        finally:
            loop.set_task_factory(original)


class SchemaTests(unittest.TestCase):
    def test_catalog_requires_named_callables_even_when_lambda_is_assigned(self):
        from ledgence.worker.integrations._catalog import callable_identity, ReadSpec, CatalogSpec
        def named_read(*, arguments): return arguments
        assigned_lambda = lambda **kwargs: kwargs
        self.assertIn("named_read", callable_identity(named_read))
        with self.assertRaisesRegex(AgentProfileError, "named_read_callable_required"):
            ReadSpec.create("read", assigned_lambda, "1")
        with self.assertRaisesRegex(AgentProfileError, "named_read_callable_required"):
            CatalogSpec(None, "catalog", version="1", discover=assigned_lambda, registry={"read": named_read})
        with self.assertRaisesRegex(AgentProfileError, "named_read_callable_required"):
            CatalogSpec(None, "catalog", version="1", discover=named_read, registry={"read": assigned_lambda})

    def test_static_schema_binds_defaults_without_unrestricted_types(self):
        from ledgence.worker.integrations._schema import schema_for_callable, effective_arguments
        def read(x: int, *, enabled: bool = True, tags: list[str] = []): return x
        schema = schema_for_callable(read)
        self.assertEqual(schema["required"], ["x"])
        self.assertEqual(effective_arguments(read, {"x": 12.0}), {"x": 12.0, "enabled": True, "tags": []})
        first = effective_arguments(read, {"x": 12})
        first["tags"].append("mutated")
        self.assertEqual(effective_arguments(read, {"x": 12})["tags"], [])
        def unknown(x: dict): pass
        def missing(x): pass
        def bad_default(x: int = True): pass
        for function in (unknown, missing, bad_default):
            with self.assertRaises(AgentProfileError): schema_for_callable(function)

    def test_subset_validates_effective_arguments_without_numeric_coercion(self):
        schema = validate_schema({"type": "object", "additionalProperties": False,
            "properties": {"x": {"type": "integer", "enum": [12]}, "tags": {"type": "array", "items": {"type": "string"}}},
            "required": ["x"]})
        self.assertEqual(validate_arguments(schema, {"x": 12.0}), {"x": 12.0})
        for bad in ({}, {"x": True}, {"x": 11}, {"x": 12, "extra": 1}, {"x": 12, "tags": [1]}):
            with self.assertRaises(AgentProfileError): validate_arguments(schema, bad)
        for change in ({"additionalProperties": True}, {"oneOf": []}, {"$ref": "x"}, {"required": ["missing"]}):
            with self.assertRaises(AgentProfileError): validate_schema(dict(schema, **change))
