"""Resumable lifecycle observations through the public client and loopback HTTP."""
import asyncio
from dataclasses import FrozenInstanceError
import json
import unittest
from unittest.mock import patch

from aiohttp import web
from ledgence.client import (
    AsyncClient, ExecutionProgressKind, ExecutionProgressPage, Forbidden, InputError,
    NotFound, ProgressTimeout, ProtocolError, RequestTimeout, TaskState,
    Unauthenticated, Unavailable, WorkflowState,
)
import test_workflows
from support import SCOPE, response


def token(after, *, kind="task", execution_id="task", scope=None, **changes):
    raw = {"version": 1, "scope": dict(SCOPE) if scope is None else scope,
           "target": {"kind": kind, "id": execution_id}, "after": str(after), **changes}
    return json.dumps(raw, ensure_ascii=False, separators=(",", ":")).encode().hex()


def page(*, after=0, sequences=(1,), high_watermark=None, state="active", kind="task",
         execution_id="task", **changes):
    if high_watermark is None:
        high_watermark = sequences[-1] if sequences else after
    position = sequences[-1] if sequences else after
    return {"scope": dict(SCOPE), "target": {"kind": kind, "id": execution_id},
            "state": state, "terminal": state in {"succeeded", "failed", "cancelled"},
            "high_watermark": str(high_watermark),
            "events": [{"sequence": str(seq), "at": 20 - index, "reason": "claimed",
                        "activation_id": "activation" if kind == "workflow" else None,
                        "attempt_id": "attempt" if kind == "task" else None}
                       for index, seq in enumerate(sequences)],
            "next_cursor": token(position, kind=kind, execution_id=execution_id),
            "has_more": position < high_watermark, **changes}


class ProgressTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflows.WorkflowClientTests.asyncSetUp

    def serve(self, value):
        async def handle(request, body):
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.path, "/v1/executions/progress")
            return response(value)
        self.callback = handle

    async def test_typed_task_page_and_exact_resume_request(self):
        self.serve(page(sequences=(1, 2), high_watermark=3))
        task = self.client.tasks.handle("task")
        first = await task.progress(limit=2)
        self.assertIsInstance(first, ExecutionProgressPage)
        self.assertEqual(first.target.kind, ExecutionProgressKind.TASK)
        self.assertIs(first.state, TaskState.ACTIVE)
        self.assertEqual(first.scope, self.client.scope)
        self.assertEqual([event.sequence for event in first.events], [1, 2])
        self.assertEqual([event.at for event in first.events], [20, 19])
        self.assertTrue(first.has_more)
        self.assertFalse(first.caught_up)
        self.assertIsInstance(first.events, tuple)
        with self.assertRaises(FrozenInstanceError):
            first.events[0].reason = "changed"
        self.assertEqual(json.loads(self.requests[-1][3]), {
            "scope": SCOPE, "target": {"kind": "task", "id": "task"},
            "cursor": None, "limit": 2,
        })
        self.serve(page(after=2, sequences=(3,), state="succeeded"))
        final = await task.progress(cursor=first.next_cursor)
        self.assertTrue(final.terminal)
        self.assertTrue(final.caught_up)
        self.assertIs(final.state, TaskState.SUCCEEDED)
        self.assertEqual(json.loads(self.requests[-1][3])["cursor"], first.next_cursor)
        replay = await task.progress(cursor=first.next_cursor)
        self.assertEqual(replay, final)
        self.assertTrue(all(r[:2] == ("POST", "/v1/executions/progress") for r in self.requests))

    async def test_single_page_wait_is_bounded_by_request_budget_and_not_in_cursor(self):
        self.serve(page())
        async with AsyncClient(self.url, tenant="tenant", namespace="tests", request_timeout=4) as client:
            observed = await client.tasks.handle("task").progress(wait_ms=20000)
        self.assertEqual(json.loads(self.requests[-1][3])["wait_ms"], 3000)
        self.assertEqual(observed.next_cursor, token(1))
        async with AsyncClient(self.url, tenant="tenant", namespace="tests", request_timeout=.5) as client:
            await client.tasks.handle("task").progress(wait_ms=20000)
        self.assertNotIn("wait_ms", json.loads(self.requests[-1][3]))

    async def test_caught_up_follow_requests_shared_wait_and_can_disable_it(self):
        for wait_ms in (20000, 0, 100):
            queries = []
            async def handle(request, body):
                queries.append(json.loads(body))
                return response(page(sequences=()) if len(queries) == 1 else page(state="succeeded"))
            self.callback = handle
            observed = [p async for p in self.client.tasks.handle("task").follow_progress(
                wait_ms=wait_ms, poll_interval=.25)]
            self.assertEqual(len(observed), 2)
            self.assertNotIn("wait_ms", queries[0])
            self.assertEqual(queries[1].get("wait_ms", 0), wait_ms)
            self.assertEqual(queries[1]["cursor"], observed[0].next_cursor)

    async def test_follow_wait_leaves_response_allowance_inside_observation_deadline(self):
        queries = []
        async def handle(request, body):
            queries.append(json.loads(body))
            return response(page(sequences=()) if len(queries) == 1 else page(state="succeeded"))
        self.callback = handle
        pages = [p async for p in self.client.tasks.handle("task").follow_progress(
            poll_interval=.25, timeout=3)]
        self.assertEqual(len(pages), 2)
        self.assertNotIn("wait_ms", queries[0])
        self.assertGreater(queries[1]["wait_ms"], 0)
        self.assertLess(queries[1]["wait_ms"], 2000)

    async def test_slow_empty_wait_does_not_add_another_poll_interval(self):
        queries = []
        async def handle(request, body):
            queries.append(json.loads(body))
            if len(queries) == 2:
                await asyncio.sleep(.35)
            return response(page(state="succeeded") if len(queries) == 3 else page(sequences=()))
        self.callback = handle
        # This spies on observer pacing only; HTTP fixture sleeps remain real.
        with patch("ledgence.client.progress._sleep", wraps=asyncio.sleep) as pause:
            observed = [p async for p in self.client.tasks.handle("task").follow_progress(poll_interval=.25)]
        self.assertEqual(len(observed), 2)
        self.assertEqual(pause.await_count, 1)
        self.assertEqual([q.get("wait_ms", 0) for q in queries], [0, 20000, 20000])

    async def test_cancellation_during_shared_wait_does_not_restart_or_mutate_execution(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        async def handle(request, body):
            if len(self.requests) == 1:
                return response(page())
            entered.set()
            await release.wait()
            return response(page(after=1, sequences=()))
        self.callback = handle
        iterator = self.client.tasks.handle("task").follow_progress(poll_interval=.25)
        first = await anext(iterator)
        observer = asyncio.create_task(anext(iterator))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            sent = json.loads(self.requests[-1][3])
            self.assertEqual(sent["wait_ms"], 20000)
            self.assertEqual(sent["cursor"], first.next_cursor)
            observer.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await observer
        finally:
            release.set()
            await iterator.aclose()
        self.assertEqual(len(self.requests), 2)
        self.assertTrue(all(r[1] == "/v1/executions/progress" for r in self.requests))

    async def test_workflow_state_and_activation_identity_are_typed(self):
        self.serve(page(kind="workflow", execution_id="workflow", state="waiting"))
        observed = await self.client.workflows.handle("workflow").progress()
        self.assertEqual(observed.target.kind, ExecutionProgressKind.WORKFLOW)
        self.assertIs(observed.state, WorkflowState.WAITING)
        self.assertEqual(observed.events[0].activation_id, "activation")
        self.assertIsNone(observed.events[0].attempt_id)
        self.assertFalse(observed.terminal)

    async def test_new_client_resumes_saved_workflow_cursor(self):
        self.serve(page(kind="workflow", execution_id="workflow", state="waiting"))
        initial = await self.client.workflows.handle("workflow").progress()
        self.serve(page(after=1, sequences=(2, 3), kind="workflow",
                        execution_id="workflow", state="succeeded"))
        async with AsyncClient(self.url, tenant="tenant", namespace="tests") as client:
            pages = [observed async for observed in client.workflows.handle("workflow").follow_progress(
                cursor=initial.next_cursor, timeout=2)]
        self.assertEqual(len(pages), 1)
        self.assertIs(pages[0].state, WorkflowState.SUCCEEDED)
        self.assertEqual([event.sequence for event in pages[0].events], [2, 3])
        self.assertEqual(json.loads(self.requests[-1][3])["cursor"], initial.next_cursor)

    async def test_full_width_sequence_and_empty_tail(self):
        maximum = 2**64 - 1
        self.serve(page(after=maximum-2, sequences=(maximum-1, maximum), state="failed"))
        observed = await self.client.tasks.handle("task").progress(cursor=token(maximum-2))
        self.assertEqual(observed.high_watermark, maximum)
        self.assertEqual(observed.events[-1].sequence, maximum)
        self.serve(page(after=maximum, sequences=(), state="failed"))
        tail = await self.client.tasks.handle("task").progress(cursor=observed.next_cursor)
        self.assertEqual(tail.next_cursor, observed.next_cursor)
        self.assertTrue(tail.caught_up)
        self.assertEqual(tail.events, ())

    async def test_invalid_queries_never_dispatch(self):
        task = self.client.tasks.handle("task")
        bad_tokens = ["", "f", "AA", "00", "ab"*2049,
                      token(0, kind="workflow"), token(0, execution_id="other"),
                      token(0, scope={**SCOPE, "tenant_id": "other"}),
                      token(0, version=True), token(0, version=2), token("01"),
                      bytes.fromhex(token(0)).replace(b'"version":1', b'"version":1,"version":1').hex(),
                      token(-1), token(2**64), token(0, extra=True),
                      json.dumps({"version": 1, "scope": SCOPE,
                                  "target": {"kind": "task", "id": "task"}, "after": "0"}).encode().hex()]
        for cursor in bad_tokens:
            with self.subTest(cursor=cursor[:45]), self.assertRaises(InputError):
                await task.progress(cursor=cursor)
        for limit in (0, 101, -1, True, 1.0, "1"):
            with self.subTest(limit=limit), self.assertRaises(InputError):
                await task.progress(limit=limit)
        for wait_ms in (-1, 20001, True, 1.0, "1"):
            with self.subTest(wait_ms=wait_ms), self.assertRaises(InputError):
                await task.progress(wait_ms=wait_ms)
        for options in ({"wait_ms": -1}, {"wait_ms": 20001}, {"wait_ms": True},
                        {"timeout": 0}, {"timeout": True}, {"timeout": float("inf")},
                        {"poll_interval": 0.249}, {"poll_interval": False},
                        {"poll_interval": float("nan")}, {"stop_on_terminal": 1},
                        {"limit": 101}, {"cursor": "no"}):
            with self.subTest(options=options), self.assertRaises(InputError):
                await anext(task.follow_progress(**options))
        self.assertEqual(self.requests, [])

    async def test_reject_malformed_and_inconsistent_pages(self):
        original = page()
        samples = [
            {**original, "scope": {**SCOPE, "namespace": "other"}},
            {**original, "target": {"kind": "workflow", "id": "task"}},
            {**original, "target": {"kind": "task", "id": "other"}},
            {**original, "state": "running"}, {**original, "state": []},
            {**original, "terminal": True}, {**original, "terminal": 0},
            {**original, "high_watermark": 1}, {**original, "high_watermark": "01"},
            {**original, "high_watermark": str(2**64)},
            {**original, "high_watermark": "0"}, {**original, "has_more": True},
            {**original, "has_more": 0}, {**original, "events": {}},
            {**original, "events": []}, {**original, "next_cursor": None},
            {**original, "next_cursor": token(0)},
            {**original, "next_cursor": token(1, execution_id="other")},
            {**original, "unexpected_payload": {"secret": "not metadata"}},
            {key: value for key, value in original.items() if key != "terminal"},
        ]
        row = original["events"][0]
        samples += [{**original, "events": [{**row, **change}]} for change in (
            {"sequence": True}, {"sequence": 1}, {"sequence": "2"}, {"sequence": "0"},
            {"at": True}, {"at": -1}, {"at": 253402300800000},
            {"reason": ""}, {"reason": "a"*129}, {"reason": "bad\n"},
            {"reason": "unknown_transition"},
            {"activation_id": "workflow-activation"}, {"attempt_id": 1},
            {"application_data": "forbidden"},
        )]
        samples.append(page(sequences=(1, 1), high_watermark=1))
        samples.append(page(sequences=(1, 3), high_watermark=3))
        for index, value in enumerate(samples):
            self.serve(value)
            with self.subTest(index=index), self.assertRaises(ProtocolError) as failure:
                await self.client.tasks.handle("task").progress()
            self.assertEqual(failure.exception.request_id, "request")

    async def test_reject_overfull_page_and_watermark_before_start(self):
        self.serve(page(sequences=(1, 2)))
        with self.assertRaises(ProtocolError):
            await self.client.tasks.handle("task").progress(limit=1)
        self.serve(page(after=1, sequences=(), high_watermark=1))
        with self.assertRaises(ProtocolError):
            await self.client.tasks.handle("task").progress(cursor=token(2))
        self.serve(page(kind="workflow", execution_id="workflow", state="running",
                        events=[{"sequence": "1", "at": 1, "reason": "started",
                                 "activation_id": None, "attempt_id": "task-attempt"}]))
        with self.assertRaises(ProtocolError):
            await self.client.workflows.handle("workflow").progress()

    async def test_transport_enforces_progress_byte_bound(self):
        async def handle(request, body):
            return web.Response(body=b" " * (256*1024) + json.dumps(page()).encode(),
                                headers={"Content-Type": "application/json", "Request-Id": "oversized"})
        self.callback = handle
        with self.assertRaises(ProtocolError):
            await self.client.tasks.handle("task").progress()

    async def test_drain_terminal_backlog_without_poll_delay(self):
        async def handle(request, body):
            query = json.loads(body)
            after = 0 if query["cursor"] is None else int(json.loads(bytes.fromhex(query["cursor"]))["after"])
            return response(page(after=after, sequences=(after+1,), high_watermark=3, state="succeeded"))
        self.callback = handle
        pages = [p async for p in self.client.tasks.handle("task").follow_progress(
            limit=1, poll_interval=60, timeout=2)]
        self.assertEqual([p.events[0].sequence for p in pages], [1, 2, 3])
        self.assertEqual(len(self.requests), 3)
        self.assertEqual([p.has_more for p in pages], [True, True, False])
        self.assertTrue(all(json.loads(r[3]).get("wait_ms", 0) == 0 for r in self.requests))

    async def test_iterator_has_no_background_read_and_can_close(self):
        self.serve(page(high_watermark=2))
        iterator = self.client.tasks.handle("task").follow_progress()
        self.assertEqual(self.requests, [])
        first = await anext(iterator)
        self.assertEqual(first.events[0].sequence, 1)
        await asyncio.sleep(.03)
        self.assertEqual(len(self.requests), 1)
        await iterator.aclose()
        await asyncio.sleep(.03)
        self.assertEqual(len(self.requests), 1)

    async def test_empty_active_pages_are_paced_and_suppressed(self):
        async def handle(request, body):
            return response(page(sequences=(), state="active") if len(self.requests) < 3
                            else page(state="succeeded"))
        self.callback = handle
        iterator = self.client.tasks.handle("task").follow_progress(poll_interval=.25, timeout=2)
        first = await anext(iterator)
        self.assertFalse(first.terminal)
        self.assertEqual(first.events, ())
        final = await anext(iterator)
        self.assertTrue(final.terminal)
        self.assertEqual(final.events[0].sequence, 1)
        with self.assertRaises(StopAsyncIteration):
            await anext(iterator)
        self.assertEqual(len(self.requests), 3)

    async def test_timeout_while_idle_retains_delivered_cursor_without_cancelling(self):
        self.serve(page(sequences=()))
        task = self.client.tasks.handle("task")
        iterator = task.follow_progress(timeout=.08)
        first = await anext(iterator)
        with self.assertRaises(ProgressTimeout) as error:
            await anext(iterator)
        self.assertIs(error.exception.execution, task)
        self.assertEqual(error.exception.last_page, first)
        self.assertEqual(error.exception.next_cursor, first.next_cursor)
        self.assertEqual(len(self.requests), 1)

    async def test_total_timeout_bounds_inflight_request(self):
        async def handle(request, body):
            await asyncio.sleep(.3)
            return response(page())
        self.callback = handle
        start = token(0)
        with self.assertRaises(ProgressTimeout) as error:
            await anext(self.client.tasks.handle("task").follow_progress(cursor=start, timeout=.03))
        self.assertIsNone(error.exception.last_page)
        self.assertEqual(error.exception.next_cursor, start)
        self.assertEqual(len(self.requests), 1)

    async def test_request_timeout_is_not_hidden_or_retried(self):
        async def handle(request, body):
            await asyncio.sleep(.3)
            return response(page())
        self.callback = handle
        async with AsyncClient(self.url, tenant="tenant", namespace="tests", request_timeout=.03) as client:
            with self.assertRaises(RequestTimeout):
                await anext(client.tasks.handle("task").follow_progress(timeout=2))
        self.assertEqual(len(self.requests), 1)

    async def test_errors_stop_follow_without_retry_or_execution_mutation(self):
        for code, status, error in (("not_found", 404, NotFound), ("forbidden", 403, Forbidden),
                                    ("unauthenticated", 401, Unauthenticated),
                                    ("unavailable", 503, Unavailable)):
            async def handle(request, body):
                value = {"code": code}
                if code == "unavailable":
                    value["message"] = "database offline"
                return response(value, code=status)
            self.callback = handle
            calls = len(self.requests)
            with self.assertRaises(error):
                await anext(self.client.tasks.handle("task").follow_progress(timeout=1))
            self.assertEqual(len(self.requests), calls+1)
        self.assertTrue(all(r[1] == "/v1/executions/progress" for r in self.requests))

    async def test_cancellation_stops_only_inflight_observation(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        async def handle(request, body):
            entered.set()
            await release.wait()
            return response(page())
        self.callback = handle
        iterator = self.client.tasks.handle("task").follow_progress()
        observer = asyncio.create_task(anext(iterator))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            observer.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await observer
        finally:
            release.set()
            await iterator.aclose()
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(self.requests[0][1], "/v1/executions/progress")

    async def test_keep_following_observes_late_cleanup_after_terminal(self):
        async def handle(request, body):
            if len(self.requests) == 1:
                return response(page(state="failed"))
            value = page(after=1, sequences=(2,), state="failed")
            value["events"][0]["reason"] = "cleanup_confirmed"
            return response(value)
        self.callback = handle
        iterator = self.client.tasks.handle("task").follow_progress(
            stop_on_terminal=False, poll_interval=.25, timeout=2)
        first = await anext(iterator)
        self.assertTrue(first.terminal)
        late = await anext(iterator)
        self.assertTrue(late.terminal)
        self.assertEqual(late.events[0].reason, "cleanup_confirmed")
        self.assertNotEqual(first.next_cursor, late.next_cursor)
        await iterator.aclose()

    async def test_default_stop_does_not_seal_history(self):
        self.serve(page(state="failed"))
        task = self.client.tasks.handle("task")
        observed = [p async for p in task.follow_progress(timeout=2)]
        self.assertEqual(len(observed), 1)
        self.serve(page(after=1, sequences=(2,), state="failed"))
        late = await task.progress(cursor=observed[0].next_cursor)
        self.assertEqual(late.events[0].sequence, 2)

    async def test_follow_rejects_terminal_or_high_watermark_regression(self):
        for first, second, options in (
            (page(state="failed"), page(after=1, sequences=(), state="active"),
             {"stop_on_terminal": False, "poll_interval": .25}),
            (page(high_watermark=3), page(after=1, sequences=(2,), high_watermark=2), {}),
        ):
            async def handle(request, body):
                return response(first if json.loads(body)["cursor"] is None else second)
            self.callback = handle
            iterator = self.client.tasks.handle("task").follow_progress(timeout=2, **options)
            await anext(iterator)
            with self.assertRaises(ProtocolError):
                await anext(iterator)

    async def test_consumer_processing_time_is_in_total_deadline(self):
        self.serve(page(high_watermark=2))
        iterator = self.client.tasks.handle("task").follow_progress(timeout=.2)
        first = await anext(iterator)
        await asyncio.sleep(.22)
        with self.assertRaises(ProgressTimeout) as error:
            await anext(iterator)
        self.assertEqual(error.exception.last_page, first)
        self.assertEqual(error.exception.next_cursor, first.next_cursor)
        self.assertEqual(len(self.requests), 1)


if __name__ == "__main__":
    unittest.main()
