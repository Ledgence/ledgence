import asyncio
import json
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import aiohttp
from aiohttp import web

from ledgence.client import (
    AsyncClient, CancellationUncertain, ClientClosed, Conflict, InputError, NotFound,
    ProtocolError, RequestTimeout, RetryPolicy, ServiceError, SubmissionUncertain,
    TaskCancelled, TaskFailed, TransportError, Unavailable, WaitTimeout,
)
from ledgence.client import tasks, transport
from support import response, result, status, submitted


class ClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests = []
        self.callback = None
        async def handle(request):
            body = await request.read()
            self.requests.append((request.method, request.path, dict(request.query), body,
                                  dict(request.headers)))
            if self.callback is not None:
                return await self.callback(request, body)
            if request.path == "/v1/tasks": return response(submitted(json.loads(body)))
            if request.path == "/v1/tasks/status": return response(status("succeeded"))
            if request.path == "/v1/tasks/result": return response(result(output={"answer": 42}))
            if request.path == "/v1/tasks/cancel": return response("active")
            return response({"code": "not_found"}, code=404)
        app = web.Application(client_max_size=3 * 1024 * 1024)
        app.router.add_route("*", "/{path:.*}", handle)
        self.runner = web.AppRunner(app, shutdown_timeout=.1)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        self.addAsyncCleanup(self.runner.cleanup)
        self.client = await self.open_client()

    async def open_client(self, **kwargs):
        client = AsyncClient(self.url, tenant="tenant", namespace="tests", **kwargs)
        await client.__aenter__()
        self.addAsyncCleanup(client.close)
        return client

    def args(self, data=None):
        return dict(program="echo", version="1.0.0", queue="queue", data=data,
                    idempotency_key="submit")

    async def test_submit_result_and_reconnect(self):
        task = await self.client.tasks.submit(**self.args({"x": 1}))
        self.assertEqual(task.id, "task")
        self.assertEqual(await task.result(), {"answer": 42})
        resumed = self.client.tasks.handle(task.id)
        self.assertEqual((await resumed.status()).state, "succeeded")
        self.assertEqual((await resumed.outcome()).outcome.kind, "succeeded")
        self.assertTrue(all(r[0] != "POST" for r in self.requests[1:]))
        with self.assertRaises(AttributeError): self.client.scope = "other"

    async def test_frozen_submission_and_defaults(self):
        data = {"nested": [1, -0.0]}
        frozen = self.client.tasks.prepare(**self.args(data))
        data["nested"][0] = 2
        altered = frozen.to_dict(); altered["input"]["data"]["nested"][0] = 9
        await self.client.tasks.submit(frozen)
        sent = json.loads(self.requests[0][3])
        self.assertEqual(sent["input"]["data"]["nested"], [1, -0.0])
        self.assertNotIn("retry_policy", sent["input"])
        self.assertNotIn("attempt_timeout_ms", sent["input"])
        with self.assertRaises(InputError):
            await self.client.tasks.submit(frozen, data=None)
        other = AsyncClient(self.url, tenant="other", namespace="tests")
        async with other:
            with self.assertRaises(InputError): await other.tasks.submit(frozen)
        elsewhere = AsyncClient("http://127.0.0.1:9", tenant="tenant", namespace="tests")
        async with elsewhere:
            with self.assertRaises(InputError): await elsewhere.tasks.submit(frozen)
        self.assertEqual(len(self.requests), 1)

    async def test_invalid_submission_never_dispatches(self):
        invalid = [dict(self.args(), idempotency_key=""), dict(self.args(), attempt_timeout_ms=True),
                   dict(self.args(), data={1: "x"}), dict(self.args(), version="../x"),
                   dict(self.args(), queue="\n"), dict(self.args(), retry_policy={})]
        invalid.append({k: v for k, v in self.args().items() if k != "idempotency_key"})
        for args in invalid:
            with self.assertRaises(InputError): await self.client.tasks.submit(**args)
        self.assertEqual(self.requests, [])

    async def test_valid_unavailable_is_mutation_uncertainty(self):
        async def unavailable(request, body):
            return response({"code": "unavailable", "message": "commit outcome uncertain"}, code=503)
        self.callback = unavailable
        frozen = self.client.tasks.prepare(**self.args())
        with self.assertRaises(SubmissionUncertain) as exc:
            await self.client.tasks.submit(frozen)
        self.assertIs(exc.exception.submission, frozen)
        self.assertIsInstance(exc.exception.cause, Unavailable)
        self.assertEqual(exc.exception.request_id, "request")
        task = self.client.tasks.handle("task")
        with self.assertRaises(CancellationUncertain) as exc:
            await task.cancel()
        self.assertIs(exc.exception.task, task)
        self.assertEqual(len(self.requests), 2)

    async def test_uncertain_resend_preserves_exact_body_and_key(self):
        accepted = []
        async def accept(request, body):
            accepted.append(body)
            if len(accepted) == 1:
                return response({"code": "unavailable", "message": "reply lost after acceptance"}, code=503)
            return response(submitted(json.loads(body)))
        self.callback = accept
        frozen = self.client.tasks.prepare(**self.args({"nested": [1, -0.0]}))
        with self.assertRaises(SubmissionUncertain) as uncertain:
            await self.client.tasks.submit(frozen)
        resent = await self.client.tasks.submit(uncertain.exception.submission)
        self.assertEqual(resent.id, "task")
        self.assertEqual(len(accepted), 2)
        self.assertEqual(accepted[0], accepted[1])
        self.assertEqual(json.loads(accepted[1])["idempotency_key"], "submit")
        self.assertEqual(uncertain.exception.submission.idempotency_key, "submit")

    async def test_optional_request_id_uses_bounded_ascii_graphic_profile(self):
        for header, expected in (("r" * 256, "r" * 256), ("r" * 257, None),
                                 ("request id", None), ("caf\u00e9", None)):
            async def unavailable(request, body):
                reply = response({"code": "unavailable", "message": "offline"}, code=503)
                reply.headers["Request-Id"] = header
                return reply
            self.callback = unavailable
            with self.assertRaises(Unavailable) as exc:
                await self.client.tasks.handle("task").status()
            self.assertEqual(exc.exception.request_id, expected)

    async def test_definitive_errors_not_wrapped_as_uncertainty(self):
        for code, http, expected in (("conflict", 409, Conflict), ("not_found", 404, NotFound),
                                     ("invalid_input", 400, ServiceError)):
            async def reject(request, body):
                data = {"code": code}
                if code == "invalid_input": data["message"] = "invalid program"
                return response(data, code=http)
            self.callback = reject
            with self.assertRaises(expected): await self.client.tasks.submit(**self.args())

    async def test_changed_submission_echo_is_uncertain(self):
        for change in (lambda r: r.update(idempotency_key="other"),
                       lambda r: r["input"].update(data=1.0),
                       lambda r: r["descriptor"]["program"].update(id="different")):
            async def changed(request, body):
                value = submitted(json.loads(body)); change(value); return response(value)
            self.callback = changed
            with self.assertRaises(SubmissionUncertain) as exc:
                await self.client.tasks.submit(**self.args(1))
            self.assertIsInstance(exc.exception.cause, ProtocolError)

    async def test_submission_timestamp_range_failure_preserves_uncertainty(self):
        maximum = 253402300799999
        fields = ("submitted_at", "available_at", "terminal_at", "cancel_requested_at")
        times = {field: maximum for field in fields}

        async def cancelled(request, body):
            reply = submitted(json.loads(body))
            reply.update(state="cancelled", **times)
            return response(reply)

        self.callback = cancelled
        frozen = self.client.tasks.prepare(**self.args())
        task = await self.client.tasks.submit(frozen)
        self.assertEqual(task.id, "task")
        for field in fields:
            times[field] = maximum + 1
            with self.subTest(field=field):
                with self.assertRaises(SubmissionUncertain) as raised:
                    await self.client.tasks.submit(frozen)
                self.assertIs(raised.exception.submission, frozen)
                self.assertIsInstance(raised.exception.cause, ProtocolError)
            times[field] = maximum
        self.assertTrue(all(request[0] == "POST" for request in self.requests))
        self.assertEqual(len(self.requests), 5)

    async def test_program_failures_are_distinct_and_wait_returns_values(self):
        failures = [
            {"kind": "application", "error": {"kind": "business", "message": "declined"}},
            {"kind": "execution", "error": {"kind": "io", "message": "missing"},
             "phase": "preparation", "cleanup_error": {"kind": "runtime", "message": "cleanup"}},
            {"kind": "attempt_lost"},
        ]
        for failure in failures:
            async def failed(request, body):
                return response(status("failed") if request.path.endswith("status") else
                                result("failed", failure=failure, quiescence=(
                                    "unconfirmed" if failure["kind"] == "attempt_lost" else "confirmed")))
            self.callback = failed
            task = self.client.tasks.handle("task")
            observed = await task.wait()
            self.assertEqual(observed.outcome.failure.kind, failure["kind"])
            with self.assertRaises(TaskFailed) as exc: await task.result()
            self.assertEqual(exc.exception.failure.kind, failure["kind"])
            self.assertEqual(exc.exception.result.task.state, "failed")

    async def test_successful_null_and_unconfirmed_cleanup(self):
        async def succeeded(request, body):
            return response(status("succeeded") if request.path.endswith("status") else
                            result(output=None, quiescence="unconfirmed"))
        self.callback = succeeded
        task = self.client.tasks.handle("task")
        self.assertIsNone(await task.result())
        self.assertEqual((await task.outcome()).outcome.quiescence, "unconfirmed")

    async def test_cancellation_ack_does_not_fabricate_terminal_cancel(self):
        task = self.client.tasks.handle("task")
        self.assertEqual(await task.cancel(), "active")
        self.assertEqual(await task.result(), {"answer": 42})
        async def cancelled(request, body):
            return response(status("cancelled") if request.path.endswith("status") else result("cancelled"))
        self.callback = cancelled
        with self.assertRaises(TaskCancelled) as exc: await task.result()
        self.assertIsNone(exc.exception.result.task.latest_attempt_id)

    async def test_wait_budget_is_not_reset_on_unavailable(self):
        async def unavailable(request, body):
            return response({"code": "unavailable", "message": "offline"}, code=503)
        self.callback = unavailable
        task = self.client.tasks.handle("task")
        start = time.monotonic()
        with self.assertRaises(WaitTimeout) as exc:
            await asyncio.wait_for(task.result(timeout=1.15), 2.0)
        self.assertLess(time.monotonic() - start, 1.8)
        self.assertIs(exc.exception.task, task)
        self.assertIsInstance(exc.exception.last_error, Unavailable)
        self.assertIsNone(exc.exception.last_status)
        self.assertGreaterEqual(len(self.requests), 2)

    async def test_final_result_fetch_shares_wait_deadline(self):
        # Control observation time at the transport boundary: scheduler/HTTP
        # startup latency must not decide whether terminal status was observed.
        # Real transport timeout cancellation is covered separately below.
        for final_reply in ("timeout", "late_success"):
            with self.subTest(final_reply=final_reply):
                now = 1000.0
                exchanges = []
                clock = SimpleNamespace(time=lambda: now)
                timeout_error = RequestTimeout(dispatched=True)

                async def exchange(method, route, *, deadline, parser, **kwargs):
                    nonlocal now
                    exchanges.append((method, route, deadline, deadline - now))
                    if route.endswith("status"):
                        now += 6.0
                        return parser(status("succeeded"))
                    self.assertEqual(route, "/v1/tasks/result")
                    # The same ten-unit budget has only four units remaining.
                    self.assertEqual(deadline, 1010.0)
                    now = deadline
                    if final_reply == "timeout":
                        raise timeout_error
                    return parser(result())

                async def unexpected_sleep(delay):
                    self.fail("an exhausted observation budget must not sleep or retry")

                observation_time = SimpleNamespace(
                    get_running_loop=lambda: clock, sleep=unexpected_sleep)
                task = self.client.tasks.handle("task")
                with patch.object(tasks, "asyncio", observation_time), \
                        patch.object(self.client._transport, "exchange", exchange):
                    with self.assertRaises(WaitTimeout) as exc:
                        await task.result(timeout=10.0)
                self.assertIs(exc.exception.task, task)
                self.assertEqual(exc.exception.last_status.state, "succeeded")
                self.assertIs(exc.exception.last_error,
                              timeout_error if final_reply == "timeout" else None)
                self.assertEqual(exchanges, [
                    ("GET", "/v1/tasks/status", 1010.0, 10.0),
                    ("GET", "/v1/tasks/result", 1010.0, 4.0),
                ])

    async def test_known_predispatch_timeout_is_not_uncertain(self):
        client = await self.open_client(request_timeout=.01)
        prepare = client.tasks.prepare
        def slow_prepare(**kwargs):
            value = prepare(**kwargs)
            time.sleep(.03)
            return value
        with patch.object(client.tasks, "prepare", slow_prepare):
            with self.assertRaises(RequestTimeout) as exc: await client.tasks.submit(**self.args())
        self.assertFalse(exc.exception.dispatched)
        self.assertEqual(self.requests, [])

    async def test_async_cancellation_never_posts_remote_cancel(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        async def held(request, body):
            entered.set(); await release.wait(); return response(status("succeeded"))
        self.callback = held
        waiting = asyncio.create_task(self.client.tasks.handle("task").result())
        await asyncio.wait_for(entered.wait(), 1)
        waiting.cancel()
        with self.assertRaises(asyncio.CancelledError): await waiting
        release.set()
        self.assertEqual([r[0] for r in self.requests], ["GET"])

    async def test_waiting_task_does_not_occupy_network_admission(self):
        async def queued(request, body):
            return response(status("queued", request.query["task_id"]))
        self.callback = queued
        waiting = asyncio.create_task(self.client.tasks.handle("task").wait(.08))
        await asyncio.sleep(.01)
        start = time.monotonic()
        self.assertEqual((await self.client.tasks.handle("other").status()).task_id, "other")
        self.assertLess(time.monotonic() - start, .1)
        with self.assertRaises(WaitTimeout): await waiting

    async def test_closed_client_rejects_network_without_reopening(self):
        await self.client.close()
        with self.assertRaises(ClientClosed): await self.client.tasks.handle("task").status()
        with self.assertRaises(ClientClosed): await self.client.__aenter__()
        self.assertEqual(self.requests, [])

    async def test_wire_rejections(self):
        cases = [
            (b'{"x":1,"x":2}', 200, {"Content-Type": "application/json"}),
            (b'{}', 200, {"Content-Type": "text/html"}),
            (b'{}', 200, {"Content-Type": "application/json", "Content-Encoding": "gzip"}),
            (b'{"code":"not_found"}', 503, {"Content-Type": "application/json"}),
            (b'{"code":"new_error"}', 400, {"Content-Type": "application/json"}),
            (b'{}', 302, {"Content-Type": "application/json", "Location": self.url}),
        ]
        for raw, http, headers in cases:
            async def invalid(request, body): return web.Response(body=raw, status=http, headers=headers)
            self.callback = invalid
            with self.assertRaises(ProtocolError): await self.client.tasks.handle("task").status()

    async def test_error_size_independent_of_status_success_cap(self):
        async def error(request, body):
            return response({"code": "unavailable", "message": "x" * 17000}, code=503)
        self.callback = error
        with self.assertRaises(Unavailable): await self.client.tasks.handle("task").status()
        async def oversized(request, body):
            return response({"code": "unavailable", "message": "x" * 65536}, code=503)
        self.callback = oversized
        with self.assertRaises(ProtocolError): await self.client.tasks.handle("task").outcome()

    async def test_chunked_size_limit_and_slow_trickle(self):
        async def oversized(request, body):
            reply = web.StreamResponse(headers={"Content-Type": "application/json"})
            await reply.prepare(request)
            try:
                await reply.write(b'"' + b'x' * 17000); await reply.write_eof()
            except ConnectionError: pass
            return reply
        self.callback = oversized
        with self.assertRaises(ProtocolError): await self.client.tasks.handle("task").status()
        async def trickle(request, body):
            reply = web.StreamResponse(headers={"Content-Type": "application/json"})
            await reply.prepare(request)
            try:
                for part in (b'{', b'"task"', b':', b'null', b'}'):
                    await reply.write(part); await asyncio.sleep(.025)
                await reply.write_eof()
            except ConnectionError: pass
            return reply
        self.callback = trickle
        client = await self.open_client(request_timeout=.04)
        start = time.monotonic()
        with self.assertRaises(RequestTimeout) as exc: await client.tasks.handle("task").outcome()
        self.assertTrue(exc.exception.dispatched)
        self.assertLess(time.monotonic() - start, .15)

    async def test_codec_jobs_remain_bounded_after_waiter_timeout(self):
        client = await self.open_client()
        jobs = client._transport._codec
        loop = asyncio.get_running_loop()
        release = threading.Event()
        lock = threading.Lock()
        started = asyncio.Event()
        close_started = asyncio.Event()
        admissions = asyncio.Queue()
        timeouts = {}
        starts = 0
        requests = []
        original_decode = transport._decode_response
        original_timeout = asyncio.timeout_at
        original_run, original_close = jobs.run, jobs.close

        def blocked(*args):
            nonlocal starts
            with lock:
                starts += 1
                if starts == 2:
                    loop.call_soon_threadsafe(started.set)
            release.wait()
            return original_decode(*args)

        def controlled_timeout(deadline):
            operation = asyncio.current_task()
            if operation not in client._transport._operations or operation in timeouts:
                return original_timeout(deadline)
            # Exercise real timeout cancellation/mapping after the decoder has
            # started; HTTP and thread startup are not part of this assertion.
            timeout = original_timeout(None)
            timeouts[operation] = timeout
            return timeout

        async def observed_run(function):
            admissions.put_nowait(asyncio.current_task())
            return await original_run(function)

        async def observed_close():
            close_started.set()
            await original_close()

        def assert_owned_jobs():
            with lock:
                self.assertEqual(starts, 2)
            self.assertEqual(len(jobs._pending), 2)

        async def timed_out(request):
            with self.assertRaises(RequestTimeout) as caught:
                await asyncio.wait_for(request, 5)
            self.assertTrue(caught.exception.dispatched)

        with patch.object(transport, "_decode_response", blocked), \
                patch.object(transport.asyncio, "timeout_at", controlled_timeout), \
                patch.object(jobs, "run", observed_run), \
                patch.object(jobs, "close", observed_close):
            try:
                requests = [asyncio.create_task(client.tasks.handle("task").outcome()) for _ in range(2)]
                await asyncio.wait_for(started.wait(), 5)
                for _ in range(2):
                    operation = await asyncio.wait_for(admissions.get(), 5)
                    timeouts[operation].reschedule(loop.time())
                for request in requests:
                    await timed_out(request)
                assert_owned_jobs()

                # Later HTTP responses reach codec admission, but timed-out
                # waiters must not free the two still-running jobs' slots.
                for _ in range(10):
                    request = asyncio.create_task(client.tasks.handle("task").outcome())
                    requests.append(request)
                    operation = await asyncio.wait_for(admissions.get(), 5)
                    assert_owned_jobs()
                    timeouts[operation].reschedule(loop.time())
                    await timed_out(request)
                    assert_owned_jobs()
                self.assertEqual(len(self.requests), 12)

                closing = asyncio.create_task(client.close())
                await asyncio.wait_for(close_started.wait(), 5)
                self.assertFalse(closing.done())
                assert_owned_jobs()
                closing.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await closing
                self.assertFalse(client._transport._close_task.done())
                assert_owned_jobs()
                release.set()
                await asyncio.wait_for(client.close(), 5)
            finally:
                release.set()
                for request in requests:
                    if not request.done():
                        request.cancel()
                await asyncio.gather(*requests, return_exceptions=True)
                await asyncio.wait_for(client.close(), 5)
        self.assertEqual(len(jobs._pending), 0)

    async def test_dns_is_coalesced_after_repeated_cancelled_waits(self):
        client = AsyncClient("http://test.invalid:9", tenant="tenant", namespace="tests", request_timeout=.005)
        async with client:
            resolver = client._transport._session.connector._resolver
            self.assertIsInstance(resolver, aiohttp.ThreadedResolver)
            started = 0
            cancelled = asyncio.Event()
            async def resolve(*args, **kwargs):
                nonlocal started
                started += 1
                try: await asyncio.Future()
                finally: cancelled.set()
            with patch.object(resolver, "resolve", resolve):
                for _ in range(12):
                    with self.assertRaises(RequestTimeout): await client.tasks.handle("task").outcome()
                self.assertEqual(started, 1)
                await client.close()
                await asyncio.wait_for(cancelled.wait(), 1)

    async def test_late_synchronous_decoder_cannot_return_success(self):
        client = await self.open_client(request_timeout=.1)
        async def late(function):
            value = function()
            time.sleep(.12)
            return value
        with patch.object(client._transport._codec, "run", late):
            with self.assertRaises(RequestTimeout): await client.tasks.handle("task").outcome()

    async def test_late_trace_completion_cannot_return_success(self):
        from contextlib import contextmanager
        from ledgence.client import otel
        client = await self.open_client(request_timeout=.1)
        @contextmanager
        def late(*args, **kwargs):
            yield {}, None
            time.sleep(.12)
        with patch.object(otel, "_exchange", late):
            with self.assertRaises(RequestTimeout) as exc: await client.tasks.handle("task").outcome()
        self.assertEqual(exc.exception.request_id, "request")

    async def test_aiohttp_get_reconnect_does_not_replay_post(self):
        methods = []
        async def disconnected(reader, writer):
            try:
                raw = await reader.readuntil(b'\r\n\r\n')
                methods.append(raw.split(b' ', 1)[0].decode())
            finally:
                writer.close()
                await writer.wait_closed()
        server = await asyncio.start_server(disconnected, "127.0.0.1", 0)
        async with server:
            client = AsyncClient(f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}",
                                 tenant="tenant", namespace="tests")
            async with client:
                with self.assertRaises(TransportError): await client.tasks.handle("task").status()
                self.assertEqual(methods, ["GET", "GET"])
                with self.assertRaises(SubmissionUncertain): await client.tasks.submit(**self.args())
                self.assertEqual(methods, ["GET", "GET", "POST"])

    async def test_close_after_post_acceptance_preserves_uncertainty(self):
        for operation in ("submit", "cancel"):
            client = await self.open_client()
            entered = asyncio.Event(); release = asyncio.Event()
            async def accepted(request, body):
                entered.set(); await release.wait()
                return response(submitted(json.loads(body)) if operation == "submit" else "active")
            self.callback = accepted
            frozen = client.tasks.prepare(**self.args())
            task = client.tasks.handle("task")
            pending = asyncio.create_task(client.tasks.submit(frozen) if operation == "submit" else task.cancel())
            try:
                await asyncio.wait_for(entered.wait(), 1)
                await client.close()
                with self.assertRaises(SubmissionUncertain if operation == "submit" else CancellationUncertain) as exc:
                    await pending
                if operation == "submit": self.assertIs(exc.exception.submission, frozen)
                else: self.assertIs(exc.exception.task, task)
                self.assertIsInstance(exc.exception.cause, TransportError)
            finally:
                release.set()

    async def test_close_before_dispatch_does_not_invent_uncertainty(self):
        client = await self.open_client()
        transport = client._transport
        for _ in range(8): await transport._slots.acquire()
        pending = asyncio.create_task(client.tasks.submit(**self.args()))
        await asyncio.sleep(.01)
        await client.close()
        with self.assertRaises(ClientClosed): await pending
        self.assertEqual(self.requests, [])

    async def test_handle_equality_includes_endpoint_and_scope(self):
        one = self.client.tasks.handle("task")
        same = AsyncClient(self.url + "/", tenant="tenant", namespace="tests").tasks.handle("task")
        tenant = AsyncClient(self.url, tenant="other", namespace="tests").tasks.handle("task")
        endpoint = AsyncClient("http://localhost:9", tenant="tenant", namespace="tests").tasks.handle("task")
        self.assertEqual(one, same)
        self.assertEqual(len({one, same, tenant, endpoint}), 3)
        self.assertNotEqual(one, tenant)
        self.assertNotEqual(one, endpoint)

    async def test_terminal_knowledge_is_retained_when_result_unavailable(self):
        status_calls = 0
        async def unavailable_result(request, body):
            nonlocal status_calls
            if request.path.endswith("status"):
                status_calls += 1
                return response(status("succeeded" if status_calls == 1 else "queued"))
            return response({"code": "unavailable", "message": "temporary"}, code=503)
        self.callback = unavailable_result
        with self.assertRaises(WaitTimeout) as exc:
            await asyncio.wait_for(self.client.tasks.handle("task").wait(1.1), 2)
        self.assertEqual(exc.exception.last_status.state, "succeeded")
        self.assertEqual(status_calls, 1)
        self.assertGreaterEqual(sum(r[1].endswith("result") for r in self.requests), 2)

    async def test_retried_result_must_match_terminal_metadata(self):
        result_calls = 0
        async def changed_result(request, body):
            nonlocal result_calls
            if request.path.endswith("status"): return response(status("succeeded"))
            result_calls += 1
            if result_calls == 1:
                return response({"code": "unavailable", "message": "temporary"}, code=503)
            value = result(output="wrong generation")
            value["task"].update(attempt_count=2, latest_attempt_id="other")
            value["outcome"]["attempt_id"] = "other"
            return response(value)
        self.callback = changed_result
        with self.assertRaises(ProtocolError):
            await asyncio.wait_for(self.client.tasks.handle("task").wait(1.5), 2)
        self.assertEqual(result_calls, 2)
        self.assertEqual(sum(r[1].endswith("status") for r in self.requests), 1)


class EndpointTests(unittest.TestCase):
    def client(self, url):
        return AsyncClient(url, tenant="tenant", namespace="tests")

    def test_unicode_host_matches_transport_and_frozen_command_identity(self):
        for unicode_host, ascii_host in (
            ("faß.de", "xn--fa-hia.de"),
            ("ς.gr", "xn--3xa.gr"),
            ("βόλος.gr", "xn--nxasmm1c.gr"),
            ("BÜCHER.de", "xn--bcher-kva.de"),
        ):
            with self.subTest(host=unicode_host):
                unicode_client = self.client(f"https://{unicode_host}:443/api/")
                ascii_client = self.client(f"https://{ascii_host}/api")
                self.assertEqual(unicode_client.base_url, ascii_client.base_url)
                args = dict(program="echo", version="1.0.0", queue="queue", data=None,
                            idempotency_key="submit")
                self.assertEqual(unicode_client.tasks.prepare(**args),
                                 ascii_client.tasks.prepare(**args))
                self.assertEqual(unicode_client.tasks.handle("task"),
                                 ascii_client.tasks.handle("task"))
        self.assertNotEqual(self.client("https://faß.de").base_url,
                            self.client("https://fass.de").base_url)

    def test_normalization_preserves_path_ports_and_ascii_hosts(self):
        for supplied, expected in (
            ("HTTP://Example.COM:80/api///", "http://example.com/api"),
            ("https://BÜCHER.de:8443/café/%2f/", "https://xn--bcher-kva.de:8443/café/%2f"),
            ("https://[2001:0db8::1]:443/path/%2F/", "https://[2001:0db8::1]/path/%2F"),
            ("http://[fe80::1%25en0]:8080/api/", "http://[fe80::1%25en0]:8080/api"),
            ("http://service_name:80/api", "http://service_name/api"),
        ):
            with self.subTest(url=supplied):
                self.assertEqual(self.client(supplied).base_url, expected)

    def test_invalid_endpoint_validation_is_preserved(self):
        for url in ("", "//example.com", "ftp://example.com", "https://user@example.com",
                    "https://:password@example.com", "https://example.com/?q=x",
                    "https://example.com/#fragment", " https://example.com",
                    "https://example.com/space here", "https://example.com:65536",
                    "https://[not-ipv6]", "https://\ud800.example"):
            with self.subTest(url=url):
                with self.assertRaises(InputError):
                    self.client(url)
