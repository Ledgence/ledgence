import asyncio
from contextlib import contextmanager
import json
import unittest
from unittest.mock import patch

from aiohttp import web
from ledgence.client import AsyncClient, TraceContext
from ledgence.client import otel
from support import response, submitted

try:
    from opentelemetry import trace
    AVAILABLE = True
except ImportError:
    AVAILABLE = False


@unittest.skipUnless(AVAILABLE, "optional opentelemetry-api is not installed")
class TracingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.previous_api = otel._api
        otel.enable_context()
        self.received = []
        self.reply_override = None
        self.reply_delay = 0
        async def handle(request):
            command = await request.json()
            self.received.append((command, dict(request.headers)))
            if self.reply_delay:
                await asyncio.sleep(self.reply_delay)
            if self.reply_override is not None:
                return self.reply_override()
            return response(submitted(command))
        app = web.Application(); app.router.add_post("/v1/tasks", handle)
        runner = web.AppRunner(app); await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0); await site.start()
        self.addAsyncCleanup(runner.cleanup)
        self.client = AsyncClient(f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}",
                                  tenant="tenant", namespace="tests")
        await self.client.__aenter__(); self.addAsyncCleanup(self.client.close)

    async def asyncTearDown(self):
        otel._api = self.previous_api

    def active(self, digit, sampled=True):
        context = trace.SpanContext(int(digit * 32, 16), int(digit * 16, 16), False,
                                    trace.TraceFlags(1 if sampled else 0), trace.TraceState())
        return trace.use_span(trace.NonRecordingSpan(context))

    def args(self):
        return dict(program="echo", version="1", queue="queue", data={"private": "payload"},
                    idempotency_key="private-key")

    async def test_prepared_origin_and_absence_survive_send_contexts(self):
        with self.active("a", sampled=False):
            frozen = self.client.tasks.prepare(**self.args())
        absent = self.client.tasks.prepare(**self.args())
        for digit in ("b", "c"):
            with self.active(digit):
                await self.client.tasks.submit(frozen)
                await self.client.tasks.submit(absent)
        for command, _ in self.received[::2]:
            self.assertEqual(command["origin_trace"]["traceparent"],
                             "00-" + "a" * 32 + "-" + "a" * 16 + "-00")
        for command, _ in self.received[1::2]: self.assertNotIn("origin_trace", command)

    async def test_explicit_origin_and_explicit_absence_win(self):
        explicit = TraceContext("00-" + "d" * 32 + "-" + "e" * 16 + "-01", "vendor=one")
        with self.active("a"):
            await self.client.tasks.submit(**self.args(), origin_trace=explicit)
            await self.client.tasks.submit(**self.args(), origin_trace=None)
        self.assertEqual(self.received[0][0]["origin_trace"], explicit.to_dict())
        self.assertNotIn("origin_trace", self.received[1][0])

    async def test_http_spans_use_current_context_without_private_attributes(self):
        spans = []
        class Span(trace.Span):
            def __init__(self, context, attributes):
                self.context = context; self.attributes = dict(attributes); self.ended = False
            def get_span_context(self): return self.context
            def set_attribute(self, key, value): self.attributes[key] = value
            def set_attributes(self, attributes): self.attributes.update(attributes)
            def add_event(self, *args, **kwargs): pass
            def set_status(self, *args, **kwargs): pass
            def update_name(self, *args, **kwargs): pass
            def record_exception(self, *args, **kwargs): pass
            def is_recording(self): return True
            def end(self, end_time=None): self.ended = True
        class Tracer:
            @contextmanager
            def start_as_current_span(self, name, **options):
                parent = trace.get_current_span().get_span_context()
                context = trace.SpanContext(parent.trace_id, len(spans) + 1, False,
                                            parent.trace_flags, parent.trace_state)
                span = Span(context, options["attributes"]); spans.append(span)
                with trace.use_span(span, end_on_exit=True): yield span
        with self.active("a"):
            frozen = self.client.tasks.prepare(**self.args())
        with patch.object(trace, "get_tracer", return_value=Tracer()):
            for digit in ("b", "c"):
                with self.active(digit): await self.client.tasks.submit(frozen)
        self.assertEqual(len(spans), 2)
        for index, digit in enumerate(("b", "c")):
            body, headers = self.received[index]
            self.assertEqual(body["origin_trace"]["traceparent"][3:35], "a" * 32)
            self.assertEqual(headers["traceparent"][3:35], digit * 32)
            self.assertEqual(headers["traceparent"][36:52], f"{index + 1:016x}")
            self.assertTrue(spans[index].ended)
            self.assertEqual(set(spans[index].attributes), {
                "http.request.method", "ledgence.operation", "http.response.status_code", "ledgence.request.id"})
            self.assertNotIn("private", json.dumps(spans[index].attributes))

    async def test_optional_provider_failure_does_not_replace_submission(self):
        with patch.object(trace, "get_tracer", side_effect=RuntimeError("broken provider")):
            task = await self.client.tasks.submit(**self.args())
        self.assertEqual(task.id, "task")
        @contextmanager
        def broken_span(*args, **kwargs):
            class Span:
                def set_attribute(self, *args): raise RuntimeError("broken attributes")
            yield Span()
            raise RuntimeError("broken flush")
        class Tracer:
            start_as_current_span = broken_span
        with patch.object(trace, "get_tracer", return_value=Tracer()):
            self.assertEqual((await self.client.tasks.submit(**self.args())).id, "task")

    async def test_failed_exchange_spans_have_bounded_error_status(self):
        from ledgence.client import SubmissionUncertain
        class Span:
            def __init__(self): self.attributes = {}; self.status = None
            def set_attribute(self, key, value): self.attributes[key] = value
            def set_status(self, value): self.status = value
        span = Span()
        class Tracer:
            @contextmanager
            def start_as_current_span(self, name, **kwargs):
                span.attributes.update(kwargs["attributes"])
                yield span
        cases = [
            (lambda: response({"code": "unavailable", "message": "private-diagnostic"}, code=503), 0, "http_503"),
            (lambda: web.Response(body=b'not json', headers={"Content-Type": "application/json"}), 0, "protocol"),
            (None, .1, "timeout"),
        ]
        async with AsyncClient(self.client.base_url, tenant="tenant", namespace="tests", request_timeout=.025) as client:
            with patch.object(trace, "get_tracer", return_value=Tracer()), self.active("a"):
                for reply, delay, kind in cases:
                    span = Span()
                    self.reply_override = reply; self.reply_delay = delay
                    with self.assertRaises(SubmissionUncertain): await client.tasks.submit(**self.args())
                    self.assertEqual(span.attributes["error.type"], kind)
                    self.assertEqual(span.status.status_code, trace.StatusCode.ERROR)
                    self.assertNotIn("private", json.dumps(span.attributes))
