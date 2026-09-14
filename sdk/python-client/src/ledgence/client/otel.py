"""Explicit, API-only OpenTelemetry integration; applications own their provider."""
from __future__ import annotations

from contextlib import contextmanager
import asyncio

from .errors import ProtocolError, RequestTimeout, ServiceError, Unavailable

from .models import TraceContext

_api = None


def enable_context() -> None:
    """Enable context capture and HTTP spans without installing an SDK/exporter."""
    global _api
    if _api is None:
        from opentelemetry import trace
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
        _api = (trace, TraceContextTextMapPropagator())


def _capture() -> TraceContext | None:
    if _api is None:
        return None
    try:
        trace, propagator = _api
        if not trace.get_current_span().get_span_context().is_valid:
            return None
        carrier = {}
        propagator.inject(carrier)
        return TraceContext(carrier["traceparent"], carrier.get("tracestate"))
    except Exception:
        # Optional application instrumentation cannot reject a valid command.
        return None


class _SafeSpan:
    def __init__(self, span, trace):
        self._span = span
        self._trace = trace
        self._failed = False

    def set_attribute(self, name, value):
        try:
            self._span.set_attribute(name, value)
        except Exception:
            pass

    def record_error(self, kind: str):
        if self._failed:
            return
        self._failed = True
        self.set_attribute("error.type", kind)
        try:
            self._span.set_status(self._trace.Status(self._trace.StatusCode.ERROR))
        except Exception:
            pass


@contextmanager
def _exchange(method: str, operation: str, *, deadline: float | None = None):
    if _api is None:
        yield {}, None
        return
    manager = None
    entered = False
    headers = {}
    span = None
    try:
        trace, propagator = _api
        tracer = trace.get_tracer("ledgence.client", "0.1.0")
        manager = tracer.start_as_current_span(
            f"ledgence.http.{operation}", kind=trace.SpanKind.CLIENT,
            attributes={"http.request.method": method, "ledgence.operation": operation},
            record_exception=False, set_status_on_exception=False,
        )
        raw_span = manager.__enter__()
        entered = True
        carrier = {}
        propagator.inject(carrier)
        if "traceparent" in carrier:
            headers = TraceContext(carrier["traceparent"], carrier.get("tracestate")).to_dict()
        span = _SafeSpan(raw_span, trace)
    except Exception:
        # No payload/exception data is emitted to recover failed instrumentation.
        pass
    try:
        yield headers, span
    except BaseException as error:
        if span is not None:
            expired = deadline is not None and asyncio.get_running_loop().time() >= deadline
            if isinstance(error, (RequestTimeout, TimeoutError)) or expired:
                kind = "timeout"
            elif isinstance(error, asyncio.CancelledError):
                kind = "cancelled"
            elif isinstance(error, ProtocolError):
                kind = "protocol"
            elif isinstance(error, Unavailable):
                kind = "unavailable"
            elif isinstance(error, ServiceError):
                kind = "service_error"
            else:
                kind = "transport"
            span.record_error(kind)
        raise
    finally:
        if span is not None and deadline is not None and asyncio.get_running_loop().time() >= deadline:
            span.record_error("timeout")
        if entered:
            try:
                # Client exception text may contain caller-controlled content.
                manager.__exit__(None, None, None)
            except Exception:
                pass
