"""Explicit API-only OpenTelemetry context bridge (MIT).

The application supplies opentelemetry-api and owns any SDK/exporter. Importing
this module alone neither imports OpenTelemetry nor configures its provider.
"""

from contextlib import contextmanager

_api = None


def enable_context():
    """Enable fixed W3C propagation using the application's packaged OTel API."""
    global _api
    if _api is None:
        from opentelemetry import context, trace
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

        _api = (context, trace, TraceContextTextMapPropagator())


@contextmanager
def _activate(carrier):
    if _api is None:
        yield
        return
    context, _, propagator = _api
    # A new Context explicitly excludes initialization/previous-invocation state.
    extracted = context.Context()
    if carrier is not None:
        values = {"traceparent": carrier.traceparent}
        if carrier.tracestate is not None:
            values["tracestate"] = carrier.tracestate
        extracted = propagator.extract(values, context=extracted)
    token = context.attach(extracted)
    try:
        yield
    finally:
        context.detach(token)


def _active_ids():
    if _api is None:
        return None
    span = _api[1].get_current_span().get_span_context()
    if not span.is_valid:
        return None
    return {"trace_id": format(span.trace_id, "032x"),
            "span_id": format(span.span_id, "016x")}
