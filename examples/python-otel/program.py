"""Optional application spans with a local in-memory exporter (MIT)."""

from ledgence_worker import current_invocation, get_logger, register_shutdown
from ledgence_worker.otel import enable_context
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

# The application owns this provider. Ledgence's helper never installs one.
exporter = InMemorySpanExporter()
provider = TracerProvider(shutdown_on_exit=False)
provider.add_span_processor(SimpleSpanProcessor(exporter))
trace.set_tracer_provider(provider)
enable_context()
register_shutdown(provider.shutdown)
tracer = trace.get_tracer(__name__)
log = get_logger(__name__)


def handle(event):
    invocation = current_invocation()
    try:
        with tracer.start_as_current_span("example.application"):
            log.info("Handling example", extra={"attributes": {"example": "in-memory"}})
            result = {"task_id": invocation.task_id, "input": event["data"]}
        finished = exporter.get_finished_spans()
        if invocation.processing_context is not None and finished:
            expected_parent = int(invocation.processing_context.traceparent.split("-")[2], 16)
            assert finished[-1].parent.span_id == expected_parent
        return result
    finally:
        # A reused process must not retain an ever-growing in-memory span history.
        exporter.clear()
