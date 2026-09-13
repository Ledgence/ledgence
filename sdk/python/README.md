# Ledgence Python helper

MIT-licensed, standard-library-only program support. The Rust worker starts the
configured CPython executable with `-I -S -B` and this directory's bootstrap. Python
is a separately installed runtime; Ledgence does not embed or download CPython.
Programs declare an exact supported Python major/minor (at least 3.11), with code
and vendored dependencies in their immutable artifact directory.

The worker supplies the `ledgence_worker` context helper alongside the runner;
applications do not need to vendor that helper. Application dependencies belong
in the program artifact. The bootstrap loads its own helper first, then adds the
absolute artifact directory to Python's import path. Bytecode generation is
explicitly disabled, so ordinary imports do not add `__pycache__` directories to
an artifact even when filesystem permissions would allow it.

The worker sets a separate temporary working directory for each session. Relative
writes go there and persist across that session's invocations. Confirmed session
cleanup removes this directory. Read packaged resources relative to the program
module's `__file__`, not the working directory. Directory separation is file
lifecycle management and does not change OS access permissions.

Expose a synchronous callable as `module:function`. Its module and package
parents must originate inside the artifact. Names already loaded by the bootstrap
(such as `json`, `os`, and `ledgence_worker`) cannot be handler modules; conflicts
are rejected before readiness. Use an application-specific module name. Regular
and namespace packages are supported, and application code may still import the
standard library normally.

The callable receives the complete
CloudEvent dictionary and returns a JSON-compatible value. `current_invocation()`
provides transport execution identifiers when needed. The helper starts a fresh
`contextvars.Context` for every invocation; process/module globals intentionally
persist. Programs must clean up other invocation state and background work.

One process handles one invocation at a time. Results support null, booleans,
Unicode scalar strings, integers from `-2**63` through `2**64 - 1`, finite binary64
floating-point values, arrays, and objects with string keys. Python tuples are
encoded as JSON arrays. At most 64 nested containers are permitted; a scalar has
depth zero. Cycles, non-string keys, lone surrogate characters, out-of-range
integers, non-finite numbers, coroutines, and oversized results produce typed
`invalid_output` failures and leave the process reusable. Rust checks the shared
wire profile when accepting the response.

The default protocol frame limit is 2 MiB, including result/input envelopes and
the newline. The worker supplies the configured limits explicitly when starting
the helper. The delivery submission limit remains 1 MiB of application data;
the additional frame capacity carries CloudEvent metadata and protocol fields.

Business exceptions produce typed failure results. Error text is shortened by the
complete frame's encoded UTF-8 byte budget, including event and attempt IDs, JSON
escaping, and the newline. The handler is not called if those IDs cannot fit even
an empty failure response. A protocol
fault terminates the process. Python and native stdout writes are redirected to
stderr; an isolated descriptor carries bounded JSON-lines responses. Parent-side
log capture must continue draining stderr even after its retention limit.

This process boundary executes trusted code with the worker's OS permissions.
The helper is not a sandbox and cannot undo external effects on retries.

## Protocol versions and contextual logs

Existing `runtime.protocol: 1` packages keep the original invoke/result protocol.
New packages should select protocol 2. The ready version must match the manifest;
a mismatch fails before calling the handler. Protocol 2 passes an invocation-local
`processing_context` outside the complete, unchanged CloudEvent. It is null when
worker tracing is disabled. It never replaces the event's origin `traceparent`.

`current_invocation()` provides `event_id`, `attempt_id`, `source`, `tenant_id`,
`namespace`, `run_id`, `task_id`, `attempt_no`, and the optional frozen W3C
`processing_context`. Context is reset on success, business exception, and invalid
output. No invocation context is written to process-global environment variables.

```python
from ledgence_worker import current_invocation, get_logger

log = get_logger(__name__)

def handle(event):
    log.info("Invoice issued", extra={"attributes": {"invoice.id": event["data"]["invoice_id"]}})
    return {"task_id": current_invocation().task_id}
```

`get_logger` returns a normal `logging.Logger`, adding one Ledgence handler while
preserving existing handlers and root configuration. An unset logger level becomes
INFO. User attributes occupy a separate map. A record snapshots correlation and
attributes synchronously at emission; intentionally copied old contexts retain
their original IDs even if a background thread logs during a later invocation.
Outside an invocation, records carry process identity only unless the application
explicitly activates its own OTel span. Raw stdout/stderr remains process-level.

One dedicated v2 writer owns ready, result, log and closing frames. Its optional
queue is bounded to 64 records and 1 MiB; each log frame is at most 16 KiB or the
configured output limit, including the newline. Encoding bounds the complete
attribute tree (64 nodes, four nested levels, shared text budget). Oversized
optional content is shortened; a record whose correlation cannot fit is dropped.
Telemetry encoding errors and queue overflow drop logs and increment a saturating
local counter; they do not replace a handler result. This Python-side count stays
in the process: it is not carried in result or closing frames, durable task
history, or a collector export. Worker-side optional-record validation and output
budget drops have separate bounded diagnostics. Results/control have a
reserved priority slot. At most one already-writing bounded log precedes a newly
queued result; IPC itself still has backpressure. Closing discards remaining
optional records. Telemetry is best effort and may be lost on shutdown or crash.

## Optional OpenTelemetry API bridge

Programs may vendor `opentelemetry-api` and call
`from ledgence_worker.otel import enable_context; enable_context()` once at module
initialization. The bridge activates only the invocation's processing carrier
using the fixed W3C propagator, and attaches an empty context when it is null.
It resets the OTel context in `finally`. It neither installs a provider nor creates
a duplicate span for the worker's execution span. Logging inside application
child spans captures those children's active trace/span IDs.

The default helper imports no OpenTelemetry dependency. To record custom spans,
the application supplies and owns its SDK/provider and dependencies, with their
legal notices. Use bounded asynchronous processors for any application exporter.
Ledgence does not bundle a Python network exporter or serialize Python spans over
the result channel. The worker exports its own execution spans independently.

`register_shutdown(provider.shutdown)` optionally registers an application-owned
callback. Each registered callback runs once before the graceful closing ACK;
exceptions are reported on stderr and other callbacks continue. The parent's
existing process shutdown deadline bounds callbacks, including a hanging exporter.
Forced retirement/crashes do not promise a callback or complete flush. There is no
per-invocation flush or reliance on `atexit`.

The default tests require only the standard library. Optional API/SDK parentage
tests accept an already prepared dependency directory and make no network calls:

```sh
LEDGENCE_PYTHON_OTEL_TEST_PACKAGES=/path/to/reviewed-api-sdk-packages \
  python3 -m unittest discover -s sdk/python/tests -v
```

The reviewed test set is `opentelemetry-api==1.44.0`,
`opentelemetry-sdk==1.44.0`, `opentelemetry-semantic-conventions==0.65b0`, and
`typing_extensions==4.16.0`. The test exporter is in memory. Tests copy prepared
packages into the temporary artifact to exercise the same isolated import path as
real programs; nothing is installed at execution time.
