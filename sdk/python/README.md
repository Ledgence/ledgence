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

Expose a callable as `module:function`; protocols 1/2 require a synchronous
handler, while protocol 3 also supports `async def`. Its module and package
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
integers, non-finite numbers, unawaited values, and oversized results produce typed
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
Protocol 2 adds contextual logs; protocol 3 also enables asynchronous handlers and
workflow control exchanges. The ready version must match the manifest; a mismatch
fails before calling the handler. Protocols 2/3 pass an invocation-local
`processing_context` outside the complete, unchanged CloudEvent. It is null when
worker tracing is disabled. It never replaces the event's origin `traceparent`.

`current_invocation()` provides `event_id`, `attempt_id`, `source`, `tenant_id`,
`namespace`, `run_id`, `task_id`, `attempt_no`, and the optional frozen W3C
`processing_context`. Optional `workflow_id` and `activation_id` come from envelope
extensions. Nested workflow invocations also expose `parent_workflow_id` and
`root_workflow_id`; root workflow invocations expose a null parent and their own
workflow ID as the root. Ordinary tasks outside workflows have neither. Protocol 3
logs include the paired ancestor IDs only for nested workflows, preserving older
root log shapes. These identifiers never enter user data. Context is reset on
success, business exception, and invalid
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

One dedicated writer in protocols 2/3 owns ready, result, log and closing frames. Its optional
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

## Explicit checkpoint workflows (protocol 3)

Workflow programs use `from ledgence_worker.workflow import workflow_context` and
return `ctx.suspend(...)`, `ctx.continue_(...)`, `ctx.wait_event(...)`,
`ctx.sleep(...)`, `ctx.complete(output)`, or `ctx.fail(kind, message)`. `ctx.continuation` starts as `"start"`; `ctx.state` is
explicit JSON state and `ctx.inputs` is the frozen batch of child outcomes.
The complete CloudEvent, including user-owned `data`, remains the handler argument.

`await ctx.local(key, fn, **json_kwargs)` executes in the current process and
returns only after the Rust owner acknowledges durable storage of the result.
Calls sharing an activation-local key and the same callable/input reuse the result.
The callable does application work and may make ordinary nested Python calls.
It cannot call workflow control APIs, start another journaled local step, or stage
distributed tasks or subworkflows; the controller does those after awaiting its result. Otherwise
replaying the cached result would skip those workflow operations. This boundary
also applies through asynchronous child tasks and `asyncio.to_thread`.
Pass all changing inputs explicitly: closure variables and process memory are not
part of the persisted binding. Ordinary calls have no durable result record. An
external effect can still repeat after a crash before its commit acknowledgement;
use an external idempotency key for effects that require deduplication.

`ctx.local(...)` starts an owned operation immediately and returns an awaitable.
Use `await ctx.gather(ctx.local(...), ctx.local(...))` to overlap asynchronous I/O.
A synchronous callable retains its normal blocking semantics. The activation
retains ownership of started local operations and drains them before returning a
checkpoint, even when an observer stops awaiting one. An unobserved local failure
fails the activation; explicitly caught local errors remain under controller control.
A failed commit cannot be
caught and converted into successful workflow completion.

`ctx.task(key, program=..., version=..., queue=..., data=...)` stages a distributed
child command. The following checkpoint atomically registers those commands;
calling `task()` alone makes no network call. Child keys belong to the whole
workflow, and an identical binding reuses the original child. Use iteration
suffixes such as `"invoice:3"` to request a new child in a loop. A changed binding
for an existing key conflicts. `ctx.suspend(continuation=..., state=..., until=[ref])`
waits until all listed children are terminal; the next activation can read
`ctx.get_result(ref_or_key)` for successful output or inspect `ctx.inputs` for
failures. String keys can also refer to children from earlier activations.

`ctx.workflow(key, program=..., version=..., queue=..., data=...)` stages an owned
subworkflow with the same retry and attempt-timeout options as `ctx.task`. Both
methods return references that are not awaitable. Return a checkpoint to dispatch
work, mixing both reference kinds in `until` when needed:

```python
child = ctx.workflow("invoice-flow", program="invoice-flow", version="1.0.0",
                     queue="billing", data=event["data"])
summary = ctx.task("summary", program="summary", version="1.0.0",
                   queue="billing", data=event["data"])
return ctx.suspend(continuation="collect", state=None, until=[child, summary])
```

Tasks and workflows share the parent-wide key namespace. Reusing a key with a
different kind conflicts, as does changing its program, input, or scheduling
options. A child workflow wakes the parent only when the whole child workflow is
terminal. A completed controller activation by itself does not resolve the wait.
On resume, use the original string keys with `ctx.get_result(...)`, or inspect
`ctx.inputs[key]`: task results retain `task_id`, while workflow results have
`kind="workflow"` and `workflow_id`. Failed or cancelled children are inspectable
outcomes, and `get_result` raises for them.

Parent cancellation or failure drains owned descendants before becoming terminal.
Completion with unfinished children is rejected. A decision can stage at most 64
combined child commands and wait on at most 64 children. Each parent may have at
most 64 live subworkflows; nested depth is capped at 16 (root depth is zero).
See [`docs/subworkflows.md`](../../docs/subworkflows.md) for the lifecycle,
lineage, limits, and upgrade requirements.

External events and durable timers also use explicit checkpoint decisions:

```python
from ledgence_worker.workflow import workflow_context

def handle(event):
    ctx = workflow_context()
    if ctx.continuation == "start":
        return ctx.wait_event(
            "approval:1", continuation="approved", state={}, timeout_ms=60_000,
        )
    wake = ctx.wake
    if wake["kind"] == "event":
        return ctx.complete(wake["event"]["data"])
    return ctx.fail("approval_timeout", "No approval arrived before the deadline")
```

`ctx.wake` is `None` initially and when no external wait resumed the activation.
An event wake is `{"kind": "event", "key": ..., "event": <full CloudEvent>,
"accepted_at": <milliseconds>}`. Event timeouts carry `{"kind": "timeout",
"key": ..., "deadline": <milliseconds>}`; timers use `kind="timer"` with the same
key/deadline fields. Returned wake/state/input values are independent JSON copies.
`ctx.inputs` continues to contain only child outcomes. Event `data` and its original
context envelope are preserved separately from the controller invocation event.

Return `ctx.sleep("retry:1", 5_000, continuation="retry", state={...})` to register
a durable timer. Both helpers stage the existing child commands in the same
checkpoint and end this activation; the Rust orchestrator owns the persisted wait
and later activation. They do not call `asyncio.sleep` or hold a worker slot until
the deadline. Timeout/delay values are integer milliseconds from zero through
31,536,000,000 (365 days). `timeout_ms=None` waits for an event without a deadline.
Timer deadlines are persisted by the orchestrator when the checkpoint is applied.

Wait keys are one-shot across the workflow run, including later activations.
Use a fresh key such as `"retry:2"` for the next loop iteration. Reusing a closed
key does not start another wait. Events can be accepted before their wait is
registered. An event wake carries at most 64 KiB of complete encoded CloudEvent;
child inputs plus wake share a 256 KiB encoded budget, and the entire activation
context retains its 640 KiB budget. Application event data retains depth64 while
workflow envelope metadata has separate room.

External event IDs are limited to 128 UTF-8 bytes and sources to 2,048 UTF-8
bytes, in addition to the complete event budget. Sources must be valid URI
references; encode non-ASCII URI characters with percent escapes.

Rust and Python may spell the same finite float differently. Reading a context
already accepted by Rust therefore allows a bounded encoding expansion: for each
float, at most `max(0, len(repr(value)) - 3)` additional bytes. Rust's canonical
float tokens have at least three bytes; CPython JSON uses that float representation
(and the helper bounds it at 32 bytes). Existing traversal, string, node and depth
checks still apply. The allowance covers received state, child inputs, event
wakes, journal records, copied getters and exact committed-step replay.

New decisions, child commands, local inputs/results and added journal entries
retain the strict Python encoding limits. Copying a near-boundary received value
into a new write, or adding to a ledger whose Python encoding expanded, can be
rejected conservatively even when Rust's representation would fit. Pure reading
and exact committed replay do not spend a new-write budget.

Each protocol 3 invocation uses a fresh event loop inside the reused Python
process. Create and close loop-bound clients inside the handler, rather than
saving them in module globals. Started asynchronous work is drained or cancelled
before the process is reused. While local operations run, the activation holds
one consumer/process slot; a committed suspension releases it for other tasks.

Unexpected controller exceptions are retryable activation runtime failures.
`ctx.fail(...)` explicitly requests workflow failure. Ordinary task output is
never interpreted as a workflow decision, even when it contains a `kind` field.

See [`docs/workflows.md`](../../docs/workflows.md) for the supported first slice,
checkpoint limits, cancellation, and recovery semantics.
