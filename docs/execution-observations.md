# Execution observations

Execution observations describe work performed by a runtime. They supplement the
durable task and workflow records used by Console; they do not authorize a
decision, settle a task, or determine which local results a workflow replays.

The bundled Python helper collects a bounded in-memory observation for each
invocation. The Rust worker attaches valid observations to its execution report.
Once the orchestrator accepts that report, a compact projection can be inspected
without reading application input, output, lease authority, or the full report.
There is no external telemetry service requirement.

## Timing and local operations

`runtime_started_at_ms` is the runtime host's wall-clock timestamp immediately
before invoking application code. `runtime_elapsed_us` uses a monotonic clock and
excludes package preparation and process startup. It includes the invocation's
local work, asynchronous waits, and durable local-result acknowledgments. CPU time
is a different measurement. Do not infer cross-host ordering from wall clocks.

Local observations identify the activation's existing step key and callable. They
contain start time, elapsed microseconds, and one of these outcomes:

| Outcome | Evidence |
| --- | --- |
| `returned` | The callable returned a valid value. Its result may not yet have been durably accepted. |
| `failed` | The callable or validation of its returned value raised an exception. |
| `cancelled` | The local operation was cancelled. |
| `replayed` | A previously accepted result was reused without calling the function again. |

The accepted local-result journal remains the authority for successful durable
results. A returned observation cannot substitute for that journal. Replaying a
step preserves its logical identity and does not create another business node.
Concurrent local operations share the same process; their durations can overlap.
Local measurements are not separate scheduled tasks and have no independent
cancellation action.

These observations become available after report acceptance. They are not a live
stream of currently running local functions. An abrupt process exit, timeout, lost
report, older helper, or a result frame without enough remaining space can leave
measurements unavailable. Missing evidence must never be displayed as zero usage,
an empty successful execution, or proof that a local operation was never reached.

## Resource scope

| Measurement | Scope |
| --- | --- |
| `process_cpu_user_us` | User CPU consumed by the Python process and all its threads during the invocation. |
| `process_cpu_system_us` | System CPU consumed by that same process during the invocation. |
| `process_lifetime_peak_rss_bytes` | The operating system's resident-memory high-water mark for the process's entire lifetime. |

CPU measurements exclude child processes, including a spawned Codex process.
They include helper and application threads; they cannot be attributed to each
concurrent local operation. Unsupported resource counters are `null`.

Memory units are normalized to bytes on Linux and macOS. The memory measurement
is **not an invocation peak**: a warm process can retain a high-water mark from an
earlier invocation. Do not add memory peaks across attempts, or present this value
as exclusive memory belonging to a task. Workflow elapsed time includes durable
waiting and must not be presented as consumed CPU or active runtime.

## Bounds, compatibility and retention

An invocation carries at most 128 local observations and 128 KiB of optional
observation metadata. `local_steps_truncated` signals that some local observations
were omitted. The observation buffer contains no application inputs, outputs or
exception messages. Valid program output takes priority if a result frame cannot
accommodate both the output and observations.

The execution-session port defaults to no observations, so other runtime adapters
and older workers remain valid. Console counters use decimal strings to preserve
unsigned 64-bit values; timestamps remain bounded epoch milliseconds.

The compact attempt metadata expires with its accepted settlement. Pre-upgrade
records have no synthetic measurement backfill. Retained workflow graph metadata
and attempt measurements have different lifetimes, so a visible local node can
outlive the detailed attempt that supplied its observation.

The Python implementation uses [`resource.getrusage`](https://docs.python.org/3/library/resource.html)
and [monotonic clocks](https://docs.python.org/3/library/time.html#time.monotonic_ns).
No provider billing or cost estimate is derived from these measurements.
