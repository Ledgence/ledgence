# CPython subprocess adapter

`SubprocessRuntime::new(python, runner)` implements the portable worker runtime
port using Tokio. `python` selects the separately installed host interpreter;
`runner` locates `sdk/python/ledgence/worker/bootstrap.py`. Deploy the complete
`ledgence/worker/` helper directory beneath a Python import root, preserving its
parent `ledgence/` namespace directory. Do not add `ledgence/__init__.py`: the
client and worker contribute separate packages to this shared namespace. For
example, a deployment may use `/opt/ledgence/python/ledgence/worker/bootstrap.py`
as its runner. The bootstrap makes `/opt/ledgence/python` available to the isolated
interpreter. The client SDK and its HTTP dependencies are not required by the
worker helper. `LEDGENCE_PYTHON` selects the interpreter for integration tests;
the worker CLI selects its interpreter with `--python`.

Each session starts CPython with `-I -S -B`, imports the manifest's synchronous
`module:function` handler from the prepared artifact, and waits for a bounded
readiness message. The declared Python major/minor must match exactly. Packages
contain application code and vendored dependencies; the Rust worker does not
embed Python or install packages at execution time. Handler module origins are
checked against the prepared artifact. Preloaded-module collisions are rejected
before readiness rather than selecting an unrelated function from Python's
module cache. Bytecode writes are disabled before application imports.

Each process receives a private temporary working directory outside the
immutable artifact cache. Relative writes persist between that session's
invocations and are removed after confirmed process cleanup. The prepared
artifact remains an absolute import location. If child or group cleanup is
uncertain, the worker preserves the working directory and logs its path for
recovery. This directory separation does not restrict the program's filesystem
permissions.

The session accepts one invocation at a time. Requests contain the unchanged
CloudEvent, with event and attempt IDs repeated in the protocol envelope. Both
IDs must match the response before a result is accepted. JSON lines are bounded
in both directions. Business exceptions and invalid handler outputs produce
typed failures and leave the process reusable. Runtime or protocol failures
retire it. The adapter never resends an invocation. The result value profile
requires string object keys, Unicode scalar strings, i64/u64 integers, finite
binary64 floats, and no more than 64 nested containers. Python validates these
constraints before encoding; Rust uses the API's shared structural validator.
Protocol frames use UTF-8 JSON. Failure messages are shortened to fit the complete
encoded frame, preserving mandatory invocation identity.

The default frame limit is 2 MiB in each direction, including the complete JSON
envelope and newline. It leaves room for the delivery contract's 1 MiB of
application input plus generated CloudEvent metadata and protocol fields.
`SubprocessConfig::max_frame_bytes` remains an explicit local override: a smaller
limit may reject valid delivery submissions, and a larger limit must be checked
against the receiving report or settlement limit. Oversized handler results
produce an `invalid_output` failure within the configured frame limit.

A shared runtime record owns the child, artifact lease, and working directory;
the Tokio supervisor borrows that record independently of the caller's future.
The record survives supervisor failure. Startup
timeout, invocation deadline, cancellation, dropped operation, and dropped
session all lead to cleanup. On Linux/macOS the worker requests termination of
the owned process group before reaping the direct child. The artifact lease
remains held through that cleanup. Explicit `close()` waits for confirmation. The group-signal outcome is recorded
before waiting for the child. If fallback cleanup is interrupted while reaping,
a later close resumes from that recorded outcome without sending another group
signal or overwriting a successful signal with a zombie-only permission error.
The SDK acknowledges shutdown with `closing` while staying alive until its
parent signals the group or closes stdin. This keeps normal shutdown from racing
Darwin's refusal to signal a group that contains only a zombie. Successful
repeated `close()` calls remain successful; unresolved cleanup errors remain
errors on repeated calls and must not release a quarantined pool slot.
Dropping a session schedules cleanup while the Tokio runtime remains alive;
applications must explicitly close sessions before shutting down that runtime.

`start()` returns `StartOutcome::Ready` on success. An ordinary error means no
process was created or cleanup was confirmed. `StartOutcome::CleanupRequired`
returns the startup error together with an owned session that must retain its
pool reservation. This also applies when the startup channel disappears before
the supervisor confirms cleanup. The runtime retains the resource record until
cleanup succeeds, including if a caller drops the returned handle. A subsequent
`close()` can finish interrupted cleanup or retry removal of a repaired working
directory. After reaping, a failed group signal can be resolved by a read-only
signal-0 probe returning `ESRCH`, confirming that the group is absent. An existing
group or any other probe result retains the original error and ownership. No
actual signal is delivered to a remembered group ID after reaping, including
when the operating system has reused that ID.

User stdout, including native writes to descriptor 1, is redirected to stderr.
The protocol uses a separate descriptor. Rust continuously drains stderr using
a fixed buffer and emits only the configured byte allowance through `tracing`.
Native log chunks carry PID and artifact digest. Core invocation records carry
event, task, attempt, and trace identity; raw stderr has no reliable invocation
framing. Allowance resets at invocation start, so background log activity shares
the next allowance.

The initial runtime is for trusted code. It does not sandbox filesystem access,
network access, memory, or environment variables. Programs must clean up their
own persistent globals and background work between invocations. Descendants
that deliberately leave the process group or change credentials can escape
group cleanup. Following a spontaneous child exit, macOS group signaling can
report `EPERM` for an already dead group as well as for a real permission denial;
the adapter retains that cleanup uncertainty unless a later read-only probe
confirms the group is absent. Repeating a
task after a crash can repeat external effects; idempotency belongs to the
program and orchestration contract.

Validation:

```sh
LEDGENCE_PYTHON=/absolute/path/to/python3 cargo test -p ledgence-adapter-subprocess
python3 -m unittest discover -s sdk/python/tests -v
```

The interpreter must be CPython 3.11 or newer. Lifecycle integration tests need
permission to inspect their child processes and signal their process groups.


Runtime protocol 1 remains supported. The Python import migration described in
[program packages](../../docs/program-packages.md) applies independently of the
selected protocol. Protocol 2 adds
`RuntimeInvocation.processing_context`, a W3C execution carrier outside the
unchanged event, and multiplexes bounded structured log frames with results.
The manifest selects the protocol before startup; mismatches fail readiness.
Rust drains optional log frames both during execution and while warm/idle,
yielding after at most 16 consecutive frames and prioritizing idle control
commands. Logs never wait on a collector or terminal pipe: the worker's tracing
subscriber uses its bounded nonblocking JSON output writer. The configured
`max_log_bytes` allowance covers raw and structured output together, resetting at
invocation start. Invalid optional log records are dropped with logarithmically
rate-limited diagnostics; malformed framing or mismatched results still retire
the session. Validated log records carry their own creation-time execution IDs
and active trace/span IDs, even when received late, and never inherit the current
actor span. User attributes remain a separate JSON object in the protocol and are
JSON-encoded in the tracing `attributes` field. Raw stderr remains PID/digest only.

The v2 Python writer has one reserved control/result slot and a bounded optional
queue; see the [helper contract](../../sdk/python/README.md) for exact limits.
Optional application provider shutdown callbacks run before the closing ACK and
remain bounded by `SubprocessConfig::shutdown_timeout`. Cleanup and process-group
ownership rules are unchanged.
