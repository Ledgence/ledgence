# CPython subprocess adapter

`SubprocessRuntime::new(python, runner)` implements the portable worker runtime
port using Tokio. `python` selects the separately installed host interpreter;
`runner` locates `sdk/python/ledgence_worker/bootstrap.py`. Keep the bootstrap and
its sibling `__init__.py` together. `LEDGENCE_PYTHON` selects the interpreter for
integration tests. The worker CLI selects its interpreter with `--python`.

Each session starts CPython with `-I -S`, imports the manifest's synchronous
`module:function` handler from the prepared artifact, and waits for a bounded
readiness message. The declared Python major/minor must match exactly. Packages
contain application code and vendored dependencies; the Rust worker does not
embed Python or install packages at execution time.

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
retire it. The adapter never resends an invocation.

A shared runtime record owns the child, artifact lease, and working directory;
the Tokio supervisor borrows that record independently of the caller's future.
The record survives supervisor failure. Startup
timeout, invocation deadline, cancellation, dropped operation, and dropped
session all lead to cleanup. On Linux/macOS the worker requests termination of
the owned process group before reaping the direct child. The artifact lease
remains held through that cleanup. Explicit `close()` waits for confirmation.
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
directory. A process-group error after the direct child has been reaped remains
unresolved; numeric process-group IDs are never signaled after ownership is lost.

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
the adapter conservatively
reports that cleanup uncertainty after reaping the direct child. Repeating a
task after a crash can repeat external effects; idempotency belongs to the
program and orchestration contract.

Validation:

```sh
LEDGENCE_PYTHON=/absolute/path/to/python3 cargo test -p ledgence-adapter-subprocess
python3 -m unittest discover -s sdk/python/tests -v
```

The interpreter must be CPython 3.11 or newer. Lifecycle integration tests need
permission to inspect their child processes and signal their process groups.
