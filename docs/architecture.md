# Worker foundation

This milestone proves local program preparation and process lifecycle behavior. It deliberately leaves the orchestration transport behind a future adapter: the worker does not yet poll a production queue, renew distributed leases, persist results, or reconcile uncertain settlement.

## Crate boundaries

| Crate | Responsibility | Production workspace dependencies |
| --- | --- | --- |
| `ledgence-worker-api` | Events, manifests, descriptors, cancellation, and adapter ports | None |
| `ledgence-worker-core` | Admission, preparation coordination, process capacity and reuse, shutdown | API |
| `ledgence-adapter-artifact` | Filesystem/HTTPS stores, ZIP publication and local cache | API |
| `ledgence-adapter-subprocess` | Supervised CPython processes and invocation protocol | API |
| `ledgence-worker` | Configuration, command-line entry points, local fixture composition | All four |

`tools/check-boundaries.py` checks normal and build dependencies, including target-specific edges. Integration tests may compose adapters. The API uses standard-library futures and owned contract types; concrete storage clients and Tokio process types stay behind adapters. The core currently uses Tokio for scheduling.

The public ports are `ProgramStore`, `ArtifactCache`, `ExecutionRuntime`, and `ExecutionSession`. Third-party Rust adapters are compiled into a composition executable. This does not establish a stable dynamic-library ABI or a plugin marketplace.

## Invocation ownership

1. The caller supplies a validated CloudEvent and an immutable program descriptor already bound to the logical task.
2. The worker registers the tenant/namespace/attempt identity and reserves a consumer permit. Concurrent ownership of the same attempt in one worker is rejected.
3. The cache returns a pinned artifact or the worker downloads, verifies, and publishes it. Concurrent requests for one digest share preparation. Dropping the caller does not release unfinished preparation. A fetch timeout can return a failure promptly, but the same fetch future, consumer permit, and attempt registration remain owned until its underlying I/O completes. Late download bytes are discarded after timeout; they never trigger execution. If cancellation or the invocation deadline occurs during a cache lookup, its completion remains owned, but a cache miss does not start a new download.
4. A compatible warm process is selected first, then unused capacity. An incompatible idle process is retired only when all slots are occupied. One process accepts one invocation at a time.
5. The runtime sends the whole event once. Response event and attempt IDs must match. A business failure is a valid response; an uncertain runtime/protocol result retires the process and is never silently retried.
6. Healthy processes return to the warm pool. Cleanup must be confirmed before releasing their process reservation and artifact pin.

`N` is the global limit for consumers and for starting/warm/running/retiring process slots, across all program versions. Cache byte limits and lifecycle timeouts are resource controls, not additional concurrency settings. Preparation happens only after consumer admission, so downloads are also bounded by `N`.

The session key is artifact digest + tenant + namespace. This prevents accidental reuse across those boundaries; it does not provide OS isolation. Cache pressure can evict unpinned artifacts or retire idle processes to release pins. A hard package limit is rejected without retiring healthy processes.

`ExecutionRuntime::start` returns either a ready session or a cleanup-required session handle. An ordinary error promises that no process remains owned. Unconfirmed cleanup keeps the handle, pin, and pool slot quarantined. Execution failures report their phase and whether invocation may have started. Cleanup uncertainty must not be treated as proof that external effects did not occur.

Detached supervisors retain work when a caller drops its future. Shutdown stops admission, drains for a grace period, cancels remaining work, and uses a separate cleanup budget. Dropping the shutdown caller does not abandon its supervisor. Incomplete shutdown remains an error with ownership retained; embedding applications must keep the Tokio runtime alive through explicit cleanup. The CLI retains ownership and retries incomplete cleanup every five seconds. SIGINT and SIGTERM use this path, including during preparation and startup. The first signal stops dispatch; a further signal requests forced exit with a nonzero status and an explicit unresolved-cleanup error. Only that explicit force bypasses waiting for unfinished blocking work; it cannot certify that all descendants stopped. Unix signals may coalesce, so two rapidly repeated identical signals are not a reliable two-step shutdown request.

The CLI owns a separate writer thread for each output stream. Result submission waits asynchronously for a complete write, with a five-second deadline that includes queueing; eight records can wait in the result queue, and a CLI record is limited to eight MiB. This is confirmation of a local write, not durable settlement. A result write failure stops further dispatch, cancels running siblings, preserves normal process cleanup, and exits nonzero without rerunning program effects. A failed write may leave an incomplete final line at its destination.

Logs never wait for output capacity. The log queue holds 64 records of at most 256 KiB each; excess records are counted and discarded. Lost logs or a log destination failure make the final CLI status nonzero. When stderr is still usable, shutdown writes the lost-record count there. Writers preserve record ordering within each separate stream; merging stdout and stderr can interleave the streams. Keep them separate when parsing result JSON.

Pipe and terminal output uses nonblocking writes, so a paused reader cannot stop task timers or signal handling. The CLI retains writer ownership and signal subscriptions through final output draining and restores shared descriptor flags after normal completion. A filesystem write that the operating system cannot interrupt remains owned; the existing explicit second-signal force policy applies to unfinished output as well as process cleanup.

The subprocess adapter signals its owned process group before reaping its direct child. It never signals a remembered PID after reaping. On macOS, an already-dead group may produce an ambiguous permission error; this remains an unresolved cleanup result rather than freeing capacity on an assumption. See the [runtime details](../crates/ledgence-adapter-subprocess/README.md).

Adapter panics close worker admission from inside the retained supervisor. Session handles remain available for retirement or quarantine even when the original caller is gone. An adapter that panics during startup before returning a cleanup handle leaves an explicitly unresolved reservation and artifact pin; the worker cannot invent cleanup confirmation. A preparation adapter panic without a recovery handle likewise retains an unresolved-operation marker and prevents successful shutdown. Adapters must honor the documented ownership contract and avoid panics.

## Current boundaries of reliability

The CLI resolves all fixture program references before execution, rejects duplicate attempt identities, and pins one descriptor per logical task for that batch. A future orchestration service must persist this binding so retries across workers and restarts cannot change program bytes. The local registry is not durable deduplication.

Reports and failures share `InvocationIdentity`: source/event ID, tenant/namespace, run/task/attempt ID, attempt number, and optional `traceparent`/`tracestate`. Bound program identity and digest accompany that context, including preparation failures before a PID exists. Warning and error logs carry the same context even when informational spans are filtered. A secondary cleanup error is retained separately from the original execution error. Raw program stderr is tagged with process ID and artifact digest because arbitrary byte streams cannot be assigned reliably to an invocation. There is no OpenTelemetry span activation/exporter or metrics backend yet.

This milestone does not implement distributed at-least-once delivery. That remains the target orchestration contract. Applications will still need idempotency for external effects; neither process supervision nor an event ID can guarantee exactly-once business outcomes.

## Design references

The packaging and handler lifecycle use the same broad separation as AWS Lambda: deployment code and dependencies, a separately supplied runtime, and initialization followed by repeated invocations. Ledgence uses an ordinary subprocess and does not inherit Lambda's isolation or service guarantees. [Lambda Python packaging](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html), [runtime lifecycle](https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtime-environment.html).

Explicit child ownership and reaping follow Tokio's process semantics: a dropped handle does not itself establish confirmed cleanup. [Tokio Child documentation](https://docs.rs/tokio/latest/tokio/process/struct.Child.html).

Blocking operations cannot be cancelled by dropping their async waiter; the retained preparation design follows [Tokio blocking-task semantics](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html). Persistent signal subscriptions follow [Tokio Unix signal semantics](https://docs.rs/tokio/latest/tokio/signal/unix/fn.signal.html).
