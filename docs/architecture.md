# Architecture

Ledgence provides a worker, a transport-independent delivery driver, and a Rust orchestration service backed by PostgreSQL. The worker prepares programs and manages subprocess lifecycles; the driver connects acquisition, lease renewal, execution, and settlement through `TaskService`. The service and storage adapter persist tasks, leases, results, and history. The [HTTP composition](http-orchestration.md) supplies an orchestrator executable, connected worker command, and task administration CLI. Integrated acquisition uses [bounded long polling](acquisition-waits.md) coordinated through portable storage probes and optional wake hints. The optional [dispatch source](dispatch-delivery.md) receives readiness references from SQS Standard or ElasticMQ, then requests a targeted durable claim over HTTP before acknowledging the broker record.

## Crate boundaries

| Crate | Responsibility | Production workspace dependencies |
| --- | --- | --- |
| `ledgence-worker-api` | Events, manifests, descriptors, runtime input, portable trace carriers, cancellation, and adapter ports | None |
| `ledgence-adapter-otel` | Optional trace provider/exporter, context bridge, correlated JSON logging | Worker API |
| `ledgence-worker-core` | Admission, preparation coordination, process capacity and reuse, shutdown | API |
| `ledgence-worker-delivery` | Service sessions, consumer cursors, lease monitoring, execution and settlement reconciliation | Worker API/core, orchestration API/core |
| `ledgence-adapter-artifact` | Filesystem/HTTPS stores, ZIP publication and local cache | API |
| `ledgence-adapter-subprocess` | Supervised CPython processes and invocation protocol | API |
| `ledgence-worker` | Local fixture and connected worker composition | Worker API/core/delivery, orchestration API, artifact/subprocess/HTTP adapters, optional OTel and SQS adapters |
| `ledgence-orchestration-api` | Submission, delivery, lease, receipt, and service contracts | Worker API |
| `ledgence-orchestration-core` | Pure lifecycle transitions and conservative local work authority | Orchestration API, worker API |
| `ledgence-orchestration-service` | Submission resolution and portable service composition | Orchestration API/core, worker API |
| `ledgence-adapter-postgres` | Atomic PostgreSQL operations, row codecs, and migrations | Orchestration API/core, worker API |
| `ledgence-adapter-sqs` | Optional SQS Standard publishing, receiving, acknowledgment and deployment configuration | Orchestration API, worker API |
| `ledgence-adapter-http` | Optional HTTP client/server implementations of `TaskService` | Orchestration API, worker API |
| `ledgence-orchestrator` | HTTP serving, explicit migrations, readiness, supervised recovery and optional dispatch publication | Orchestration API/service, worker API, HTTP/artifact/PostgreSQL adapters, optional OTel and SQS adapters |
| `ledgence-cli` | `ledgence task` submission, discovery, inspection, history and cancellation | Orchestration API, worker API, HTTP adapter, optional OTel adapter |

`tools/check-boundaries.py` checks normal and build dependencies, including target-specific edges. Integration tests may compose adapters. The API uses standard-library futures and owned contract types; concrete storage clients and Tokio process types stay behind adapters. The worker core uses Tokio for scheduling; the orchestration core performs no I/O.

[Task discovery](task-discovery.md) uses portable query/page contracts and required service/store methods. Metadata reads have no lifecycle transitions and keep application payloads out of listing results.

The HTTP adapter has empty default features and separate `client` and `server` features. The worker and task CLI select the client; the orchestrator selects the server and supplies its own `ApplicationService`. Neither HTTP side depends on SQLx. `tools/check-http-features.py` separately checks each selection so a workspace build's feature unification cannot conceal coupling between the two sides.

The worker ports are `ProgramStore`, `ArtifactCache`, `ExecutionRuntime`, and `ExecutionSession`. Runtime execution receives `RuntimeInvocation`: the unchanged event plus an optional ephemeral execution carrier. `TraceBridge` connects existing tracing spans to portable W3C values; SDK and exporter types remain in the OTel adapter. Orchestration exposes `TaskService`, `TaskStore`, and `RecoveryStore`, plus `DispatchIntentStore`, `DispatchPublisher`, and `AckQueue` for durable publication and individually acknowledged sources. `AcquisitionSource` composes either integrated or broker delivery with the same driver. Third-party Rust adapters are compiled into a composition executable. This does not establish a stable dynamic-library ABI or a plugin marketplace.

The [delivery contract](delivery-contract.md) defines the portable `TaskService` boundary and executable orchestration decisions. The [delivery driver](worker-delivery.md) accepts `Worker` and `Arc<dyn TaskService>` and derives N from `Worker::concurrency()`. `Worker::reserve_consumer` uses the existing N semaphore to retain capacity before acquisition and through settlement; its local execution method is single-use. Execution reports live in worker-api and remain reexported by worker-core. The [PostgreSQL persistence adapter](postgres.md) commits all transition records atomically before returning a durable acknowledgement. The driver has no PostgreSQL dependency; integration tests compose the real service, database, artifact, and subprocess adapters.

The driver opens a session and starts N consumers. Each reserves worker capacity before its ordered acquisition. Idle acquisitions wait up to 20 seconds without retaining a database transaction. For integrated delivery, a completed Empty advances the sequence; an early Empty observes the remaining minimum one-second cycle delay. Broker-empty polls do not consume a durable sequence; a confirmed nonauthority disposition advances it without idle pacing. Assigned work requires a dispatch renewal before preparation/execution. A separate lease monitor cancels user work at its conservative local deadline while settlement and cleanup reconciliation continue. Uncertain acquisition, renewal, and settlement replies reuse the same operation identity. The driver retains its reservation until remote ownership is resolved and local work is quiescent.

Stopping the driver closes worker admission and runs worker cleanup concurrently with those consumers. If an execution returns while its local work or cleanup is still outstanding, the driver reports unconfirmed quiescence and drains the whole worker. The driver retains one cleanup operation across status checks and retries only after an adapter returns an error; a separate confirmation finishes accepted reports without changing them. The embedding Tokio runtime must remain alive while this process is pending.

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

Logs never wait for output capacity. The log queue holds 64 records of at most 256 KiB each; excess records are counted and discarded. Optional dropped records do not change successful execution outcomes. Actual result/output-destination failures remain separately reported. When stderr is still usable, shutdown writes the lost-record count there. Writers preserve record ordering within each separate stream; merging stdout and stderr can interleave the streams. Keep them separate when parsing result JSON.

Pipe and terminal output uses nonblocking writes, so a paused reader cannot stop task timers or signal handling. The CLI retains writer ownership and signal subscriptions through final output draining and restores shared descriptor flags after normal completion. A filesystem write that the operating system cannot interrupt remains owned; the existing explicit second-signal force policy applies to unfinished output as well as process cleanup.

The subprocess adapter signals its owned process group before reaping its direct child. After reaping, it never delivers a signal to a remembered PID or process group. On macOS, a group containing only an unreaped zombie can return a permission error. A later read-only signal-0 probe resolves that uncertainty only when the operating system confirms the group no longer exists; any remaining group or ambiguous error retains cleanup ownership and capacity. See the [runtime details](../crates/ledgence-adapter-subprocess/README.md).

Adapter panics close worker admission from inside the retained supervisor. Session handles remain available for retirement or quarantine even when the original caller is gone. An adapter that panics during startup before returning a cleanup handle leaves an explicitly unresolved reservation and artifact pin; the worker cannot invent cleanup confirmation. A preparation adapter panic without a recovery handle likewise retains an unresolved-operation marker and prevents successful shutdown. Adapters must honor the documented ownership contract and avoid panics.

## Current boundaries of reliability

The CLI resolves all fixture program references before execution, rejects duplicate attempt identities, and pins one descriptor per logical task for that batch. The orchestration service and PostgreSQL adapter persist that binding so later acquisitions and retries retain the same program bytes across restarts. The delivery driver executes the descriptor in the assignment without resolving the release label again. Worker/service restart and persisted cache reuse are covered by PostgreSQL/Python acceptance tests. The local registry is not durable deduplication.

Reports and failures share `InvocationIdentity`: source/event ID, tenant/namespace, run/task/attempt ID, attempt number, and optional `traceparent`/`tracestate`. Bound program identity and digest accompany that context, including preparation failures before a PID exists. Warning and error logs carry the same context even when informational spans are filtered. A secondary cleanup error is retained separately from the original execution error. Raw program stderr is tagged with process ID and artifact digest because arbitrary byte streams cannot be assigned reliably to an invocation. The optional [OpenTelemetry adapter](observability.md) adds actual active span contexts and OTLP/HTTP export; metrics export remains deferred.

The driver retains uncertain operations only in memory. A worker process crash loses that local state; a replacement starts a new worker session, and service-side lease expiry and the task's retry policy recover unfinished attempts. The orchestrator schedules expiry recovery and supervises readiness; custom service compositions must do the same. An expired or unknown session drains the current driver instead of recreating it and transplanting old cursors. The HTTP acceptance gate exercises separate processes and socket faults with PostgreSQL. Applications need idempotency for external effects; neither process supervision nor an event ID guarantees exactly-once business outcomes.

## Design references

The packaging and handler lifecycle use the same broad separation as AWS Lambda: deployment code and dependencies, a separately supplied runtime, and initialization followed by repeated invocations. Ledgence uses an ordinary subprocess and does not inherit Lambda's isolation or service guarantees. [Lambda Python packaging](https://docs.aws.amazon.com/lambda/latest/dg/python-package.html), [runtime lifecycle](https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtime-environment.html).

Explicit child ownership and reaping follow Tokio's process semantics: a dropped handle does not itself establish confirmed cleanup. [Tokio Child documentation](https://docs.rs/tokio/latest/tokio/process/struct.Child.html).

Blocking operations cannot be cancelled by dropping their async waiter; the retained preparation design follows [Tokio blocking-task semantics](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html). Persistent signal subscriptions follow [Tokio Unix signal semantics](https://docs.rs/tokio/latest/tokio/signal/unix/fn.signal.html).

## Workflow coordination

The orchestration API adds optional `WorkflowStore` and `WorkflowService` ports. The application service resolves program descriptors outside transactions and applies bounded durable completion work through the store. The PostgreSQL adapter owns atomic workflow checkpoint, child registration, wait, and scheduling obligations. The subprocess adapter understands a generic versioned runtime extension/request protocol; it has no dependency on orchestration or a database SDK. Worker delivery binds its callback to the acquired activation lease.

Interactive activations use the existing consumer reservation and process pool. Their completed local results are acknowledged durably during execution, and a distributed wait releases the invocation. See [workflow contracts, recovery and limits](workflows.md).
