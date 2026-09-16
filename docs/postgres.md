# PostgreSQL persistence

Ledgence provides a Rust application service and an initial PostgreSQL 18 storage adapter. Together they implement durable single-task submission, acquisition, lease renewal, settlement, cancellation, history, and expiry recovery. They call the existing lifecycle core inside database transactions and return mutation success only after commit.

The [delivery driver](worker-delivery.md) executes assignments through the application service and this adapter, either in a Rust composition or through the [HTTP orchestrator and connected worker](http-orchestration.md). Separate gates cover in-process PostgreSQL/Python delivery and separate-process network delivery. [Bounded acquisition waits](acquisition-waits.md) and [optional OpenTelemetry traces](observability.md) are available; retention deletion remains later work. These tests do not establish exactly-once external business effects or database failover guarantees.

## Using the adapter

Create `PostgresStore` with a connection URL and `PostgresOptions`, then explicitly apply its migrations. The normal constructor never changes the schema. A migration example is available:

```sh
export LEDGENCE_POSTGRES_URL='postgres://user:password@localhost/ledgence'
cargo run -p ledgence-adapter-postgres --example migrate --locked
```

Use the installed library's `PostgresStore::migrate` in a deployment command when composing another executable. Apply migrations once as an explicit deployment step; never edit an already-applied SQL migration. SQLx checks checksums and serializes migration runners. Ordinary builds use committed `.sqlx` query metadata without connecting to a database.

The supplied deployment command is `ledgence-orchestrator migrate`, which reads `DATABASE_URL`. `ledgence-orchestrator serve --store DIR_OR_URL` calls `PostgresStore::verify_schema` before listening: it reads SQLx's migration bookkeeping, requires the complete expected version/checksum set, and rejects missing, dirty, changed, or newer migrations. It does not change the schema. `PostgresStore::check_connection` separately performs a bounded database probe. The library constructor still does not migrate or verify automatically; embedding applications choose their explicit startup sequence.

Compose `ApplicationService::new(Arc<dyn TaskStore>, Arc<dyn ProgramStore>)` with the PostgreSQL store and the chosen program-store adapter. The service implements `TaskService`. A matching submission replay avoids program resolution; on a new submission, resolution happens before the short acceptance transaction. Concurrent submissions retain the winner's immutable descriptor, digest, input, and origin context. A worker later downloads that bound program if its cache lacks it.

The delivery driver accepts `Arc<dyn TaskService>`. Tests can supply this in-process application service directly; the driver has no PostgreSQL dependency. `HttpTaskService` implements the same port for connected workers. The orchestrator composition schedules expiry recovery; custom service compositions remain responsible for it. The worker driver does not run database scans.

The platform does not require a database vendor account or PostgreSQL extension. This adapter supports PostgreSQL 18; compatibility with another major must be tested before changing that support boundary. PostgreSQL is isolated behind portable API ports. Custom adapters must uphold the same atomic and recovery behavior.

`PostgresOptions` configures database pool size, connection acquisition time, SQL statement/lock timeouts, and the total operation deadline. These control database resources; a worker still has its single N for consumers and process slots. The default pool has eight connections, five-second acquisition and statement budgets, and a thirty-second operation budget. Expiry has a thirty-second total default budget even though each shortlisted task commits independently.

Configure the connection URL's SSL mode and certificate settings for the deployment. The adapter uses Rustls with native trust roots. The durable-storage assumption is logged tables, synchronous commit, and PostgreSQL's normal durable WAL configuration, including `fsync` and `full_page_writes`. The adapter enables synchronous commit for mutation transactions. Replication and failover policies require separate deployment validation.

## Atomic records and locking

The task lifecycle tables are `tasks`, `attempts`, `worker_sessions`, `consumer_cursors`, `accepted_settlements`, and `task_history`. SQLx also maintains its migration bookkeeping. Tasks hold immutable submissions and descriptors alongside relational scheduling fields; attempt events and accepted reports use validated JSON bytes. They are not converted to JSONB, preserving supported numeric distinctions and escaped U+0000. User `data` stays user-owned.

Acquisition and every renewal lock the session for sharing, then the existing consumer cursor for update, then at most one task for non-key update. Acquisition can create a missing cursor race-safely; renewal cannot. Renewal verifies the cursor still names the requested assignment. Settlement, cleanup confirmation, cancellation, and expiry lock the task and never subsequently lock a cursor/session.

The previous assignment is read together with its task in one statement while holding the cursor. That snapshot is only used for reconciliation and is never written back. Renewals cannot extend its ownership while the cursor is held. If the same task becomes a claim candidate, the adapter uses its freshly locked state. Dependent attempt records are loaded after the target task lock, avoiding mixed snapshots after a lock wait.

Due-task acquisition skips locked rows and does not promise strict FIFO. A completed Empty disposition remains Empty when replayed, even if new work arrived. The next sequence performs a fresh acquisition. Pending probes explicitly roll back, including a provisional first-cursor insert. No transaction remains open while waiting. Committed cursor identity is unchanged; see [acquisition waits](acquisition-waits.md).

The store samples database wall time after relevant locks. Database clock discipline remains an operational assumption. Assignment replay returns remaining authority rather than resetting a lease duration. The delivery driver uses the conservative local deadline rules and renews dispatch permission before execution; all transport adapters must preserve these authority fields. Dispatch permission records possible execution before acknowledging it.

## Results, interruption, and inspection

An accepted result, its receipt, task/attempt changes, and lifecycle history commit together. Cleanup confirmation is separate and never rewrites the accepted command. A matching old receipt remains replayable after expiry or a newer attempt; changed commands conflict. Rejected reports produce bounded structured diagnostic fields without replacing accepted results.

An accepted success with unconfirmed cleanup is retained through restart and finalized by confirmation or expiry under the existing cancellation rules. An interruption without an accepted result can schedule a retry with the same task/run/program binding and new attempt/event/lease identities. Applications remain responsible for idempotency of external effects.

Database errors never become Empty or successful acknowledgement. Known deadlock/serialization aborts receive bounded retries. Connection loss during commit, or a timeout while committing, can leave an unknown outcome: repeat the same submission key, acquisition sequence, renewal sequence, or settlement command to reconcile it. Session creation has no request idempotency key; an uncertain registration can leave an unused session that expires.

Cancellation or timeout while a transaction is starting closes the uncertain connection before pool reuse. Once startup succeeds, normal transaction rollback and connection reuse apply to mutations and snapshot reads.

`inspect` returns the task; `inspect_attempt` returns a coherent task/attempt view including its accepted outcome; `history` returns at most 100 ordered lifecycle records after a sequence. Polls and ordinary renewals do not append history. Origin, invocation, and processing trace contexts stay separate; assignment replay preserves the event. Without creation-span instrumentation, new events carry the accepted origin context if supplied. Storing that context does not activate or export spans.

Invoke `RecoveryStore::expire_batch` periodically from the service composition. Its limit is 1–100 shortlisted task IDs; each candidate is locked and rechecked in a separate transaction. Progress distinguishes examined candidates from expired tasks. Locked or renewed candidates may be skipped. A later error or overall timeout does not undo earlier committed recovery. Repeat scans safely; a batch does not establish that all expired work has been exhausted.

The supplied HTTP orchestrator runs a supervised loop with one-second cadence, bounded catch-up batches, capped unavailable backoff, and readiness based on fresh progress. Multiple orchestrators may scan the same database; PostgreSQL locks and lifecycle rechecks arbitrate their changes. See [recovery and lifecycle](http-orchestration.md#recovery-readiness-and-shutdown).

The shortlist uses database statement time as a fixed index range cutoff. Each locked candidate is then rechecked against fresh database wall time before its expiry transition.

Retention deletion is not implemented: records and submission deduplication can remain beyond the proposed ninety-day terminal period. Current cursor reconciliation requires complete referenced snapshots, so deleting payloads or resetting cursors early is incorrect. The next retention implementation must preserve that behavior explicitly.

## Verification

Run ordinary workspace checks without a database using the committed offline metadata. Real database tests are explicitly ignored by default so their absence cannot be confused with a successful database validation. The dedicated gate lists and executes them against PostgreSQL 18.

For the full PostgreSQL gate, provision an empty disposable database and its Docker container, then set `LEDGENCE_POSTGRES_URL`, `LEDGENCE_POSTGRES_CONTAINER`, and `LEDGENCE_PYTHON` to a CPython 3.11-or-newer interpreter and run:

```sh
python3 tools/check-postgres.py
```

This requires `psql`, Docker, Cargo, and CPython. The gate applies the schema to the empty query-checking database, checks SQL macro metadata against the actual schema, and runs the real database tests serially, including delivery through real Python subprocesses. Individual tests use isolated databases and clean them up. The crash-recovery test deliberately kills and restarts the explicitly named disposable PostgreSQL container after proving it contains that test's unique database. Do not point this gate at a shared deployment. The scratch-schema setup is not a production migration tool.

The tests cover concurrent submissions and initial cursors, competing consumers, live ownership, lock waits crossing expiry, dispatch and cancellation, rollback after intermediate writes, replay after reconnect, immutable historical receipts, expiry recovery, JSON fidelity, schema constraints, migration checksums, and actual PostgreSQL crash recovery. Mutation/clock/ID queries have compile-time SQLx descriptions; complex snapshot hydration uses explicit PostgreSQL row codecs exercised by database tests.

Delivery acceptance tests publish a Python package with a prepared dependency, submit tasks, and run the reusable worker through `TaskService`. They verify the complete CloudEvent, one artifact download, warm process reuse, and durable results/history. A service wrapper loses acquisition, dispatch, and settlement replies after real commits; retries preserve operation identities and the handler runs once for that attempt. Another case reconnects the store and restarts the worker after removing the published descriptor/blob, then executes a previously accepted task using its persisted binding and reopened cache. These tests do not use an HTTP transport.

Additional ignored database tests compose the real HTTP adapter with controlled runtime cleanup to verify immutable unconfirmed reports, separate quiescence confirmation, and historical receipt replay after expiry/new ownership. Some of these tests adjust stored deadlines to exercise lifecycle transitions quickly; they do not establish production-duration lease behavior. The read-only schema test covers absent, matching, changed, and newer migration records.

After the database gate, run `python3 tools/check-http.py --psql /path/to/psql` for [separate-process network acceptance](http-orchestration.md#verification). It creates and drops a unique database on the supplied test server and does not restart that server. This is a separate gate from the PostgreSQL crash-recovery test above. Both gates are configured in the dedicated Linux CI job; ordinary offline workspace tests cannot substitute for them.

For changed query macros, create the migrated scratch schema, set `DATABASE_URL` to it, and generate metadata with `SQLX_OFFLINE=false SQLX_OFFLINE_DIR=<absolute .sqlx path>` while checking the adapter. Force recompilation of the adapter with `cargo clean -p ledgence-adapter-postgres` first so all macros expand. Review additions/removals rather than keeping obsolete descriptions. `tools/check-postgres.py` independently regenerates into a temporary directory and compares exact descriptions.

The standalone SQLx CLI is not required. Its reviewed 0.9.0 distribution did not pass the existing dependency gate; the verification tool uses SQLx's supported macro output and the PostgreSQL client instead. See [dependency policy](dependencies.md).

## Task discovery indexes

[Task discovery](task-discovery.md) adds migration `20260913010000_task_discovery.sql` with scoped indexes for submission order, state, queue, and exact correlation. Apply it explicitly before serving the updated application. Index creation is transactional and can block concurrent writes. Existing tasks remain unchanged. The database gate includes initial-schema upgrades and query-plan regressions over 100,000 tasks and 100,000 attempts.

## Migration execution budget

`ledgence-orchestrator migrate` defaults to a ten-minute migration budget. Use `ledgence-orchestrator migrate --timeout-ms 1800000` for a thirty-minute budget on a larger existing database. Values must be integer milliseconds from 1 through 2147483647. The budget begins after connecting and includes waiting for a pool connection, configuring migration timeouts, waiting for the migration lock, applying migrations, and closing the migration connection. Initial database connection setup retains its existing thirty-second bound and pool acquisition its five-second cap. Ordinary database operations retain their existing statement and operation limits.

Embedded Rust callers can use `PostgresStore::migrate_with_options(MigrationOptions { timeout: Duration::from_secs(1800) })`; `migrate()` uses the ten-minute default. The migration connection is dedicated to that call and discarded on every exit, preventing its longer SQL timeout and session advisory locks from being reused by normal requests. The overall deadline bounds the migration call; PostgreSQL also receives a finite statement/lock timeout based on its remaining budget. A statement timeout resets for each statement, so it does not replace the overall deadline.

Migration files remain transactional and checksum-verified. Index creation can block writes, so schedule the upgrade accordingly. A timeout or interruption does not establish that nothing committed: completed migration files remain recorded, and the server may take additional time to observe a disconnect and finish cleanup. Rerun the explicit migration command to reconcile with the migration ledger. `serve` continues to verify the schema without changing it. First SIGINT/SIGTERM drains the current migration within its budget; a second signal forces a nonzero exit.

## External dispatch persistence

Migration `20260914010000_dispatch.sql` adds durable queue routes, per-task destination bindings, delivery intents and exact targeted-claim receipts. Apply the explicit migration before using the updated server; replacing the integrated due-task index can block concurrent writes. Existing tasks keep integrated delivery. Activating an external route requires its logical queue to contain no queued or active tasks; terminal history may remain. A destination alias belongs to one tenant/namespace/queue and cannot be reassigned through startup configuration. Use a dedicated physical queue and the same route configuration on its publishers and consumers.

Task acceptance and its first intent commit together. A retry creates the next readiness generation in the same transaction as its lifecycle transition. Claim, cancellation and terminal transitions remove the obligation atomically. A confirmed send retains the intent for repair after 60 seconds; an uncertain send retries after one second with the same publication identity. Publication leases expire after 30 seconds. A deliberately repeated confirmed publication gets a new publication identity while retaining its task generation. Old publisher replies cannot overwrite a newer lease or generation.

Workers claim a specific task under its consumer cursor, session and task locks; they do not scan the integrated queue. Claim receipts bind the complete operation to that task and readiness generation. Their persisted form stores an attempt reference rather than reusable authority: replay refreshes authority only if the same cursor, session and attempt remain current. Another worker receives a nonauthority disposition. Broker acknowledgment is safe only after a matching successful disposition; a timeout or database error remains uncertain. Execution, renewal, settlement and expiry retain the existing durable lifecycle.

Publication batches use `FOR UPDATE SKIP LOCKED` over due intents. Publisher transactions lock intents without taking task locks, preserving the task-then-intent order of lifecycle changes. Ordinary renewals and integrated task transitions do not maintain external intents. Empty external receives perform no PostgreSQL acquisition or cursor update. This separates transport work; it does not eliminate the database writes required for durable ownership, results and history.

Retention remains unimplemented for targeted-claim receipts as well as existing task records. Do not delete receipts, referenced attempts or session cursors independently. See [dispatch contracts and supported backends](dispatch-delivery.md) and [performance measurement](performance.md); local conformance checks are not an AWS capacity or day-long endurance qualification.


## Workflow persistence

Migration `20260915000000_workflows.sql` adds `workflow_runs`,
`workflow_activations`, `workflow_task_links`, `workflow_local_results`,
`workflow_work`, and `workflow_history`. Existing tasks gain nullable workflow
and activation identities; standalone task behavior is unchanged. Apply the
migration explicitly before starting the updated executable.

Workflow submission commits its controller binding and first activation task
together. Each acknowledged local result is immutable and belongs to one logical
activation across its task attempts. The accepted controller decision commits
its checkpoint, consumed inputs, child bindings, wait membership, and scheduling
obligations in one transaction. Terminal task transitions insert completion work
in that same lifecycle transaction; ordinary standalone transitions skip this
workflow SQL.

Workflow operations lock the workflow for non-key update, then any work/task
records they change. Ordinary task settlement never takes a workflow mutation
lock. Coordinator claims release their transaction before external program
resolution. Application rechecks the work lease and stored task outcome under
the workflow lock. Worker local-result requests bind the exact lease owner;
accepted receipts may replay after that lease expires, while new writes require
live authority.

Compact status and local-result authority reads exclude application payloads.
Child completion notifications do not load result bodies until a continuation
needs them. All-terminal waits use bounded membership; cancellation drains up to
16 owned items per transaction, sharing the budget between task cancellation and
subworkflow cancellation obligations. The adapter retains history and journal records;
workflow retention deletion and sharding are not implemented. See
[workflow semantics and bounds](workflows.md).

Migration `20260917000000_owned_workflows.sql` adds immutable parent/root lineage,
`owned_workflow_links`, and workflow-terminal/cancel-owned work variants. Root
submission idempotency remains scoped to top-level runs. Owned children use their
parent's shared task/workflow key namespace. Parent checkpoint acceptance creates
new child runs and their first activation/dispatch obligations atomically.

A child's terminal transaction marks its direct ownership link and records a
compact parent obligation. Parent cancellation schedules bounded durable work for
children; it does not hold a parent run lock while acquiring a child run lock.
Each child drains its own descendants before becoming terminal. The parent can
therefore wait for direct-child terminal markers without scanning or locking the
whole tree. See [owned subworkflows](subworkflows.md) for bounds and migration
compatibility; retention still requires preserving the new relationships.
