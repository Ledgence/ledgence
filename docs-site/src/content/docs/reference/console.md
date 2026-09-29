---
title: Console reference
description: Self-hosted Console views, query semantics, command behavior, process observations, and deployment requirements.
---

Console is the operator interface for **one self-hosted Ledgence instance**. The Rust orchestrator serves its static assets at `/console/`, and the browser uses same-origin `/v1/console/*` APIs. A deployed Console needs no Node process, hosted frontend service, or vendor account.

**Availability:** these features are implemented in the current source tree. They are not part of the published `v0.1.1` source tag or `0.1.0` native bundle. [Explore Ledgence Console](/tutorials/use-console) uses a source checkout that contains the implementation; [Releases and packages](/reference/releases) lists published artifacts separately.

## Views and actions

| View | Recorded information | Available actions |
| --- | --- | --- |
| Executions | Unified task/workflow history. Task details provide Trace and General; workflow details also provide Graph. Input, output, attempts, resources and durable history remain available. | Submit an exact program reference, run again as new work, request cancellation, inspect a child execution, send an event to a recorded workflow wait. |
| Programs | Registered program references, versions, digests, manifests, declared kinds and descriptive metadata. | Register a published reference, update descriptive metadata explicitly, open a submission form or matching execution history. |
| Workers | Worker sessions, configured capacity, observation freshness, process slots, and validated task/attempt links. | Inspect the latest available observation and follow execution links. |

**Programs** includes tasks and workflows, with any-version kind filters and explicit mixed/unspecified kinds. Registration does not execute or upload a package. Workers is under Operations; worker inspection does not provide a drain, kill, or scaling command.

Workflow **Graph** and **Trace** use the same retained execution records. Graph shows one workflow and its direct children; a subworkflow is an opaque node that opens its own graph. Select work to inspect it; open a child execution to drill in. Back restores the previous navigation entry, Up follows ownership, and breadcrumbs select ancestors. Completed work remains visible. Correlation keys and timestamps do not prove dependencies. The graph does not predict future branches or edit a workflow definition.

Every entrypoint invocation is a node identified by its activation ID. Re-entering the same handler creates a distinct invocation; retrying one activation does not. There are no phase containers. Local work remains visible as evidence attributed to an activation; it does not gain causal edges based on code order or timestamps. Only recorded branch members belong to a distributed fork. A separate parent task is not another branch. Joins wait for terminal outcomes, including failure or cancellation. Rejected decisions do not establish applied edges, and a closed wait alone does not prove a successful wake. Partial pages and unavailable references remain explicit. The graph works without exported OpenTelemetry traces.

Edges distinguish registering work, waiting for terminal outcomes, and resuming an entrypoint. A resumed single-child wait may be compacted visually when the entire child/wait/destination chain is known; its coordination record remains inspectable and appears in Trace. Multi-member joins and incomplete evidence retain their coordination nodes. See the [relationship evidence matrix](https://github.com/Ledgence/ledgence/blob/develop/docs/console-query-model.md#entrypoint-identity-and-causal-evidence).

## Filters and pagination

Execution discovery supports task/workflow kind, exact state, program/version, queue, correlation and execution ID, plus submission time bounds. The default scope is root workflows and standalone tasks; Include child executions adds ordinary tasks and subworkflows. Program history and exact-ID lookup include children automatically. Controller activations remain inside their workflow. Unregistered programs retain execution history. The server interval is `[submitted_from, submitted_until)`. Inclusive UTC calendar-day filters use the next UTC midnight as the exclusive upper bound. Invalid ranges and kind/state combinations are rejected.

An unset correlation filter differs from filtering for an empty correlation string. Enable the form's correlation checkbox when applying either a nonempty or empty exact value. These are exact filters, not full-text search.

Pages default to **50** items and are capped at **100**. Opaque keyset cursors are bound to the resource and filters. Each response is coherent at its read, while later pages may see newer committed data. There are no synthetic total counts or a frozen snapshot across navigation.

Execution discovery refreshes its first live page automatically; older pages refresh explicitly. Worker pages refresh the latest observations. The program catalog does not poll on an interval. Hidden and offline pages pause polling. Terminal outcomes stop active execution and workflow polling. Input and output bodies load on demand, so ordinary refreshes do not repeatedly fetch execution payloads.

The browser checks the instance identity and Console contract version before accepting a response. A compatibility error is explicit rather than silently interpreting another version's fields.

## States and results

| Resource | States |
| --- | --- |
| Task | `queued`, `active`, `succeeded`, `failed`, `cancelled` |
| Workflow | `running`, `waiting`, `failing`, `cancelling`, `succeeded`, `failed`, `cancelled` |

`active` describes task lifecycle state; it is not proof that a Python handler is executing at the moment of inspection. Workflow `failing` and `cancelling` can remain nonterminal while owned children drain.

A pending result differs from a successful JSON `null`. Console preserves application JSON integer and floating-point values, including supported signed/unsigned 64-bit integers, `1.0`, and `-0.0`. Revision and sequence metadata use decimal strings. Execution identifiers remain outside application `data`.

**History** shows durable transitions. It is not a log viewer or a store of every worker observation. Attempt details and worker observations describe different evidence: accepted execution reports versus the latest reported process state.

## Commands and retries

Console does not automatically retry writes. A transport timeout can happen after a submission, event, registration, or cancellation has committed. Its explicit retry keeps the original in-memory command bytes and identity. Editing the intended operation requires starting a separate command; navigating away can discard the retained command.

**Run again** creates a new submission and a new idempotency key. It does not retry observation of the original execution, undo its effects, or reset its attempts. Programs remain responsible for idempotent external effects.

Cancellation is a request. Active process cleanup and workflow child draining may continue after the request is accepted. Likewise, an accepted external event does not mean the workflow has already resumed. Use the actual recorded wait and its exact key; preserve the event identity when reconciling an uncertain send.

Inputs, results, events, and command bodies are not persisted to browser local storage or analytics. Appearance and Graph/Trace preferences may persist. Filters and resource identifiers can appear in navigation URLs; scroll, selection and graph presentation are retained for navigation.

## Attempt resources and local observations

Resources show available measurements for one attempt. Runtime elapsed excludes preparation and startup, while CPU counts the Python process and all its threads during the invocation. Child processes, including Codex, are excluded. Concurrent local functions share the process, so its CPU cannot be attributed exclusively to a single local function.

Memory is labeled **process lifetime peak memory**. Reused processes can retain peaks from earlier invocations; this is not an invocation peak and peaks must not be summed. Unavailable counters are distinct from zero. No provider billing estimate is inferred.

Local start, elapsed, failure and replay observations are delivered with the accepted attempt report. They are not a live stream of running functions. The accepted-result journal remains authoritative: a callable returning does not prove its result was durably committed, and replay does not execute it again. Older workers, interrupted processes and lost reports can leave measurements unavailable. See the [execution observation contract](https://github.com/Ledgence/ledgence/blob/develop/docs/execution-observations.md).

## Worker process observations

Workers report their current process pool approximately every five seconds with jitter. Console shows the server receipt time and separates observation freshness from session expiry:

| Age of the last report | Freshness |
| --- | --- |
| Up to 15 seconds | Fresh |
| More than 15 seconds | Stale |
| More than 60 seconds | No recent report |

These labels describe the age of evidence. A missing report does not prove process death, release capacity, renew a lease, or change a task's authoritative result.

A worker has one concurrency limit **N** and at most N occupied process slots across programs. Starting, warm, executing, retiring, cleanup-pending, and unknown ownership all count toward occupancy. Consumer reservations and occupied process slots are separate quantities.

Slot IDs stay stable within a session. Reuse preserves a process instance identity; replacement changes it. A PID is optional diagnostic data and can be reused by the operating system. Task and attempt links appear only after the server validates that they belong to the reported session and instance.

Detail pages contain at most 100 slots. Detailed reports support up to **1024** slots and a **2 MiB** snapshot. An embedded worker with greater capacity still reports its real capacity with detailed observation unsupported; unavailable reports never become invented empty slots. Only the latest normalized snapshot is stored, not an unbounded history.

See the [worker observation contract](https://github.com/Ledgence/ledgence/blob/develop/docs/worker-observations.md) for reporter, identity, and retention details.

## Serving Console

The [local tutorial](/tutorials/use-console) builds and serves Console through Compose. For a native source deployment, build assets separately using the pinned toolchain and [Console build instructions](https://github.com/Ledgence/ledgence/blob/develop/console/README.md). A regular Cargo build does not run frontend tooling.

Create an instance configuration file:

```json
{
  "instance_id": "local",
  "name": "Local instance",
  "suggested_queues": ["default"]
}
```

With `DATABASE_URL` set for the intended PostgreSQL database, run from the source checkout:

```sh
cargo run --locked -p ledgence-orchestrator -- migrate
cargo run --locked -p ledgence-orchestrator -- serve \
  --store /absolute/path/to/program-store \
  --bind 127.0.0.1:8080 \
  --instance-config /absolute/path/to/instance.json \
  --console-dir console/dist
```

Replace the paths with your actual store and saved configuration. On a new database, omitting the compatibility scope chooses `default/default`; legacy CLI, SDK, and worker requests must use that same binding. Existing data needs its original binding explicitly configured. See [One self-hosted instance](/concepts/self-hosted-console).

Keep `--instance-config` on every subsequent startup, including headless operation. Omitting `--console-dir` disables assets, not the persisted instance binding. Missing, modified, or incompatible assets fail startup before coordinators begin. Distribute the asset manifest and retained legal notices with the built files.

## Remote access

Protect both `/console/` and `/v1/` with the same access-controlled TLS reverse proxy. The instance binding is not operator authentication. Preserve those paths and serve the UI and API from the same public origin.

Configure that exact public origin in the server's `allowed_origins`, for example `["https://ledgence.example.com"]`. The server does not infer it from proxy headers. Requests from the CLI and workers without an Origin remain supported. Loopback listeners infer their local HTTP origins when this list is empty.

Do not rewrite API failures or missing assets into HTML. The orchestrator handles supported Console navigation routes. Its content security policy permits local scripts and reviewed inline styles, without inline JavaScript or evaluation.

## Upgrades

The execution explorer uses Console contract version 3. Start matching server and assets; C2 bundles fail explicitly. `kind: "entrypoint"` replaces `phase` in explorer records, including their opaque IDs. The new migration converts only the Console projection, preserving workflow/task identities, payloads, revisions, errors and timestamps. Old migration files retain their checksums. Every C2 Explorer cursor is rejected, including cursors ending at another node kind: restart from the first page. Other endpoint cursor bindings and SDK execution protocols are unchanged. Worker errors still use their separate `phase` wire field, displayed as **Failure stage**.

Existing `/agents` and workflow links remain supported. Migrations can lock writes, so budget maintenance time according to database size. This is a coordinated upgrade, not a rolling upgrade.

1. Back up PostgreSQL and the immutable program store using a tested restoration procedure.
2. Stop orchestrators and all other writers before migrating or binding an existing database.
3. Run the explicit migration with the chosen source version.
4. Start matching server, worker, and Console assets with the saved instance configuration.

`serve` verifies the complete schema and does not migrate it. A failed migration transaction rolls back its changes; do not start with a partial or incompatible schema. Downgrading to C2 requires matching offline backups, not running an older binary on the converted projection.

A database containing multiple historical bindings is rejected; migration does not delete or reassign records. Older binaries must not run concurrently against a bound database. Returning to a version that ignores the binding requires a coordinated offline rollback plan.

The catalog persists independently of execution-history retention. Retention does not turn a registered package into an execution or delete immutable artifacts as a side effect.
