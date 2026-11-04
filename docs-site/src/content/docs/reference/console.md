---
title: Console reference
description: Self-hosted Console views, query semantics, command behavior, process observations, and deployment requirements.
---

Console is the operator interface for **one self-hosted Ledgence instance**. The Rust orchestrator serves its static assets at `/console/`, and the browser uses same-origin `/v1/console/*` APIs. A deployed Console needs no Node process, hosted frontend service, or vendor account.

**Availability:** Console is included in Ledgence **0.8.0** source and Console-enabled native bundles. Use matching contract **7** server and assets. [Explore Ledgence Console](/tutorials/use-console) covers setup; [Upgrade to 0.8.0](/how-to/upgrade-to-0-8) covers existing deployments.

**Effects** provides retained evidence and operator assessment review. Graph and
Trace include native effect-resolution and provider-recovery waits.

**General → Approvals** reviews immutable actions and recorded decisions. See [Require approval before an action](/how-to/require-approval).

In the 0.8.0 Console, the expanded sidebar shows the connected server version.
Open **Executions** to find a run by name or ID and read its labeled status. On a
workflow, **Graph** distinguishes fork membership with dashed links. **Trace** adds
a relative time scale for comparing when the recorded steps ran. For a local step,
the badge shows its durable result when one exists. The bar describes the observed
attempt. A later failed or cancelled call can appear alongside an earlier accepted result.

## Views and actions

| View | Recorded information | Available actions |
| --- | --- | --- |
| Executions | Unified task/workflow history. Task details provide Trace and General; workflow details also provide Graph. Input, output, attempts, resources and durable history remain available. | Submit an exact program reference, run again as new work, request cancellation, inspect a child execution, send an event to a recorded workflow wait. |
| Programs | Registered program references, versions, digests, manifests, declared kinds and descriptive metadata. | Register a published reference, update descriptive metadata explicitly, open a submission form or matching execution history. |
| Approvals | Requests across workflows in this instance, ordered by deadline. | Filter by status, program or deadline, review the recorded action and follow its workflow wait. |
| Effects | Retained approved actions, worker reports, provider recovery and operator assessments. | Inspect evidence and submit a revision-bound assessment with the required capabilities. |
| Workers | Worker sessions, configured capacity, observation freshness, process slots, and validated task/attempt links. | Inspect the latest available observation and follow execution links. |

**Programs** includes tasks and workflows, with any-version kind filters and explicit mixed/unspecified kinds. Registration does not execute or upload a package. **Executions**, **Programs**, **Approvals**, **Effects** and **Workers** are the navigation destinations. Worker inspection does not provide a drain, kill, or scaling command.

Workflow **Graph** and **Trace** use the same retained execution records. Graph shows one workflow and its direct children; a subworkflow is an opaque node that opens its own graph. Select work to inspect it; open a child execution to drill in. Back restores the previous navigation entry, Up follows ownership, and breadcrumbs select ancestors. Completed work remains visible. Correlation keys and timestamps do not prove dependencies. The graph does not predict future branches or edit a workflow definition.

Every entrypoint invocation is a node identified by its activation ID. Re-entering the same handler creates a distinct invocation; retrying one activation does not. There are no phase containers. A recorded local step connects to the entrypoint that invoked it; that relation does not imply an execution order between local calls. Only recorded branch members belong to a distributed fork. A separate parent task is not another branch. Joins wait for terminal outcomes, including failure or cancellation. Rejected decisions do not establish applied edges, and a closed wait alone does not prove a successful wake. Partial pages and unavailable references remain explicit. The graph works without exported OpenTelemetry traces.

Edges distinguish invocation, registration, branch membership, waiting for terminal outcomes, and resuming an entrypoint. Dashed lines identify fork branches; other recorded relationships use solid lines. When a complete entrypoint-to-fork-to-child path is loaded, the canvas omits its redundant direct invocation line. The inspector and accessible relationship list retain the underlying evidence. Joins and waits remain their own records. See the [relationship evidence matrix](https://github.com/Ledgence/ledgence/blob/v0.8.0/docs/console-query-model.md#entrypoint-identity-and-causal-evidence).

## Navigation and graph controls

Use **Collapse sidebar** to make room for the workflow. Select the Ledgence mark to expand it again. Choose **System**, **Light** or **Dark** from **Appearance** in the sidebar. **System** follows your device setting.

On small screens, **Appearance** sits beside the navigation links. Lists open directly with their content. Execution details keep the title, Back button and links to parent executions together.

In Graph, pan, zoom or drag cards to inspect the recorded execution. **Fit** frames the loaded nodes; **Reorganize** restores automatic positions. Dragging does not change the workflow or its dependencies. Incoming status updates preserve the camera and existing card positions.

**Full screen** expands the explorer; select **Exit full screen** or press Escape to return. Selection and canvas state are retained. Select a node to open its inspector, or switch to **Trace** to compare recorded intervals and acceptance milestones. Child-workflow navigation opens another level instead of expanding nested graphs inside the current canvas.

Node shapes distinguish entrypoint invocations, work, and compact fork/join records. Child durations run from submission to terminal outcome, including queue and wait time. Local durations describe the observed callable interval; **Replay** means the recorded result was reused.

**Recovery** nodes show native effect-resolution waits. Their edges
identify the entrypoint that registered the wait and, when selected, the
entrypoint it resumed. The original effect is a separate inspection link; its
location or timestamp does not imply a dependency on another branch. Use
**Inspect effect** to read its retained evidence. Back restores the workflow's
selection and graph position. A closed wait without a resumed activation remains
explicit, and closing with an unknown outcome is not displayed as success.

Provider-operation waits have their own node and durable namespace. The selection
records a provider outcome, operator assessment, dispute or **Needs review**;
only an actual selection connects to a resumed entrypoint. **Provider check**
badges distinguish the controller's reconciliation tasks in execution history and
details. Their evidence link opens the original approved occurrence. A successful
check means its report was recorded, not that the provider operation succeeded.
These tasks cannot be submitted again with **Run again**; the recovery controller
allocates them within the reviewed budget.

## Filters and pagination

Execution discovery supports task/workflow kind, exact state, program/version, queue, correlation and execution ID, plus submission time bounds. The default scope is root workflows and standalone tasks; Include child executions adds ordinary tasks and subworkflows. Program history and exact-ID lookup include children automatically. Controller activations remain inside their workflow. Unregistered programs retain execution history. The server interval is `[submitted_from, submitted_until)`. Inclusive UTC calendar-day filters use the next UTC midnight as the exclusive upper bound. Invalid ranges and kind/state combinations are rejected.

An unset correlation filter differs from filtering for an empty correlation string. Enable the form's correlation checkbox when applying either a nonempty or empty exact value. These are exact filters, not full-text search.

Execution history uses infinite scrolling: reaching the end loads the next page without a row-count selector or numbered pages. If loading fails, existing rows remain visible and **Retry loading** retries the read. Changing filters starts a new traversal; returning from a detail restores cached rows and scroll position. Inactive cached queries expire after five minutes, and reloading the browser starts a fresh traversal.

The API still uses bounded keyset pages, defaulting to **50** items with a maximum of **100**. Opaque cursors are bound to the resource and filters. Each response is coherent at its read, while later pages may see newer committed data. There are no synthetic total counts or a frozen snapshot across navigation. Workflow details retain navigation to additional pages, without a row-count selector or Refresh control.

Execution discovery refreshes its first live page automatically. Loading older pages pauses automatic updates to preserve your place; **Refresh executions** restarts at the latest matching results while keeping your filters. Worker pages refresh the latest observations. The program catalog does not poll on an interval. Hidden and offline pages pause polling. Terminal outcomes stop active execution and workflow polling. Input and output bodies load on demand, so ordinary refreshes do not repeatedly fetch execution payloads.

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

Inputs, results, events, and command bodies are not persisted to browser local storage or analytics. Appearance, sidebar collapse and Graph/Trace preferences may persist. Filters, resource identifiers and selected nodes can appear in navigation URLs; scroll, focus and graph presentation are retained in bounded navigation memory.

## Review durable action approvals

The **Approvals** navigation item opens the instance inbox.
See [Find pending requests across workflows](/how-to/require-approval#find-pending-requests-across-workflows).

Open **General → Approvals** in a workflow detail to inspect its persisted action requests.
Review the action name and version, effective arguments, optional original
proposal, and deadline before approving or rejecting. The decision binds the
stored request and effective action; it cannot replace the arguments. The server
determines whether the request is still pending and eligible.

After an uncertain reply, retry the retained decision unchanged. Approval records
permission to proceed; inspect the resumed workflow and its result to establish
what happened afterward. The reviewer field is claimed attribution.
Operator-trusted deployments provide their own authentication and authorization boundary.
Ledgence 0.8.0 supports [instance credentials](/how-to/configure-instance-access).
Native credential mode records verified acceptance separately from the reviewer
label. [Inspect operator audit](/how-to/audit-operators).

An application's generic `wait_event` review flow is an external wait rather than
a durable action approval. The support-agent and Codex change-review examples use
that pattern and their companion event commands. See
[Require approval](/how-to/require-approval) for the dedicated API.

<span id="review-external-effects-in-development"></span>

## Review external effects

Open **Effects** to find retained approved external writes. Filter the inbox by
journal confirmation, reported discrepancies or disputed assessments, then open
an occurrence. Its address consists of the workflow ID, original effect
activation and approval key. The review page shows the approved action, program,
worker claims, reported results and operator assessments. Follow additional
evidence pages until no next page remains; large entries can shorten a page.

Provider-operation filters add pending, succeeded, failed, cancelled, disputed and
review-needed recovery. **Needs review** includes operations requiring external
action. The approved binding identifies the provider target,
business operation, deduplication domain/key/horizon, completion criterion,
immutable program and finite recovery policy. Credential values stay outside the
binding; it contains a reference used by the program.

Recovery details show allocated checks, deadline, next check and stopped reason.
Each allocation consumes budget even if its worker fails before making a request.
The timeline distinguishes dispatch, probe allocation, callback source, typed
report and finalization. Provider-backed operations have no local result journal.
Pending or unknown reports do not prove failure, and a recorded response remains
an adapter's classified evidence rather than independently verified provider truth.

The inbox requires `metadata_read` in credential mode and also works in an
operator-trusted deployment. Opening sensitive evidence requires `sensitive_read`
and configured operator audit. The server records that read before returning its
payload. See [Inspect external-effect evidence](/how-to/inspect-effects) for the
access setup and equivalent CLI and Python operations.

An operator with `effect_resolve` can record success, failure or closure with an
unknown outcome when the server supports native resolution. Enter a reason and
references to the evidence you reviewed, then select **Review assessment**. The
dialog freezes the approved action, occurrence, evidence revision, predecessor
assessment and chosen result. **Record assessment** sends that exact command.

After an uncertain reply, **Retry same assessment** keeps the same identity and
body. **Refresh evidence** reads newer facts without silently rewriting the
pending command. **Start a new review** is a separate decision. The prepared
command is retained in bounded navigation memory. Choose **Download saved
command** before dispatch or after an uncertain reply to keep it after a browser
reload. The downloaded file contains the reviewed action and result, scope,
server destination and resolution identity, without credentials. Treat it as
application data; Console does not automatically persist it in browser storage.

The CLI can submit that file with `ledgence effect resolve --file FILE`; Python
can restore it with `client.effects.restore_resolution(...)`. Use the same
canonical server URL and scope, and the same stable actor when reconciling an
uncertain write. Credential rotation for that actor is supported. The exported
destination is the browser's origin: another hostname, port or scheme is not an
equivalent destination. [Saved-command CLI reference](/reference/cli) ·
[Python client](/reference/python-client).

A receipt means the assessment was recorded. It does not independently verify a
provider outcome, confirm the local result journal, issue another dispatch grant
or prove the workflow resumed. A matching native wait can resume according to
the workflow's code; cancelled workflows are not revived. The page loads the
verified acceptance event separately from the operator's reason and references.
Retained effect evidence survives execution-history collection and has no
automatic expiry in this capability.

## Attempt resources and local observations

Resources show available measurements for one attempt. Runtime elapsed excludes preparation and startup, while CPU counts the Python process and all its threads during the invocation. Child processes, including Codex, are excluded. Concurrent local functions share the process, so its CPU cannot be attributed exclusively to a single local function.

Memory is labeled **process lifetime peak memory**. Reused processes can retain peaks from earlier invocations; this is not an invocation peak and peaks must not be summed. Unavailable counters are distinct from zero. No provider billing estimate is inferred.

Local start, elapsed, failure and replay observations are delivered with the accepted attempt report. They are not a live stream of running functions. The accepted-result journal remains authoritative: a callable returning does not prove its result was durably committed, and replay does not execute it again. Older workers, interrupted processes and lost reports can leave measurements unavailable. See the [execution observation contract](https://github.com/Ledgence/ledgence/blob/v0.8.0/docs/execution-observations.md).

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

See the [worker observation contract](https://github.com/Ledgence/ledgence/blob/v0.8.0/docs/worker-observations.md) for reporter, identity, and retention details.

## Serving Console

All three [local installation paths](/getting-started/installation) serve the published Console. The [Console tutorial](/tutorials/use-console) explores the executions from the task and workflow lessons without rebuilding the platform. The 0.8.0 [native bundles](/how-to/install-native) include a matching `console/` directory, so you do not need to build frontend assets when using those bundles. For a source deployment, build assets separately using the pinned toolchain and [Console build instructions](https://github.com/Ledgence/ledgence/blob/v0.8.0/console/README.md). A regular Cargo build does not run frontend tooling.

For a manually operated service, create an instance configuration file named `instance.json`:

```json title="instance.json"
{
  "instance_id": "local",
  "name": "Local instance",
  "suggested_queues": ["default"]
}
```

For this manual setup, `DATABASE_URL` supplies the PostgreSQL connection string
to migrations and the server. Set it in the service environment for your intended
database; the local Docker kit configures its own connection automatically.
With the CLI installed and those settings in place:

```sh
ledgence orchestrator migrate
ledgence orchestrator serve \
  --store /absolute/path/to/program-store \
  --bind 127.0.0.1:8080 \
  --instance-config /absolute/path/to/instance.json \
  --console-dir /absolute/path/to/console-assets
```

Replace the paths with your actual store, saved configuration and Console assets. A source build produces assets in `console/dist`; a complete native bundle can select its bundled assets automatically as described below. On a new database, omitting the compatibility scope chooses `default/default`; legacy CLI, SDK, and worker requests must use that same binding. Existing data needs its original binding explicitly configured. See [One self-hosted instance](/concepts/self-hosted-console).

Keep `--instance-config` on every subsequent startup, including headless operation. In a complete 0.8.0 native bundle, supplying `--instance-config` automatically selects the bundled Console when `--console-dir` is omitted; omission does not disable it. A source build or a bundle packaged with `--headless` can omit `--console-dir` to run without assets while preserving the instance binding. Missing, modified, or incompatible selected assets fail startup before coordinators begin. Distribute the asset manifest and retained legal notices with the built files.

## Remote access

Operator-trusted deployments protect both `/console/` and `/v1/` with the same access-controlled TLS reverse proxy. Ledgence 0.8.0 can use [instance credentials](/how-to/configure-instance-access) with TLS and an explicit origin. Preserve those paths and serve the UI and API from the same public origin.

Configure that exact public origin in the server's `allowed_origins`, for example `["https://ledgence.example.com"]`. The server does not infer it from proxy headers. Requests from the CLI and workers without an Origin remain supported. Operator-trusted loopback listeners infer their local HTTP origins when this list is empty. Credential access requires exactly one explicit Console origin.

Do not rewrite API failures or missing assets into HTML. The orchestrator handles supported Console navigation routes. Its content security policy permits local scripts and reviewed inline styles, without inline JavaScript or evaluation.

## Upgrades

Ledgence 0.8.0 uses Console contract version 7. The historical 0.7.0 explorer
uses version 5. Start matching server and assets; incompatible Console
bundles fail explicitly. Each node carries typed invocation, registration,
branch-membership, terminal-wait and resumption relations from the backend.
References remain available when an endpoint is outside the loaded page. A local
connects to its invoking entrypoint; that connection does not imply completion
or membership in a fork or join. Joins wait for terminal outcomes, including
failure and cancellation.

C5 was introduced with the 0.3.1 database migration for durable approvals. Version 0.7.0 adds two PostgreSQL migrations for child-program resolution and the approval inbox; follow the [upgrade guide](/how-to/upgrade-to-0-7) for your starting version. The earlier C3-to-C4 change preserved node IDs without adding a database migration. Restart Explorer traversal because older Explorer cursors are rejected. When upgrading from C2, the existing migration also converts Console `phase` records into `entrypoint`, preserving workflow/task identities, payloads, revisions, errors and timestamps. Worker errors still use their separate `phase` wire field, displayed as **Failure stage**.

C6 adds native effect-wait metadata and requires the native resolution and
Console native-wait migrations. Restart wait-page traversal on upgrade. Native
waits use a separate durable namespace, so an ordinary event with the same key
cannot resume them. The graph preserves existing execution identities and links
the parking and resumed activations only when those relationships were recorded.
See the [query contract](https://github.com/Ledgence/ledgence/blob/develop/docs/console-query-model.md#contract-5-to-6-upgrade)
for the exact response and migration changes.

C7 adds provider-wait metadata and required, nullable provider-check provenance
on task status, task details and execution history. Apply the provider-operation
migration, use matching server/assets, and restart wait-page traversal. Ordinary
events cannot satisfy a provider wait, even when their logical keys match. The
original effect and the entrypoint that parks can be different activations; the
graph preserves both identities.

The graph uses retained Ledgence records and works without an OTel backend. Local results are recorded durably on completion; abrupt process loss before result acceptance or a retained attempt report may leave no local node. Optional timing and tracing evidence must not be interpreted as a complete record of every started operation.

Existing `/agents` and workflow links remain supported. Migrations can lock writes, so budget maintenance time according to database size. This is a coordinated upgrade, not a rolling upgrade.

1. Back up PostgreSQL and the immutable program store using a tested restoration procedure.
2. Stop orchestrators and all other writers before migrating or binding an existing database.
3. Run the explicit migration with the chosen source version.
4. Start matching server, worker, and Console assets with the saved instance configuration.

`serve` verifies the complete schema and does not migrate it. A failed migration transaction rolls back its changes; do not start with a partial or incompatible schema. Downgrading to C2 requires matching offline backups, not running an older binary on the converted projection.

A database containing multiple historical bindings is rejected; migration does not delete or reassign records. Older binaries must not run concurrently against a bound database. Returning to a version that ignores the binding requires a coordinated offline rollback plan.

The catalog persists independently of execution-history retention. Retention does not turn a registered package into an execution or delete immutable artifacts as a side effect.

## Inspect an accepted operation

Open **Execution History**, select a workflow
and choose a local step in **Graph** or **Trace**. Open the operation details
to see the effective arguments and accepted result. Model and tool calls show
their saved callable and version. Ordinary local steps show the input and result
saved by the program. A default applied inside an ordinary Python callable
can be absent from the recorded input.

The recorded attempt and timestamp tell you when the result became durable.
To investigate reuse, check the observations for a later attempt. An absent
observation leaves that attempt's replay behavior unknown. Console retrieves
payloads only when the inspector is opened.

<span id="instance-access-in-development"></span>

## Instance access

Use scoped credentials with matching 0.8.0 components. The
[instance access guide](/how-to/configure-instance-access) covers provisioning,
Console identity, client files, capabilities, rotation and operating limits.


<span id="verified-attribution-in-development"></span>

## Verified attribution

Approval review and decision dialogs load recorded acceptance independently from the caller-supplied reviewer label. Loading, verified acceptance, historical absence and unavailable attribution have distinct displays. Identity changes clear protected cached attribution.
[Setup, queries and operating limits](/how-to/audit-operators).

## Resource admission in development (C13)

Matching development assets and server use C13 to show admission wait episodes
and accepted callback results. Offer and Start indicate readiness/authorization,
not evidence that a callback executed. Open an accepted operation for its
original occurrence's audited arguments/result. The published 0.8.0 Console
remains C7. [Admission guide](/how-to/control-resource-capacity).


### Protected provider admission (development C9)

C9 adds a required `subject` to admission wait metadata: `operation` for an
ordinary registered call, or `provider` with the immutable original provider
ticket. The wait preserves the approval's original activation even when capacity
is granted to a later activation. Historical persisted C8 waits are projected as
ordinary operations; use matching C13 server and browser assets.

Open an admission wait and choose **Inspect admission record** to load its
approved arguments and linked dispatch/report evidence through the audited
inspector. This read is on demand and requires the corresponding sensitive-read
permission. The capacity state and explicit call completion are shown separately:
a completed call can leave the provider's business operation pending, and an
unknown call can retain capacity even when a provider result was reported.
Inspect the protected operation ledger for business status. A ticket or an offer
does not authorize another dispatch.

Provider admission does not add a successful local-operation node. The ordinary
accepted-operation node remains reserved for a committed generic callback
result; recording a provider Start alone does not prove that its callback ran.
Published Ledgence 0.8.0 continues to use C7; C13 is the current development contract.

### Standalone task admission (development C13)

When the installation enables task admission, **New execution** offers an optional
named resource for a task. **Run again** preserves the requirement. In the task's
**General → Resource admission** section, inspect a single generation: waiting
and preparation are distinct from an authorized handler invocation. The original
task ID, source ID, revision and deferral count remain available without inferring
work from timestamps. Sensitive evidence is loaded on demand. Expired ownership
alone does not release an uncertain source. A worker's local quiescence does not
certify completion of external work.

## Queue concurrency in development

Limit outstanding attempts across worker replicas, independently of each worker’s local poll weights. Inspect occupied and uncertain slots and edit a limit from Console, CLI, HTTP, Python or MCP. [Limit a queue across workers](/how-to/limit-queue-concurrency).

### Resource offers (development C13)

Open **Resources**, then select a named resource. Its policy, occupancy and default queue group appear above the offer history. Each weighted offer records its source execution, policy revision and why that group was selected. Select the execution to see the task or workflow. This view requires authenticated resource admission and `metadata_read`. FIFO resources have no new weighted decisions.

### Shared run allowances (development C13)

On a budgeted workflow or task, open the budget view to inspect its root, calls, delegations, limit refusals and usage observations. Inspecting a child resolves to its root. These metadata reads require authenticated budget support and `metadata_read`.
