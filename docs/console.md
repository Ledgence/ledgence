# Ledgence Console

Console is the self-hosted operator UI for one Ledgence instance. It serves static
files from the Rust orchestrator and calls the same instance's APIs. Production
needs no Node process, CDN, vendor account or hosted web service.

Console is included in Ledgence 0.3.1 source and Console-enabled native bundles.
The historical `v0.1.1` source tag and `0.1.0` native bundle predate it. The [guided Console tutorial](https://docs.ledgence.com/tutorials/use-console)
walks through startup and real executions. The [public reference](https://docs.ledgence.com/reference/console)
covers the interface and operational behavior.

## Open the local deployment

Follow [local deployment](local-deployment.md), including the explicit publication
command, then open [Console](http://127.0.0.1:8080/console/). Ledgence 0.7.0 includes Executions, Programs, Workers and Approvals.
The supplied demo uses one compatibility binding configured on the
server. There is no workspace, tenant or namespace selector in the browser.

Matching development builds use Console contract **6** and add **Effects** for
retained external-effect evidence and operator assessments. Published 0.7.0
bundles use contract **5**; upgrade the server, workers and assets together to use
the development capability.

- **Executions**: one history for tasks and workflows, filtered by type, status,
  program/version, queue, correlation, exact execution ID and submission time.
  Inspect input, output, attempts, resources and history; submit a program, run it
  again as new work, or request cancellation. Tasks offer Trace and General;
  workflows also offer Graph for entrypoints, children, local steps and waits.
- **Programs**: inspect registered packages, exact opaque versions, digests and
  runtime requirements. Registration is explicit and independent of execution.
- **Workers**: inspect server-received observations, stable process slots and
  validated execution links. Fresh/stale/no-recent-report describes the age of
  information; it does not establish process death or task authority.

Execution history loads more rows automatically as you scroll, without a row-count
selector or page-navigation controls. Its first page updates automatically. Loading
older results pauses those updates to preserve your place; **Refresh executions**
returns to the latest matching results with the same filters. Failed loads retain
the visible rows and offer **Retry loading**. Returning from a detail restores
cached rows and the previous scroll position.

Each keyset page reflects its own server read, not a snapshot frozen across all
pages. There are no synthetic fleet totals. Inputs and terminal results load on
demand; normal updates read bounded metadata. Hidden/offline pages pause polling.
The browser validates instance identity and contract version before accepting
responses.

Use **Collapse sidebar** to make room for inspection. Select the Ledgence mark
to expand it again. In 0.7.0, choose **System**, **Light** or **Dark** with the
**Appearance** control in the top bar. In development builds, find it in the
desktop sidebar or beside the navigation links on small screens. **System**
follows your device setting. The browser remembers the choice when local
storage is available.

The global history starts with root workflows and standalone tasks. **Include
child executions** adds ordinary child tasks and subworkflows; controller
activations remain inside their workflow. Program/version history and exact-ID
lookup include children automatically. Executions remain discoverable even when
their packages have not been added to the registry. Catalog type filters match
any registered version; mixed kinds and unspecified kinds stay explicit.

## Find approvals across workflows

The **Approvals** navigation item opens the inbox for
the bound instance. It lists pending requests by deadline and supports filters
for program, status and an exclusive deadline. Open a request to inspect or decide its exact action
and navigate to its workflow. [Approval inbox contract](workflow-approvals.md#approval-inbox).

## Explore a workflow

Graph and Trace use the same durable execution records. Select a node or row to
inspect it, then use **Open task** or **Open workflow** to drill into its execution. Back
restores the previous navigation entry; Up follows ownership. Breadcrumbs provide
direct access to ancestors. A child deep link works without visiting its parent.

Graph shows one workflow and its direct children: entrypoint invocations, tasks,
opaque subworkflow nodes, local operations, forks, joins and external waits. Each
entrypoint invocation is a node, not a group or band. Re-entry to the same handler
has its own activation ID; retries of one activation retain that identity. Opening
a subworkflow displays its own graph rather than recursively expanding the parent.

In C6, native effect-resolution waits appear as **Recovery** nodes in both views.
The registering entrypoint and any resumed entrypoint have separate recorded
edges. The inspector links to the original effect and its entrypoint; that origin
link does not invent a dependency on another branch. A closed wait without a
resumed activation remains distinct from a selected assessment. **Inspect effect**
opens the retained evidence, and Back restores the workflow's selection and
graph position.

Pan or zoom to inspect a larger graph. **Fit** shows the loaded graph; **Reorganize**
restores automatic card positions. Dragging a card changes only its presentation.
**Full screen** expands the explorer, with Escape to return. Selection, camera and
positions survive this change, and incoming status updates do not recenter the
canvas. Select a node to open its inspector or switch to **Trace** for recorded
intervals and milestones. Workflow details expose navigation to additional pages
without a row-count selector or Refresh control.

The graph does not reconstruct arbitrary Python statements or predict unexecuted
branches. A fork's members come from recorded branch keys. A task registered by
the parent after the fork remains direct work of its entrypoint. Local nodes
connect to the entrypoint that invoked them; they do not gain dependencies on
another local or a join based on their position. Typed edges distinguish invocation,
registration, branch membership, terminal-outcome waits and resumption. Dashed
lines identify fork branches; other recorded relationships use solid lines. A
complete entrypoint-to-fork-to-child path replaces its redundant direct invocation
line in the canvas, while the inspector retains both recorded relations.
Correlation and timestamp order never establish a dependency. The [query model](console-query-model.md) lists the required evidence.

Trace distinguishes recorded intervals from acceptance milestones. Submission
to terminal includes queue and wait time; an accepted local result alone does not
provide its runtime interval. A closed wait alone does not prove a successful
wake. Rejected controller decisions do not create execution edges, and workflow
`failing`/`cancelling` remain nonterminal while children drain.

The explorer reads bounded pages and identifies records not yet loaded. Retained
references can remain visible when their target has become unavailable; generic
unavailability does not prove retention removed it. No graph depends on exported
OpenTelemetry traces. Older histories may lack observations that were never
recorded, and the Console does not fabricate them.

Resources are inspected per attempt. CPU covers the Python process, excluding
subprocesses; memory is explicitly a process-lifetime high-water mark, not an
invocation peak. Local lifecycle observations arrive with the accepted attempt
report rather than as a live stream. See [execution observations](execution-observations.md)
for scope, bounds, replay and unavailable-measurement behavior.

## Native startup

Build assets explicitly with the pinned toolchain in [Console build guide](https://github.com/Ledgence/ledgence/blob/develop/console/README.md),
or use the `console/` directory from the matching 0.7.0 native bundle. The historical native `0.1.0` bundle has no Console assets. A regular Cargo
build/test does not run frontend tooling. Create a server-only instance file:

```json
{
  "instance_id": "local",
  "name": "Local instance",
  "suggested_queues": ["default"]
}
```

For new installations the omitted compatibility scope is `default/default`.
Legacy SDK and worker calls must match it. For existing data, configure its actual
binding before starting; see [binding and migration](self-hosted-instance.md).
With `DATABASE_URL` set for the intended database, from a source checkout:

```sh
cargo run -p ledgence-cli -- orchestrator migrate
cargo run -p ledgence-cli -- orchestrator serve --store /path/to/program-store \
  --bind 127.0.0.1:8080 --instance-config /path/to/instance.json \
  --console-dir console/dist
```

For a newly built native bundle use `bin/ledgence orchestrator serve` and
`--console-dir /absolute/path/to/bundle/console`. Keep `--instance-config` on every
startup. In a complete 0.7.0 native bundle, `--instance-config` automatically
selects the bundled Console when `--console-dir` is omitted. To run without
assets, use a source build or a bundle packaged with `--headless`, and omit
`--console-dir`; the instance binding remains required. A missing,
modified or incompatible static asset fails startup before coordinators begin.
The console source manifest and retained notices accompany the assets.

## Register immutable programs

Ledgence 0.7.0 can prepare and upload programs from the CLI with
[`program build` and `publish --server URL --register`](program-publication.md).
The Console registration form still registers an existing store reference; it
does not become a code uploader. Publication must be explicitly enabled on the
server, and previously saved kits do not gain that capability automatically.

Publish a prepared package into the configured store first. Register its exact
reference against the running instance:

```sh
ledgence program register --server http://127.0.0.1:8080 \
  --program invoice-issuer --version 1.0.0 --kind task
ledgence program register --server http://127.0.0.1:8080 \
  --program invoice-workflow --version release-september --kind workflow
```

Registration fetches and verifies the archive/descriptor/manifest without executing
it. Packages for another supported host platform can be registered; a worker must
still satisfy their actual runtime requirements. The catalog does not enumerate
private stores or upload/compile code. Package versions are immutable and are not
assumed to be SemVer. Declare `task`, `workflow` or `unspecified` explicitly; the
runtime protocol does not classify business intent.

Registration of identical metadata and bytes is idempotent. Use
`--update-metadata true` deliberately to change descriptive metadata for an
existing reference. Changing its digest is a conflict and needs a new version.
If publication succeeds but registration fails, retain the immutable artifact and
repeat registration to reconcile. Do not delete it as a rollback. The Compose
publish command performs these two steps in this order.

## Commands and observation

Transport timeouts can occur after a mutation commits. Console never automatically
retries a write. Its explicit retry retains the exact in-memory body and identity;
editing the intended submission requires a separate action. Navigating away can
lose that in-memory command. **Run again** creates a new submission and new key.
Business effects remain at-least-once; application idempotency is still necessary.

User JSON preserves signed/unsigned 64-bit integers and floating categories,
including successful `null`, `1.0` and `-0.0`. Platform metadata stays outside
user `data`. A pending result is distinct from a completed null result.
Cancellation is a request: active cleanup or draining children can remain pending.
An event accepted by the server is distinct from a workflow that has resumed.

Worker observation uses a separate bounded reporter. A missing or stale report
does not renew a lease, release capacity, change recovery or stop program execution.
A healthy reused process keeps its identity; replacement gets a new identity.
Capacity above 1024 remains supported by execution, while detailed slot observation
is explicitly unavailable at that size. See [worker observations](worker-observations.md)
and [query contracts](console-query-model.md).

## Reverse proxy and upgrades

For the published 0.7.0 operator-trusted deployment, protect both `/console/`
and `/v1/` with the same access-controlled TLS proxy for remote access.
Development builds can use [instance credentials](instance-access.md) with a
TLS proxy and an explicit origin. Preserve
these paths and serve assets/API from the same public origin. Configure that
exact origin in the server's `allowed_origins`, for example
`["https://ledgence.example.com"]`; the server does not infer it from proxy headers.
CLI/worker requests without an Origin remain supported. Operator-trusted loopback
listeners infer local HTTP origins when the configured list is empty. Credential
access requires exactly one explicit Console origin.

Console's CSP allows local scripts only, without inline JavaScript or eval. Inline
CSS is allowed for the reviewed accessible dialog scroll-lock behavior. Do not
rewrite unknown assets or API errors into HTML. The orchestrator already handles
supported SPA deep links and exposes Notices through the UI.

Upgrade all writers together, back up PostgreSQL and immutable artifacts, run
explicit migrations, and start with matching instance configuration and assets.
Mixed historical bindings fail startup and require an offline operational decision.
No migration deletes or reassigns them. Rollback to an older binary that ignores
the persisted binding requires an offline plan; never run it concurrently against
the bound database. Catalog retention is independent of execution-history cleanup.

Published 0.7.0 uses Console contract version 5. The development explorer requires
contract version 6 and matching server and assets. Nodes include typed invocation, registration, fork-membership,
terminal-wait and resumption relations from the backend. The browser resolves
their references to loaded nodes; timestamps do not establish dependencies.
Local steps connect to their invoking entrypoint without becoming fork or join
members. Retries remain attempts of the same logical node. See the
[query model](console-query-model.md) for identity scopes and evidence limits.

The historical C3-to-C4 relation upgrade preserved existing node IDs without
a new database migration. C5 adds durable approval records and requires the
approval migration before serving matching assets. Stop writers before applying
migrations; `serve` verifies the schema but does not migrate it. Restart old
Explorer traversal from the first page. Existing execution identities remain
unchanged. C6 adds native effect-wait metadata to workflow details, wait pages and
the explorer. Apply the native resolution and Console native-wait migrations
before starting matching components. Native waits have a separate durable
namespace from ordinary events, even when their keys have the same text.
Useful `/agents` and workflow deep links remain available.

When upgrading from C2, a transactional migration converts retained Console records without
changing authoritative workflow, task, input/output or settlement records. Earlier
migration checksums are unchanged. Budget maintenance time for write locks; do not
mix historical writers/readers or incompatible assets. `serve` only verifies schema. If migration
fails, its transaction rolls back; downgrade requires matching offline backups.

## Qualification

The reproducible native acceptance gate uses only uniquely owned disposable
resources and requires a PostgreSQL server permitting database creation:

```sh
LEDGENCE_POSTGRES_URL=postgres://postgres:local-test@127.0.0.1:5432/ledgence \
LEDGENCE_PYTHON=/path/to/python3.14 \
python3 tools/check-console.py --binaries target/debug --console-dist console/dist \
  --evidence /tmp/ledgence-console-evidence \
  --browser-command pnpm --dir console test:live
```

Component fixtures supplement this real HTTP/PostgreSQL/worker gate. Container
recreation is checked by `tools/check-deployment.py`; relocated native bundles by
[release verification](releasing.md). Preserve their separate evidence and actual
platform/source identity. Running a mocked browser suite alone does not qualify a
deployment, and local compatibility tests do not certify AWS or cluster capacity.

## Approval review

Open **General → Approvals** in workflow details to inspect persisted action requests,
compare proposed and effective arguments, and approve or reject a pending
request. Only the server decides eligibility and expiry. After an uncertain
response, retry the frozen decision; the Console does not replace it with a
new command. See [durable workflow approvals](workflow-approvals.md) for the
API, identity and execution guarantees.

## Inspect an accepted operation

Open a local step in Graph or Trace to inspect its
accepted arguments and result on demand. The attempt that recorded the result
can differ from a later attempt that reused it. [Operation inspection](agent-recovery.md#inspect-an-accepted-operation).

## Credential access in development

Matching development components support optional installation credentials across
API operations, workers and Console. Configure the authority, client credential
files and capabilities using the [instance access guide](instance-access.md).
The existing operator-trusted mode requires its deployment access boundary.
Verified ingress identity and durable approval attribution have separate scope.


### Verified approval attribution

Development credential mode exposes `operator_audit` in access discovery.
Approval review and decision dialogs load an exact acceptance event separately
from the caller-supplied reviewer label. Loading, verified actor, successful
historical absence and unavailable attribution are distinct states. Changed
identity or capabilities clear cached attribution. The separate audit routes
did not change existing approval payloads or require the C6 upgrade by themselves.
[Operator audit](operator-audit.md).

## Review external effects in development

**Effects** lists retained approved occurrences by their original workflow,
activation and approval key. Its filters distinguish journal confirmation,
reported-value discrepancies and a disputed operator assessment. These are
different facts: a recorded assessment does not confirm the local result journal
or independently verify the provider's outcome.

Open an occurrence to inspect its approved action, program binding and bounded
timeline of claims, reported results and assessments. Follow additional evidence
pages when available; a short page can still have a next page. The metadata
inbox needs `metadata_read` in credential mode. Sensitive inspection requires
`sensitive_read` and durable operator audit before the server returns its payload.
The inbox also works in operator-trusted mode, but sensitive inspection does not.

With complete native resolution support and `effect_resolve`, the review page
can record success, failure or closure with an unknown outcome. Enter a reason
and evidence references, then choose **Review assessment**. The confirmation
dialog freezes the occurrence, effective action, evidence revision, predecessor
assessment and result. **Record assessment** sends that command. An uncertain
reply offers **Retry same assessment**, preserving the exact command; refreshing
the evidence does not replace it. A new assessment requires a separate review.
Use **Download saved command** in the review dialog before sending, or after an
uncertain reply, to preserve it beyond the current browser session. The saved
file contains the reviewed action and result, scope, server destination and
resolution identity; it contains no credential. Store it with the same care as
the application data it contains. Console does not automatically write command
bodies to durable browser storage.

Recover the downloaded envelope with `ledgence effect resolve --file FILE` or
Python's `client.effects.restore_resolution(...)`, using the same canonical
server URL and scope. An uncertain command must be retried by the same stable
operator identity; a replacement credential for that identity is supported.
The destination is the browser's origin, so a different hostname, port or scheme
is not silently substituted. See the [saved-assessment CLI
instructions](cli.md#submit-a-saved-operator-assessment).

The receipt means the assessment was recorded. A matching native workflow wait
may resume later; recording it does not revive a cancelled workflow or dispatch
the provider action again. Its verified audit event is loaded separately from
the assessment's reason and references. Retained evidence survives execution
history collection and has no automatic expiry in this capability.
See [retained effect evidence and native waits](effect-evidence.md).
