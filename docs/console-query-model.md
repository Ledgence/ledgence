# Console query model

Execution, program catalog and worker metadata reads use the `ConsoleQueryStore` port. They never acquire tasks, renew leases, expire attempts or wake workflows. The application fixes the installation scope; the browser cannot supply it. This projection is separate from the generic task and workflow APIs.

Each response is assembled inside a short PostgreSQL repeatable-read, read-only transaction. A response is coherent at that read; subsequent pages may reflect later committed state. Pages use an opaque cursor bound to the endpoint, installation scope, parent IDs, filters and persisted position. Limits default to 50 and cannot exceed 100. Catalog pages may return fewer items to stay within the 2 MiB metadata response limit; follow `next_cursor` whenever it is present, even on a short page. No query computes a total count or uses offset pagination.

Task discovery first selects a bounded metadata page, then loads its program descriptors in one batch. Attempt lists select authority-free fields and real claimed/dispatch timestamps from the durable history. Only explicit attempt inspection reads the bounded accepted settlement to project process and failure metadata; it returns neither output nor reusable lease authority. Legacy reports do not contain a stable process instance ID, so this field remains absent unless actually recorded by a supported protocol.

Workflow pages read durable activations, task ownership, owned workflows, waits, history and local-step records. Correlation is an exact filter, never a relationship. Both task and subworkflow child branches seek by creating revision and immutable keys, cap their shortlist independently, and merge at most twice the page limit plus two metadata rows. Retiring roots return not found before children are queried; retiring child targets are excluded. Empty valid parents return empty pages.

The console query migration adds compact queue and callable columns and creating-revision projections to existing workflow records. It backfills them once from already bounded submission/local-step bytes and persisted activation references; application input and output remain byte payloads. For this extraction only, a temporary SQL helper substitutes actual NUL escapes in a parsing copy because PostgreSQL JSON extraction rejects U+0000 in otherwise valid application data. It preserves escaped literal backslash sequences; queue and callable identifiers prohibit actual control characters. The helper is removed within the migration, and original payload bytes remain unchanged. This is not an ongoing payload conversion. New writes maintain the projections. The migration adds ordered workflow discovery, relation, wait and attempt-history indexes. It uses the existing explicit transactional migration process and can block writes; stop writers and budget maintenance time according to database size. It does not promise a zero-downtime upgrade.

The new API serializes revision and sequence values as decimal strings. Program versions remain opaque exact strings. Unknown timestamps and unrecorded process identity are null rather than estimates. Claimed time and worker elapsed time are labeled separately; neither queue age nor an active scheduling state proves that a process is currently executing.

**Development source:** Console contract **16** includes bounded live execution history
and execution-file inspection and download.
It requires matching server and assets from the development checkout; published
0.9.0 continues to use contract 14. See [execution progress](execution-progress.md)
and [execution files](execution-artifacts.md).

## Unified discovery and execution explorer

Ledgence 0.9.0 uses Console contract version 14, with typed native effect/provider
waits and these discovery and inspection endpoints. The historical 0.7.0 release
uses version 5.

- `GET /v1/console/executions` merges tasks and workflows in a stable descending
  order of submission time, kind and immutable ID. Each typed index seek is
  bounded before descriptor hydration; the browser does not merge unrelated pages.
- `GET /v1/console/programs/catalog` filters on any registered version's declared
  kind, rather than the editable program summary. Compact membership flags and
  immutable program/version columns support indexed filtering.
- `GET /v1/console/workflows/explorer` supplies bounded observed graph records to
  both Graph and Trace. A response includes its observation time and a cursor
  for records not yet loaded. The parent workflow revision is not a change cursor.
- `GET /v1/console/executions/ancestry` follows recorded ownership through the
  supported depth, including explicit unavailable references where identity survives.
- `GET /v1/console/workflows/input` lazily reads the original application input.
- `GET /v1/console/attempts/observations` reads compact accepted runtime
  measurements without loading the application result or settlement authority.

Global discovery defaults to root workflows and standalone tasks. Program and
exact-ID filters include ordinary children. Controller activation tasks are
excluded from business discovery and remain available through workflow details.
An unregistered program does not hide its executions. The time interval is
inclusive at its start and exclusive at its end. Filter changes invalidate cursors;
live inserts or status changes can affect later pages, which are not a frozen
snapshot of an entire traversal.

The explorer projection stores compact entrypoint invocations, children, forks, accepted local
results, applied child waits, external waits, native effect waits and provider waits at existing transaction boundaries.
Only successfully applied decisions create decision/resumption edges. An accepted
controller result, rejected decision, closed wait or matching correlation does not
establish one. Child status is read from its target at query time, avoiding a
mutable root-wide counter updated by every descendant.

Optional local lifecycle observations supplement the accepted-result journal.
They do not decide whether a result is replayed. Measurements can be unavailable
for old workers, interrupted processes and lost reports; acceptance time is not
substituted for an unrecorded runtime interval. See [execution observations](execution-observations.md).

Compact graph identities remain with their owning workflow until its bounded
retention cleanup. They can explain a reference after child detail is removed,
without retaining a second archive of application payloads. Historical metadata
is backfilled only from retained authoritative records; already removed identities
and unrecorded observations are not reconstructed. Target unavailability alone
does not establish the cause of removal.

## Entrypoint identity and causal evidence

Each `kind: "entrypoint"` record represents one logical invocation, identified by
`activation_id`. Its handler name is descriptive: re-entering the same name creates
another activation, while attempts and replay of one activation retain one node.
The opaque ID is canonically encoded from `[kind, workflow_id, activation_id, key]`.
Revisions and other u64 metadata remain decimal strings; timestamps are UTC Unix
milliseconds. Worker failure `phase` remains a separate wire field (Failure stage).

Each node includes a required `relations` array supplied by the backend. These
relations are projected from the node's retained metadata, never from timestamps,
array order or the presence of neighboring records in the current page. No extra
SQL queries, graph writes or presentation coordinates are needed. Serialized
relations count toward the existing 2 MiB response bound.

Each relation contains an opaque `id`, a typed `kind`, and `source`/`target`
references. References are scoped to the selected workflow and use existing keys:

| Reference kind | Fields |
| --- | --- |
| `entrypoint`, `child_wait` | `activation_id` |
| `child`, `fork`, `external_wait` | `key` |
| `local` | `activation_id`, `key` |
| `native_effect_wait`, `provider_wait` | `parking_activation_id` |

A child's key is workflow-wide; a local key is activation-scoped. These references
can identify an endpoint not loaded in the current page without fabricating its
node or querying its payload. Relation IDs encode
`[workflow_id, kind, source, target]`; consumers treat them as opaque. The carrier
node identifies the evidence supporting a relation. Multiple carriers can support
the same relation ID, for example a fork's branch list and a child's `fork_key`.
The browser merges their evidence references and resolves endpoints against loaded
nodes. Missing endpoints remain unresolved, rather than proving nonexistence.

| Kind and direction | Required evidence |
| --- | --- |
| `invokes`: entrypoint → child | Child record carries its original creating activation. |
| `invokes`: entrypoint → local | Local result acceptance or retained attempt observation attributed to that activation. |
| `registers`: entrypoint → fork | Accepted fork associated with that activation. |
| `branch`: fork → child | Retained fork `branch_keys` or child `fork_key`; each is an explicit membership fact. |
| `registers`: entrypoint → child wait | Recorded wait and its `applied_at`. |
| `awaits_terminal`: child → child wait | Child key included in `member_keys`; the target waits for the source's terminal outcome. Terminal does not imply success. |
| `resumes`: child wait → entrypoint | Explicit `resumed_activation_id`. |
| `registers`: entrypoint → external wait | Recorded wait associated with the activation. |
| `resumes`: external wait → entrypoint | Recorded wake and explicit resumed activation; closure alone is insufficient. |
| `registers`: entrypoint → native effect wait | Applied native wait owned by the parking activation. |
| `resumes`: native effect wait → entrypoint | Immutable selected resolution wake with its explicit resumed activation; closure alone is insufficient. |
| `registers`: entrypoint → provider wait | Applied provider wait owned by its parking activation. |
| `resumes`: provider wait → entrypoint | Immutable selected provider wake and explicit resumed activation; a response or closed wait alone is insufficient. |
| `resumes`: entrypoint → entrypoint | Applied `continue`, no error and explicit resumed activation. |

Invocation does not imply that its source completed before the target started.
Locals connect to their invoking entrypoint, but this does not establish
local-to-local or local-to-join dependencies. A parent task
registered after a fork is still direct work of its entrypoint, not a fork member.
Each explorer query covers one workflow and direct child references. Opening a
child queries that workflow independently; correlation does not establish ancestry.

Successful locals commit their result and projection before returning the result.
There is no durable local-start registration: abrupt process loss before result
acceptance or a retained attempt report may leave no local node. OTel observations
can enrich diagnostics but never supply missing orchestration relationships.
The graph represents retained execution evidence, not all possible code paths.

An already resumed single-child wait can be visually compacted only when the full
child/wait/destination chain is evidenced. The wait and evidence IDs remain in
Trace and inspection. Multi-member joins, pending waits and partial chains are not
compacted into invented edges. Missing records on a page do not prove absence.

Discovery time bounds are `[submitted_from, submitted_until)`. Inclusive UTC day
filters convert the last day to the next UTC midnight before querying. Invalid
ranges and incompatible kind/state combinations are rejected. Without a kind,
a state unique to tasks or workflows narrows discovery to that resource type.

## Contract 2 to 3 upgrade

Migration [`20260929000000_console_entrypoints.sql`](../crates/ledgence-adapter-postgres/migrations/20260929000000_console_entrypoints.sql)
transforms only the persisted Console projection:
both `kind='phase'` and its JSON discriminator become `entrypoint`. Activation and
workflow IDs, revisions, timestamps, errors and application payloads are preserved,
including legal escaped U+0000. Existing migration files and checksums are retained.
The new constraint rejects `phase` after conversion.

Changing the kind changes explorer sort order and node IDs. All prior Explorer
cursors are rejected using the new internal binding `workflows/explorer/v3`, even
if their last record was a child or fork. Start Explorer again from its first page.
The HTTP route remains `/v1/console/workflows/explorer`; unrelated cursor bindings
and SDK execution protocols are unchanged.

Stop all writers and back up the database and artifact store before explicit
migration. Start matching server and assets afterwards. The migration
is transactional and may hold write locks. `serve` verifies schema without
migrating it. C2 assets fail validation, and an older binary cannot be rolled back
onto the new schema; restore matching backups for an offline rollback.

## Contract 3 to 4 upgrade

C4 adds required backend-projected relations to each Explorer node. No additional
database migration or runtime protocol change is required on a C3 database; node
IDs, key scopes and persisted records are unchanged. Deploy matching C4 server
and assets together. C3 assets and older Console contracts are rejected explicitly.
The route stays `/v1/console/workflows/explorer`. Its cursor binding is now
`workflows/explorer/v4`, so restart Explorer traversal; unrelated cursor bindings
remain unchanged. Earlier schema migrations still apply when upgrading from C2
or older releases.

## Contract 4 to 5 upgrade

C5 adds the `approval` wait kind and `approved`, `rejected`, and `expired` wake
reasons. Apply `20261004000000_workflow_approvals.sql` with writers stopped, then
start matching server, workers, Python helper and assets. Existing node IDs and
relation directions are retained. The dedicated approval endpoints project the
persisted request and decision; application arguments are loaded only through
these explicit detail endpoints. Approval inspect/list operations hold a brief
shared workflow lock while reading the proposal and sampling database time, so
a concurrent decision cannot produce a false expired observation. See
[workflow approvals](workflow-approvals.md) for API and effect guarantees.

## Contract 5 to 6 upgrade

C6 adds required nullable `native_effect_wait` fields to workflow details and wait
rows, the `effect_resolution` wait kind, and `native_effect_wait` explorer nodes
and references. Historical C5 fixtures remain versioned separately; C5 tabs and
assets must not interpret C6 responses. Apply
`20261103040000_native_effect_resolution.sql` and
`20261103050000_console_native_effect_wait.sql` with writers stopped, after all
earlier migrations, then start matching server, worker and Console components.

A native wait records its parking activation, stable key, original effect
occurrence and nullable `after_resolution_id`. The current native wait is mutually
exclusive with a child or external wait. Its durable namespace is separate from
ordinary events and approvals. Equal key text does not identify the same wait,
and sending an ordinary event cannot select a native resolution wake.

Wait pages merge independently bounded legacy and native index seeks. Their
cursor position is `(registered_at, kind, key, activation_id)`; native rows use
the parking activation. This preserves distinct rows with equal keys and times.
Restart wait traversal on upgrade; its old two-field cursor is invalid. The
explorer retains its existing node identities and `workflows/explorer/v4` cursor
binding; its new node kind is handled by the C6 contract check.

A native graph record carries the original occurrence, registration and closure
times, and nullable selected resolution ID, outcome kind and resumed activation.
It contains no action arguments, provider output, assessment reason or selected
result. Its `activation_id` identifies the parking entrypoint; the occurrence's
activation identifies where the effect began. Registration and resumption edges
connect only the recorded entrypoints. The origin is an inspection link, not an
inferred scheduling dependency. Cancellation can close a wait without selecting
a wake. A selected `closed_unknown` or `disputed` outcome must not be presented
as successful provider execution.

## Contract 6 to 7 upgrade

C7 adds required nullable `provider_wait` on workflow detail and wait records,
the `provider_operation` wait kind, and `provider_wait` explorer nodes and
references. It also adds required nullable `provider_probe` on task status,
task detail and execution-history rows. Workflow rows in execution history carry null; probe
task markers contain the original effect occurrence, probe ID and decimal-string
generation. The task's provider role is projected directly from its stored
assignment, without inspecting application `data` or joining a provider history.

Apply `20261103060000_provider_operations.sql` after prior migrations with writers
stopped. Start matching C7 assets and server, and restart wait traversal. The
three wait namespaces each use an independently bounded ordered seek, merged by
`(registered_at, kind, key, activation_id)`. Equal text keys and timestamps remain
distinct. Existing execution identities and historical C6 fixtures are preserved.

Provider wait metadata carries its parking activation, original effect occurrence,
nullable `after_effect_revision`, registration/closure times, nullable selected
effect revision, selection kind and resumed activation. Selection kinds distinguish
provider success/failure/cancellation, operator assessment, dispute and need for
review. The graph contains no report result or approved arguments; audited Effects
inspection supplies those payloads. An original effect's activation can differ
from the activation that parks, and the origin link is not a scheduling edge.

The credentialed access snapshot adds `provider_operation`, independently of
actor permissions, only when the complete configured capability is available.
The legacy operator-trusted access response remains unchanged. C7 does not grant
provider dispatch authority to the browser.

<span id="effects-and-operator-assessments-in-development"></span>

## Effects and operator assessments

These operations use the retained evidence and verified operator ports, rather
than the metadata-only `ConsoleQueryStore`:

Within the `/console/` application, `/effects` opens the inbox and
`/effects/review?workflow_id=…&activation_id=…&approval_key=…` addresses one
occurrence. Query values are URL-encoded identifiers, not action payloads.

| Console route (POST) | Required access in credential mode | Response |
| --- | --- | --- |
| `/v1/console/workflows/approved-effects/inbox` | `metadata_read` | Bounded metadata page, 1–100 occurrences. Also available in operator-trusted mode. |
| `/v1/console/workflows/approved-effects/inspect` | `sensitive_read` plus configured operator audit | Approved binding, current assessment and up to four timeline entries per page, subject to a 1 MiB encoded-response limit. |
| `/v1/console/workflows/approved-effects/resolve` | `effect_resolve` plus complete native resolution support | Payload-free acceptance receipt, at most 16 KiB. |

Console requests omit installation scope; the server supplies it. Generic API
aliases omit `/console` and require their explicit scope. Platform u64 fields in
Console, including revisions, sequences, counters and program size, are canonical
decimal strings. Timestamps remain JSON numbers; application-owned JSON is not
rewritten. Detail pages may be shorter than requested to respect the byte limit;
follow every non-null `next_cursor`. Each inspection records a durable sensitive
read before response bytes are released. Audit queries do not recursively audit
themselves.

Provider recovery extends this same read family. Optional `provider_filter` is
omitted for A/B queries and cannot be null; provider-filtered traversals use their
own cursor binding. Provider summaries and timeline records distinguish knowledge,
handling, check allocation, actual callback sources and finalization. Platform
generations/revisions use decimal strings; bounded policy durations and check
counts remain numbers. A provider report's application result remains unchanged.

Resolution commands are limited to 256 KiB and bind the exact original action,
occurrence, evidence revision and predecessor assessment. Admission denial is
definitive; a failure after a possible commit remains uncertain. Retrying the
same command reconciles acceptance without changing the reviewed action or
revision. The metadata receipt proves recording, not workflow resumption,
provider confirmation or a new dispatch grant. A separate exact audit-event
query supplies verified acceptance attribution. See [the effect evidence
contract](effect-evidence.md) for immutable selection, conflict and retention
semantics.

Console can explicitly export a frozen assessment using the shared saved-command
envelope `{format: "ledgence.effect-resolution.v1", base_url, body}`. The
destination is the canonical browser origin. `body` is the generic API command
as UTF-8 JSON text: it includes the bound scope and an exact numeric u64 revision,
while the Console request uses a decimal-string revision and server-injected
scope. Both retain the same immutable assessment binding. The envelope is at
most 1 MiB and contains no authentication material. CLI/Python restore validates
destination and scope before sending the saved body, without re-reading or
refreshing its action or revision. Export is available before dispatch and for
an uncertain command; it is an explicit download, not automatic local storage.

## Approval inbox

`POST /v1/console/approvals/inbox` reads a bounded page of approval metadata
within the configured instance scope. It orders by deadline and includes the
workflow ID, wait key and owning activation for navigation. Detail reads and
decision reconciliation use the existing approval endpoints. An initial explorer GET can take `approval_key`
to seek directly to the recorded wait in the given workflow. Its first node is
the requested wait and its next cursor uses ordinary explorer paging. Supply
either the key or a cursor per request. A missing or retired target returns 404.
A changed status
can move a request out of a filtered view between page reads. [Inbox contract](workflow-approvals.md#approval-inbox).

## Inspect one local step

Ledgence 0.9.0 accepts `GET /v1/console/workflows/local-steps/inspect` with
`workflow_id`, `activation_id` and `step_key`. The server obtains the scope from
the instance. A successful response contains the local-step summary in `step`,
a `kind` of `local`, `model` or `tool`, a nullable `version`, and `arguments`
and `result` as application JSON. Use lossless JSON decoding. The summary
includes `attempt_id` and `accepted_at` (UTC epoch milliseconds). Both refer to
the accepted journal entry. For a recognized model/tool binding the response
unwraps the effective arguments and operation version. Other local records
return their stored input and a null version.

A missing, mismatched or retired identity returns 404. Invalid parameters return
400. An adapter without the optional read capability returns 503. Requesting the
detail reads one retained record and has no execution side effects.

<span id="admission-projections-development-c14"></span>

## Admission projections

`admission_wait` records a native wait episode, and `admission_operation` records
an accepted result with its original source activation/attempt. Existing
registers/resumes/invokes relationships retain their durable meaning. Replaying
the accepted result does not create another physical-operation node.
`POST /v1/console/admissions/inspect` accepts workflow_id, origin_activation_id
and key; the server binds scope and records sensitive-read audit before exposing
the frozen arguments or retained result. See [admission contract](resource-admission.md).


<span id="protected-provider-admission-development-c9"></span>

### Protected provider admission

C9 adds a required `subject` to admission wait metadata: `operation` for an
ordinary registered call, or `provider` with the immutable original provider
ticket. The wait preserves the approval's original activation even when capacity
is granted to a later activation. Historical persisted C8 waits are projected as
ordinary operations; use matching C14 server and browser assets.

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
Deploy matching C14 server and assets together.

<span id="standalone-task-admission-development-c14"></span>

### Standalone task admission

When the installation enables task admission, **New execution** offers an optional
named resource for a task. **Run again** preserves the requirement. In the task's
**General → Resource admission** section, inspect a single generation: waiting
and preparation are distinct from an authorized handler invocation. The original
task ID, source ID, revision and deferral count remain available without inferring
work from timestamps. Sensitive evidence is loaded on demand. Expired ownership
alone does not release an uncertain source. A worker's local quiescence does not
certify completion of external work.

<span id="cluster-wide-queue-concurrency-development"></span>

## Cluster-wide queue concurrency

A queue policy sets a shared maximum across worker sessions. New attempts reserve a slot through the same PostgreSQL transaction on integrated HTTP and external SQS claim paths. Console C14 exposes **Queues** for credential-bound operators. [Policy semantics, timeouts and operator recovery](queue-concurrency.md).

<span id="resource-offers-development-c14"></span>

### Resource offers

Open **Resources**, then select a named resource. Its policy, occupancy and default queue group appear above the offer history. Each weighted offer records its source execution, policy revision and why that group was selected. Select the execution to see the task or workflow. This view requires authenticated resource admission and `metadata_read`. FIFO resources have no new weighted decisions.

<span id="shared-run-allowances-development-c14"></span>

### Shared run allowances

On a budgeted workflow or task, open the budget view to inspect its root, calls, delegations, limit refusals and usage observations. Inspecting a child resolves to its root. These metadata reads require authenticated budget support and `metadata_read`.
