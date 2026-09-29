# Console query model

Console metadata reads use a dedicated `ConsoleQueryStore` port. They never acquire tasks, renew leases, expire attempts or wake workflows. The application fixes the installation scope; the browser cannot supply it. Existing task and workflow responses are unchanged.

Each response is assembled inside a short PostgreSQL repeatable-read, read-only transaction. A response is coherent at that read; subsequent pages may reflect later committed state. Pages use an opaque cursor bound to the endpoint, installation scope, parent IDs, filters and persisted position. Limits default to 50 and cannot exceed 100. Catalog pages may return fewer items to stay within the 2 MiB metadata response limit; follow `next_cursor` whenever it is present, even on a short page. No query computes a total count or uses offset pagination.

Task discovery first selects a bounded metadata page, then loads its program descriptors in one batch. Attempt lists select authority-free fields and real claimed/dispatch timestamps from the durable history. Only explicit attempt inspection reads the bounded accepted settlement to project process and failure metadata; it returns neither output nor reusable lease authority. Legacy reports do not contain a stable process instance ID, so this field remains absent unless actually recorded by a supported protocol.

Workflow pages read durable activations, task ownership, owned workflows, waits, history and local-step records. Correlation is an exact filter, never a relationship. Both task and subworkflow child branches seek by creating revision and immutable keys, cap their shortlist independently, and merge at most twice the page limit plus two metadata rows. Retiring roots return not found before children are queried; retiring child targets are excluded. Empty valid parents return empty pages.

The console query migration adds compact queue and callable columns and creating-revision projections to existing workflow records. It backfills them once from already bounded submission/local-step bytes and persisted activation references; application input and output remain byte payloads. For this extraction only, a temporary SQL helper substitutes actual NUL escapes in a parsing copy because PostgreSQL JSON extraction rejects U+0000 in otherwise valid application data. It preserves escaped literal backslash sequences; queue and callable identifiers prohibit actual control characters. The helper is removed within the migration, and original payload bytes remain unchanged. This is not an ongoing payload conversion. New writes maintain the projections. The migration adds ordered workflow discovery, relation, wait and attempt-history indexes. It uses the existing explicit transactional migration process and can block writes; stop writers and budget maintenance time according to database size. It does not promise a zero-downtime upgrade.

The new API serializes revision and sequence values as decimal strings. Program versions remain opaque exact strings. Unknown timestamps and unrecorded process identity are null rather than estimates. Claimed time and worker elapsed time are labeled separately; neither queue age nor an active scheduling state proves that a process is currently executing.

## Unified discovery and execution explorer

Console contract version 3 reuses the existing discovery and inspection endpoints:

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
results, applied child waits and external waits at existing transaction boundaries.
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

The browser builds edges from retained evidence, never from timestamps or array
order. No coordinates, presentation labels or additional edges DTO are persisted.

| Relationship | Required evidence |
| --- | --- |
| Entrypoint → fork | Accepted fork associated with that activation. |
| Fork → child | Matching `fork_key` and membership in `branch_keys`. |
| Entrypoint → child outside a fork | Same creating activation and an applied decision without error. |
| Entrypoint → child wait | Same activation and recorded `applied_at`. |
| Child → child wait | Child key included in `member_keys`; terminal does not imply success. |
| Child wait → entrypoint | Explicit `resumed_activation_id`. |
| Entrypoint → external wait | Recorded wait associated with the activation. |
| External wait → entrypoint | Recorded wake and explicit resumed activation; closure alone is insufficient. |
| Entrypoint → entrypoint | Applied `continue`, no error and explicit resumed activation. |

Locals retain their activation attribution and callable/acceptance observations.
These do not establish local-to-local or local-to-join dependencies. A parent task
registered after a fork is still direct work of its entrypoint, not a fork member.
Each explorer query covers one workflow and direct child references. Opening a
child queries that workflow independently; correlation does not establish ancestry.

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
migration. Start matching version-3 server and assets afterwards. The migration
is transactional and may hold write locks. `serve` verifies schema without
migrating it. C2 assets fail validation, and an older binary cannot be rolled back
onto the new schema; restore matching backups for an offline rollback.
