# Console query model

Console metadata reads use a dedicated `ConsoleQueryStore` port. They never acquire tasks, renew leases, expire attempts or wake workflows. The application fixes the installation scope; the browser cannot supply it. Existing task and workflow responses are unchanged.

Each response is assembled inside a short PostgreSQL repeatable-read, read-only transaction. A response is coherent at that read; subsequent pages may reflect later committed state. Pages use an opaque cursor bound to the endpoint, installation scope, parent IDs, filters and persisted position. Limits default to 50 and cannot exceed 100. Catalog pages may return fewer items to stay within the 2 MiB metadata response limit; follow `next_cursor` whenever it is present, even on a short page. No query computes a total count or uses offset pagination.

Task discovery first selects a bounded metadata page, then loads its program descriptors in one batch. Attempt lists select authority-free fields and real claimed/dispatch timestamps from the durable history. Only explicit attempt inspection reads the bounded accepted settlement to project process and failure metadata; it returns neither output nor reusable lease authority. Legacy reports do not contain a stable process instance ID, so this field remains absent unless actually recorded by a supported protocol.

Workflow pages read durable activations, task ownership, owned workflows, waits, history and local-step records. Correlation is an exact filter, never a relationship. Both task and subworkflow child branches seek by creating revision and immutable keys, cap their shortlist independently, and merge at most twice the page limit plus two metadata rows. Retiring roots return not found before children are queried; retiring child targets are excluded. Empty valid parents return empty pages.

The console query migration adds compact queue and callable columns and creating-revision projections to existing workflow records. It backfills them once from already bounded submission/local-step bytes and persisted activation references; application input and output remain byte payloads. For this extraction only, a temporary SQL helper substitutes actual NUL escapes in a parsing copy because PostgreSQL JSON extraction rejects U+0000 in otherwise valid application data. It preserves escaped literal backslash sequences; queue and callable identifiers prohibit actual control characters. The helper is removed within the migration, and original payload bytes remain unchanged. This is not an ongoing payload conversion. New writes maintain the projections. The migration adds ordered workflow discovery, relation, wait and attempt-history indexes. It uses the existing explicit transactional migration process and can block writes; stop writers and budget maintenance time according to database size. It does not promise a zero-downtime upgrade.

The new API serializes revision and sequence values as decimal strings. Program versions remain opaque exact strings. Unknown timestamps and unrecorded process identity are null rather than estimates. Claimed time and worker elapsed time are labeled separately; neither queue age nor an active scheduling state proves that a process is currently executing.

## Unified discovery and execution explorer

Console contract version 2 adds endpoints without changing the legacy DTOs:

- `GET /v1/console/executions` merges tasks and workflows in a stable descending
  order of submission time, kind and immutable ID. Each typed index seek is
  bounded before descriptor hydration; the browser does not merge unrelated pages.
- `GET /v1/console/programs/catalog` filters on any registered version's declared
  kind, rather than the editable program summary. Compact membership flags and
  immutable program/version columns support indexed filtering.
- `GET /v1/console/workflows/explorer` supplies bounded observed graph records to
  both Graph and Timeline. A response includes its observation time and a cursor
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

The explorer projection stores compact phases, children, forks, accepted local
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

These migrations require stopped writers and can hold locks while backfilling
metadata or building indexes. Start matching version-2 server and Console assets
after migration. Old bundles fail compatibility validation explicitly.
