# Task discovery

Find tasks by scheduling state or business correlation through `GET /v1/tasks`, `ledgence task list`, or the Python client. Every read requires a tenant and namespace. It returns one bounded page of [TaskStatus](task-results.md#status) values, without application input, output, CloudEvents, or program package bytes.

```python
from ledgence.client import AsyncClient

async with AsyncClient("http://127.0.0.1:8080", tenant="acme", namespace="billing") as client:
    page = await client.tasks.list(state="failed", correlation_key="INV-1042", limit=50)
    for task in page.items:
        print(task.task_id, task.state, task.latest_attempt_id)
    if page.next_cursor is not None:
        next_page = await client.tasks.list(
            state="failed", correlation_key="INV-1042", limit=50, cursor=page.next_cursor,
        )
```

```sh
ledgence task list --server http://127.0.0.1:8080 \
  --tenant acme --namespace billing --state failed \
  --correlation-key INV-1042 --limit 50
```

The CLI writes a JSON page object to stdout, with `items` and a `next_cursor` that is null or a continuation token. It writes `Request-Id` diagnostics to stderr. It makes one request; it does not fetch every page. The Python `TaskPage` is immutable, with an `items` tuple and nullable `next_cursor`.

## Filters and limits

HTTP query fields use the names below; CLI options replace underscores with hyphens and use `--tenant` for `tenant_id`. Python fixes tenant/namespace when constructing the client. All supplied filters combine with AND.

| Field | Meaning |
| --- | --- |
| `tenant_id`, `namespace` | Required exact scope; existing 128 UTF-8 byte identifier limits apply. |
| `state` | One of `queued`, `active`, `succeeded`, `failed`, `cancelled`. |
| `queue` | Exact queue, up to 128 UTF-8 bytes. |
| `correlation_key` | Exact business correlation, up to 512 UTF-8 bytes. Empty string matches empty keys; omission applies no filter. No substring search or normalization. |
| `submitted_from` | Inclusive lower submission timestamp, UTC milliseconds since Unix epoch. |
| `submitted_until` | Exclusive upper timestamp. When both bounds are supplied, from must be less than until. |
| `limit` | Integer 1–100; default 50. |
| `cursor` | Opaque continuation token, at most 8192 UTF-8 bytes. Repeat the original scope and filters; page size may change. |

Timestamps range from 0 through 253402300799999. HTTP numbers contain unsigned decimal digits. Unknown, duplicate, malformed, or invalid query fields return InvalidInput; GET bodies are rejected. The list query string is bounded to 16 KiB and the response body to 2 MiB. Both `items` and `next_cursor` are required response fields. An empty result is a successful empty page, including when no task exists in the requested scope.

## Pagination while tasks change

Ordering is newest submission first: `submitted_at DESC`, then task ID in descending UTF-8 byte order. Task IDs resolve timestamp ties. A cursor starts strictly after the last returned position in this ordering. Cursors bind scope and all filters, survive service restarts for the current cursor version, and do not depend on the boundary task still existing. Treat them as opaque; their encoding is not a client extension point.

Each page is one committed database statement snapshot. Pages do not share a frozen snapshot. State changes may cause tasks in the remaining range to enter or leave the results. Newer submissions above a cursor are seen on a fresh first page. A late commit or a task becoming eligible above an already passed boundary may be missed during that traversal. Returned tasks are not repeated when continuing with the server's cursors and immutable ordering fields.

A non-null cursor means another matching row existed when that page was read. The next page may be empty after concurrent changes. A null cursor means no further matching row existed at that read. It does not establish that no matching work can appear later. Listing does not expire leases or mutate task state; use by-ID status/result reads and bounded waiting to follow execution. It is not a work-acquisition or coordination API.

## Storage and adapters

Portable `TaskFilters`, `TaskListQuery`, `TaskPage`, and the required `TaskService::list_tasks` / `TaskStore::list_tasks` methods define discovery independently of PostgreSQL. Custom adapters must enforce filter, ordering, page-size, and cursor semantics; the application service validates both queries and adapter responses. CloudEvent `data` stays user-owned and unindexed by this feature.

The PostgreSQL adapter uses bound conditional predicates and keyset seeks. It selects at most `limit + 1` task rows before looking up their latest attempts through the existing `(task_id, generation)` index. A separate migration adds scoped ordering indexes for unfiltered, state, queue, and correlation queries. Combined filters may use an index plus residual filtering; scan cost depends on data distribution. These indexes add storage and write work. No exact total count or arbitrary metadata/program filter is provided.

Run the explicit `ledgence-orchestrator migrate` before starting the updated server. Migrations have a separate ten-minute default budget; use `migrate --timeout-ms 1800000` to allow thirty minutes for a larger existing database. See [migration budgets and interruption](postgres.md#migration-execution-budget). The discovery migration adds normal indexes transactionally, preserving task data. Index creation can block writes while it runs; schedule the migration accordingly. This is not a concurrent-index rollout. Startup continues to verify migration checksums rather than changing the schema automatically.

## Verification

The PostgreSQL gate exercises ties, scope isolation, exact/empty correlation keys, submission bounds, malformed cursors, changing states, new submissions, a concurrent uncommitted transition, and upgrades from the initial schema with existing tasks. Its planner regression uses 100,000 tasks and 100,000 attempts to check indexed first/deep pages and bounded latest-attempt hydration. This is a regression fixture, not a production latency guarantee.

HTTP/CLI contract tests and installed Python-client acceptance cover cross-language filters and continuations against actual Rust services and PostgreSQL. Existing request deadlines, bounded decoding, no-store replies, tracing, and request diagnostics also apply to discovery.
