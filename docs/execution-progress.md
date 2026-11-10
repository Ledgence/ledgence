# Resumable execution progress

**Development source for the next release.** This capability is not included in
0.9.0 binaries, images or packages. Use matching server and client source builds.

Read lifecycle facts already committed for one task or workflow. Progress reads
never run recovery, allocate a worker, resubmit an execution or change its state.
The optional `ExecutionProgressReader` port is implemented by the PostgreSQL
adapter and the HTTP client. Other adapters explicitly report unavailable until
they implement the complete capability.

## HTTP contract

`POST /v1/executions/progress` is a read-only metadata operation. It requires
`metadata_read` when credentials are enabled. Every request checks the principal
and bound installation scope; possession of a cursor grants no access.

```json
{
  "scope": {"tenant_id": "acme", "namespace": "demo"},
  "target": {"kind": "task", "id": "TASK_ID"},
  "cursor": null,
  "limit": 100,
  "wait_ms": 0
}
```

Use `kind: "workflow"` for a workflow ID. `limit` defaults to 100 and accepts
1–100. Optional `wait_ms` defaults to zero and accepts 0–20,000. A positive value
allows the service to wait for new retained events, up to that budget; adapters
may return sooner. Backlog and terminal snapshots return immediately. It does
not change cursor identity. The body is bounded to 8 KiB. The response is bounded to 256 KiB:

| Field | Meaning |
| --- | --- |
| `scope`, `target` | The requested execution, never another tenant or descendant. |
| `state`, `terminal` | Committed logical state at this read snapshot. |
| `high_watermark` | Last committed lifecycle sequence at that same snapshot, as a decimal string. |
| `events` | Consecutive retained records after the requested cursor, up to the limit. |
| `next_cursor` | Opaque consumed-through checkpoint, always present, including on empty pages. |
| `has_more` | More records remain through this snapshot's high-watermark. |

Each event has `sequence` (a decimal u64 string), `at` (UTC milliseconds), `reason`,
`activation_id` and `attempt_id`. Nullable IDs are explicit. Task events identify
an attempt when applicable; workflow events identify an activation when
applicable. Sequences remain lossless above JavaScript's safe integer limit.
Identity is scope + execution kind/ID + sequence. Timestamps do not establish
ordering or dependencies between branches.

The cursor binds the scope and target. Pass it unchanged with the next request;
page size can change. The server rejects malformed, mismatched or ahead-of-tail
cursors with `InvalidInput`. Missing or retired executions return `NotFound`.
Invalid adapter snapshots or backend failures return `Unavailable`, never a
successful empty page. Normal HTTP request IDs, no-store headers and the
30-second exchange budget apply.

## Reconnect and terminal behavior

Process a complete page and then persist its `next_cursor`. A disconnect can
redeliver a page when the caller has not saved its checkpoint. Consumers requiring
idempotent application of events must deduplicate using the identity above.
Neither the transport nor the SDK promises exactly-once business effects.

`has_more: false` means caught up **through the observed snapshot**. It does not
mean the history is permanently closed. For example, a task failed by a budget
fence can receive an authenticated cleanup confirmation after becoming terminal.
A follower may stop after draining a terminal snapshot, or keep observing to see
later operational events. It can always reconnect using its saved cursor while
the execution remains retained.

Pages use live snapshots. New commits can extend the tail between requests;
existing records keep their identities. Follow a child execution separately.
Workflow history includes its existing starts, decisions, waits, forks and
coordination transitions, not every child attempt or durable local result.
This endpoint contains no input/output, application log messages or model tokens.

## Python

Both `client.tasks.handle(id)` and `client.workflows.handle(id)` expose:

```python
page = await execution.progress(cursor=saved_cursor, limit=50)

async for page in execution.follow_progress(
    cursor=saved_cursor, poll_interval=1.0, timeout=60, limit=50,
):
    for event in page.events:
        print(event.sequence, event.reason)
    saved_cursor = page.next_cursor  # Persist after processing this whole page.
```

`follow_progress` yields frozen, bounded page objects. Sequence values are Python
integers; `state` is the existing task/workflow enum. It reads backlog immediately
and uses server waits only when caught up. `wait_ms` defaults to 20,000 for
following and zero for a single `progress()` call. Set it to zero to use immediate
polling. Each request wait is capped by the remaining request/observation budget,
leaving one second for the reply. The minimum request cycle interval is controlled
by `poll_interval` (at least 0.25 seconds; default one second). Time spent reading
and processing a page counts toward that interval, so a long wait adds no extra
polling sleep. Fast empty replies remain paced, including late terminal cleanup. Pull iteration provides backpressure:
there is no background receiver or unbounded event buffer.

The default `stop_on_terminal=True` stops after a terminal snapshot is drained.
Set it to `False` to continue until cancellation or timeout. `ProgressTimeout`
contains the execution handle, last yielded page and its `next_cursor`; delivery
is not an application acknowledgment. A timeout or cancellation stops observation
only. Transport, protocol, authorization and retention errors propagate; there is
no hidden resubmission or infinite reconnect loop. Existing `subscribe()` methods
still register durable terminal webhooks.

The [runnable example](../examples/execution-progress/README.md) saves a cursor
and resumes in a new process without submitting new work.

## CLI and MCP

```sh
ledgence task progress --server http://127.0.0.1:8080 \
  --tenant acme --namespace demo --task TASK_ID --limit 50
ledgence workflow progress --server http://127.0.0.1:8080 \
  --tenant acme --namespace demo --workflow WORKFLOW_ID --follow
```

Pass `--cursor CURSOR` to resume. `--follow` emits JSONL pages with events or a
changed observed snapshot; it does not repeatedly print unchanged empty pages.
`--wait-ms` accepts 0–20,000, defaulting to 20,000 for follow and zero for a single
page. The first read and backlog reads in follow mode are immediate. `--poll-interval-ms` is the
minimum request cycle interval (default 1000, minimum 250), not an extra delay
after a long response. The wait is shortened to respect the observer deadline.
`--timeout-ms` bounds observation. `--keep-following` continues beyond a drained
terminal snapshot. Interrupting or failing to write the output stops the observer
without cancelling the execution. A page has a five-second stdout delivery
budget; the next read starts only after the complete page is written.
`--timeout-ms` bounds observation, not total process shutdown or blocked stderr
diagnostics. Supply `--credentials-file` where required.

MCP exposes `ledgence_execution_progress` as a bounded read-only tool. It accepts
an execution kind/ID, cursor and limit within the MCP server's configured scope.
It never holds a tool request open for the execution's entire lifetime.

## Console

Matching development Console contract **15** adds a live **Lifecycle** timeline
under **Trace** in task details and **General** in workflow details. The browser uses a metadata-authorized, same-origin
`POST /v1/console/executions/progress` with the installation scope supplied by the
server. The `execution_progress` capability is false when the complete reader is
unavailable; the existing paginated history remains the fallback. Browser waits
use five seconds within the existing ten-second exchange budget.

The feed keeps a bounded window and deduplicates replay by durable sequence.
A transient reconnect retains the accepted cursor. Reloading or navigating back
rebuilds the view from retained history; progress and credentials are not saved
to browser storage. Earlier history remains explicitly accessible. Navigation,
logout and leaving the active view stop its observer. Retirement is displayed as
unavailable history; the UI never silently resets a rejected cursor. Graph and
trace views continue to use their existing durable relationship contracts.

## Retention and performance

Progress reuses `task_history` and `workflow_history`, whose per-owner sequence is
incremented in the same transaction as the transition. It adds no table, migration,
trigger or normal-execution write. A repeatable-read, read-only transaction reads
compact owner columns and an indexed bounded history range. It never loads the
program descriptor, CloudEvent, input, output or settlement payload.

History currently shares the execution's retention lifetime. Active executions
keep their history; terminal executions retain at least 90 days, subject to the
[retention contract](retention.md). Retirement hides the entire execution before
physical collection, so a reader sees a coherent old snapshot or `NotFound`.
There is no independently expiring history cursor or partial history truncation.
A retired execution cannot be recovered by silently resetting the cursor.

Positive waits share a request-owned coordinator in each application-service
instance. Identical scope, execution, cursor and limit share one authoritative
probe per one-second cycle. Wait durations remain independent. A new observer never receives a snapshot
from before its registration. If its budget expires before a fresh snapshot can
be obtained, the call returns `Unavailable`; it does not fabricate an empty page.
Use an immediate read (`wait_ms=0`) when no server wait is desired. Positive waits
have these fixed limits per application-service instance:

| Resource | Limit |
| --- | --- |
| Distinct scope/execution/cursor/limit combinations | 256 |
| Observers across all combinations | 4,096 |
| Observers sharing one combination | 256 |
| Simultaneous authoritative probes | 4 |

Capacity exhaustion returns `Unavailable`. These limits govern shared waits;
they do not limit execution concurrency or replace the HTTP server's own limits.
Each active combination retains at most one bounded page or error. The final
observer releases the key; the service does not cache its response for later requests.
There is no background task per idle observer or database connection checked out
between probes. HTTP disconnect detection depends on the transport; the request
wait/deadline bounds any remaining registration. Shutdown closes admission and
wakes existing observers.

Storage reads remain immediate, and observation adds no execution-path writes.
Separate service instances each perform their own bounded probes; no shared
cache or notification broker is required. This reduces duplicate idle reads for
observers at the same position, not arbitrary distinct-target workload. It is not
a claim of unlimited subscriber scale. Arbitrary application milestones and
provisional token streams are outside this lifecycle feed. Terminal webhooks
remain the alternative when only completion notification is needed.
