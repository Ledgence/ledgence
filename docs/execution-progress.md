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
  "limit": 100
}
```

Use `kind: "workflow"` for a workflow ID. `limit` defaults to 100 and accepts
1–100. The body is bounded to 8 KiB. The response is bounded to 256 KiB:

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
and polls at the supplied interval only when caught up. The interval must be at
least 0.25 seconds; the default is one second. Pull iteration provides backpressure:
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
`--poll-interval-ms` defaults to 1000 and has a minimum of 250.
`--timeout-ms` bounds observation. `--keep-following` continues beyond a drained
terminal snapshot. Interrupting or failing to write the output stops the observer
without cancelling the execution. A page has a five-second stdout delivery
budget; the next read starts only after the complete page is written.
`--timeout-ms` bounds observation, not total process shutdown or blocked stderr
diagnostics. Supply `--credentials-file` where required.

MCP exposes `ledgence_execution_progress` as a bounded read-only tool. It accepts
an execution kind/ID, cursor and limit within the MCP server's configured scope.
It never holds a tool request open for the execution's entire lifetime.

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

This increment uses immediate page reads and client-side polling. An idle
subscriber incurs one bounded request per interval; no database connection stays
checked out for the observer between reads. Idle pooled connections may remain open. The existing worker concurrency remains unchanged. This is
not a claim of unlimited subscriber scale. Server-coalesced long polling, live
Console accumulation, arbitrary application milestones and provisional token
streams are separate follow-up work. Terminal webhooks remain the alternative
when only completion notification is needed.
