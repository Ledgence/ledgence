# External workflow events and durable timers

Workflows can checkpoint and wait for one directly addressed external event, or
for a durable timer. The orchestration store owns the wait. No Python invocation,
consumer reservation, database connection, or broker visibility lease remains
allocated while it is waiting. An ordinary warm subprocess can remain available
in the bounded pool. Active local I/O still occupies its invocation as described
in [checkpoint workflows](workflows.md).

## Waiting in a Python workflow

These are explicit continuation decisions, not Python coroutine suspension:

```python
from ledgence.worker.workflow import workflow_context

async def handle(event):
    ctx = workflow_context()
    if ctx.continuation == "start":
        return ctx.wait_event(
            "approval:1",
            continuation="after_approval",
            state={"invoice_id": event["data"]["invoice_id"]},
            timeout_ms=24 * 60 * 60 * 1000,
        )
    if ctx.continuation == "after_approval":
        wake = ctx.wake
        if wake["kind"] == "timeout":
            return ctx.fail("approval_expired", "Approval did not arrive in time")
        return ctx.complete({
            "invoice_id": ctx.state["invoice_id"],
            "approval": wake["event"]["data"],
        })
    return ctx.fail("unknown_continuation", ctx.continuation)
```

`return ctx.sleep("backoff:1", 5000, continuation="after_delay", state={})`
persists a standalone timer. Its next `ctx.wake` is
`{"kind":"timer","key":"backoff:1","deadline":...}`. An event timeout uses
`kind: "timeout"`. A delivered event uses `kind: "event"`, the original full
`event` envelope, and its authoritative `accepted_at` timestamp. Timestamps are
Unix milliseconds. The user-owned invocation `event["data"]` is unchanged.

`timeout_ms=None` waits without a deadline. Durations are integer milliseconds,
from zero through 365 days. Relative time starts when the checkpoint decision is
accepted under workflow authority, not when the Python handler starts. The
anchor is database time sampled under that lock, not a database commit timestamp. The deadline is stored
once and never restarted by coordinator or activation retries. Timer scheduling
can be delayed by downtime, backlog, or capacity; it never intentionally fires
before its persisted deadline. Absolute business-deadline APIs are not included.

Existing staged `ctx.task` commands can accompany an external wait. Their registrations
and dispatch obligations commit atomically with the checkpoint; execution starts
asynchronously after dispatch. External waits resume with their selected wake
only; unrelated child outcomes remain available for an explicit child wait or
continuation. Existing `ctx.suspend(until=[...])` still means all listed children
are terminal; it does not become an event/timer race.

## Sending an event

Use the workflow ID returned by submission, the same tenant and namespace, and
the exact wait key. The event is a JSON CloudEvent with a sender-stable ID:

```python
workflow = client.workflows.handle(workflow_id)
command = workflow.prepare_event(
    "approval:1",
    event={
        "specversion": "1.0",
        "id": "evt_approval_INV-1042_1",
        "source": "urn:billing:approvals",
        "type": "com.example.invoice.approved.v1",
        "datacontenttype": "application/json",
        "data": {"approved": True},
    },
)
receipt = await workflow.send_event(command)
```

`POST /v1/workflows/events` accepts `{scope, workflow_id, key, event}`. A receipt
contains that scope, workflow ID and key, `event_id`, `event_source`, immutable
`accepted_at`, and `already_accepted`. It confirms durable acceptance, not that a
workflow has processed the event. An unknown workflow returns `not_found`.

The event's original optional `time`, `subject`, `dataschema`, `traceparent`,
`tracestate`, and valid context extensions are preserved. Its `data` belongs
entirely to the sender. Invocation identifiers are not required on external
events; any forwarded execution extensions do not select the receiving workflow
or override its execution identities. Routing comes from the outer command.
When tracing is enabled, acceptance and activation spans link the original
event trace without replacing the invocation or attempt processing trace.

`WorkflowEventUncertain` means the request may have committed. It retains the
frozen command for explicit resending. The client does not generate another
event ID or automatically retry the mutation. Preserve the same source, ID, key,
and entire event envelope when reconciling:

```python
from ledgence.client import WorkflowEventUncertain

try:
    receipt = await workflow.send_event(command)
except WorkflowEventUncertain as uncertain:
    receipt = await workflow.send_event(uncertain.command)
```

This illustrates one reconciliation attempt; an application chooses its own
bounded retry/backoff policy if the endpoint remains unavailable.

## Identity, races, and recovery

- Wait keys are one-shot within a workflow. Use a new key for each loop iteration
  or approval round. Installing a second logical wait with the same key conflicts.
- Each event key accepts one immutable event. The same `(source, id)` is also
  unique within that workflow. Exact repeats return the original receipt;
  changed key, payload, or envelope conflicts. JSON object ordering is ignored,
  but numeric representations such as `1` and `1.0` remain distinct.
- Events can arrive before their wait is registered, including before the first
  activation executes. They are buffered durably for an existing running or
  waiting workflow. No global broadcast, predicate matching, or stream-style
  subscription is implied.
- An event accepted under workflow authority strictly before an installed
  deadline is eligible. At the deadline or later, timeout wins. Sender `time`
  and HTTP request start time do not decide eligibility. A delayed coordinator
  still considers an earlier accepted event before timing out.
- After checking existing receipt bindings, new events for expired/closed
  waits, timers, or failing/cancelling/terminal workflows return
  `obsolete_operation`. Exact accepted receipts remain
  reconcilable after consumption, cancellation, or workflow completion.
- A timer key cannot replace an early buffered event with the same key; that
  decision conflicts. New events cannot target an installed timer.
- Selecting the wake, closing the wait, persisting the frozen activation context,
  and creating its task/dispatch obligation are one transaction. Retrying the
  resumed activation sees the same wake; duplicate arrivals do not schedule
  another continuation.
- Cancellation closes outstanding waits and follows the existing owned-task
  drain semantics. An event or timer cannot revive a cancelled workflow.

## Bounds and deployment

The original event is limited to 64 KiB, and the HTTP event command to 70 KiB.
Event IDs and wait keys use at most 128 UTF-8 bytes; sources use at most 2048.
The pending inbox permits at most 128 events and 256 KiB of encoded events per
workflow. Capacity pressure rejects new acceptance; existing receipts still
reconcile. Consumed receipts are retained for deduplication. This feature does
not add retention deletion; operational retention remains separate work.

Frozen child inputs and the optional wake share a 256 KiB budget. The whole
activation context remains bounded at 640 KiB, including its existing checkpoint
and local-result journal limits. Event payloads do not create an additional
unbounded per-activation allowance.

Server byte limits use its compact JSON encoding. Python may spell the same
finite float with additional characters. Reading accepted data permits a bounded
per-float encoding allowance inside the helper; this does not increase server
admission or storage limits. New Python submissions, decisions and local-result
writes retain their existing encoding limits and can conservatively reject a
boundary-sized value that a Rust caller could submit. Reading an accepted wake,
checkpoint or child result, and replaying a committed local result, do not depend
on re-encoding it within the sender's original byte count.

Apply migration `20260916000000_workflow_events_timers.sql` before serving the
updated orchestrator. Upgrade both orchestrator and worker before using the new
wait decisions; older workers do not understand the new wake context. The PostgreSQL adapter uses workflow-level transaction
ordering and indexed, bounded work batches. A sleeping timer's dormant lifetime
does not count against the existing failed completion-application retry cutoff;
wait processing retries remain durable. Workflows may use integrated delivery or
the SQS-compatible adapter; the broker carries execution references, not timer
state. Local SQS-compatible validation uses ElasticMQ.

Inbound events resume workflows. [Owned subworkflows](subworkflows.md) can use
these waits independently and report their terminal outcome to their parent.
Outbound completion notifications and general event streams remain separate
capabilities.
