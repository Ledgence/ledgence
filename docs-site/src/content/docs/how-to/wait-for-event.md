---
title: Wait for an external event
description: Checkpoint an approval wait, send a directly addressed CloudEvent, and handle timeout or uncertain acceptance.
---

Use an external event wait when another application needs to resume a specific workflow—for example, after an approval. The workflow checkpoints and releases its invocation while Ledgence stores the wait.

## Prerequisites

Use the matching orchestrator, worker, and database migrations from the `v0.2.0` source tag, as in [Run Ledgence locally](/tutorials/run-locally). Package your controller with runtime protocol **3**, publish it, and connect a worker to its queue. To send events, use the [Python client](/reference/python-client) with the same server, tenant, and namespace as the workflow.

The controller below is application code to package and publish, with `program:handle` as its manifest handler. It is not a preinstalled example.

## Return an event-wait decision

```python
from enum import StrEnum
from ledgence.worker.workflow import Workflow

class Entry(StrEnum):
    START = "start"
    AFTER_APPROVAL = "after_approval"

workflow = Workflow(Entry)

@workflow.entrypoint(Entry.START, default=True)
def start(event, ctx):
    return ctx.wait_event(
        "approval:1",
        continuation=Entry.AFTER_APPROVAL,
        state={"document_id": event["data"]["document_id"]},
        timeout_ms=24 * 60 * 60 * 1000,
    )

@workflow.entrypoint(Entry.AFTER_APPROVAL)
def after_approval(event, ctx):
    wake = ctx.wake
    if wake["kind"] == "timeout":
        return ctx.fail("approval_expired", "Approval did not arrive in time")
    approval = wake["event"]["data"]
    if not isinstance(approval, dict) or approval.get("approved") is not True:
        return ctx.fail("approval_declined", "The document was not approved")
    return ctx.complete({
        "document_id": ctx.state["document_id"],
        "approved": True,
    })

handle = workflow.build()
```

The example expects submission data such as `{"document_id": "DOC-1042"}` and event data containing an `approved` boolean. Validate additional application fields according to your own contract.

`timeout_ms` is an integer duration. It starts when the orchestrator accepts the checkpoint, and the persisted deadline survives retries. Use `None` to wait without a deadline.

## Send an approval

Save the workflow ID from submission. Inside an open `AsyncClient`, prepare and send this command:

```python
workflow = client.workflows.handle(workflow_id)
command = workflow.prepare_event(
    "approval:1",
    event={
        "specversion": "1.0",
        "id": "evt_approval_DOC-1042_1",
        "source": "urn:documents:approvals",
        "type": "com.example.document.approved.v1",
        "datacontenttype": "application/json",
        "data": {"approved": True},
    },
)
receipt = await workflow.send_event(command)
```

The workflow ID, the instance's fixed compatibility binding, and the wait key route the event. The CloudEvent's `data` remains your application's payload. The receipt confirms **durable acceptance**, not that the resumed controller has already processed it.

An event can arrive before the controller registers its wait, provided the workflow already exists and can accept events. There is no need to poll until the controller reaches `wait_event`.

## Reconcile an uncertain response

Keep the same command if a network failure leaves acceptance uncertain:

```python
from ledgence.client import WorkflowEventUncertain

try:
    receipt = await workflow.send_event(command)
except WorkflowEventUncertain as uncertain:
    receipt = await workflow.send_event(uncertain.command)
```

This shows one explicit reconciliation attempt. Choose a bounded retry/backoff policy for your application if the service remains unavailable. Persist the prepared command before awaiting when it must survive caller cancellation or a process restart.

Exact repeats return the original receipt. Do not generate a fresh event ID or alter the payload to retry the same event.

## Handle deadlines and repeated approval rounds

Wait keys are one-shot for the whole workflow. Use `approval:2` for a second approval round. Reusing a closed key does not create another wait.

An event must be accepted strictly before an installed deadline to win. At or after the deadline, timeout wins; the sender's timestamp does not decide this race. New events for a closed wait or terminal workflow are rejected, while an already accepted receipt can still be reconciled.

Complete encoded events are limited to 64 KiB. Wait keys and event IDs are limited to 128 UTF-8 bytes. See the [workflow context reference](/reference/workflow-context) for duration and activation limits.

**Source:** [External event contract](https://github.com/Ledgence/ledgence/blob/v0.2.0/docs/workflow-events.md) · [Client event API](https://github.com/Ledgence/ledgence/blob/v0.2.0/sdk/python-client/README.md#external-workflow-events)
