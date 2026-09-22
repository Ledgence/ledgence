---
title: Python client
description: Core submission, observation, task discovery, and external-event APIs in ledgence.client.
---

The `ledgence-client` package exposes `ledgence.client` for asynchronous submission and observation. It is separate from the worker-supplied `ledgence.worker` helper.

This reference covers the core client operations. The current client is an early development API with no stable-release compatibility promise. Python 3.11–3.14 is the configured test matrix.

## Installation from source

From the Ledgence repository root, in a virtual environment:

```sh
python3 -m pip install ./sdk/python-client
```

You can also install a locally built candidate wheel. These instructions do not assume a published package-registry release.

## `AsyncClient`

```python
from ledgence.client import AsyncClient

async with AsyncClient(
    "http://127.0.0.1:8080",
    tenant="acme",
    namespace="demo",
    request_timeout=30.0,
) as client:
    task = client.tasks.handle(saved_task_id)
    status = await task.status()
```

| Argument | Meaning |
| --- | --- |
| `base_url` | HTTP API endpoint. |
| `tenant` | Required tenant scope. |
| `namespace` | Required namespace scope. |
| `request_timeout` | Per-request deadline in seconds; defaults to `30.0`. |

Open the client with `async with`, reuse it across calls, and keep it on its owning event loop. It exposes `tasks`, `workflows`, and `completions`. Handles retain their scoped client; reconnect through a new client after the original closes.

## Submit a task or root workflow

`client.tasks.prepare(...)` returns a frozen `Submission`. `client.workflows.prepare(...)` returns a distinct `WorkflowSubmission`. Neither sends a request.

Both accept:

| Argument | Contract |
| --- | --- |
| `program`, `version` | Required published program ID and version. |
| `queue` | Required execution queue. |
| `data` | Required JSON application input, at most 1 MiB. |
| `idempotency_key` | Required submission identity, at most 255 UTF-8 bytes. |
| `correlation_key` | Optional application lookup key, at most 512 UTF-8 bytes. |
| `retry_policy` | Optional `RetryPolicy(max_attempts=..., retry_delay_ms=...)`. |
| `attempt_timeout_ms` | Optional integer from `60000` through `86400000`. |
| `origin_trace` | Optional `TraceContext`; explicit `None` freezes absence. |

`RetryPolicy.max_attempts` ranges from 1 through 1000 and `retry_delay_ms` from 0 through 86400000. Defaults omitted by the client are supplied by the service.

Send a prepared command with `await client.tasks.submit(submission)` or `await client.workflows.submit(submission)`. The convenience form accepts the same keyword arguments directly:

```python
workflow = await client.workflows.submit(
    program="workflow-example",
    version="1.0.0",
    queue="demo",
    data={"urls": ["http://receiver:8091/page.txt"] * 4, "queue": "demo"},
    idempotency_key="docs:workflow:1",
)
```

The returned handle has an `id`. Construct a handle for an existing execution with `client.tasks.handle(task_id)` or `client.workflows.handle(workflow_id)`; construction makes no network request.

The client does not publish packages. Public workflow submission creates a root workflow; controllers create owned children with [the workflow context](/reference/workflow-context).

## Observe execution

| Method | Task handle | Workflow handle |
| --- | --- | --- |
| `await status()` | Compact `TaskStatus`. | Compact `WorkflowStatus`. |
| `await outcome()` | `TaskResult`; `.outcome` is `None` while pending. | `WorkflowResult`; `.outcome` is `None` while pending. |
| `await wait(timeout=60.0)` | Terminal result, including failure or cancellation as a value. | Terminal result, including failure or cancellation as a value. |
| `await result(timeout=60.0)` | Successful JSON output, or `TaskFailed` / `TaskCancelled`. | Successful JSON output, or `WorkflowFailed` / `WorkflowCancelled`. |
| `await cancel()` | Acknowledged `TaskState`; may still be `active`. | Acknowledged `WorkflowStatus`; may still be `cancelling`. |

`wait` and `result` use observation deadlines in **seconds**. A timeout neither cancels execution nor submits it again. `WorkflowWaitTimeout` also subclasses `WaitTimeout`; it retains the workflow and last observation. Observe the saved handle again to continue waiting.

A successful JSON `null` becomes Python `None`; distinguish it from an absent `.outcome` on a pending result. Failure exceptions retain their full result as `.result`.

Task success does not prove exactly-once effects or physical process cleanup. Inspect `result.outcome.quiescence` when cleanup evidence matters. Workflow `failing` and `cancelling` are nonterminal states while owned work drains.

## Find tasks

```python
page = await client.tasks.list(
    state="failed",
    queue="demo",
    correlation_key="docs:workflow",
    limit=50,
)
for item in page.items:
    print(item.task_id, item.state)
```

Filters apply inside the client's tenant and namespace. Optional fields are `state`, `queue`, `correlation_key`, `submitted_from`, `submitted_until`, and `cursor`.

`limit` defaults to 50 and ranges from 1 through 100. Submission times are Unix milliseconds: `submitted_from` is inclusive and `submitted_until` exclusive. If both are supplied, the lower bound must be smaller.

Results are ordered by `(submitted_at, task_id)` descending. Use the opaque `page.next_cursor` with the same filters for the next page; `None` ends the traversal. Pages are current observations, not a frozen snapshot. This API lists tasks, not workflow roots.

## Send an external workflow event

`workflow.prepare_event(key, *, event)` freezes the event command without sending it. `await workflow.send_event(command)` sends it. The convenience form is `await workflow.send_event(key="approval:1", event=event)`.

The CloudEvent requires `specversion="1.0"`, nonempty `id`, `source`, and `type`, `datacontenttype="application/json"`, and `data` (which may be null).

The returned `WorkflowEventReceipt` contains scope, workflow ID, key, event identity, `accepted_at`, and `already_accepted`. It confirms storage rather than processing. See [Wait for an external event](/how-to/wait-for-event) for the complete pattern.

## Reconcile uncertain mutations

The SDK does not automatically retry submission, cancellation, event, or completion-subscription mutations. A transport failure after dispatch can leave acceptance uncertain.

For submission, `SubmissionUncertain.submission` preserves the frozen task or workflow command. Resend it explicitly through the corresponding collection with the same endpoint and scope. For external events, `WorkflowEventUncertain.command` preserves the command to resend.

Prepare and persist commands before awaiting if they must survive caller cancellation. `asyncio.CancelledError` remains cancellation of the caller; it does not prove the server rejected the mutation.

## Completion notifications

Both task and workflow handles expose `prepare_subscribe(destination=..., idempotency_key=...)` and `await subscribe(...)`. The destination is an operator-configured alias. Registration is separate from submission; its guarantee starts after acceptance.

Save the returned subscription ID and reconnect with `client.completions.handle(subscription_id)`. See [the complete subscription API](https://github.com/Ledgence/ledgence/blob/develop/sdk/python-client/README.md#durable-completion-subscriptions) for delivery status, explicit redelivery, and uncertainty handling.

**Source:** [Client contract](https://github.com/Ledgence/ledgence/blob/develop/sdk/python-client/README.md) · [Typed implementation](https://github.com/Ledgence/ledgence/tree/develop/sdk/python-client/src/ledgence/client)
