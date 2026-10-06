---
title: Receive results without keeping a client connected
description: Choose bounded waiting, reconnect by execution ID, or durable completion webhooks for long-running tasks and workflows.
---

Ledgence 0.4.0 supports both bounded client waiting and durable completion
subscriptions. Use waiting for an interactive caller; use a subscription when
the application should receive a notification after disconnecting.

## Observe an existing execution

Install the [Python client](/reference/python-client) and use the tenant and
namespace configured for your instance. The following function accepts an
existing task ID; it never submits another task:

```python
from ledgence.client import AsyncClient, WaitTimeout

async def observe(task_id: str):
    async with AsyncClient(
        "http://127.0.0.1:8080", tenant="acme", namespace="demo",
    ) as client:
        task = client.tasks.handle(task_id)
        try:
            print(await task.result(timeout=60))
        except WaitTimeout:
            print("No terminal result observed within the budget; task ID:", task.id)
```

`result(timeout=60)` polls for at most 60 seconds and returns successful JSON
output. A wait timeout stops local observation: it does not cancel, retry or
resubmit execution. Recreate the handle in a later client session with the saved
ID. Successful JSON `null` is a valid result, so do not use a return value's
truthiness to infer execution status.

| Method | Behavior |
| --- | --- |
| `await task.status()` | Read compact current scheduling state. |
| `await task.outcome()` | Read immediately; `.outcome is None` means the task is still pending. |
| `await task.wait(timeout=60)` | Observe a terminal result, including failures/cancellation as values. |
| `await task.result(timeout=60)` | Return successful output, or raise `TaskFailed` / `TaskCancelled` for terminal failure/cancellation. |
| `await task.cancel()` | Explicitly request cancellation. Closing a client does not do this. |

Workflow handles have the same observation methods. Use
`client.workflows.handle(workflow_id)` for the complete workflow's output; the
internal controller task's output is a workflow decision. Workflow failures,
cancellation and wait expiry have corresponding workflow exception types.
Timeouts are positive finite seconds; they are observation budgets, separate
from execution retry and attempt timeout policies.

## Configure a callback destination

The operator runs an HTTP POST receiver and supplies an immutable destination
alias to the orchestrator. For example, save this as `completion.json`:

```json
{
  "destinations": [{
    "scope": {"tenant_id": "acme", "namespace": "demo"},
    "destination": "application-results",
    "url": "http://127.0.0.1:9000/completions"
  }]
}
```

Add `--completion-config /absolute/path/to/completion.json` to your existing
`ledgence orchestrator serve` command. Retain its database, program store and
instance configuration. The URL must be reachable from the orchestrator; inside
a container, loopback refers to that container. Ledgence does not start the
receiver for you. The [source Compose example](/tutorials/run-locally) includes a
local receiver; the base [image distribution](/how-to/run-local-distribution)
does not enable one by default.

The subscription selects the alias, not an arbitrary URL. Keep at least one
orchestrator configured for that destination to dispatch its durable backlog.

## Register, then disconnect

With an open `AsyncClient` and a task handle:

```python
subscription = await task.subscribe(
    destination="application-results",
    idempotency_key="application-result-v1",
)
print(subscription.id)  # Persist for later status inspection.
```

A workflow supports `await workflow.subscribe(...)` as well. Once the server
accepts the subscription, the client can disconnect. There is no listener
coroutine or open subscription socket to keep alive. Subscription also works
after completion while the execution is retained.

Submission and subscription are separate durable commands. Persist their
identities and reconcile uncertain replies using the original prepared commands
and idempotency keys. `task.prepare_subscribe(...)` produces a command that can
be passed to `task.subscribe(command)`. A lost reply raises
`CompletionSubscriptionUncertain`; it does not prove registration failed.

## Accept and inspect delivery

The receiver gets a CloudEvent referring to the terminal task or workflow,
including a relative `ldgresultref`. Resolve that result reference against your
configured Ledgence API, or inspect through the corresponding SDK handle. The
notification does not contain the full application output.
Durably store the event before returning `2xx`, deduplicate by `(source, id)`,
then retrieve the result when needed. There is no cross-execution delivery
ordering guarantee.

```python
subscription = client.completions.handle(saved_subscription_id)
delivery = await subscription.status()
print(delivery.state, delivery.generation)
```

Failed or uncertain deliveries retry with backoff, with eight leased attempts
per generation. Exhaustion affects notification delivery, not execution. After
repairing the receiver, use `prepare_retry(expected_generation=...)` and
`retry(command)` to rearm an exhausted subscription within the supported
generation limit. Redelivery preserves the completion event's identity.

For waits **inside a workflow**, use durable joins, timers or
[external-event waits](/how-to/wait-for-event), which release the worker slot.
An application's HTTP callback subscription is a separate integration.

**Source:** [Completion contract](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/completion-notifications.md) · [Result contract](https://github.com/Ledgence/ledgence/blob/ae6734a2dfa58d931c3e3fcfa0e791382bfe15bf/docs/task-results.md)
