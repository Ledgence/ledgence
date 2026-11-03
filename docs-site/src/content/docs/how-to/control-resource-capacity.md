---
title: Share capacity across model and tool calls
description: Park workflows while a resource is full, then run a registered operation and retain its result.
---

**In development.** Use matching components built from this checkout, including
migrations and Console C8. Published 0.8.0 packages do not contain this capability.

Suppose many workflows call the same model endpoint, but you want at most four
calls authorized at once. A resource policy coordinates those workflows across
workers. When capacity is full, Ledgence saves the wait and releases the worker
process to execute other work.

## Try a real local example

The [resource-admission example](https://github.com/Ledgence/ledgence/tree/develop/examples/resource-admission)
contains two entrypoints and a registered classifier. Its local HTTP provider
records calls independently. The complete check starts real processes and tests
restart, lost responses, expired offers and result reuse without a model account.
Open its `program.py` in your editor to follow the flow.

## Enable the server capability

First [configure instance access](/how-to/configure-instance-access). Create
`admission.json` next to your installation configuration:

```json
{
  "v": 1,
  "audience": {
    "authority_id": "YOUR_ACCESS_AUTHORITY_ID",
    "instance_id": "YOUR_INSTANCE_ID",
    "scope": {"tenant_id": "acme", "namespace": "demo"}
  },
  "dispatch": "enabled"
}
```

Copy these identifiers from your access configuration. Add
`--admission-config admission.json` to each orchestrator's existing start command.
All replicas must use the same policy. The file is loaded at startup. Workers
negotiate the capability automatically; older workers cannot execute its callbacks.

## Create a named resource

Create `configure_capacity.py` in your editor. Use an operator credential with
`resource_control`; a worker credential cannot administer resources.

```python
import asyncio
from pathlib import Path
from ledgence.client import AsyncClient, ResourcePolicy, ResourceRate

async def main():
    async with AsyncClient(
        "http://127.0.0.1:8080", tenant="acme", namespace="demo",
        credentials_file=Path("operator.json"),
    ) as client:
        receipt = await client.resources.put(
            "support-model",
            policy=ResourcePolicy(
                max_occupancy=4,
                rate=ResourceRate(numerator=10, period_ms=1_000, burst=10),
            ),
            request_id="create-support-model-v1",
            reason="Share classifier capacity across workflows",
        )
        print(receipt.resource.revision)

asyncio.run(main())
```

Run `python configure_capacity.py` in an environment containing this checkout's
Python client. The optional rate policy allows ten authorizations per second,
with up to ten tokens stored for a burst. It is not a rolling-window limit.
For an update, read the resource first and pass its `revision` as
`expected_revision`. After a lost response, resend identical arguments and the
same request ID; generating a new ID would be a new decision.

## Register and bind an operation

A registered function belongs to the workflow's immutable program package:

```python
@workflow.operation(kind=OperationKind.MODEL, version="1")
async def classify(*, text):
    return await model_client.classify(text)  # Application-owned integration.
```

Here `workflow` is your `Workflow(Entry)` registry, and `model_client` is your
application's runtime client. Keep clients and credentials out of persisted
arguments. Bind the effective arguments before returning a checkpoint:

```python
return ctx.request_admission(
    "classify:0", resource="support-model",
    operation=classify.bind(text=event["data"]["text"]),
    resume=Entry.CALL, state={}, timeout_ms=60_000,
)
```

In `Entry.CALL`, use the registered function without replacing those arguments:

```python
outcome = await ctx.admitted_operation(classify)
if outcome.kind == AdmissionOutcomeKind.SUCCEEDED:
    return ctx.complete(outcome.result)
if outcome.kind in (AdmissionOutcomeKind.WAIT_REQUIRED, AdmissionOutcomeKind.NEEDS_REVIEW):
    return ctx.wait_admission(resume=Entry.CALL, state={}, timeout_ms=60_000)
return ctx.fail("admission_closed", "No completed result was accepted")
```

Import `Workflow` and `OperationKind` from `ledgence.worker.workflow`, and
`AdmissionOutcomeKind` from `ledgence.worker.admission`. Call `workflow.build()`
after registering the entrypoints and operations. The runnable example supplies
the complete imports, registry, provider transport and tests.

## Understand uncertain outcomes

An offer reserves capacity; only an accepted Start authorizes execution. Losing
that reply can leave occupied capacity even if no call happened. Finish saves
the result and releases capacity together. A retried workflow can reuse that
result without calling the model again.

A timeout, exception or worker loss does not prove remote completion. Unknown
usage stays occupied until completion evidence or an explicit operator release.
Before Start, rewaiting cannot extend the original admission deadline. After
Start, a separate bounded wait may await evidence beyond that deadline; it cannot
grant another call or release capacity merely by timing out.

This limits registered callback authorizations. If your SDK automatically retries,
follows redirects or starts background requests, those physical requests need
separate accounting. Disable hidden retries or use an adapter that counts each
attempt. This first implementation does not compose admission with protected
provider operations or enforce inherited monetary/token budgets.

## Inspect and recover

Use `ledgence resource inspect` for occupancy counters and
`ledgence admission inspect` for an original occurrence. The full arguments and
result require audited sensitive access. Console shows each admission wait and
an accepted-operation inspector in Graph and Trace. An offer alone is never
shown as an executed operation.

`ledgence admission release` is an audited operator decision with a saved source,
revision and request ID. It does not stop remote work or mark the workflow
successful. MCP exposes read tools by default and resource mutations only with
`--allow-resource-control`, subject to the API's permissions.

Before starting a restored database, set `dispatch` to `quarantined` on every
replica. Reconcile external usage before enabling dispatch. A backup cannot know
about grants accepted after it was taken.

## Plan for retained records

This initial capability has no automatic expiry for admission occurrences, frozen
operation bindings and arguments, accepted results, resource records or operator
policy/release command receipts. They remain after completion and execution
retirement. The retention interval for executions does not erase them, and
releasing capacity does not delete the evidence. Plan storage for their continued
growth.

[Full contract, routes and failure semantics](https://github.com/Ledgence/ledgence/blob/develop/docs/resource-admission.md).
