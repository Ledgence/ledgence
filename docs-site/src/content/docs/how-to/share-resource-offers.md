---
title: Share resource offers between queue groups
description: Give support traffic a weighted turn when batch work is already using the same service.
---

**Development feature.** Build the client, orchestrators, workers and Console from the same development checkout. The published 0.8.0 packages do not support weighted resource offers. Apply the migrations and upgrade every orchestrator before enabling the policy.

Suppose a model endpoint allows five simultaneous calls. Batch can use all five when support is quiet. When both queues have work waiting, you want roughly three of each four new offers to go to support. Configure the shared resource once. Existing programs only need to request that resource.

## Configure the resource

First [enable resource admission](/how-to/control-resource-capacity) and obtain an operator credential with `resource_control` to write the policy and `metadata_read` to inspect its revision and offer history. Save this as `share_capacity.py`. The example creates a new resource named `model.requests` in the operator's scope.

```python
import asyncio
from pathlib import Path

from ledgence.client import AsyncClient, ResourceFairGroup, ResourceFairSharing, ResourcePolicy


async def main():
    async with AsyncClient(
        "http://127.0.0.1:8080", tenant="acme", namespace="demo",
        credentials_file=Path("operator.json"),
    ) as client:
        receipt = await client.resources.put(
            "model.requests",
            policy=ResourcePolicy(
                max_occupancy=5,
                fair_sharing=ResourceFairSharing(
                    groups=(
                        ResourceFairGroup("support", weight=3, queues=("support",)),
                        ResourceFairGroup("batch", weight=1),
                    ),
                    default_group="batch",
                ),
            ),
            request_id="model-requests-create-v1",
            reason="Give support traffic three turns per batch turn during contention",
        )
        print("Policy revision:", receipt.resource.revision)


asyncio.run(main())
```

Run `python share_capacity.py`. To change an existing resource, read it with `client.resources.get`, pass its current revision as `expected_revision`, and use a new request ID for the change. Retry an uncertain write using the identical request.

## What to expect

- Ledgence classifies standalone tasks using their submission queue. Registered workflow operations and approved provider calls use the owning workflow's queue. Unmapped queues all share the default group.
- If five batch calls are running, support waits for a slot to open. Existing work runs to closure. While both groups stay eligible, every complete four-offer cycle has three support offers and one batch offer. Within a group, the oldest eligible request is served first. Weights are relative: 300 and 100 produce the same turn order as 3 and 1. Each round can contain several groups, so spacing within a cycle depends on the configured weights and group order.
- If one group has no eligible work, the other may use every offer. Idle groups do not accumulate credit. The weights govern reservations. Time spent running, worker polls and the duration of external calls can differ between groups.
- The offer reserves a slot. A worker still needs a successful Start before calling the provider. Rate accounting, unknown occupied slots and explicit recovery continue to apply.

## See who received a slot

Open **Resources** in Console and select **model.requests**. **Offer history** shows the revision, source queue, execution and reason for each weighted decision. Open an execution to investigate a specific call while its details are retained. The offer record remains available after those details expire. A decision remains in history if the offer later expires. FIFO policies do not create these entries.

For an automated reader with `metadata_read`, use `await client.resources.decisions("model.requests", limit=20)`. Follow `page.next` using `before` to read older offers. The CLI exposes the same records through `ledgence resource decisions`, and MCP through `ledgence_resource_decisions`.
