# Limit a queue across workers

**Development feature.** Use matching development builds of the orchestrator, workers and Console (contract C12), run the database migrations and configure instance credentials. The released 0.8.0 stack does not expose queue policies.

Suppose the payment gateway can serve five requests concurrently. Put gateway tasks on the `payments` queue and set its limit to five. Three workers with four consumers each can then have at most five outstanding attempts on that queue between them. A queue such as `email` keeps its own policy. Workers can continue to poll it while `payments` is full.

## Set the limit in Console

1. Open **Queues**, enter `payments` and choose **Create policy**. Opening the page requires `metadata_read`. Saving a change also requires `resource_control`. Existing 0.8.0 operator credentials keep their previous permissions. When upgrading, use `ledgence access issue` with the current access configuration to issue a credential with the needed capabilities, then start the orchestrator with the updated access configuration.
2. Select **Limit concurrent attempts**, enter **5** and give a reason such as “Payment gateway accepts five simultaneous requests.” Save the policy. The page shows the accepted revision and current occupancy.
3. To drain work, edit the maximum or choose **Pause new attempts**. Reducing five to two allows the existing five to finish and admits the next attempt once occupancy falls below two. Zero pauses new attempts. Unlimited removes the ceiling. After a queue has been enrolled, Ledgence continues tracking its holds when unlimited.

Policies are scoped to the instance tenant, namespace and queue name. The limit covers standalone tasks, workflow activations, and recovery tasks assigned on that queue through integrated HTTP or configured external SQS delivery. Each outstanding **attempt** costs one slot. A program making three parallel calls inside one attempt still costs one. Route all work using the downstream system through the protected queue, or use a shared resource policy when several queues or callbacks share the same budget.

## Understand a held slot

PostgreSQL records the reservation in the transaction that creates the attempt. Polling an empty queue or retrying the same claim after a lost response consumes no additional slot. Existing workers and new replicas see the same limit. During first enrollment, Ledgence adopts retained attempts that can still be running. Before turning on a limit for a live system, reconcile any older external activity whose execution history has already been pruned. Upgrade the orchestrators and workers before enabling the first policy. An older orchestrator cannot assign new work on an enrolled queue. An older worker may need operator reconciliation if it loses settlement after execution. For retained attempts adopted at enrollment, the hold timeout is measured from enrollment.

**Occupied** includes reserved, running and uncertain attempts. **Uncertain** means the attempt may have started and its physical closure is unknown. Losing a lease, finishing a business request or losing a worker connection alone does not establish that external work has stopped. Use **Inspect holds** with `sensitive_read` to see the affected task, attempt and worker session. Exact inspection is audited.

By default a possibly running attempt keeps its slot until its original worker confirms cleanup. An operator with `sensitive_read` and `resource_control` can inspect an uncertain hold, verify the downstream state, select **Review release**, enter the reason and choose **Release this slot**. Manual release makes capacity available. Keep the original task outcome and the recorded release cause when investigating it later.

## Configure a timeout

Leave timeout disabled for the default hold-until-confirmed behavior. To set one, select **Request a stop after a timeout**. For new assignments the timeout starts when the slot is reserved and includes program preparation. For attempts already active at first enrollment it starts at enrollment. An edit leaves existing holds on their original settings. The worker uses its conservative execution deadline and will stop a managed handler if it is running. The current worker subtracts a five-second safety margin when computing its local execution deadline. Allow time for this margin and startup. A timeout of five seconds or less can prevent dispatch.

* **Wait for confirmed closure.** On expiry the orchestrator records the stop and fences the attempt. If the worker is unreachable, capacity stays occupied until its cleanup is confirmed or an operator reconciles the hold.
* **Release after a grace period.** The grace starts when PostgreSQL records the stop request. If cleanup is still unknown at the end of the grace, Ledgence can return the slot with release cause `timeout`. A replacement task may then overlap an external operation that survived the original worker. Late confirmation preserves the original release cause.

Queue holds and revisions survive an orchestrator restart in PostgreSQL. A database outage prevents new assignments. Outstanding holds remain recorded, and timeout recovery resumes when PostgreSQL is available. Each configured queue serializes its grant decisions against its own policy row. First enrollment briefly fences new assignments and lifecycle writes for that queue while scanning retained attempts. The duration depends on its history. Unconfigured queues check for policy on assignment and do not create hold records. Released hold records persist for inspection. Budget storage according to the throughput of enrolled queues. On SQS, a claim against a full queue keeps a durable intent to publish a replacement. A sustained backlog on a saturated or paused queue can generate repeat broker traffic. Account for this when using a long pause.

## Use the API, CLI, Python or MCP

The HTTP API uses `POST /v1/queue-policies/put`. For a first policy, save this JSON body as `policy.json` and send it with your operator credential:

```json
{
  "scope": {"tenant_id": "acme", "namespace": "demo"},
  "queue": "payments",
  "request_id": "payments-limit-001",
  "expected_revision": null,
  "policy": {"max_concurrency": 5, "timeout": null},
  "reason": "Payment gateway accepts five simultaneous requests"
}
```

For an edit, read `/v1/queue-policies/inspect` and supply the current revision as a decimal string. After a lost reply, resend the **identical** request with the original ID. A stale revision returns a conflict. Use `list` for configured queues, `holds` for an exact audited inspection and `release` for an inspected uncertain attempt. Summary reads require `metadata_read`.

With a matching CLI and credential file, inspect the same queue:

```sh
ledgence queue inspect --server http://127.0.0.1:8080 --tenant acme \
  --namespace demo --credentials-file ./operator.json --queue payments
```

The CLI also supports `queue list`, `holds`, `put` and `release`. Mutations read a saved `--file` with `format` set to `ledgence.queue-policy.command.v1`, the exact destination in `base_url`, and the original JSON command serialized as the string `body`. For the `policy.json` above, save this as `prepare_command.py`:

```python
import json
from pathlib import Path

command = {
    "format": "ledgence.queue-policy.command.v1",
    "base_url": "http://127.0.0.1:8080",
    "body": Path("policy.json").read_text(encoding="utf-8"),
}
Path("saved-policy.json").write_text(json.dumps(command), encoding="utf-8")
```

Submit this command to the same destination and keep the saved file for an exact retry:

```sh
python3 prepare_command.py
ledgence queue put --server http://127.0.0.1:8080 --tenant acme \
  --namespace demo --credentials-file ./operator.json --file saved-policy.json
```

In an existing authenticated Python client, create or inspect a policy:

```python
from ledgence.client import QueueConcurrencyPolicy

receipt = await client.queue_policies.put(
    "payments", policy=QueueConcurrencyPolicy(max_concurrency=5),
    request_id="payments-limit-001", reason="Payment gateway capacity",
)
snapshot = await client.queue_policies.get("payments")
print(snapshot.occupied, snapshot.uncertain)
```

The MCP server exposes `ledgence_queue_policy_inspect`, `ledgence_queue_policy_list` and `ledgence_queue_holds`. Starting MCP with `--allow-resource-control` enables `ledgence_queue_policy_put` and `ledgence_queue_hold_release`; the API still verifies the operator credential. Read-only mode exposes inspection only.
