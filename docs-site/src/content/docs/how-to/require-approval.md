---
title: Require approval before an action
description: Persist an effective action, review its exact arguments, and resume safely after a durable decision.
---

Use a durable approval when a reviewer must authorize the exact action your workflow will execute. Ledgence stores the request and releases the worker slot while it waits.

**Availability:** included in **Ledgence 0.3.1** with matching worker, orchestrator, runtime helper, Python client and Console contract **5**. Apply migrations with old writers stopped; follow [Upgrade to 0.3.1](/how-to/upgrade-to-0-3).

## Prepare the workflow

Start with the [durable approval example](https://github.com/Ledgence/ledgence/blob/v0.3.1/examples/durable-approval/README.md), which includes preparation, publication, and submission instructions for a source deployment. Use host CPython 3.11 or newer, with the same Python major/minor when preparing and running the package. The controller uses runtime protocol **3**.

Validate and normalize arguments **before** requesting approval. This controller pattern proposes a simulated refund of 100 but limits the effective amount to 50:

```python
from enum import StrEnum
from ledgence.worker.workflow import ApprovalAction, ApprovalStatus, Workflow

class Entry(StrEnum):
    REQUEST = "request"
    RESOLVE = "resolve"

workflow = Workflow(Entry)

def simulate_refund(amount, currency="USD"):
    return {"simulated": True, "amount": amount, "currency": currency}

@workflow.entrypoint(Entry.REQUEST, default=True)
def request(event, ctx):
    data = event.get("data")
    if (type(data) is not dict or type(data.get("amount")) is not int
            or not 1 <= data["amount"] <= 10_000):
        return ctx.fail("invalid_input", "amount must be an integer from 1 to 10000")
    action = ApprovalAction.for_callable(
        simulate_refund, version="1", arguments={"amount": min(data["amount"], 50)},
    )
    return ctx.request_approval(
        "refund", action=action, proposed_arguments={"amount": data["amount"]},
        resume=Entry.RESOLVE, state=None, timeout_ms=86_400_000,
    )

@workflow.entrypoint(Entry.RESOLVE)
async def resolve(event, ctx):
    approval = ctx.approval
    if approval is None:
        return ctx.fail("missing_approval", "expected an approval wake")
    if approval.status is not ApprovalStatus.APPROVED:
        return ctx.complete({"status": approval.status.value, "executed": False})
    return ctx.complete(await ctx.approved_local(simulate_refund, version="1"))

handle = workflow.build()
```

For submission data `{"amount": 100}`, the effective arguments are `{"amount": 50, "currency": "USD"}`. `for_callable` captures the callable identity, explicit version, supplied arguments, and keyword defaults. Optional `proposed_arguments` must be an object; it is audit context and does not authorize execution.

The request key is one-shot within its workflow. Use a new key for a changed action or another review round. Every approval has a finite timeout; the database owns its deadline. Inspect and list responses report persisted status, so a request can remain `pending` after its deadline until the coordinator commits expiry. New decisions are still rejected at or after the deadline.

## Review the stored request

After the first activation commits, open the workflow in Console and select **General → Approvals**. Inspect the action name, version, effective arguments, original proposal, and deadline before choosing **Approve** or **Reject**. The execution graph links its approval wait to the same review section.

Alternatively, inside an open `ledgence.client.AsyncClient` configured for that source deployment:

```python
import json
from pathlib import Path

run = client.workflows.handle(workflow_id)
approval = await run.approval("refund")
print(approval.action.to_dict())

command = run.prepare_approval_decision(
    approval, decision_id="refund-review:1", decision="approve",
    reviewer="operator", reason="Reviewed the effective amount",
)
Path("decision.json").write_text(json.dumps(command.to_dict()))
receipt = await run.decide_approval(command)
```

Save the command in application-owned storage before sending it. `await run.approvals(limit=10)` lists requests; a request does not exist until its workflow checkpoint commits. Use `decision="reject"` to decline it.

**Reviewer is claimed attribution, not an authenticated identity.** The deployment's authenticated application boundary or access-controlled proxy must determine who can inspect and decide requests. Console's instance binding and allowed-Origin checks do not authenticate a reviewer.

## Reconcile uncertainty and observe execution

The client does not retry decisions automatically. If it raises `ApprovalDecisionUncertain`, retain its `command` and explicitly resend that exact command. After a client restart, restore the saved command using the same endpoint, workflow, and compatibility binding:

```python
command = run.restore_approval_decision(json.loads(Path("decision.json").read_text()))
receipt = await run.decide_approval(command)
```

Do not generate a new decision ID or change the action, reviewer, or reason to resolve an uncertain response. Identical retries reconcile while the record is retained, including after expiry or workflow completion. Console's retry also preserves the exact submitted decision. Generic workflow events cannot approve a request.

Approval, rejection, and expiry resume one logical activation. Cancellation closes a pending request without resuming it. Acceptance means the decision was recorded; inspect the workflow result separately to observe execution.

`approved_local` accepts no replacement arguments. It checks the saved callable identity and version, uses a private copy of the approved arguments, and reuses an acknowledged local result on activation retry. The callable remains operator-trusted Python code. An external effect can succeed **before** its local result commits; use the external service's idempotency mechanism or reconcile that gap. Durable approval does not guarantee exactly-once external effects.

For CLI commands, HTTP endpoints, limits, and retention semantics, see the [source approval contract](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/workflow-approvals.md).
