---
title: Require approval before an action
description: Persist an effective action, review its exact arguments, and resume safely after a durable decision.
---

Use a durable approval when a reviewer must authorize the exact action your workflow will execute. Ledgence stores the request and releases the worker slot while it waits.

**Availability:** introduced in **Ledgence 0.3.1**; this guide uses **0.8.0** with matching worker, orchestrator, runtime helper, Python client and Console contract **7**. Apply migrations with old writers stopped; follow [Upgrade to 0.8.0](/how-to/upgrade-to-0-8).

## Prepare the workflow

Start with the [durable approval example](https://github.com/Ledgence/ledgence/blob/v0.8.0/examples/durable-approval/README.md), which includes preparation, publication, and submission instructions for a source deployment. Use host CPython 3.11 or newer, with the same Python major/minor when preparing and running the package. The controller uses runtime protocol **3**.

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

## Find pending requests across workflows

To plan a review shift, use **Due before (UTC)** in Console.
For example, 17:00 UTC on November 6 includes a request due at 16:59:59 UTC that
day and excludes one due at exactly 17:00. The CLI equivalent is
`--due-before 2026-11-06T17:00:00Z`.

Open **Approvals** in Console to see requests from all
workflows in this instance, with the earliest deadline first. Choose a program
to narrow the list. Open a request and compare the effective action with what
you intended before approving it. The workflow link opens its graph at the approval
wait. You can return to the list to review another
workflow. In the CLI, the equivalent discovery command is:

```sh
ledgence approval inbox --server http://127.0.0.1:8080 --tenant acme --namespace demo
```

The summary includes the workflow ID and approval key. Use them in the existing
workflow detail or with `ledgence approval inspect` to read the arguments.

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

For CLI commands, HTTP endpoints, limits, and retention semantics, see the [source approval contract](https://github.com/Ledgence/ledgence/blob/v0.8.0/docs/workflow-approvals.md).

<span id="protect-an-approved-write-in-development"></span>

## Protect an approved write

Use `ctx.request_effect_approval` with the same parameters as
`request_approval` to declare the execution contract before reviewing the
action.

```python
return ctx.request_effect_approval(
    "refund", action=action, resume=Entry.APPLY, state=None,
    timeout_ms=86_400_000,
)
```

The saved request carries `protection: "external_effect"`. The client echoes it
when preparing the decision. `approved_local` rejects this occurrence before
calling the function, even with a cached result. The backend rejects an ordinary
journal commit for the same occurrence. Existing untagged requests retain their
behavior. Use matching 0.8.0 clients and Console to review typed requests.

Suppose a payment provider accepts a refund just before the worker dies. A
retry of `approved_local` could send it again. In 0.8.0,
`await ctx.approved_effect(issue_refund, version="1", lookup=find_refund)`
asks the orchestrator for permission before making a write. Only one claim
can permit the send. If no grant committed before the original attempt died,
a retry can still get that first grant. After the grant commits, recovery uses
`find_refund` with the exact approved arguments; a saved receipt replays
without either function running. If the refund remains uncertain, Ledgence
keeps its workflow evidence even after cancellation and past the normal
retention period. When a callback confirms the receipt, Ledgence saves the
result and lifts that hold together.

Keep `find_refund` read-only. Both callbacks should validate that the receipt
matches the approved action. If the outcome is uncertain, raise
`EffectOutcomeUnknown` and have the workflow stop or wait for investigation.
A lost grant may mean no refund was sent; a missing receipt is not permission
to try again. Disable automatic write retries inside the provider client and
keep the same protected helper on every attempt. Trusted application code
remains responsible for its own direct provider calls.

This method requires an offline migration and matching 0.8.0 components.
The [source contract and example](https://github.com/Ledgence/ledgence/blob/develop/docs/workflow-approvals.md#protect-an-approved-external-write)
explain the upgrade and the limits.
