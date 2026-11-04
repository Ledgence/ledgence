---
title: Give an investigation a shared call allowance
description: Bound calls and delegations across one workflow and its descendants.
---

**Development feature.** Use matching development components with resource admission enabled. The published 0.8.0 packages predate run budgets.

A support agent has two tickets to analyze, but you only want to pay for one classifier call. Setting a limit on each child would give them two calls in total. Instead, declare the allowance once when you submit the parent. Both child workflows then use the same balance.

## Submit the parent

[Enable resource admission](/how-to/control-resource-capacity) and publish the two programs from the [shared run budget example](https://github.com/Ledgence/ledgence/tree/develop/examples/shared-run-budget). Open the example's `submit.py` in your editor. This is the part that sets the limits:

```python
from ledgence.client import RunBudgetPolicy

budget = RunBudgetPolicy(max_calls=1, max_delegations=2)
```

Pass `budget=budget` to `client.workflows.submit(...)`. Two delegations let the parent create both children. One call means only one child can receive the authorization that permits the classifier request. An offer to prepare a call consumes no unit. If the orchestrator restarts before the children finish, they still share the original balance.

## Read the outcome

Use `await client.budgets.inspect(workflow_id=run.id)` to see the root and its counters. Then call `await client.budgets.entries(workflow_id=run.id, limit=20)` to see the execution and reason behind each decision. Inspecting either child resolves to the same root. A refusal such as `calls_exhausted` differs from an ordinary wait for a busy model endpoint: finishing an existing call releases resource capacity, while the consumed run allowance stays spent.

You can independently cap delegation or supply an absolute `deadline_at_ms`. These limits govern new work Ledgence authorizes. An already-started external call may still finish. Use an adapter that exposes every physical attempt when you need a call limit to account for provider retries.

Cancelling the investigation stops new governed work throughout its tree. Cancelling a child affects that branch. Its siblings can still use the remaining allowance.

An owned task created with `ctx.task` consumes a delegation. If a child must make a governed physical call, use an owned workflow with a registered operation. The ticket-review example uses this pattern for each classifier request.
