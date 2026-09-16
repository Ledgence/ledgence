# Owned subworkflows

A workflow can stage another workflow with `ctx.workflow(...)`. The child has
its own workflow ID, pinned program package, checkpoints, activation tasks,
local-step journals, event/timer waits, and terminal result. Its parent can wait
for it using the same explicit continuation model as ordinary child tasks.

```python
from ledgence_worker.workflow import workflow_context

async def handle(event):
    ctx = workflow_context()
    if ctx.continuation == "start":
        invoice = ctx.workflow(
            "issue-invoice",
            program="invoice-workflow",
            version="1.0.0",
            queue="billing",
            data={"invoice_id": event["data"]["invoice_id"]},
        )
        return ctx.suspend(continuation="after_invoice", state={}, until=[invoice])
    if ctx.continuation == "after_invoice":
        result = ctx.inputs["issue-invoice"]
        if result["state"] == "succeeded":
            return ctx.complete(ctx.get_result("issue-invoice"))
        return ctx.fail("invoice_incomplete", "The invoice workflow failed or was cancelled")
    return ctx.fail("unknown_continuation", ctx.continuation)
```

`ctx.workflow()` stages a command and returns a non-awaitable `WorkflowRef`.
The child is registered only when the controller's checkpoint decision commits.
Use `ctx.suspend(...)` to wait, or `ctx.continue_(...)` to keep the parent
advancing while owned children run. `until` can mix ordinary task references,
workflow references, and keys registered by earlier parent activations.
The [runnable example](../examples/owned-subworkflows/README.md) joins both kinds
with worker concurrency `1`.

## Ownership and child identity

Children are newly created runs in their parent's tenant and namespace. Each has
one immutable parent. Task and workflow child keys share one namespace across
all of that parent's activations. Reusing a key with the same kind and normalized
submission reuses the original child and pinned descriptor. Changing its kind,
program binding, queue, data, or execution policy conflicts. Use a fresh key for
a fresh iteration. A replay does not resolve an already pinned program again.

Public top-level workflow idempotency keys and owned-child bindings use separate
identity namespaces. A top-level submission cannot adopt an existing owned run.
Detached children and attaching existing workflows are not part of this API.

The parent checkpoint, child registration, first child activation, and required
dispatch obligations commit together. Execution starts asynchronously after
that commit. A child controller task succeeding may only mean that the child
saved another checkpoint. The parent consumes the child's **terminal workflow
outcome**, never the outcome of an individual child controller task.

## Waiting, results, and failure

A sealed `suspend` waits until every listed member is terminal. A failed or
cancelled child is a result the continuation can inspect; it does not restart
the entire child workflow or automatically determine the parent's result.
A workflow child input has this shape:

```json
{
  "kind": "workflow",
  "workflow_id": "wf_invoice",
  "state": "succeeded",
  "outcome": {"kind": "succeeded", "output": {"invoice_id": "INV-1042"}}
}
```

Failed outcomes contain `error`; cancelled outcomes have `kind: "cancelled"`.
Ordinary task inputs preserve their existing `task_id`, state, and task-outcome
shape. `ctx.get_result(key)` returns either kind's successful output.

Terminal workflow transitions create durable parent-completion work. A child
that finishes before its parent's wait is installed is still observed. Duplicate
completion work cannot consume the child twice or schedule another continuation
for an already resolved wait. A resumed parent's inputs remain frozen across its
activation attempts. External event/timer waits keep their existing semantics;
child outcomes that were not consumed remain available for a later child wait.

`ctx.complete(...)` with unfinished owned tasks or workflows is an invalid
decision. Parent cancellation, intentional failure, or exhausted controller
retries stops further child scheduling and starts cancellation/drain of the
owned descendants. The parent stays nonterminal until their logical terminal
boundaries are established. Cancelling an individual child remains possible
through its normal workflow handle; its parent observes that cancelled outcome.
Cancellation cannot undo effects that a program already performed. Existing
[task cleanup and lease guarantees](task-results.md) apply throughout the tree.

## Lineage and observability

Nested workflow status and activation context contain paired
`parent_workflow_id` and `root_workflow_id` fields. Roots and old contexts omit
both. Python context/status properties expose `None` for a root's parent and
its own workflow ID as its root. These relationships stay immutable through
retries and continuations.

Invocation CloudEvents for a nested workflow's activations and ordinary child
tasks add `ldgparentworkflowid` and `ldgrootworkflowid`. Their `ldgworkflowid`
continues to identify the owning workflow, and `ldgactivationid` is present only
for controllers. Existing task/run/attempt identities remain distinct. User data
is unchanged. Traces add `ledgence.workflow.parent.id` and
`ledgence.workflow.root.id`; protocol 3 structured program logs retain the paired
ancestry. Child submission causality follows the spawning activation's processing
trace when available, otherwise its workflow's accepted submission trace. The
invocation's producer span is a child of that origin; the CloudEvent carries the
producer context, which becomes the parent of the worker's processing span.
External event waits preserve their existing producer links.

## Bounds, persistence, and compatibility

The shared decision limit is 64 commands, and a sealed wait has at most 64
members across both child kinds. Each parent may own at most 64 nonterminal
subworkflows at a time. Root depth is zero; child depth may be at most 16.
These limits do not cap cumulative historical child keys. Inputs and event wakes
share the existing 256 KiB budget; the complete activation context remains
640 KiB. Use compact results or application-owned object references.

Waiting retains no parent invocation, consumer reservation, database connection,
or broker visibility lease. The PostgreSQL coordinator uses bounded work batches.
A child's terminal transition records a compact parent obligation without locking
the whole workflow tree. Cancellation is propagated through durable work under
each descendant's authority; a parent does not hold its run lock while waiting
for a child run lock. Ordinary task paths without workflow ownership retain their
existing behavior.

The existing 24-hour transient completion-application retry cutoff also applies
to child-workflow terminal obligations. Cancellation/drain work keeps retrying;
the lifetime of a dormant event/timer wait does not age out its recovery work.
These local bounds and recovery rules are not a throughput qualification.

Apply migration `20260917000000_owned_workflows.sql` and upgrade the orchestrator
and workers before using subworkflow commands. Runtime protocol 3 and activation
context v1 remain additive: existing task-only commands, results, and root
contexts retain their wire shapes. Third-party Rust adapters must adopt the new
child kinds and explicit `WorkflowWorkSource` variants and implement atomic owned
workflow lifecycle obligations. Public core contracts contain no vendor types.

Outbound completion notifications to external recipients remain a separate
capability. Parent completion handling uses internal durable coordination.
