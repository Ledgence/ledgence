---
title: Checkpoints and recovery
description: Understand what survives an activation retry, why explicit continuations matter, and where effects can repeat.
---

A checkpoint records the information needed for the next workflow activation. It does not snapshot the Python process.

That boundary makes recovery explicit: your controller chooses the next entrypoint, saves JSON state, and identifies the work it needs before resuming. Ledgence persists that decision and later schedules a fresh activation.

## What survives

A checkpoint records the next continuation and state together with staged child registrations and the chosen wait. The coordinator applies these changes atomically, so a committed checkpoint does not leave its child registrations or dispatch obligations only in process memory.

A resumed handler receives the original CloudEvent and a separate workflow context. The context contains the saved state and frozen outcomes for that activation. Python locals, open sockets, coroutine stacks, and in-memory clients are not restored.

Register each handler with `@workflow.entrypoint(...)` and select the next enum member in `ctx.join(...)`, `ctx.suspend(...)`, or another decision. Ledgence invokes that handler when the next activation starts. Existing handlers that route on the string `ctx.continuation` remain supported.

A distributed child reference is not awaitable in the current coroutine. Returning a wait decision saves the required state and releases the invocation instead.

## Local results reduce repeated work

`await ctx.local(key, fn, **inputs)` returns after the result record is committed and acknowledged. If that logical activation retries, an already committed result is loaded and returned after its binding is checked. The helper does not deliberately run that local function again.

Ordinary code around those steps still executes from the current continuation. Put all changing inputs in the local step's explicit arguments so that its durable binding describes the operation. A closure or mutable process global is not part of that binding.

A local step must not issue workflow control commands or stage children. Replaying its stored result would skip those commands. Keep coordination in the controller after awaiting the local result.

For model and tool calls, `ctx.operation` also binds an explicit kind, adapter/function version, and effective keyword arguments, including defaults. Read the committed model response before selecting tools and the committed tool response before advancing the transcript. Other nondeterminism that controls routing must also be recorded or saved in checkpoint state. Each new activation starts with a fresh local journal; a result is replayed only across attempts of its original activation. See [Recover model and tool calls](/how-to/recover-agent-calls).

## Acknowledged forks survive the parent invocation

`await ctx.fork(...)` durably registers the branch workflows before the parent checkpoints. The parent can continue local work after the acknowledgment. If it then stops unexpectedly, the accepted branches remain registered.

On retry, the same fork key and ordered branch bindings reconcile those children instead of launching replacements. An uncertain acknowledgment prevents a successful final decision; it must not be treated as an empty or successful fork. Use a new fork key and new child keys for a new loop iteration.

`return ctx.join(...)` saves the parent's state and waits for all branch outcomes, including failure and cancellation. A branch that finished before the join was recorded still contributes its retained outcome. The resumed entrypoint decides whether to recover or fail; the join itself does not fail fast.

## A checkpoint does not make external effects exactly once

Consider a local step that calls a payment API:

1. The payment provider accepts the charge.
2. The process stops before the local result is durably confirmed.
3. A later attempt has no confirmed local result and calls the provider again.

The external effect and the Ledgence result record are different transactions. Use a stable business idempotency key accepted by the external service, or reconcile with that service before repeating an uncertain operation.

The same principle applies to ordinary task retries. Cancelling a workflow does not reverse effects that already happened.

## Identity distinguishes retries from new work

There are several useful identity scopes:

| Identity | Scope and purpose |
| --- | --- |
| Submission idempotency key | Identifies a task or workflow submission within the instance's fixed compatibility binding; task and workflow submission keys are separate. |
| Activation ID | Identifies one logical entrypoint invocation across its task attempts. A later visit to the same entrypoint gets a new activation. |
| Local/operation key | Identifies work within one logical activation and its retries; both helpers share the journal namespace. |
| Child key | Identifies an owned task or subworkflow throughout its parent workflow, including fork branches. |
| Fork key | Identifies one immutable ordered branch registration throughout the parent workflow. |
| Event/timer/approval wait key | Identifies one one-shot wait throughout the workflow. |

Reusing a key with the same binding can reconcile existing work. Reusing it for different work conflicts. Iteration identifiers such as `summary:round-2` make a new operation explicit.

These identities are not interchangeable with tracing IDs. Trace context helps explain execution; it does not define its durable ownership or idempotency.

## Waiting survives the controller

After suspension, PostgreSQL owns the wait. A child that finishes before the wait is installed is found through durable state rather than being lost as an in-memory notification.

External events can also arrive before their wait is registered. Once an event or timer selects a wake, Ledgence persists the wake and next activation together. Retrying that activation sees the same wake instead of selecting a new one.

Timers use a persisted deadline. Downtime or capacity can delay execution after that deadline, but retries do not restart the duration. A timer is a scheduling boundary, not a promise of execution at an exact wall-clock instant.

An approval wait additionally persists an immutable action and its effective arguments. The review decision binds to that exact request; approval, rejection, or expiry resumes one logical activation with a frozen approval wake. `ctx.approved_local` takes its arguments from the approved record, and its committed result survives activation retries. The grant does not carry into later activations or child workflows. As with any local effect, approval cannot close the gap between an external side effect and the result commit. See [Require approval before an action](/how-to/require-approval).

## Failure and cancellation drain owned work

`ctx.fail(...)` deliberately requests workflow failure. Unexpected controller exceptions and timeouts instead follow the activation task's retry policy. When those retries are exhausted, the workflow fails and its owned work is drained.

A workflow in `failing` or `cancelling` is still nonterminal. Ledgence prevents new decisions from launching more work and drains its owned tasks and subworkflows before establishing the workflow's terminal outcome.

A controller cannot complete successfully while owned children are still nonterminal. The application must decide how to use failed or cancelled child outcomes; an all-terminal wait does not mean all children succeeded.

## Keep the current boundaries in mind

State and results are bounded inline JSON. Use references to application-managed storage for larger values. The current workflow model does not provide automatic code upgrades, administrative redrive, or arbitrary Python stack replay.

The PostgreSQL adapter also bounds retries of failed completion-obligation application: after an obligation is at least 24 hours old, a further transient application failure fails the workflow and starts draining owned work. This is a retry cutoff, not a general workflow lifetime limit; an older obligation can still apply successfully.

For exact methods and byte limits, use the [workflow context reference](/reference/workflow-context). To practice the successful path, follow [your first workflow](/tutorials/first-workflow).

**Source:** [Workflow persistence and recovery](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/workflows.md#persistence-and-recovery) · [Event races and recovery](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/workflow-events.md#identity-races-and-recovery) · [Task result semantics](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/task-results.md)
