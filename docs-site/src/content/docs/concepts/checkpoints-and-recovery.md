---
title: Checkpoints and recovery
description: Understand what survives an activation retry, why explicit continuations matter, and where effects can repeat.
---

A checkpoint records the information needed for the next workflow activation. It does not snapshot the Python process.

That boundary makes recovery explicit: your controller chooses a continuation label, saves JSON state, and identifies the work it needs before resuming. Ledgence persists that decision and later schedules a fresh activation.

## What survives

A checkpoint records the next continuation and state together with staged child registrations and the chosen wait. The coordinator applies these changes atomically, so a committed checkpoint does not leave its child registrations or dispatch obligations only in process memory.

A resumed handler receives the original CloudEvent and a separate workflow context. The context contains the saved state and frozen outcomes for that activation. Python locals, open sockets, coroutine stacks, and in-memory clients are not restored.

This is why the handler branches on `ctx.continuation` and returns `ctx.suspend(...)` rather than awaiting a distributed child reference in the same coroutine.

## Local results reduce repeated work

`await ctx.local(key, fn, **inputs)` returns after the result record is committed and acknowledged. If that logical activation retries, an already committed result is loaded and returned after its binding is checked. The helper does not deliberately run that local function again.

Ordinary code around those steps still executes from the current continuation. Put all changing inputs in the local step's explicit arguments so that its durable binding describes the operation. A closure or mutable process global is not part of that binding.

A local step must not issue workflow control commands or stage children. Replaying its stored result would skip those commands. Keep coordination in the controller after awaiting the local result.

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
| Submission idempotency key | Identifies a task or workflow submission within its tenant and namespace; task and workflow submission keys are separate. |
| Local step key | Identifies work within one logical activation and its retries. |
| Child key | Identifies an owned task or subworkflow throughout its parent workflow. |
| Event/timer wait key | Identifies one one-shot wait throughout the workflow. |

Reusing a key with the same binding can reconcile existing work. Reusing it for different work conflicts. Iteration identifiers such as `summary:round-2` make a new operation explicit.

These identities are not interchangeable with tracing IDs. Trace context helps explain execution; it does not define its durable ownership or idempotency.

## Waiting survives the controller

After suspension, PostgreSQL owns the wait. A child that finishes before the wait is installed is found through durable state rather than being lost as an in-memory notification.

External events can also arrive before their wait is registered. Once an event or timer selects a wake, Ledgence persists the wake and next activation together. Retrying that activation sees the same wake instead of selecting a new one.

Timers use a persisted deadline. Downtime or capacity can delay execution after that deadline, but retries do not restart the duration. A timer is a scheduling boundary, not a promise of execution at an exact wall-clock instant.

## Failure and cancellation drain owned work

`ctx.fail(...)` deliberately requests workflow failure. Unexpected controller exceptions and timeouts instead follow the activation task's retry policy. When those retries are exhausted, the workflow fails and its owned work is drained.

A workflow in `failing` or `cancelling` is still nonterminal. Ledgence prevents new decisions from launching more work and drains its owned tasks and subworkflows before establishing the workflow's terminal outcome.

A controller cannot complete successfully while owned children are still nonterminal. The application must decide how to use failed or cancelled child outcomes; an all-terminal wait does not mean all children succeeded.

## Keep the current boundaries in mind

State and results are bounded inline JSON. Use references to application-managed storage for larger values. The current workflow model does not provide automatic code upgrades, administrative redrive, or arbitrary Python stack replay.

The PostgreSQL adapter also bounds retries of failed completion-obligation application: after an obligation is at least 24 hours old, a further transient application failure fails the workflow and starts draining owned work. This is a retry cutoff, not a general workflow lifetime limit; an older obligation can still apply successfully.

For exact methods and byte limits, use the [workflow context reference](/reference/workflow-context). To practice the successful path, follow [your first workflow](/tutorials/first-workflow).

**Source:** [Workflow persistence and recovery](https://github.com/Ledgence/ledgence/blob/develop/docs/workflows.md#persistence-and-recovery) · [Event races and recovery](https://github.com/Ledgence/ledgence/blob/develop/docs/workflow-events.md#identity-races-and-recovery) · [Task result semantics](https://github.com/Ledgence/ledgence/blob/develop/docs/task-results.md)
