---
title: Workflow context
description: Entrypoint registration, context properties, child operations, decisions, and inline limits for Python workflows.
---

This reference covers protocol **3** workflow handlers and their worker-supplied context.

**Available since 0.2.0:** `Workflow` registration, `ctx.entrypoint`, `branch`, `fork`, and `join` require matching orchestrator and worker versions and all database migrations, including `20260928000000_workflow_forks.sql`. The existing `workflow_context()` interface and string continuations remain supported. See [Upgrade to 0.3.1](/how-to/upgrade-to-0-3).

A handler can obtain the context directly:

```python
from ledgence.worker.workflow import workflow_context

async def handle(event):
    ctx = workflow_context()
    return ctx.complete(event["data"])
```

`workflow_context()` returns the active `WorkflowContext`. Application code does not construct this object. The handler receives the original CloudEvent; workflow control state is separate from `event["data"]`.

## Entrypoint registration

```python
from enum import StrEnum
from ledgence.worker.workflow import Workflow

class Entry(StrEnum):
    START = "start"
    RESUME = "resume"

workflow = Workflow(Entry)

@workflow.entrypoint(Entry.START, default=True)
def start(event, ctx):
    return ctx.sleep("delay:0", 1000, continuation=Entry.RESUME,
                     state={"saved": event["data"]})

@workflow.entrypoint(Entry.RESUME)
def resume(event, ctx):
    return ctx.complete(ctx.state["saved"])

handle = workflow.build()
```

`Workflow(Entry)` accepts a `StrEnum` family. Register each member once with `@workflow.entrypoint(member, default=False)` and mark exactly one default. Registration rejects aliases and duplicate handlers. `build()` requires a handler for every member and exactly one default, then freezes the registry. Each handler receives `(event, ctx)` and may be synchronous or asynchronous; the exported `handle` is asynchronous. Keep the package manifest's handler as the exported callable, for example `program:handle`.

Public workflow submissions start at the default; there is no public initial-entrypoint override. Named entrypoints are selected by branch creation or a resume decision. The initial wire continuation `"start"` selects the default, which may have another name such as `"main"`. If the enum declares `"start"`, it must be the default.

In a registered workflow, use this exact enum family for `branch(entrypoint=...)`, `join(resume=...)`, and `continuation` in `continue_`, `suspend`, `wait_event`, and `sleep`. Strings and another enum's members are rejected. Existing unregistered contexts continue accepting string continuations. Enum values are stable wire addresses; publish a new immutable package version for code changes.

See [Mix local work and workflow branches](/how-to/fork-workflow-branches) for a complete package and execution instructions.

## Properties

| Property | Value |
| --- | --- |
| `workflow_id` | Workflow execution ID. |
| `activation_id` | Current logical controller activation ID; stable across its task attempts. |
| `revision` | Checkpoint revision supplied to this activation. |
| `continuation` | Wire continuation string; initially `"start"` for a public submission. A branch starts at its selected entrypoint. |
| `entrypoint` | Selected enum member in a registered workflow; otherwise the legacy continuation string. |
| `state` | JSON state saved by the previous checkpoint. |
| `inputs` | Frozen child outcomes, indexed by child key. |
| `wake` | External event, timeout, timer or approval wake; otherwise `None`. |
| `approval` | Typed immutable approval view for an approval wake; otherwise `None`. |
| `parent_workflow_id` | Parent workflow ID for an owned subworkflow; otherwise `None`. |
| `root_workflow_id` | Root ancestor's ID; equal to `workflow_id` for a root. |

Reading `state`, `inputs`, or `wake` returns an independent JSON copy. Mutating it does not update the stored checkpoint; return a new decision to save state.

## Local steps

### `operation(key, fn, *, kind, version, arguments)` — added in 0.3.1

Use `OperationKind.MODEL` or `OperationKind.TOOL` from `ledgence.worker.workflow`. This helper uses the local journal with an explicit operation kind/version and signature-bound JSON arguments, including defaults. It returns only a completed, acknowledged result and rejects an existing key whose binding changed. Keys share the activation's local-step namespace. See [Recover model and tool calls](/how-to/recover-agent-calls) for boundaries, checkpoints and setup.


### `local(key, fn, **kwargs)`

Starts an owned local operation and returns an awaitable. Awaiting it returns the JSON output only after its durable result record has been acknowledged.

The binding includes the stable callable name and explicit JSON keyword arguments. Keys are scoped to a logical activation. Reusing the same key and binding shares or replays its result; changing the binding is an error. Pass all changing inputs explicitly—closure variables are not part of the binding.

The function can be synchronous or asynchronous. A synchronous function keeps normal blocking behavior. A local function cannot create branch specifications, fork or stage children, issue control decisions, or start another journaled local step.

### `await gather(*operations)`

Waits for the supplied local operations and returns their results in argument order. Asynchronous I/O can overlap in the current process. If any operation fails, the helper waits for the gathered operations and raises a failure. At most 128 operations may be gathered.

Started local work remains owned by the activation and is drained before it returns. An unobserved local failure fails the activation; a failed durable commit cannot be converted into a successful decision.

## Child operations

### `task(key, *, program, version, queue, data, retry_policy=None, attempt_timeout_ms=300000)`

Stages an independently scheduled task and returns a `TaskRef`. The reference is not awaitable. Dispatch follows acceptance of a returned checkpoint decision.

### `workflow(key, *, program, version, queue, data, retry_policy=None, attempt_timeout_ms=300000)`

Stages an owned subworkflow and returns a `WorkflowRef`. Its terminal outcome resolves the parent wait, rather than any individual controller activation's outcome.

Both operations use the following options:

| Argument | Contract |
| --- | --- |
| `key` | Child identity within the parent workflow; shared across task and workflow children, including fork branches. |
| `program`, `version` | Published program ID/version; lowercase portable path components, at most 128 bytes each. |
| `queue` | Queue for the child execution. |
| `data` | User-owned JSON input. |
| `retry_policy` | Dictionary with `max_attempts` and `retry_delay_ms`; defaults to `3` and `5000`. |
| `attempt_timeout_ms` | Integer from `60000` through `86400000`; defaults to `300000`. |

`max_attempts` must be from 1 through 1000; `retry_delay_ms` from 0 through 86400000. These are integer values.

An identical binding for an existing key reuses the original child. Changing its kind, program, input, or scheduling options conflicts. Use a new iteration key for new work.

### `branch(key, *, entrypoint, queue, data, retry_policy=None, attempt_timeout_ms=300000)`

Builds an immutable `BranchSpec` in the current context. It performs no RPC, schedules nothing, and adds no staged child command. `entrypoint` selects a registered handler. The branch uses the parent's exact pinned program descriptor, so there is no `program` or `version` argument and no new program lookup.

`queue`, `data`, retry policy, and attempt timeout follow the options above. The explicit `data` becomes the branch's user-owned input. Entrypoint and execution metadata remain outside CloudEvent `data`. The branch's workflow ID, activation attempts, checkpoints, local journal, and event/timer waits are independent of the parent's.

### `await fork(key, *, branches)`

Atomically registers an ordered list of 1–64 branch specifications and the children's durable scheduling obligations. It returns a `ForkRef` only after acknowledgment. The parent stays in the same activation, revision, and process and can continue local work. Child execution can begin after commit; acknowledgment does not establish that a child has started or completed.

The fork key is stable across the workflow run. Branch keys share the parent's child-key namespace. An exact retry reuses the original registration; changed membership, order, entrypoint, queue, data, or execution policy conflicts. A new fork cannot adopt another fork's branch or a staged child. Use fresh iteration-qualified keys for new work.

`ForkRef.key` is the fork key; `ForkRef.branch_keys` is its immutable ordered tuple of child keys. A reference belongs to the context that created it and is not JSON checkpoint state. Reconcile the same fork binding in a later activation or use `suspend(until=[...])` with durable child keys.

A registration survives a parent crash. An uncertain acknowledgment prevents a successful final decision, even if application code catches the error. Calling `fork` does not commit other staged `task` or `workflow` commands; those still belong to the returned checkpoint decision.

### `get_result(ref_or_key)`

Returns a successful child's JSON output from this activation's `inputs`. It raises `WorkflowError` if the child is absent, failed, or cancelled. Inspect `ctx.inputs[key]["outcome"]` when handling unsuccessful outcomes.

References belong to the activation that created them. Use a saved string key in a later continuation.

## Decisions

Return one of these decisions from the controller handler:

| Method | Effect when accepted |
| --- | --- |
| `join(fork, *, resume, state)` | Save state and wait until every branch in the acknowledged `ForkRef` is terminal; returns a suspend decision. |
| `suspend(*, continuation, state, until=())` | Save state and wait until every listed child is terminal. Accepts distinct child references or keys; an empty wait is immediately ready. |
| `continue_(*, continuation, state)` | Save state and schedule the next activation without waiting. |
| `wait_event(key, *, continuation, state, timeout_ms=None)` | Save state and wait for one external event, optionally with a persisted timeout. |
| `sleep(key, delay_ms, *, continuation, state)` | Save state and register a durable timer. |
| `request_approval(key, *, action, state, timeout_ms, continuation=None, resume=None, proposed_arguments=None)` | Persist an immutable effective action, save state, and wait for its durable approval outcome. |
| `complete(output)` | Finish with JSON output. Cannot discard staged launches or finish with nonterminal owned children. |
| `fail(kind, message)` | Request intentional workflow failure and draining of owned work. |

`join` and `suspend` wait for all listed terminal outcomes, including failure and cancellation. They do not fail fast or automatically cancel siblings. The resume handler receives frozen outcomes in `inputs`, including children that finished before wait registration. A later explicit join can read retained terminal results again without re-executing the children. Automatic pending-input delivery through `continue_` consumes each input only once.

Returning a durable wait releases the parent invocation's worker slot. A resume may run in another process. Python stacks and locals are not serialized; save every required value in explicit JSON state.

Checkpoint decisions include staged child commands. Calling a decision method alone does not dispatch or durably save it; return the decision from the handler.

Wait keys are one-shot across the workflow. Event timeouts and timer delays accept integer milliseconds from zero through **31536000000** (365 days). `timeout_ms=None` has no deadline. Error messages passed to `fail` are limited to 4096 UTF-8 bytes. Unexpected handler exceptions use the activation retry policy. Intentional failure, cancellation, or exhausted activation retries drain owned descendants; external effects are not undone.

## Durable approvals

`ApprovalAction.for_callable(fn, version="1", arguments={...})` binds the callable and effective JSON arguments, including defaults. Return `ctx.request_approval(...)` with exactly one `resume` or `continuation` to persist this action and release the worker slot. On resume, inspect `ctx.approval.status`; `await ctx.approved_local(fn, version="1")` executes only an approved saved action and accepts no replacement arguments. Its acknowledged local result is replayable. Generic external events cannot decide an approval.

See [Require approval before a tool call](/how-to/require-approval) for the typed statuses, deadline and recovery behavior.

## Wake shapes

An event wake contains the complete accepted CloudEvent:

```text
{"kind": "event", "key": ..., "event": ..., "accepted_at": ...}
```

Timeout and timer wakes contain the persisted deadline:

```text
{"kind": "timeout", "key": ..., "deadline": ...}
{"kind": "timer", "key": ..., "deadline": ...}
```

Timestamps are Unix milliseconds. `inputs` remains reserved for child outcomes. A resumed activation sees a frozen wake across retries.

## Inline limits

| Resource | Limit |
| --- | --- |
| Checkpoint state | 64 KiB |
| One local record, including binding and output | 128 KiB |
| Local journal per logical activation | 128 records / 256 KiB combined |
| One decision, including staged commands | 256 KiB |
| Combined child commands per decision | 64 |
| Children in an all-terminal wait | 64 |
| Frozen child inputs | 64 entries |
| Child inputs and optional wake together | 256 KiB |
| Complete activation context | 640 KiB |
| Complete external event | 64 KiB |
| Live owned subworkflows per parent | 64 |
| Nested subworkflow depth | 16, with the root at depth zero |

Fork limits additionally apply:

| Resource | Limit |
| --- | --- |
| Entrypoint ID, fork key, branch key, and queue | Nonempty text, at most 128 UTF-8 bytes each |
| Branches in one fork | 1–64 |
| Complete fork request | 128 KiB |
| Fork command including lease owner and trace | 144 KiB |
| Accepted fork ledger per logical activation, including retries | 64 forks / 256 KiB combined |
| Newly registered children per logical activation across forks and staged decisions | 64 combined |

Bounds use compact encoded JSON, not Python object memory size. The server's JSON encoding is authoritative; new Python writes can conservatively reject values near a byte limit because float representations differ. Accepted reads and exact committed local-result replay have a bounded encoding allowance.

Application JSON supports at most 64 nested containers, finite numbers, and string object keys. Use application-controlled storage references for payloads larger than the inline limits.

**Release contracts:** [Worker helper implementation](https://github.com/Ledgence/ledgence/blob/v0.3.1/sdk/python/ledgence/worker/workflow.py) · [Workflow contract](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/workflows.md) · [Owned subworkflows](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/subworkflows.md)

**Additional contracts:** [Entrypoints and forks](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/workflow-entrypoints.md) · [Worker helper](https://github.com/Ledgence/ledgence/blob/v0.3.1/sdk/python/ledgence/worker/workflow.py)
