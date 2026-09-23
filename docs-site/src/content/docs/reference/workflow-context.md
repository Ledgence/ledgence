---
title: Workflow context
description: Properties, decisions, child operations, and inline limits for Python checkpoint workflows.
---

Import the worker-supplied helper inside a protocol **3** workflow handler:

```python
from ledgence.worker.workflow import workflow_context

async def handle(event):
    ctx = workflow_context()
    return ctx.complete(event["data"])
```

`workflow_context()` returns the active `WorkflowContext`. Application code does not construct this object. The handler receives the original CloudEvent; workflow control state is separate from `event["data"]`.

## Properties

| Property | Value |
| --- | --- |
| `workflow_id` | Workflow execution ID. |
| `activation_id` | Current logical controller activation ID; stable across its task attempts. |
| `revision` | Checkpoint revision supplied to this activation. |
| `continuation` | Explicit entry label; initially `"start"`. |
| `state` | JSON state saved by the previous checkpoint. |
| `inputs` | Frozen child outcomes, indexed by child key. |
| `wake` | External event, timeout, or timer wake; otherwise `None`. |
| `parent_workflow_id` | Parent workflow ID for an owned subworkflow; otherwise `None`. |
| `root_workflow_id` | Root ancestor's ID; equal to `workflow_id` for a root. |

Reading `state`, `inputs`, or `wake` returns an independent JSON copy. Mutating it does not update the stored checkpoint; return a new decision to save state.

## Local steps

### `local(key, fn, **kwargs)`

Starts an owned local operation and returns an awaitable. Awaiting it returns the JSON output only after its durable result record has been acknowledged.

The binding includes the stable callable name and explicit JSON keyword arguments. Keys are scoped to a logical activation. Reusing the same key and binding shares or replays its result; changing the binding is an error. Pass all changing inputs explicitly—closure variables are not part of the binding.

The function can be synchronous or asynchronous. A synchronous function keeps normal blocking behavior. A local function cannot stage workflow children, issue control decisions, or start another journaled local step.

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
| `key` | Child identity within the parent workflow; shared across task and workflow children. |
| `program`, `version` | Published program ID/version; lowercase portable path components, at most 128 bytes each. |
| `queue` | Queue for the child execution. |
| `data` | User-owned JSON input. |
| `retry_policy` | Dictionary with `max_attempts` and `retry_delay_ms`; defaults to `3` and `5000`. |
| `attempt_timeout_ms` | Integer from `60000` through `86400000`; defaults to `300000`. |

`max_attempts` must be from 1 through 1000; `retry_delay_ms` from 0 through 86400000. These are integer values.

An identical binding for an existing key reuses the original child. Changing its kind, program, input, or scheduling options conflicts. Use a new iteration key for new work.

### `get_result(ref_or_key)`

Returns a successful child's JSON output from this activation's `inputs`. It raises `WorkflowError` if the child is absent, failed, or cancelled. Inspect `ctx.inputs[key]["outcome"]` when handling unsuccessful outcomes.

References belong to the activation that created them. Use a saved string key in a later continuation.

## Decisions

Return one of these decisions from the controller handler:

| Method | Effect when accepted |
| --- | --- |
| `suspend(*, continuation, state, until=())` | Save state and wait until every listed child is terminal. Accepts distinct child references or keys; an empty wait is immediately ready. |
| `continue_(*, continuation, state)` | Save state and schedule the next activation without waiting. |
| `wait_event(key, *, continuation, state, timeout_ms=None)` | Save state and wait for one external event, optionally with a persisted timeout. |
| `sleep(key, delay_ms, *, continuation, state)` | Save state and register a durable timer. |
| `complete(output)` | Finish with JSON output. Cannot discard staged launches or finish with nonterminal owned children. |
| `fail(kind, message)` | Request intentional workflow failure and draining of owned work. |

Checkpoint decisions include staged child commands. Calling a decision method alone does not dispatch or durably save it; return the decision from the handler.

Wait keys are one-shot across the workflow. Event timeouts and timer delays accept integer milliseconds from zero through **31536000000** (365 days). `timeout_ms=None` has no deadline. Error messages passed to `fail` are limited to 4096 UTF-8 bytes.

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

Bounds use compact encoded JSON, not Python object memory size. The server's JSON encoding is authoritative; new Python writes can conservatively reject values near a byte limit because float representations differ. Accepted reads and exact committed local-result replay have a bounded encoding allowance.

Application JSON supports at most 64 nested containers, finite numbers, and string object keys. Use application-controlled storage references for payloads larger than the inline limits.

**Source:** [Worker helper implementation](https://github.com/Ledgence/ledgence/blob/develop/sdk/python/ledgence/worker/workflow.py) · [Workflow contract](https://github.com/Ledgence/ledgence/blob/develop/docs/workflows.md) · [Owned subworkflows](https://github.com/Ledgence/ledgence/blob/develop/docs/subworkflows.md)
