---
title: Recover model and tool calls
description: Persist individual agent decisions and tool results, then resume a bounded loop after failures.
---

Use `ctx.operation` to persist each completed model or tool response before the workflow uses it. A retry of the same activation reads the committed response instead of deliberately repeating that call.

**Availability:** this helper requires the **current source checkout**. Published **0.2.0 workers predate it**. Use the worker and Python runtime helper from the same checkout. It reuses protocol 3 and the existing Rust local journal; it adds no database migration or provider dependency.

## Bind each call

Import the typed operation kind alongside your workflow:

```python
from ledgence.worker.workflow import OperationKind

response = await ctx.operation(
    "model:turn:0", request_model,
    kind=OperationKind.MODEL, version="1",
    arguments={"model": "your-model-version", "messages": messages,
               "tools": tool_definitions, "temperature": 0},
)
result = await ctx.operation(
    "tool:turn:0:call-123", lookup_order,
    kind=OperationKind.TOOL, version="1",
    arguments={"order_id": "ORD-1042"},
)
```

`request_model` and `lookup_order` are application-owned sync or async functions. Each accepts JSON keyword arguments and returns a complete JSON result. The helper freezes the callable identity, operation kind, explicit adapter/function version and effective arguments, including defaults, before starting work. It waits for the result's durable acknowledgment before releasing that result to the caller.

Keep provider/model identity, messages, settings, tool definitions and effective tool arguments explicit. Clients and credentials stay outside journal data; closure, object and environment state are not captured. Validate and convert provider responses inside the adapter. A provider SDK object, generator or partial stream is not a completed JSON result.

## Resume a bounded loop

The [runnable example](https://github.com/Ledgence/ledgence/blob/develop/examples/agent-recovery/README.md) uses a scripted model and order service, so no credentials or network access are needed. It records a model response and a tool response, then checkpoints the transcript:

```python
return ctx.continue_(
    continuation=Entry.TURN,
    state={"round": next_round, "messages": next_messages},
)
```

The next activation receives that state and a fresh local journal. Python locals and stacks are not persisted. Keep model-generated tool-call IDs in the saved model response and use turn plus call occurrence in operation keys. An existing key with a changed binding fails before executing the changed call. Two distinct calls with identical inputs should use different keys.

Use a turn/tool budget. Current limits are 128 records and 256 KiB of journal per activation, 128 KiB per complete record, and 64 KiB per checkpoint state. Long prompts repeated across calls consume the journal budget; compact deliberately or use immutable application-owned object references. Checkpointing can release the worker slot between bounded batches, but has scheduling cost. Active local I/O still occupies its workflow activation's slot.

The execution graph shows model/tool calls as local steps using their operation keys and prefixed callable identities. Attempts and available execution/replay observations remain separate from logical node identity. Token counts and provider costs are not inferred.

## Understand the failure boundary

A committed model decision is reused before selecting its tools. Committed tool results are reused before updating the transcript. Other routing-relevant nondeterminism must also be recorded or checkpointed; the helper does not make arbitrary Python control flow recoverable.

If an external call succeeds and the process dies before its result commits, that call can repeat. Use stable business/provider idempotency keys or reconcile the outcome where supported. Attempt IDs change during recovery and are unsuitable as stable external idempotency keys. Durable execution does not guarantee exactly-once external effects or token-stream resumption.

The [source contract and verification guide](https://github.com/Ledgence/ledgence/blob/develop/docs/agent-recovery.md) covers fault injection, process restart, lost acknowledgments, JSON binding rules and the primary-source design comparison. Framework adapters and dynamic tool discovery are separate integrations.
