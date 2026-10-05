# Durable model and tool calls

A workflow can record individual model and tool responses before using them to
choose its next action. On an activation retry, Ledgence returns the committed
response for the same operation key and binding. The model can make a different
decision on a new invocation; recovery of a committed invocation reuses the
original decision.

This helper is included in Ledgence 0.3.1. Use matching worker and Python
runtime-helper versions. See the [upgrade guide](upgrading-to-0.3.md) when moving
an existing deployment from 0.2.0.
There is no new database migration, queue message type, or provider dependency.

## Bind the effective call

```python
from ledgence.worker.workflow import OperationKind

response = await ctx.operation(
    "model:turn:0", request_model,
    kind=OperationKind.MODEL, version="1",
    arguments={
        "model": "your-model-version",
        "messages": messages,
        "tools": tool_definitions,
        "temperature": 0,
    },
)

result = await ctx.operation(
    "tool:turn:0:call-123", lookup_order,
    kind=OperationKind.TOOL, version="1",
    arguments={"order_id": "ORD-1042"},
)
```

`request_model` and `lookup_order` are application functions in the pinned
program package. They accept JSON keyword arguments and return a complete JSON
value, synchronously or asynchronously. The helper binds the function's module
and qualified name, operation kind, explicit adapter/function version, and all
effective keyword arguments, including defaults. `OperationKind` is an enum;
plain strings are rejected. Input is copied before scheduling the call, and the
function receives its own copy. Returned results are copied too.

Include every semantic input: provider/model identity, messages, sampling
settings, tool schemas, prepared tool arguments, and any business idempotency
key. `version` identifies your adapter/function contract; it does not pin a
provider's mutable model alias. Clients, sockets and credentials remain outside
journal payloads. Closure state, bound-object state and environment configuration
are not captured. Keep semantic configuration explicit rather than hiding it in
those objects. Never put API keys in arguments or returned values.

Normalize and validate the provider response inside your adapter before it
returns. SDK response objects, generators and partial streams are not JSON
results. If consuming a stream, finish collecting its complete response within
the operation; live token delivery and resuming a provider stream are separate
capabilities. Nested workflow operations inside an operation callback are not
allowed: wrap each model or tool boundary, not an entire `agent.run()` loop.

## Identity, recovery, and uncertainty

Operation keys share the local-step namespace and are scoped to one logical
activation. Include turn and call occurrence in the key. Two distinct calls with
identical arguments need distinct keys; this is invocation recovery, not a
cross-call cache. Concurrent calls with the same key and binding share one owned
execution. Reusing a key with a changed kind, version, callable, model request or
effective tool arguments fails before executing that changed call. Numeric
identity preserves the existing JSON distinctions, including `1` versus `1.0`.

The Rust store commits each completed result before its await resolves. On
retry, the same activation ID and key find that result under a new worker
attempt. Committed model output must be read before selecting its tools; committed
tool output must be read before advancing the agent transcript. Ordinary Python
around those boundaries runs again. Record any other nondeterminism that affects
routing, including tool discovery or race-dependent selection, or carry the
necessary state across an explicit checkpoint. This is not Python stack recovery.

A failed callable has no committed result and can run again under the activation
retry policy. An error deliberately returned as JSON becomes a durable result.
An uncertain commit acknowledgment prevents the original context from returning
a successful workflow decision; the worker reconciles the commit or a later
attempt reads the accepted record.

An external call can succeed before its result is committed. In that window it
may repeat, including billable model calls. Use a stable business/provider
idempotency key or reconcile the external outcome where supported. A key derived
from workflow ID, activation ID and operation key stays stable across attempts;
an attempt ID does not. Ledgence does not guarantee exactly-once external effects.
No provider-specific retry, cancellation, or reconciliation policy is added by
this helper.

## Bound the loop and checkpoint explicitly

The [agent recovery example](../examples/agent-recovery/README.md) runs one model
turn and its selected tool in an activation, then returns
`ctx.continue_(continuation=Entry.TURN, state=...)`. The next activation starts
with a fresh local journal and the saved transcript/cursor. Only state explicitly
included in that checkpoint crosses the boundary. An acknowledged local result
survives retries of its activation; it is not automatically imported into another
activation.

Use a turn/tool budget. Current bounds remain 128 local records and 256 KiB of
combined journal per activation, 128 KiB per complete record, and 64 KiB per
checkpoint state. Repeating a long prompt in several bindings consumes that
journal budget. Compact application state deliberately or use immutable
application-owned object references for large content. Exceeding a bound fails;
Ledgence does not silently trim messages or checkpoints.

Calls execute locally in the current workflow subprocess. They do not create a
separate task, queue delivery or worker slot per model/tool invocation. Each new
completed call still requires a durable result acknowledgment. A call doing
active I/O retains its activation's worker slot; an explicit checkpoint or
durable wait returns that slot. Checkpointing every turn trades another
activation for bounded recovery state; choose a bounded batch appropriate to your
workload. The existing graph displays these calls as local steps with their keys
and model/tool-prefixed callable names. Provider token counts and cost accounting
are not inferred from responses.

## Design research

Primary sources reviewed on 2026-10-04:

| System | Relevant approach | Ledgence decision |
| --- | --- | --- |
| [Temporal](https://docs.temporal.io/workflow-definition) | Workflow replay reuses completed activity results; orchestration commands must stay compatible. | Keep nondeterministic calls behind durable boundaries and reject a changed binding for an existing invocation key. |
| [PydanticAI backend builder](https://pydantic.dev/docs/ai/capabilities/durable_execution/backends/) | Public adapters wrap model, tool, discovery and capability operations with stable names and codecs. | Establish a provider-neutral primitive first; a framework adapter can remain optional. |
| [Prefect caching](https://docs.prefect.io/v3/concepts/caching) | Persisted task results are reused according to cache identity and storage configuration. | Distinguish invocation occurrence from equivalent input; same-input calls can be separate work. |
| [LangGraph checkpoints](https://docs.langchain.com/oss/python/langgraph/checkpointers) | Checkpoints and pending task writes recover graph progress; persistence mode changes crash guarantees. | Acknowledge each result before dependent work; use explicit bounded checkpoints for loop state. |
| [Restate durable steps](https://docs.restate.dev/develop/python/durable-steps) | Journals nondeterministic operation results for replay. | Reuse Ledgence's existing Rust journal rather than adding a second persistence engine. |
| [Hatchet durable tasks](https://docs.hatchet.run/v1/durable-tasks) | Replays between checkpoints and places side effects in child tasks. | Preserve the distinction between recoverable orchestration and retryable external effects. |

These systems already support dynamic agents and durable execution. This feature
makes Ledgence's own binding and recovery contract explicit; it does not claim
that competitors require a predefined agent DAG. No performance ranking follows
from these architectural comparisons.

The [PydanticAI backend interface](https://raw.githubusercontent.com/pydantic/pydantic-ai/main/pydantic_ai_slim/pydantic_ai/durable_exec/_operation_backend.py) supplies a cache-identity projection, not
the complete original request to every callback. A future adapter must account
for that difference before claiming the same strict input binding. This delivery
does not install or implement a PydanticAI, ADK, or other framework integration.

## Verification

```sh
python3.13 examples/agent-recovery/check.py
python3.13 -m unittest discover -s sdk/python/tests -p 'test_workflow_operations.py'
# With an owned disposable PostgreSQL 18 instance and the installed client:
python3.13 tools/check-workflows.py --scenario agent-crash-model \
  --scenario agent-crash-tool --scenario agent-lost-ack \
  --scenario agent-effect-gap --scenario agent-binding --scenario agent-example \
  --psql /path/to/psql --binaries target/debug --evidence /path/to/new-evidence
```

The offline check simulates journal transport. The process gate runs the Rust
orchestrator and worker with PostgreSQL, uses controlled process termination and
lost acknowledgments, and records per-attempt evidence. Add
`--endpoint http://127.0.0.1:9324` to use a disposable local ElasticMQ queue.
Its provider fixtures are synthetic: the gate proves recovery behavior, not a
live model integration or throughput target.
