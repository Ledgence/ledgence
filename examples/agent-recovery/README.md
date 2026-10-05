# Recover individual agent calls

This example separates a model decision and its selected tool into durable
operations, checkpoints the transcript, then asks the model for a final answer.
A retry of the first activation reuses its acknowledged model and tool results.
The next turn reads the saved transcript in a new activation.

The included model and order lookup are **scripted fixtures**, not real AI or a
production order service. They need no accounts, API keys, network calls or
application dependencies. The example isolates Ledgence's recovery behavior;
replace `scripted_model` with an application adapter for a real model.

```text
TURN (activation 0)
  model:turn:0 -> tool:turn:0:lookup:0 -> checkpoint transcript
TURN (activation 1)
  model:turn:1 -> complete answer
```

Run the offline check with Python 3.11 or newer:

```sh
python3.13 examples/agent-recovery/check.py
```

It simulates lost acknowledgments after the model and tool records commit,
restores the recorded results into a new context, and verifies that completed
callbacks are not rerun. It also checks the turn budget and invalid input. This
check does not run a database or prove process-crash recovery; the real process
gate and its scenarios are documented in the [recovery guide](../../docs/agent-recovery.md#verification).

## Run with an orchestrator

Use matching Ledgence 0.3.0 components; earlier 0.2.0 workers do not contain
`ctx.operation`. Start the orchestrator and a protocol-3 worker using the
[checkpoint workflow setup](../checkpoint-workflow/README.md), with the runtime
helper from this checkout. Reuse that setup's `LEDGENCE_PYTHON`, `workflow_demo`
store and `workflows` queue. The worker can run with concurrency one.

```sh
"$LEDGENCE_PYTHON" examples/agent-recovery/prepare.py "$workflow_demo/agent-recovery/program"
./target/debug/ledgence program publish \
  --source "$workflow_demo/agent-recovery/program" --store "$workflow_demo/store"
```

Inside an open `ledgence.client.AsyncClient` for that deployment:

```python
run = await client.workflows.submit(
    program="agent-recovery", version="1.0.0", queue="workflows",
    data={"order_id": "ORD-1042"}, idempotency_key="agent-recovery:1",
)
print(await run.result(timeout=60))
```

Expected output:

```json
{
  "answer": "Order ORD-1042 is shipped; estimated delivery in 2 days.",
  "turns": 2,
  "model": "scripted-order-assistant-v1",
  "simulated": true
}
```

Use a new submission key for a new run. Inspect the workflow graph to see each
turn's activation and local model/tool steps; a retry retains those logical
identities and gains a new attempt ID.

## Adapt the boundary

Keep the model, messages, tool definitions and model settings explicit JSON
arguments. An adapter creates or obtains its transport client separately, reads
credentials outside journal data, validates its provider response, and returns
complete JSON. Preserve the model's tool-call ID in its committed response and
use it with the turn/occurrence when selecting the tool's operation key.

The adapter/function `version` is part of the binding. Defaults are materialized:
the scripted model records `temperature: 0`, and the lookup records
`region: "us-east"`. Changing an existing operation's effective request conflicts
before the changed call runs. The loop checkpoints after each tool turn and has
a three-turn cap; long conversations need explicit state compaction or immutable
object references within Ledgence's documented bounds.

Recovery does not guarantee an external call runs once. A crash after a provider
succeeds but before its result commits can repeat that call. Real tools should
use stable business/provider idempotency keys or reconciliation where available.
The process acceptance gate includes that gap explicitly. Live streaming,
framework adapters and dynamic tool discovery require their own integration.
