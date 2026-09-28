# Mixed local and distributed workflow

This protocol 3 package registers ordinary Python functions as enum-addressed entrypoints. Its default entrypoint durably registers two owned workflows in the same immutable package, computes a local summary, and returns a durable join. The double branch saves its own checkpoint and timer before completing. The triple branch completes directly. The parent handles their terminal outcomes in ordinary Python.

Use current-source orchestrator and workers with workflow fork support and the database migrations installed; older published packages may lack this API. The worker must run the same CPython major/minor used to prepare the package. Follow the [checkpoint workflow example](../checkpoint-workflow/README.md) for PostgreSQL, store, worker, and client setup. Prepare and publish this additional package. Preparation requires a new output directory and refuses an existing path so stale files cannot enter the package:

```sh
"$LEDGENCE_PYTHON" examples/mixed-workflow/prepare.py "$workflow_demo/mixed/program"
./target/debug/ledgence-worker publish --source "$workflow_demo/mixed/program" --store "$workflow_demo/store"
```

Submit it using the configured Python client:

```python
run = await client.workflows.submit(
    program="mixed-workflow", version="1.0.1", queue="workflows",
    data={"values": [1, 2, 3], "queue": "workflows"},
    idempotency_key="mixed-workflow:1.0.1:1",
)
print(await run.result(timeout=60))
# {"local": {"count": 3, "sum": 6}, "double": 12, "triple": 18}
```

`await ctx.fork(...)` acknowledges registration and durable scheduling obligations. It does not await branch completion. `ctx.join(...)` returns a checkpoint; the parent invocation releases its worker slot while the branches finish. Worker concurrency one is enough to complete this example. Higher capacity permits branch execution to overlap local parent work; registration acknowledgment does not promise that a child has already started.

Branch keys and the fork key are stable for this workflow run. Retrying the parent activation reconciles the same registration and reuses an acknowledged local result. The branches have independent checkpoints and retries. Cancellation or failure drains the owned descendants. Python stack frames and local variables are not preserved across checkpoints; the parent explicitly saves its local summary in checkpoint state.
The example accepts 1–1000 integers between -1000000 and 1000000 and a queue
name of 1–128 printable ASCII characters. Invalid input fails before any fork is
registered. Failed and cancelled branches produce explicit `branch_failed` and
`branch_cancelled` workflow decisions; unexpected runtime exceptions continue to
use the activation's retry policy.
