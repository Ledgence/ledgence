# Send a report between workers

**Development source for the next release; not included in 0.9.0.** An agent can
create a report, patch or other file that the next task needs. Sending a local
path fails when the next task runs on another host; embedding the entire file in
workflow JSON adds unnecessary payload size. This example passes a small durable
artifact reference instead.

Read the three programs in order:

1. [`workflow.py`](workflow.py) records a request note, launches the producer and
   waits. After the producer succeeds, it gives the reference to the consumer.
2. [`producer.py`](producer.py) writes a synthetic report and calls
   `await artifacts.put_file(path, key="report")`. Substitute your agent's output
   file here; no agent framework is required.
3. [`consumer.py`](consumer.py) calls `await artifacts.download(reference, path)`.
   It gets a verified local file even when its worker has no access to the
   producer's filesystem. The workflow then completes with the verification.

The programs import `from ledgence.worker import artifacts`. Sources must be
regular files, at most 64 MiB; empty files are supported. Downloads never
replace existing destinations. All executions belong to the workflow root;
references do not authorize unrelated tasks to read its files.

Prepare three protocol-3 programs named `artifact-workflow`,
`artifact-producer` and `artifact-consumer`, version `1.0.0`, with their respective
file as `program.py`. Use the [program build and publication guide](../../docs/program-publication.md)
to build packages for your worker platform, then publish and register them.
Run workers on queues `artifact-producer` and `artifact-consumer`. The workflow
itself uses the producer queue; workers still use one concurrency setting each.

The orchestrator needs an execution artifact store, separate from program package
storage. Apply the explicit database migrations first and supply an
`--instance-config` with the fixed tenant/namespace for this installation. For a single-host evaluation, save this as `artifacts.json` next to the
`objects` and `spool` directories and add `--artifact-config artifacts.json` to
`ledgence orchestrator serve`:

```json
{
  "schema": "ledgence.artifact-store.v1",
  "store_id": "local-execution-files",
  "backend": {"kind": "filesystem", "directory": "objects"},
  "spool_directory": "spool"
}
```

The paths resolve relative to that configuration file; the adapter creates the
directories. The filesystem profile is qualified for one orchestrator on a
persistent local volume, with concurrent retention maintenance. Use the S3
adapter for the separately qualified shared-storage deployment profile. Retain
both the PostgreSQL catalog and the execution objects across restarts.

With the development `ledgence-client` installed, save this as `submit.py` and
run it from your environment:

```python
import asyncio
from ledgence.client import AsyncClient

async def main():
    async with AsyncClient(
        "http://127.0.0.1:8080", tenant="acme", namespace="demo"
    ) as client:
        run = await client.workflows.submit(
            program="artifact-workflow", version="1.0.0", queue="artifact-producer",
            data={"producer_queue": "artifact-producer", "consumer_queue": "artifact-consumer", "size": 300_000},
            idempotency_key="report-demo-1",
        )
        print(await run.result(timeout=60))

asyncio.run(main())
```

Repeated preparation with the same logical producer, key and metadata can return
the accepted reference. This does not make arbitrary external side effects
exactly once. If an upload acknowledgement is lost, persist its selection and use
explicit `artifacts.upload_status()` and `artifacts.reconcile()` before deciding
whether to replace a generation. Do not generate a new key to hide uncertainty.

The repository gate `tools/check-execution-artifacts.py` runs these real programs
with two worker processes, empty and multichunk files, an interrupted producer
attempt, server restart, forbidden cross-root reads, and a lost upload ACK. Its
fault injection is in the test harness, separate from the example. The gate also
verifies public CLI commands, the installed Python client and MCP metadata tools.
