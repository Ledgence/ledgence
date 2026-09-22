# Local self-hosted deployment

The Compose deployment builds Ledgence from this checkout and runs PostgreSQL,
explicit schema migration, the orchestrator, one reusable-process worker, and a
small example callback receiver. Programs are published into a shared program
store after startup and fetched into the worker's persistent verified cache.
There is no required vendor account or hosted service.

This is a local, operator-trusted-code deployment. The API is bound to host
loopback. Database credentials are fixed nonsecret demo values, the internal
network uses HTTP, and the example receiver is deliberately bounded. Do not
expose this configuration as a public multi-tenant service.

## Start and run the example

Install Docker Engine or Docker Desktop with Compose v2 supporting `up --wait`.
Use a Linux container platform (`linux/amd64` or `linux/arm64`). The build uses
pinned image digests, Rust 1.98.1, the committed Cargo lock, and CPython 3.14.
The initial build needs access to the image registry and crates.io. Rust and
Python do not need to be installed on the host for this example.

From the repository root:

```sh
export LEDGENCE_SOURCE_REVISION="$(git rev-parse HEAD)"
docker compose -f deploy/local/compose.yaml build
docker compose -f deploy/local/compose.yaml up -d --wait --wait-timeout 120
docker compose -f deploy/local/compose.yaml run --rm --no-deps publish
docker compose -f deploy/local/compose.yaml run --rm --no-deps demo
```

The example publishes three platform-correct immutable program packages. It
submits two invoice tasks, checks that their healthy Python process is reused,
then runs a checkpoint workflow with four concurrent local I/O steps and a
distributed summary task. The workflow releases the single worker slot while
waiting and resumes from its checkpoint. Finally it checks durable task and
workflow completion callbacks, registered deliberately after completion.

The printed JSON contains task/workflow/subscription IDs and `passed: true`.
Repeating `publish` with identical packages is safe; repeating `demo` uses fresh
idempotency keys. Changed package contents require a new program version. The
runtime's concurrency defaults to **one** for the demo's reuse assertion; set
`LEDGENCE_CONCURRENCY` before startup for ordinary operation, but run this
particular acceptance example at one slot.

The public API is `http://127.0.0.1:8080`. Set `LEDGENCE_HTTP_PORT` before `up` to
choose another host port. Install `sdk/python-client` in a virtual environment
(or install its candidate wheel), then use:

```python
import asyncio
from ledgence.client import AsyncClient

async def main():
    async with AsyncClient("http://127.0.0.1:8080", tenant="acme", namespace="demo") as client:
        task = await client.tasks.submit(
            program="invoice-issuer", version="1.0.0", queue="demo",
            data={"invoice_id": "INV-1042"},
            idempotency_key="issue:INV-1042", correlation_key="INV-1042",
        )
        subscription = await task.subscribe(
            destination="demo-callback", idempotency_key="notify:INV-1042",
        )
        print(task.id, subscription.id)
        print(await task.result(timeout=60))

asyncio.run(main())
```

A result timeout limits observation; it does not retry execution. Submission
and subscription are separate operations. Delivery guarantees start after
subscription registration is accepted. The receiver deduplicates `(source,id)`
and persists its small record before acknowledging. Its 256-event capacity is a
demo limit; it returns 503 when full. [Callback semantics](completion-notifications.md)
explain retry exhaustion, redelivery and production receiver responsibilities.

## ElasticMQ instead of integrated acquisition

Use a separate Compose project and port so its durable logical queue route does
not conflict with an existing integrated deployment:

```sh
export LEDGENCE_HTTP_PORT=8081
docker compose -p ledgence-elasticmq -f deploy/local/compose.yaml -f deploy/local/compose.elasticmq.yaml up -d --wait --wait-timeout 120
docker compose -p ledgence-elasticmq -f deploy/local/compose.yaml -f deploy/local/compose.elasticmq.yaml run --rm --no-deps publish
docker compose -p ledgence-elasticmq -f deploy/local/compose.yaml -f deploy/local/compose.elasticmq.yaml run --rm --no-deps demo
```

Build the common image first using the earlier command. The override starts the
reviewed pinned ElasticMQ image and declares its Standard queue in configuration.
A bounded readiness job checks the exact queue before the orchestrator starts.
The worker and orchestrator use fixed nonsecret credentials against the explicit
`elasticmq` service endpoint on the Compose network; no AWS account or metadata
service is involved. These are environment-chain credentials because the
container's service hostname is not a loopback endpoint.

PostgreSQL remains the durable task authority. This local ElasticMQ fixture
keeps queue messages in memory; restart recovery relies on Ledgence's retained
dispatch obligations. The default deployment persists PostgreSQL, program
packages, worker cache and receiver state. Local compatibility tests do not
establish real AWS SQS acceptance or a throughput promise.

## Startup, shutdown and recovery

`migrate` runs after PostgreSQL is healthy and must exit successfully before the
orchestrator starts. `serve` verifies the schema rather than changing it. A
worker starts after orchestrator readiness. Receiver failure after startup does
not change task outcomes; durable delivery retries independently.

```sh
docker compose -f deploy/local/compose.yaml ps
docker compose -f deploy/local/compose.yaml logs --tail 100 orchestrator worker
docker compose -f deploy/local/compose.yaml down --timeout 65
docker compose -f deploy/local/compose.yaml up -d --wait --wait-timeout 120
```

`down` preserves the named volumes. Both process supervisors receive SIGTERM and
have 65 seconds before Docker forces termination. Abrupt loss still uses durable
leases and recovery; application effects remain at-least-once and must be
idempotent. Cache contents may be rebuilt from the immutable store, but published
packages and database state must be backed up together according to application
requirements. Copying a live PostgreSQL volume is not a consistent backup; use
PostgreSQL backup tools and validate restoration before upgrades.

Before changing the source image or schema, back up the database/program store,
stop the worker and orchestrator, build the chosen committed source, rerun the
explicit migration job, then start the services. Forward migration does not imply
that older binaries are compatible with the new schema. Restore a matching
backup when rollback requires it; do not invent down migrations.

To erase this local deployment's data intentionally:

```sh
docker compose -f deploy/local/compose.yaml down --volumes --timeout 65
```

Include the same `-p` and override `-f` options when operating on the ElasticMQ
project. This removes that project's database, program store, cache and callback
data. It does not remove unrelated projects or images.

## Qualification and distribution boundary

```sh
python3 tools/check-deployment.py --backend both --evidence /tmp/ledgence-compose-evidence
```

The gate owns random project/image names, ephemeral loopback ports and disposable
volumes. It builds a real Linux image, runs both complete demos, recreates the
containers without deleting volumes, verifies retained workflow/callback state,
runs the demo again and cleans up only its own projects. Evidence records the
actual image ID, source label, OS and architecture. A configured architecture is
not a claim that it was executed; consult the result for the candidate being
released.

Pinned inputs and normalized candidate archives improve repeatability; Ledgence
does not claim byte-identical compiled binaries across hosts. The Docker image
is built locally, not published by release tooling. Its upstream Debian, CPython
and utility components retain their licenses and possible source-distribution
obligations. Their notices remain in the base image; `/opt/ledgence/legal`
contains the Python license, runtime package inventory, Cargo notices and Rust
copyright inventory. Review the complete base-image redistribution obligations
before publishing an OCI image. This does not impose those OS licenses on
Ledgence-owned code or on user programs. Native [candidate bundles](releasing.md)
keep the host CPython and system libraries separately supplied.

Primary references: [Compose startup order](https://docs.docker.com/compose/how-tos/startup-order/),
[Docker digest pinning](https://docs.docker.com/build/building/best-practices/),
[ElasticMQ queue configuration](https://github.com/softwaremill/elasticmq/blob/v1.7.1/README.md),
[CPython licenses](https://docs.python.org/3.14/license.html).
