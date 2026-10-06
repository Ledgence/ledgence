# Local self-hosted deployment

The Compose deployment builds Ledgence from this checkout and runs PostgreSQL,
explicit schema migration, the orchestrator, one reusable-process worker, and a
small example callback receiver. Programs are published into a shared program
store after startup and fetched into the worker's persistent verified cache.
There is no required vendor account or hosted service.

This guide describes the source-built Compose deployment, including Console.
Use the `v0.4.0` source tag for this release; building `develop` uses the current
checkout's code and dependency pins. For prebuilt images and `ledgence local`,
start with [installation](installation.md) and the separate
[container distribution](container-distribution.md). Follow
[Explore Ledgence Console](https://docs.ledgence.com/tutorials/use-console) for the
browser-guided setup. Existing deployments must follow the
[upgrade guide](upgrading-to-0.3.md) before reusing their database. It also links
the required earlier migration steps for 0.1 installations.

This is a local, operator-trusted-code deployment. The API is bound to host
loopback. Database credentials are fixed nonsecret demo values, the internal
network uses HTTP, and the example receiver is deliberately bounded. Do not
expose this configuration as a public multi-tenant service.

## Start and run the example

Install Docker Engine or Docker Desktop with Compose v2.23.1 or newer, supporting `up --wait` and inline configs.
Use a Linux container platform (`linux/amd64` or `linux/arm64`). The build uses
pinned image digests, Rust 1.98.1, the committed Cargo lock, and CPython 3.14.
A separate pinned Node 24.21.0/pnpm 11.27.1 build stage produces the static Console. Node and node_modules are excluded from the runtime. The initial build needs access to the image registry, npm and crates.io. Rust and
Python do not need to be installed on the host for this example.

From the repository root:

```sh
export LEDGENCE_SOURCE_REVISION="$(git rev-parse HEAD)"
export LEDGENCE_SOURCE_DIRTY="$(test -z "$(git status --porcelain)" && echo false || echo true)"
docker compose -f deploy/local/compose.yaml build
docker compose -f deploy/local/compose.yaml up -d --wait --wait-timeout 120
docker compose -f deploy/local/compose.yaml run --rm --no-deps publish
docker compose -f deploy/local/compose.yaml run --rm --no-deps demo
```

Open [Console](http://127.0.0.1:8080/console/) after startup. Its server-owned instance config binds this demo to the existing `acme/demo` compatibility scope. Legacy workers/SDK requests must match; the browser has no scope selector.

The example publishes and then explicitly registers three platform-correct immutable program packages: `invoice-issuer@1.0.0`, `workflow-example@1.0.1`, and `workflow-summary@1.0.0`. Registration failures leave the immutable artifact intact; repeat publish/registration to reconcile. It
submits two invoice tasks, checks that their healthy Python process is reused,
then runs a checkpoint workflow with four concurrent local I/O steps and a
distributed summary task. The workflow releases the single worker slot while
waiting and resumes at a typed entrypoint from its checkpoint. The controller
validates input before scheduling work and handles failed or cancelled summary
tasks explicitly. Finally it checks durable task and
workflow completion callbacks, registered deliberately after completion.

The printed JSON contains task/workflow/subscription IDs and `passed: true`.
Repeating `publish` with identical packages is safe; repeating `demo` uses fresh
idempotency keys. Changed package contents require a new program version. The
runtime's concurrency defaults to **one** for the demo's reuse assertion; set
`LEDGENCE_CONCURRENCY` before startup for ordinary operation, but run this
particular acceptance example at one slot.

The public API is `http://127.0.0.1:8080`; Console is under `/console/`. Set `LEDGENCE_HTTP_PORT` before `up` to
choose another host port. Install the published client in a Python 3.11+ virtual
environment with `python -m pip install "ledgence-client==0.4.0"`, then use:

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

## Run the installed Python client example

After starting the stack and publishing its programs, install the published
**0.4.0** client in a host virtual environment. Use host CPython 3.11–3.14; this
interpreter runs the client, while programs execute using the separately declared
interpreter inside the worker container.

```sh
python3 -m venv /tmp/ledgence-compose-client
/tmp/ledgence-compose-client/bin/python -m pip install "ledgence-client==0.4.0"
/tmp/ledgence-compose-client/bin/python -I -B examples/local-compose-client.py --server http://127.0.0.1:8080
```

Run the companion from the source checkout after the Compose setup above. The
[Console tutorial](https://docs.ledgence.com/tutorials/use-console) follows this
0.4.0 stack. The [release tutorial](https://docs.ledgence.com/tutorials/run-locally)
starts from the matching `v0.4.0` tag. See the
[release reference](https://docs.ledgence.com/reference/releases) for available
native bundles and registry versions; these are released separately.

For local package qualification, install the wheel produced by the
[package gate](registry-packages.md#qualification) in place of the PyPI command.
A native bundle also includes a wheel and this companion example; use its actual
wheel version and keep the source checkout for Compose files and program
publication. Installation resolves the package's pinned dependencies; offline
installation needs a separately prepared reviewed wheelhouse.

The companion uses `from ledgence.client import AsyncClient` for every task,
workflow and completion operation. It runs two invoice tasks and verifies exact
outputs and warm process reuse, then executes the published workflow's four local
I/O steps and distributed summary task. It registers task/workflow subscriptions
only after completion, then observes their persisted `delivered` statuses and
reference-only CloudEvents. Every result and delivery wait has a 90-second bound;
the complete example has a 300-second bound. A timeout or uncertain submission
fails the example without retrying execution. Keep concurrency at one and avoid
concurrent example traffic for the reuse assertion. `--server` also supports the
ElasticMQ project's loopback API port.

Its JSON output records the actual installed SDK version, module paths and
hashes. Editable installs and source-shadowed imports are rejected. The receiver
stays private on the Compose network; the qualification gate below additionally
compares its persisted events with the complete SDK-observed events, including
both `source` and `id`, before and after container recreation.

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

# Add the host interpreter containing the installed client wheel to qualify both paths.
python3 tools/check-deployment.py --backend both --client-python /tmp/ledgence-compose-client/bin/python --evidence /tmp/ledgence-compose-sdk-evidence
```

The gate owns random project/image names, ephemeral loopback ports and disposable
volumes. It builds a real Linux image, runs both complete demos, recreates the
containers without deleting volumes, verifies retained workflow/callback state,
runs the demo again and cleans up only its own projects. With `--client-python`,
it additionally runs the installed-SDK companion on both deployments before and
after recreation and verifies its retained workflow/callback state and receiver
events. Evidence records the actual installed SDK provenance and the
actual image ID, source label, OS and architecture. A configured architecture is
not a claim that it was executed; consult the result for the candidate being
released.

Pinned inputs and normalized candidate archives improve repeatability; Ledgence
does not claim byte-identical compiled binaries across hosts. This Compose path
builds its image locally. Versioned image publication and local-kit generation
have separate [distribution gates](container-distribution.md). The 0.4.0 release
provides the qualified images and kit; this source-built path does not use them.
The image's upstream Debian, CPython
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
