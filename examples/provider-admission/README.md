# Approved exports with shared capacity

Several workflows want to export data, but only one may start a provider call at
a time. Each workflow gets approval, waits durably for shared capacity, starts
its approved export once, and then waits separately for the export result.
Waiting does not occupy a worker subprocess.

Use the `v0.9.0` source checkout, matching Ledgence components, the database migrations and Console C14.
The provider uses real loopback HTTP and writes CSV files. No model, provider
account or API key is needed.

```mermaid
sequenceDiagram
    participant W as Workflow
    participant L as Ledgence
    participant U as Reviewer
    participant P as Local export provider
    W->>L: Freeze export arguments, provider binding and resource
    U->>L: Approve that exact intent
    L->>W: Resume reserve entrypoint
    W->>L: Request export-dispatch capacity and park
    L->>W: Resume dispatch when an offer is available
    W->>L: Atomically consume capacity and the one dispatch grant
    W->>P: POST /exports once
    P-->>W: HTTP 202, export pending
    W->>L: Record completed call + pending business result
    Note over L: Release call capacity; retain business recovery
    W->>L: Park for the provider result
    L->>P: Worker runs a bounded read-only probe
    P-->>L: CSV committed + result
    L->>W: Resume result entrypoint
```

## Read the program

Open [program.py](program.py) in your editor. Four typed entrypoints keep the
steps explicit:

1. `request` asks for approval. The resource name is part of the reviewed provider
   binding; it cannot be substituted after approval.
2. `reserve` returns `ctx.request_provider_admission(...)`. Ledgence saves the
   wait and releases the process, preserving the original approved effect identity.
3. `dispatch` uses `ctx.admitted_provider_operation()`. Only the first executable
   grant calls the registered adapter. A retained ticket is reused after recovery.
   An equal operation in another workflow is returned as a reference, not adopted.
4. `result` receives the selected provider outcome. Only provider success supplies
   the export result; review, dispute and other states remain explicit.

The application-owned `ProviderAdapter` has two callbacks. `start_export` returns
`ProviderDispatchOutcome`, which separates call completion from business knowledge.
`inspect_export` returns a plain `ProviderReport` and never starts an export.
They share a bounded HTTP transport with no automatic retries or redirects.

A fully received HTTP 202 completes the call but does not complete the export.
A lost response makes original call completion unknown even if the provider
created the CSV. A successful later probe can finish the workflow while that
uncertain call still occupies capacity. An operator may release it explicitly
after inspection; release never permits a second mutation or invents success.
Read-only probes are outside the dispatch resource limit.

## Run the complete local check

The check starts owned orchestrator and worker processes, creates a disposable
database, configures one shared resource and uses the existing synthetic export
service. It records received POSTs, GETs and actual CSV effects independently of
Ledgence. It also inspects the same retained record through API, CLI, MCP,
installed Python client and Console contracts.

Prepare a source-built `ledgence` binary with all features, PostgreSQL 18 with
permission to create disposable databases, `psql`, and host CPython 3.11 or later.
From this example directory, run `uv sync` to install the client and helper from
the checkout. From the repository root:

```sh
LEDGENCE_POSTGRES_URL='postgres://postgres:local-password@127.0.0.1:5432/postgres' \
python3 tools/check-resource-admission.py --provider \
  --binaries target/debug --psql /path/to/psql \
  --python-client examples/provider-admission/.venv/bin/python \
  --evidence /tmp/ledgence-provider-admission
```

Replace the connection and paths with your local values. The environment variable
selects a disposable database server; the check creates and drops its own random
database and preserves the named database. Use a new evidence directory each run.
Read `summary.json`, `results.json` and `synthetic-provider/provider.jsonl` afterward.
The check stops its services when finished. Add
`--endpoint http://127.0.0.1:9324` to run the same cases through local ElasticMQ;
it creates and deletes its own queues for workflow and probe delivery.

The gate covers parking and restart, competing workflows, lost Start and Report
replies, unknown call occupancy after successful recovery, duplicate business
identities, replacement workers, both restore fences, and original frozen reports
after execution retirement. Native-only fixtures observe result selection and
hold responses at deliberate failure points; provider requests and actual CSV
creation are counted independently. These checks establish behavior under those
faults, not production throughput or AWS service limits.

## Use an existing installation

Enable [provider recovery](../../docs/provider-operations.md) and
[resource admission](../../docs/resource-admission.md) with the same authenticated
audience on every API replica. Create an `export-dispatch` resource with capacity
one and run matching workers on the workflow and dedicated probe queues. Prepare
and publish this program for the workers' actual target using
[program publication](../../docs/program-publication.md); replace the target image
placeholder in `ledgence.toml` with your digest-pinned worker image.

The service and interactive approval client are shared with the
[provider-recovery example](../provider-recovery/README.md). From this directory,
start the provider in a terminal:

```sh
uv run python ../provider-recovery/provider.py \
  --directory /tmp/ledgence-admitted-export-provider --port 8092
```

It binds loopback, so the workers must reach that same network namespace. In a
second terminal, submit and review the frozen request:

```sh
uv run python ../provider-recovery/run.py --program provider-admission \
  --server http://127.0.0.1:8080 --credentials ./operator.json \
  --tenant acme --namespace demo --queue demo --probe-queue provider-checks \
  --decision-file export-approval-1.json
```

The client prints the full binding before asking for approval and saves the exact
decision before sending it. Preserve that file after an uncertain reply. A local
result timeout only stops waiting; it does not cancel or resubmit the workflow.

## Test the adapter locally

```sh
uv run python -m unittest discover -s tests -v
```

These tests use real HTTP, check call-completion versus business classifications,
and count provider requests. The native gate above additionally verifies durable
orchestration, capacity, identity and recovery.
