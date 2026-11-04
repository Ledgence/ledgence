# Durable asynchronous exports

One approved request creates a CSV export. The provider finishes it later. Ledgence
releases the workflow worker, schedules a finite number of read-only checks, and
resumes the workflow from a typed outcome.

This example uses the Python standard library and a real local HTTP service.
There is no model, API key, cloud account, or external provider call. The service
writes a CSV artifact and a separate append-only request/effect log, so its side
effects can be compared with Ledgence's retained evidence.

Use matching Ledgence 0.8.0 CLI, API, worker and Python packages. Run the
source example from the corresponding `v0.8.0` checkout.

```mermaid
sequenceDiagram
    participant U as Reviewer
    participant W as Workflow worker
    participant L as Ledgence
    participant P as Export provider
    participant C as Check worker
    W->>L: Request approval of arguments + complete provider binding
    U->>L: Approve the frozen action and binding
    L->>W: Resume dispatch entrypoint
    W->>L: Claim the one possible dispatch
    W->>P: POST /exports
    P-->>W: Pending + export reference
    W->>L: Record typed report and park for provider outcome
    Note over W: Process available for other work
    L->>C: Scheduled read-only check
    C->>P: GET /exports?key=...
    P-->>C: CSV committed + result
    C->>L: Record typed report
    L->>W: Resume completion entrypoint with selected outcome
```

## Read the workflow

Open [program.py](program.py) in your editor. It has three named entrypoints.

1. `request` binds the business export key, effective dataset arguments, target,
   adapter contract, provider key and bounded check policy before approval.
2. `dispatch` uses `ctx.approved_provider_operation()`. The helper calls the
   registered dispatch function only after the durable grant. An existing
   operation in another workflow is returned as an explicit reference.
3. `complete` receives `ctx.provider_operation_result`. Only
   `ProviderSelectionKind.PROVIDER_SUCCEEDED` exposes `.result`. Needs-review
   and disputed outcomes are returned explicitly for application handling.

The `ProviderAdapter` registers `start_export` and `inspect_export` in this same
pinned workflow package. They share a small HTTP and classification helper. A
missing resource, lost response or HTTP 5xx becomes **unknown**. Business failure
requires an explicit provider report. There are no automatic HTTP retries or redirects. A provider
key expiring does not grant permission to repeat the POST.

`provider.py` is the synthetic service. It intentionally binds loopback. Workers
running this example must reach `http://127.0.0.1:PORT` in their own network
namespace. The service belongs to this example and runs separately from Ledgence.

## Run the local demonstration from the checkout

The native demonstration starts owned API and worker processes, runs a real
export, checks CLI/API/MCP/installed-client reads and removes its disposable
PostgreSQL database. It keeps the CSV, independent provider log and summary in
the evidence directory. Services stop when the command finishes.

Prepare the following prerequisites.

- A current `ledgence` binary built with PostgreSQL and MCP support.
- Host CPython 3.11 or later, matching the runtime used for the prepared fixture.
- A disposable PostgreSQL 18 server with database-creation rights and `psql`.
- The current client installed in a virtual environment. From this example
  directory, `uv sync` installs the client and helper from the checkout via the
  local sources in `pyproject.toml`.

From the repository root, replace the PostgreSQL connection and `psql` path with
your local values. `LEDGENCE_POSTGRES_URL` selects only the disposable database
server. The gate creates and drops a new random database while preserving the
named database. The example below assumes your binary is in `target/debug`.

```sh
LEDGENCE_POSTGRES_URL='postgres://postgres:local-password@127.0.0.1:5432/postgres' \
python3 tools/check-provider-operations.py --scenario demo \
  --binaries target/debug --psql /path/to/psql \
  --python-client examples/provider-recovery/.venv/bin/python \
  --evidence /tmp/ledgence-export-demo
```

Use a new evidence directory each time. Read `summary.json` and
`pending-provider/provider.jsonl`. The latter counts mutation requests, status
GETs, actual committed operations and completed CSV artifacts independently of
Ledgence's database. Full failure/recovery qualification uses the default
`--scenario all`, which additionally requires the preserved
prior binary/helper and exercises loss, cancellation, replacement, retention and
actual database restore.

A separate two-scenario gate covers SQS-compatible external delivery with an
owned local ElasticMQ instance. It routes **both** workflow activations and
provider checks through external queues, verifies a durable terminal wake, and
loses one committed check-report reply to verify exact replay without another
provider GET or check generation. It creates and deletes two unique test queues.
This is local compatibility validation; it does not certify AWS service limits
or production throughput.

```sh
LEDGENCE_POSTGRES_URL='postgres://postgres:local-password@127.0.0.1:5432/postgres' \
python3 tools/check-provider-operations.py --scenario sqs \
  --endpoint http://127.0.0.1:9324 \
  --binaries target/debug --psql /path/to/psql \
  --python-client examples/provider-recovery/.venv/bin/python \
  --evidence /tmp/ledgence-export-sqs
```

The 0.8.0 native delivery configuration used by this example binds one logical queue per
orchestrator process. This gate starts two processes against the same disposable
database and authority, with one publisher per queue. Both processes enable the
same provider policy and access configuration. It checks external destinations,
claim receipts and zero integrated acquisition requests, so an unintended
fallback cannot satisfy the gate. SQS remains an at-least-once transport; the
retained Ledgence operation/report identities provide the tested replay behavior.
See [Amazon SQS delivery semantics](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues.html).

## Submit from your own client

For an existing installation, enable its explicit provider policy
and configure a dedicated check queue with matching workers. See the
[provider recovery guide](../../docs-site/src/content/docs/how-to/recover-provider-operations.md).
Prepare and publish this workflow for the worker's actual platform using
[program build/publication](../../docs/program-publication.md). The provided
`ledgence.toml` requires your digest-pinned target image. Select the worker's
actual target even when the workstation uses another platform.

Start the local provider in the same network namespace as the workers.

```sh
python provider.py --directory /tmp/ledgence-export-provider --port 8092
```

Open [run.py](run.py). It uses `AsyncClient`, waits for the approval, prints the
complete frozen binding, asks you to approve it, and saves the exact decision
before sending it. It refuses to overwrite an existing decision file. Run it
from this example directory.

```sh
uv run python run.py --server http://127.0.0.1:8080 \
  --credentials ./operator.json --tenant acme --namespace demo \
  --queue demo --probe-queue provider-checks \
  --decision-file export-approval-1.json
```

If an approval reply is lost, keep that file and reconcile/retry the same saved
decision. Preserve the existing workflow and command identities throughout
recovery. A client result timeout stops waiting locally and does not cancel or
repeat the workflow.

## Test the adapter without a Ledgence deployment

```sh
uv run python -m unittest discover -s tests -v
```

These tests exercise real loopback HTTP, CSV creation and provider census. They
also prove this adapter sends one POST on a dropped reply, 5xx or redirect. They
are complemented by the native PostgreSQL/worker recovery gate for orchestration
and failure recovery.
