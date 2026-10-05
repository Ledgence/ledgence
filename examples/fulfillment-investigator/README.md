# Fulfillment investigator

**The problem:** delivery performance appears to have dropped. Is the carrier
late, or is the report missing warehouse data?

**The solution:** Ledgence ingests four sources, checks that the evidence is
complete, and runs four independent investigations. A bounded agent assembles a
report from verified results. A person reviews the exact report before the
workflow publishes it locally.

All orders, warehouse records, carrier events, and support tickets are
**synthetic**. The default agent is a **scripted fixture**, not a model. Optional
Codex mode uses a real provider on the same synthetic evidence. No commerce,
carrier, or support account is needed, and this example sends no customer
messages or external reports.

This is application **1.0.0**, included in the **Ledgence 0.3.0** source release.
Use the matching orchestrator, worker, helper and Python client, and apply all
migrations. Earlier Ledgence 0.2.0 does not include the durable operation and
action-approval APIs used here.

## Tutorial: run the investigation

Use CPython **3.11 or newer**, Rust, and a dedicated PostgreSQL **18** database.
The commands below select `python3.13`; substitute another supported interpreter
if needed (the macOS system `python3` may be older).
Prepare and run the package on the same operating system, CPU architecture, and
Python major/minor. The application uses the standard library, including SQLite;
SQLite must be available in the host interpreter.

The orchestrator, workers, and companion client in this example share the same
host and absolute artifact-directory path. The program package and artifact
store are separate. This example does not configure a remote object store or
lakehouse catalog.

Commands run from the repository root. Select your interpreter and a fresh run
directory outside the repository; the preparation command refuses to overwrite
an existing directory.

```sh
export LEDGENCE_PYTHON="$(command -v python3.13)"
"$LEDGENCE_PYTHON" --version
cargo build -p ledgence-cli --locked
export FULFILLMENT_HOME="$HOME/.local/share/ledgence-fulfillment"
"$LEDGENCE_PYTHON" -m venv "$FULFILLMENT_HOME/client"
"$FULFILLMENT_HOME/client/bin/python" -m pip install --require-hashes --only-binary=:all: \
  -r sdk/python-client/third_party/runtime-requirements.txt \
  -r sdk/python-client/third_party/build-requirements.txt
"$FULFILLMENT_HOME/client/bin/python" -m pip install --no-deps --no-build-isolation ./sdk/python-client
export FULFILLMENT_RUN="$FULFILLMENT_HOME/run-1"
"$LEDGENCE_PYTHON" examples/fulfillment-investigator/prepare.py \
  --directory "$FULFILLMENT_RUN" --binaries target/debug
```

The preparation output records the immutable program descriptor, package-file
checksums, and synthetic fixture references in `prepared.json`. It publishes
`fulfillment-investigator@1.0.0` to the local program store and prepares both
scenarios in the separate `data` directory. It does not start services or submit
an execution.

Set `DATABASE_URL` to your dedicated demonstration database. Do not point the
migration or acceptance setup at an unrelated deployment.

```sh
export DATABASE_URL='postgres://USER:PASSWORD@127.0.0.1:5432/ledgence_fulfillment'
target/debug/ledgence orchestrator migrate
target/debug/ledgence orchestrator serve --bind 127.0.0.1:8087 \
  --store "$FULFILLMENT_RUN/store" \
  --instance-config "$FULFILLMENT_RUN/instance.json"
```

For Console, build the current assets following [its README](../../console/README.md)
and add `--console-dir console/dist` to `serve`. Open
[the local Console](http://127.0.0.1:8087/console/). Its graph shows recorded
execution, with one workflow level per canvas. Open a branch to inspect its
steps and return to the parent to see the join.

Keep the orchestrator running. In a second terminal, restore the same
`FULFILLMENT_RUN` and `LEDGENCE_PYTHON` values, then register the program and start
a worker:

```sh
target/debug/ledgence program register --server http://127.0.0.1:8087 \
  --program fulfillment-investigator --version 1.0.0 --kind workflow
target/debug/ledgence worker connect --server http://127.0.0.1:8087 \
  --tenant acme --namespace demo --queue fulfillment \
  --store "$FULFILLMENT_RUN/store" --cache "$FULFILLMENT_RUN/cache" \
  --python "$LEDGENCE_PYTHON" --runner "$PWD/sdk/python/ledgence/worker/bootstrap.py" \
  --concurrency 1
```

One slot is sufficient: joins and waits release the parent invocation slot so
its children can run. Use more slots, such as `--concurrency 4`, for possible
branch overlap. A recorded fork confirms child registration; it does not prove
that every child started at the same time.

In a third terminal, submit the missing-data scenario once. Keep its returned
workflow ID:

```sh
"$FULFILLMENT_HOME/client/bin/python" examples/fulfillment-investigator/client.py \
  --server http://127.0.0.1:8087 submit \
  --store "$FULFILLMENT_RUN/data" --scenario data-gap --mode fixture \
  --idempotency-key fulfillment:data-gap:1
```

Follow its execution in Console, or inspect it with the companion client:

```sh
"$FULFILLMENT_HOME/client/bin/python" examples/fulfillment-investigator/client.py \
  --server http://127.0.0.1:8087 status --workflow WORKFLOW_ID
```

The initial warehouse batch is missing **12 records**. The data-quality gate
stops the investigation from treating that absence as a business explanation.
The parent waits for the missing source without holding an invocation slot.
Send the fixture's source-ready signal:

```sh
"$FULFILLMENT_HOME/client/bin/python" examples/fulfillment-investigator/client.py \
  --server http://127.0.0.1:8087 source-ready --workflow WORKFLOW_ID \
  --event-id fulfillment:data-gap:source-ready:1
```

The workflow reingests the corrected **warehouse source only**; it retains the
other acknowledged source results. After the quality gate passes, the workflow
builds an immutable analytical snapshot and forks carrier, warehouse,
source-health, and support analysis. The agent can consume only bounded,
approved evidence tools. Its report must pass deterministic verification before
it can reach the approval step.

When the workflow waits for approval, inspect its exact action and report:

```sh
"$FULFILLMENT_HOME/client/bin/python" examples/fulfillment-investigator/client.py \
  --server http://127.0.0.1:8087 review --workflow WORKFLOW_ID \
  --output "$FULFILLMENT_HOME/review-1"
```

Open `$FULFILLMENT_HOME/review-1/review.html` and inspect `review.json`. Check the
snapshot identity, evidence references, report digest, synthetic/mode labels, and
publication arguments. Copy the full `candidate_sha256` printed by the command
into the decision below. Approve only the artifact you inspected:

```sh
"$FULFILLMENT_HOME/client/bin/python" examples/fulfillment-investigator/client.py \
  --server http://127.0.0.1:8087 decide --workflow WORKFLOW_ID \
  --decision approve --decision-id fulfillment:data-gap:decision:1 \
  --candidate-sha256 CANDIDATE_SHA256 \
  --reviewer local-demo-reviewer --file "$FULFILLMENT_HOME/decision-1.json"
"$FULFILLMENT_HOME/client/bin/python" examples/fulfillment-investigator/client.py \
  --server http://127.0.0.1:8087 result --workflow WORKFLOW_ID
```

The result returns the publication artifact references and paths; open the
returned HTML file to inspect the final published report. The earlier review
remains a pending snapshot. The decision file preserves the frozen command for
explicit retry after an uncertain response. Publication is a local, idempotent
artifact operation. A reviewer name is attribution, not authentication; access control for a remote
installation remains a deployment responsibility.

## How-to: compare outcomes and recover

### Run the real-delay scenario

Submit another workflow with `--scenario real-delay` and a **new** idempotency
key. Its source data is complete, so it can proceed directly to analysis. It
contains a deliberate carrier-delay signal. The contrast matters: the same
quality gate should distinguish incomplete evidence from a supported
operational finding. These are constructed fixtures, not measured outcomes
from a real shipping service.

```sh
"$FULFILLMENT_HOME/client/bin/python" examples/fulfillment-investigator/client.py \
  --server http://127.0.0.1:8087 submit \
  --store "$FULFILLMENT_RUN/data" --scenario real-delay --mode fixture \
  --idempotency-key fulfillment:real-delay:1
```

Expected fixture results:

| Scenario | Initial quality result | Verified delivery result |
| --- | --- | --- |
| `data-gap` | 12 dispatches missing; wait for the warehouse correction. | 4 of 40 current orders late, matching 4 of 40 baseline orders (10%). |
| `real-delay` | All sources complete; proceed to investigation. | 16 of 40 current orders late (40%), compared with 4 of 40 baseline orders (10%). |

The carrier fixture also includes 8 duplicate records and 80 older shipment
versions; reconciliation must prevent both double counting and stale overwrites.

Review and approve or reject this new workflow separately. A rejection must
finish without publishing the report. Use `--decision reject` to demonstrate
that path.

### Use a real Codex agent

Start with a verified fixture run. Install and authenticate a host Codex CLI
separately; the CLI and credentials are not part of the Ledgence package.
Export `LEDGENCE_CODEX_BIN` as its absolute executable path **before starting
the worker**. Confirm the existing sign-in in that same environment:

```sh
export LEDGENCE_CODEX_BIN=/absolute/path/to/codex
"$LEDGENCE_CODEX_BIN" --version
"$LEDGENCE_CODEX_BIN" login status
```

Submit a new workflow using `--mode codex` and an explicit model:

```sh
"$FULFILLMENT_HOME/client/bin/python" examples/fulfillment-investigator/client.py \
  --server http://127.0.0.1:8087 submit \
  --store "$FULFILLMENT_RUN/data" --scenario real-delay --mode codex \
  --model gpt-6-luna --idempotency-key fulfillment:real-delay:codex:1
```

The adapter requests structured JSON from `codex exec` and limits the agent to
six model turns and the four `read_evidence` topics. Built-in tools are disabled;
Ledgence invokes the selected evidence tool and records its result. See
[`fulfillment/agent.py`](fulfillment/agent.py) and
[`fulfillment/codex.py`](fulfillment/codex.py) for the exact bounds and CLI
requirements, and the official [non-interactive guide](https://learn.chatgpt.com/docs/non-interactive-mode).

This sends the supplied synthetic evidence to the configured provider and uses
that account's allowance. A real model result may fail verification; preserve
that result rather than silently replacing it with a fixture. The fixture mode
continues to work without any provider account.

### Retry the same intent

Retain the workflow ID, original input, submission key, event ID, and decision
file. Repeating an identical submission reconciles the same intent; a different
scenario or model selection requires a new key. Retry an uncertain decision
with its saved command and original identity instead of creating another
approval. See [durable workflow approvals](../../docs/workflow-approvals.md).

Acknowledged `ctx.operation` results can be reused after activation recovery.
A provider call may have succeeded before its result was durably accepted; this
example does not promise exactly-once provider calls or remote side effects.
The publication adapter uses artifact identity to make repeated local writes
safe. See [agent recovery](../../docs/agent-recovery.md).

### Check the example

```sh
"$LEDGENCE_PYTHON" examples/fulfillment-investigator/check.py
```

The default check uses deterministic fixtures and a simulated workflow driver.
It needs no provider credentials or PostgreSQL. Its assertions are not a
substitute for a native orchestrator/worker run.

For native acceptance, use the current-source client environment and a
**disposable PostgreSQL server** whose account can create and drop an isolated
test database. Choose a fresh evidence directory:

```sh
export LEDGENCE_POSTGRES_URL='postgres://USER:PASSWORD@127.0.0.1:5432/postgres'
"$FULFILLMENT_HOME/client/bin/python" examples/fulfillment-investigator/check.py \
  --native --directory "$FULFILLMENT_HOME/check-native-1" \
  --binaries target/debug --psql psql
```

Use `--psql /absolute/path/to/psql` if it is not on `PATH`. Native checks use
scripted model output; they do not invoke Codex. Keep their evidence when
reporting which recovery and approval scenarios actually passed. Consult
`check.py --help` for the command interface.

The native gate uses the unchanged prepared application package and one worker
slot. It checks both outcomes, duplicate submission/source-event/decision
reconciliation, restarts during the committed source and approval waits, lost
model and publication journal replies, rejection, and both deadlines. Offline
checks additionally exercise malformed data, failed branches, fabricated claims,
artifact tampering, and a simulated failure after report-file writes but before
the local journal commit. The latter is an application reconciliation test, not
a native process-kill or power-loss qualification. CI runs the offline suite and
the native gate without provider requests.

## Reference

| Component | Responsibility |
| --- | --- |
| [`program.py`](program.py) | Typed entrypoints, source and analysis forks, joins, source-ready wait, agent turns, durable approval, and publication. |
| [`client.py`](client.py) | Submit, observe, signal the missing source, inspect approval, persist a decision, and read a result. |
| [`fulfillment/`](fulfillment/) | Deterministic fixture data, bounded ingestion/analysis, content-addressed storage, model/tool adapters, and report verification. |
| [`prepare.py`](prepare.py) | Create a fresh native program package and separate fixture store; publish the program and record provenance. |
| [`check.py`](check.py) | Offline workflow assertions and explicitly requested native acceptance. |
| [`DEMO.md`](DEMO.md) | Recording outline, evidence to show, and precise claims. |

The submission contract in [`fulfillment/config.py`](fulfillment/config.py)
accepts these application fields:

| Field | Default or requirement |
| --- | --- |
| `store` | Required absolute shared artifact-directory path. |
| `scenario` | `data-gap`; alternatively `real-delay`. |
| `mode` | `fixture`; alternatively `codex`. |
| `model` | `gpt-6-luna`, used by the Codex adapter. |
| `queue` | `fulfillment`; workers must consume the same queue. |
| `source_timeout_ms` | 3,600,000; range 0–86,400,000. |
| `approval_timeout_ms` | 3,600,000; range 0–86,400,000. |

Timeouts finish without publishing. `warehouse:corrected` is the source-ready
wait key; `publish-report` is the durable approval key. Use the companion client
to construct their bound payloads rather than editing workflow state.

Prepared directory:

```text
run-1/
  packages/workflow/       Python source, native manifest, MIT license
  store/                  immutable Ledgence program store
  data/
    fixtures.json         deterministic synthetic input references
    artifacts/            input batches and analytical artifacts by digest
  instance.json           one self-hosted binding: acme/demo
  prepared.json           package descriptor, file digests, fixture provenance
```

Use the paths and artifact references returned by the workflow to locate its
report. Preserve the entire `data` store while reviewing or rerunning a saved
result. Deleting those files can make otherwise valid workflow references
unreadable. Program version `1.0.0` is immutable: publish changed code with a new
application version rather than replacing an existing descriptor.

## Explanation: why the orchestration matters

```mermaid
flowchart TD
    A[Start: four source batches] --> F{Fork ingestion}
    F -.-> O[Orders]
    F -.-> W[Warehouse]
    F -.-> C[Carrier]
    F -.-> S[Support]
    O --> J[Join source results]
    W --> J
    C --> J
    S --> J
    J --> Q{Evidence complete?}
    Q -- No --> E[Wait for source-ready event]
    E --> R[Reingest warehouse only]
    R --> Q
    Q -- Yes --> D[Build verified snapshot]
    D --> B{Fork four investigations}
    B -.-> B1[Carrier]
    B -.-> B2[Warehouse]
    B -.-> B3[Source health]
    B -.-> B4[Support]
    B1 --> M[Join evidence]
    B2 --> M
    B3 --> M
    B4 --> M
    M --> T[Bounded agent and read-only tools]
    T --> V{Verify report claims}
    V -- Invalid --> X[Stop with verification evidence]
    V -- Valid --> H[Wait for report-bound approval]
    H --> P[Publish approved local report]
```

The lake/database layer computes facts; the workflow coordinates when those
facts can be used. The agent explains evidence and can request bounded tools.
It does not decide whether the source is complete or whether an unsupported
number is acceptable. The model selects a structured conclusion and evidence
IDs; the renderer takes factual numbers from a verified claim catalog. It does
not accept unrestricted prose as a verified report. Publication rechecks the
evidence against the pinned snapshot. These checks provide a concrete gate,
not a promise that arbitrary natural-language reasoning is correct.

Immutable input references and a snapshot identity keep the four analyses on
one consistent dataset. Forks and named entrypoints expose causality in Console;
waits and approvals preserve the investigation between activations. None of
this turns Ledgence into a storage engine, CDC connector, stream processor, or
remote service authentication layer.

The failure model reflects ordinary integration problems: webhook providers
document duplicate and unordered delivery, and object notifications can be
redelivered. The example fixtures deliberately reproduce selected conditions;
they are not captured provider incidents. See [Shopify webhook behavior](https://shopify.dev/docs/apps/build/webhooks)
and [S3 event ordering](https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html)
for the underlying problem. All example code is covered by Ledgence's
[MIT license](../../LICENSE); no third-party datasets are bundled.
