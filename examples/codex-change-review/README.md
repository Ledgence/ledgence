# From an empty page to a reviewed fix

**The problem:** a document search shows two pages for exactly 100 results,
even though every result fits on the first page. The second page is empty.

**The solution:** Codex proposes a small pagination fix. Ledgence checks the
same candidate in three independent branches, compares the behavior, and waits
for a person's decision before completing the change.

The demo presentation in `review.html` explains this path with an animated
workflow, short contextual code snippets, and details you can open as needed.
Playback is an **illustrative replay of a saved result**, not a live connection
to Ledgence. Offline fixture output is labelled separately. Use Console to
follow actual execution, and see [the recording guide](DEMO.md) for the video
story and shot list.

**Version:** application packages `1.2.0`, introduced in the Ledgence **0.3.1**
source release. Use matching **0.5.0** runtime and Python client components for
this checkout. The original example in the `v0.2.0` tag uses
application `1.0.0`; application `1.1.0` uses the previous fixture. Prepare fresh
`1.2.0` packages for these instructions rather than replacing an existing version.

```mermaid
flowchart TD
    S[Start: Codex proposes a candidate] --> F{Fork three branches}
    F -.-> T[Run fixed regression tests]
    F -.-> R[Independent Codex review]
    F -.-> N[Codex drafts a release note]
    F --> C[Local step: measure before and after]
    T --> J[Join all branch outcomes]
    R --> J
    N --> J
    C --> J
    J --> P[Prepare the review packet]
    P --> W{Checks passed?}
    W -- No --> X[Needs changes]
    W -- Yes --> A[Wait for a human decision]
    A --> D[Resume: approve, reject or expire]
    D --> E[Finalize the evidence]
```

Read [`program.py`](program.py) for the orchestration. Six main entrypoints
(`start`, `check_candidate`, `prepare_review`, `await_decision`, `on_decision`,
`finish`) express its lifecycle. Three branch entrypoints (`run_tests`,
`review_code`, `draft_note`) execute one level below it.

## What the viewer sees, and what actually runs

| Moment | Execution | Evidence |
| --- | --- | --- |
| Propose | A task invokes Codex on `change-review-agents`. | Source, canonical diff, base/candidate digests and Codex metadata. |
| Fork | Three owned workflows use the parent's exact program descriptor. | Tests on `change-review`; independent review and draft note on `change-review-agents`. |
| Keep working | After the fork is acknowledged, the parent runs `compare:0` locally. | Measured page counts for 99, 100 and 101 documents. |
| Join | The parent checkpoints its state and waits for every branch to finish. | The same candidate SHA-256 in every report. |
| Inspect | The `prepare:0` task validates and assembles the review packet. | A task result accessible while the parent waits for approval. |
| Decide | `wait_event` resumes on a candidate-bound approve/reject event or a deadline. | Decision, event identity and candidate digest. |
| Finish | A final task assembles the evidence, and optionally publishes an explicitly configured draft PR. | Approved, rejected, expired or needs-changes report. |

A fork acknowledgment confirms durable registration, not that all branches have
started. The setup uses one control slot and two agent slots. Real overlap
depends on scheduling and workload; no artificial delays are added to production
code. Test-only rendezvous points prove overlap in the offline acceptance gate.
The same graph also completes with one slot, executing branches sequentially.

Both joining and waiting for approval release the parent invocation slot.
Healthy Python processes may remain warm for later work. A `ctx.local()` result
is acknowledged durably, but code that runs before that acknowledgment can run
again after an interruption.

The human decision in this example uses an application-validated external event
bound to the workflow and candidate. It does not create a durable action-approval
request or appear in Console's **Approvals** view. Use this example's `approve`
or `reject` client command. For persisted effective-action requests and the
dedicated decision API, see [Durable action approval](../durable-approval/README.md).

## Requirements

- Ledgence 0.5.0 source, a matching `ledgence` executable and all migrations,
  including workflow forks. The historical `v0.1.1` source and `v0.1.0` native
  artifacts do not provide this example's workflow API.
- CPython **3.13** on a supported macOS or Linux host. Prepare packages on the
  same OS, architecture, and Python major/minor as their workers.
- PostgreSQL **18** and `psql`; the automated check requires a disposable server
  on which its account can create and drop an isolated test database.
- For live runs, a host Codex CLI supporting `exec --json --output-schema`,
  `--ephemeral`, `--ignore-user-config`, and `--ignore-rules`, with ChatGPT sign-in
  and access to the selected model. The default is **`gpt-6-luna`**.
- Git and GitHub CLI are needed only for optional PR publishing.

The application uses the Python standard library. Ledgence supplies its worker
helper, and the host supplies Codex. Credentials are not packaged or included
in workflow data. Real runs send the synthetic pagination module, requirements and
candidate to OpenAI and consume the account's Codex allowance. See official
[non-interactive execution](https://learn.chatgpt.com/docs/non-interactive-mode)
and [authentication](https://learn.chatgpt.com/docs/auth) documentation.

Commands below run from the repository root.

## Try the complete workflow

Build Ledgence and install the reviewed client dependencies in a virtual environment:

```sh
cargo build -p ledgence-cli --locked
export CHANGE_HOME="$HOME/.local/share/ledgence-change-review"
python3.13 -m venv "$CHANGE_HOME/client"
"$CHANGE_HOME/client/bin/python" -m pip install --require-hashes --only-binary=:all: \
  -r sdk/python-client/third_party/runtime-requirements.txt \
  -r sdk/python-client/third_party/build-requirements.txt
"$CHANGE_HOME/client/bin/python" -m pip install --no-deps --no-build-isolation ./sdk/python-client
```

Use an existing disposable PostgreSQL server, or start one:

```sh
docker run --detach --name ledgence-change-review-postgres \
  --publish 127.0.0.1:55435:5432 \
  --env POSTGRES_USER=postgres --env POSTGRES_PASSWORD=demo-local \
  --env POSTGRES_DB=ledgence \
  postgres:18.6@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280
docker exec ledgence-change-review-postgres pg_isready -U postgres -d ledgence
export LEDGENCE_POSTGRES_URL='postgres://postgres:demo-local@127.0.0.1:55435/ledgence'
```

Wait for `pg_isready` to report accepting connections. Run the offline acceptance
gate first; it uses real Ledgence services and deterministic Codex protocol
fixtures, and makes no provider or GitHub requests:

```sh
"$CHANGE_HOME/client/bin/python" examples/codex-change-review/check.py \
  --binaries target/debug --psql psql --evidence "$CHANGE_HOME/check-offline"
```

For a real implementation, independent review and draft note, supply the actual Codex
executable and use a fresh evidence directory:

```sh
export LEDGENCE_CODEX_BIN=/absolute/path/to/codex
"$LEDGENCE_CODEX_BIN" --version
"$LEDGENCE_CODEX_BIN" login status
"$CHANGE_HOME/client/bin/python" examples/codex-change-review/check.py \
  --binaries target/debug --psql psql --evidence "$CHANGE_HOME/check-live" \
  --live-codex --codex-bin "$LEDGENCE_CODEX_BIN"
```

Use `--psql /absolute/path/to/psql` when it is not on `PATH`. The gate makes a scripted approval decision for eligible candidates; this is
acceptance automation, not a person reviewing a change. The gate owns its
temporary database, services and package store and cleans them up; the original
PostgreSQL server stays running. A live run submits one workflow, with one
implementation, one independent review and one release-note CLI invocation on its normal path. It never
automatically submits another workflow to obtain a better answer. CLI-internal
model requests/retries are not exposed as a reliable HTTP-call count.

Inspect the exported files under the evidence directory's `bundle/`:

| File | Contents |
| --- | --- |
| `review.html` | Guided problem/solution presentation with an animated workflow replay, contextual code and expandable evidence. |
| `pagination.py` | Exact candidate source. |
| `change.patch` | Diff against the bundled original. |
| `review.json` | Candidate identity, tests, independent review, release note, comparison, decision and execution metadata. |
| `pull-request.md` | Title and body ready for human review. |

## Run interactively and inspect Console

Prepare fresh immutable program packages. All three use application version
`1.2.0`; changing their code requires a new version when publishing to an
existing store.

```sh
python3.13 examples/codex-change-review/prepare.py --directory "$CHANGE_HOME/prepared-1.2.0"
export DATABASE_URL="$LEDGENCE_POSTGRES_URL"
target/debug/ledgence orchestrator migrate
target/debug/ledgence orchestrator serve --bind 127.0.0.1:8084 \
  --store "$CHANGE_HOME/prepared-1.2.0/store" \
  --instance-config "$CHANGE_HOME/prepared-1.2.0/instance.json"
```

For Console, build it using [its README](../../console/README.md), add
`--console-dir console/dist` to `serve`, and open
[localhost:8084/console](http://127.0.0.1:8084/console/).

Keep the orchestrator running. In another terminal, register the programs:

```sh
target/debug/ledgence program register --server http://127.0.0.1:8084 \
  --program codex-change-review --version 1.2.0 --kind workflow
target/debug/ledgence program register --server http://127.0.0.1:8084 \
  --program codex-change-implement --version 1.2.0 --kind task
target/debug/ledgence program register --server http://127.0.0.1:8084 \
  --program codex-change-finalize --version 1.2.0 --kind task
```

Start the control worker, reusing the same `CHANGE_HOME`:

```sh
target/debug/ledgence worker connect --server http://127.0.0.1:8084 \
  --tenant acme --namespace demo --queue change-review \
  --store "$CHANGE_HOME/prepared-1.2.0/store" --cache "$CHANGE_HOME/cache-control" \
  --python "$(command -v python3.13)" --runner "$PWD/sdk/python/ledgence/worker/bootstrap.py" \
  --concurrency 1
```

In a separate terminal, start the agent worker with **two slots**, so the
review and release-note branches can overlap. Export `LEDGENCE_CODEX_BIN` to
the same authenticated executable there:

```sh
target/debug/ledgence worker connect --server http://127.0.0.1:8084 \
  --tenant acme --namespace demo --queue change-review-agents \
  --store "$CHANGE_HOME/prepared-1.2.0/store" --cache "$CHANGE_HOME/cache-agents" \
  --python "$(command -v python3.13)" --runner "$PWD/sdk/python/ledgence/worker/bootstrap.py" \
  --concurrency 2
```

Submit once and retain the returned workflow ID:

```sh
"$CHANGE_HOME/client/bin/python" examples/codex-change-review/client.py submit \
  --change-id pagination-100 --idempotency-key pagination-100:1
```

Open that workflow in Console. Follow **Graph** as the candidate is created,
the three branches run, and their results join. Open each branch to inspect
its own level. Local execution uses a solid edge; fork relationships use dashed
edges. Graph nodes represent recorded execution, not predicted future steps.

When the workflow waits for approval, select the **`prepare:0`** task in **Graph**
or find it under **General → Recorded work**, then copy its task ID. Export the
packet before making a decision:

```sh
"$CHANGE_HOME/client/bin/python" examples/codex-change-review/client.py review \
  --task PREPARE_TASK_ID --output "$CHANGE_HOME/review-1"
```

Open `$CHANGE_HOME/review-1/review.html` in your browser. Start with the brief
problem and solution, then play or step through the workflow. Selecting a node
pauses playback at its step and reveals the relevant explanation and code; use
**Play** to resume. The evidence section contains the measured
page counts, exact patch, test output, independent findings and draft note.
Playback controls only change this local presentation; they cannot approve or
submit work. The command prints the workflow ID and complete candidate SHA-256.
These must match the candidate you approve:

```sh
"$CHANGE_HOME/client/bin/python" examples/codex-change-review/client.py approve \
  --workflow WORKFLOW_ID --candidate-sha256 CANDIDATE_SHA256 \
  --event-id pagination-100:decision:1
"$CHANGE_HOME/client/bin/python" examples/codex-change-review/client.py result \
  --workflow WORKFLOW_ID --timeout 300 --output "$CHANGE_HOME/result-1"
```

Use `reject` with the same flags instead of `approve` when rejecting. A decision
is a one-shot event for this workflow and candidate. Do not send both decisions.
Use a stable event ID; retry an uncertain delivery with **all arguments
unchanged**. Successful event acceptance is not the final workflow outcome;
observe `result` to confirm it. The default approval deadline is one hour;
`submit --approval-timeout-ms` accepts 0 through 86,400,000 milliseconds.

The final `review.html` records the decision. The earlier review file stays a
snapshot of the pending packet; it does not fetch a newer result. An animation
in either file explains the saved evidence rather than reporting current service
activity. Console's **Send
event** is a generic JSON action, not a dedicated Approve button. The companion
commands provide the correctly bound decision event.

If tests or the measured comparison fail, or the independent review requests
changes, the workflow
completes with `needs_changes` without waiting for approval. Export it with
`result`; there is no automatic repair loop. Infrastructure failures remain
explicit failed executions to investigate.

A client timeout only limits observation; it does not cancel or resubmit work.
Keep the same workflow ID to observe again. Reconcile an uncertain submission
with the exact same arguments and idempotency key. Use a new key for intentional
new work. `status` and `cancel` also accept `--workflow`.

## Optional GitHub draft PR

By default, even an approved run stops at the evidence bundle. To enable a draft
PR, create a **dedicated sample repository** containing the original
[`pagination.py`](change_review/fixtures/pagination.py) at its root, commit and push
it, and record the full base commit. Ledgence's product repository is excluded.

On the control worker host, configure an authorized GitHub CLI login. Create a
publication JSON file outside the checkout:

```json
{
  "repository": "YOUR_ACCOUNT/pagination-example",
  "base_branch": "main",
  "base_commit": "REPLACE_WITH_THE_FULL_40_CHARACTER_COMMIT_SHA"
}
```

Add `--publication /absolute/path/to/publication.json` to `submit` with a fresh
idempotency key. This explicitly configures external writes for that run.
Publication occurs only **after the checks pass and the exact candidate receives
approval**. Rejection, expiry and failed checks never publish.

The final task verifies the pinned base, creates a deterministic branch and
commit, and creates or reconciles a matching **draft** PR. It preserves other
files and never merges. A moved base or conflicting branch/PR fails rather than
overwriting work. Keep the target, candidate and event unchanged when
reconciling an uncertain response.

## Outcomes and limits

| Status | Meaning |
| --- | --- |
| `waiting_for_approval` | Packet snapshot: all three branches finished, tests and comparison pass and the independent review approves. The parent is still waiting. |
| `approved` | An approval event for this exact workflow and candidate was recorded. This does not mean merged or deployed. |
| `rejected` | A rejection event was recorded. No publication. |
| `expired` | The approval deadline passed. No publication. |
| `needs_changes` | Assertions or the measured comparison failed, or the reviewer requested changes. Full evidence remains available. |

A failed provider invocation, invalid response, cancelled branch or infrastructure
error fails the workflow; it is not converted into a negative review or a passing
check. Each branch result is validated before it can reach human approval.

The pagination module uses a fixed page size of 100. `page_count(item_count)`
accepts non-negative integers, rejects booleans and non-integers with `TypeError`,
and rejects negative integers with `ValueError`. The regression is visible at
exact page boundaries: zero documents need zero pages, and 100 documents need
one. Tests are fixed independently of the model.
The comparison and test runner execute candidate code in fresh, bounded
subprocesses with a minimal environment. This is an **operator-trusted local
example**, not an untrusted-code security sandbox. Codex itself produces
structured source/review/note output in fresh, read-only, tool-free sessions;
it does not modify the Ledgence checkout.

Candidate JSON is bounded to 24 KiB, source to 6 KiB, and the assembled packet
below the platform's 64 KiB checkpoint limit. The HTML presentation escapes
model text. Embedded JavaScript handles local playback; the page makes no
network requests and loads no external assets. It includes an explicit replay label and identifies offline fixtures when they generated the
candidate or reports. Full source, patch and JSON evidence remain available
alongside the presentation.

Stable task, fork, branch, local-step and event identities reconcile recovery.
An interrupted provider call may already have consumed usage; accepted results
are reused, but the workflow cannot guarantee an external call happened exactly
once. Optional GitHub effects also require reconciliation. A changed source is
a new candidate and requires a new run with new evidence and a new decision.

## Development checks

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD/sdk/python:$PWD/sdk/python-client/src" \
  "$CHANGE_HOME/client/bin/python" -m unittest discover -s examples/codex-change-review/tests -v
```

Run the offline native gate above after unit tests. It checks production package
bytes, three owned branches, single-slot completion, measured overlap using
fixture gates, failed checks, failed review cleanup, lost acknowledgments,
restarts during join and approval, repeated decision delivery, rejected and
expired decisions, and mismatched candidate/workflow rejection. Any added gate
instrumentation is confined to explicitly labelled test package variants.
The live check remains opt-in and is not run by CI.

The application code is MIT. No dependencies were added to the application;
Codex and GitHub remain optional host integrations with their own account terms.
