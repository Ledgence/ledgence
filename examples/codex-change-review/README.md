# From bug report to reviewed change

Codex proposes a fix. Ledgence runs local regression tests alongside an
independent distributed review, joins their results, and prepares the change
for a person to inspect.

The exercise is deliberately small: orders of exactly **$100** should receive
free shipping, but a Python calculator charges **$5**. Amounts use integer cents.
The code, tests, and expected behavior fit on one screen; the workflow is the
interesting part.

```mermaid
flowchart TD
    A[Codex implementation task] --> B[Immutable candidate]
    B --> C[Local regression tests]
    B --> D[Distributed Codex review]
    C --> E[Durable join]
    D --> E
    E --> F[Final task: evidence and PR description]
    F --> G[Ready for review or needs changes]
    F -. Explicit GitHub configuration .-> H[Draft PR]
```

Read [`program.py`](program.py) first. Its five typed entrypoints describe the
whole flow. [`change_review/steps.py`](change_review/steps.py) implements the
operations, and [`change_review/config.py`](change_review/config.py) defines
program identities, queues, and requirements once. All three program packages
are built from this one application source tree.

## What runs where

| Step | Execution | Durable result |
| --- | --- | --- |
| Implement | `codex-change-implement` task on `change-review-agents`. | Candidate source, canonical diff, base/candidate hashes and Codex execution metadata. |
| Test | `ctx.local()` in the parent activation on `change-review`. | Independent regression-test report for that candidate. |
| Review | Same-package owned workflow at `Entry.REVIEW`, on `change-review-agents`. | Fresh Codex session, structured findings and candidate hash. |
| Join | Parent saves the test result and waits for the review. | Explicit JSON state and terminal child outcome. |
| Finalize | `codex-change-finalize` task on `change-review`. | Review bundle, status and optional draft-PR URL. |

`await ctx.fork(...)` registers the remote review before local testing starts.
It acknowledges scheduling, not review completion. The local operation stays
within its existing invocation; its test subprocess is not another Ledgence
consumer or distributed task. After the test result is acknowledged,
`return ctx.join(...)` releases the parent invocation slot until review finishes.

Two available slots permit overlap. The setup below uses two workers, one slot
per worker and one queue per worker. Very short tests can finish before a live
review starts; the example never inserts artificial work to promise overlap.
The offline acceptance gate uses explicit test-only rendezvous points to prove
that both branches can be active concurrently. It also checks the topology with
one slot by routing its test-owned packages through one queue.

## Requirements

- Ledgence 0.2.0 source, a matching `ledgence` executable and all migrations,
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
in workflow data. Real runs send the synthetic calculator, requirements and
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

For a real implementation and independent review, supply the actual Codex
executable and use a fresh evidence directory:

```sh
export LEDGENCE_CODEX_BIN=/absolute/path/to/codex
"$LEDGENCE_CODEX_BIN" --version
"$LEDGENCE_CODEX_BIN" login status
"$CHANGE_HOME/client/bin/python" examples/codex-change-review/check.py \
  --binaries target/debug --psql psql --evidence "$CHANGE_HOME/check-live" \
  --live-codex --codex-bin "$LEDGENCE_CODEX_BIN"
```

Use `--psql /absolute/path/to/psql` when it is not on `PATH`. The gate owns its
temporary database, services and package store and cleans them up; the original
PostgreSQL server stays running. A live run submits one workflow, with one
implementation and one review CLI invocation on its normal path. It never
automatically submits another workflow to obtain a better answer. CLI-internal
model requests/retries are not exposed as a reliable HTTP-call count.

Inspect the exported files under the evidence directory's `bundle/`:

| File | Contents |
| --- | --- |
| `shipping.py` | Exact candidate source. |
| `change.patch` | Diff against the bundled original. |
| `review.json` | Candidate identity, tests, independent review, execution metadata and final status. |
| `pull-request.md` | Title and body ready for human review. |

## Run interactively and inspect Console

Prepare fresh immutable program packages. All three use application version
`1.0.0`; changing their code requires a new version when publishing to an
existing store.

```sh
python3.13 examples/codex-change-review/prepare.py --directory "$CHANGE_HOME/prepared"
export DATABASE_URL="$LEDGENCE_POSTGRES_URL"
target/debug/ledgence orchestrator migrate
target/debug/ledgence orchestrator serve --bind 127.0.0.1:8084 \
  --store "$CHANGE_HOME/prepared/store" \
  --instance-config "$CHANGE_HOME/prepared/instance.json"
```

For Console, build it using [its README](../../console/README.md), add
`--console-dir console/dist` to `serve`, and open
[localhost:8084/console](http://127.0.0.1:8084/console/).

Keep the orchestrator running. In another terminal, register the programs:

```sh
target/debug/ledgence program register --server http://127.0.0.1:8084 \
  --program codex-change-review --version 1.0.0 --kind workflow
target/debug/ledgence program register --server http://127.0.0.1:8084 \
  --program codex-change-implement --version 1.0.0 --kind task
target/debug/ledgence program register --server http://127.0.0.1:8084 \
  --program codex-change-finalize --version 1.0.0 --kind task
```

Start the control worker, reusing the same `CHANGE_HOME`:

```sh
target/debug/ledgence worker connect --server http://127.0.0.1:8084 \
  --tenant acme --namespace demo --queue change-review \
  --store "$CHANGE_HOME/prepared/store" --cache "$CHANGE_HOME/cache-control" \
  --python "$(command -v python3.13)" --runner "$PWD/sdk/python/ledgence/worker/bootstrap.py" \
  --concurrency 1
```

In a separate terminal, start the agent worker. Export `LEDGENCE_CODEX_BIN` to
the same authenticated executable there:

```sh
target/debug/ledgence worker connect --server http://127.0.0.1:8084 \
  --tenant acme --namespace demo --queue change-review-agents \
  --store "$CHANGE_HOME/prepared/store" --cache "$CHANGE_HOME/cache-agents" \
  --python "$(command -v python3.13)" --runner "$PWD/sdk/python/ledgence/worker/bootstrap.py" \
  --concurrency 1
```

Submit once and retain the returned workflow ID:

```sh
"$CHANGE_HOME/client/bin/python" examples/codex-change-review/client.py submit \
  --change-id shipping-100 --idempotency-key shipping-100:1
"$CHANGE_HOME/client/bin/python" examples/codex-change-review/client.py result \
  --workflow WORKFLOW_ID --timeout 300 --output "$CHANGE_HOME/result-1"
```

The result timeout limits observation; it does not cancel or resubmit work. Use
the same workflow ID to observe again. Reconcile an uncertain submission with
the exact same arguments and idempotency key. Use a new key for intentional new
work. `status` and `cancel` also accept `--workflow`.

## Optional GitHub draft PR

Default execution stops at the review bundle. To publish, create a **dedicated
sample repository** with the original
[`shipping.py`](change_review/fixtures/shipping.py) at its root. Commit and push
that original, and record its full base commit SHA. Ledgence's own product
repository is explicitly excluded from this publisher.

On the control worker host, configure `gh auth login` for an account authorized
to create branches and PRs in that repository. Create a
small publication file outside the checkout, replacing these example values:

```json
{
  "repository": "YOUR_ACCOUNT/shipping-example",
  "base_branch": "main",
  "base_commit": "REPLACE_WITH_THE_FULL_40_CHARACTER_COMMIT_SHA"
}
```

Add `--publication /absolute/path/to/publication.json` to `client.py submit`
with a new idempotency key. This explicitly enables GitHub writes for that run.
Only `ready_for_review` candidates are published. The final task verifies the
pinned base file, creates a deterministic commit and branch, and creates a
**draft** PR or reconciles an existing matching PR. It preserves all other repository files and
does not merge. A moved base or conflicting existing branch/PR is an error to
inspect, not permission to overwrite work. Keep the target and candidate
unchanged when reconciling a lost response.

## Outcomes and recovery

- `ready_for_review`: the fixed regression suite passes and the independent
  reviewer approves. This is evidence for human review, not proof of correctness.
- `needs_changes`: assertions failed or the reviewer requested changes. The
  complete reports remain available, and no PR is published.
- Infrastructure failures: failed implementation, review, or finalization remain
  explicit failed tasks/branches. Timeouts and malformed provider responses are
  not converted into passing reports.

The candidate is at most 24 KiB of encoded JSON, and saved local state is bounded
below the platform's 64 KiB checkpoint limit. Source and diff are small enough to
travel together without a separate artifact service. Both branches check the
same source identity and use fresh temporary directories; they do not share a
mutable checkout. The tests are predefined independently of Codex's output.

Stable task, fork, and local-step keys reconcile retries. Accepted local test
results are reused after recovery; waiting retains no parent invocation. A
provider call interrupted before result acceptance can still consume usage, and
GitHub effects require reconciliation. Start a new candidate and rerun both
checks for any subsequent code change. There is one candidate per workflow,
without an automatic repair loop.

## Development checks

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD/sdk/python:$PWD/sdk/python-client/src" \
  "$CHANGE_HOME/client/bin/python" -m unittest discover -s examples/codex-change-review/tests -v
```

Unit and offline integration tests cover the real workflow contract, input and
digest validation, regression failures, review findings, bounded child processes,
replay, and GitHub publication reconciliation using local fixtures. The live
check is explicit and is not run by CI. The application code is MIT; Codex and
GitHub are optional host integrations with their own accounts and terms.
