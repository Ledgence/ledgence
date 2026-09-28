# Support agent with human review

A small Google ADK agent answers a support ticket using a bundled snapshot of
Ledgence's documentation. Ledgence runs the agent as a task, persists its draft,
and pauses the workflow until a person approves or rejects that specific draft.
No email or other external message is sent.

```mermaid
sequenceDiagram
    participant User
    participant Ledgence
    participant Worker
    participant Gemini
    User->>Ledgence: Submit support-workflow + ticket
    Ledgence->>Worker: Activate controller: start
    Worker->>Ledgence: Create support-agent task; suspend
    Ledgence->>Worker: Execute support-agent
    loop Bounded agent loop
        Worker->>Gemini: Question, instructions, tool results
        Gemini-->>Worker: search_docs / read_doc / final draft
        Note over Worker: Tools read bundled documentation
    end
    Worker->>Ledgence: Persist validated draft + references
    Ledgence->>Worker: Activate controller: review
    Worker->>Ledgence: Checkpoint draft; wait for approval:1
    Note over Ledgence,Worker: Waiting workflow occupies no running process slot
    User->>Ledgence: Review draft and send approval event
    Ledgence->>Worker: Activate controller: finish
    Worker->>Ledgence: Persist approved or rejected result
```

The demo separates the agent's tool loop from the durable workflow. ADK manages
the short model interaction; Ledgence owns the task, accepted result, child
relationship, approval deadline and continuation. The agent session is transient;
the explicit JSON checkpoint is what survives a restart.

## Requirements

- Current repository source containing `demos/`, Rust/rustup, and PostgreSQL 18
  (the commands below use Docker).
- **CPython 3.13**, with pip and venv, on **macOS arm64** or **Linux x86_64 with
  glibc 2.28 or newer**. The reviewed dependency locks are specific to these
  targets. Packages must run on their matching OS, architecture and interpreter.
- A Gemini Developer API key and access to the selected model. The default is
  `gemini-3.8-flash`; `--model` can select another tool-capable Gemini model.
  Availability, quotas and pricing depend on your provider project. A free tier
  is not a guarantee that every run succeeds or is free on every account.

Gemini receives the ticket and the documentation passed through its tools. Use
the supplied synthetic ticket or other data appropriate for your provider account.
Ledgence itself requires no Google account; only this optional demo does.

Run commands from the repository root. Build outputs and credentials stay outside
the checkout. These commands use port **8082**, leaving a stack on 8080 alone.

## 1. Build the programs

```sh
cargo build --workspace --locked
export DEMO_HOME="$HOME/.local/share/ledgence-support-demo"
mkdir -p "$DEMO_HOME"
python3.13 demos/support-agent/prepare.py --directory "$DEMO_HOME/prepared"
```

Preparation downloads the exact hash-locked wheels, verifies the legal inventory,
installs application dependencies into the agent package, and publishes
`support-agent@1.0.1` and `support-workflow@1.0.1` into a local program store.
Workers fetch and cache these immutable packages; they never install dependencies
while executing a task. The host supplies CPython and Ledgence's worker helper.
`prepared.json` records artifact digests, package sizes and build provenance.

The output directory must be new. A failed preparation can be inspected and
retried with a different directory. When changing an implementation for an
existing store, publish a new program version and update the controller/client
references; never overwrite an immutable version.

Create a separate environment for the public client, using this checkout's
reviewed runtime and build dependencies:

```sh
python3.13 -m venv "$DEMO_HOME/client"
"$DEMO_HOME/client/bin/python" -m pip install --require-hashes --only-binary=:all: \
  -r sdk/python-client/third_party/runtime-requirements.txt \
  -r sdk/python-client/third_party/build-requirements.txt
"$DEMO_HOME/client/bin/python" -m pip install --no-deps --no-build-isolation ./sdk/python-client
```

The companion script uses `from ledgence.client import AsyncClient`.

## 2. Start a dedicated local instance

```sh
docker run --detach --name ledgence-support-demo-postgres \
  --publish 127.0.0.1:55432:5432 \
  --env POSTGRES_USER=postgres --env POSTGRES_PASSWORD=demo-local \
  --env POSTGRES_DB=ledgence \
  postgres:18.6@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280
docker exec ledgence-support-demo-postgres pg_isready -U postgres -d ledgence
export DATABASE_URL='postgres://postgres:demo-local@127.0.0.1:55432/ledgence'
target/debug/ledgence-orchestrator migrate
target/debug/ledgence-orchestrator serve --bind 127.0.0.1:8082 \
  --store "$DEMO_HOME/prepared/store" \
  --instance-config "$DEMO_HOME/prepared/instance.json"
```

Wait for `pg_isready` to report that connections are accepted before migrating.
The orchestrator stays in the foreground. To use the browser UI, first build
Console following [its build guide](../../console/README.md), then add
`--console-dir console/dist` to the serve command. Open
[Console](http://127.0.0.1:8082/console/). The configured instance uses tenant
`acme`, namespace `demo` and queue `support-demo`. Keep its instance file on
every restart.

In another terminal, export the same `DEMO_HOME` and register the packages:

```sh
export DEMO_HOME="$HOME/.local/share/ledgence-support-demo"
target/debug/ledgence program register --server http://127.0.0.1:8082 \
  --program support-agent --version 1.0.1 --kind task
target/debug/ledgence program register --server http://127.0.0.1:8082 \
  --program support-workflow --version 1.0.1 --kind workflow
```

## 3. Supply the credential to the worker

Create a private file outside the repository using an editor. Use
[`.env.example`](.env.example) as the format, replacing the empty value with your
own key. For example, keep it at `$DEMO_HOME/private/gemini.env` in a directory
with mode `700`, and set the file to mode `600`. Do not put the key in commands,
tickets, CloudEvents, published packages, screenshots or Git.

```sh
python3.13 demos/support-agent/run_worker.py \
  --directory "$DEMO_HOME/prepared" --server http://127.0.0.1:8082 \
  --env-file "$DEMO_HOME/private/gemini.env"
```

The launcher parses the file as data and passes the key only in the worker
environment. It uses **one consumer and one process slot**. The worker and its
operator-trusted programs share that environment; this is not per-program secret
isolation. The orchestrator, client and Console do not need the key.
If the key is already exported as `GOOGLE_API_KEY` in the current terminal,
`--env-file` can be omitted. A credential file is not loaded automatically.

## 4. Submit, inspect and approve

```sh
"$DEMO_HOME/client/bin/python" demos/support-agent/client.py --server http://127.0.0.1:8082 \
  submit --ticket demos/support-agent/tickets/observation-timeout.json \
  --idempotency-key support-demo:SUP-1042:1
```

Save the returned `workflow_id`. In Console, open that workflow, follow its
**Children** entry to the agent execution and inspect **Result**. Its structured
output includes the reply, classification, source documents actually read,
model name, logical `model_calls`, executed `tool_calls`, total `http_attempts`
(including retries), `http_retries`, and `retry_wait_ms` (completed intervals
between a retryable failure and its next send; a cancelled partial wait is not
included). These HTTP counters describe client sends, not provider billing or
guaranteed generations.
You can also read it with the child's ID:

```sh
"$DEMO_HOME/client/bin/python" demos/support-agent/client.py --server http://127.0.0.1:8082 \
  draft --task TASK_ID
```

The default review deadline is one hour. Once the workflow shows the pending
`approval:1` wait, send the decision for the **exact ticket and child task**:

```sh
"$DEMO_HOME/client/bin/python" demos/support-agent/client.py --server http://127.0.0.1:8082 \
  review --workflow WORKFLOW_ID --ticket-id SUP-1042 --draft-task TASK_ID \
  --decision approve --event-id review:SUP-1042:1
"$DEMO_HOME/client/bin/python" demos/support-agent/client.py --server http://127.0.0.1:8082 \
  result --workflow WORKFLOW_ID --timeout 60
```

Replace the uppercase ID placeholders with the returned IDs. Use `--decision
reject` for rejection. Results have `status` equal to `approved`, `rejected` or
`expired`, and retain the draft and child task ID. A review referencing another
ticket or draft fails the workflow instead of approving unrelated content.
The event is a CloudEvent; application fields remain under `data`.

`result --timeout 60` only limits observation. It neither cancels nor resubmits
remote work. Use `status --workflow WORKFLOW_ID` to inspect it, or `cancel` to
request cancellation. After an uncertain submission or event reply, repeat the
exact command with the same identity, arguments and unchanged ticket file to
reconcile. A new submission key creates new work and can call Gemini again.

## Behavior and limits

- The agent must search and read local documents before returning a reply. It
  can cite only documents actually read. This validates provenance, not factual
  correctness; human review remains necessary.
- Every task invocation creates a fresh ADK session and event loop. Total HTTP
  sends, including retries, are capped at **six**; logical model calls are also
  capped at six, executed tool calls at eight, and agent execution at **120
  seconds**. Each HTTP request has a 20-second HTTPX timeout per network phase,
  within the overall agent deadline.
- Google GenAI retries HTTP **500, 502, 503 and 504**, HTTPX timeouts and
  `ConnectError`. Each logical request has at most **three attempts including the first**,
  with exponential backoff of 1 then 2 seconds plus 0–1 second of jitter, capped
  at 4 seconds. Those attempts share the six-send budget; they do not rerun prior
  successful model turns or tools. HTTP **400, 401, 403, 404 and 429** are not
  retried automatically.
- A valid `Retry-After` or Google `RetryInfo.retryDelay` sets a minimum wait before
  another request. The agent accounts for time already spent in native backoff.
  If the requested delay exceeds four seconds or cannot fit in the remaining
  deadline, the task stops with an explicit failure. Long quota/capacity waits
  need a future durable retry design; the transient ADK session is not saved for
  resumption. Brief HTTP backoff occupies the agent's current process slot.
- The child has **one execution attempt**. Controller activations can retry
  without repeating an accepted draft; the entire agent is not automatically
  restarted after its HTTP budget or deadline is exhausted.
- This is not an exactly-once guarantee for provider calls or costs. A timeout
  can happen after Gemini accepts a request, so an HTTP retry may duplicate a
  generation. A process can also fail after a provider request but before
  Ledgence accepts the result. The demo fails
  that child instead of automatically submitting another model execution.
- Approval state and its original deadline are durable. The workflow releases
  the worker while waiting. Restarting the worker and orchestrator during that
  wait preserves the accepted draft; resuming does not ask Gemini to recreate it.
- The four corpus documents are snapshots of real repository documentation.
  [`SOURCE.json`](agent/corpus/SOURCE.json) records revision and hashes. Refresh
  them deliberately and publish a new version when changing the corpus.

## Verification

Unit tests use real ADK execution and the GenAI retry path with a substituted HTTP
transport, plus the actual Ledgence workflow helper. They cover transient errors,
send budgets, provider wait hints, cancellation and preserving prior tool work.
They do not call Gemini:

```sh
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH="$DEMO_HOME/prepared/packages/agent:$PWD/sdk/python" \
  python3.13 -m unittest discover -s demos/support-agent/tests -v
python3.13 demos/support-agent/third_party/test_verify.py -v
"$DEMO_HOME/client/bin/python" -m unittest discover -s demos/support-agent/tests/client -v
```

Run the explicit live acceptance check against a PostgreSQL parent database whose
user may create/drop databases. It creates its own database and loopback service
ports, publishes a probe, invokes Gemini, verifies a spare task can execute with
concurrency one while approval is pending, restarts the services, approves the
same draft, and reconciles a duplicate event. The check cleans up only its own
processes and database; it leaves the parent database and container running.

```sh
DATABASE_URL='postgres://postgres:demo-local@127.0.0.1:55432/ledgence' \
  python3.13 demos/support-agent/check.py \
  --directory "$DEMO_HOME/prepared" --binaries target/debug \
  --env-file "$DEMO_HOME/private/gemini.env" --psql /path/to/psql \
  --evidence "$DEMO_HOME/evidence/run-1" --live-gemini
```

The live flag permits actual provider usage; this check is not run in ordinary
CI. CI verifies both target dependency inventories and runs provider-substituted
unit tests. Retained acceptance evidence excludes the key and redacts process
logs. Do not publish your own tickets or generated replies without reviewing them.

On failure, the printed `failed_check` describes the failed check. A configuration
failure can happen before the evidence directory is created. If the workflow
terminates before approval, `failure-diagnostics.json` retains the workflow and
child task outcomes and attempts before the temporary database is removed.
Diagnostics are collected on a best-effort basis; collection failures do not hide
the original error. Known Gemini API and transport failures use fixed diagnostic
messages, including HTTP status categories, without provider response bodies.
For example, HTTP 429 indicates a quota or rate limit; inspect the model quota in
your Google project before starting another live run.

Stop foreground demo services with Ctrl-C. Remove the dedicated database only
when you are finished with its results:

```sh
docker rm --force --volumes ledgence-support-demo-postgres
```

## Dependencies and licenses

The demo pins Google ADK 2.10.0 and its complete selected dependency graph.
[`third_party/NOTICE.md`](third_party/NOTICE.md) documents source provenance,
native components and retained legal material. Runtime packages retain upstream
wheel notices and supplemental license texts. No ADK dependency is added to
Ledgence's Rust core or public Python client.

Ledgence-owned demo code is MIT. Dependencies keep their own terms. In particular,
the unmodified `certifi` source/data component remains MPL-2.0 and is included in
source form; redistribution or modification of that component must satisfy its
MPL obligations. This does not apply MPL to your application or Ledgence-owned
code. See the [dependency policy](../../docs/dependencies.md).
