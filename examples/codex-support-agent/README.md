# Codex support agent with human review

An agent researches a support ticket using bundled Ledgence documentation and
produces a structured draft. Ledgence persists the result, releases the worker
while approval is pending, and continues from the saved draft after a person
approves or rejects it. No email or other external message is sent.

The agent runs through Codex CLI using **ChatGPT sign-in** and **`gpt-6-luna`**.
It consumes the account's Codex usage allowance. This route does not need an
OpenAI API key; API billing is separate. See the official
[authentication guide](https://learn.chatgpt.com/docs/auth) and
[non-interactive execution guide](https://learn.chatgpt.com/docs/non-interactive-mode).

```mermaid
sequenceDiagram
    participant User
    participant Ledgence
    participant Worker
    participant Codex
    participant Docs as Local documentation tools
    User->>Ledgence: Submit codex-support-workflow + ticket
    Ledgence->>Worker: Activate workflow: start
    Worker->>Ledgence: Register codex-support-agent task; suspend
    Ledgence->>Worker: Execute agent task
    Worker->>Codex: One ephemeral invocation, Luna, output schema
    loop Documentation research
        Codex->>Docs: search_docs / read_doc
        Docs-->>Codex: Bundled text; record actual tool use
    end
    Codex-->>Worker: Structured draft and reported token usage
    Worker->>Ledgence: Validated reply, citations and execution metadata
    Ledgence->>Worker: Activate workflow: review
    Worker->>Ledgence: Persist draft; wait for approval:1
    Note over Worker,Ledgence: Waiting occupies no worker process slot
    User->>Ledgence: Approval event identifies ticket and draft task
    Ledgence->>Worker: Activate workflow: finish
    Worker->>Ledgence: Complete with saved draft and review outcome
```

Codex owns the short agent interaction. Ledgence owns execution identity,
accepted results, child relationships, the approval deadline and durable
continuations. The Codex conversation is ephemeral; the validated draft is the
workflow checkpoint.

The controller registers `Entry.START`, `Entry.REVIEW` and `Entry.FINISH` with
`Workflow`. The default entrypoint stages the draft task; the others validate the
accepted draft and approval. Continuations use enum members, while each durable
checkpoint keeps its existing string ID and explicit JSON state.

The draft uses `ctx.task(...)` because it runs a separately packaged program.
`ctx.fork(...)` is for branches in the same pinned workflow package; this linear
review flow does not need one. The stable `draft` child key reconciles controller
retries, and `wait_event` checkpoints the accepted reply before releasing the
worker slot. Neither the provider session nor a Python coroutine is the durable
checkpoint. See [typed workflow entrypoints](../../docs/workflow-entrypoints.md).

## Requirements

- Repository source, Rust/rustup and PostgreSQL 18.
- CPython **3.13** on **macOS arm64** or **Linux x86_64**.
- A working Codex CLI with `exec --json`, `--output-schema`, `--ephemeral`,
  `--ignore-user-config` and `--ignore-rules`, plus a ChatGPT login with access to
  Luna. The local integration was developed against `0.158.0-alpha.2.1`;
  offline CI uses a protocol fixture, not a hosted account.

Follow the official [Codex setup documentation](https://learn.chatgpt.com/docs/auth)
for sign-in. Supply the executable's absolute path explicitly:

```sh
export LEDGENCE_CODEX_BIN=/absolute/path/to/codex
"$LEDGENCE_CODEX_BIN" --version
"$LEDGENCE_CODEX_BIN" login
"$LEDGENCE_CODEX_BIN" login status
```

The last command must report ChatGPT sign-in. The demo uses Codex's existing
credential storage; it never reads, copies or packages authentication files.
No `.env` file is needed. Account limits and model availability still apply.
Only this optional demo needs Codex; Ledgence itself has no account requirement.

The synthetic ticket and documentation read by the tools are sent to OpenAI.
Use data appropriate for your account. Work from the repository root; keep
build outputs outside the checkout.

## Prepare packages

```sh
cargo build -p ledgence-cli --locked
export DEMO_HOME="$HOME/.local/share/ledgence-codex-support-demo"
python3.13 examples/codex-support-agent/prepare.py --directory "$DEMO_HOME/prepared"
```

This publishes `codex-support-agent@1.0.1` and `codex-support-workflow@1.0.2`.
Both use the Python standard library and Ledgence's supplied runtime helper;
preparation does not download wheels. Codex is a separately installed host
executable. Workers fetch and cache immutable application packages by digest.
`prepared.json` records sizes, digests and source hashes.

Use a fresh output directory for each preparation. A changed program destined
for an existing store needs a new version, including updated controller and
client references; do not overwrite an immutable version.

For the companion client:

```sh
python3.13 -m venv "$DEMO_HOME/client"
"$DEMO_HOME/client/bin/python" -m pip install --require-hashes --only-binary=:all: \
  -r sdk/python-client/third_party/runtime-requirements.txt \
  -r sdk/python-client/third_party/build-requirements.txt
"$DEMO_HOME/client/bin/python" -m pip install --no-deps --no-build-isolation ./sdk/python-client
```

It uses the public `from ledgence.client import AsyncClient` interface.

## Start a local instance

Use a dedicated PostgreSQL database. If the existing support demo container is
already running on port 55432, it can also be used for the automated check below,
which creates its own temporary database. For a separate interactive instance:

```sh
docker run --detach --name ledgence-codex-demo-postgres \
  --publish 127.0.0.1:55433:5432 \
  --env POSTGRES_USER=postgres --env POSTGRES_PASSWORD=demo-local \
  --env POSTGRES_DB=ledgence \
  postgres:18.6@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280
docker exec ledgence-codex-demo-postgres pg_isready -U postgres -d ledgence
export DATABASE_URL='postgres://postgres:demo-local@127.0.0.1:55433/ledgence'
target/debug/ledgence orchestrator migrate
target/debug/ledgence orchestrator serve --bind 127.0.0.1:8083 \
  --store "$DEMO_HOME/prepared/store" \
  --instance-config "$DEMO_HOME/prepared/instance.json"
```

Wait for database readiness before migrating. The server stays in the foreground.
Build [Console](../../console/README.md) and add `--console-dir console/dist`
to inspect executions at [localhost:8083/console](http://127.0.0.1:8083/console/).
Keep the instance configuration on every restart: tenant `acme`, namespace
`demo`, queue `codex-support-demo`.

In another terminal, export the same `DEMO_HOME` and `LEDGENCE_CODEX_BIN`, then:

```sh
target/debug/ledgence program register --server http://127.0.0.1:8083 \
  --program codex-support-agent --version 1.0.1 --kind task
target/debug/ledgence program register --server http://127.0.0.1:8083 \
  --program codex-support-workflow --version 1.0.2 --kind workflow
python3.13 examples/codex-support-agent/run_worker.py \
  --directory "$DEMO_HOME/prepared" --server http://127.0.0.1:8083 \
  --codex-bin "$LEDGENCE_CODEX_BIN"
```

The worker uses **one consumer and one managed process slot**. During the draft,
the trusted Python program starts Codex and its local documentation tool server.
These are child processes within that execution; the slot is occupied until the
agent finishes. This is not a separate distributed worker per tool call.

## Submit and review

```sh
"$DEMO_HOME/client/bin/python" examples/codex-support-agent/client.py --server http://127.0.0.1:8083 \
  submit --ticket examples/codex-support-agent/tickets/observation-timeout.json \
  --idempotency-key codex-demo:SUP-1042:1
```

Save the `workflow_id`. In Console, follow the workflow's child execution to its
result, or read it with the child task ID:

```sh
"$DEMO_HOME/client/bin/python" examples/codex-support-agent/client.py --server http://127.0.0.1:8083 \
  draft --task TASK_ID
```

The draft includes the reply, classification, citations, model and `execution`:
provider, CLI version, ephemeral thread ID, one CLI invocation, actual local tool
call count and reported token usage. Unreported reasoning-token usage is `null`.
Codex does not expose a complete HTTP-attempt count here; no such count is inferred.

Once the workflow waits on `approval:1`, approve the exact draft:

```sh
"$DEMO_HOME/client/bin/python" examples/codex-support-agent/client.py --server http://127.0.0.1:8083 \
  review --workflow WORKFLOW_ID --ticket-id SUP-1042 --draft-task TASK_ID \
  --decision approve --event-id review:SUP-1042:1
"$DEMO_HOME/client/bin/python" examples/codex-support-agent/client.py --server http://127.0.0.1:8083 \
  result --workflow WORKFLOW_ID --timeout 60
```

Replace the uppercase placeholders with actual IDs. Use `reject` for rejection.
The default review deadline is one hour; results are `approved`, `rejected` or
`expired`. Waiting for a result times out locally without cancelling or resubmitting
remote work. Reuse the same workflow ID to observe it again.
After an uncertain submission or review response, repeat the exact command with
the same IDs, arguments and unchanged ticket file. A new submission key creates
new work and can invoke Codex again.

## Limits and verification

- The agent must search, then read each cited document. The local MCP server
  records actual executions and limits tool calls to eight. Citations come from
  fixed metadata, not model-supplied URLs. This checks provenance; a person still
  reviews factual correctness.
- Each child has one execution attempt and one ephemeral Codex invocation,
  bounded to 120 seconds. Codex manages its internal model requests; there is no
  promised HTTP-send or billing cap. A failed child is not automatically resubmitted.
- A failed CLI run exits the Python helper so Ledgence retires its process group,
  including any remaining Codex or documentation-server descendants. That helper
  is not returned to the warm pool after an uncertain CLI shutdown.
- The agent runs with a read-only sandbox, explicit documentation tools and
  isolated per-invocation working files. Authentication stays managed by Codex.
- After an accepted draft, approval and service restarts do not rerun Codex.
  A process failure before result acceptance cannot guarantee that no provider
  work occurred.
- Six public documentation snapshots are recorded in
  [`SOURCE.json`](agent/corpus/SOURCE.json), including typed workflow entrypoints/forks and the Python client's timeout
  and task-handle contracts. Refresh deliberately and version new packages.
  These snapshots come from commit `a491c970f69c37673ab053e4696b2361cba47b6d`.

Offline checks require no Codex account or model requests:

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD/sdk/python" \
  python3.13 -m unittest discover -s examples/codex-support-agent/tests -v
"$DEMO_HOME/client/bin/python" -m unittest discover -s examples/codex-support-agent/tests/client -v
python3.13 examples/codex-support-agent/check_cleanup.py --binaries target/debug
```

The opt-in live check starts real Ledgence services and a temporary database,
generates one draft, verifies another task can use the single slot during review,
restarts services, approves the saved draft and reconciles a duplicate event:

```sh
DATABASE_URL='postgres://postgres:demo-local@127.0.0.1:55433/ledgence' \
  python3.13 examples/codex-support-agent/check.py \
  --directory "$DEMO_HOME/prepared" --binaries target/debug \
  --codex-bin "$LEDGENCE_CODEX_BIN" --psql /absolute/path/to/psql \
  --evidence "$DEMO_HOME/evidence/run-1" --live-codex
```

The database user needs permission to create and drop databases. The checker
removes its own services and database, leaving pre-existing infrastructure alone.
Read `summary.json` and `draft.json` in the evidence directory. On child failure,
`failure-diagnostics.json` preserves outcomes and attempts before cleanup.
Codex raw transcripts and authentication files are not retained. Diagnostic
credential-pattern scans do not claim to inspect Codex's credential storage.
On a CLI failure, worker logs retain a fixed category such as authentication,
model availability, MCP startup, timeout or event-stream validation. The helper
then exits so Ledgence can confirm cleanup of its process group.

The cleanup check uses the real Rust worker with a fake Codex executable that
leaves a child process running when it fails. A second task verifies retirement
of that process group before another agent execution. It makes no model request.

The live flag permits subscription usage. CI runs offline subprocess/MCP,
workflow and Rust-worker cleanup tests on macOS and Linux; it does not invoke a hosted model. Ledgence-owned
demo code is MIT. Codex remains an optional, separately distributed host program;
see the [dependency policy](../../docs/dependencies.md).
