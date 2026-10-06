# Ledgence

Run your agents and data pipelines on infrastructure you control.

Ledgence is an open-source orchestration platform built in Rust, with a Python
client and runtime. Publish your code with its dependencies, run it on reusable
workers, and coordinate tasks through workflows that checkpoint, wait, and resume.
Self-host without a required vendor account.

[Documentation](https://docs.ledgence.com) ·
[Capabilities](https://docs.ledgence.com/reference/capabilities) ·
[Quickstart](https://docs.ledgence.com/tutorials/run-locally) ·
[Examples](examples/README.md) ·
[Console guide](https://docs.ledgence.com/tutorials/use-console) ·
[Releases](https://github.com/Ledgence/ledgence/releases) ·
[Python client](https://pypi.org/project/ledgence-client/)

## Get started

- **Choose an installation:** see [installation and local startup](docs/installation.md)
  for native tools, the Python client, and the new `ledgence local` commands.
  The installer and local lifecycle are available in development builds;
  published **0.3.1** still uses the manual native and source Compose guides below.
- **Choose an example:** browse the [example catalog](examples/README.md) for Python programs,
  workflow patterns, client usage, and optional provider integrations.
- **Explore Console:** follow the [Console tutorial](https://docs.ledgence.com/tutorials/use-console)
  to inspect executions, workflows, registered programs, and worker process slots in your browser.
- **Run the complete stack:** follow the [local tutorial](https://docs.ledgence.com/tutorials/run-locally)
  to start PostgreSQL, the orchestrator, a worker, and example programs with Docker Compose.
- **Try the native worker:** [install the Linux x86_64 or macOS Apple Silicon bundle](https://docs.ledgence.com/how-to/install-native)
  and run a Python program without building Rust.
- **Connect an application:** install the [Python client](sdk/python-client/README.md)
  to submit tasks and workflows to your Ledgence service.
- **Connect an MCP client:** use [`ledgence mcp serve`](docs/mcp.md) to discover programs, submit work, and inspect results through the existing API. Available in 0.3.1.
- **Run a real agent workflow:** try the [Codex support agent](examples/codex-support-agent/README.md)
  with ChatGPT sign-in, documentation tools and durable human review, or the
  [Google ADK and Gemini variant](examples/support-agent/README.md).

**Ledgence 0.3.1** adds action-bound human approvals, durable model/tool call
recovery, and an optional MCP server in the unified CLI. Native bundles target **Linux x86_64/glibc** and
**macOS arm64** and include Console; the Python client and Rust API crates share
version **0.3.1**. See the [release notes](docs/releases/0.3.1.md),
[upgrade guide](docs/upgrading-to-0.3.md), and
[release reference](https://docs.ledgence.com/reference/releases). Before 1.0,
public APIs may evolve; pin versions and review changes before upgrading.

The historical native `v0.1.0` and source/package `v0.1.1` releases retain their
original commands and do not include Console.

### One-line installation for the next release

The next release will include the installer and a matching local distribution.
**The command below requires those published assets; 0.3.1 does not provide
them.** Use the [current native installation guide](https://docs.ledgence.com/how-to/install-native)
or [source Compose tutorial](docs/local-deployment.md) until then.

```sh
curl --proto '=https' --tlsv1.2 -sSfL https://github.com/Ledgence/ledgence/releases/latest/download/install.sh | sh
```

The installer selects the native bundle, verifies its contents and configures
your shell startup files. Open a new Bash/Zsh terminal, or activate it in the
current one:

```sh
. "$HOME/.local/share/ledgence/env"
ledgence --version
```

With Docker running and a released bundle containing its matching local kit,
`ledgence local up` starts the stack and prints the API and Console URLs.
`ledgence local down` stops it while preserving data. See the
[local distribution guide](https://docs.ledgence.com/how-to/run-local-distribution)
for platforms, state and upgrade behavior. The installer needs no Rust, Python,
Node, Docker or administrator access; Docker is required only for the local
container stack, and native Python workers require a matching host interpreter.

## What you can build

- **Run tasks on reusable workers.** Workers fetch and verify immutable program
  packages on demand, cache them locally, and share a bounded pool of Python
  subprocesses across programs. Healthy processes are reused for matching work.
- **Coordinate durable workflows.** Combine concurrent local Python steps,
  distributed tasks, and owned subworkflows using
  [typed entrypoints and durable forks](docs/workflow-entrypoints.md).
  Checkpoint state, release the invocation’s worker slot while waiting for a task,
  timer, or external event, then resume at a registered handler.
- **Recover agent calls and review actions.** Persist model/tool responses with
  [explicit operation bindings](docs/agent-recovery.md), and use
  [durable approvals](docs/workflow-approvals.md) to bind a person's decision to
  the exact saved action. Provider SDKs remain application-owned.
- **Inspect execution and deliver results.** Submit, discover, cancel, and observe
  tasks with the CLI or Python client. PostgreSQL stores task ownership, attempts,
  leases, and results; optional completion callbacks retain their delivery state
  and retry after failures.
- **Operate one self-hosted instance.** Console serves from the Rust orchestrator:
  follow recorded workflow relationships, inspect task attempts and results,
  register immutable agent versions, and explore actual worker process observations.
  There is no tenant management or required frontend hosting service.
- **Choose optional integrations.** Use HTTP worker delivery or SQS Standard /
  ElasticMQ transport. Export OpenTelemetry traces and metrics, and write
  correlated Python logs to stderr. PostgreSQL remains the task authority in
  either delivery mode.

Ledgence currently executes **operator-trusted Python code**. Workers manage
process lifecycles; they do not provide a hostile-code sandbox. Programs must
manage their own idempotent external effects. See the
[execution model](https://docs.ledgence.com/concepts/execution-model) and
[delivery guarantees](docs/delivery-contract.md) before designing recovery behavior.

## Documentation

The [documentation site](https://docs.ledgence.com) separates tutorials, how-to
guides, reference, and concepts. Start with a tutorial, then use the detailed
contracts for [programs](docs/program-packages.md), [HTTP orchestration](docs/http-orchestration.md),
[workflows](docs/workflows.md), [events and timers](docs/workflow-events.md),
[subworkflows](docs/subworkflows.md), [durable approvals](docs/workflow-approvals.md),
[agent call recovery](docs/agent-recovery.md),
and [completion callbacks](docs/completion-notifications.md).

The [capability map](https://docs.ledgence.com/reference/capabilities) provides
an entry point to the complete feature set and its release availability. Use the
guides for [receiving long-running results](https://docs.ledgence.com/how-to/receive-results),
[exporting traces and metrics](https://docs.ledgence.com/how-to/configure-observability),
and [choosing queue delivery](https://docs.ledgence.com/concepts/queue-delivery)
alongside their detailed contracts.

Console documentation covers [operation](https://docs.ledgence.com/reference/console),
[agent registration](https://docs.ledgence.com/how-to/register-agent), and
[the single-instance model](https://docs.ledgence.com/concepts/self-hosted-console).

Operational guides cover [worker delivery](docs/worker-delivery.md),
[installation](docs/installation.md), [queue transport](docs/dispatch-delivery.md), [observability](docs/observability.md),
and [retention maintenance](docs/retention.md). The [docs-site source](docs-site/README.md)
lives beside the implementation so documentation can evolve with the code.

## Run a program from source

For the complete self-hosted stack, use the [local Compose deployment](docs/local-deployment.md).
The example below exercises program publication, local execution, and process reuse.
Ledgence 0.3.1 provides one `ledgence` executable; see the
[CLI command groups and migration guide](docs/cli.md). Published release bundles
retain the command layout documented with their release.
The [bundle packaging guide](docs/releasing.md) describes qualification and artifact
preparation for maintainers.

Requirements: Rust through rustup, CPython 3.11 or newer, and Linux or macOS on x86_64 or aarch64. The repository pins its Rust toolchain. A program declares the exact Python major/minor and OS/architecture it targets; the worker supplies that interpreter. The examples below use `python3.12`.

From the repository root:

```sh
export LEDGENCE_PYTHON="$(command -v python3.12)"
cargo build -p ledgence-cli --locked

demo_dir="$(mktemp -d)"
cargo run --locked -p ledgence-cli -- program example \
  --directory "$demo_dir/example" --python "$LEDGENCE_PYTHON"
cargo run --locked -p ledgence-cli -- program publish \
  --source "$demo_dir/example/program" --store "$demo_dir/store"
cargo run --locked -p ledgence-cli -- worker run \
  --tasks "$demo_dir/example/tasks.json" \
  --store "$demo_dir/store" --cache "$demo_dir/cache" \
  --python "$LEDGENCE_PYTHON" \
  --runner "$PWD/sdk/python/ledgence/worker/bootstrap.py" \
  --concurrency 1
```

The two JSON reports have the same `process_id`; the second sets `reused_process` to `true`. Application output is under `report.outcome.output`. Both success and failure records retain the full event source, tenant, namespace, run, task, attempt, program, and available trace context. Worker and program logs go to stderr. The CLI keeps execution and shutdown responsive when an output reader pauses; result-output failures produce a nonzero exit status; optional log drops are reported without changing execution results. See [output delivery and shutdown](docs/architecture.md) for the bounded delivery policy. `--store` also accepts an HTTPS base URL serving the published directory layout.

Write a synchronous Python handler:

```python
def handle(event):
    invoice = event["data"]
    return {"invoice_id": invoice["invoice_id"], "accepted": True}
```

All public Python imports share the `ledgence` namespace: use
`from ledgence.client import AsyncClient` in callers,
`from ledgence.worker import current_invocation, get_logger` in programs, and
`from ledgence.worker.workflow import Workflow` for registered workflow handlers
(`workflow_context` remains available for existing controllers). The client
SDK and worker helper remain separate components. Existing programs using the
legacy `ledgence_worker` imports must update and republish their packages; see
[the import migration](docs/program-packages.md#python-import-namespace).

The handler receives the complete event. Ledgence validates the envelope and preserves the logical JSON value of `data` within the [documented numeric precision](docs/events.md), including application-defined business identifiers and nested structures. The worker does not install application dependencies during execution.

## Design

There is one concurrency setting: `N` consumers and at most `N` managed process slots across all programs. Starting, warm, running, and retiring processes all count. Healthy processes are reused only for the same artifact digest, tenant, and namespace. A matching warm process is preferred, then an unused slot; an incompatible idle process is retired only when replacement is needed at full capacity.

The workspace separates portable contracts, worker behavior, adapters, and executable composition. Other Rust applications can implement the ports and supply their own adapters. See [architecture](docs/architecture.md), [program packages](docs/program-packages.md), [event contract](docs/events.md), and the [Python helper](sdk/python/README.md).

The initial runtime executes operator-trusted programs with the worker's OS permissions. It provides lifecycle management, not a hostile-code sandbox. Python globals and each process's scratch directory persist between invocations. Programs must finish their background work and manage their own idempotent side effects.

## Quality checks

Set `LEDGENCE_PYTHON` to a supported interpreter before running tests. The same gates are configured in CI for Linux and macOS:

```sh
cargo fmt --all -- --check
python3 tools/check-boundaries.py
python3 tools/check-python-client-dependencies.py
python3 tools/check-python-client.py
python3 tools/check-http-features.py
python3 tools/check-otel-features.py
python3 tools/check-sqs-features.py
python3 tools/check-mcp-features.py
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
cargo test --workspace --doc --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
"$LEDGENCE_PYTHON" -m unittest discover -s sdk/python/tests -v
cargo deny --locked check
```

Install the reviewed dependency checker with `cargo install cargo-deny --version 0.20.2 --locked`. Tests include real Python subprocesses, archive integrity and limits, cache recovery, cancellation, process capacity, and the full publish-to-execution flow. Delivery fault tests exercise lease expiry and uncertain replies. Real PostgreSQL/Python delivery tests run separately through the [database gate](docs/postgres.md#verification); ordinary workspace tests leave those explicitly ignored. The [HTTP acceptance gate](docs/http-orchestration.md#verification) runs separate server, worker, and task-client processes using the `ledgence` executable with a real database and fault proxy. Git integration follows feature branches from `develop`, passing checks before merging back; `main` is reserved for stable releases.

## License

Ledgence-owned code is [MIT licensed](LICENSE). Your applications and programs can remain proprietary. Third-party components retain their own licenses and required legal notices; see [dependency policy and release obligations](docs/dependencies.md).

## Durable orchestration storage

The Rust application service and PostgreSQL 18 adapter implement transactional task submission, attempts, leases, result acceptance, cancellation, inspection, and expiry recovery. See [PostgreSQL setup and guarantees](docs/postgres.md). The HTTP executable schedules recovery and exposes readiness; embedding applications can also supply the service directly to the [delivery driver](docs/worker-delivery.md). The driver depends on the portable `TaskService` interface and does not depend on PostgreSQL or a particular transport.

Operational metrics are available through the optional OTLP adapter; see [metrics configuration and counting semantics](docs/metrics.md).

## Self-hosted Console

[Ledgence Console](docs/console.md) brings **Executions**, **Programs**, and
**Workers** into one self-hosted interface. Follow tasks and workflows in a
filterable execution table with infinite scrolling. Inspect a workflow one level
at a time in Graph, follow its recorded intervals in Trace, and open General for
inputs, outputs and resources. Task Trace shows attempts and lifecycle history.
Child workflows open their own graph;
recorded fork, join and entrypoint relationships remain visible after completion.

The graph supports pan, zoom, automatic layout and full-screen inspection. A
collapsible sidebar and light, dark or system appearance keep the same operator
views usable across screen sizes. Registered programs and worker process slots
connect package identity and observed capacity to the work you are inspecting.

Console ships in the 0.3.1 native bundles and the
[local Compose deployment](docs/local-deployment.md). Publish and register the
local examples, then open `http://127.0.0.1:8080/console/`. The
[guided tutorial](https://docs.ledgence.com/tutorials/use-console) walks through
registration, submission and inspection.

Static assets are served by Rust; Node is only a separate frontend build tool.
One server-owned instance binding replaces browser scope selection. Existing
SDK, CLI, and worker scope fields must match that binding; they do not introduce
multi-tenant administration. [Instance binding](docs/self-hosted-instance.md),
[query contracts](docs/console-query-model.md), and
[worker observations](docs/worker-observations.md) describe the operational boundaries.
