# Ledgence

Open-source task orchestration with portable programs, reusable workers, and traceable execution.

Ledgence is being built in Rust. Its worker can publish a Python program with its application dependencies, fetch and verify it on demand, cache it, and run complete CloudEvents through a bounded pool of reusable subprocesses.

Run `ledgence-orchestrator` for the PostgreSQL-backed HTTP task service, `ledgence-worker connect` for reusable workers, and `ledgence task` to submit and inspect tasks. The [Python client](sdk/python-client/README.md) exposes `from ledgence.client import AsyncClient` for submission, [task discovery](docs/task-discovery.md), bounded waiting, and [task results](docs/task-results.md). The delivery driver acquires assignments, renews leases, executes programs, and reconciles durable results. The local `run` command also supports task fixtures. See the [HTTP quickstart and contract](docs/http-orchestration.md), [worker delivery](docs/worker-delivery.md), and [delivery contract](docs/delivery-contract.md). This is an early development version with bounded HTTP long polling and optional PostgreSQL wake notifications. Optional [SQS Standard and ElasticMQ delivery](docs/dispatch-delivery.md) separates queue transport from durable task ownership; PostgreSQL remains the task authority. Optional [OpenTelemetry traces and correlated Python logs](docs/observability.md) are available. There is no stable public API commitment yet.

[Checkpoint workflows](docs/workflows.md) and [external events/timers](docs/workflow-events.md) combine concurrent durable local Python steps with explicitly distributed tasks. A workflow checkpoints and releases its invocation while waiting, then resumes an explicit continuation from persisted state. The Python client exposes these operations through `client.workflows`.

## Try it

Requirements: Rust through rustup, CPython 3.11 or newer, and Linux or macOS on x86_64 or aarch64. The repository pins its Rust toolchain. A program declares the exact Python major/minor and OS/architecture it targets; the worker supplies that interpreter. The examples below use `python3.12`.

From the repository root:

```sh
export LEDGENCE_PYTHON="$(command -v python3.12)"
cargo build --workspace --locked

demo_dir="$(mktemp -d)"
cargo run --locked -p ledgence-worker -- example \
  --directory "$demo_dir/example" --python "$LEDGENCE_PYTHON"
cargo run --locked -p ledgence-worker -- publish \
  --source "$demo_dir/example/program" --store "$demo_dir/store"
cargo run --locked -p ledgence-worker -- run \
  --tasks "$demo_dir/example/tasks.json" \
  --store "$demo_dir/store" --cache "$demo_dir/cache" \
  --python "$LEDGENCE_PYTHON" \
  --runner "$PWD/sdk/python/ledgence_worker/bootstrap.py" \
  --concurrency 1
```

The two JSON reports have the same `process_id`; the second sets `reused_process` to `true`. Application output is under `report.outcome.output`. Both success and failure records retain the full event source, tenant, namespace, run, task, attempt, program, and available trace context. Worker and program logs go to stderr. The CLI keeps execution and shutdown responsive when an output reader pauses; result-output failures produce a nonzero exit status; optional log drops are reported without changing execution results. See [output delivery and shutdown](docs/architecture.md) for the bounded delivery policy. `--store` also accepts an HTTPS base URL serving the published directory layout.

Write a synchronous Python handler:

```python
def handle(event):
    invoice = event["data"]
    return {"invoice_id": invoice["invoice_id"], "accepted": True}
```

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
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
cargo test --workspace --doc --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
"$LEDGENCE_PYTHON" -m unittest discover -s sdk/python/tests -v
cargo deny --locked check
```

Install the reviewed dependency checker with `cargo install cargo-deny --version 0.20.2 --locked`. Tests include real Python subprocesses, archive integrity and limits, cache recovery, cancellation, process capacity, and the full publish-to-execution flow. Delivery fault tests exercise lease expiry and uncertain replies. Real PostgreSQL/Python delivery tests run separately through the [database gate](docs/postgres.md#verification); ordinary workspace tests leave those explicitly ignored. The [HTTP acceptance gate](docs/http-orchestration.md#verification) runs separate server, worker, and CLI binaries with a real database and fault proxy. Git integration follows feature branches from `develop`, passing checks before merging back; `main` is reserved for stable releases.

## License

Ledgence-owned code is [MIT licensed](LICENSE). Your applications and programs can remain proprietary. Third-party components retain their own licenses and required legal notices; see [dependency policy and release obligations](docs/dependencies.md).

## Durable orchestration storage

The Rust application service and PostgreSQL 18 adapter implement transactional task submission, attempts, leases, result acceptance, cancellation, inspection, and expiry recovery. See [PostgreSQL setup and guarantees](docs/postgres.md). The HTTP executable schedules recovery and exposes readiness; embedding applications can also supply the service directly to the [delivery driver](docs/worker-delivery.md). The driver depends on the portable `TaskService` interface and does not depend on PostgreSQL or a particular transport.
