# Command-line interface

Ledgence 0.2.0 builds one public executable, `ledgence`, from the
`ledgence-cli` crate. It provides program packaging, worker execution,
orchestrator operation, and task administration under command groups. Worker and
orchestrator processes still run separately and can run on different hosts.

This replaces the command layout in the historical `v0.1.1` source tag and
native `0.1.0` bundle. Those artifacts retain their original executables and
commands; use their bundled documentation when operating them.

## Build and inspect

```sh
cargo build -p ledgence-cli --locked
./target/debug/ledgence --help
./target/debug/ledgence --version
./target/debug/ledgence program --help
./target/debug/ledgence worker --help
./target/debug/ledgence orchestrator --help
./target/debug/ledgence task --help
```

Use `cargo run --locked -p ledgence-cli -- COMMAND ...` to run directly from the
checkout. `ledgence --version` reports the compiled platform version; an exact
program version passed to `ledgence program register --version VALUE` continues
to identify the application package.

The default `otel` feature enables optional telemetry for administration, worker
and orchestrator commands. The MCP command does not install a telemetry exporter.
`cargo build -p ledgence-cli --no-default-features --locked` omits optional integrations.
Use `--features sqs` to include optional SQS delivery for both worker and
orchestrator commands, or `--all-features` to include every supported integration.
These are build features; telemetry export and SQS operation still require
explicit runtime configuration. See [observability](observability.md) and
[dispatch delivery](dispatch-delivery.md).

## Command migration

Update executable names and command prefixes in shell scripts, process managers,
and deployment configuration:

| Previous command | Current source command |
| --- | --- |
| `ledgence-worker example` | `ledgence program example` |
| `ledgence-worker publish` | `ledgence program publish` |
| `ledgence-worker run` | `ledgence worker run` |
| `ledgence-worker connect` | `ledgence worker connect` |
| `ledgence-orchestrator migrate` | `ledgence orchestrator migrate` |
| `ledgence-orchestrator serve` | `ledgence orchestrator serve` |
| `ledgence-orchestrator retain` | `ledgence orchestrator retain` |
| `ledgence task ...` | `ledgence task ...` |
| `ledgence program register ...` | `ledgence program register ...` |

Keep each command's existing options after its new prefix. Current source builds
and newly built bundles contain only the `ledgence` executable; they do not
provide legacy executable aliases. The `ledgence-worker` and
`ledgence-orchestrator` Rust crates remain internal composition libraries.
Telemetry service names remain `ledgence-worker`, `ledgence-orchestrator`, and
`ledgence-cli` for the corresponding roles.

`program example` creates a local fixture and `program publish` writes immutable
package contents to a store. `program register` makes a separate HTTP request to
register an existing published reference. Publication does not register a
program or start a worker. See [program packages](program-packages.md) and
[Console registration](console.md#register-immutable-programs).

`worker run` executes local task fixtures; `worker connect` acquires work from a
service. `orchestrator migrate` applies database migrations explicitly;
`orchestrator serve` verifies the schema and starts the service. Retention stays
an explicit scoped operator operation through `orchestrator retain`.

## Task administration

The `task` commands and `program register` retain their HTTP behavior: one
bounded exchange, one JSON result on stdout, and diagnostics on stderr. Exit
status `0` means the operation was accepted, `2` means an input or usage
rejection, and `1` means a service or transport failure. A successful submission
does not mean the task completed successfully. After an uncertain submission,
retry the same input and idempotency key to reconcile the original request.

Use `task list` to discover work, `task status` for scheduling metadata,
`task result` for the authoritative logical outcome, and `task inspect`,
`task attempt`, and `task history` for diagnostics. `task cancel` requests
cancellation. See the [HTTP quickstart](http-orchestration.md#run-a-task) for
complete examples and the [task result contract](task-results.md) for outcome
semantics.

## Durable approvals

Use [durable workflow approvals](workflow-approvals.md) when a review must bind
to an existing immutable action and its effective arguments. Generic events
remain application input; they cannot approve an action. The unified CLI
provides `ledgence approval list`, `inspect`, and `decide`.

## MCP clients

Current source builds after 0.2.0 include the optional `mcp` feature by default.
Run `ledgence mcp serve --server URL --tenant ID --namespace NAME` for a stdio
session backed by that HTTP API. Add `--read-only` to expose only observation
tools. No worker, database, or model provider starts in the MCP process.
See [MCP setup, tools and recovery](mcp.md).

`--no-default-features` omits both MCP and OpenTelemetry. Add `--features mcp`
to include only MCP, or `--features otel` for telemetry without MCP. Published
0.2.0 binaries retain their original command set; build current source for MCP.
