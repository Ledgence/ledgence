---
title: Command-line interface
description: Ledgence 0.3.1 CLI command groups, build features, task administration, and migration from the earlier executables.
---

Ledgence 0.3.1 builds one public executable, `ledgence`, from the
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
./target/debug/ledgence approval --help
./target/debug/ledgence mcp --help
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
explicit runtime configuration. See [observability](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/observability.md) and
[dispatch delivery](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/dispatch-delivery.md).

## Command migration

Update executable names and command prefixes in shell scripts, process managers,
and deployment configuration:

| Previous command | Current command |
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

Keep each command's existing options after its new prefix. The 0.3.1 source builds
and native bundles contain only the `ledgence` executable; they do not
provide legacy executable aliases. The `ledgence-worker` and
`ledgence-orchestrator` Rust crates remain internal composition libraries.
Telemetry service names remain `ledgence-worker`, `ledgence-orchestrator`, and
`ledgence-cli` for the corresponding roles.

`program example` creates a local fixture and `program publish` writes immutable
package contents to a store. `program register` makes a separate HTTP request to
register an existing published reference. Publication does not register a
program or start a worker. See [program packages](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/program-packages.md) and
[Console registration](/how-to/register-agent).

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
cancellation. See the [HTTP quickstart](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/http-orchestration.md#run-a-task) for
complete examples and the [task result contract](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/task-results.md) for outcome
semantics.

## Approval decisions

Use `ledgence approval list` and `inspect` to review persisted requests. `ledgence approval decide --server URL --file decision.json` sends a complete saved decision command. Preserve that command when acceptance is uncertain; retries must use the same decision identity and action. See [durable approvals](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/workflow-approvals.md#cli-and-http) for fields and examples.

## MCP

Ledgence 0.3.1 builds enable the optional `mcp` feature by default.
`ledgence mcp serve --server URL --tenant ID --namespace NAME` connects an MCP
client over stdio to that API. Add `--read-only` for observation only.
See [Connect an MCP client](/how-to/connect-mcp).

`--no-default-features --features mcp` builds MCP without OpenTelemetry or SQS.
Earlier 0.2.0 binaries do not include this command.
