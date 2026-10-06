---
title: Command-line interface
description: Ledgence 0.4.0 command groups, local lifecycle, build features, and installed resources.
---

Ledgence builds one public executable, `ledgence`, from the `ledgence-cli`
crate. It packages programs, runs workers and the orchestrator, administers
tasks and approvals, connects MCP clients, and manages a local container stack.
Worker and orchestrator processes run separately and can run on different hosts.

**Version availability:** Ledgence **0.4.0** includes every group below, the
local lifecycle commands, and automatic installed-resource discovery. Start with
the [one-line installation](/how-to/install-native#one-line-installation) and
[local distribution guide](/how-to/run-local-distribution).

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
./target/debug/ledgence local --help
```

Use `cargo run --locked -p ledgence-cli -- COMMAND ...` to run directly from the
checkout. `ledgence --version` reports the compiled platform version; an exact
program version passed to `ledgence program register --version VALUE` continues
to identify the application package.

The default features are `otel` and `mcp`. `otel` enables optional telemetry for
administration, worker and orchestrator commands. The MCP command does not
install a telemetry exporter.
`cargo build -p ledgence-cli --no-default-features --locked` omits optional integrations.
Use `--features sqs` to include optional SQS delivery for both worker and
orchestrator commands, or `--all-features` to include every supported integration.
These are build features; telemetry export and SQS operation still require
explicit runtime configuration. See [observability](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/observability.md) and
[dispatch delivery](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/dispatch-delivery.md).

## Command groups

Use `ledgence <group> <command> --help` for the flags accepted by your executable.
MCP requires its build feature, which is included in native release bundles.

| Group | Commands | Purpose |
| --- | --- | --- |
| `local` | `up`, `status`, `logs`, `down` | Manage a saved installation from a pinned Compose distribution. |
| `program` | `example`, `publish`, `register` | Create a fixture, publish a prepared immutable package, and register its reference. |
| `worker` | `run`, `connect` | Execute local fixtures or acquire durable work from an API. |
| `orchestrator` | `migrate`, `serve`, `retain` | Apply schema changes, run the API, and preview or apply scoped retention. |
| `task` | `submit`, `list`, `inspect`, `status`, `result`, `attempt`, `history`, `cancel` | Submit and inspect task executions. |
| `approval` | `list`, `inspect`, `decide` | Review and decide persisted workflow actions. |
| `mcp` | `serve` | Expose a scoped Ledgence API through MCP over stdio. |

There is no separate `workflow` command group. Use the
[HTTP API](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/http-orchestration.md), [Python client](/reference/python-client),
Console or MCP for workflow and event operations.

## Local lifecycle

These commands require Docker running Linux containers and Compose 2.23.1+.
The native installation supplies its matching qualified kit:

```sh
ledgence local up
ledgence local status
ledgence local logs --follow --service worker
ledgence local down
```

A source-built CLI needs `--distribution /absolute/path/to/qualified-local-kit`.
Use `--directory /absolute/path/to/state` for a separate installation and repeat
it for every command on that installation. Default state lives at `$XDG_DATA_HOME/ledgence/local`, or
`$HOME/.local/share/ledgence/local` if unset. The default port is 8080 and
concurrency is 1. Choose a nondefault port, concurrency (1–1024), or named local
Docker `--context` on first startup; saved options do not change implicitly.

| Command | Behavior |
| --- | --- |
| `local up` | Verify and copy the kit on first use, start the saved Compose project, and wait for readiness. Print API/Console URLs, scope, image, context and worker platform. |
| `local status` | Display all service states and saved settings without starting containers. |
| `local logs` | Show 100 recent lines per service by default. Accept `--tail 1..10000`, `--follow`, and `--service postgres\|migrate\|orchestrator\|worker`. |
| `local down` | Stop the project with a 65-second grace period, preserving data volumes and configuration. |

The base stack runs PostgreSQL, a migrator, an orchestrator serving Console,
and one worker in tenant `acme`, namespace `demo`, queue `demo`. Programs and
the callback receiver are optional additions. The CLI rejects remote Docker
endpoints and checks the kit's qualified platforms against the engine.

The saved state remains tied to its original absolute directory and Docker
endpoint. Installing a newer CLI does not upgrade that stack. There is no
`local restore` command; database and program-store backups are separate from
the state directory. See [local distribution operations](/how-to/run-local-distribution)
for startup failures, data preservation and direct Compose use.

## Installed resources

The CLI resolves symlink launchers and locates resources relative
to the actual executable in a complete bundle:

- `worker run` and `worker connect` use the bundled
  `runtime/ledgence/worker/bootstrap.py` when `--runner` is omitted and the file
  exists. An explicit `--runner` wins; source builds without the bundle layout
  still require it.
- `orchestrator serve --instance-config FILE` uses bundled `console/` assets
  when they exist and `--console-dir` is omitted. An explicit `--console-dir`
  wins and still requires instance configuration.
- Without `--instance-config`, the orchestrator remains headless. Finding
  Console files does not enable Console on its own.

Keep the complete bundle, including legal notices, together when moving a
native installation. The host CPython interpreter is supplied separately.
The [native installation guide](/how-to/install-native) also explains explicit
`--runner` and `--console-dir` overrides for operator-managed service configurations.

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

Keep each command's existing options after its new prefix. The 0.4.0 source builds
and native bundles contain only the `ledgence` executable; they do not
provide legacy executable aliases. The `ledgence-worker` and
`ledgence-orchestrator` Rust crates remain internal composition libraries.
Telemetry service names remain `ledgence-worker`, `ledgence-orchestrator`, and
`ledgence-cli` for the corresponding roles.

`program example` creates a local fixture and `program publish` writes immutable
package contents to a store. `program register` makes a separate HTTP request to
register an existing published reference. Publication does not register a
program or start a worker. See [program packages](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/program-packages.md) and
[Console registration](/how-to/register-agent).

`worker run` executes local task fixtures; `worker connect` acquires work from a
service. `orchestrator migrate` applies database migrations explicitly;
`orchestrator serve` verifies the schema and starts the service. Retention stays
an explicit scoped operator operation through `orchestrator retain`.
Both migration and service operation require `DATABASE_URL`. Retention previews
eligible records by default, requires a tenant and namespace, and changes data
only with `--apply`; its minimum retention is 90 days. See
[retention](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/retention.md).

## Task administration

The `task` commands, `approval` commands and `program register` use one
bounded exchange, one JSON result on stdout, and diagnostics on stderr. Exit
status `0` means the operation was accepted, `2` means an input or usage
rejection, and `1` means a service or transport failure. A successful submission
does not mean the task completed successfully. After an uncertain submission,
retry the same input and idempotency key to reconcile the original request.

Use `task list` to discover work, `task status` for scheduling metadata,
`task result` for the authoritative logical outcome, and `task inspect`,
`task attempt`, and `task history` for diagnostics. `task cancel` requests
cancellation. See the [HTTP quickstart](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/http-orchestration.md#run-a-task) for
complete examples and the [task result contract](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/task-results.md) for outcome
semantics.

```sh
ledgence task submit --server http://127.0.0.1:8080 --file task.json
ledgence task list --server http://127.0.0.1:8080 --tenant acme --namespace demo
ledgence task result --server http://127.0.0.1:8080 \
  --tenant acme --namespace demo --task TASK_ID
```

`task.json` contains the complete submission command, including its
`idempotency_key`. `task list` accepts state, queue, correlation and submission
time filters; pass its returned cursor with unchanged filters for another page.
`task result` reads the current result once and does not wait for completion.
`task history --after N` starts after a recorded sequence, and `task attempt`
requires both task and attempt IDs.

## Approval decisions

Use `approval list` and `inspect` to review persisted requests and their exact
effective actions:

```sh
ledgence approval list --server http://127.0.0.1:8080 \
  --tenant acme --namespace demo --workflow WORKFLOW_ID --limit 10
ledgence approval inspect --server http://127.0.0.1:8080 \
  --tenant acme --namespace demo --workflow WORKFLOW_ID --key APPROVAL_KEY
ledgence approval decide --server http://127.0.0.1:8080 --file decision.json
```

Pass the list response's `next_cursor` as `--after-key` for the next page.
The saved decision includes `scope`, `workflow_id`, `key`, `activation_id`,
`revision`, `action`, `decision_id`, `decision`, `reviewer`, and `reason`.
After an uncertain response, retry the identical file to reconcile that
decision. Generic events cannot approve actions. See
[durable approvals](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/workflow-approvals.md#cli-and-http)
for the complete contract and examples.

## MCP

Ledgence 0.4.0 builds enable the optional `mcp` feature by default.
`ledgence mcp serve --server URL --tenant ID --namespace NAME` connects an MCP
client over stdio to that API. Add `--read-only` for observation only.
Scope is fixed for the session. Submissions require caller-supplied idempotency
keys and return immediately; inspect results separately. Stdout carries MCP
messages. The process starts no database, worker or model provider.
See [Connect an MCP client](/how-to/connect-mcp).

`--no-default-features --features mcp` builds MCP without OpenTelemetry or SQS.
Earlier 0.2.0 binaries do not include this command.
