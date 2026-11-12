---
title: Command-line interface
description: Published CLI commands, Python program scaffolding, local lifecycle, build features, and installed resources.
---

Ledgence builds one public executable, `ledgence`, from the `ledgence-cli`
crate. It packages programs, runs workers and the orchestrator, administers
tasks and approvals, connects MCP clients, and manages a local container stack.
Worker and orchestrator processes run separately and can run on different hosts.

**Version availability:** Ledgence **0.9.0** includes explicit program builds,
HTTP publication and recovery alongside the existing local lifecycle and
installed-resource discovery. Start with the
[one-line installation](/how-to/install-native#one-line-installation) and
[local distribution guide](/how-to/run-local-distribution).

Ledgence 0.9.0 includes `program new` for functional task and workflow projects.
See [create a Python program](#create-a-python-program).

This replaces the command layout in the historical `v0.1.1` source tag and
native `0.1.0` bundle. Those artifacts retain their original executables and
commands; use their bundled documentation when operating them.

<span id="build-and-inspect"></span>

## Inspect the installed CLI

After following your chosen [installation route](/getting-started/installation),
run these commands in any terminal:

```sh
ledgence --help
ledgence --version
ledgence program --help
ledgence worker --help
ledgence orchestrator --help
ledgence task --help
ledgence approval --help
ledgence mcp --help
ledgence local --help
```

`ledgence --version` reports the platform version. A program's `--version`
identifies your application's package and has a separate lifecycle.

### Source build features

For compilation instructions, use [Build the CLI from source](/how-to/build-cli-from-source).
Within a source checkout, `cargo run --locked -p ledgence-cli -- COMMAND ...`
runs a command without installing it. Cargo commands in this subsection are for
people changing the CLI; an installed release needs no Rust build.

The default features are `otel` and `mcp`. `otel` enables optional telemetry for
administration, worker and orchestrator commands. The MCP command does not
install a telemetry exporter.
`cargo build -p ledgence-cli --no-default-features --locked` omits optional integrations.
Use `--features sqs` to include optional SQS delivery for both worker and
orchestrator commands, or `--all-features` to include every supported integration.
These are build features; telemetry export and SQS operation still require
explicit runtime configuration. See [observability](https://github.com/Ledgence/ledgence/blob/develop/docs/observability.md) and
[dispatch delivery](https://github.com/Ledgence/ledgence/blob/v0.9.0/docs/dispatch-delivery.md).

## Command groups

Use `ledgence <group> <command> --help` for the flags accepted by your executable.
MCP requires its build feature, which is included in native release bundles.

| Group | Commands | Purpose |
| --- | --- | --- |
| `local` | `up`, `status`, `logs`, `down` | Manage a saved installation from a pinned Compose distribution. |
| `program` | `new`, `build`, `publish`, `register` | Create source projects, prepare, publish and register programs; `build` requires local Docker and an explicit worker target. |
| `worker` | `run`, `connect` | Execute local fixtures or acquire durable work from an API. |
| `orchestrator` | `migrate`, `serve`, `retain` | Apply schema changes, run the API, and preview or apply scoped retention. |
| `task` | `submit`, `list`, `inspect`, `status`, `result`, `attempt`, `history`, `cancel` | Submit and inspect task executions. |
| `approval` | `inbox`, `list`, `inspect`, `decide` | Review and decide persisted workflow actions. |
| `mcp` | `serve` | Expose a scoped Ledgence API through MCP over stdio. |

The published 0.9.0 CLI has no separate `workflow` command group. Use the
[HTTP API](https://github.com/Ledgence/ledgence/blob/develop/docs/http-orchestration.md), [Python client](/reference/python-client),
Console or MCP for workflow and event operations.

## Execution progress in development source

Matching development builds add `ledgence task progress` and
`ledgence workflow progress` for bounded lifecycle pages. Add `--follow` to follow
new events and `--cursor` to resume retained history. These commands require a
matching development server and are not in the published 0.9.0 CLI.

```sh
ledgence task progress --server http://127.0.0.1:8080 \
  --tenant acme --namespace demo --task TASK_ID --follow
```

`--wait-ms` accepts 0–20,000, defaulting to 20,000 in follow mode and zero for a
single page. The first read and backlog reads in follow mode are immediate.
`--timeout-ms` bounds observation without cancelling the execution.
[Follow an execution and resume from a cursor](/how-to/follow-execution-progress).

## Create a Python program

**Available in Ledgence 0.9.0.** [Install the CLI](/how-to/install-native), or
[build the CLI from matching source](/how-to/build-cli-from-source). Once the executable is
on your `PATH`:

```sh
ledgence program new invoice-issuer
ledgence program new invoice-flow --kind workflow
```

The default kind is `task`. `--name ID` overrides the destination directory's
basename as the program ID; the initial program version is `0.1.0`. IDs use
1–128 lowercase ASCII letters, digits, dots, underscores or hyphens and must
begin and end with a letter or digit to be valid Python project names. The command creates `program.py`, `tests/test_program.py`, `pyproject.toml`, `ledgence.toml`,
`requirements.txt`, `.gitignore` and `README.md`. The workflow starter uses a typed
entrypoint and completes through its workflow context. Both starters separate
business logic from the handler and include standard-library unit tests.
Both are functional greeting programs: `data={"name": "Ada"}` produces
`{"message": "Hello, Ada!"}` once the program is built, published and executed.

The destination must not exist, including as an empty directory, file or symlink;
its parent must already exist. Scaffolding is offline: it runs no Python, package
manager, Git, Docker, service or network operation. Dependency installation,
tests, build, publication and execution are separate steps. Success prints one
JSON result with the absolute `directory`, `program` identity, `kind`, generated
`files` and `target_configured`. The last field means target values were supplied
and validated, not that a worker or Docker image has been contacted.

`pyproject.toml` declares a uv application project (`package = false`) with
`ledgence-worker` and `ledgence-client` pinned as development dependencies to the
generating CLI's version. The helper supports program authoring; the client
provides `from ledgence.client import AsyncClient` for submitting and inspecting
executions against a running service. `uv sync` installs both into the project
environment. The worker supplies its own helper at execution time; these
development dependencies are not automatically included in the program build.
Generated tests exercise business logic and handler imports; they are not an
offline durable workflow runtime. From the generated directory, with uv and
Python available:

```sh
uv sync
uv run python -m unittest discover -s tests -v
```

By default, `ledgence.toml` contains target placeholders that must be replaced
before building. If the worker target is already known, supply **all three**
`--platform`, `--python` and `--image` options together. For example, after setting
`LEDGENCE_WORKER_IMAGE` to your worker's full digest-pinned image reference:

```sh
ledgence program new invoice-issuer-linux --name invoice-issuer \
  --platform linux/amd64 --python 3.14 --image "$LEDGENCE_WORKER_IMAGE"
```

Platforms are `linux/amd64` or `linux/arm64`; Python is an exact major/minor of
at least `3.11`; the image must end with `@sha256:` and 64 lowercase hexadecimal
digits. The target is never inferred from the developer's computer or a service.
See [Build and publish Python programs](/how-to/build-and-publish-programs) for target compatibility,
production dependencies and deployment.

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
| `local down` | In published 0.9.0, stop the project with a 65-second grace period, preserving data volumes and configuration. The 0.10 candidate honors each service’s saved Compose grace period. |

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
| `ledgence-worker publish` | `ledgence program publish` |
| `ledgence-worker run` | `ledgence worker run` |
| `ledgence-worker connect` | `ledgence worker connect` |
| `ledgence-orchestrator migrate` | `ledgence orchestrator migrate` |
| `ledgence-orchestrator serve` | `ledgence orchestrator serve` |
| `ledgence-orchestrator retain` | `ledgence orchestrator retain` |
| `ledgence task ...` | `ledgence task ...` |
| `ledgence program register ...` | `ledgence program register ...` |

Keep each command's existing options after its new prefix. The 0.9.0 source builds
and native bundles contain only the `ledgence` executable; they do not
provide legacy executable aliases. The `ledgence-worker` and
`ledgence-orchestrator` Rust crates remain internal composition libraries.
Telemetry service names remain `ledgence-worker`, `ledgence-orchestrator`, and
`ledgence-cli` for the corresponding roles.

`program publish` writes immutable package contents to a store.
`program register` makes a separate HTTP request to register an existing
published reference. Publication starts no worker or
execution. Ledgence 0.9.0 supports explicit subsequent registration
with `publish --server URL --register`. See [program packages](https://github.com/Ledgence/ledgence/blob/develop/docs/program-packages.md) and
[Console registration](/how-to/register-agent).

`worker run` executes local task fixtures; `worker connect` acquires work from a
service. `orchestrator migrate` applies database migrations explicitly;
`orchestrator serve` verifies the schema and starts the service. Retention stays
an explicit scoped operator operation through `orchestrator retain`.
Both migration and service operation require `DATABASE_URL`. Retention previews
eligible records by default, requires a tenant and namespace, and changes data
only with `--apply`; its minimum retention is 90 days. See
[retention](https://github.com/Ledgence/ledgence/blob/v0.9.0/docs/retention.md).

## Program builds and HTTP publication

Use matching 0.9.0 CLI and server components. A server must opt in with
`--allow-program-publication`, an `--instance-config` and writable filesystem
`--store`; an existing released stack does not gain this by updating its CLI.

```sh
ledgence program build --config ledgence.toml --output .ledgence/prepared
ledgence program publish --source .ledgence/prepared \
  --server http://127.0.0.1:8080 --register
ledgence program publish --resume /absolute/path/to/receipt.json
```

`build` accepts `--config` (default `ledgence.toml`), `--output` (default
`.ledgence/prepared`, relative to the config directory), `--context NAME` and
`--timeout-seconds 1..3600` (default 600). The configuration declares selected
inputs, exact target, hash-pinned production wheels and the worker runtime image
by digest. It uses local Docker and refuses to overwrite an existing output.

`publish` requires exactly one destination: `--store DIR` or `--server URL`.
`--source` defaults to `.ledgence/prepared`. It never builds implicitly. For
HTTP, `--register` uses matching build-receipt metadata unless `--kind`,
`--display-name` or `--description` overrides it. `--update-metadata true`
explicitly replaces existing descriptive values. `--store` retains descriptor-only
JSON and rejects registration options.

`--resume RECEIPT.json` cannot be combined with other options. It verifies the
retained ZIP and repeats only unconfirmed stages at the recorded server. A
registration failure after a confirmed upload returns nonzero and partial JSON,
including the published descriptor and a registration-only command. Separate
registration can require `--expected-digest sha256:HEX --expected-size BYTES`
together. See [Build and publish Python programs](/how-to/build-and-publish-programs)
for configuration, complete output and recovery semantics.

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
cancellation. See the [HTTP quickstart](https://github.com/Ledgence/ledgence/blob/develop/docs/http-orchestration.md#run-a-task) for
complete examples and the [task result contract](https://github.com/Ledgence/ledgence/blob/develop/docs/task-results.md) for outcome
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

## Approval inbox

Use `ledgence approval inbox --server URL --tenant TENANT --namespace NAMESPACE` to find pending
requests. Pass `--status all` for all retained statuses, `--program ID` for an exact workflow
program, `--due-before 2026-11-06T17:00:00Z` for earlier deadlines, and `--cursor` to continue from a response's `next_cursor`. [Walkthrough](/how-to/require-approval#find-pending-requests-across-workflows).

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
[durable approvals](https://github.com/Ledgence/ledgence/blob/v0.9.0/docs/workflow-approvals.md#cli-and-http)
for the complete contract and examples.

## MCP

Ledgence 0.9.0 builds enable the optional `mcp` feature by default.
`ledgence mcp serve --server URL --tenant ID --namespace NAME` connects an MCP
client over stdio to that API. Add `--read-only` for observation only.
Scope is fixed for the session. Submissions require caller-supplied idempotency
keys and return immediately; inspect results separately. Stdout carries MCP
messages. The process starts no database, worker or model provider.
See [Connect an MCP client](/how-to/connect-mcp).

`--no-default-features --features mcp` builds MCP without OpenTelemetry or SQS.
Earlier 0.2.0 binaries do not include this command.

<span id="instance-access-in-development"></span>

## Instance access

Use scoped credentials with matching 0.9.0 components. The
[instance access guide](/how-to/configure-instance-access) covers provisioning,
Console identity, client files, capabilities, rotation and operating limits.


<span id="operator-audit-in-development"></span>

## Operator audit

Use `ledgence audit list` for bounded scoped pages or `ledgence audit approval` for one exact approval occurrence. Both use the configured credential file.
[Setup, queries and operating limits](/how-to/audit-operators).

<span id="retained-effect-evidence-in-development"></span>

## Retained effect evidence

The 0.9.0 CLI and server provide the `effect` group. These commands read retained
records without invoking a program or authorizing another external action.

```sh
ledgence effect inbox --server http://127.0.0.1:8080 \
  --tenant acme --namespace billing --filter unconfirmed \
  --credentials-file operator.json
ledgence effect inspect --server http://127.0.0.1:8080 \
  --tenant acme --namespace billing --workflow WORKFLOW_ID \
  --activation EFFECT_ACTIVATION_ID --key APPROVAL_KEY \
  --credentials-file operator.json
```

Use the workflow, original effect activation and approval key returned by the
inbox. `operator.json` is the destination-bound credential file from instance
access provisioning; it is not an evidence output file.

`inbox` accepts `--filter all|unconfirmed|confirmed|discrepant|disputed` (default `all`),
optional `--workflow`, `--limit 1..100` (default 50), and `--cursor`. It returns
metadata only and requires `metadata_read` in credential mode. Trusted mode can
also read the metadata. `inspect` requires the three occurrence identifiers,
accepts `--limit 1..4` (default 4) and `--cursor`, and requires credential mode,
`sensitive_read` and durable audit before the frozen binding or evidence is
returned. A permission or audit failure is an error, not an empty page.

Responses are at most 1 MiB. Continue with `next_cursor` and the same query even
when the page is shorter than `--limit`; entries are never truncated. One JSON
response goes to stdout, diagnostics to stderr; there are no implicit retries.
The evidence distinguishes authenticated worker reports from unattributed legacy
records. Neither proves provider truth or permits redispatch. Native effect
resolution is not part of these commands. [Effect evidence contract](https://github.com/Ledgence/ledgence/blob/develop/docs/effect-evidence.md).


<span id="submit-an-operator-effect-assessment-in-development"></span>

## Submit an operator effect assessment

Export a reviewed command from Console or `command.to_json()` in the Python
client, then submit its complete saved envelope:

```sh
ledgence effect resolve --server http://127.0.0.1:8080 \
  --tenant acme --namespace billing --credentials-file resolver.json \
  --file effect-assessment.json
```

This requires matching 0.9.0 CLI/server components and `effect_resolve`. The CLI
checks the original canonical endpoint and scope, then
sends the exact frozen body once. The envelope is at most 1 MiB, body 256 KiB.
The file contains sensitive reviewed action/result data but no credential. There
is no CLI preparation command. Use the same saved file and verified actor to
reconcile uncertain delivery; rotated credentials are allowed, changed commands
or actors conflict. A metadata receipt proves audited acceptance, not workflow
resumption or provider truth. It never redispatches an effect.

`effect inbox --filter disputed` selects current assessment conflict, separately
from historical `discrepant` reports. See the [full saved-command contract](https://github.com/Ledgence/ledgence/blob/develop/docs/cli.md#submit-a-saved-operator-assessment).

<span id="provider-recovery-in-development"></span>

## Provider recovery

Use `ledgence effect inbox --provider-filter needs_review` with the same server,
scope and credential options as other Effects commands. Accepted provider
filters are `all`, `pending`, `needs_review`, `succeeded`, `failed`, `cancelled`
and `disputed`; keep the ordinary `--filter all`. `effect inspect` includes
provider binding, typed reports and finite reconciliation metadata. These
options require matching 0.9.0 components.

`ledgence orchestrator serve --provider-config FILE` requires access and instance
configurations. The versioned file binds the exact installation audience and
chooses `enabled` or `declared_restore_quarantine`; omitting it disables provider
operations. It contains no provider credentials. See the
[recovery guide](/how-to/recover-provider-operations) and
[complete CLI policy format](https://github.com/Ledgence/ledgence/blob/develop/docs/cli.md#provider-operation-recovery-in-development).

<span id="resource-commands-in-development"></span>

## Resource commands

Ledgence 0.9.0 adds `resource list|inspect|decisions|put`,
`admission inspect|release` and `orchestrator serve --admission-config FILE`.
Mutations require `resource_control` and a destination-bound saved command.
`mcp serve --allow-resource-control` separately opts into policy/release tools.
`ledgence audit list --resource NAME` selects audit events for that resource's
policy; choose only one of `--resource`, `--task` or `--workflow`. Admission
release and inspection events use `--workflow WORKFLOW_ID` for workflow operations
or `--task TASK_ID` for standalone tasks.
The existing credential, scope, limit and cursor options apply.
Use matching 0.9.0 components. [Guide](/how-to/control-resource-capacity).

With 0.9.0 components, `ledgence admission inspect` accepts `--task ID
--generation N` instead of a workflow occurrence. The same `admission release`
command accepts a source-bound saved task release. [Task resource admission](/how-to/control-resource-capacity#apply-the-resource-to-a-single-task).

<span id="worker-poll-plans-development"></span>

## Worker poll plans

`ledgence worker connect --poll-plan FILE` selects among integrated HTTP or SQS Standard queues
with weighted Stride or lottery. Supply it instead of `--queue`; use a matching
0.9.0 build. For SQS, add `--delivery-config FILE` with exactly the plan’s queue names. A multi-queue SQS plan defaults to a one-second receive wait. [Configuration and live reload](/how-to/share-worker-polls).

<span id="queue-concurrency-in-development"></span>

## Queue concurrency

Limit outstanding attempts across worker replicas, independently of each worker’s local poll weights. Inspect occupied and uncertain slots and edit a limit from Console, CLI, HTTP, Python or MCP. [Limit a queue across workers](/how-to/limit-queue-concurrency).

Use `ledgence resource sources --server URL --credentials-file FILE --tenant ID --namespace NAME --name RESOURCE [--state possibly_dispatched|unknown_occupied] [--cursor CURSOR] [--limit 1..100]` to discover charged occurrences. Each page requires `sensitive_read` and a committed response audit. Pass `next_cursor` back through `--cursor` for the same resource and state. [Investigate unknown occupancy](/how-to/control-resource-capacity#find-the-source-of-unknown-occupancy).

In Ledgence 0.9.0, `ledgence resource decisions --server URL --credentials-file FILE --tenant ID --namespace NAME --name RESOURCE [--before SEQUENCE] [--limit 1..100]` reads weighted offers newest first. Use the returned `next` as the next exclusive cursor. This metadata operation requires `metadata_read`.

<span id="inspect-a-shared-run-budget-development"></span>

### Inspect a shared run budget

Read `ledgence budget inspect --server URL --credentials-file FILE --tenant ID --namespace NAME --workflow ID`. Replace `--workflow ID` with `--task ID` for a task. `ledgence budget entries` takes the same scope and subject; pass `--before SEQUENCE` to follow the returned cursor. Both require `metadata_read`.

## Execution files in development

Matching development components add `ledgence artifacts put`, `inspect`, `list`,
`download`, `status`, `reconcile` and `replace`. These are separate from program
package publication and require a configured execution store. `put` accepts a
file and stable key attached to an existing task/workflow; `download` verifies
bytes and never overwrites a local destination. Uncertain uploads have explicit
status, reconciliation and revision-bound replacement commands.

Use `ledgence artifacts --help` in the development CLI and see the
[file-sharing guide](/how-to/share-execution-files) or
[exact CLI contract](https://github.com/Ledgence/ledgence/blob/develop/docs/cli.md#execution-files-development-source).
This surface is not included in published 0.9.0. Source builds can add the optional
`s3-artifacts` feature independently of SQS transport.
