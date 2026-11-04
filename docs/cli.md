# Command-line interface

Ledgence builds one public executable, `ledgence`, from the `ledgence-cli`
crate. It packages programs, runs workers and the orchestrator, administers
tasks and approvals, connects MCP clients, and manages a local container stack.
Worker and orchestrator processes run separately and can run on different hosts.

**Release availability:** Ledgence **0.8.0** includes program builds, HTTP
publication and recovery receipts alongside the existing `local` commands and
installed-resource defaults. See [program preparation and HTTP publication](#program-preparation-and-publication)
and [installation and local startup](installation.md). Existing saved local
installations retain their kit and image; updating a CLI alone does not upgrade
services or enable uploads.

Ledgence 0.8.0 includes `program new` for functional task and workflow projects.
See [create a Python program](#create-a-python-program).

The unified executable replaces the command layout in the historical `v0.1.1` source tag and
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
explicit runtime configuration. See [observability](observability.md) and
[dispatch delivery](dispatch-delivery.md).

## Command groups

Use `ledgence <group> <command> --help` for the supported flags. These groups
are available in 0.8.0.
MCP requires its build feature.

| Group | Commands | Purpose |
| --- | --- | --- |
| `local` | `up`, `status`, `logs`, `down` | Manage a saved installation from a pinned Compose distribution. |
| `program` | `new`, `build`, `publish`, `register` | Create source projects, prepare, publish and register immutable programs; `build` requires local Docker and an explicit worker target. |
| `worker` | `run`, `connect` | Execute local fixtures or acquire durable work from an API. |
| `orchestrator` | `migrate`, `serve`, `retain` | Apply schema changes, run the API, and preview or apply scoped retention. |
| `task` | `submit`, `list`, `inspect`, `status`, `result`, `attempt`, `history`, `cancel` | Submit and inspect task executions. |
| `approval` | `inbox`, `list`, `inspect`, `decide` | Review and decide persisted workflow actions. |
| `mcp` | `serve` | Expose a scoped Ledgence API through MCP over stdio. |

There is no separate `workflow` command group. Workflow and event operations
are available through the [HTTP API](http-orchestration.md),
[Python client](../sdk/python-client/README.md), Console and MCP. Use the
appropriate interface for the operation you need.

## Create a Python program

**Available in Ledgence 0.8.0.** [Install the CLI](installation.md), or build
`ledgence-cli` from matching source as shown above. Once the executable is on your
`PATH`:

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
See [build and publication](program-publication.md) for target compatibility,
production dependencies and deployment.

## Local stack lifecycle

A complete 0.8.0 installation supplies a versioned
`local/` distribution next to `bin/`; a source build can use `--distribution DIR`
to select a separately qualified kit. Docker Engine or Docker Desktop must
already be running Linux containers, with Docker Compose 2.23.1 or newer.

```sh
ledgence local up
ledgence local status
ledgence local logs --follow --service worker
ledgence local down
```

These commands use a local Docker context. They do not install Docker, build the
runtime image from source, or install the Python client. The pinned image
contains the worker's CPython runtime. Remote Docker endpoints are rejected;
operate a remote engine with the kit's Compose files directly instead.

On first startup, choose any nondefault settings explicitly:

```sh
ledgence local up --directory "$HOME/.local/share/ledgence/local-preview" \
  --distribution /absolute/path/to/qualified-local-kit \
  --port 8086 --concurrency 2 --context desktop-linux
ledgence local status --directory "$HOME/.local/share/ledgence/local-preview"
ledgence local down --directory "$HOME/.local/share/ledgence/local-preview"
```

Use a context name from your own Docker installation; `desktop-linux` above is
an example. `--concurrency` accepts 1–1024 and controls both task consumers and
the process pool. The default is one. `--port` defaults to 8080; API and Console
share that port. `--directory` defaults to `$XDG_DATA_HOME/ledgence/local` when
set, otherwise `$HOME/.local/share/ledgence/local`.

| Command | Behavior |
| --- | --- |
| `local up` | Copy and verify the kit on first use, start the saved project, and wait for Compose readiness. Print the API/Console URLs, scope, image, context and actual worker platform. |
| `local status` | Display all service states and the saved installation settings without starting containers. |
| `local logs` | Show the most recent 100 lines per service. Use `--tail 1..10000`, `--follow`, or `--service postgres\|migrate\|orchestrator\|worker`. |
| `local down` | Stop that project with a 65-second grace period, preserving its database, program store, cache, and configuration. |

The first successful initialization saves a unique Compose project identity,
the kit version and immutable image digest, Docker context/endpoint, port, and
concurrency. Subsequent commands reuse those settings. Updating the native CLI
does not update an existing stack, and supplying different settings or another
distribution does not silently migrate it. Use a separate directory for a
separate installation; use a planned upgrade procedure for retained data.
Keep the state directory at its original absolute path: moving it is rejected.
The directory stores configuration, while database and program data live in
Docker volumes. See [backup and restore constraints](installation.md#keep-local-data-and-configuration)
before preserving or restoring an installation.

A port conflict before initialization creates no installation, so the same
directory can be retried with another `--port`. If Compose fails after
initialization, state remains available for `status`, `logs`, and another `up`
with the same options. Do not delete its state or volumes to reconcile a failed
startup. Following logs does not prevent another terminal from stopping the
stack.

The basic kit starts PostgreSQL, its one-shot migrator, the orchestrator serving
Console, and one worker. Its instance uses tenant `acme`, namespace `demo`, and
queue `demo`. It does not publish application programs or start a callback
receiver. The copied kit's `README.md` documents optional examples and direct
Compose operation using a separate project. Follow the
[example handoff](installation.md#run-the-optional-container-examples) to avoid
port conflicts and keep that project's lifecycle and volumes distinct. [Installation details](installation.md) explain state
locations, platform support and the existing source deployment route.

## Installed resources

Ledgence 0.8.0 locates resources relative to the actual executable, resolving
symlink launchers first. Move the complete native bundle together; copying only
`bin/ledgence` omits its runtime helper, Console assets and legal notices.

- `worker run` and `worker connect` use the installed
  `runtime/ledgence/worker/bootstrap.py` when `--runner` is omitted and that file
  exists. An explicit `--runner` always takes precedence. Source builds without
  that layout still require the flag.
- `orchestrator serve --instance-config FILE` uses the installed `console/`
  assets when available and `--console-dir` is omitted. An explicit
  `--console-dir` takes precedence and still requires instance configuration.
- Without `--instance-config`, `serve` remains headless; discovering installed
  Console files does not enable it. If the bundle lacks Console, the existing
  explicit configuration rules continue to apply.

Published 0.3.1 bundles retain their original explicit `--runner` and
`--console-dir` instructions. The host CPython interpreter remains separately
supplied for native worker execution; these defaults do not install it.

## Command migration

Update executable names and command prefixes in shell scripts, process managers,
and deployment configuration:

| Previous command | Current source command |
| --- | --- |
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

`program publish` writes immutable package contents to a store.
`program register` makes a separate HTTP request to register an existing
published reference. Publication starts no worker or
execution; the CLI can explicitly request subsequent
registration with `publish --server URL --register`. See [program packages](program-packages.md) and
[Console registration](console.md#register-immutable-programs).

`worker run` executes local task fixtures; `worker connect` acquires work from a
service. `orchestrator migrate` applies database migrations explicitly;
`orchestrator serve` verifies the schema and starts the service. Retention stays
an explicit scoped operator operation through `orchestrator retain`.

## Program preparation and publication

Use matching 0.8.0 CLI and server components. Enable the server writer explicitly;
updating the CLI alone does not add upload capability to an existing stack.

```sh
ledgence program build --config ledgence.toml --output .ledgence/prepared
ledgence program publish --source .ledgence/prepared \
  --server http://127.0.0.1:8080 --register
ledgence program publish --resume /absolute/path/to/receipt.json
```

`build` defaults to `ledgence.toml` and `.ledgence/prepared`, with config-relative
paths, an explicit Linux target and digest-pinned worker runtime image. It uses
local Docker, exact hash-pinned production wheels and a 600-second default
budget (`--timeout-seconds 1..3600`). `--context NAME` selects a local Docker
context. It neither executes the handler nor overwrites existing outputs.

`publish` defaults to `.ledgence/prepared` and requires exactly one destination,
`--store DIR` or `--server URL`. It never builds implicitly. `--store` retains
its descriptor-only JSON and rejects `--register` and catalog metadata flags.
For `--server`, optional `--register` uses matching build-receipt metadata;
`--kind`, `--display-name` and `--description` may override it. Replacing existing
metadata requires `--update-metadata true`.

The orchestrator must opt in with `--allow-program-publication`, an
`--instance-config` and a writable filesystem `--store`. A read-only HTTPS
store remains a valid deployment without upload capability.

HTTP publication saves its exact ZIP and a receipt before sending. It makes one
PUT without automatic retries. `--resume RECEIPT.json` is exclusive of other
options and retries only unconfirmed stages against the recorded server. A
confirmed upload followed by failed registration returns nonzero and a partial
JSON report, including a registration-only command. Separate `program register`
accepts `--expected-digest sha256:HEX --expected-size BYTES` together to require
the exact uploaded descriptor. See the [complete configuration, output and
recovery contract](program-publication.md).

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

## Find pending approvals

Run `ledgence approval inbox --server URL --tenant TENANT --namespace NAMESPACE`
to discover pending actions without a workflow ID. Filter by `--program ID` or
`--status` (including `all`). Add `--due-before 2026-11-06T17:00:00Z` for
deadlines strictly earlier than the given instant. Continue with `--cursor` from `next_cursor` and
the same filters. Use the returned workflow ID and key with `approval inspect`.

## Durable approvals

Use [durable workflow approvals](workflow-approvals.md) when a review must bind
to an existing immutable action and its effective arguments. Generic events
remain application input; they cannot approve an action. The unified CLI
provides `ledgence approval list`, `inspect`, and `decide`.

```sh
ledgence approval list --server http://127.0.0.1:8080 \
  --tenant acme --namespace demo --workflow WORKFLOW_ID --limit 10
ledgence approval inspect --server http://127.0.0.1:8080 \
  --tenant acme --namespace demo --workflow WORKFLOW_ID --key APPROVAL_KEY
ledgence approval decide --server http://127.0.0.1:8080 --file decision.json
```

Pass the list response's `next_cursor` as `--after-key` for the next page.
The decision file includes `scope`, `workflow_id`, `key`, `activation_id`,
`revision`, `action`, `decision_id`, `decision`, `reviewer`, and `reason`.
Save the complete decision before sending it. After an uncertain response,
retry the identical file so the server can reconcile the same decision.

## MCP clients

Ledgence builds have included the optional `mcp` feature by default since 0.3.1.
Run `ledgence mcp serve --server URL --tenant ID --namespace NAME` for a stdio
session backed by that HTTP API. Add `--read-only` to expose only observation
tools. No worker, database, or model provider starts in the MCP process.
See [MCP setup, tools and recovery](mcp.md).

`--no-default-features` omits both MCP and OpenTelemetry. Add `--features mcp`
to include only MCP, or `--features otel` for telemetry without MCP. Use a 0.8.0 native bundle or build the matching source for MCP; earlier 0.2.0
binaries retain their original command set.

<span id="credential-access-in-development"></span>

## Credential access

Matching 0.8.0 components support optional installation credentials across
API operations, workers and Console. Configure the authority, client credential
files and capabilities using the [instance access guide](instance-access.md).
The existing operator-trusted mode requires its deployment access boundary.
Native credential mode requires transactional verified approval attribution and
audit before sensitive payload responses. [Operator audit](operator-audit.md).


### Inspect operator audit

Ledgence 0.8.0 adds `ledgence audit list` with an optional `--task` or
`--workflow` resource filter, `--limit` and `--cursor`. `ledgence audit approval`
queries exact acceptance with `--workflow`, `--key`, `--activation`, `--revision`
and `--decision`. Both take `--server`, `--tenant`, `--namespace` and the configured
`--credentials-file`. Commands make one read exchange. [Copyable examples and
interpretation](operator-audit.md#query-audit-events).

<span id="retained-effect-evidence-in-development"></span>

## Retained effect evidence

The 0.8.0 CLI and server provide the `effect` group. These commands read retained
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
records. Neither proves provider truth or permits redispatch. Read commands do not resolve effects. [Effect evidence](effect-evidence.md).


### Submit a saved operator assessment

Matching 0.8.0 builds add `ledgence effect resolve`. Export the reviewed
command from Console or the Python client's `command.to_json()` and save the
complete versioned envelope as `effect-assessment.json`:

```sh
ledgence effect resolve --server http://127.0.0.1:8080 \
  --tenant acme --namespace billing --credentials-file resolver.json \
  --file effect-assessment.json
```

The credential needs `effect_resolve`; this is independent of `approval_decide`
and `sensitive_read`. Newly created `access init` operator credentials include it;
existing credentials remain unchanged. The CLI validates the saved canonical
endpoint and scope before sending its original request bytes. It rejects bare
wire-command JSON, symlinks, nonregular files, duplicate JSON fields and envelopes
over 1 MiB (the command body is at most 256 KiB). There is no CLI preparation command.

One metadata receipt goes to stdout. If a reply is lost, retain the file and
reconcile with the same endpoint, scope and stable actor; a rotated credential
for that actor is accepted. Never change the resolution ID, reviewed revision or
body to retry uncertain delivery. A changed command or actor conflicts. Acceptance
is durably audited, does not prove resumption and never redispatches an effect.
Cancelled or collected executions remain closed. `--filter disputed` finds a
current assessment disputed by later evidence; `discrepant` remains historical.
See the [Python saved-command example](../sdk/python-client/README.md#operator-effect-assessments-development).

<span id="provider-operation-recovery-in-development"></span>

## Provider operation recovery

Ledgence 0.8.0 supports `--provider-filter` on `effect inbox`. Use the Effects
commands to inspect provider recovery:

```sh
ledgence effect inbox --server http://127.0.0.1:8080 \
  --tenant acme --namespace billing --credentials-file operator.json \
  --provider-filter needs_review
```

Values are `all`, `pending`, `needs_review`, `succeeded`, `failed`, `cancelled`
and `disputed`. Keep `--filter all` (the default); provider selection cannot be
combined with the local-journal filter. Omission retains the existing mixed
inbox. Cursors bind the provider selection and all original query fields.
`needs_review` excludes historical conflicts covered by the current clean
assessment; `disputed` keeps them visible. A later conflict against that
assessment makes the occurrence need review again.
`effect inspect` shows the complete frozen provider binding, current typed
report, finite reconciliation state and typed timeline. Pagination, permissions,
durable sensitive-read audit and JSON integer fidelity remain unchanged.
`effect resolve --file` continues to accept the same saved assessment envelope.

Enabling provider dispatch is an installation decision. Add
`--provider-config ./provider.json` to `ledgence orchestrator serve`, together
with `--access-config` and `--instance-config`. The bounded version-1 file is:

```json
{
  "v": 1,
  "audience": {
    "authority_id": "YOUR_AUTHORITY_ID",
    "instance_id": "YOUR_INSTANCE_ID",
    "scope": {"tenant_id": "acme", "namespace": "billing"}
  },
  "dispatch": "enabled"
}
```

Copy the audience exactly from this installation's access configuration. The
file contains policy, not a provider credential. Missing configuration leaves
provider operations disabled; a mismatched or malformed file prevents startup.
After a declared database restore, use `"dispatch": "declared_restore_quarantine"`
before restarting API replicas. Existing operations can be reconciled by reads;
new dispatch grants are denied, including business keys absent from the restored
snapshot. A provider idempotency key does not make redispatch safe.

All API replicas must agree on this policy. Run matching workers on the dedicated
probe queue named in each immutable binding. There is no additional worker flag
and no scheduler that redirects such work to an eligible older worker.
[Provider recovery guide](https://docs.ledgence.com/how-to/recover-provider-operations/).

## Resource administration in development

The development checkout adds `resource list|inspect|put` and
`admission inspect|release`, plus `orchestrator serve --admission-config FILE`.
Use scoped credentials and retain a destination-bound command file for mutations.
`mcp serve --allow-resource-control` explicitly enables resource policy/release
tools; it conflicts with read-only mode. These commands are not in 0.8.0.

`ledgence audit list --resource NAME` reads audit events for a named resource
policy. `--resource`, `--task` and `--workflow` are mutually exclusive; the
existing scope, credential, limit and cursor options apply. Admission release
and inspection events belong to the original workflow, so use `--workflow` for
those records.
See [resource admission](resource-admission.md#http-cli-python-and-mcp) for the
command envelope, authority requirements and uncertain-outcome handling.

For a standalone task use `ledgence admission inspect --server URL --tenant ID
--namespace NAME --task TASK_ID --generation 1 --credentials-file FILE`. The
existing `admission release --file FILE` command distinguishes task and workflow
occurrences by their typed addresses and requires the original source/revision.
The JSON command passed to `ledgence task submit` accepts optional
`input.admission: {"resource":"support-model"}` in matching development builds.
See [standalone task admission](resource-admission.md#standalone-task-admission).

## Connect one worker to multiple queues (development)

Use `--poll-plan FILE` in place of `--queue NAME` for integrated HTTP or SQS delivery.
The regular JSON file contains a revision, strategy and queue weights. Publish
updates by replacing the file atomically at the same path. To consume from SQS, add `--delivery-config FILE` with the same queue names and scope. A worker polling multiple SQS routes defaults to a one-second receive wait. The existing single-queue and integrated modes keep their 20-second defaults. [Plan schema, scope and recovery](worker-delivery.md#worker-local-poll-plans-development).
