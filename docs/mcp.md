# MCP server

Ledgence 0.3.1 exposes an optional MCP server in the unified `ledgence`
executable. Earlier 0.2.0 binaries do not include it.
The server connects to your existing HTTP API; the orchestrator, database and
workers continue running separately. No AI framework, model or vendor account
is required by this integration.

## Connect

Build the matching release source or use its native bundle, then configure your MCP host to launch:

```sh
cargo build -p ledgence-cli --locked
./target/debug/ledgence mcp serve \
  --server http://127.0.0.1:8080 \
  --tenant acme \
  --namespace billing
```

This command speaks newline-delimited MCP JSON over stdin/stdout, not an
interactive terminal prompt. The host launches one process for its connection.
Only protocol messages go to stdout; operational errors go to stderr. The
server URL can point to a remote deployment over HTTPS.

For hosts using a `mcpServers` configuration, supply the absolute executable path:

```json
{
  "mcpServers": {
    "ledgence": {
      "command": "/absolute/path/to/ledgence",
      "args": [
        "mcp", "serve",
        "--server", "http://127.0.0.1:8080",
        "--tenant", "acme",
        "--namespace", "billing"
      ]
    }
  }
}
```

Host configuration formats vary; the executable and arguments above are the
integration contract. Add `--read-only` to omit mutation tools and reject direct
attempts to call them. Scope and API URL are fixed at startup; tool arguments
cannot select another tenant, namespace or server. Scope is a constraint, not an
authentication mechanism. Published 0.7.0 uses the operator-trusted deployment
boundary. Ledgence 0.9.0 clients can use [instance credentials](instance-access.md)
with a service-specific `--credentials-file`.

Use the binding from the orchestrator's instance file. The local Compose stack
uses `acme/demo`; a new instance that omits `scope` uses `default/default`. Replace
the illustrative `acme/billing` scope above to match that configuration. See
[self-hosted instance binding](self-hosted-instance.md).

The `mcp` Cargo feature is enabled by default in `ledgence-cli`. Build with
`--no-default-features --features mcp` for MCP without OpenTelemetry or SQS.
`--no-default-features` omits MCP; its command then reports that the feature is
unavailable. Core crates never depend on the MCP SDK.

## Tools

The tool list is static and does not depend on which programs are registered.
Each tool includes an input schema and read-only/idempotency/destructive hints;
those hints do not replace application authorization.

| Tools | Operation |
| --- | --- |
| `ledgence_program_list` | One page of registered programs, optionally filtered by kind. |
| `ledgence_program_versions`, `ledgence_program_inspect` | Published catalog versions and exact registered package metadata. |
| `ledgence_task_submit`, `ledgence_workflow_submit` | Submit an existing package version and return execution identities immediately. |
| `ledgence_task_list` | One filtered page of task executions. |
| `ledgence_task_status`, `ledgence_workflow_status` | Current durable state, without waiting for completion. |
| `ledgence_task_result`, `ledgence_workflow_result` | Current result; `outcome: null` means no terminal result is available yet. |
| `ledgence_task_cancel`, `ledgence_workflow_cancel` | Explicitly request execution cancellation. |
| `ledgence_workflow_send_event` | Deliver an original CloudEvent to an application wait key. |
| `ledgence_approval_inbox` | Discover approval summaries within the server's configured scope. Defaults to pending. Optional `due_before` selects deadlines strictly earlier than the supplied UTC epoch millisecond. Use the returned workflow ID and key with approval inspection. |
| `ledgence_approval_list`, `ledgence_approval_inspect` | Read persisted requests and their exact effective action. |

Ledgence 0.9.0 preserves the optional `protection: "external_effect"`
field when listing or inspecting typed approval requests. It identifies the
required execution helper. Use matching 0.9.0 components for these
requests. The operator's application boundary still controls approval access.

Program discovery reads the existing installation catalog. Configure the
orchestrator's `--instance-config` and register packages with
`ledgence program register`. Serving frontend assets is not required.
Catalog requests carry expected tenant/namespace query guards and fail when the
installation scope differs. Older servers that lack these guards reject the
request; there is no unguarded fallback. An empty catalog means no registered
programs, not that the artifact store is empty.

Package upload/registration, worker administration, approval decisions, model
sampling, MCP prompts/resources, Streamable HTTP and MCP's own Tasks extension
are outside this server's current tool surface. Ledgence task/workflow IDs are
ordinary durable execution identities, independent from MCP request IDs.
Approval inspection does not grant approval; use the existing human interface or
`ledgence approval decide` to submit a decision bound to the stored action.
A generic workflow event cannot approve a durable action.

## Submit and observe

Arguments for `ledgence_task_submit`:

```json
{
  "program": "invoice-issuer",
  "version": "1.0.0",
  "queue": "billing",
  "data": {"invoice_id": "INV-1042"},
  "idempotency_key": "issue:INV-1042",
  "correlation_key": "INV-1042"
}
```

Use the same shape with `ledgence_workflow_submit` for a workflow package.
Optional scheduling and trace fields are documented by the tool's schema.
The first accepted `origin_trace` remains attached to the execution; a later
idempotent retry with omitted or different tracing does not replace it.
User `data` stays user-owned. The server neither generates submission keys nor
executes the package itself. After acceptance, save `task_id` or `workflow_id`
and query the corresponding status/result tool. It does not hold an MCP request
open for the entire execution or automatically poll for completion.

Successful tool results carry structured JSON and a text serialization of the
same object for client compatibility. Ledgence service failures return an MCP
tool error (`isError: true`) containing `code`, `message`, and `outcome_unknown`.
Malformed arguments and unavailable tool names are protocol errors.

An unavailable or timed-out submission may have committed. Do not change its
idempotency key or input: explicitly retry the identical command to recover its
identity. The server makes no automatic write retries. A known conflict does
not justify inventing a new key to bypass the conflict. The existing API's
idempotency retention and business-effect limitations remain unchanged.

Cancelling an MCP request or closing the session stops observation/local HTTP
work; it does not cancel a Ledgence task or workflow and does not prove that an
in-flight mutation failed. Use an explicit execution-cancellation tool when
that is intended. A successful cancellation request need not mean that all
running work has already stopped.

## Bounds and compatibility

The official Rust MCP SDK handles current `2026-07-28` discovery and legacy
`initialize`/`notifications/initialized` negotiation. The server advertises only
tools. It does not expose autonomous sampling or an approval-decision tool.

Inputs are capped at 2 MiB per newline-delimited frame. Tool JSON output is
capped at 2 MiB without truncation; read larger results through the HTTP API.
Serialized MCP replies are capped at 8 MiB, accounting for the structured and
escaped text copies. At most 16 outstanding requests are accepted per session.
Each tool call has a 30-second budget. Invalid JSON, duplicate object keys,
oversized frames, duplicate outstanding request IDs or excessive in-flight
requests close the session with a sanitized diagnostic. Business-operation
validation errors leave the session usable.

JSON preserves signed/unsigned 64-bit integers and finite binary64 values under
Ledgence's existing contract. Encode larger exact integers and decimal amounts
as strings. Replaying a mutation must preserve its semantic arguments, not just
an MCP request ID. MCP hosts may have stricter JSON/number or response-size limits.

For embeddings, `ledgence-adapter-mcp` accepts the existing `TaskService`,
`WorkflowService` and `ProgramCatalogService` ports. The CLI supplies HTTP
adapters. No HTTP client, database driver, PydanticAI, ADK or model SDK appears in
the production MCP adapter dependencies. Applications using a tracing subscriber
must avoid verbose `rmcp` logs, which can include request/result data; the CLI
MCP command installs no subscriber or exporter.

## Verification and design references

```sh
cargo test -p ledgence-adapter-mcp --locked
python3 tools/check-mcp-features.py
# Requires a disposable PostgreSQL server and a role able to create databases:
LEDGENCE_POSTGRES_URL='postgres://postgres:test@127.0.0.1:5432/postgres' \
  python3 tools/check-mcp.py --binaries target/debug --psql psql
```

The acceptance gate owns a database and subprocesses and cleans them up. It
exercises actual stdio, HTTP, PostgreSQL and Python execution, including lost
submission replies and disconnects. It requires no model provider.

The design follows the [current MCP specification](https://modelcontextprotocol.io/specification/2026-07-28)
and [official Rust SDK](https://github.com/modelcontextprotocol/rust-sdk).
[Hatchet](https://docs.hatchet.run/reference/cli/mcp) also offers a local CLI MCP
server; [Prefect](https://github.com/PrefectHQ/prefect-mcp-server) provides a
separate MCP integration. Ledgence reuses its existing service semantics rather
than creating a second orchestration authority. See [dependency review](dependencies.md#optional-mcp-server)
for the selected SDK and license material.

<span id="credential-access-in-development"></span>

## Credential access

Matching 0.9.0 components support optional installation credentials across
API operations, workers and Console. Configure the authority, client credential
files and capabilities using the [instance access guide](instance-access.md).
The existing operator-trusted mode requires its deployment access boundary.
Verified ingress identity and durable approval attribution have separate scope.


<span id="operator-audit-in-development"></span>

## Operator audit

Matching 0.9.0 components expose `ledgence_audit_query` in read-only and
ordinary MCP mode. It accepts an optional typed `filter` (`all`, `resource` or
`approval_acceptance`), `cursor` and `limit`, with scope fixed by the server.
Exact acceptance requires the complete occurrence, `limit=2` and no cursor.
Native credential mode and `metadata_read` authorize the metadata feed.
A shared MCP credential records the service identity. The tool's errors remain
distinct from a successful empty page. The portable adapter's audit tool is
available when its `AuditReader` port is configured. [Audit contract](operator-audit.md).

<span id="retained-effect-tools-in-development"></span>

## Retained effect tools

Ledgence 0.9.0 includes both effect-read tools. Both are available with `--read-only`; the startup scope and credential
remain fixed. Tool arguments cannot override scope, identity or credentials.

| Tool | Arguments and access |
| --- | --- |
| `ledgence_effect_inbox` | Optional `filter` (`all`, `unconfirmed`, `confirmed`, `discrepant`, `disputed`; default `all`), `workflow_id`, `cursor`, and `limit` (1–100; default 50). Metadata only; requires `metadata_read` in credential mode and is also available in trusted mode. |
| `ledgence_effect_inspect` | Required `workflow_id`, `activation_id`, `approval_key`; optional `cursor` and `limit` (1–4; default 4). Uses the original effect activation from the inbox. Requires credential mode, `sensitive_read` and a successful durable audit observation before returning sensitive evidence. |

The underlying HTTP response is limited to 1 MiB and contains whole entries.
Follow `next_cursor` with the same identity or filters even if a page is short;
a short page may have reached the byte budget. No polling or retry happens
inside either tool. Denied or unavailable reads remain errors, not historical
absence. Effect records remain readable after execution collection.

The detail separates immutable approval binding, journal state, claim provenance
and reported observations. Authenticated worker attribution does not verify the
external provider's outcome. Legacy sources remain unattributed. These tools
cannot approve, resolve or dispatch an action. See the [effect evidence
contract](effect-evidence.md) for retention and evidence interpretation.


<span id="operator-effect-resolution-in-development"></span>

## Operator effect resolution

Enable the mutation tool explicitly when starting the native server:

```sh
ledgence mcp serve --server http://127.0.0.1:8080 \
  --tenant acme --namespace billing --credentials-file resolver.json \
  --allow-effect-resolution
```

`--read-only` and `--allow-effect-resolution` cannot be combined. With audit configured, the native catalog exposes 30 tools by default and 25 in read-only mode. Effect resolution alone exposes 31 tools, resource control alone exposes 35, and enabling both exposes 36.
The added `ledgence_effect_resolve` also requires the credential's dedicated
`effect_resolve` capability. Fixed scope and credentials cannot be supplied or
overridden through tool arguments. Worker credentials cannot resolve effects.

Arguments are `workflow_id`, original `activation_id`, `approval_key`, stable
`resolution_id`, `expected_effect_revision`, `supersedes_resolution_id` (explicit
null for the first assessment), the reviewed `action`, `assessment`, mandatory
`reason` and ordered `evidence_references`. Assessment variants are
`{"kind":"record_success","result":...}`,
`{"kind":"record_failure","failure":{"kind":"...","message":"..."}}`, or
`{"kind":"close_unknown"}`. Success/failure require at least one reference;
closing unknown may use an empty list. Limits: result 128 KiB, full command 256 KiB,
reason 4096 UTF-8 bytes, and at most eight references of 1024 UTF-8 bytes each.

The input revision must be a **canonical decimal string**, for example `"42"`.
All effect-tool platform revision, sequence and count values, including program
size, are decimal strings in outputs and declared in output schemas. Timestamps
remain JSON numbers. Application arguments, reported outputs and assessment
results remain unchanged JSON. Never convert platform counters through JavaScript
floating-point numbers.

The server records an append-only operator assessment and returns a metadata
receipt with `acceptance_event_id`. It never dispatches work or proves provider
truth or workflow resumption. `sensitive_read` is independently needed to inspect
evidence. Keep the complete original arguments and stable actor after an
`outcome_unknown` error; no automatic retry occurs. An identical retry returns the
original receipt, including after supersession or collection. Changed arguments
or actor conflict. For opaque evidence, `disputed` filters current success-assessment conflict;
`discrepant` remains historical disagreement. Cancelled executions stay closed.

Use `ledgence_audit_query` with `filter={"kind":"event","event_id":"..."}`,
`limit=2` and no cursor to inspect a receipt's exact scoped audit event. A successful
empty response means absence; denied or unavailable reads remain errors.

<span id="typed-provider-evidence-in-development"></span>

## Typed provider evidence

The existing `ledgence_effect_inbox` accepts optional `provider_filter`:
`all`, `pending`, `needs_review`, `succeeded`, `failed`, `cancelled` or `disputed`.
It requires the ordinary `filter` to be `all` or omitted. Omission preserves the
mixed effect inbox; cursors cannot cross provider selections.
Provider `needs_review` excludes a historical dispute covered by the current
clean assessment; provider `disputed` still exposes that history. Typed provider
evidence can also dispute a recorded failure, without changing the resolve input.
`ledgence_effect_inspect` exposes the same original occurrence, full approved
provider binding, current typed report and whole-entry timeline as HTTP.
With audit configured, the native catalog exposes 30 tools by default and 25 in read-only mode. Effect resolution alone exposes 31 tools, resource control alone exposes 35, and enabling both exposes 36. No MCP approval mutation
or automatic provider dispatch is introduced.

All platform u64 counters, revisions, sequences, probe generations and pinned
program sizes are canonical decimal strings in MCP results. Timestamps and u32
check budgets remain JSON numbers; application arguments and results retain
their original JSON types. In a task read, `provider_probe` marks a managed
provider check; it does not change the task kind or imply business success.

Provider knowledge, reconciliation schedule, operator handling and the immutable
selected workflow wake describe different facts. A lost reply, missing lookup
result or expired provider key never grants another POST. Existing permissions,
startup-bound scope, sensitive-read audit and explicit `--allow-effect-resolution`
apply unchanged. These additions require matching 0.9.0 components.
[Provider recovery guide](https://docs.ledgence.com/how-to/recover-provider-operations/).

<span id="resource-admission-in-development"></span>

## Resource admission

Matching 0.9.0 builds add six resource read tools: `ledgence_resource_list`,
`ledgence_resource_inspect`, `ledgence_resource_decisions`, audited `ledgence_resource_sources`, `ledgence_admission_inspect` and
`ledgence_task_admission_inspect`. The native
catalog with audit has 30 default tools and 25 read-only tools. Explicit effect
resolution adds one. `--allow-resource-control` separately adds
`ledgence_resource_put`, `ledgence_admission_release`, `ledgence_task_admission_release`,
`ledgence_queue_policy_put` and `ledgence_queue_hold_release`
(35 tools, or 36 with both mutation opt-ins).
Call `ledgence_resource_sources` with `name` and optional `state`, `cursor` and
`limit`. A credential with `sensitive_read` can discover charged task,
workflow and provider occurrences. Every page records a resource response
audit. Pass `next_cursor` unchanged with the same name and state.
Session scope is fixed. Policy/release commands require stable caller request
IDs, inspected revisions and the API's `resource_control` permission. Read-only
mode rejects either mutation opt-in. Release never asserts remote success.
See [resource admission](resource-admission.md) for limits and recovery semantics.

`ledgence_task_submit` accepts optional `admission: {"resource":"support-model"}`.
Only standalone tasks support it. The task inspector addresses `task_id` and
`generation`; release additionally requires `source_id`, `expected_revision`, a
stable `request_id` and `reason`. The MCP server fixes scope and never requests
a worker dispatch grant.

The 0.9.0 catalog includes `ledgence_queue_policy_inspect`, `ledgence_queue_policy_list` and `ledgence_queue_holds` in both default and read-only modes. Exact hold reads require `sensitive_read`; edits and releases require the resource-control opt-in and server permission. [Queue policy contract](queue-concurrency.md).

In Ledgence 0.9.0, `ledgence_budget_inspect` and `ledgence_budget_entries` read the budget shared by an execution tree. Give either tool a `subject` such as `{"kind":"workflow","workflow_id":"wf_123"}`. The server binds the session scope. For entries, pass the returned `next` as `before` for older records. Both tools require `metadata_read` and are available in read-only mode. Root workflow and standalone task submissions accept an optional `budget`.
