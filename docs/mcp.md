# MCP server

Current source builds after 0.2.0 expose Ledgence through an optional MCP server
in the unified `ledgence` executable. Published 0.2.0 binaries do not include it.
The server connects to your existing HTTP API; the orchestrator, database and
workers continue running separately. No AI framework, model or vendor account
is required by this integration.

## Connect

Build current source, then configure your MCP host to launch:

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
authentication mechanism. The existing operator-trusted deployment boundary
still applies.

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
| `ledgence_approval_list`, `ledgence_approval_inspect` | Read persisted requests and their exact effective action. |

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
