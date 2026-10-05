---
title: Connect an MCP client
description: Discover programs, submit work, and inspect durable executions through the optional Ledgence MCP server.
---

Ledgence 0.3.1 includes the optional MCP server in its native bundles and default
CLI build. The server connects to your existing Ledgence HTTP API; it does not
start infrastructure or require an AI framework.

## Install and configure

Use the executable from a [native bundle](/how-to/install-native), or build the
matching source:

```sh
cargo build -p ledgence-cli --locked
./target/debug/ledgence mcp serve \
  --server http://127.0.0.1:8080 \
  --tenant acme \
  --namespace billing
```

Configure your MCP host to launch that executable with those arguments. The
command communicates over stdio; stdout is reserved for protocol messages.
Use an absolute executable path in the host configuration. For hosts using
`mcpServers`:

```json
{
  "mcpServers": {
    "ledgence": {
      "command": "/absolute/path/to/ledgence",
      "args": ["mcp", "serve", "--server", "http://127.0.0.1:8080", "--tenant", "acme", "--namespace", "billing"]
    }
  }
}
```

Add `--read-only` to expose observation tools only. The API URL and scope are
fixed at startup. The API may be remote; no MCP network listener is opened.
`--no-default-features --features mcp` builds this command without telemetry or SQS.

## Submit and follow work

The server exposes tools for program discovery, task/workflow submission,
status/results, task listing, cancellation, external workflow events, and
approval inspection. Program discovery requires an installation catalog with
matching scope and registered packages; serving the Console frontend is optional.

For `ledgence_task_submit`, provide:

```json
{
  "program": "invoice-issuer",
  "version": "1.0.0",
  "queue": "billing",
  "data": {"invoice_id": "INV-1042"},
  "idempotency_key": "issue:INV-1042"
}
```

The response returns an execution ID immediately. Save it and use the matching
status/result tool. An observation timeout or disconnected MCP session does not
cancel the execution. If submission acceptance is uncertain, resend the exact
same key and input; do not create a new key automatically. Only explicit execution
cancellation tools request cancellation.

Approval tools inspect the stored effective action. Decisions remain in the
human approval interface or CLI; the MCP server cannot approve its own work.

Tool outputs are bounded to 2 MiB without truncation; larger results remain
available through the HTTP API. This version provides stdio tools, not Streamable
HTTP, model sampling, prompts/resources, package uploads, or MCP Tasks-extension
state. See the [full tool contract and recovery guide](https://github.com/Ledgence/ledgence/blob/v0.3.1/docs/mcp.md)
for limits, error semantics, verification and architecture.
