---
title: Connect an MCP client
description: Discover programs, submit work, and inspect durable executions through the optional Ledgence MCP server.
---

Ledgence 0.6.0 includes the optional MCP server in its native bundles and default
CLI build. The server connects to your existing Ledgence HTTP API; it does not
start infrastructure or require an AI framework.

## Install and configure

Use the CLI from your [chosen installation](/getting-started/installation).
The local tutorial instance uses the following server and compatibility binding:

```sh
ledgence mcp serve \
  --server http://127.0.0.1:8080 \
  --tenant acme \
  --namespace demo
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
      "args": ["mcp", "serve", "--server", "http://127.0.0.1:8080", "--tenant", "acme", "--namespace", "demo"]
    }
  }
}
```

Add `--read-only` to expose observation tools only. The API URL and scope are
fixed at startup. The API may be remote; no MCP network listener is opened.
`--no-default-features --features mcp` builds this command without telemetry or SQS.

Set `--tenant` and `--namespace` to the running instance's saved binding. The
local installation routes use `acme/demo`; a new instance with omitted scope uses
`default/default`. A catalog scope
mismatch is rejected; it does not switch installations. See
[One self-hosted instance](/concepts/self-hosted-console).

## Submit and follow work

The server exposes tools for program discovery, task/workflow submission,
status/results, task listing, cancellation, external workflow events, and
approval inspection. Program discovery requires an installation catalog with
matching scope and registered packages; serving the Console frontend is optional.

After publishing the task in [Hello, Ledgence!](/tutorials/first-task), provide
this input to `ledgence_task_submit`:

```json
{
  "program": "hello-ledgence",
  "version": "0.1.0",
  "queue": "demo",
  "data": {"name": "Ledgence"},
  "idempotency_key": "hello-ledgence:mcp:1"
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
state. See the [full tool contract and recovery guide](https://github.com/Ledgence/ledgence/blob/v0.6.0/docs/mcp.md)
for limits, error semantics, verification and architecture.
