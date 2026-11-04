---
title: Connect an MCP client
description: Discover programs, submit work, and inspect durable executions through the optional Ledgence MCP server.
---

Ledgence 0.8.0 includes the optional MCP server in its native bundles and default
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
state. See the [full tool contract and recovery guide](https://github.com/Ledgence/ledgence/blob/v0.8.0/docs/mcp.md)
for limits, error semantics, verification and architecture.

Ledgence 0.8.0 exposes `ledgence_approval_inbox` in read-only mode. Optionally set `due_before` to UTC epoch milliseconds to select earlier deadlines. Discover requests in the configured scope, then pass the workflow ID and key to `ledgence_approval_inspect`. [Approval inbox](/how-to/require-approval#find-pending-requests-across-workflows).

<span id="instance-access-in-development"></span>

## Instance access

Use scoped credentials with matching 0.8.0 components. The
[instance access guide](/how-to/configure-instance-access) covers provisioning,
Console identity, client files, capabilities, rotation and operating limits.


<span id="operator-audit-in-development"></span>

## Operator audit

Matching builds add `ledgence_audit_query` as a read-only tool. It uses the fixed instance scope and service credential, requiring `metadata_read` with native credential mode.
[Setup, queries and operating limits](/how-to/audit-operators).

<span id="retained-effect-tools-in-development"></span>

## Retained effect tools

Matching 0.8.0 components add two read-only tools, bringing the native
catalog with audit configured to 19 tools. Both are available with `--read-only`; the startup scope and credential
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
contract](https://github.com/Ledgence/ledgence/blob/develop/docs/effect-evidence.md) for retention and evidence interpretation.


<span id="operator-effect-resolution-in-development"></span>

## Operator effect resolution

Enable the mutation tool explicitly when starting the native server:

```sh
ledgence mcp serve --server http://127.0.0.1:8080 \
  --tenant acme --namespace billing --credentials-file resolver.json \
  --allow-effect-resolution
```

`--read-only` and `--allow-effect-resolution` cannot be combined. Matching 0.8.0
builds expose 19 tools by default with audit configured, 14 in read-only mode, or
20 with resolution enabled.
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
or actor conflict. `disputed` filters current success-assessment conflict;
`discrepant` remains historical disagreement. Cancelled executions stay closed.

Use `ledgence_audit_query` with `filter={"kind":"event","event_id":"..."}`,
`limit=2` and no cursor to inspect a receipt's exact scoped audit event. A successful
empty response means absence; denied or unavailable reads remain errors.

## Inspect provider recovery

Matching 0.8.0 MCP and API builds reuse `ledgence_effect_inbox` and
`ledgence_effect_inspect`. Set `provider_filter` to `needs_review` to find
operations needing review; leave ordinary `filter` as `all` or omit it. Other
provider filters are `all`, `pending`, `succeeded`, `failed`, `cancelled` and
`disputed`. Keep the same selection when following its cursor.

Inspection requires `sensitive_read` and durable audit. Provider reports and
check scheduling are typed evidence, not a promise that task success equals
external success. Platform u64 fields, including probe generations and program
sizes, are decimal strings; application JSON remains unchanged. The tool count
and explicit effect-resolution opt-in are unchanged. [Recover provider operations](/how-to/recover-provider-operations).

## Resource tools in development

Matching source builds add `ledgence_resource_list`, `ledgence_resource_inspect`
and audited `ledgence_admission_inspect`. Explicit `--allow-resource-control`
adds policy and release tools, subject to `resource_control`. It conflicts with
`--read-only`. An administrative release acknowledges uncertain usage; it never
asserts remote success. [Admission guide](/how-to/control-resource-capacity).

Matching development builds add `ledgence_task_admission_inspect` for a task ID
and generation. `ledgence_task_submit` accepts one optional named resource.
`ledgence_task_admission_release` requires the explicit resource-control opt-in,
source, inspected revision, request ID and operator reason. [Task resource admission](/how-to/control-resource-capacity#apply-the-resource-to-a-single-task).
