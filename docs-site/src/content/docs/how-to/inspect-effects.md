---
title: Inspect and resolve external effects
description: Inspect approved writes, review retained evidence and resume a parked workflow from an operator assessment.
---

Available in Ledgence 0.8.0 with matching components and the required migrations.

When a worker disappears during an external write, start with its retained
evidence. The inbox identifies the approved occurrence. Inspection shows the
approved arguments, program, worker claims and reported results, including
reports retained after execution history has been collected.

## Prepare access

Upgrade and migrate all API replicas and workers together. Configure an operator
credential with `metadata_read` and `sensitive_read`. To record an assessment,
also grant `effect_resolve`; approval permission alone does not include it.
Give the worker its
own `worker_execute` credential. The native server records a durable audit event
before returning sensitive inspection data.
[Configure instance access](/how-to/configure-instance-access).

## Find an unconfirmed write

In matching Console contract 7, open **Effects** from the sidebar, filter the
inbox and select an occurrence to review its original action and evidence.
Journal confirmation and operator handling are separate: a closed assessment
can still have an unconfirmed journal. Use the CLI when investigating without
the browser:

Run this command from the directory containing your operator credential file.
Replace the tenant and namespace with your installation's values.

```sh
ledgence effect inbox --server http://127.0.0.1:8080 \
  --tenant acme --namespace billing \
  --credentials-file ./operator.json --filter unconfirmed
```

Each item contains an `occurrence`. Copy its `workflow_id`, `activation_id` and
`approval_key` into the next command. The activation identifies where this
approved effect began, even when a later worker recovered its result.

```sh
ledgence effect inspect --server http://127.0.0.1:8080 \
  --tenant acme --namespace billing --credentials-file ./operator.json \
  --workflow wf_example --activation task_approved --key refund --limit 4
```

Use `--cursor` with the returned `next_cursor` to read another page. Continue
until the cursor is null. Large entries can make a page shorter than its limit.

## Understand the result

| Field or entry | Meaning |
| --- | --- |
| `initial_permission` | The dispatch or lookup permission originally recorded for that claim. |
| `verified_worker_claim` | The authenticated worker and the callback mode it reported. |
| `legacy_journal` | A historical accepted result whose original worker identity was unavailable. |
| `journal_state` | Whether Ledgence accepted a durable local result for the occurrence. |
| `has_discrepancy` | Different historical reported values exist. |
| `current_assessment` | The latest full operator assessment, even when its timeline entry is outside this page. |
| `operator_resolution` | Metadata for the current assessment and a possible later conflicting report. |

A dispatch grant records permission. A provider's actual outcome comes from the
application adapter's observations. An unavailable lookup or failed connection
can leave that outcome unknown. Cancelling an execution keeps its retained
evidence available.

Python exposes `client.effects.inbox()` and `client.effects.inspect()`.
MCP exposes `ledgence_effect_inbox` and `ledgence_effect_inspect` with the same
access checks. Metadata inbox queries also work in operator-trusted deployments.
Sensitive inspection requires credential mode and durable audit.
[Python client](/reference/python-client) · [MCP setup](/how-to/connect-mcp).

The evidence ledger has no automatic expiry in this capability.
Include it in storage planning and database backups. See the source contract
for migration and callback details.
[Retained evidence contract](https://github.com/Ledgence/ledgence/blob/develop/docs/effect-evidence.md).


## Record what the investigation established

Choose a result based on the original approved action and the evidence you
reviewed. **Record success** stores your selected JSON result. **Record failure**
stores a failure kind and message. **Close unknown** records that no reliable
outcome was selected. None of these authorizes another write, confirms the
execution journal or claims independent provider verification.

A reason is required. Include references to the receipts, support cases or
records you used; success and failure require at least one. The assessment binds
the exact effect revision and previous assessment visible when prepared. It does
not claim that every timeline page was read. Follow pagination when your review
needs the full history.

Prepare the assessment in Console or with the Python client, and save the frozen
command before sending. For Python, use `command.to_json()`; this is a versioned
envelope with the original endpoint and exact request bytes. It contains sensitive
action/result data, not credentials. The saved envelope also works with the CLI:

```sh
ledgence effect resolve --server http://127.0.0.1:8080 \
  --tenant acme --namespace billing --credentials-file ./operator.json \
  --file effect-assessment.json
```

If delivery fails after you send, keep that file. Reconcile using the same file,
endpoint, scope and verified actor. A replacement credential for the same actor
is allowed. Do not change the resolution ID or refresh the revision to retry an
uncertain delivery. An exact retry returns the original acceptance receipt;
changed commands or actors conflict. A new reviewed judgment requires a new
resolution ID bound to the latest revision and previous assessment.

An accepted receipt links a verified audit event through `acceptance_event_id`.
It proves the operator judgment was stored; it does not prove the workflow has
resumed. A workflow must explicitly park using `ctx.wait_effect_resolution(...)`.
That wait releases the worker slot, and the coordinator can select assessments
accepted before or after parking. The resumed entrypoint receives the typed
`ctx.effect_resolution` result. Generic workflow events cannot supply it.
[Worker context](/reference/workflow-context#native-effect-resolution-in-development)
· [Python saved-command example](/reference/python-client#operator-effect-assessments-in-development).

## Handle later evidence

`disputed` means a later report differs from the selected result of the current
success assessment. It is separate from `discrepant`, which preserves historical
report disagreement. Ledgence keeps arbitrary report JSON opaque; a key named
`state` does not turn it into a provider-verified success or failure.

Earlier workflow wakes stay frozen. Another assessment requires an explicit
follow-up native wait to affect workflow execution. Cancelled or terminal runs
stay closed, even when evidence or assessments arrive later. An accepted
assessment releases the unresolved execution retention hold; ordinary retention
rules still apply, and the binding, reports, assessments and receipts remain
available after execution collection. Later disagreement does not reopen that hold.

## Provider operations

For approved asynchronous operations, use **Effects** and the provider filter.
The provider panel separates reported knowledge, read-only check schedule and
operator handling. Open a managed check task from the schedule to inspect its
attempt diagnostics; its successful completion alone is not provider success.
The workflow graph shows a durable provider wait and its recorded resume, with
an exact link back to the original effect occurrence.

`unknown`, `pending`, `requires_action`, terminal reports and disputed evidence
remain distinct. A dispute can also concern conflicting provider references
before a terminal outcome. The complete binding was frozen before
approval; inspect it together with the effective arguments. Provider reports
never confirm the legacy local journal. An operator assessment cannot grant
redispatch or rewrite an already selected wake.
The provider **Needs review** filter excludes historical conflicts covered by the
current clean assessment. **Disputed** keeps that history visible; new evidence
that conflicts with the current assessment makes it need review again.
[Configure and use provider recovery](/how-to/recover-provider-operations).
