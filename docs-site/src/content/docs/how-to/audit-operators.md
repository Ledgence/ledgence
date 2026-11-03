---
title: Inspect operator audit
description: Inspect verified approval and effect-assessment acceptance and durable sensitive-response observations.
---

Available in Ledgence 0.8.0 with credentials enabled. Existing operator-trusted
installations keep their deployment access boundary.

Native credential mode records verified approval and effect-assessment acceptance
and authorizes sensitive responses through a durable audit boundary. Enable the
authority and migrate the database before starting every API replica. Startup requires the
complete `OperatorAccess` port. The PostgreSQL adapter implements that port.
[Instance setup](/how-to/configure-instance-access).

## Identify who accepted a decision

A new approval acceptance stores its verified issuer, subject, principal class
and credential ID in the same transaction as the decision and eligible workflow
continuation. Its timestamp matches the stored decision's acceptance time.
`reviewer` remains a caller-supplied label. A shared service credential identifies
that service, including when an MCP host serves several people.

An audited decision replay requires the identical command and the same stable actor in the
same authority, instance and scope. Credential rotation for that actor is
supported when the replacement credential has the required current permissions.
A different actor cannot claim the existing acceptance by repeating its command.
Historical accepted decisions keep their original bytes and have no fabricated
accepting identity. Their exact replay remains available to a currently authorized
operator and preserves the historical absence of verified attribution. Existing
worker continuations keep their original schema.

A failure after possible acceptance returns an uncertain `503 unavailable`.
Preserve the command for explicit reconciliation. Definitive `401` and `403`
rejections occur before acceptance. Cancellation after acceptance preserves the
recorded decision and follows the existing workflow cancellation rules.

## Identify who recorded an effect assessment

A verified effect assessment appends an `effect_resolved` event in the same
transaction as its immutable assessment, effect revision and any eligible native
wait obligation. The event's `recorded_at` equals the receipt's `accepted_at`.
Its operation binds the original occurrence, resolution ID, accepted and basis
revisions, nullable predecessor ID and assessment kind. The receipt's
`acceptance_event_id` identifies this exact event.

Resolution requires `effect_resolve`, independently of `approval_decide` and
`sensitive_read`. The response contains acceptance metadata only, so it needs
no additional sensitive-response observation. Inspection of the action,
reported results or assessment contents still requires the separate read audit.
The event records an operator judgment, not verified provider truth, journal
confirmation or a promise that the workflow resumed.

Exact assessment replay requires the original command and stable verified actor,
with currently valid permissions. It retains the first actor/credential evidence,
even after credential rotation, supersession, cancellation or execution cleanup.
A new assessment creates a new event; earlier events remain unchanged.
[Assessments and workflow waits](/how-to/inspect-effects#record-what-the-investigation-established).

## Query audit events

The `metadata_read` capability permits the payload-free audit feed for the bound
instance scope, including other operators' identities and resource references.
The feed is available with configured credential authority. Operator-trusted
mode has no verified actor to authorize this feed.

Read a bounded page with the 0.8.0 CLI.

```sh
./target/debug/ledgence audit list \
  --server http://127.0.0.1:8080 \
  --credentials-file access/operator.json \
  --tenant acme --namespace demo \
  --workflow wf_123 --limit 50
```

Use `--task` for a task resource, or omit both resource flags to list the scope.
Workflow filtering includes its directly recorded approval, effect-assessment,
effect-inspection, input, result and local-step events. Child task and subworkflow
records have their own resource identities. Reuse `next_cursor` with the same filters for the next page.

For an exact acceptance, take every identity from the saved approval and decision.

```sh
./target/debug/ledgence audit approval \
  --server http://127.0.0.1:8080 \
  --credentials-file access/operator.json \
  --tenant acme --namespace demo \
  --workflow wf_123 --key refund --activation task_456 \
  --revision 0 --decision review_789
```

A successful empty exact page means no verified accepting actor was recorded.
An unavailable, denied or malformed response leaves attribution unavailable.
Console shows these states separately in the approval review and decision dialog.

Python clients expose `client.audit.list()` and
`client.audit.approval_acceptance(approval)`. The latter accepts the retained,
endpoint-bound `WorkflowApproval` returned by inspection or a decision receipt.
It returns an `AuditEvent` or `None` for a successfully validated empty lookup.
For an effect receipt, `await client.audit.event(receipt.acceptance_event_id)`
looks up its exact event without scanning workflow history. The typed `event`
filter is also available through `client.audit.list()` and the MCP tool
`ledgence_audit_query`, which uses the process's fixed scope and configured
service credential. When displaying effect attribution, bind the returned
`effect_resolved` operation to the receipt's full occurrence, resolution ID,
revisions and acceptance time; another event or the latest observer is not its
accepting actor.

## HTTP contract

`POST /v1/audit/events` accepts `scope`, `filter`, nullable `cursor` and `limit`.
`POST /v1/console/audit/events` derives scope from the installation. Filters are
`all`, `resource` with `resource_kind` and `resource_id`, `approval_acceptance`
with `workflow_id`, `key`, `activation_id`, `revision` and `decision_id`, or
`event` with `event_id`.

All/resource pages permit limits from 1 through 100. Both exact approval and exact
event filters require `limit=2` and `cursor=null`, returning zero or one item and
`next_cursor=null`. Event lookup is scoped to the configured installation; the ID
does not grant access to another scope.
Conflicting or mismatched evidence fails the query. Generic revisions are unsigned
64-bit integers. Console uses decimal strings, preserving large revisions.

Each event contains `event_id`, `recorded_at`, `audience`, `actor` and a typed
`operation`. In published 0.8.0, operations are `approval_accepted`,
`effect_resolved` or `sensitive_response`. Those events contain bounded identities
and operation metadata; action arguments, application results, reviewer labels,
reasons and bearer credentials are excluded. Development admission events retain
operator reasons as described below.
Pagination follows an ordering sequence. Sequence allocation is independent of
transaction commit order, and multiple pages do not form a database snapshot.

## Sensitive response observations

The server validates and serializes the response, persists its observation and
validates the durable receipt before returning sensitive bytes. An unavailable
or malformed audit receipt prevents successful payload delivery. An observation
records authorization to return a response. Network delivery can still fail.

The boundary covers task inspection/results, workflow results/inputs, accepted
local-step details, approval inspection/list/decision and retained effect
inspection through generic and Console aliases. Every exposed payload creates an
observation, including terminal JSON null, failures and repeated inspection. Accepted decisions have a
separate immutable acceptance event. Audit queries do not recursively audit
another response.

Fully validated task/workflow result responses with a pending `outcome=null`
retain sensitive-read authorization and create zero audit observations.
Worker acquisition, renewal, context and settlement keep their existing path.

## Retention and operating limits

The scoped `ledgence orchestrator retain` command includes a bounded lane for
response observations older than the selected retention interval, at least
90 days. Preview includes `audit_observation_candidates`. Each deletion page
respects the existing batch size and concurrent maintenance uses row locks.
[Retention procedure](https://github.com/Ledgence/ledgence/blob/develop/docs/retention.md).

Approval and effect-assessment acceptance events survive execution cleanup and
have no automatic expiry in this increment. Plan storage for accumulated
acceptance evidence and repeated payload reads. Database administrators remain trusted. These records
provide durable application evidence without cryptographic nonrepudiation.

## Retained effect inspection

The evidence inspector uses `effect_inspect` and an `effect` resource
containing the workflow ID, original activation ID and approval key. Sensitive
inspection requires configured credentials, `sensitive_read` and durable audit
before either generic or Console response bytes are released. Worker claims and
reported callback results have their own evidence provenance.
[Retained effect evidence](/how-to/inspect-effects).

## Resource admission audit in development

Matching development builds add `resource_policy_accepted` and
`admission_released`; published 0.8.0 does not include them. Policy events identify
the resource, request, revision and policy digest. Release events identify the
original admission occurrence, source, request and revision. Both retain and
return the operator's reason exactly, bounded to 1024 UTF-8 bytes. Frozen callback
arguments and accepted results are excluded from these audit events.

Use `ledgence audit list --resource NAME` for policy events, choosing only one
of `--resource`, `--task` and `--workflow`. MCP's `ledgence_audit_query` accepts
`filter={"kind":"resource","resource_kind":"resource","resource_id":"NAME"}`;
Python uses `AuditFilter.resource(AuditResourceKind.RESOURCE, "NAME")`. All retain
the existing `metadata_read` requirement and fixed scope.

Release events and `admission_inspect` sensitive-response observations belong to
the original workflow; use its `--workflow` filter. Policy/release acceptance
events and command receipts have no automatic expiry after execution retirement.
Sensitive-response observations use the existing retention lane.
[Admission setup and retained-record limits](/how-to/control-resource-capacity).
