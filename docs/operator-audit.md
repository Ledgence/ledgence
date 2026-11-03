# Operator audit

Available in Ledgence 0.8.0 with credentials enabled. Existing operator-trusted
installations keep their deployment access boundary.

Native credential mode records verified approval and effect-assessment acceptance
and authorizes sensitive responses through a durable audit boundary. Enable the
authority and migrate the database before starting every API replica. Startup requires the
complete `OperatorAccess` port. The PostgreSQL adapter implements that port.
[Instance setup](instance-access.md).

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
The event records an operator judgment. Provider evidence, journal confirmation
and workflow resumption retain their independent records.

Exact assessment replay requires the original command and stable verified actor,
with currently valid permissions. It retains the first actor/credential evidence,
even after credential rotation, supersession, cancellation or execution cleanup.
Each assessment creates its own immutable audit event.
[Assessment and rendezvous semantics](effect-evidence.md#record-an-operator-assessment).

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
revisions and acceptance time to identify its accepting actor.

## HTTP contract

`POST /v1/audit/events` accepts `scope`, `filter`, nullable `cursor` and `limit`.
`POST /v1/console/audit/events` derives scope from the installation. Filters are
`all`, `resource` with `resource_kind` and `resource_id`, `approval_acceptance`
with `workflow_id`, `key`, `activation_id`, `revision` and `decision_id`, or
`event` with `event_id`.

All/resource pages permit limits from 1 through 100. Both exact approval and exact
event filters require `limit=2` and `cursor=null`, returning zero or one item and
`next_cursor=null`. Event lookup requires the configured installation scope.
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
[Retention procedure](retention.md).

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
[Retained effect evidence](effect-evidence.md).

Provider-operation details use that same `effect_inspect` boundary and exact
occurrence resource. Typed provider reports and probe source admission belong to
the retained worker-evidence ledger, not `AuditActor` operator events. The
existing `effect_resolved` acceptance event records an operator assessment;
stopping provider recovery is part of that acceptance transaction. Inspecting
provider targets, bindings or reports requires the same sensitive permission
and durable read observation through generic and Console aliases.
[Provider operations](provider-operations.md).

## Resource admission audit in development

Matching development builds add `resource_policy_accepted` and
`admission_released`; these operations are not in published 0.8.0. Policy events
record the resource name, request ID, accepted revision, policy digest and
operator reason. Release events record the original admission occurrence,
source ID, request ID, accepted revision and operator reason. The reason is
retained and returned exactly as supplied, bounded to 1024 UTF-8 bytes. These
events contain no frozen callback arguments or accepted callback result.

`ledgence audit list --resource NAME` selects policy events for that resource.
Only one of `--resource`, `--task` and `--workflow` may be supplied. HTTP and
MCP use `filter={"kind":"resource","resource_kind":"resource","resource_id":"NAME"}`;
Python exposes `AuditFilter.resource(AuditResourceKind.RESOURCE, "NAME")`.
Queries retain their existing `metadata_read` requirement and fixed scope.

Admission release events and audited sensitive inspections are attached to the
original workflow; query its `--workflow` filter for those records. An inspection
uses read kind `admission_inspect` and an `admission` resource containing the
original occurrence. It requires `sensitive_read` and a committed observation
before returning arguments or results through either the generic or Console route.

Policy and release acceptance events and command receipts have no automatic
expiry, including after execution retirement. Sensitive-response observations
follow the existing observation-retention lane. See
[resource admission](resource-admission.md#http-cli-python-and-mcp) for the public
interfaces and [retention](retention.md#retained-admission-usage-in-development)
for the independent admission journal's lifetime.
