# Operator audit

Available in matching development components. The published 0.7.0 packages use
their existing operator-trusted deployment contract.

Native credential mode records verified approval acceptance and authorizes
sensitive responses through a durable audit boundary. Enable the authority and
migrate the database before starting every API replica. Startup requires the
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

## Query audit events

The `metadata_read` capability permits the payload-free audit feed for the bound
instance scope, including other operators' identities and resource references.
The feed is available with configured credential authority. Operator-trusted
mode has no verified actor to authorize this feed.

Read a bounded page with the development CLI.

```sh
./target/debug/ledgence audit list \
  --server http://127.0.0.1:8080 \
  --credentials-file access/operator.json \
  --tenant acme --namespace demo \
  --workflow wf_123 --limit 50
```

Use `--task` for a task resource, or omit both resource flags to list the scope.
Workflow filtering includes its directly recorded approval, input, result and
local-step events. Child task and subworkflow records have their own resource
identities. Reuse `next_cursor` with the same filters for the next page.

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
The MCP tool `ledgence_audit_query` uses the process's fixed scope and configured
service credential.

## HTTP contract

`POST /v1/audit/events` accepts `scope`, `filter`, nullable `cursor` and `limit`.
`POST /v1/console/audit/events` derives scope from the installation. Filters are
`all`, `resource` with `resource_kind` and `resource_id`, or `approval_acceptance`
with `workflow_id`, `key`, `activation_id`, `revision` and `decision_id`.

All/resource pages permit limits from 1 through 100. Exact acceptance requires
`limit=2` and `cursor=null`, returning zero or one item and `next_cursor=null`.
Conflicting or mismatched evidence fails the query. Generic revisions are unsigned
64-bit integers. Console uses decimal strings, preserving large revisions.

Each event contains `event_id`, `recorded_at`, `audience`, `actor` and a typed
`operation`. Operations are `approval_accepted` or `sensitive_response`.
The feed contains bounded identities and operation metadata. Action arguments,
application results, reviewer labels, reasons and bearer credentials are excluded.
Pagination follows an ordering sequence. Sequence allocation is independent of
transaction commit order, and multiple pages do not form a database snapshot.

## Sensitive response observations

The server validates and serializes the response, persists its observation and
validates the durable receipt before returning sensitive bytes. An unavailable
or malformed audit receipt prevents successful payload delivery. An observation
records authorization to return a response. Network delivery can still fail.

The boundary covers task inspection/results, workflow results/inputs, accepted
local-step details and approval inspection/list/decision through generic and
Console aliases. Every exposed payload creates an observation, including
terminal JSON null, failures and repeated inspection. Accepted decisions have a
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

Approval acceptance events survive execution cleanup and have no automatic
expiry in this increment. Plan storage for accumulated acceptance evidence and
repeated payload reads. Database administrators remain trusted. These records
provide durable application evidence without cryptographic nonrepudiation.

## Retained effect inspection

The development evidence inspector uses `effect_inspect` and an `effect` resource
containing the workflow ID, original activation ID and approval key. Sensitive
inspection requires configured credentials, `sensitive_read` and durable audit
before either generic or Console response bytes are released. Worker claims and
reported callback results have their own evidence provenance.
[Retained effect evidence](effect-evidence.md).
