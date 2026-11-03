# Durable resource admission

**Development capability:** this contract belongs to the current source checkout.
It is not included in the published 0.8.0 packages or images. Use matching server,
worker, Python helper/client and Console assets, and apply all migrations explicitly.

A workflow can park while a named resource has no capacity, then resume to invoke
one registered operation. Waiting consumes no worker subprocess. The resource is
scoped to the installation's authenticated audience, tenant and namespace.

## Enable admission

Configure instance access first. Each API replica needs the same installation,
credentials and an explicit `--admission-config admission.json`. The configuration
is read once at startup; changing it requires restarting the replicas.

```json
{
  "v": 1,
  "audience": {
    "authority_id": "YOUR_ACCESS_AUTHORITY_ID",
    "instance_id": "YOUR_INSTANCE_ID",
    "scope": {"tenant_id": "acme", "namespace": "demo"}
  },
  "dispatch": "enabled"
}
```

Copy the audience values from the installation's access configuration; they are
identifiers, not credentials. Missing or mismatched configuration does not enable
admission. Workers negotiate `resource_admission.v1` and a verified worker binding.
Provider adapters and protected-effect support are not prerequisites.

Resource administration requires an **operator** credential with
`resource_control`. Newly generated `ledgence access init` bundles include it;
existing credentials do not acquire permissions on upgrade. Issue a replacement
credential explicitly. Metadata reads require `metadata_read`; frozen operation
arguments/results require `sensitive_read` and a committed response-audit record.

## Policies and accounting

A policy has `enabled`, `max_occupancy`, optional `rate`, `offer_timeout_ms` and
`start_timeout_ms`. `rate` is a token bucket `{numerator, period_ms, burst}`:
`numerator` authorizations per `period_ms`, capped at `burst` stored tokens.
A token bucket permits bursts; it is not a rolling-window ceiling.

| State | Occupies capacity | Charges rate credit |
| --- | --- | --- |
| Waiting | No | No |
| Offered | Yes | Reserves credit, without charging |
| Started or uncertain | Yes | Charged once at Start |
| Completed with accepted result | No | No refund |
| Expired offer before Start | No | Unused reservation released |
| Administratively released | No | No refund; uncertain fact retained |

`occupied` includes offered, started and unknown usage; `offered` and `unknown`
are subsets, not extra values to add. Policy changes use compare-and-set
`expected_revision` and a caller-supplied `request_id`. Exact replays are inert;
a changed command under the same identity conflicts. Updates invalidate unstarted
offers but preserve charged usage and uncertain debt. Reducing capacity can leave
occupancy above the new limit; it does not terminate running callbacks.

The implementation uses integer credit and PostgreSQL time sampled after locking
the resource. Backward clock movement cannot create additional refill credit;
a forward jump can refill to the configured burst. FIFO selection is resource-local.
This version does not implement weighted fairness, inherited monetary/token
budgets or global ordering across resources.

## Write a workflow

Register module-level functions with `@workflow.operation(kind=OperationKind.MODEL,
version="1")` (or `TOOL`). `workflow.build()` freezes the registry. Before parking,
`operation.bind(**arguments)` freezes effective JSON arguments, including defaults.
The immutable program artifact and operation version become part of the binding.
Runtime clients and credentials stay inside the function, outside persisted arguments.

Return `ctx.request_admission(key, resource=..., operation=..., resume=Entry.CALL,
state={}, timeout_ms=...)` from the requesting entrypoint. This always checkpoints,
even when capacity is immediately available. In the resumed entrypoint, await
`ctx.admitted_operation(registered_function)` without supplying replacement arguments.

| Outcome | Application action |
| --- | --- |
| `AdmissionOutcomeKind.SUCCEEDED` | Use `outcome.result`; the callback may have been replayed from its accepted result. |
| `WAIT_REQUIRED` | Return `ctx.wait_admission(...)` for the same occurrence. |
| `NEEDS_REVIEW` | Inspect the uncertain source; optionally wait durably for completion evidence or an operator decision. Do not dispatch again. |
| `CLOSED` | Handle closure without interpreting it as business success. |

A logical occurrence is `(scope, workflow_id, origin_activation_id, key)`.
Rewaiting preserves this identity and creates a separate wait episode. A fresh
request from another activation is new work. Before Start, rewaiting never extends the original absolute admission deadline.
After Start, an independent bounded wait episode may wait beyond that deadline
for Finish or an operator decision. Its timeout returns `NEEDS_REVIEW`, grants no
new Start and does not release occupancy. The original admission deadline stays
unchanged; timeout is not proof that an external call stopped.

See the [runnable example](../examples/resource-admission/README.md) for two typed
entrypoints and a registered asynchronous operation.

## Start, Finish and failures

An offer identifies its generation, policy revision, expiry and exact resumed
activation. It is readiness information, not permission to call. The worker asks
for Start using its live lease. Only the first accepted Start response grants
permission. An identical Start retry returns the existing state or result, never
another executable grant. A lost Start response can leave occupied capacity even
when the callback never ran; that is deliberate uncertainty.

After the callback returns a bounded JSON result, Finish commits that result and
releases capacity in the same transaction. The worker retries an uncertain Finish
using the identical source and output. A later activation loads the retained
result by original occurrence, without dispatching the callback again. This is a
separate journal from `ctx.local`; do not add a second local-result commit.

A callback exception, cancellation, invalid result or lost process supplies no
false Finish. Even if application code catches the exception, capacity remains
occupied until qualifying completion evidence or explicit release. Late Finish
requires the original source and stable authenticated worker identity, may settle
retained usage after lease loss, and cannot revive a cancelled workflow. An
operator release records the exact source, inspected revision, stable request ID
and reason; it does not assert completion. A late Finish after release cannot
release capacity a second time.

This governs **one callback authorization**, not every hidden HTTP request inside
an arbitrary SDK. Disable or separately account for SDK retries, redirects,
nested requests and background tasks. Rate is measured at authorization time,
not provider arrival time. A callback's returned result is not independent proof
that an asynchronous provider business operation completed. Protected provider
composition uses the separate path below. Standalone task admission remains deferred.

## Compose admission with protected provider dispatch

An approval-bound provider adapter may include
`admission=ProviderAdmissionRequirement(resource="export-dispatch")` in its binding.
Import this type from `ledgence.worker.providers`. The resource is part of the
reviewed immutable intent, alongside the effective arguments and pinned program.
This is binding version 2; ungoverned version-1 bindings keep their original
encoding and behavior. Do not add admission after an approval has been accepted.

In the approved continuation, return
`ctx.request_provider_admission("export-capacity", resume=Entry.DISPATCH,
state=None, timeout_ms=60_000)`. The backend derives the subject from the original
accepted approval; application code does not supply a replacement ticket or action.
After the capacity wait, `await ctx.admitted_provider_operation()` returns a typed
`ProviderAdmissionOutcomeKind` from `ledgence.worker.provider_admission`:

| Kind | Meaning |
| --- | --- |
| `TICKET` | The protected operation has a durable ticket. Continue with `ctx.wait_provider_operation(...)` to learn its business result. |
| `WAIT_REQUIRED` | Return `ctx.wait_admission(...)` for the existing occurrence. |
| `NEEDS_REVIEW` | Permission or usage is uncertain. Inspect or wait; do not dispatch again. |
| `CLOSED` | Admission ended before a ticket became available. No business success is implied. |

The first successful combined Start commits resource usage, the provider's
one-shot permission and its recovery obligation together. A retry never grants
another call. Original approval and effect identities survive the new capacity
wait activation. An equal existing business/provider identity returns its
existing ticket and releases the unused offer without charging a new rate token.
If that ticket belongs to another workflow, return or inspect the reference;
it cannot be adopted as this workflow's native provider wait.

If an offer expires before the combined grant commits, including while waiting
for database locks, Ledgence rolls back tentative grants and rate charges and
returns `WAIT_REQUIRED` while the activation remains authorized. Rewait the same
occurrence; its original deadline remains fixed. Offer expiry is separate from
loss of the task lease or worker authorization, which still rejects the request.

A governed dispatch callback must explicitly separate **call completion** from
**business knowledge**:

```python
from ledgence.worker.providers import ProviderDispatchOutcome, ProviderReport

# A fully received HTTP 202 completes this call; the remote export is pending.
return ProviderDispatchOutcome.completed(
    ProviderReport.pending(target=request.target, provider_reference=export_id)
)
```

Use `ProviderDispatchOutcome.unknown(report)` when the original call's completion
is uncertain. The worker freezes this envelope before reporting it. A plain
`ProviderReport`, an exception or an invalid envelope cannot release occupancy.
The combined Report stores the provider evidence and justified capacity release
in one transaction. Full provider results stay in the provider ledger; they do
not enter the smaller generic admission result field. Retried reports preserve
both the business fact and the completion classification exactly.

| Accepted evidence | Original call occupancy | Business recovery |
| --- | --- | --- |
| Completed call + pending report | Released | Continues through finite probes. |
| Unknown call + successful report | Retained | Success does not prove the original call stopped. |
| Later successful probe | Unchanged | May complete the business wait. |
| Operator admission release | Released explicitly | Does not create success or another dispatch grant. |

Probes remain read-only ordinary leaf tasks and return plain `ProviderReport`.
They are **outside this resource policy** and cannot settle original call occupancy.
This limit therefore bounds protected dispatch authorizations, not the provider's
entire asynchronous business workload or all reconciliation traffic. No transaction
is held during a network call. Hidden SDK retries still require application control.

A frozen original completed-call report may settle usage after lease loss or
execution retirement. After an explicit operator release it is retained without
releasing capacity twice. An already accepted unknown-completion report cannot be
changed to completed under the same identity; use explicit operator release when
no qualifying original report is available. Business assessments and observations
remain separate from admission release.

Enable both admission and provider policies with the same audience on every
replica. Workers require the complete `provider_admission.v1` capability; support
for the two separate capabilities alone is insufficient. Either admission
quarantine or provider declared-restore quarantine blocks fresh combined Starts.
Neither fence discards late evidence. Generic Start/Finish and ordinary provider
Claim/Report cannot bypass a governed binding.

The [approved export example](../examples/provider-admission/README.md) shows the
four entrypoints, typed routing and a provider that records received calls and
CSV effects independently.

## Restore and operate

Before serving a restored database, configure `dispatch: "quarantined"` on every
API replica. This external setting blocks governed starts, including new keys
missing from the restored snapshot. The database cannot infer grants accepted
after its backup. Reconcile external usage before deliberately enabling dispatch.
Leases and timeouts never establish that unknown remote work stopped.

The recovery loop scans bounded due work. Resource-local locks avoid serializing
unrelated resources, and no transaction spans a callback or network request.
Ordinary ungoverned executions skip admission maintenance using a persisted marker.
Recovery participates in readiness and remains supervised through HTTP shutdown.
Admission occurrences, frozen bindings and accepted results have no automatic
expiry in this initial capability, even after completion or execution retirement.
Resource records and operator policy/release command receipts also remain stored.
The execution-retention interval does not erase these records. Plan storage for
their continued growth; releasing capacity does not delete the retained evidence.

## HTTP, CLI, Python and MCP

All HTTP operations are POST with strict bounded JSON:

| Route | Authority | Contract |
| --- | --- | --- |
| `/v1/resources/put` | `resource_control` | `ResourcePutCommand` → `ResourcePutReceipt` |
| `/v1/resources/query` | `metadata_read` | `ResourceQuery` → `ResourceSnapshot` |
| `/v1/resources/list` | `metadata_read` | `ResourceListQuery` → `ResourcePage` |
| `/v1/admissions/start` | Worker | `AdmissionStartCommand` → `AdmissionStartReceipt` |
| `/v1/admissions/finish` | Original worker actor | `AdmissionFinishCommand` → `AdmissionFinishReceipt` |
| `/v1/provider-admissions/start` | Worker with live lease | `ProviderAdmissionStartCommand` → `ProviderAdmissionStartReceipt` |
| `/v1/provider-admissions/report` | Original worker actor | `ProviderAdmissionReportCommand` → `ProviderAdmissionReportReceipt` |
| `/v1/admissions/inspect` | Audited `sensitive_read` | `AdmissionQuery` → `AdmissionSnapshot` |
| `/v1/admissions/release` | `resource_control` | `AdmissionReleaseCommand` → `AdmissionReleaseReceipt` |

CLI groups are `resource list|inspect|put` and `admission inspect|release`.
Mutations take a saved file with format `ledgence.admission.command.v1`, exact
`base_url`, and `body` containing the command as a JSON string. Retain the same
file after a lost reply; do not regenerate its request ID. Endpoint and scope
are checked before sending. Use `--help` for required fields.

Python exposes `client.resources.put/get/list` and `client.admissions.inspect/release`,
with typed `ResourcePolicy`, `ResourceRate` and `AdmissionOccurrence` values.
Resource/list metadata contain no callback arguments or results.

MCP adds `ledgence_resource_list`, `ledgence_resource_inspect` and
`ledgence_admission_inspect`. `--allow-resource-control` explicitly exposes
`ledgence_resource_put` and `ledgence_admission_release`; it conflicts with
`--read-only`. Worker Start/Finish and composed provider Start/Report are not MCP tools. API credentials remain the
authority even when a tool is enabled. MCP session scope cannot be overridden in arguments.

Resource-policy audit events can be queried with:

```sh
ledgence audit list --resource support-model \
  --server http://127.0.0.1:8080 --tenant acme --namespace demo \
  --credentials-file operator.json
```

Choose only one of `--resource`, `--task` or `--workflow`. The resource filter
selects policy events; admission release and sensitive-inspection events belong
to the original workflow and use `--workflow WORKFLOW_ID`. Queries require
`metadata_read` in credential mode. Preserve the filter when following a cursor.

MCP's `ledgence_audit_query` accepts
`filter={"kind":"resource","resource_kind":"resource","resource_id":"support-model"}`.
In an existing asynchronous Python client context, the equivalent typed query is:

```python
from ledgence.client import AuditFilter, AuditResourceKind

page = await client.audit.list(
    filter=AuditFilter.resource(AuditResourceKind.RESOURCE, "support-model"),
)
```

Console C9 renders each wait episode and its resumed entrypoint. An accepted
operation result has its own occurrence-bound inspector. Offer or Start alone
does not fabricate an executed operation. Graph metadata does not expose arguments
or results. Protected dispatch has a typed provider record and separate admission and business-result waits; it does not fabricate a generic accepted-operation result. The scope-free `/v1/console/admissions/inspect` wrapper binds the
installation scope and audits the sensitive response.

## Validation

The focused gate is `tools/check-resource-admission.py`. It creates and removes
its own database and temporary credentials on a disposable PostgreSQL 18 server.
It exercises real orchestrator/worker processes and a local classifier, recording
HTTP calls independently of Ledgence's state. `--endpoint` runs the same cases
through an owned loopback ElasticMQ queue. No provider account or model key is used.
The gate covers parking/restart, ordinary work during a wait, competing capacity,
lost Start/Finish replies, offer expiry, result replay after worker loss and restore
quarantine. PostgreSQL tests cover transactional races, policy changes and late reports.
`--provider` runs the composed export cases, including approval identity across parking, unknown occupancy despite successful probes, duplicate business tickets and original reports after execution retirement. Both delivery modes use the same production paths. These checks do not establish a production throughput benchmark or real AWS SQS qualification.
