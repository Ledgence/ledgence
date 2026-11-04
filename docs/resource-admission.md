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

A policy has `enabled`, `max_occupancy`, optional `rate`, `offer_timeout_ms`,
`start_timeout_ms` and optional [`fair_sharing`](#weighted-offers). `rate` is a token bucket `{numerator, period_ms, burst}`:
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
a forward jump can refill to the configured burst. Each resource offers capacity using FIFO by default or [weighted queue groups](#weighted-offers) when configured. A [run budget](run-budgets.md) can also bound governed calls, delegation and their deadline across a root and its children. Monetary ceilings and ordering across resources are outside this contract.

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
composition and standalone tasks use the separate paths below.

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
| `/v1/resources/decisions` | `metadata_read` | `ResourceDecisionQuery` → `ResourceDecisionPage` |
| `/v1/admissions/start` | Worker | `AdmissionStartCommand` → `AdmissionStartReceipt` |
| `/v1/admissions/finish` | Original worker actor | `AdmissionFinishCommand` → `AdmissionFinishReceipt` |
| `/v1/provider-admissions/start` | Worker with live lease | `ProviderAdmissionStartCommand` → `ProviderAdmissionStartReceipt` |
| `/v1/provider-admissions/report` | Original worker actor | `ProviderAdmissionReportCommand` → `ProviderAdmissionReportReceipt` |
| `/v1/admissions/inspect` | Audited `sensitive_read` | `AdmissionQuery` → `AdmissionSnapshot` |
| `/v1/admissions/release` | `resource_control` | `AdmissionReleaseCommand` → `AdmissionReleaseReceipt` |

Resource lists are capped at 256 KiB. A response can contain fewer than the requested number of records and still have another page. Follow `next` until it is null.

CLI groups are `resource list|inspect|decisions|put` and `admission inspect|release`.
Mutations take a saved file with format `ledgence.admission.command.v1`, exact
`base_url`, and `body` containing the command as a JSON string. Retain the same
file after a lost reply; do not regenerate its request ID. Endpoint and scope
are checked before sending. Use `--help` for required fields.

Python exposes `client.resources.put/get/list` and `client.admissions.inspect/release`,
with typed `ResourcePolicy`, `ResourceRate` and `AdmissionOccurrence` values.
Resource/list metadata contain no callback arguments or results.

MCP adds `ledgence_resource_list`, `ledgence_resource_inspect`, `ledgence_resource_decisions` and
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
selects policy events. Workflow release and inspection records use
`--workflow WORKFLOW_ID`; standalone task records use `--task TASK_ID`. Queries require
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

Console C13 renders each wait episode and its resumed entrypoint. An accepted
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
`--provider` runs the composed export cases, including approval identity across parking, unknown occupancy despite successful probes, duplicate business tickets and original reports after execution retirement. `--task` runs standalone tasks through cold preparation, shared task/workflow capacity,
unused deferral, worker loss, uncertain Start, broker compatibility and late closure.
Each run needs a new evidence directory. These checks do not establish a production
throughput benchmark or real AWS SQS qualification.

## Standalone task admission

A root task can require **one existing resource in its own scope**. This is an
optional part of the immutable submission, outside CloudEvent `data`. Omitting it
preserves ordinary delivery. Reusing an idempotency key with a different resource
conflicts, even if the application data is identical. Governed workflow submissions,
child tasks, controller activations and recovery probes are outside this contract.

```python
from ledgence.client import TaskAdmission

task = await client.tasks.submit(
    program="hello-ledgence", version="0.1.0", queue="demo",
    data={"name": "Ada"}, idempotency_key="hello:ada:1",
    admission=TaskAdmission(resource="support-model"),
)
```

Publish the task and create the named resource first. See the [complete submission
file](../examples/resource-admission/submit_task.py). The CLI `task submit` file
uses `input.admission: {"resource":"support-model"}`; the MCP task submission tool
accepts the same `admission` object. Workers negotiate `task_admission.v1` in an
authenticated session. A worker or adapter without this capability cannot silently
run governed work through ordinary dispatch.

| Stage | What happens |
| --- | --- |
| Waiting | Task state is `queued`. It owns no attempt or worker process while waiting for its first offer. The wait has no separate admission deadline; cancel it if it is no longer wanted. |
| Offered / preparing | An offer reserves shared capacity. Acquisition binds it to one attempt and permits bounded artifact/process preparation. It does not authorize the handler. |
| Start | Once the runtime is ready, only the first committed grant can invoke the handler. Start charges rate once. A duplicate or lost response never grants another invocation. |
| Deferral before Start | A definitive closure fences the old attempt and requeues it with backoff. `attempt_count` preserves assigned generations; `admission_deferrals` counts unused deferrals. Retry use is their difference. Actual preparation failures and accepted Starts still consume retry budget. |
| Close / uncertainty | Authenticated confirmation from the original worker can release the exact source after local invocation quiescence. Task result, lease expiry, cancellation or worker disappearance alone do not establish this. An uncertain source can block later attempts until reconciled or explicitly released. |

The governed unit is **entrypoint invocation**, not module import or external
side effects. Put governed work inside the handler: Python imports application
modules before the runtime reports ready. A healthy process can remain warm after
a handler completes. Quiescence does not prove a detached thread, remote API
request or external business effect stopped. This is not exactly-once execution.
Under repeated policy changes unused attempts retain history. Generation uses the
positive CloudEvents integer range (through 2,147,483,647); exhausting it closes
the task explicitly instead of recycling an old identity.

Inspect `(scope, task_id, generation)`; the initial queued generation is 1.
`client.task_admissions.inspect(task.id, generation=1)`, `ledgence admission
inspect --task ID --generation N` (with the usual server, scope and credential
flags), `ledgence_task_admission_inspect`, and Console's **Resource admission**
view expose the ledger. `POST /v1/task-admissions/inspect` requires audited
`sensitive_read`. Workers use the private-authority `sessions`, `start` and
`close` endpoints under `/v1/task-admissions/`; these are not submission APIs.

Results and source closure are persisted independently: a Close outage does not
prevent an otherwise valid result from being recorded. An occupied source keeps
the owning task discoverable even if its ordinary retention period has elapsed.

After inspecting the exact source, an operator can explicitly accept the risk
of freeing capacity via `client.task_admissions.release(inspected,
request_id=..., reason=...)`, `ledgence admission release --file FILE`, or MCP's
opt-in `ledgence_task_admission_release`. `POST /v1/task-admissions/release`
checks source, inspected revision, request ID and reason. Preserve the original
command for uncertain acknowledgement. Release does not stop remote work, change
the task result, or refund the rate charge.

For mixed-version broker fleets, pre-A3 workers can read dispatch generations
1 through 1,000. They stop consuming on larger generations before contacting
the server. Upgrade all consumers on a shared broker destination, or route
governed work to a queue served only by matching workers, before allowing
repeated capacity deferrals to exceed this limit. Integrated HTTP acquisition
excludes governed assignments for older sessions.

## Cluster-wide queue concurrency (development)

A queue policy sets a shared maximum across worker sessions. New attempts reserve a slot through the same PostgreSQL transaction on integrated HTTP and external SQS claim paths. Console C13 exposes **Queues** for credential-bound operators. [Policy semantics, timeouts and operator recovery](queue-concurrency.md).

## Weighted offers

**Development capability.** A resource may optionally assign queue groups weighted turns when choosing the next reservation. FIFO remains the default when `fair_sharing` is omitted. Explicit JSON null is invalid. Old FIFO policies preserve their wire representation and request digest.

```json
{"enabled":true,"max_occupancy":5,"rate":null,"offer_timeout_ms":30000,"start_timeout_ms":5000,"fair_sharing":{"groups":[{"name":"support","weight":3,"queues":["support"]},{"name":"batch","weight":1,"queues":[]}],"default_group":"batch"}}
```

A task's submission queue or an operation's owning workflow queue selects a group. Every unmapped queue goes to the same configured default. A policy supports 1–16 named groups, weights 1–1000, at most 64 distinct queue mappings in total and a 12 KiB encoded fair-sharing configuration. Ledgence divides all configured weights by their greatest common divisor, so 300:100 has the same cycle as 3:1 even while a group is idle. Group order breaks ties. Each round visits eligible groups in configured order whose normalized weight reaches that round. Empty groups are skipped, and each group serves its oldest eligible request. With two continuously eligible groups at 3:1, each complete cycle grants three offers to the first and one to the second. A sole eligible group can use the entire resource.

A committed offer advances the durable cursor in the same PostgreSQL transaction that reserves capacity and appends the decision. Failed or rolled-back reservations do not advance it. An expired, previously committed offer remains in history. Start still controls dispatch and rate debit. Shares count offers. Running work is never preempted, and unequal operation durations can produce unequal occupancy time. Worker poll plans independently govern which queues each free consumer polls.

Capacity and rate edits preserve the rotation. Scaling all weights proportionally or reordering queues within a group also preserves the cursor. A change to the effective weights, group order or queue assignments starts a new rotation and reclassifies pending work under the resource lock. Previously granted work keeps its evidence. Removing `fair_sharing` returns to FIFO and retains prior decisions. Apply the migration and upgrade all orchestrator replicas before enabling weights. Creating the group indexes and changing membership on a large live backlog can delay admissions; rehearse these operations against a representative restore.

Read decisions with `POST /v1/resources/decisions` using `{scope,name,before,limit}`. Start with `before: null`, then pass the returned `next` to read older offers, newest first. A sequence is scoped to one resource and serializes as a positive decimal string. Each row records the policy revision, source queue, subject, grant generation, selected group and its configured weight, eligible-group count, normalized round, reason and offer time. This journal is retained with its resource and grows with committed weighted offers. Include its storage in capacity planning. Console **Resources**, `ledgence resource decisions`, Python `client.resources.decisions` and MCP `ledgence_resource_decisions` expose this metadata to `metadata_read` operators.

To reproduce the FIFO comparison, run `python tools/check-resource-fairness.py` from the source checkout with a newly built native binary directory, the installed development Python client interpreter, a disposable PostgreSQL configuration and an unused evidence directory. See `--help` for arguments. Its evidence separates admission wait, observed parallelism and completion throughput across a two-coordinator restart.
