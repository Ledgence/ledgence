# Recoverable provider operations

Available in Ledgence 0.8.0. Use matching server, worker, helper and Console
components, apply the database migrations and enable the policy explicitly.

A provider operation separates permission to issue a mutation, the provider's
reported state, and the workflow's continuation. Ledgence durably grants the
initial mutation once, then schedules finite read-only checks. A timeout,
missing result or `pending` response never authorizes another mutation.
Provider adapters remain application code. The platform has no mandatory
provider SDK or hosted service.

## Enable it explicitly

Use the authenticated installation described in [instance access](instance-access.md).
The orchestrator additionally requires `--provider-config provider.json` with
the following policy file.

```json
{
  "v": 1,
  "audience": {
    "authority_id": "your-existing-authority",
    "instance_id": "your-existing-instance",
    "scope": {"tenant_id": "acme", "namespace": "billing"}
  },
  "dispatch": "enabled"
}
```

Copy the existing installation identifiers into the policy file. Authority
creation remains part of instance setup. Startup validates the exact audience
and the complete provider, retained-evidence, operator-resolution and audit
capabilities before listening.
Absent configuration disables provider operations and their scheduler. The file
is bounded to 16 KiB, rejects unknown fields and must be a regular local file.

Run matching authenticated workers on the workflow queue and a **separate
reconciliation queue**. Only workers supporting provider probes should consume
that queue. This version does not provide general worker eligibility routing.
An incompatible worker fails before entering the registered application callback.
Its allocated check still consumes the finite budget and may require operator review.

With SQS or ElasticMQ, configure an external delivery route for both logical
queues. Each orchestrator delivery configuration routes one queue, so run one
publisher process per queue against the same database and authority. Both
processes need the same provider policy and authenticated access configuration.
Use the matching external delivery configuration on each worker. The
[provider recovery example](../examples/provider-recovery/README.md#run-the-local-demonstration-from-the-checkout)
includes a local ElasticMQ check of probe delivery and a lost report reply.

## Review the whole immutable binding

A workflow requests an approval with `protection="provider_operation"` and a
`provider_binding`. The server resolves the owning workflow's already pinned
program descriptor into that binding before review. Dispatch and reconciliation
callables must be registered in that same package. Resolution never selects a
newer package version during recovery.

The binding records the complete intent and its operating limits.

- A business namespace and key for the intended operation.
- Adapter identity, version, digest, completion criterion and registered callables.
- Provider, environment, account, API variant, operation, endpoint identity and
  adapter-specific target dimensions.
- Optional provider deduplication domain, key and its declared protection horizon.
- A credential reference, without embedding the credential value.
- The dedicated queue, finite check budget, deadline and retry timing policy.

`action.arguments` is the sole effective request. The canonical binding digest
includes those exact arguments and the resolved binding. Changing an integer to
a float, a target account, adapter version, policy or program digest changes the
binding. Approval decisions must echo it exactly. Older generic local-result and
approved-effect operations cannot bypass this request-time protection.

Business identity distinguishes intentional operations, even when they have
identical parameters. Reusing one business key requires the original binding.
Storage reserves the business key and, when supplied, the provider's declared
domain/key in the same transaction
as the shared dispatch grant. An equal duplicate returns the existing ticket.
A different binding conflicts. A workflow cannot adopt another workflow's ticket
as its own native wait.

## Dispatch, report and wait

`ctx.approved_provider_operation()` executes the registered dispatch only when
it receives the original grant. It returns a durable ticket. An exact retry or
replacement attempt returns the existing ticket without dispatching again.
The worker freezes the source identity and report ID before entering the callback.
Losing the grant response may therefore produce zero mutations. Recovery
preserves the consumed grant even when delivery is uncertain.

The callback returns a typed report with `unknown`, `pending`, `requires_action`,
`succeeded`, `failed` or `cancelled`. A successful JSON `null` is a real terminal
result. A Python exception, transport error, malformed result or lost worker is
an execution failure. Provider failure requires a classified report. The adapter
is responsible for classifying provider responses and satisfying its declared completion
criterion. Ledgence authenticates the reporting worker and binding. External
state remains an assertion made by the adapter.

Every outcome uses `ctx.wait_provider_operation(...)` and a new workflow
activation. A terminal dispatch report can make the wait immediately eligible.
No provider result is written into the original activation's local journal.
`journal_state="unconfirmed"` remains literal for these occurrences and must not
be read as a provider-pending flag. Use the separate provider knowledge,
reconciliation and handling fields instead.

Native provider waits have a separate durable namespace. A generic event with
the same text key cannot satisfy or poison them. Wait registration, selected
wake and resumed activation are bound to their exact occurrence and parking
activation. The selected wake is immutable. A follow-up wait can use
`after_effect_revision` to wait for a new selectable outcome without repeatedly
resuming on the same review condition.

## Finite read-only reconciliation

The initial grant also records the first recovery obligation. Its defaults and
supported bounds are listed below.

| Setting | Default | Supported range |
| --- | --- | --- |
| Initial delay | 30 seconds | 1 second–24 hours, before the deadline |
| Base interval | 5 seconds | 1 second–24 hours |
| Maximum interval | 5 minutes | At least the base interval, at most 24 hours |
| Maximum checks | 12 | 1–256 |
| Total recovery window | 1 hour | 1 minute–7 days |
| One probe invocation | 10 seconds | 1–30 seconds |

The first due time and absolute deadline are anchored at the grant. Read-only
checks may overlap an original mutation still in flight. A negative or missing
lookup never grants another write. When lookup is unavailable, or a by-reference
adapter has no reference, recovery selects `needs_review` without allocating a
dummy check. A later original terminal report can still settle an unselected wait.

A check is a normal leaf task with trusted provider metadata, the exact pinned
program, a 60-second attempt and one attempt maximum. The host applies the shorter
probe budget immediately before invoking the callback, after preparation, and
requires an additional 10 seconds of remaining attempt time for reporting.
The original workflow's dispatch callback retains its ordinary attempt bound.
Adapters should also configure their provider socket timeouts.

The host reports the frozen callback result durably, then settles the internal
check task with a small receipt. A lost report reply retries the frozen report
while preserving the completed callback result. Retry intervals grow exponentially
up to the configured cap, with a deterministic
80–100% spread (never below one second). The accepted due time is persisted and
clamped to the absolute deadline. The next generation is allocated only after the
previous task's actual cleanup/terminal state is persisted. Task terminalization enqueues a
provider finalization obligation. Active checks remain dormant between these
events and their absolute deadline. Preparation failures, timeouts and lost
workers consume their allocated check. Exhaustion, deadline, unsupported lookup, required user action or a
permanent recovery failure yields an explicit review outcome.

## Conflicts, assessments and cancellation

An accepted terminal fact is never downgraded by a later pending or unknown
report. Distinct terminal facts or incompatible nonnull provider references are
retained and exposed as disputed. The first reference is kept for read-only lookup.
A reference conflict stops automatic checks and preserves that original reference. Duplicate
reports of the same canonical fact do not create a new conflict simply because
they arrived later. Provider references are part of that comparison.

Operators use the existing [effect assessment](effect-evidence.md#record-an-operator-assessment)
command with `effect_resolve`. Its expected revision covers provider evidence and
recovery state as well as prior assessments. Acceptance, verified audit, stopping
future checks and eligible continuation work commit together. A new conflicting
terminal fact can challenge a recorded success or failure. A repeated historical
fact already covered by an assessment does not challenge it again. `close_unknown`
closes handling without asserting a provider outcome.

Cancellation stops new checks and cancels active ones through the ordinary
cleanup protocol. It does not undo a provider write. A surviving independent
worker caller can submit its one frozen report through the evidence-only
observation route after lease expiry or execution collection. The retained source
and current stable worker identity must match. Observation authority covers only
delivery of that frozen report. The original lookup budget, execution ownership
and cancellation remain in force. Managed processes killed before reporting
cannot promise late delivery.

If an evidence-only observation wins a race with a live report, the live caller
receives the original evidence-only receipt. It can settle its still-owned probe
as recorded elsewhere. The receipt retains its original evidence-only disposition.

## Declared restore quarantine

Set `dispatch` to `declared_restore_quarantine` before starting a restored
installation. The storage grant boundary refuses new provider mutations,
including keys absent from the restored database. Known retained operations can
still be inspected and reconciled read-only. Repeating a known claim does not
restore its original dispatch permission.

This fence requires an explicit operator declaration. A backup can omit a
mutation that happened later. Ledgence cannot detect that fact
using only the restored database and its clock. Validate external state and
provider protection windows before intentionally enabling new dispatch. Declared
provider horizons describe the adapter's knowledge of that provider's protection.
They grant no permission for automatic mutation retries.

## Inspect, retain and operate

Provider operations appear in the existing Effects inbox and detail API. The
optional `provider_filter` selects all, pending, needs-review, succeeded, failed,
cancelled or disputed provider occurrences. Ordinary effect queries keep their
existing behavior. Pending means unresolved unknown/pending knowledge with an
active recovery schedule. It excludes stopped cancelled/failed workflows.
Needs-review describes current handling: a clean operator assessment removes a
historical dispute from that queue. The disputed filter still includes the
retained disagreement, and new evidence that challenges the assessment returns
the occurrence to needs-review.

The single paginated timeline contains dispatch/source admission, probe scheduling,
typed reports, finalization and recovery-stop records alongside operator
assessments. Detail inspection remains bounded and audited before payload bytes
are returned. Claims and reports preserve worker provenance separately from
verified operator audit. Historical lease authority is never exposed by these
operator projections. Console contract 7 displays provider waits and probe task
roles without inventing a workflow dependency from timestamps.

The generic worker API exposes five routes.

- `POST /v1/workflows/provider-operations/claim`
- `POST /v1/workflows/provider-operations/report`
- `POST /v1/workflows/provider-operations/observe`
- `POST /v1/provider-probes/context`
- `POST /v1/provider-probes/report`

They require authenticated worker authority. Context admission persists the
callback source before execution and therefore performs a durable write. Public read
routes remain the existing approved-effects inbox and inspector. Generic HTTP
uses exact unsigned 64-bit numbers. Console and MCP project platform counters
and revisions as canonical decimal strings.

The provider ledger, immutable reports, business/provider key reservations and
exact receipts survive execution retention with no automatic expiry. A terminal
provider outcome or verified operator assessment releases the unresolved-effect
hold independently of local journal confirmation. Cancellation alone does not.
Actual task cleanup, terminal age and other ordinary retention protections still
apply. Plan capacity and backup retention for these records.

Disabled provider recovery adds no provider lookup to ordinary task transitions.
The nullable native-wait foreign key has a fixed PostgreSQL relation/constraint
metadata cost even for ordinary workflows. Enabled recovery performs a bounded
indexed due scan even when idle. Only marked provider tasks add
source/report/finalization work. These costs support the durability contract.
Production throughput still requires measurement under the intended workload.

## Development: share dispatch capacity

The development checkout adds optional resource admission to the reviewed
provider binding. This extension is not in 0.8.0. A version-2 binding names the
resource before approval; version-1 bindings retain their published behavior.
Use the composed workflow methods and explicit call-completion envelope described
in [resource admission](resource-admission.md#compose-admission-with-protected-provider-dispatch).
A new admission activation preserves the original approved effect identity.

A completed dispatch call can release capacity while the provider's business
operation remains pending. Conversely, a successful probe does not prove the
original call stopped and cannot release its uncertain occupancy. Reconciliation
probes remain outside the dispatch resource policy. The existing ordinary
provider claim/report paths reject governed dispatch sources; use the composed
Start/Report capability. [Runnable example](../examples/provider-admission/README.md).
