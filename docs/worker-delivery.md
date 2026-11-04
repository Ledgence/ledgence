# Worker delivery

`ledgence-worker-delivery` connects an existing `Worker` to `Arc<dyn TaskService>`. It manages service registration, bounded acquisition, dispatch permission, lease monitoring, local execution, and durable report reconciliation. It is a Rust library, composed with the HTTP client by `ledgence worker connect`. See the [HTTP quickstart](http-orchestration.md#run-a-task). The `ledgence worker run` command also supports local fixtures. Acquisition uses bounded long polling by default, with an explicit immediate mode.

## Composition

Build a `Worker` with `WorkerConfig`, a `ProgramStore`, an `ArtifactCache`, and an `ExecutionRuntime`. Configure concurrency there once. The driver derives its N consumer identities from `Worker::concurrency()` and reserves that worker's existing capacity before each acquisition.

An embedding executable supplies the service implementation:

```rust
use ledgence_orchestration_api::{ContractError, Scope, TaskService};
use ledgence_worker_core::Worker;
use ledgence_worker_delivery::{DeliveryConfig, DeliveryDriver, DeliveryHandle};
use std::sync::Arc;

fn start_delivery(
    worker: Worker,
    service: Arc<dyn TaskService>,
) -> Result<DeliveryHandle, ContractError> {
    let scope = Scope {
        tenant_id: "tenant_acme".into(),
        namespace: "billing".into(),
    };
    let config = DeliveryConfig::new(scope, "python-billing");
    Ok(DeliveryDriver::new(worker, service, config)?.start())
}
```

Call this inside a running Tokio runtime. The driver owns worker shutdown; construct a new `Worker` to restart after it finishes. In-process integration tests supply `ApplicationService` backed by PostgreSQL. The driver itself does not import SQLx, a PostgreSQL adapter, or an HTTP client. A service adapter must uphold the [delivery contract](delivery-contract.md), including commit before acknowledging mutations.

The service binds a program descriptor at submission. The driver passes the acquired descriptor and entire CloudEvent to the worker, which downloads the package only when its verified cache lacks it. The driver never resolves a program label again and never changes user-owned `data`. Programs must still make external effects idempotent across distinct attempts.

## Configuration

`DeliveryConfig` contains scope, queue, and timing controls. It has no independent concurrency field.

| Field | Default | Meaning |
| --- | --- | --- |
| `acquire_wait` | 20 seconds | Maximum acquisition wait; zero requests immediate completion |
| `idle_delay` | 1 second | Minimum Empty acquisition cycle, including time already spent waiting |
| `retry_delay` | 250 milliseconds | Delay before retrying an uncertain or busy service operation |
| `request_timeout` | 30 seconds | Maximum time waiting for one control exchange; timeout leaves its outcome uncertain |
| `renew_interval` | 15 seconds | Delay after an acknowledged renewal before the next one |
| `session_extend_interval` | 1 hour | Delay between successful session extensions |

`acquire_wait` permits zero through 20 seconds. Other durations must be positive and representable. Request timeout cannot exceed 30 seconds, renewal interval cannot exceed 15 seconds, and session extension interval cannot exceed half the 24-hour session validity. These are local exchange and scheduling controls; they do not change the service's lease, execution, or session policy.

A full acquisition wait can be followed immediately by the next sequence after a committed Empty. If Empty arrives early, the driver waits only the remaining `idle_delay` cycle. Replaying that completed sequence still returns Empty. A service failure retains its original sequence and retry backoff. `acquire_wait` is a timing option and does not change N.

## Attempt ownership

Each consumer keeps its capacity reservation while an acquisition outcome is uncertain. During shutdown it can cancel its local long-wait future and reconcile the same sequence with zero wait; that cancellation never proves the server rolled back. It repeats the same command rather than advancing its cursor. An assigned task must receive dispatch authorization before preparation and local execution. The single-use reservation prevents the driver from dispatching that local attempt again after an uncertain reply.

A newly acquired assignment starts in `AwaitingAuthority`. Its first valid Dispatch renewal creates the execution control using that renewal's request-start time and fresh remaining budget. The acquisition hold does not consume a newly claimed attempt's local budget. Initial confirmation is bounded to 30 seconds, and cancellation/expiry is monotone. The lease monitor then runs during preparation, startup, execution, and settlement. `LeaseTracker` charges exchange latency from request start, applies the safety margin, and preserves the fixed execution deadline. Expiry, cancellation, or loss of authority stops user work permanently for that attempt. Delayed replies cannot revive it. Stopping user work does not abandon cleanup or a pending result.

Renewal retries reuse their sequence and intent. Settlement retries preserve the entire command, including operation ID, report, quiescence, and processing trace. Once accepted, an unconfirmed report is finalized with a separate cleanup confirmation; its original bytes are never replaced. With an enabled trace bridge, the driver captures its processing span once and freezes that context in the report. Disabled tracing leaves it absent.

Before dispatch, the driver checks assignment identities against the consumer and event, including the attempt number/generation. Generated event/run/task/attempt IDs are limited to 128 bytes and event source to 2,048 bytes. Before the first settlement exchange, it validates the complete report against the 8 MiB contract. Oversized output, business failure text, or adapter errors become a compact `Protocol` failure with the original execution identity and a conservative started flag. This is a terminal failure under the current retry policy; the driver does not truncate an output or rerun the program to obtain a smaller result.

Local `tracing` spans correlate consumer operations by scope, queue, session, and consumer ID, and attempt operations by task, attempt, lease, event, and program digest. Accepted and rejected reports are logged without logging application data. An optional [OTel adapter](observability.md) establishes producer/consumer parentage and exports traces independently of diagnostic log filtering.

Successful execution can leave a healthy warm subprocess while its invocation is quiescent. If local work or required cleanup remains outstanding when execution returns, the driver sends an unconfirmed report and stops the whole worker. This lets the existing worker shutdown owner advance cleanup concurrently with report reconciliation. A subsequent confirmation is sent only after the reservation becomes quiescent.

## Shutdown and diagnostics

`DeliveryHandle::stop()` requests shutdown. `shutdown(timeout).await` requests shutdown and waits up to that duration. A `ShutdownPending` result retains the same supervisor and reservation ownership; retry the wait while keeping the Tokio runtime alive. Dropping the handle also requests stop, and dropping a wait future does not abort the supervisor. The timeout limits the caller's wait; it is not an adapter cleanup deadline.

For an embedding application that wants bounded status updates while it waits:

```rust
use ledgence_worker_delivery::{DeliveryHandle, DeliveryStatus};
use std::time::Duration;

async fn stop_delivery(handle: &mut DeliveryHandle) -> DeliveryStatus {
    loop {
        match handle.shutdown(Duration::from_secs(30)).await {
            Ok(status) => return status,
            Err(pending) => {
                eprintln!("Delivery shutdown is pending: {:?}", pending.status);
            }
        }
    }
}
```

The driver uses `Worker::shutdown_until_quiescent()` and keeps that cleanup operation running while consumers reconcile assignments and reports. A handle wait timeout or status check does not cancel an adapter's `close` future. If cleanup returns an error, the session remains quarantined and the driver retries after a short delay. It cannot finish while required local cleanup is unconfirmed. A protocol conflict or invalid response stops admission but does not prove that an uncertain acquisition, result, or cleanup operation can be discarded. An unavailable service or an adapter without recoverable cleanup may therefore leave shutdown pending indefinitely.

The existing `Worker::shutdown(grace, cleanup)` API provides explicit grace and cleanup budgets for local callers. `shutdown_until_quiescent()` cancels work immediately and imposes no cleanup deadline; an adapter that never completes can keep it pending. Both retain their supervisor if the caller stops waiting.

`status()` exposes the session ID, stopping/finished flags, accepted-settlement and lost-attempt counters, and the last observed error. An accepted settlement may describe a failure or still await cleanup; `settled_attempts` is not a successful-task count. `last_error` can retain an earlier transient failure after recovery. Use `TaskService::inspect`, `inspect_attempt`, and `history` for durable task outcomes. Check `finished` when observing completion; status diagnostics do not certify external business effects.

## Restart and validation boundaries

The driver has no local durable journal. An operating-system process crash loses its in-memory cursor and pending report. A replacement worker opens a new session; service-side expiry recovery and the task's retry policy recover unfinished work. The orchestrator periodically invokes `RecoveryStore::expire_batch`; custom service compositions must schedule it themselves. A lost reply from session creation can leave an unused session that expires normally.

An expired or unknown session stops the current driver and drains owned work. It does not automatically create a replacement session or transplant old cursors. Lease expiry cannot establish that arbitrary external effects did not happen, and a recovered task may run another attempt.

Ordinary driver tests use controlled service/runtime adapters for capacity, retries, lease deadlines, and retained shutdown. The [PostgreSQL gate](postgres.md#verification) also runs real Python programs through publication, submission, dynamic download, warm reuse, and durable inspection. It injects lost replies after committed acquisition, dispatch, and settlement and verifies restart with persisted bindings/cache. Those tests validate an in-process service composition. The [HTTP acceptance gate](http-orchestration.md#verification) separately exercises actual server/worker/CLI processes and a fault proxy. The gate also exercises pending rollback, cross-replica completion hints, lost Empty replies, notification failure fallback, and accepted-wait shutdown.


## Workflow activations

An assignment may carry a workflow activation marker matching its immutable
CloudEvent. After normal dispatch authorization, the worker fetches the activation
context and uses protocol 3 interactive execution through the same consumer and
subprocess reservation. Local-result RPCs use the host's acquired lease owner;
user data cannot choose that authority. Lost acknowledgments retry the identical
record while authority remains live. A returned controller decision uses the
existing settlement and cleanup path.

Ledgence 0.8.0 workflows can declare `protection: "external_effect"` in their
approval request. The helper rejects `approved_local` for that saved request,
and the store checks the persisted wake before accepting any reserved ordinary
local-result commit. This check applies before the first dispatch grant.

Ledgence 0.8.0 workers also handle `approved_effect.claim` for an approved
workflow continuation. Unlike a local-result commit, the worker sends a claim
once; replaying a previously committed grant would permit a second write. It
checks the receipt against the live owner, approval and action before passing
the disposition to Python. A retry with no accepted result must claim again
and can only look up the outcome if permission was already consumed. The
claim binds the lookup callback before dispatch. The worker confirms the
returned record through `approved_effect.confirm`. It may retry that exact
confirmation after losing a reply. A valid reply identifies the scope,
workflow, activation, attempt, approval and journal key. See
[approved external writes](workflow-approvals.md#protect-an-approved-external-write).

Before starting a connected worker, matching 0.8.0 binaries request
`GET /v1/access`. A credentialed connection validates the worker class,
permissions, scope and expiry. When complete v2 support is advertised, the
connection retains the returned authority, instance and stable worker identity
for receipt validation. The host advertises this capability per invocation,
outside the persisted activation context.

The helper then uses `approved_effect.claim.v2` and
`approved_effect.confirm.v2`. The claim is sent once. Confirmation retries the
same frozen command while the runtime control remains live. A replay receipt
restores the accepted journal without calling the provider or appending an
observation. Original v1 and 0.7.0 helpers retain their existing protocols.
A valid legacy discovery response selects the original protocol. Invalid
credentialed discovery prevents worker startup. V2 errors never downgrade an
invocation. [Negotiated evidence protocol](effect-evidence.md).

Native operator resolution additionally requires the versioned worker feature
`native_effect_resolution.v1` and v2 effect evidence. Connected workers request
this manifest with `Ledgence-Worker-Features: 1` on the existing startup access
exchange. The host passes supported features through the outer
`runtime_features` field, preserving the earlier `runtime_capabilities` shape.
A native wait without this negotiated support fails before it can park. An
accepted wait releases the same invocation reservation and resumes only through
the durable coordinator's selected assessment. See
[native operator rendezvous](effect-evidence.md#park-without-occupying-a-worker).

#### Runtime feature discovery for Rust compositions (development)

On `develop`, `HttpTaskService::discover_worker_runtime()` returns the validated
feature manifest separately from the optional authenticated effect-evidence
binding. A valid manifest can retain unknown feature names without enabling
effect evidence. `DeliveryDriver::with_runtime_features()` carries these names
into interactive workflow invocations; adding an effect binding later does not
replace the manifest. The older `discover_worker()` method remains compatible:
it returns `None` when effect evidence is unavailable.

A feature name is not execution authority. Native effect resolution and provider
operations still require their existing evidence prerequisites, and an unknown
name enables neither operation. Credentialed discovery still rejects invalid
worker identity, scope, expiry or malformed features before starting the worker.
The exchange uses the existing request, header and response format; no additional
startup or per-task request is introduced. The production server continues to
advertise only its implemented capabilities. This change prepares Rust adapter
composition; it does not implement shared resource limits or run budgets, and is
not part of the published 0.8.0 release.

Standalone tasks do not fetch a workflow context or use local-result RPCs.
Suspension ends the controller invocation; normal process-pool reuse remains
available. Distributed children have their own ordinary task leases and may use
protocol 1 or 2 packages. See [workflows](workflows.md).

<span id="provider-operation-recovery-in-development"></span>

### Provider operation recovery

The additional `provider_operation.v1` feature requires the complete authenticated
provider capability and native effect resolution. The startup exchange negotiates
it once; ordinary tasks and earlier approved-effect paths add no provider RPC.
The workflow host sends `provider_operation.claim.v1` once. A lost permission
reply cannot authorize a second mutation. The helper recovers an existing ticket
on a later explicit invocation. `provider_operation.report.v1` retries identical
typed facts while its original attempt remains live; it does not call the provider
again. These operations do not create local-result journal confirmations.

The coordinator schedules read-only checks on the separately configured queue.
Each is an ordinary task with one attempt, a 60-second attempt budget and a
server-issued `provider_probe` marker. The marker supplies no authority by itself:
the host obtains and validates the exact context using its current worker identity
and lease. The context binds the original program digest, registered callable,
source, report ID and generation. It enters the runtime through
`ledgence.provider.probe.v1`, never through user-owned CloudEvent data.

After package preparation and process startup, the host applies the configured
1–30 second check budget (10 seconds by default). It requires another 10 seconds
of parent budget before entering the callback. Cancellation remains shared with
the parent attempt. Report delivery retries preserve the same frozen report and
do not repeat the read-only callback. A report already retained through late
observation keeps that original provenance; task settlement records
`recorded_elsewhere` under the current lease. The next check cannot be allocated
until the previous task has finalized and its cleanup is confirmed.

Run matching workers on the reconciliation queue. Unsupported helpers or missing
negotiation fail before the registered probe runs. Runtime failures remain task
failures, not evidence that the provider operation failed. A successful probe task
means its report was retained; that report may still say pending or unknown.
The original mutation callback keeps the normal workflow-attempt budget. A due
read-only check can overlap it; neither a missing result nor a lookup miss permits
resending the mutation.

In matching development builds, authenticated workers can negotiate `task_admission.v1`.
Capacity waiting occurs before assignment. A governed assignment authorizes
bounded preparation, never ordinary Dispatch. The runtime-ready grant is checked
again at the subprocess invocation frame. A lost reply does not authorize replay;
only authenticated source-bound Close or an audited operator release can free a
started source. [Standalone task admission](resource-admission.md#standalone-task-admission).

## Worker-local poll plans (development)

`ledgence worker connect --poll-plan FILE` gives one physical worker a fixed set
of integrated queues in its tenant and namespace. Use it instead of `--queue`.
There are still N consumers and at most N subprocesses across all routes. The
orchestrator registers one existing queue-bound session per route, each able
to identify the same N consumer indexes. The per-session concurrency values
must not be added together to estimate physical capacity.

The [example plan](../examples/worker-poll-plan.json) selects `stride` with
weights 7 and 3. One turn is a **fresh logical poll**. An empty response counts;
an uncertain acquire is reconciled on its original queue and sequence. It gets
no extra turn. On continuously eligible queues, Stride apportions opportunities
deterministically, and `lottery` samples in proportion to positive weights.
Neither mode specifies proportions of task completions, running time or cost.
A long poll keeps its consumer until it returns. The usual empty-poll pacing
also applies when switching queues. Choose `--acquire-wait-ms` for the latency
and request rate your service can support.

The plan is a regular UTF-8 JSON file of at most 16 KiB. It uses
`schema_version: 1`, an unsigned 64-bit `revision`, `strategy: "stride"` or
`"lottery"`, and between 1 and 64 distinct queue names with unsigned 32-bit
`weight`. Weights are relative; multiplying all weights by the same factor
leaves the distribution unchanged. Set a queue to zero to disable its next
polls. All zero weights pause new polls. The worker keeps its sessions and
current tasks while paused.

Write a complete replacement file in the same directory and atomically rename it
over the original path. The worker re-reads it about once per second. A higher
revision changes future polls. Replaying equivalent weights preserves strategy
history. A malformed file, missing file, stale revision, conflicting edit at the
same revision, or changed route membership or strategy leaves the last valid
plan in force. Check the worker's stderr for `worker poll plan loaded` or
`worker poll plan rejected`. Restart the worker to change membership or strategy.
The revision and selection history are local to this worker process; after a
restart it loads the current file afresh. A program can generate the plan file
or an embedded Rust caller can install a plan on the shared planner directly.

During shutdown, the worker drains uncertain acquisitions and assignments on their
original sessions. Session registration is queue-scoped; sessions opened before
a startup failure expire normally. Worker observations publish the physical
snapshot on the primary session, the first queue in the startup plan. Reordering queues in a live revision keeps that primary session. Other subscribed sessions represent routes,
not additional observed pools. Secondary execution slots still count toward
physical occupancy; their primary-session snapshot shows `unknown` without
task/attempt links. Use task history for those links. The final JSON report's
`route_sessions` maps each queue to its durable session ID.

To inspect individual planned polls, set
`RUST_LOG=info,ledgence_worker_delivery::polls=debug` when starting the worker.
A `worker.poll.issued` event records a fresh binding with queue, plan revision,
session, consumer and sequence. Retries keep that identity. A `worker.poll.resolved`
event reports the reconciled disposition. These local diagnostics are optional
and do not measure task duration or replace server receipts.

This mode supports integrated HTTP. Existing SQS/ElasticMQ delivery uses
`--queue` with `--delivery-config` and one external route. Poll weights are
local to one worker. Durable resource admission remains a separate limit on
whether governed work can start; see [resource admission](resource-admission.md).

## Cluster-wide queue concurrency (development)

A queue policy sets a shared maximum across worker sessions. New attempts reserve a slot through the same PostgreSQL transaction on integrated HTTP and external SQS claim paths. Console C11 exposes **Queues** for credential-bound operators. [Policy semantics, timeouts and operator recovery](queue-concurrency.md).
