# Worker delivery

`ledgence-worker-delivery` connects an existing `Worker` to `Arc<dyn TaskService>`. It manages service registration, bounded acquisition, dispatch permission, lease monitoring, local execution, and durable report reconciliation. It is a Rust library. The `ledgence-worker run` CLI still reads local fixtures; an HTTP server/client, long polling, and a delivery CLI command remain future work.

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
| `idle_delay` | 1 second | Delay after a committed Empty before the next acquisition sequence |
| `retry_delay` | 250 milliseconds | Delay before retrying an uncertain or busy service operation |
| `request_timeout` | 30 seconds | Maximum time waiting for one control exchange; timeout leaves its outcome uncertain |
| `renew_interval` | 15 seconds | Delay after an acknowledged renewal before the next one |
| `session_extend_interval` | 1 hour | Delay between successful session extensions |

Durations must be positive and representable. Request timeout cannot exceed 30 seconds, renewal interval cannot exceed 15 seconds, and session extension interval cannot exceed half the 24-hour session validity. These are local exchange and scheduling controls; they do not change the service's lease, execution, or session policy.

The current acquisition operation returns immediately. Replaying a completed Empty remains Empty, so the driver waits `idle_delay` and advances the sequence. A future long-poll implementation needs a way to wait without committing Empty or retaining a database transaction during the wait.

## Attempt ownership

Each consumer keeps its capacity reservation while an acquisition outcome is uncertain. It repeats the same command rather than advancing its cursor. An assigned task must receive dispatch authorization before preparation and local execution. The single-use reservation prevents the driver from dispatching that local attempt again after an uncertain reply.

The lease monitor runs during preparation, startup, execution, and settlement. `LeaseTracker` charges exchange latency from request start, applies the safety margin, and preserves the fixed execution deadline. Expiry, cancellation, or loss of authority stops user work permanently for that attempt. Delayed replies cannot revive it. Stopping user work does not abandon cleanup or a pending result.

Renewal retries reuse their sequence and intent. Settlement retries preserve the entire command, including operation ID, report, quiescence, and processing trace. Once accepted, an unconfirmed report is finalized with a separate cleanup confirmation; its original bytes are never replaced. The driver currently supplies no processing trace context and does not activate or export OpenTelemetry spans.

Before dispatch, the driver checks assignment identities against the consumer and event, including the attempt number/generation. Generated event/run/task/attempt IDs are limited to 128 bytes and event source to 2,048 bytes. Before the first settlement exchange, it validates the complete report against the 8 MiB contract. Oversized output, business failure text, or adapter errors become a compact `Protocol` failure with the original execution identity and a conservative started flag. This is a terminal failure under the current retry policy; the driver does not truncate an output or rerun the program to obtain a smaller result.

Local `tracing` spans correlate consumer operations by scope, queue, session, and consumer ID, and attempt operations by task, attempt, lease, event, and program digest. Accepted and rejected reports are logged without logging application data. These logs do not establish OpenTelemetry parentage or export.

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

The driver has no local durable journal. An operating-system process crash loses its in-memory cursor and pending report. A replacement worker opens a new session; service-side expiry recovery and the task's retry policy recover unfinished work. The service composition must periodically invoke `RecoveryStore::expire_batch`. A lost reply from session creation can leave an unused session that expires normally.

An expired or unknown session stops the current driver and drains owned work. It does not automatically create a replacement session or transplant old cursors. Lease expiry cannot establish that arbitrary external effects did not happen, and a recovered task may run another attempt.

Ordinary driver tests use controlled service/runtime adapters for capacity, retries, lease deadlines, and retained shutdown. The [PostgreSQL gate](postgres.md#verification) also runs real Python programs through publication, submission, dynamic download, warm reuse, and durable inspection. It injects lost replies after committed acquisition, dispatch, and settlement and verifies restart with persisted bindings/cache. This validates an in-process service composition; network interruption, proxy behavior, and long-poll wakeups require separate transport tests.
