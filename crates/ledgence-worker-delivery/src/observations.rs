//! Best-effort observation is independent of acquisition, leases and settlement.

use super::*;
use ledgence_orchestration_api::console::*;
use ledgence_worker_api::{ProcessSlotState, WorkerObservationDetailState};
use tokio::sync::oneshot;

const REPORT_TIMEOUT: Duration = Duration::from_secs(2);
const WARNING_INTERVAL: Duration = Duration::from_secs(60);

pub(super) struct ObservationReporting {
    pub publisher: Arc<dyn WorkerObservationPublisher>,
    pub display_name: Option<String>,
}

/// Dropping this signal requests one deadline-bounded final snapshot, but does
/// not make shutdown await an unavailable observability endpoint.
pub(super) struct ReportingGuard(Option<oneshot::Sender<()>>);
impl Drop for ReportingGuard {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

impl ObservationReporting {
    pub fn start(self, worker: Worker, session: WorkerSession) -> ReportingGuard {
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(
            self.run(worker, session, receiver)
                .with_current_subscriber(),
        );
        ReportingGuard(Some(sender))
    }

    async fn run(self, worker: Worker, session: WorkerSession, mut stop: oneshot::Receiver<()>) {
        let mut sequence = 1_u64;
        let mut last_warning = None;
        loop {
            let next = tokio::time::Instant::now() + interval(&session.id, sequence);
            let result = tokio::select! {
                biased;
                _ = &mut stop => {
                    // The cancelled exchange may already have committed. A new
                    // snapshot must never reuse its immutable sequence identity.
                    if let Some(final_sequence) = sequence.checked_add(1) {
                        let _ = self.publish(&worker, &session, final_sequence).await;
                    }
                    return;
                }
                result = self.publish(&worker, &session, sequence) => result,
            };
            if let Err(error) = result {
                let now = Instant::now();
                if last_warning.is_none_or(|last| now.duration_since(last) >= WARNING_INTERVAL) {
                    tracing::warn!(error = %error, "worker observation unavailable; execution continues");
                    last_warning = Some(now);
                }
            }
            let Some(next_sequence) = sequence.checked_add(1) else {
                return;
            };
            sequence = next_sequence;
            tokio::select! {
                biased;
                _ = &mut stop => {
                    let _ = self.publish(&worker, &session, sequence).await;
                    return;
                }
                _ = tokio::time::sleep_until(next) => {}
            }
        }
    }

    async fn publish(&self, worker: &Worker, session: &WorkerSession, sequence: u64) -> Result<()> {
        exchange(REPORT_TIMEOUT, || async {
            let mut command = WorkerObservationCommand {
                schema_version: 1,
                worker_session_id: session.id.clone(),
                scope: session.scope.clone(),
                sequence: ConsoleU64(sequence),
                display_name: self.display_name.clone(),
                snapshot: worker.observation().await,
            };
            isolate_scope(&mut command);
            // A complete summary is better than truncated detail. Structural
            // validation still runs after this byte-bound fallback.
            if !fits_body(&command) {
                command.snapshot.slots.clear();
                command.snapshot.detail_state = WorkerObservationDetailState::Unavailable;
            }
            command.validate()?;
            let receipt = self.publisher.publish_observation(&command).await?;
            if receipt.worker_session_id != session.id
                || receipt.sequence.0 != sequence
                || receipt.received_at > CONSOLE_MAX_TIMESTAMP
            {
                return Err(protocol("worker observation receipt identity mismatch"));
            }
            Ok(())
        })
        .await
    }
}

fn fits_body(command: &WorkerObservationCommand) -> bool {
    struct BoundedCount(usize);
    impl std::io::Write for BoundedCount {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len());
            if self.0 > WORKER_OBSERVATION_MAX_BYTES {
                return Err(std::io::Error::other("observation exceeds body bound"));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(BoundedCount(0), command).is_ok()
}

fn isolate_scope(command: &mut WorkerObservationCommand) {
    for slot in &mut command.snapshot.slots {
        if slot.scope.as_ref().is_some_and(|scope| {
            scope.tenant_id != command.scope.tenant_id || scope.namespace != command.scope.namespace
        }) {
            // Occupancy belongs to the global pool, but another binding's
            // program and invocation must never leave the connected adapter.
            slot.state = ProcessSlotState::Unknown;
            slot.process_instance_id = None;
            slot.process_id = None;
            slot.program = None;
            slot.digest = None;
            slot.scope = None;
            slot.invocation = None;
        }
    }
}

fn interval(session: &str, sequence: u64) -> Duration {
    // Stable, dependency-free phase jitter; sessions do not publish in lockstep.
    let hash = session.bytes().fold(sequence, |value, byte| {
        value
            .wrapping_mul(1099511628211)
            .wrapping_add(u64::from(byte))
    });
    Duration::from_millis(4500 + hash % 1001)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledgence_worker_api::{
        InvocationObservation, SlotObservation, WorkerObservationScope, WorkerObservationSnapshot,
    };

    #[test]
    fn foreign_scope_metadata_is_hidden_without_freeing_global_occupancy() {
        let mut command = WorkerObservationCommand {
            schema_version: 1,
            worker_session_id: "session".into(),
            scope: Scope {
                tenant_id: "tenant".into(),
                namespace: "billing".into(),
            },
            sequence: ConsoleU64(1),
            display_name: None,
            snapshot: WorkerObservationSnapshot {
                configured_concurrency: 1,
                accepting: true,
                active_consumers: 1,
                occupied_process_slots: 1,
                detail_state: WorkerObservationDetailState::Available,
                slots: vec![SlotObservation {
                    slot_id: 0,
                    state: ProcessSlotState::Executing,
                    process_instance_id: Some("foreign-process".into()),
                    process_id: Some(123),
                    program: Some(ledgence_worker_api::ProgramRef {
                        id: "foreign-program".into(),
                        version: "1".into(),
                    }),
                    digest: Some(ledgence_worker_api::Digest(format!("sha256:{:064x}", 1))),
                    scope: Some(WorkerObservationScope {
                        tenant_id: "another-tenant".into(),
                        namespace: "billing".into(),
                    }),
                    invocation: Some(InvocationObservation {
                        task_id: "foreign-task".into(),
                        attempt_id: "foreign-attempt".into(),
                    }),
                }],
            },
        };
        isolate_scope(&mut command);
        command.validate().unwrap();
        assert_eq!(command.snapshot.occupied_process_slots, 1);
        assert_eq!(command.snapshot.active_consumers, 1);
        assert_eq!(command.snapshot.slots[0].state, ProcessSlotState::Unknown);
        let wire = serde_json::to_string(&command).unwrap();
        assert!(!wire.contains("foreign"));
        assert!(!wire.contains("another-tenant"));
        assert!(!wire.contains("123"));
        assert!(fits_body(&command));
        command.display_name = Some("x".repeat(WORKER_OBSERVATION_MAX_BYTES));
        assert!(!fits_body(&command));
    }

    #[test]
    fn intervals_are_bounded_jittered_and_deterministic() {
        let intervals = (1..20)
            .map(|sequence| interval("session", sequence))
            .collect::<Vec<_>>();
        assert!(intervals.iter().all(|duration| {
            (Duration::from_millis(4500)..=Duration::from_millis(5500)).contains(duration)
        }));
        assert!(intervals.windows(2).any(|pair| pair[0] != pair[1]));
        assert_ne!(interval("session-a", 1), interval("session-b", 1));
        assert_eq!(interval("session", 2), interval("session", 2));
    }
}
