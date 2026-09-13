use super::*;
use ledgence_worker_api::{
    Error, ErrorKind, ExecutionContext, ExecutionFailure, ExecutionRequest, Phase,
};
use ledgence_worker_core::ConsumerReservation;
use renewal::Monitor;

impl Context {
    #[tracing::instrument(name = "delivery_attempt", skip_all, fields(
        task_id = %assignment.lease.owner.task_id, attempt_id = %assignment.lease.owner.attempt_id,
        lease_id = %assignment.lease.owner.lease_id, generation = assignment.lease.owner.generation,
        event_id = %assignment.event.id(), program_id = %assignment.descriptor.program.id,
        program_version = %assignment.descriptor.program.version, digest = %assignment.descriptor.digest.0
    ))]
    pub(super) async fn attempt(
        self: &Arc<Self>,
        mut reservation: ConsumerReservation,
        assignment: Assignment,
        started: Instant,
    ) {
        let request = ExecutionRequest {
            descriptor: assignment.descriptor.clone(),
            event: assignment.event.clone(),
        };
        let monitor = Arc::new(Monitor::new(&assignment, started));
        let _monitor_guard = MonitorGuard(monitor.clone());
        let ongoing = monitor.clone();
        let context = self.clone();
        let renewal = tokio::spawn(
            async move { ongoing.run(context, assignment).await }
                .in_current_span()
                .with_current_subscriber(),
        );
        let report = if monitor.dispatch(&self.shared).await {
            match reservation
                .execute(request.clone(), monitor.control.clone())
                .await
            {
                Ok(report) => AttemptReport::Completed(report),
                Err(failure) => AttemptReport::Failed(failure),
            }
        } else {
            failure(
                &request,
                ErrorKind::Cancelled,
                "delivery stopped before local dispatch",
                false,
            )
        };
        let quiescence = if reservation.is_quiescent() {
            Quiescence::Confirmed
        } else {
            // Existing worker shutdown is the owner of quarantine retries. Drain
            // the whole worker so local cleanup and remote reconciliation can
            // progress concurrently without introducing a second cleanup owner.
            self.shared.stop();
            Quiescence::Unconfirmed
        };
        let owner = monitor.owner();
        let mut command = SettleCommand {
            owner,
            operation_id: "worker-result-v1".into(),
            report,
            quiescence,
            processing_trace: None,
        };
        if !valid_settlement(&command) {
            command.report = failure(
                &request,
                ErrorKind::Protocol,
                "execution report exceeds the settlement contract",
                true,
            );
        }
        let report_valid = valid_settlement(&command);
        // No changes to command after the first exchange, even when cleanup
        // finishes meanwhile. Accepted Unconfirmed reports use a separate call.
        let accepted = loop {
            if !report_valid {
                self.shared.fatal(protocol(
                    "normalized settlement remains invalid; ownership is unresolved",
                ));
                tokio::time::sleep(self.config.retry_delay).await;
                continue;
            }
            match exchange(self.config.request_timeout, || {
                self.service.settle(&command)
            })
            .await
            {
                Ok(reply)
                    if reply.receipt.operation_id == command.operation_id
                        && reply.receipt.task_id == command.owner.task_id
                        && reply.receipt.attempt_id == command.owner.attempt_id =>
                {
                    tracing::info!(operation_id = %command.operation_id, already_accepted = reply.already_accepted, "execution report durably accepted");
                    self.shared
                        .status
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .settled_attempts += 1;
                    break true;
                }
                Err(ContractError::OwnershipLost) => {
                    tracing::warn!(operation_id = %command.operation_id, "execution report rejected after ownership loss");
                    self.shared
                        .status
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .lost_attempts += 1;
                    break false;
                }
                Ok(_) => self.shared.fatal(protocol(
                    "settlement receipt changed operation or attempt identity",
                )),
                Err(error) if retryable(&error) => self.shared.error(error),
                // A protocol conflict is not a cleanup/ownership certificate.
                // Keep the reservation and immutable report pending for diagnosis.
                Err(error) => self.shared.fatal(error),
            }
            tokio::time::sleep(self.config.retry_delay).await;
        };
        while !reservation.is_quiescent() {
            tokio::time::sleep(TICK).await;
        }
        if accepted && quiescence == Quiescence::Unconfirmed {
            loop {
                match exchange(self.config.request_timeout, || {
                    self.service.confirm_quiescence(&command.owner)
                })
                .await
                {
                    Ok(_) | Err(ContractError::OwnershipLost) => break,
                    Err(error) if retryable(&error) => self.shared.error(error),
                    Err(error) => self.shared.fatal(error),
                }
                tokio::time::sleep(self.config.retry_delay).await;
            }
        }
        monitor.done.store(true, Ordering::Release);
        if let Err(error) = renewal.await {
            self.shared.fatal(ContractError::Unavailable(format!(
                "lease supervisor failed: {error}"
            )));
        }
        reservation.release();
    }
}

pub(super) fn validate_assignment(command: &AcquireCommand, assignment: &Assignment) -> Result<()> {
    let owner = &assignment.lease.owner;
    let event = &assignment.event;
    if owner.scope != command.scope
        || owner.worker_session_id != command.worker_session_id
        || owner.consumer_id != command.consumer_id
        || assignment.authority.owner != *owner
        || event.tenant_id() != owner.scope.tenant_id
        || event.namespace() != owner.scope.namespace
        || event.task_id() != owner.task_id
        || event.attempt_id() != owner.attempt_id
        || owner.generation == 0
        || event.value()["ldgattemptno"].as_u64() != Some(u64::from(owner.generation))
        || validate_text(&owner.lease_id, 128).is_err()
    {
        return Err(protocol("assignment does not match acquisition identity"));
    }
    assignment.descriptor.validate()?;
    for key in ["id", "ldgrunid", "ldgtaskid", "ldgattemptid"] {
        validate_text(event.value()[key].as_str().unwrap_or_default(), 128)?;
    }
    validate_text(event.value()["source"].as_str().unwrap_or_default(), 2048)?;
    Ok(())
}

struct MonitorGuard(Arc<Monitor>);
impl Drop for MonitorGuard {
    fn drop(&mut self) {
        self.0.control.cancel();
        self.0.done.store(true, Ordering::Release);
    }
}
fn failure(
    request: &ExecutionRequest,
    kind: ErrorKind,
    message: &str,
    started: bool,
) -> AttemptReport {
    AttemptReport::Failed(ExecutionFailure {
        context: Box::new(ExecutionContext::from(request)),
        error: Error::new(kind, message),
        cleanup_error: None,
        phase: if started {
            Phase::Execution
        } else {
            Phase::Admission
        },
        execution_may_have_started: started,
    })
}

/// Bound the extra serialized allocation, including arbitrarily long adapter
/// error strings. Decoding checks the complete portable report contract.
fn valid_settlement(command: &SettleCommand) -> bool {
    if let AttemptReport::Completed(report) = &command.report
        && let ledgence_worker_api::ProgramOutcome::Success { output } = &report.outcome
        && ledgence_worker_api::validate_wire_value(output).is_err()
    {
        return false;
    }
    struct Bounded(Vec<u8>);
    impl std::io::Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > SETTLEMENT_MAX_BYTES.saturating_sub(self.0.len()) {
                return Err(std::io::Error::other("settlement limit"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut bytes = Bounded(Vec::new());
    serde_json::to_writer(&mut bytes, command).is_ok() && SettleCommand::decode(&bytes.0).is_ok()
}
