use super::*;
use ledgence_worker_api::{
    Error, ErrorKind, ExecutionContext, ExecutionFailure, ExecutionRequest, Phase,
};
use ledgence_worker_core::ConsumerReservation;
use renewal::Monitor;

impl Context {
    pub(super) async fn attempt(
        self: &Arc<Self>,
        reservation: ConsumerReservation,
        assignment: Assignment,
    ) {
        let started = Instant::now();
        let event = &assignment.event;
        let owner = &assignment.lease.owner;
        let span = tracing::info_span!("ledgence.attempt.process", otel.kind = "consumer",
            ledgence.tenant.id = %event.tenant_id(), ledgence.namespace = %event.namespace(),
            ledgence.workflow.id = event.value()["ldgworkflowid"].as_str(),
            ledgence.workflow.parent.id = event.value()["ldgparentworkflowid"].as_str(),
            ledgence.workflow.root.id = event.value()["ldgrootworkflowid"].as_str(),
            ledgence.activation.id = event.value()["ldgactivationid"].as_str(),
            ledgence.run.id = %event.value()["ldgrunid"].as_str().expect("validated run ID"), ledgence.task.id = %owner.task_id,
            ledgence.attempt.id = %owner.attempt_id, ledgence.attempt.number = i64::from(owner.generation),
            ledgence.worker.session.id = %owner.worker_session_id, ledgence.consumer.id = i64::from(owner.consumer_id),
            ledgence.program.id = %assignment.descriptor.program.id,
            ledgence.program.version = %assignment.descriptor.program.version,
            ledgence.program.digest = %assignment.descriptor.digest.0,
            cloudevents.event_id = %event.id(), cloudevents.event_source = %event.value()["source"].as_str().expect("validated source"),
            otel.status_code = tracing::field::Empty, ledgence.outcome = tracing::field::Empty,
            ledgence.duration_ms = tracing::field::Empty,
            task_id = %owner.task_id, attempt_id = %owner.attempt_id,
            lease_id = %owner.lease_id, generation = owner.generation,
            event_id = %event.id(), program_id = %assignment.descriptor.program.id,
            program_version = %assignment.descriptor.program.version, digest = %assignment.descriptor.digest.0);
        let bridge = self.worker.trace_bridge();
        bridge.set_parent(&span, TraceContext::from_event(event).as_ref());
        // Capture exactly once before dispatch. Settlement normalization and every
        // subsequent transport retry retain this value, including unsampled IDs.
        let processing_trace = bridge.context(&span);
        self.process(reservation, assignment, processing_trace)
            .instrument(span.clone())
            .await;
        span.record(
            "ledgence.duration_ms",
            started.elapsed().as_millis().try_into().unwrap_or(i64::MAX),
        );
    }

    async fn process(
        self: &Arc<Self>,
        mut reservation: ConsumerReservation,
        assignment: Assignment,
        processing_trace: Option<TraceContext>,
    ) {
        let request = ExecutionRequest {
            descriptor: assignment.descriptor.clone(),
            event: assignment.event.clone(),
        };
        let workflow_activation_id = assignment.workflow_activation_id.clone();
        let monitor = Arc::new(Monitor::new(&assignment));
        let _monitor_guard = MonitorGuard(monitor.clone());
        let ongoing = monitor.clone();
        let context = self.clone();
        let renewal = tokio::spawn(
            async move { ongoing.run(context, assignment).await }
                .in_current_span()
                .with_current_subscriber(),
        );
        let report = if let Some(control) = monitor.dispatch(&self.shared).await {
            if let Some(activation_id) = workflow_activation_id {
                if activation_id != monitor.owner().task_id {
                    failure(
                        &request,
                        ErrorKind::Protocol,
                        "workflow assignment identity mismatch",
                        false,
                    )
                } else {
                    match self
                        .workflow_runtime(
                            monitor.owner(),
                            request.event.value()["ldgworkflowid"]
                                .as_str()
                                .expect("validated workflow assignment"),
                            request.event.value()["ldgparentworkflowid"].as_str(),
                            request.event.value()["ldgrootworkflowid"].as_str(),
                            &control,
                        )
                        .await
                    {
                        Ok(runtime) => {
                            // Link the external cause before this new span enters
                            // or materializes context. The attempt's already captured
                            // processing trace and original invocation remain stable.
                            let span = tracing::info_span!("ledgence.workflow.activation",
                                ledgence.workflow.id = request.event.value()["ldgworkflowid"].as_str(),
                                ledgence.workflow.parent.id = request.event.value()["ldgparentworkflowid"].as_str(),
                                ledgence.workflow.root.id = request.event.value()["ldgrootworkflowid"].as_str(),
                                ledgence.activation.id = %activation_id,
                                ledgence.workflow.wake = runtime.extension.payload["wake"]["kind"].as_str(),
                                cloudevents.event_id = runtime.extension.payload["wake"]["event"]["id"].as_str(),
                                cloudevents.event_source = runtime.extension.payload["wake"]["event"]["source"].as_str());
                            if let Some(origin) = &runtime.wake_trace {
                                self.worker.trace_bridge().add_link(&span, origin);
                            }
                            match reservation
                                .execute_interactive(
                                    request.clone(),
                                    control,
                                    runtime.extension,
                                    runtime.handler,
                                )
                                .instrument(span)
                                .await
                            {
                                Ok(report) => AttemptReport::Completed(report),
                                Err(failure) => AttemptReport::Failed(failure),
                            }
                        }
                        Err(error) => failure(&request, error.kind, &error.message, false),
                    }
                }
            } else {
                match reservation.execute(request.clone(), control).await {
                    Ok(report) => AttemptReport::Completed(report),
                    Err(failure) => AttemptReport::Failed(failure),
                }
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
            processing_trace,
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
        let settlement_span = tracing::info_span!(
            "ledgence.attempt.settle",
            otel.kind = "internal",
            ledgence.outcome = tracing::field::Empty,
            otel.status_code = tracing::field::Empty
        );
        let accepted = async { loop {
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
        } }.instrument(settlement_span.clone()).await;
        settlement_span.record(
            "ledgence.outcome",
            if accepted {
                "accepted"
            } else {
                "ownership_lost"
            },
        );
        if !accepted {
            settlement_span.record("otel.status_code", "ERROR");
        }
        drop(settlement_span);
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
        tracing::Span::current().record(
            "ledgence.outcome",
            if accepted {
                "settled"
            } else {
                "ownership_lost"
            },
        );
        if !accepted {
            tracing::Span::current().record("otel.status_code", "ERROR");
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
    assignment
        .validate_workflow_identity()
        .map_err(|_| protocol("workflow assignment does not match event identity"))?;
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
        self.0.stop();
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
        && validate_task_output(output, report.context.identity.activation_id.is_some()).is_err()
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
