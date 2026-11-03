//! Host-managed, read-only provider checks. This path requires a server marker.
use super::*;
use ledgence_worker_api::{
    Error, ErrorKind, ExecutionContext, ExecutionFailure, ExecutionReport, ExecutionRequest, Phase,
    PortFuture, ProgramOutcome, RuntimeCapabilities, RuntimeExtension, RuntimeFeatures,
    RuntimeReply, RuntimeRequest, RuntimeRequestHandler,
};
use ledgence_worker_core::ConsumerReservation;
use serde_json::{Value, json};
use workflow::{controlled, pause, runtime_error};

#[cfg(test)]
#[path = "provider_tests.rs"]
mod tests;

pub(super) fn mismatch() -> Error {
    Error::new(
        ErrorKind::Protocol,
        "provider reply disagrees with its command or negotiated worker",
    )
}
pub(super) fn report_projection(reply: &ProviderReportReply) -> Value {
    let receipt = reply.receipt();
    json!({"kind":if matches!(reply,ProviderReportReply::Accepted{..}) {"recorded"} else {"recorded_elsewhere"},
        "ticket":receipt.ticket,"source_id":receipt.source_id,"report_id":receipt.report_id,
        "accepted_at":receipt.accepted_at,"recorded_revision":receipt.recorded_revision,
        "accepted_via":receipt.accepted_via,"already_accepted":receipt.already_accepted})
}
struct ProbeHandler {
    features: RuntimeFeatures,
    timeout: Duration,
}
impl RuntimeRequestHandler for ProbeHandler {
    fn features(&self) -> RuntimeFeatures {
        self.features.clone()
    }
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            approved_effect_evidence_v2: true,
        }
    }
    fn invocation_timeout(&self) -> Option<Duration> {
        Some(self.timeout)
    }
    fn completion_reserve(&self) -> Duration {
        Duration::from_secs(10)
    }
    fn handle<'a>(&'a self, _: RuntimeRequest, _: RunControl) -> PortFuture<'a, RuntimeReply> {
        Box::pin(async {
            Err(Error::new(
                ErrorKind::Protocol,
                "provider checks cannot orchestrate work",
            ))
        })
    }
}
impl Context {
    pub(super) async fn provider_probe(
        &self,
        reservation: &mut ConsumerReservation,
        request: ExecutionRequest,
        owner: LeaseOwner,
        probe: ProviderProbeAssignment,
        control: RunControl,
    ) -> (AttemptReport, Option<ProviderProbeFailureKind>) {
        let prepared = self.prepare_probe(&request, &owner, probe, &control).await;
        let (context, access) = match prepared {
            Ok(value) => value,
            Err(error) => return failed(&request, error, Phase::Admission, false),
        };
        // Explicit projection prevents child code from receiving credentials or
        // confusing cached provider knowledge with worker authority.
        let payload = json!({"ticket":context.ticket,"binding":context.binding,"action":context.action,
            "probe_id":context.probe_id,"generation":context.generation,"task_id":context.task_id,
            "attempt_id":context.attempt_id,"source_id":context.source_id,"report_id":context.report_id,
            "provider_reference":context.provider_reference,"check_timeout_ms":context.check_timeout_ms});
        let outcome = reservation
            .execute_interactive(
                request.clone(),
                control.clone(),
                RuntimeExtension {
                    schema: "ledgence.provider.probe.v1".into(),
                    payload,
                },
                Arc::new(ProbeHandler {
                    features: access.features.clone(),
                    timeout: Duration::from_millis(context.check_timeout_ms),
                }),
            )
            .await;
        let mut execution = match outcome {
            Ok(execution) => execution,
            Err(failure) => {
                let report = AttemptReport::Failed(failure);
                let kind = classify_failure(&report);
                return (report, kind);
            }
        };
        let outcome = std::mem::replace(
            &mut execution.outcome,
            ProgramOutcome::Success {
                output: Value::Null,
            },
        );
        let ProgramOutcome::Success { output } = outcome else {
            return failed_execution(
                execution,
                Error::new(
                    ErrorKind::Runtime,
                    "provider probe did not return the typed host envelope",
                ),
            );
        };
        let result: ProviderProbeRuntimeResult = match serde_json::from_value(output) {
            Ok(result) => result,
            Err(_) => {
                return failed_execution(
                    execution,
                    Error::new(ErrorKind::Protocol, "invalid provider probe result"),
                );
            }
        };
        if result.validate().is_err() {
            return failed_execution(
                execution,
                Error::new(ErrorKind::Protocol, "invalid provider probe result"),
            );
        }
        let report = match result {
            ProviderProbeRuntimeResult::Report { report, .. } => *report,
            ProviderProbeRuntimeResult::Rejected { failure, .. } => {
                let (kind, classification) = match failure {
                    ProviderProbeRuntimeRejection::ContractMismatch => (
                        ErrorKind::Incompatible,
                        ProviderProbeFailureKind::ContractMismatch,
                    ),
                    ProviderProbeRuntimeRejection::ContextInvalid => (
                        ErrorKind::InvalidInput,
                        ProviderProbeFailureKind::ContextInvalid,
                    ),
                    ProviderProbeRuntimeRejection::ResultInvalid => {
                        (ErrorKind::Protocol, ProviderProbeFailureKind::ResultInvalid)
                    }
                    ProviderProbeRuntimeRejection::InvocationFailed => (
                        ErrorKind::Runtime,
                        ProviderProbeFailureKind::InvocationFailed,
                    ),
                };
                let (report, _) = failed_execution(
                    execution,
                    Error::new(kind, "provider probe rejected its typed invocation"),
                );
                return (report, Some(classification));
            }
        };
        if report.reported_target != context.binding.target {
            return failed_execution(
                execution,
                Error::new(
                    ErrorKind::Protocol,
                    "provider report target differs from approval",
                ),
            );
        }
        let command = ProviderProbeReportCommand {
            owner,
            ticket: context.ticket.clone(),
            generation: context.generation,
            source_id: context.source_id,
            report_id: context.report_id,
            report,
        };
        match self
            .submit_probe_report(&access, &context.probe_id, &command, &control)
            .await
        {
            Ok(reply) => {
                let receipt = reply.receipt();
                execution.outcome = ProgramOutcome::Success {
                    output: json!({"kind":"provider_probe",
                    "report_id":receipt.report_id,"recorded_revision":receipt.recorded_revision,
                    "disposition":if matches!(reply,ProviderReportReply::Accepted{..}) {"recorded"} else {"recorded_elsewhere"}}),
                };
                (AttemptReport::Completed(execution), None)
            }
            Err(error) => failed_execution(execution, error),
        }
    }
    async fn prepare_probe(
        &self,
        request: &ExecutionRequest,
        owner: &LeaseOwner,
        probe: ProviderProbeAssignment,
        control: &RunControl,
    ) -> ledgence_worker_api::Result<(ProviderProbeContext, Arc<EffectAccess>)> {
        let access = self
            .effect_access
            .clone()
            .filter(|a| a.features.provider_operation())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Incompatible,
                    "provider operation feature was not negotiated",
                )
            })?;
        let workflows = self.workflows.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::Incompatible,
                "provider operation service unavailable",
            )
        })?;
        let service = workflows
            .provider_operation_client()
            .map_err(runtime_error)?;
        let command = ProviderProbeContextCommand {
            owner: owner.clone(),
            probe,
        };
        command.validate().map_err(runtime_error)?;
        let context = loop {
            control.check()?;
            match controlled(control, self.config.request_timeout, || {
                service.probe_context(&command)
            })
            .await
            {
                Ok(context) => break context,
                Err(error) if retryable(&error) => pause(control, self.config.retry_delay).await?,
                Err(error) => {
                    control.check()?;
                    return Err(runtime_error(error));
                }
            }
        };
        control.check()?;
        if !context.matches(&command).unwrap_or(false) || !access.matches(&context.worker) {
            return Err(mismatch());
        }
        if context.binding.program != request.descriptor {
            return Err(Error::new(
                ErrorKind::Integrity,
                "provider probe program differs from frozen approval",
            ));
        }
        Ok((context, access))
    }
    async fn submit_probe_report(
        &self,
        access: &EffectAccess,
        probe_id: &str,
        command: &ProviderProbeReportCommand,
        control: &RunControl,
    ) -> ledgence_worker_api::Result<ProviderReportReply> {
        command.validate().map_err(runtime_error)?;
        let service = self
            .workflows
            .as_ref()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Incompatible,
                    "provider operation service unavailable",
                )
            })?
            .provider_operation_client()
            .map_err(runtime_error)?;
        submit_probe_report(service, &self.config, access, probe_id, command, control).await
    }
}
async fn submit_probe_report(
    service: &dyn ProviderOperationClient,
    config: &DeliveryConfig,
    access: &EffectAccess,
    probe_id: &str,
    command: &ProviderProbeReportCommand,
    control: &RunControl,
) -> ledgence_worker_api::Result<ProviderReportReply> {
    command.validate().map_err(runtime_error)?;
    loop {
        control.check()?;
        match controlled(control, config.request_timeout, || {
            service.report_probe(command)
        })
        .await
        {
            Ok(reply) => {
                control.check()?;
                if !reply.matches(command).unwrap_or(false)
                    || !access.matches(&reply.receipt().worker)
                    || !matches!(&reply.receipt().source,ProviderSourceReference::Probe{probe_id:id,..} if id==probe_id)
                {
                    return Err(mismatch());
                }
                return Ok(reply);
            }
            Err(error) if retryable(&error) => pause(control, config.retry_delay).await?,
            Err(error) => {
                control.check()?;
                return Err(runtime_error(error));
            }
        }
    }
}
fn failed_execution(
    execution: ExecutionReport,
    error: Error,
) -> (AttemptReport, Option<ProviderProbeFailureKind>) {
    let report = AttemptReport::Failed(ExecutionFailure {
        observations: execution.observations,
        context: execution.context,
        error,
        cleanup_error: None,
        phase: Phase::Execution,
        execution_may_have_started: true,
    });
    let kind = classify_failure(&report);
    (report, kind)
}
fn failed(
    request: &ExecutionRequest,
    error: Error,
    phase: Phase,
    started: bool,
) -> (AttemptReport, Option<ProviderProbeFailureKind>) {
    let report = AttemptReport::Failed(ExecutionFailure {
        observations: None,
        context: Box::new(ExecutionContext::from(request)),
        error,
        cleanup_error: None,
        phase,
        execution_may_have_started: started,
    });
    let kind = classify_failure(&report);
    (report, kind)
}
pub(super) fn classify_failure(report: &AttemptReport) -> Option<ProviderProbeFailureKind> {
    // Typed host errors only. Application failure strings carry no authority.
    use ProviderProbeFailureKind::*;
    [
        IncompatibleWorker,
        ContractMismatch,
        ResultInvalid,
        ContextInvalid,
        InvocationTimeout,
        InvocationFailed,
        PreparationFailed,
    ]
    .into_iter()
    .find(|kind| kind.validate_settlement(report).is_ok())
}
