//! Bind runtime requests to the acquired activation's exact lease owner.
use super::*;
use ledgence_worker_api::{
    Error, ErrorKind, PortFuture, RuntimeExtension, RuntimeReply, RuntimeRequest,
    RuntimeRequestHandler,
};
use serde_json::json;
mod evidence;
mod provider;

pub(super) struct WorkflowRuntime {
    pub extension: RuntimeExtension,
    pub handler: Arc<dyn RuntimeRequestHandler>,
    pub wake_trace: Option<TraceContext>,
}

impl Context {
    pub(super) async fn workflow_runtime(
        &self,
        owner: LeaseOwner,
        expected_workflow: &str,
        expected_parent: Option<&str>,
        expected_root: Option<&str>,
        processing_trace: Option<TraceContext>,
        control: &RunControl,
    ) -> ledgence_worker_api::Result<WorkflowRuntime> {
        if let Some(trace) = &processing_trace {
            trace.validate()?;
        }
        let service = self.workflows.clone().ok_or_else(|| {
            Error::new(
                ErrorKind::Incompatible,
                "workflow delivery is not configured",
            )
        })?;
        let context = fetch_context(
            service.as_ref(),
            &owner,
            expected_workflow,
            expected_parent,
            expected_root,
            &self.config,
            control,
        )
        .await?;
        if matches!(context.wake, Some(WorkflowWake::EffectResolution(_)))
            && !self
                .effect_access
                .as_ref()
                .is_some_and(|access| access.features.native_effect_resolution())
        {
            return Err(Error::new(
                ErrorKind::Incompatible,
                "native effect resolution was not negotiated for this worker",
            ));
        }
        if matches!(context.wake, Some(WorkflowWake::ProviderOperation(_)))
            && !self
                .effect_access
                .as_ref()
                .is_some_and(|access| access.features.provider_operation())
        {
            return Err(Error::new(
                ErrorKind::Incompatible,
                "provider operations were not negotiated for this worker",
            ));
        }
        let wake_trace = context.wake.as_ref().and_then(|wake| match wake {
            WorkflowWake::Event { event, .. } => event.trace_context(),
            _ => None,
        });
        let payload = serde_json::to_value(context)
            .map_err(|error| Error::new(ErrorKind::Protocol, error.to_string()))?;
        let handler = Arc::new(WorkflowOperations {
            service,
            effect_access: self.effect_access.clone(),
            owner,
            workflow_id: expected_workflow.to_owned(),
            processing_trace,
            request_timeout: self.config.request_timeout,
            retry_delay: self.config.retry_delay,
        });
        Ok(WorkflowRuntime {
            extension: RuntimeExtension {
                schema: WORKFLOW_RUNTIME_SCHEMA.into(),
                payload,
            },
            handler,
            wake_trace,
        })
    }
}

async fn fetch_context(
    service: &dyn WorkflowService,
    owner: &LeaseOwner,
    expected_workflow: &str,
    expected_parent: Option<&str>,
    expected_root: Option<&str>,
    config: &DeliveryConfig,
    control: &RunControl,
) -> ledgence_worker_api::Result<WorkflowActivationContext> {
    let context = loop {
        control.check()?;
        match controlled(control, config.request_timeout, || {
            service.activation_context(owner)
        })
        .await
        {
            Ok(context) => break context,
            Err(error) if retryable(&error) => pause(control, config.retry_delay).await?,
            Err(error) => {
                control.check()?;
                return Err(runtime_error(error));
            }
        }
    };
    control.check()?;
    context.validate_for_owner(owner).map_err(runtime_error)?;
    if context.workflow_id != expected_workflow
        || context.parent_workflow_id.as_deref() != expected_parent
        || context.root_workflow_id.as_deref() != expected_root
    {
        return Err(Error::new(
            ErrorKind::Protocol,
            "activation context identity mismatch",
        ));
    }
    Ok(context)
}

struct WorkflowOperations {
    service: Arc<dyn WorkflowService>,
    effect_access: Option<Arc<EffectAccess>>,
    owner: LeaseOwner,
    workflow_id: String,
    processing_trace: Option<TraceContext>,
    request_timeout: Duration,
    retry_delay: Duration,
}
impl RuntimeRequestHandler for WorkflowOperations {
    fn features(&self) -> ledgence_worker_api::RuntimeFeatures {
        self.effect_access
            .as_ref()
            .map(|access| access.features.clone())
            .unwrap_or_default()
    }
    fn capabilities(&self) -> ledgence_worker_api::RuntimeCapabilities {
        ledgence_worker_api::RuntimeCapabilities {
            approved_effect_evidence_v2: self.effect_access.is_some(),
        }
    }
    fn handle<'a>(
        &'a self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> PortFuture<'a, RuntimeReply> {
        Box::pin(async move {
            request.validate()?;
            match request.operation.as_str() {
                "provider_operation.claim.v1" => {
                    return self.claim_provider(request, control).await;
                }
                "provider_operation.report.v1" => {
                    return self.report_provider(request, control).await;
                }
                "approved_effect.claim.v2" => {
                    return self.claim_effect_v2(request, control).await;
                }
                "approved_effect.confirm.v2" => {
                    return self.confirm_effect_v2(request, control).await;
                }
                "approved_effect.claim" => {
                    return self.claim_approved_effect(request, control).await;
                }
                "approved_effect.confirm" => {
                    return self.confirm_approved_effect(request, control).await;
                }
                "workflow.fork" => return self.fork(request, control).await,
                "local_step.commit" => {}
                _ => {
                    return Err(Error::new(
                        ErrorKind::Protocol,
                        "unsupported workflow runtime operation",
                    ));
                }
            }
            let record: LocalStepRecord = serde_json::from_value(request.payload)
                .map_err(|error| Error::new(ErrorKind::Protocol, error.to_string()))?;
            record.validate().map_err(runtime_error)?;
            let command = LocalResultCommand {
                owner: self.owner.clone(),
                record,
            };
            loop {
                control.check()?;
                match controlled(&control, self.request_timeout, || {
                    self.service.record_local_result(&command)
                })
                .await
                {
                    Ok(receipt) => {
                        control.check()?;
                        if receipt.key != command.record.key {
                            return Err(Error::new(
                                ErrorKind::Protocol,
                                "local result receipt key mismatch",
                            ));
                        }
                        return Ok(RuntimeReply {
                            id: request.id,
                            result: json!({"committed":true}),
                        });
                    }
                    Err(error) if retryable(&error) => pause(&control, self.retry_delay).await?,
                    Err(error) => {
                        control.check()?;
                        return Err(runtime_error(error));
                    }
                }
            }
        })
    }
}

impl WorkflowOperations {
    async fn claim_approved_effect(
        &self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> ledgence_worker_api::Result<RuntimeReply> {
        let claim: ApprovedEffectClaimRequest = serde_json::from_value(request.payload)
            .map_err(|error| Error::new(ErrorKind::Protocol, error.to_string()))?;
        let command = ApprovedEffectClaimCommand {
            owner: self.owner.clone(),
            workflow_id: self.workflow_id.clone(),
            approval_key: claim.approval_key,
            action: claim.action,
            lookup: claim.lookup,
        };
        command.validate().map_err(runtime_error)?;
        control.check()?;
        // A lost grant may already have consumed the only dispatch. Only a
        // fresh, verified reply may reach the subprocess.
        let effects = self.service.approved_effects().map_err(runtime_error)?;
        let result = controlled(&control, self.request_timeout, || {
            effects.claim_approved_effect(&command)
        })
        .await;
        control.check()?;
        let receipt = result.map_err(|error| match error {
            // A different approval wake cannot become eligible by retrying this activation.
            ContractError::ObsoleteOperation => Error::new(ErrorKind::Protocol, error.to_string()),
            other => runtime_error(other),
        })?;
        if !matches!(receipt.matches(&command), Ok(true)) {
            return Err(Error::new(
                ErrorKind::Protocol,
                "approved effect receipt identity mismatch",
            ));
        }
        Ok(RuntimeReply {
            id: request.id,
            result: json!({
                "workflow_id": receipt.workflow_id,
                "activation_id": receipt.activation_id,
                "approval_key": receipt.approval_key,
                "action": receipt.action,
                "lookup": receipt.lookup,
                "disposition": receipt.disposition,
            }),
        })
    }

    async fn confirm_approved_effect(
        &self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> ledgence_worker_api::Result<RuntimeReply> {
        let confirm: ApprovedEffectConfirmRequest = serde_json::from_value(request.payload)
            .map_err(|error| Error::new(ErrorKind::Protocol, error.to_string()))?;
        let command = ApprovedEffectConfirmCommand {
            owner: self.owner.clone(),
            workflow_id: self.workflow_id.clone(),
            approval_key: confirm.approval_key,
            lookup: confirm.lookup,
            record: confirm.record,
        };
        command.validate().map_err(runtime_error)?;
        let effects = self.service.approved_effects().map_err(runtime_error)?;
        // Retrying confirmation sends only the same frozen evidence to Ledgence.
        // The provider call has already returned and cannot run in this loop.
        loop {
            control.check()?;
            match controlled(&control, self.request_timeout, || {
                effects.confirm_approved_effect(&command)
            })
            .await
            {
                Ok(receipt) => {
                    control.check()?;
                    if !matches!(receipt.matches(&command), Ok(true)) {
                        return Err(Error::new(
                            ErrorKind::Protocol,
                            "approved effect confirmation identity mismatch",
                        ));
                    }
                    return Ok(RuntimeReply {
                        id: request.id,
                        result: json!({"committed": true}),
                    });
                }
                Err(error) if retryable(&error) => pause(&control, self.retry_delay).await?,
                Err(error) => {
                    control.check()?;
                    return Err(runtime_error(error));
                }
            }
        }
    }

    async fn fork(
        &self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> ledgence_worker_api::Result<RuntimeReply> {
        let fork: WorkflowForkRequest = serde_json::from_value(request.payload)
            .map_err(|error| Error::new(ErrorKind::Protocol, error.to_string()))?;
        let command = WorkflowForkCommand {
            owner: self.owner.clone(),
            fork,
            processing_trace: self.processing_trace.clone(),
        };
        command.validate().map_err(runtime_error)?;
        loop {
            control.check()?;
            match controlled(&control, self.request_timeout, || {
                self.service.fork_workflow(&command)
            })
            .await
            {
                Ok(receipt) => {
                    control.check()?;
                    if receipt.validate().is_err() || !receipt.matches(&command) {
                        return Err(Error::new(
                            ErrorKind::Protocol,
                            "workflow fork receipt identity mismatch",
                        ));
                    }
                    return Ok(RuntimeReply {
                        id: request.id,
                        result: json!({
                            "committed": true,
                            "key": receipt.key,
                            "branch_keys": receipt.branch_keys,
                        }),
                    });
                }
                Err(error) if retryable(&error) => pause(&control, self.retry_delay).await?,
                Err(error) => {
                    control.check()?;
                    return Err(runtime_error(error));
                }
            }
        }
    }
}

/// Cancellation can leave a write unconfirmed. The immutable command and the
/// store's lease fence/receipt make that uncertainty recoverable on replay.
pub(super) async fn controlled<T, F: Future<Output = Result<T>>>(
    control: &RunControl,
    timeout: Duration,
    call: impl FnOnce() -> F,
) -> Result<T> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .unwrap_or_else(Instant::now)
        .min(control.deadline());
    // Capture the budget before constructing adapter I/O. The shared exchange
    // supervisor bounds construction and polling, including late synchronous
    // completion, without losing the immutable command's retry semantics.
    let future = super::exchange_until(deadline, || {
        let future = call();
        async {
            // Constructing a port future is synchronous but can still race a
            // cancellation from another task or thread. Fence dispatch again.
            if control.check().is_err() {
                return Err(ContractError::OwnershipLost);
            }
            future.await
        }
    });
    tokio::pin!(future);
    loop {
        if control.check().is_err() {
            return Err(ContractError::OwnershipLost);
        }
        tokio::select! {
            result = &mut future => {
                // A synchronous adapter poll can also race cancellation. Do not
                // acknowledge the result after the attempt lost permission.
                if control.check().is_err() {
                    return Err(ContractError::OwnershipLost);
                }
                return result;
            },
            _ = tokio::time::sleep(TICK) => {},
        }
    }
}
pub(super) async fn pause(
    control: &RunControl,
    delay: Duration,
) -> ledgence_worker_api::Result<()> {
    let until = Instant::now() + delay;
    while Instant::now() < until {
        control.check()?;
        tokio::time::sleep(TICK.min(until.saturating_duration_since(Instant::now()))).await;
    }
    control.check()
}
pub(super) fn runtime_error(error: ContractError) -> Error {
    let kind = match error {
        ContractError::OwnershipLost => ErrorKind::Cancelled,
        ContractError::InvalidInput(_) | ContractError::Conflict | ContractError::NotFound => {
            ErrorKind::Protocol
        }
        _ => ErrorKind::Unavailable,
    };
    Error::new(kind, error.to_string())
}

#[cfg(test)]
mod tests;
