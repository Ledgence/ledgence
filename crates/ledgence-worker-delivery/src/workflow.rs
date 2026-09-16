//! Bind runtime requests to the acquired activation's exact lease owner.
use super::*;
use ledgence_worker_api::{
    Error, ErrorKind, PortFuture, RuntimeExtension, RuntimeReply, RuntimeRequest,
    RuntimeRequestHandler,
};
use serde_json::json;

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
        control: &RunControl,
    ) -> ledgence_worker_api::Result<WorkflowRuntime> {
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
        let wake_trace = context.wake.as_ref().and_then(|wake| match wake {
            WorkflowWake::Event { event, .. } => event.trace_context(),
            _ => None,
        });
        let payload = serde_json::to_value(context)
            .map_err(|error| Error::new(ErrorKind::Protocol, error.to_string()))?;
        let handler = Arc::new(LocalJournal {
            service,
            owner,
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
        match controlled(
            control,
            config.request_timeout,
            service.activation_context(owner),
        )
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
    context.validate().map_err(runtime_error)?;
    if context.activation_id != owner.task_id
        || context.workflow_id != expected_workflow
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

struct LocalJournal {
    service: Arc<dyn WorkflowService>,
    owner: LeaseOwner,
    request_timeout: Duration,
    retry_delay: Duration,
}
impl RuntimeRequestHandler for LocalJournal {
    fn handle<'a>(
        &'a self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> PortFuture<'a, RuntimeReply> {
        Box::pin(async move {
            request.validate()?;
            if request.operation != "local_step.commit" {
                return Err(Error::new(
                    ErrorKind::Protocol,
                    "unsupported workflow runtime operation",
                ));
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
                match controlled(
                    &control,
                    self.request_timeout,
                    self.service.record_local_result(&command),
                )
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

/// Cancellation can leave a write unconfirmed. The immutable command and the
/// store's lease fence/receipt make that uncertainty recoverable on replay.
async fn controlled<T>(
    control: &RunControl,
    timeout: Duration,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .unwrap_or_else(Instant::now)
        .min(control.deadline());
    tokio::pin!(future);
    loop {
        if control.check().is_err() {
            return Err(ContractError::OwnershipLost);
        }
        if Instant::now() >= deadline {
            return Err(ContractError::Unavailable(
                "workflow control request timed out; outcome is uncertain".into(),
            ));
        }
        tokio::select! {
            result = &mut future => {
                if Instant::now() >= deadline { return Err(ContractError::Unavailable("workflow control request completed after its deadline; outcome is uncertain".into())); }
                return result;
            },
            _ = tokio::time::sleep(TICK) => {},
        }
    }
}
async fn pause(control: &RunControl, delay: Duration) -> ledgence_worker_api::Result<()> {
    let until = Instant::now() + delay;
    while Instant::now() < until {
        control.check()?;
        tokio::time::sleep(TICK.min(until.saturating_duration_since(Instant::now()))).await;
    }
    control.check()
}
fn runtime_error(error: ContractError) -> Error {
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
