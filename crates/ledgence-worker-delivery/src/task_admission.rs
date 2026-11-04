//! Host-owned ready grant and source-bound cleanup for a plain task invocation.
use super::*;
use ledgence_worker_api::{
    AdmittedExecutionResult, Error, ErrorKind, InvocationAdmission, InvocationAdmissionDecision,
    InvocationPermit, InvocationReady, PortFuture,
};

pub(super) struct TaskAdmissionAttempt {
    service: Arc<dyn TaskService>,
    access: Arc<EffectAccess>,
    command: TaskAdmissionStartCommand,
    binding: TaskAdmissionBinding,
    timeout: Duration,
    requested: AtomicBool,
    source: Mutex<Option<TaskAdmissionSource>>,
}
impl TaskAdmissionAttempt {
    pub(super) fn new(context: &Context, assignment: &Assignment) -> Result<Option<Arc<Self>>> {
        let Some(requirement) = &assignment.task_admission else {
            return Ok(None);
        };
        if !context.runtime_features.task_admission()
            || assignment.workflow_activation_id.is_some()
            || assignment.provider_probe.is_some()
        {
            return Err(protocol(
                "task admission was not negotiated for this plain task",
            ));
        }
        requirement.validate()?;
        context.service.task_admission_client()?;
        let access = context
            .runtime_binding
            .clone()
            .ok_or_else(|| protocol("task admission lacks authenticated worker identity"))?;
        let command = TaskAdmissionStartCommand {
            owner: assignment.lease.owner.clone(),
            offer_generation: requirement.offer_generation,
            start_id: "worker-task-start-v1".into(),
        };
        let binding = TaskAdmissionBinding {
            occurrence: TaskAdmissionOccurrence::from(&command.owner),
            resource: requirement.resource.clone(),
            program: assignment.descriptor.clone(),
        };
        command.validate()?;
        binding.validate()?;
        Ok(Some(Arc::new(Self {
            service: context.service.clone(),
            access,
            command,
            binding,
            timeout: context.config.request_timeout,
            requested: AtomicBool::new(false),
            source: Mutex::new(None),
        })))
    }
    fn matches_source(&self, source: &TaskAdmissionSource) -> Result<bool> {
        Ok(source.matches(&self.command)
            && source.matches_binding(&self.binding)?
            && source.worker.audience == self.access.audience
            && source.worker.issuer == self.access.identity.issuer
            && source.worker.subject == self.access.identity.subject)
    }
    pub(super) async fn execute(
        self: &Arc<Self>,
        reservation: &mut ledgence_worker_core::ConsumerReservation,
        request: ledgence_worker_api::ExecutionRequest,
        control: RunControl,
    ) -> Option<AttemptReport> {
        match reservation
            .execute_admitted(request, control.clone(), self.clone())
            .await
        {
            AdmittedExecutionResult::NotInvoked { .. } => None,
            AdmittedExecutionResult::Executed(Ok(report)) => Some(AttemptReport::Completed(report)),
            AdmittedExecutionResult::Executed(Err(failure)) => {
                // A ready gate (including a lost grant response) never reached
                // the runtime. Close determines whether this attempt is free or
                // charged. Actual preparation failures keep normal retry policy.
                if !failure.execution_may_have_started
                    && (self.requested.load(Ordering::Acquire)
                        || (failure.error.kind == ErrorKind::Cancelled && control.is_cancelled()))
                {
                    None
                } else {
                    Some(AttemptReport::Failed(failure))
                }
            }
        }
    }
    pub(super) async fn close(
        &self,
        context: &Context,
        outcome: TaskAdmissionCloseOutcome,
    ) -> Option<TaskAdmissionCloseReceipt> {
        let command = TaskAdmissionCloseCommand {
            owner: self.command.owner.clone(),
            offer_generation: self.command.offer_generation,
            start_id: self.command.start_id.clone(),
            operation_id: "worker-task-close-v1".into(),
            outcome,
        };
        loop {
            let reply = exchange(self.timeout, || async {
                self.service
                    .task_admission_client()?
                    .close_task_admission(&command)
                    .await
            })
            .await;
            match reply {
                Ok(reply)
                    if reply.validate().is_ok()
                        && reply.matches(&command)
                        && reply
                            .source
                            .as_ref()
                            .is_none_or(|source| self.matches_source(source).unwrap_or(false))
                        && self
                            .source
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .as_ref()
                            .is_none_or(|source| reply.source.as_ref() == Some(source)) =>
                {
                    return Some(reply);
                }
                Ok(_) => {
                    context.shared.fatal(protocol(
                        "task admission close receipt changed original authority",
                    ));
                    return None;
                }
                Err(error) if retryable(&error) => context.shared.error(error),
                Err(error) => {
                    // Drain this worker. Never manufacture a release receipt: the
                    // original ledger remains available for operator reconciliation.
                    context.shared.fatal(error);
                    return None;
                }
            }
            // Closing is retained even after cancellation or lease loss. Neither
            // one is proof that original occupancy was released remotely.
            tokio::time::sleep(context.config.retry_delay).await;
        }
    }
}
impl InvocationAdmission for TaskAdmissionAttempt {
    fn ready<'a>(
        &'a self,
        ready: &'a InvocationReady,
        control: RunControl,
    ) -> PortFuture<'a, InvocationAdmissionDecision> {
        Box::pin(async move {
            if ready.context.identity.tenant_id != self.command.owner.scope.tenant_id
                || ready.context.identity.namespace != self.command.owner.scope.namespace
                || ready.context.identity.attempt_no != self.command.owner.generation
                || ready.context.identity.task_id != self.command.owner.task_id
                || ready.context.identity.attempt_id != self.command.owner.attempt_id
                || ready.context.program != self.binding.program.program
                || ready.context.digest != self.binding.program.digest
            {
                return Err(Error::new(
                    ErrorKind::Protocol,
                    "prepared invocation differs from task admission",
                ));
            }
            control.check()?;
            if self.requested.swap(true, Ordering::AcqRel) {
                return Err(Error::new(
                    ErrorKind::Protocol,
                    "task admission Start is one-shot",
                ));
            }
            let began = Instant::now();
            let client = self
                .service
                .task_admission_client()
                .map_err(workflow::runtime_error)?;
            let reply = workflow::controlled(&control, self.timeout, || {
                client.start_task_admission(&self.command)
            })
            .await
            .map_err(workflow::runtime_error)?;
            control.check()?;
            if !reply
                .matches(&self.command)
                .map_err(workflow::runtime_error)?
            {
                return Err(Error::new(
                    ErrorKind::Protocol,
                    "task admission Start identity mismatch",
                ));
            }
            match reply {
                TaskAdmissionStartReceipt::Granted { source, .. } => {
                    if !self
                        .matches_source(&source)
                        .map_err(workflow::runtime_error)?
                    {
                        return Err(Error::new(
                            ErrorKind::Protocol,
                            "task admission source binding mismatch",
                        ));
                    }
                    let permit = InvocationPermit {
                        start_before: began
                            + Duration::from_millis(source.start_deadline_at - source.accepted_at),
                    };
                    *self.source.lock().unwrap_or_else(|p| p.into_inner()) = Some(*source);
                    permit.check()?;
                    Ok(InvocationAdmissionDecision::Invoke(permit))
                }
                TaskAdmissionStartReceipt::Existing { snapshot } => {
                    if snapshot.binding() != self.binding
                        || snapshot
                            .source
                            .as_ref()
                            .is_some_and(|source| !self.matches_source(source).unwrap_or(false))
                    {
                        return Err(Error::new(
                            ErrorKind::Protocol,
                            "task admission existing state mismatch",
                        ));
                    }
                    *self.source.lock().unwrap_or_else(|p| p.into_inner()) = snapshot.source;
                    Ok(InvocationAdmissionDecision::DoNotInvoke)
                }
            }
        })
    }
}
