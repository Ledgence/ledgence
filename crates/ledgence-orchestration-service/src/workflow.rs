//! Workflow orchestration over an optional transactional store.
use super::*;
use ledgence_worker_api::ProgramDescriptor;
use std::time::Duration;

impl ApplicationService {
    /// Enable workflow operations using the same transactional authority as the
    /// task store passed to `new`. The adapter must satisfy `WorkflowStore`'s
    /// atomic task/terminal-obligation invariants; unrelated stores cannot be
    /// combined safely by this application service.
    pub fn with_workflows(mut self, store: Arc<dyn WorkflowStore>) -> Self {
        self.workflows = Some(store);
        self
    }

    fn workflows(&self) -> Result<&dyn WorkflowStore> {
        self.workflows
            .as_deref()
            .ok_or_else(|| ContractError::InvalidInput("workflow support is not configured".into()))
    }

    /// Apply a bounded batch of durable completion obligations. Each item has
    /// independent ownership/backoff; no worker reservation waits for this loop.
    pub async fn advance_workflows(&self, limit: u32) -> Result<WorkflowProgress> {
        if !(1..=WORKFLOW_MAX_WORK_BATCH).contains(&limit) {
            return Err(ContractError::InvalidInput(
                "invalid workflow recovery batch".into(),
            ));
        }
        let work = self.workflows()?.claim_work(limit).await?;
        if work.len() > limit as usize {
            return Err(ContractError::Unavailable(
                "workflow store exceeded requested batch".into(),
            ));
        }
        let mut pending = tokio::task::JoinSet::new();
        for item in work {
            let service = self.clone();
            pending.spawn(async move { service.apply_workflow_item(item).await });
        }
        let mut total = WorkflowProgress::default();
        let mut failure = None;
        while let Some(result) = pending.join_next().await {
            match result {
                Ok(Ok(progress)) => {
                    total.processed += progress.processed;
                    total.activations_scheduled += progress.activations_scheduled;
                    total.children_scheduled += progress.children_scheduled;
                }
                Ok(Err(error)) => failure = Some(error),
                Err(error) => {
                    failure = Some(ContractError::Unavailable(format!(
                        "workflow coordinator task failed: {error}"
                    )))
                }
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(total),
        }
    }

    async fn apply_workflow_item(&self, work: WorkflowWork) -> Result<WorkflowProgress> {
        let store = self.workflows()?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let resolution =
            tokio::time::timeout_at(deadline, self.resolve_workflow_children(&work)).await;
        // A resolver may finish in a poll that crosses its deadline. Its result
        // cannot extend the bounded coordinator ownership window.
        if tokio::time::Instant::now() >= deadline {
            store
                .retry_work(&work, "program resolution deadline exceeded")
                .await?;
            return Ok(WorkflowProgress::default());
        }
        let resolved = match resolution {
            Ok(Ok(children)) => children,
            Ok(Err(error))
                if matches!(
                    error,
                    ContractError::InvalidInput(_)
                        | ContractError::NotFound
                        | ContractError::Conflict
                ) =>
            {
                store
                    .reject_work(&work, &application_failure("decision_invalid", &error))
                    .await?;
                return Ok(WorkflowProgress {
                    processed: 1,
                    ..Default::default()
                });
            }
            Ok(Err(error)) => {
                store.retry_work(&work, &error.to_string()).await?;
                return Ok(WorkflowProgress::default());
            }
            Err(_) => {
                store
                    .retry_work(&work, "program resolution deadline exceeded")
                    .await?;
                return Ok(WorkflowProgress::default());
            }
        };
        match store.apply_work(&work, &resolved).await {
            Ok(progress) => Ok(progress),
            // Another coordinator may have recovered an expired work lease.
            Err(ContractError::OwnershipLost | ContractError::ObsoleteOperation) => {
                Ok(WorkflowProgress::default())
            }
            Err(
                error @ (ContractError::InvalidInput(_)
                | ContractError::NotFound
                | ContractError::Conflict),
            ) => {
                store
                    .reject_work(
                        &work,
                        &application_failure("decision_application_failed", &error),
                    )
                    .await?;
                Ok(WorkflowProgress {
                    processed: 1,
                    ..Default::default()
                })
            }
            Err(error) => {
                store.retry_work(&work, &error.to_string()).await?;
                Ok(WorkflowProgress::default())
            }
        }
    }

    pub(super) async fn resolve_workflow_children(
        &self,
        work: &WorkflowWork,
    ) -> Result<Vec<ResolvedWorkflowChild>> {
        let WorkflowWorkSource::TaskTerminal {
            task_id,
            activation: true,
        } = &work.source
        else {
            return Ok(Vec::new());
        };
        let outcome = work
            .outcome
            .as_ref()
            .ok_or_else(|| ContractError::Unavailable("missing controller outcome".into()))?;
        let TaskOutcome::Succeeded { output, .. } = outcome else {
            return Ok(Vec::new());
        };
        let decision = WorkflowDecision::decode(output)?;
        if decision.activation_id != *task_id {
            return Err(ContractError::Conflict);
        }
        if work.resolved_children.len() > WORKFLOW_MAX_COMMANDS {
            return Err(ContractError::Unavailable(
                "registered workflow bindings exceed command limit".into(),
            ));
        }
        let command_keys: std::collections::BTreeSet<_> = decision
            .commands()
            .iter()
            .map(|command| command.key.as_str())
            .collect();
        let mut registered = std::collections::BTreeMap::new();
        for binding in &work.resolved_children {
            if !command_keys.contains(binding.key.as_str()) {
                return Err(ContractError::Unavailable(
                    "registered binding does not belong to workflow decision".into(),
                ));
            }
            binding.descriptor.validate().map_err(|_| {
                ContractError::Unavailable("invalid registered workflow child descriptor".into())
            })?;
            if registered
                .insert(
                    binding.key.clone(),
                    (binding.kind, binding.descriptor.clone()),
                )
                .is_some()
            {
                return Err(ContractError::Unavailable(
                    "duplicate registered workflow child binding".into(),
                ));
            }
        }
        let mut newly_resolved = std::collections::BTreeMap::new();
        let mut resolved = Vec::with_capacity(decision.commands().len());
        for command in decision.commands() {
            let program_key = (command.program.id.clone(), command.program.version.clone());
            let descriptor = if let Some((kind, descriptor)) = registered.get(&command.key) {
                if *kind != command.kind || descriptor.program != command.program {
                    return Err(ContractError::Conflict);
                }
                descriptor.clone()
            } else if let Some(descriptor) = newly_resolved.get(&program_key) {
                ProgramDescriptor::clone(descriptor)
            } else {
                let descriptor = self.resolve_workflow_program(&command.program).await?;
                newly_resolved.insert(program_key, descriptor.clone());
                descriptor
            };
            resolved.push(ResolvedWorkflowChild {
                kind: command.kind,
                key: command.key.clone(),
                descriptor,
            });
        }
        Ok(resolved)
    }

    async fn resolve_workflow_program(
        &self,
        program: &ledgence_worker_api::ProgramRef,
    ) -> Result<ProgramDescriptor> {
        let descriptor = self
            .programs
            .resolve(program)
            .await
            .map_err(resolution_error)?;
        descriptor.validate().map_err(|error| {
            ContractError::Unavailable(format!("invalid resolved workflow program: {error}"))
        })?;
        if descriptor.program != *program {
            return Err(ContractError::Unavailable(
                "program store resolved a different workflow program".into(),
            ));
        }
        Ok(descriptor)
    }
}

impl WorkflowService for ApplicationService {
    fn send_workflow_event<'a>(
        &'a self,
        command: &'a WorkflowEventCommand,
    ) -> ContractFuture<'a, WorkflowEventReceipt> {
        Box::pin(async move {
            command.validate()?;
            let receipt = self.workflows()?.send_workflow_event(command).await?;
            receipt
                .validate()
                .map_err(|_| ContractError::Unavailable("invalid workflow event receipt".into()))?;
            if !receipt.matches(command) {
                return Err(ContractError::Unavailable(
                    "workflow event receipt identity mismatch".into(),
                ));
            }
            Ok(receipt)
        })
    }
    fn submit_workflow<'a>(
        &'a self,
        command: &'a SubmitCommand,
    ) -> ContractFuture<'a, WorkflowSnapshot> {
        Box::pin(async move {
            validate_submission(command)?;
            let store = self.workflows()?;
            if let Some(accepted) = store.replay_workflow_submission(command).await? {
                return validate_workflow_submission_reply(command, accepted);
            }
            let resolved = self.resolve_workflow_program(&command.input.program).await;
            let descriptor = match resolved {
                Ok(descriptor) => descriptor,
                Err(error) => {
                    return match store.replay_workflow_submission(command).await? {
                        Some(accepted) => validate_workflow_submission_reply(command, accepted),
                        None => Err(error),
                    };
                }
            };
            let accepted = store.accept_resolved_workflow(command, &descriptor).await?;
            validate_workflow_submission_reply(command, accepted)
        })
    }
    fn workflow_status<'a>(
        &'a self,
        scope: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowSnapshot> {
        Box::pin(async move {
            scope.validate()?;
            validate_text(id, 128)?;
            self.workflows()?.workflow_status(scope, id).await
        })
    }
    fn workflow_result<'a>(
        &'a self,
        scope: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowResult> {
        Box::pin(async move {
            scope.validate()?;
            validate_text(id, 128)?;
            self.workflows()?.workflow_result(scope, id).await
        })
    }
    fn activation_context<'a>(
        &'a self,
        owner: &'a LeaseOwner,
    ) -> ContractFuture<'a, WorkflowActivationContext> {
        Box::pin(async move {
            let context = self.workflows()?.activation_context(owner).await?;
            context.validate()?;
            if context.activation_id != owner.task_id {
                return Err(ContractError::Conflict);
            }
            Ok(context)
        })
    }
    fn record_local_result<'a>(
        &'a self,
        command: &'a LocalResultCommand,
    ) -> ContractFuture<'a, LocalResultReceipt> {
        Box::pin(async move {
            command.record.validate()?;
            self.workflows()?.record_local_result(command).await
        })
    }
    fn cancel_workflow<'a>(
        &'a self,
        scope: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, WorkflowSnapshot> {
        Box::pin(async move {
            scope.validate()?;
            validate_text(id, 128)?;
            self.workflows()?.cancel_workflow(scope, id).await
        })
    }
}

fn application_failure(kind: &str, error: &ContractError) -> ApplicationError {
    let mut message = error.to_string();
    while message.len() > 4096 {
        message.pop();
    }
    ApplicationError {
        kind: kind.into(),
        message,
    }
}

// Submission can already have committed. Invalid adapter replies are uncertain,
// never definitive rejections or permission to adopt an owned child as a root.
pub(super) fn validate_workflow_submission_reply(
    command: &SubmitCommand,
    accepted: WorkflowSnapshot,
) -> Result<WorkflowSnapshot> {
    accepted
        .validate()
        .map_err(|_| ContractError::Unavailable("invalid workflow submission reply".into()))?;
    if accepted.scope.tenant_id != command.input.tenant_id
        || accepted.scope.namespace != command.input.namespace
        || accepted.correlation_key != command.input.correlation_key
        || accepted.parent_workflow_id.is_some()
        || accepted.root_workflow_id.is_some()
    {
        return Err(ContractError::Unavailable(
            "workflow submission reply identity mismatch".into(),
        ));
    }
    Ok(accepted)
}
