//! Workflow orchestration over an optional transactional store.
use super::*;
use ledgence_worker_api::ProgramDescriptor;
use std::{collections::BTreeMap, time::Duration};

const RESOLUTIONS_PER_WORK_ITEM: usize = 4;

impl ApplicationService {
    /// Enable workflow operations using the same transactional authority as the
    /// task store passed to `new`. The adapter must satisfy `WorkflowStore`'s
    /// atomic task/terminal-obligation invariants; unrelated stores cannot be
    /// combined safely by this application service.
    pub fn with_workflows(mut self, store: Arc<dyn WorkflowStore>) -> Self {
        self.workflows = Some(store);
        self
    }

    pub(super) fn workflows(&self) -> Result<&dyn WorkflowStore> {
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
        let mut resolved = Vec::new();
        let resolution = tokio::time::timeout_at(
            deadline,
            self.resolve_workflow_children(&work, &mut resolved),
        )
        .await;
        // A resolver may finish in a poll that crosses its deadline. Its result
        // cannot extend the bounded coordinator ownership window.
        if tokio::time::Instant::now() >= deadline {
            store
                .retry_work_with_bindings(&work, "program resolution deadline exceeded", &resolved)
                .await?;
            return Ok(WorkflowProgress::default());
        }
        let native = match resolution {
            Ok(Ok(native)) => native,
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
                store
                    .retry_work_with_bindings(&work, &error.to_string(), &resolved)
                    .await?;
                return Ok(WorkflowProgress::default());
            }
            Err(_) => {
                store
                    .retry_work_with_bindings(
                        &work,
                        "program resolution deadline exceeded",
                        &resolved,
                    )
                    .await?;
                return Ok(WorkflowProgress::default());
            }
        };

        let provider = matches!(
            work.source,
            WorkflowWorkSource::ProviderWait | WorkflowWorkSource::ProviderProbeTerminal { .. }
        ) || work.outcome.as_ref().is_some_and(|outcome| match outcome {
            TaskOutcome::Succeeded { output, .. } => {
                WorkflowDecision::decode(output).is_ok_and(|d| {
                    matches!(
                        d.action,
                        WorkflowAction::Wait {
                            wait: WorkflowWait::ProviderOperation { .. }
                                | WorkflowWait::Approval {
                                    protection: ApprovalProtection::ProviderOperation,
                                    ..
                                },
                            ..
                        }
                    )
                })
            }
            _ => false,
        });
        let admission = matches!(work.source, WorkflowWorkSource::AdmissionWait)
            || work.outcome.as_ref().is_some_and(|outcome| match outcome {
                TaskOutcome::Succeeded { output, .. } => WorkflowDecision::decode(output)
                    .is_ok_and(|d| {
                        matches!(
                            d.action,
                            WorkflowAction::Wait {
                                wait: WorkflowWait::ResourceAdmission { .. },
                                ..
                            }
                        )
                    }),
                _ => false,
            });
        let applied = if admission {
            self.admissions()?;
            store
                .admissions()?
                .apply_admission_work(&work, &resolved)
                .await
        } else if provider {
            self.provider_operations()?;
            store
                .provider_operations()?
                .apply_provider_work(&work, &resolved)
                .await
        } else if native {
            self.effect_resolution()?;
            store
                .effect_resolution()?
                .apply_effect_work(&work, &resolved)
                .await
        } else {
            store.apply_work(&work, &resolved).await
        };
        match applied {
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
                store
                    .retry_work_with_bindings(&work, &error.to_string(), &resolved)
                    .await?;
                Ok(WorkflowProgress::default())
            }
        }
    }

    pub(super) async fn resolve_workflow_children(
        &self,
        work: &WorkflowWork,
        resolved: &mut Vec<ResolvedWorkflowChild>,
    ) -> Result<bool> {
        let WorkflowWorkSource::TaskTerminal {
            task_id,
            activation: true,
        } = &work.source
        else {
            return Ok(matches!(
                work.source,
                WorkflowWorkSource::EffectResolutionWait
            ));
        };
        let outcome = work
            .outcome
            .as_ref()
            .ok_or_else(|| ContractError::Unavailable("missing controller outcome".into()))?;
        let TaskOutcome::Succeeded { output, .. } = outcome else {
            return Ok(matches!(
                work.source,
                WorkflowWorkSource::EffectResolutionWait
            ));
        };
        let decision = WorkflowDecision::decode(output)?;
        if matches!(
            &decision.action,
            WorkflowAction::Wait {
                wait: WorkflowWait::ResourceAdmission { .. },
                ..
            }
        ) {
            self.admissions().map_err(|_| {
                ContractError::InvalidInput("resource admission is unavailable".into())
            })?;
        }
        if matches!(
            &decision.action,
            WorkflowAction::Wait {
                wait: WorkflowWait::ProviderOperation { .. }
                    | WorkflowWait::Approval {
                        protection: ApprovalProtection::ProviderOperation,
                        ..
                    },
                ..
            }
        ) {
            self.provider_operations().map_err(|_| {
                ContractError::InvalidInput("provider operations are unavailable".into())
            })?;
        }
        if let WorkflowAction::Wait { wait, .. } = &decision.action
            && (matches!(
                wait,
                WorkflowWait::ResourceAdmission {
                    request: AdmissionWaitRequest::Provider { .. },
                    ..
                }
            ) || matches!(wait, WorkflowWait::Approval { provider_binding: Some(binding), .. } if binding.admission.is_some()))
        {
            self.provider_admissions().map_err(|_| {
                ContractError::InvalidInput("protected provider admission is unavailable".into())
            })?;
        }
        let native = matches!(
            decision.action,
            WorkflowAction::Wait {
                wait: WorkflowWait::EffectResolution { .. },
                ..
            }
        );
        if native {
            self.effect_resolution().map_err(|_| {
                ContractError::InvalidInput("native effect resolution is unavailable".into())
            })?;
        }
        if decision.activation_id != *task_id {
            return Err(ContractError::Conflict);
        }
        if work.resolved_children.len() > WORKFLOW_MAX_COMMANDS {
            return Err(ContractError::Unavailable(
                "registered workflow bindings exceed command limit".into(),
            ));
        }
        let commands = decision.commands();
        let positions: BTreeMap<_, _> = commands
            .iter()
            .enumerate()
            .map(|(index, command)| (command.key.as_str(), index))
            .collect();
        let mut registered = vec![false; commands.len()];
        for binding in &work.resolved_children {
            let &index = positions.get(binding.key.as_str()).ok_or_else(|| {
                ContractError::Unavailable(
                    "registered binding does not belong to workflow decision".into(),
                )
            })?;
            binding.descriptor.validate().map_err(|_| {
                ContractError::Unavailable("invalid registered workflow child descriptor".into())
            })?;
            if registered[index] {
                return Err(ContractError::Unavailable(
                    "duplicate registered workflow child binding".into(),
                ));
            }
            let command = &commands[index];
            if binding.kind != command.kind || binding.descriptor.program != command.program {
                return Err(ContractError::Conflict);
            }
            registered[index] = true;
        }
        resolved.extend_from_slice(&work.resolved_children);
        // Resolve each new program once. Keep the same pinned descriptor for all
        // newly resolved occurrences, without replacing existing child bindings.
        let mut groups = BTreeMap::<_, Vec<usize>>::new();
        for (index, command) in commands.iter().enumerate() {
            if !registered[index] {
                groups
                    .entry((&command.program.id, &command.program.version))
                    .or_default()
                    .push(index);
            }
        }
        let mut groups = groups.into_values();
        let mut pending = tokio::task::JoinSet::new();
        loop {
            while pending.len() < RESOLUTIONS_PER_WORK_ITEM
                && let Some(indices) = groups.next()
            {
                let service = self.clone();
                let program = commands[indices[0]].program.clone();
                pending.spawn(async move {
                    let descriptor = service.resolve_workflow_program(&program).await?;
                    Ok::<_, ContractError>((indices, descriptor))
                });
            }
            let Some(result) = pending.join_next().await else {
                break;
            };
            let (indices, descriptor) = result.map_err(|error| {
                ContractError::Unavailable(format!("workflow program resolver failed: {error}"))
            })??;
            for index in indices {
                let command = &commands[index];
                resolved.push(ResolvedWorkflowChild {
                    kind: command.kind,
                    key: command.key.clone(),
                    descriptor: descriptor.clone(),
                });
            }
        }
        resolved.sort_by_key(|binding| positions[binding.key.as_str()]);
        Ok(native)
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
        self.check_catalog_descriptor(&descriptor).await?;
        Ok(descriptor)
    }
}

impl ApprovedEffects for ApplicationService {
    fn claim_approved_effect<'a>(
        &'a self,
        command: &'a ApprovedEffectClaimCommand,
    ) -> ContractFuture<'a, ApprovedEffectClaimReceipt> {
        Box::pin(async move {
            command.validate()?;
            let receipt = self
                .workflows()?
                .approved_effects()?
                .claim_approved_effect(command)
                .await?;
            if !receipt.matches(command).unwrap_or(false) {
                return Err(ContractError::Unavailable(
                    "invalid approved effect claim reply".into(),
                ));
            }
            Ok(receipt)
        })
    }
    fn confirm_approved_effect<'a>(
        &'a self,
        command: &'a ApprovedEffectConfirmCommand,
    ) -> ContractFuture<'a, ApprovedEffectConfirmReceipt> {
        Box::pin(async move {
            command.validate()?;
            let receipt = self
                .workflows()?
                .approved_effects()?
                .confirm_approved_effect(command)
                .await?;
            if !receipt.matches(command).unwrap_or(false) {
                return Err(ContractError::Unavailable(
                    "invalid approved effect confirmation reply".into(),
                ));
            }
            Ok(receipt)
        })
    }
}

impl WorkflowService for ApplicationService {
    fn provider_admissions(&self) -> Result<&dyn ProviderAdmissionOperations> {
        self.admissions()?;
        self.provider_operations()?;
        self.workflows()?.provider_admissions()?;
        Ok(self)
    }

    fn admissions(&self) -> Result<&dyn AdmissionOperations> {
        self.workflows()?.admissions()?;
        self.workflows()?.operator_access()?;
        Ok(self)
    }
    fn provider_operations(&self) -> Result<&dyn ProviderOperations> {
        self.effect_resolution()?;
        self.workflows()?.provider_operations()?;
        Ok(self)
    }
    fn effect_resolution(&self) -> Result<&dyn EffectResolution> {
        self.workflows()?.effect_resolution()?;
        self.workflows()?.approved_effect_evidence()?;
        self.workflows()?.effect_evidence_reader()?;
        self.workflows()?.operator_access()?;
        Ok(self)
    }
    fn approved_effect_evidence(&self) -> Result<&dyn ApprovedEffectEvidence> {
        self.workflows()?.approved_effect_evidence()?;
        self.workflows()?.effect_evidence_reader()?;
        self.workflows()?.operator_access()?;
        Ok(self)
    }
    fn effect_evidence_reader(&self) -> Result<&dyn EffectEvidenceReader> {
        self.workflows()?.effect_evidence_reader()?;
        Ok(self)
    }

    fn operator_access(&self) -> Result<&dyn OperatorAccess> {
        self.workflows()?.operator_access()?;
        Ok(self)
    }
    fn approved_effects(&self) -> Result<&dyn ApprovedEffects> {
        self.workflows()?.approved_effects()?;
        Ok(self)
    }
    fn approval<'a>(
        &'a self,
        scope: &'a Scope,
        workflow_id: &'a str,
        key: &'a str,
    ) -> ContractFuture<'a, ApprovalSnapshot> {
        Box::pin(async move {
            scope.validate()?;
            validate_text(workflow_id, 128)?;
            validate_text(key, 128)?;
            let reply = self.workflows()?.approval(scope, workflow_id, key).await?;
            reply.validate().map_err(|_| approval_reply_error())?;
            if &reply.scope != scope || reply.workflow_id != workflow_id || reply.key != key {
                return Err(approval_reply_error());
            }
            Ok(reply)
        })
    }
    fn approval_inbox<'a>(
        &'a self,
        query: &'a ApprovalInboxQuery,
    ) -> ContractFuture<'a, ApprovalInboxPage> {
        Box::pin(async move {
            query.validate()?;
            let reply = self.workflows()?.approval_inbox(query).await?;
            reply
                .validate_for(query)
                .map_err(|_| approval_reply_error())?;
            Ok(reply)
        })
    }
    fn list_approvals<'a>(
        &'a self,
        scope: &'a Scope,
        workflow_id: &'a str,
        after_key: Option<&'a str>,
        limit: u32,
    ) -> ContractFuture<'a, ApprovalPage> {
        Box::pin(async move {
            scope.validate()?;
            validate_text(workflow_id, 128)?;
            validate_approval_page(after_key, limit)?;
            let reply = self
                .workflows()?
                .list_approvals(scope, workflow_id, after_key, limit)
                .await?;
            reply.validate().map_err(|_| approval_reply_error())?;
            if !reply.matches(scope, workflow_id, after_key, limit) {
                return Err(approval_reply_error());
            }
            Ok(reply)
        })
    }
    fn decide_approval<'a>(
        &'a self,
        command: &'a ApprovalDecisionCommand,
    ) -> ContractFuture<'a, ApprovalDecisionReceipt> {
        Box::pin(async move {
            command.validate()?;
            let reply = self.workflows()?.decide_approval(command).await?;
            validate_approval_decision_reply(command, reply)
        })
    }
    fn fork_workflow<'a>(
        &'a self,
        command: &'a WorkflowForkCommand,
    ) -> ContractFuture<'a, WorkflowForkReceipt> {
        Box::pin(async move {
            command.validate()?;
            // Forks reuse the parent's pinned descriptor inside the store's
            // transaction; mutable catalogs and program locators play no role.
            let receipt = self.workflows()?.fork_workflow(command).await?;
            receipt
                .validate()
                .map_err(|_| ContractError::Unavailable("invalid workflow fork receipt".into()))?;
            if !receipt.matches(command) {
                return Err(ContractError::Unavailable(
                    "workflow fork receipt identity mismatch".into(),
                ));
            }
            Ok(receipt)
        })
    }
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
            if command.input.admission.is_some() {
                return Err(ContractError::InvalidInput(
                    "task admission requires a standalone task".into(),
                ));
            }
            self.require_catalog_scope(command)?;
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
            validate_activation_context_reply(owner, context)
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

fn approval_reply_error() -> ContractError {
    ContractError::Unavailable("invalid approval service response".into())
}

pub(super) fn validate_approval_decision_reply(
    command: &ApprovalDecisionCommand,
    reply: ApprovalDecisionReceipt,
) -> Result<ApprovalDecisionReceipt> {
    reply.validate().map_err(|_| approval_reply_error())?;
    if !reply.matches(command).map_err(|_| approval_reply_error())? {
        return Err(approval_reply_error());
    }
    Ok(reply)
}

pub(super) fn validate_activation_context_reply(
    owner: &LeaseOwner,
    context: WorkflowActivationContext,
) -> Result<WorkflowActivationContext> {
    context.validate_for_owner(owner)?;
    Ok(context)
}

/// Validate every receipt before allowing a transport to expose user payloads.
impl OperatorAccess for ApplicationService {
    fn decide_verified<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ApprovalDecisionCommand,
        request: &'a OperatorRequest,
    ) -> ContractFuture<'a, AuditedDecision> {
        Box::pin(async move {
            command.validate()?;
            principal.require(AccessCapability::ApprovalDecide)?;
            principal.require(AccessCapability::SensitiveRead)?;
            if command.scope != principal.audience().scope {
                return Err(ContractError::Forbidden);
            }
            let result = self
                .workflows()?
                .operator_access()?
                .decide_verified(principal, command, request)
                .await?;
            result.validate_for(principal, command)?;
            Ok(result)
        })
    }
    fn record_sensitive_response<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        observation: &'a SensitiveObservation,
    ) -> ContractFuture<'a, AuditReceipt> {
        Box::pin(async move {
            observation.validate()?;
            principal.require(AccessCapability::SensitiveRead)?;
            let receipt = self
                .workflows()?
                .operator_access()?
                .record_sensitive_response(principal, observation)
                .await?;
            receipt.validate_for(principal, observation)?;
            Ok(receipt)
        })
    }
    fn query_audit<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        query: &'a AuditQuery,
    ) -> ContractFuture<'a, AuditPage> {
        Box::pin(async move {
            query.validate()?;
            principal.require(AccessCapability::MetadataRead)?;
            if query.scope != principal.audience().scope {
                return Err(ContractError::Forbidden);
            }
            let page = self
                .workflows()?
                .operator_access()?
                .query_audit(principal, query)
                .await?;
            page.validate_for(query, principal.audience())?;
            Ok(page)
        })
    }
}
