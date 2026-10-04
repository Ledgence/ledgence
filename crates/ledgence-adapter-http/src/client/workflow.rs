use super::*;

impl WorkflowService for HttpTaskService {
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
            let request = ApprovalReference {
                scope: scope.clone(),
                workflow_id: workflow_id.into(),
                key: key.into(),
            };
            let expected = request.clone();
            self.post_validated(
                "v1/workflows/approvals/inspect",
                &request,
                APPROVAL_SNAPSHOT_MAX_BYTES,
                move |reply: &ApprovalSnapshot| {
                    if reply.scope != expected.scope
                        || reply.workflow_id != expected.workflow_id
                        || reply.key != expected.key
                    {
                        return Err(unavailable("approval response identity mismatch"));
                    }
                    Ok(())
                },
            )
            .await
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
            let request = ApprovalListRequest {
                scope: scope.clone(),
                workflow_id: workflow_id.into(),
                after_key: after_key.map(str::to_owned),
                limit,
            };
            let expected = request.clone();
            self.post_validated(
                "v1/workflows/approvals/list",
                &request,
                APPROVAL_SNAPSHOT_MAX_BYTES,
                move |reply: &ApprovalPage| {
                    if !reply.matches(
                        &expected.scope,
                        &expected.workflow_id,
                        expected.after_key.as_deref(),
                        expected.limit,
                    ) {
                        return Err(unavailable("approval page identity mismatch"));
                    }
                    Ok(())
                },
            )
            .await
        })
    }
    fn decide_approval<'a>(
        &'a self,
        command: &'a ApprovalDecisionCommand,
    ) -> ContractFuture<'a, ApprovalDecisionReceipt> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/workflows/approvals/decide",
                command,
                APPROVAL_SNAPSHOT_MAX_BYTES,
                move |reply: &ApprovalDecisionReceipt| {
                    if !reply
                        .matches(&expected)
                        .map_err(|_| unavailable("invalid approval receipt"))?
                    {
                        return Err(unavailable("approval receipt identity mismatch"));
                    }
                    Ok(())
                },
            )
            .await
        })
    }
    fn submit_workflow<'a>(
        &'a self,
        command: &'a SubmitCommand,
    ) -> ContractFuture<'a, WorkflowSnapshot> {
        Box::pin(async move {
            let scope = Scope {
                tenant_id: command.input.tenant_id.clone(),
                namespace: command.input.namespace.clone(),
            };
            let correlation = command.input.correlation_key.clone();
            self.post_validated(
                "v1/workflows",
                command,
                SUBMISSION_MAX_BYTES,
                move |reply: &WorkflowSnapshot| {
                    if reply.scope != scope
                        || reply.correlation_key != correlation
                        || reply.parent_workflow_id.is_some()
                        || reply.root_workflow_id.is_some()
                    {
                        return Err(unavailable(
                            "workflow submission response identity mismatch",
                        ));
                    }
                    Ok(())
                },
            )
            .await
        })
    }
    fn send_workflow_event<'a>(
        &'a self,
        command: &'a WorkflowEventCommand,
    ) -> ContractFuture<'a, WorkflowEventReceipt> {
        Box::pin(async move {
            command.validate()?;
            let scope = command.scope.clone();
            let workflow_id = command.workflow_id.clone();
            let key = command.key.clone();
            let event_id = command.event.id().to_owned();
            let event_source = command.event.source().to_owned();
            self.post_validated(
                "v1/workflows/events",
                command,
                WORKFLOW_EVENT_COMMAND_MAX_BYTES,
                move |reply: &WorkflowEventReceipt| {
                    if reply.scope != scope
                        || reply.workflow_id != workflow_id
                        || reply.key != key
                        || reply.event_id != event_id
                        || reply.event_source != event_source
                    {
                        return Err(unavailable("workflow event receipt identity mismatch"));
                    }
                    Ok(())
                },
            )
            .await
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
            let fields = workflow_query(scope, id);
            let expected_scope = scope.clone();
            let expected_id = id.to_owned();
            self.get_validated(
                "v1/workflows/status",
                &fields,
                move |reply: &WorkflowSnapshot| {
                    check_identity(reply, &expected_scope, &expected_id)
                },
            )
            .await
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
            let fields = workflow_query(scope, id);
            let expected_scope = scope.clone();
            let expected_id = id.to_owned();
            self.get_validated(
                "v1/workflows/result",
                &fields,
                move |reply: &WorkflowResult| {
                    check_identity(&reply.workflow, &expected_scope, &expected_id)
                },
            )
            .await
        })
    }
    fn activation_context<'a>(
        &'a self,
        owner: &'a LeaseOwner,
    ) -> ContractFuture<'a, WorkflowActivationContext> {
        Box::pin(async move {
            let activation = owner.task_id.clone();
            let scope = owner.scope.clone();
            self.post_validated(
                "v1/workflows/activations/context",
                owner,
                SUBMISSION_MAX_BYTES,
                move |reply: &WorkflowActivationContext| {
                    if reply.activation_id != activation
                        || matches!(&reply.wake, Some(WorkflowWake::Approval { approval }) if approval.scope != scope)
                    {
                        return Err(unavailable("workflow activation identity mismatch"));
                    }
                    Ok(())
                },
            )
            .await
        })
    }
    fn record_local_result<'a>(
        &'a self,
        command: &'a LocalResultCommand,
    ) -> ContractFuture<'a, LocalResultReceipt> {
        Box::pin(async move {
            command.record.validate()?;
            let key = command.record.key.clone();
            self.post_validated(
                "v1/workflows/local-results",
                command,
                SUBMISSION_MAX_BYTES,
                move |reply: &LocalResultReceipt| {
                    if reply.key != key {
                        return Err(unavailable("workflow local receipt identity mismatch"));
                    }
                    Ok(())
                },
            )
            .await
        })
    }
    fn fork_workflow<'a>(
        &'a self,
        command: &'a WorkflowForkCommand,
    ) -> ContractFuture<'a, WorkflowForkReceipt> {
        Box::pin(async move {
            command.validate()?;
            let key = command.fork.key.clone();
            let branch_keys: Vec<_> = command
                .fork
                .branches
                .iter()
                .map(|branch| branch.key.clone())
                .collect();
            self.post_validated(
                "v1/workflows/forks",
                command,
                WORKFLOW_FORK_COMMAND_MAX_BYTES,
                move |reply: &WorkflowForkReceipt| {
                    if reply.key != key || reply.branch_keys != branch_keys {
                        return Err(unavailable("workflow fork receipt identity mismatch"));
                    }
                    Ok(())
                },
            )
            .await
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
            let expected_scope = scope.clone();
            let expected_id = id.to_owned();
            self.post_validated(
                "v1/workflows/cancel",
                &WorkflowReference {
                    scope: scope.clone(),
                    workflow_id: id.into(),
                },
                SUBMISSION_MAX_BYTES,
                move |reply: &WorkflowSnapshot| {
                    check_identity(reply, &expected_scope, &expected_id)
                },
            )
            .await
        })
    }
}
fn workflow_query(scope: &Scope, id: &str) -> Vec<(&'static str, String)> {
    vec![
        ("tenant_id", scope.tenant_id.clone()),
        ("namespace", scope.namespace.clone()),
        ("workflow_id", id.into()),
    ]
}
fn check_identity(reply: &WorkflowSnapshot, scope: &Scope, id: &str) -> Result<()> {
    if &reply.scope != scope || reply.workflow_id != id {
        return Err(unavailable(
            "workflow response identity disagrees with request",
        ));
    }
    Ok(())
}
