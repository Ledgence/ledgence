use super::*;

impl WorkflowService for HttpTaskService {
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
                    if reply.scope != scope || reply.correlation_key != correlation {
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
            self.post_validated(
                "v1/workflows/activations/context",
                owner,
                SUBMISSION_MAX_BYTES,
                move |reply: &WorkflowActivationContext| {
                    if reply.activation_id != activation {
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
