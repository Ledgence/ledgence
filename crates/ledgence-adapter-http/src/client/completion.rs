use super::*;

impl CompletionService for HttpTaskService {
    fn subscribe_completion<'a>(
        &'a self,
        command: &'a CompletionSubscribeCommand,
    ) -> ContractFuture<'a, CompletionSubscription> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/completion-subscriptions",
                command,
                COMPLETION_COMMAND_MAX_BYTES,
                move |reply: &CompletionSubscription| {
                    if !reply.matches(&expected) {
                        return Err(unavailable(
                            "completion subscription response identity mismatch",
                        ));
                    }
                    Ok(())
                },
            )
            .await
        })
    }

    fn completion_status<'a>(
        &'a self,
        scope: &'a Scope,
        id: &'a str,
    ) -> ContractFuture<'a, CompletionSubscription> {
        Box::pin(async move {
            scope.validate()?;
            validate_text(id, 128)?;
            let fields = vec![
                ("tenant_id", scope.tenant_id.clone()),
                ("namespace", scope.namespace.clone()),
                ("subscription_id", id.to_owned()),
            ];
            let expected_scope = scope.clone();
            let expected_id = id.to_owned();
            self.get_validated(
                "v1/completion-subscriptions/status",
                &fields,
                move |reply: &CompletionSubscription| {
                    check_identity(reply, &expected_scope, &expected_id)
                },
            )
            .await
        })
    }

    fn retry_completion<'a>(
        &'a self,
        command: &'a CompletionRetryCommand,
    ) -> ContractFuture<'a, CompletionSubscription> {
        Box::pin(async move {
            command.validate()?;
            let expected = command.clone();
            self.post_validated(
                "v1/completion-subscriptions/retry",
                command,
                COMPLETION_COMMAND_MAX_BYTES,
                move |reply: &CompletionSubscription| {
                    check_identity(reply, &expected.scope, &expected.subscription_id)?;
                    if reply.generation <= expected.expected_generation {
                        return Err(unavailable(
                            "completion retry response did not acknowledge generation",
                        ));
                    }
                    Ok(())
                },
            )
            .await
        })
    }
}
fn check_identity(reply: &CompletionSubscription, scope: &Scope, id: &str) -> Result<()> {
    if &reply.command.scope != scope || reply.subscription_id != id {
        return Err(unavailable(
            "completion response identity disagrees with request",
        ));
    }
    Ok(())
}
