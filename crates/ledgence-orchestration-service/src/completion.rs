use crate::*;

impl ApplicationService {
    pub fn with_completions(mut self, store: Arc<dyn CompletionStore>) -> Self {
        self.completions = Some(store);
        self
    }
    fn completion_store(&self) -> Result<&dyn CompletionStore> {
        self.completions.as_deref().ok_or_else(|| {
            ContractError::InvalidInput("completion subscriptions are unavailable".into())
        })
    }
}
impl CompletionService for ApplicationService {
    fn subscribe_completion<'a>(
        &'a self,
        command: &'a CompletionSubscribeCommand,
    ) -> ContractFuture<'a, CompletionSubscription> {
        Box::pin(async move {
            command.validate()?;
            let reply = self
                .completion_store()?
                .subscribe_completion(command)
                .await?;
            reply.validate().map_err(|_| invalid_reply())?;
            if !reply.matches(command) {
                return Err(invalid_reply());
            }
            Ok(reply)
        })
    }
    fn completion_status<'a>(
        &'a self,
        scope: &'a Scope,
        subscription_id: &'a str,
    ) -> ContractFuture<'a, CompletionSubscription> {
        Box::pin(async move {
            scope.validate()?;
            validate_text(subscription_id, 128)?;
            let reply = self
                .completion_store()?
                .completion_status(scope, subscription_id)
                .await?;
            reply.validate().map_err(|_| invalid_reply())?;
            if reply.command.scope != *scope || reply.subscription_id != subscription_id {
                return Err(invalid_reply());
            }
            Ok(reply)
        })
    }
    fn retry_completion<'a>(
        &'a self,
        command: &'a CompletionRetryCommand,
    ) -> ContractFuture<'a, CompletionSubscription> {
        Box::pin(async move {
            command.validate()?;
            let reply = self.completion_store()?.retry_completion(command).await?;
            reply.validate().map_err(|_| invalid_reply())?;
            if reply.command.scope != command.scope
                || reply.subscription_id != command.subscription_id
                || reply.generation <= command.expected_generation
            {
                return Err(invalid_reply());
            }
            Ok(reply)
        })
    }
}
fn invalid_reply() -> ContractError {
    ContractError::Unavailable("invalid completion store reply".into())
}
