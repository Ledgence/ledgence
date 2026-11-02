//! Scope-bound, validating console reads, independent of execution commands.
use ledgence_orchestration_api::{ContractFuture, Scope, console::*};
use std::sync::Arc;

#[derive(Clone)]
pub struct ConsoleApplicationService {
    store: Arc<dyn ConsoleQueryStore>,
    scope: Scope,
}
impl ConsoleApplicationService {
    pub fn new(
        store: Arc<dyn ConsoleQueryStore>,
        scope: Scope,
    ) -> ledgence_orchestration_api::Result<Self> {
        scope.validate()?;
        Ok(Self { store, scope })
    }
}
impl ConsoleQueryService for ConsoleApplicationService {
    fn query_console<'a>(
        &'a self,
        query: &'a ConsoleQuery,
    ) -> ContractFuture<'a, ConsoleQueryReply> {
        Box::pin(async move {
            query.validate(&self.scope)?;
            let reply = self.store.query_console(&self.scope, query).await?;
            reply.validate(&self.scope, query)?;
            Ok(reply)
        })
    }

    fn inspect_local_step<'a>(
        &'a self,
        lookup: &'a ConsoleLocalStepLookup,
    ) -> ContractFuture<'a, ConsoleLocalStepDetail> {
        Box::pin(async move {
            lookup.validate()?;
            let detail = self.store.inspect_local_step(&self.scope, lookup).await?;
            lookup.validate_reply(&detail)?;
            Ok(detail)
        })
    }

    fn explore_approval<'a>(
        &'a self,
        focus: &'a ConsoleApprovalFocus,
    ) -> ContractFuture<'a, ConsoleWorkflowExplorer> {
        Box::pin(async move {
            focus.validate(&self.scope)?;
            let reply = self.store.explore_approval(&self.scope, focus).await?;
            focus.validate_reply(&self.scope, &reply)?;
            Ok(reply)
        })
    }
}
