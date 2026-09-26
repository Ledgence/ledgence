//! Observation transport never grants or extends execution authority.
use ledgence_orchestration_api::{console::*, *};
use std::sync::Arc;
#[derive(Clone)]
pub struct WorkerObservationApplicationService {
    scope: Scope,
    store: Arc<dyn WorkerObservationStore>,
}
impl WorkerObservationApplicationService {
    pub fn new(scope: Scope, store: Arc<dyn WorkerObservationStore>) -> Result<Self> {
        scope.validate()?;
        Ok(Self { scope, store })
    }
}
impl WorkerObservationPublisher for WorkerObservationApplicationService {
    fn publish_observation<'a>(
        &'a self,
        command: &'a WorkerObservationCommand,
    ) -> ContractFuture<'a, WorkerObservationReceipt> {
        Box::pin(async move {
            command.validate()?;
            if command.scope != self.scope {
                return Err(ContractError::UnknownSession);
            }
            let reply = self.store.record_observation(command).await?;
            if reply.worker_session_id != command.worker_session_id
                || reply.sequence != command.sequence
                || reply.received_at > CONSOLE_MAX_TIMESTAMP
            {
                return Err(ContractError::Unavailable(
                    "inconsistent worker receipt".into(),
                ));
            }
            Ok(reply)
        })
    }
}
impl WorkerObservationService for WorkerObservationApplicationService {
    fn query_workers<'a>(
        &'a self,
        query: &'a WorkerObservationQuery,
    ) -> ContractFuture<'a, WorkerObservationReply> {
        Box::pin(async move {
            query.validate(&self.scope)?;
            let reply = self.store.query_workers(&self.scope, query).await?;
            reply.validate(&self.scope, query)?;
            Ok(reply)
        })
    }
}
