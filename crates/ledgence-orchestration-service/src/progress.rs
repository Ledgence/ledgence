//! Request-owned progress waits. Durable storage remains authoritative and is
//! never held idle. Waiting observers of the same page share bounded probes.
mod coordinator;
use super::*;
pub(crate) use coordinator::Coordinator;
pub use coordinator::ProgressStatistics;

impl ExecutionProgressReader for ApplicationService {
    fn execution_progress<'a>(
        &'a self,
        query: &'a ExecutionProgressQuery,
    ) -> ContractFuture<'a, ExecutionProgressPage> {
        Box::pin(async move {
            query.validate()?;
            if self
                .catalog
                .as_ref()
                .is_some_and(|(scope, _)| scope != &query.scope)
            {
                return Err(ContractError::NotFound);
            }
            let reader = self.store.progress_reader()?;
            if query.wait_ms > 0 {
                return self.progress.read(reader, query).await;
            }
            let page = reader.execution_progress(query).await?;
            validate_page(&page, query)?;
            Ok(page)
        })
    }
}
fn validate_page(page: &ExecutionProgressPage, query: &ExecutionProgressQuery) -> Result<()> {
    page.validate_for(query)
        .map_err(|_| ContractError::Unavailable("invalid execution progress response".into()))
}
