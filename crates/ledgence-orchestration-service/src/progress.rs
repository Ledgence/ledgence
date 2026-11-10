use super::*;

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
            let page = self
                .store
                .progress_reader()?
                .execution_progress(query)
                .await?;
            page.validate_for(query).map_err(|_| {
                ContractError::Unavailable("invalid execution progress response".into())
            })?;
            Ok(page)
        })
    }
}
