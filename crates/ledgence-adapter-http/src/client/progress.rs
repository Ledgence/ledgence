use super::*;
impl ResponseValue for ExecutionProgressPage {
    const MAX_BYTES: usize = EXECUTION_PROGRESS_PAGE_MAX_BYTES;
}
impl ExecutionProgressReader for HttpTaskService {
    fn execution_progress<'a>(
        &'a self,
        query: &'a ExecutionProgressQuery,
    ) -> ContractFuture<'a, ExecutionProgressPage> {
        Box::pin(async move {
            query.validate()?;
            let expected = query.clone();
            self.post_validated(
                "v1/executions/progress",
                query,
                EXECUTION_PROGRESS_QUERY_MAX_BYTES,
                move |page: &ExecutionProgressPage| {
                    page.validate_for(&expected)
                        .map_err(|_| unavailable("invalid execution progress response"))
                },
            )
            .await
        })
    }
}
