use super::*;
impl ResponseValue for RunBudgetSnapshot {
    const MAX_BYTES: usize = ADMISSION_PAGE_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl ResponseValue for RunBudgetPage {
    const MAX_BYTES: usize = ADMISSION_PAGE_MAX_BYTES;
    fn validate_values(&self) -> Result<()> {
        self.validate()
    }
}
impl RunBudgetClient for HttpTaskService {
    fn run_budget<'a>(
        &'a self,
        query: &'a RunBudgetQuery,
    ) -> ContractFuture<'a, RunBudgetSnapshot> {
        Box::pin(async move {
            query.validate()?;
            let expected = query.clone();
            self.post_validated(
                "v1/budgets/inspect",
                query,
                4096,
                move |v: &RunBudgetSnapshot| v.validate_for(&expected),
            )
            .await
        })
    }
    fn list_run_budget_entries<'a>(
        &'a self,
        query: &'a RunBudgetEntriesQuery,
    ) -> ContractFuture<'a, RunBudgetPage> {
        Box::pin(async move {
            query.validate()?;
            let expected = query.clone();
            self.post_validated(
                "v1/budgets/entries",
                query,
                4096,
                move |v: &RunBudgetPage| v.validate_for(&expected),
            )
            .await
        })
    }
}
