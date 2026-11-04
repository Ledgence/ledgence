use super::*;
use crate::admission::operator;

impl RunBudgetOperations for ApplicationService {
    fn run_budget<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        query: &'a RunBudgetQuery,
    ) -> ContractFuture<'a, RunBudgetSnapshot> {
        Box::pin(async move {
            query.validate()?;
            operator(principal, &query.scope, AccessCapability::MetadataRead)?;
            let store = self.workflows()?;
            store.operator_access()?;
            let snapshot = store.budgets()?.run_budget(principal, query).await?;
            snapshot
                .validate_for(query)
                .map_err(|_| ContractError::Unavailable("invalid run budget response".into()))?;
            Ok(snapshot)
        })
    }
    fn list_run_budget_entries<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        query: &'a RunBudgetEntriesQuery,
    ) -> ContractFuture<'a, RunBudgetPage> {
        Box::pin(async move {
            query.validate()?;
            operator(principal, &query.scope, AccessCapability::MetadataRead)?;
            let store = self.workflows()?;
            store.operator_access()?;
            let page = store
                .budgets()?
                .list_run_budget_entries(principal, query)
                .await?;
            page.validate_for(query)
                .map_err(|_| ContractError::Unavailable("invalid run budget page".into()))?;
            Ok(page)
        })
    }
}
