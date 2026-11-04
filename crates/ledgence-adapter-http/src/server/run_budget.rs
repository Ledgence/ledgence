use super::*;
use serde::{Deserialize, Serialize};

fn service(server: &Server) -> Result<&dyn RunBudgetOperations> {
    if access::control(server).is_none() || !audit::enabled(server) {
        return Err(unavailable("run budgets require authenticated operation"));
    }
    let workflows = server
        .workflows
        .as_ref()
        .ok_or_else(|| unavailable("workflow capabilities unavailable"))?;
    workflows.operator_access()?;
    workflows.budgets()
}
pub(super) fn available(server: &Server) -> bool {
    service(server).is_ok()
}

pub(super) fn task_support(server: &Server) -> bool {
    server.service.supports_task_run_budgets() && server.service.task_admissions().is_ok()
}

pub(super) fn require_submission(
    server: &Server,
    command: &SubmitCommand,
    task: bool,
) -> Result<()> {
    if command.input.budget.is_none() {
        return Ok(());
    }
    service(server)?;
    if task {
        if command.input.admission.is_none() {
            return Err(ContractError::InvalidInput(
                "standalone run budget requires task admission".into(),
            ));
        }
        if !task_support(server) {
            return Err(unavailable("task service cannot enforce run budgets"));
        }
    }
    Ok(())
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Subject {
    subject: RunBudgetSubject,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Entries {
    subject: RunBudgetSubject,
    #[serde(deserialize_with = "Option::deserialize")]
    before: Option<ledgence_orchestration_api::console::ConsoleU64>,
    limit: u32,
}
pub(super) async fn post(
    server: &Server,
    operation: &str,
    bytes: Vec<u8>,
    scope: Option<&Scope>,
    context: &audit::RequestContext,
) -> std::result::Result<Vec<u8>, Failure> {
    let service = service(server)?;
    let p = context.principal()?;
    match operation {
        "inspect" => {
            let q = if let Some(scope) = scope {
                let input: Subject = server.decode(bytes, 4096).await?;
                RunBudgetQuery {
                    scope: scope.clone(),
                    subject: input.subject,
                }
            } else {
                server.decode(bytes, 4096).await?
            };
            q.validate()?;
            server.require_scope(&q.scope)?;
            context.recheck(server)?;
            let snapshot = service.run_budget(p, &q).await?;
            snapshot
                .validate_for(&q)
                .map_err(|_| unavailable("invalid run budget reply"))?;
            server
                .blocking(move || encode_bounded(&snapshot, ADMISSION_PAGE_MAX_BYTES))
                .await
        }
        "entries" => {
            let q = if let Some(scope) = scope {
                let input: Entries = server.decode(bytes, 4096).await?;
                RunBudgetEntriesQuery {
                    scope: scope.clone(),
                    subject: input.subject,
                    before: input.before.map(|n| n.0),
                    limit: input.limit,
                }
            } else {
                server.decode(bytes, 4096).await?
            };
            q.validate()?;
            server.require_scope(&q.scope)?;
            context.recheck(server)?;
            let page = service.list_run_budget_entries(p, &q).await?;
            page.validate_for(&q)
                .map_err(|_| unavailable("invalid run budget page"))?;
            server
                .blocking(move || encode_bounded(&page, ADMISSION_PAGE_MAX_BYTES))
                .await
        }
        _ => Err(ContractError::NotFound.into()),
    }
}
