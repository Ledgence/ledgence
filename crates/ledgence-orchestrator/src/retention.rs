//! Explicit scoped maintenance, independent of request-serving availability.
use ledgence_adapter_postgres::PostgresStore;
use ledgence_orchestration_api::{RetentionPolicy, RetentionProgress, RetentionStore, Scope};
use std::time::{Duration, Instant};
use tokio::sync::watch;

pub async fn run(
    store: &PostgresStore,
    scope: &Scope,
    policy: &RetentionPolicy,
    batches: u32,
    apply: bool,
    mut stopped: watch::Receiver<bool>,
) -> Result<(), String> {
    store
        .verify_schema()
        .await
        .map_err(|error| error.to_string())?;
    if !apply {
        let preview = store
            .retention_preview(scope, policy, Instant::now() + Duration::from_secs(30))
            .await
            .map_err(|error| error.to_string())?;
        tracing::info!(tenant=%scope.tenant_id,namespace=%scope.namespace,retain_days=policy.retain_for_ms/86_400_000,task_candidates=?preview.task_candidates,workflow_candidates=?preview.workflow_candidates,expired_session_candidates=?preview.expired_session_candidates,retiring_tasks=?preview.retiring_tasks,retiring_workflows=?preview.retiring_workflows,"retention preview only; bounded age candidates may have protective references; no records changed");
        return Ok(());
    }
    let mut total = RetentionProgress::default();
    let mut completed = 0;
    for _ in 0..batches {
        if *stopped.borrow() {
            break;
        }
        let progress = tokio::select! {
            biased;
            _=stopped.changed() => break,
            result=store.retain_batch(scope,policy,Instant::now()+Duration::from_secs(30)) => result.map_err(|error|error.to_string())?,
        };
        total.examined += progress.examined;
        total.retired += progress.retired;
        total.deleted_rows += progress.deleted_rows;
        total.deleted_executions += progress.deleted_executions;
        total.deleted_sessions += progress.deleted_sessions;
        completed += 1;
        // An empty/protected lane is not completion: persisted cursors rotate.
        tokio::task::yield_now().await;
    }
    tracing::info!(tenant=%scope.tenant_id,namespace=%scope.namespace,batches=completed,examined=total.examined,retired=total.retired,deleted_rows=total.deleted_rows,deleted_executions=total.deleted_executions,deleted_sessions=total.deleted_sessions,"retention maintenance completed; rerun to continue bounded collection");
    Ok(())
}
