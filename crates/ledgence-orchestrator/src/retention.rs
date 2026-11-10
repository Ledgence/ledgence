//! Explicit scoped maintenance, independent of request-serving availability.
use ledgence_adapter_postgres::PostgresStore;
use ledgence_orchestration_api::{
    ExecutionArtifactCatalog, RetentionPolicy, RetentionProgress, RetentionStore, Scope,
};
use std::time::{Duration, Instant};
use tokio::sync::watch;

pub(crate) struct RetainRequest {
    pub scope: Scope,
    pub policy: RetentionPolicy,
    pub batches: u32,
    pub apply: bool,
    pub artifact_config: Option<std::path::PathBuf>,
}
pub async fn run(
    store: &PostgresStore,
    request: RetainRequest,
    mut stopped: watch::Receiver<bool>,
) -> Result<(), String> {
    let RetainRequest {
        scope,
        policy,
        batches,
        apply,
        artifact_config,
    } = request;
    let (scope, policy) = (&scope, &policy);
    store
        .verify_schema()
        .await
        .map_err(|error| error.to_string())?;
    store
        .load_instance_binding()
        .await
        .map_err(|error| error.to_string())?;
    if artifact_config.is_some() {
        store
            .require_artifact_scope(scope)
            .map_err(|error| error.to_string())?;
    }
    let obligations = store
        .maintenance_progress(Instant::now() + Duration::from_secs(30))
        .await
        .map_err(|error| error.to_string())?;
    tracing::info!(
        pending = obligations.pending,
        uncertain = obligations.uncertain,
        blob_cleanup_configured = artifact_config.is_some(),
        "artifact maintenance obligations; SQL retention does not imply blob deletion"
    );
    if !apply {
        let preview = store
            .retention_preview(scope, policy, Instant::now() + Duration::from_secs(30))
            .await
            .map_err(|error| error.to_string())?;
        tracing::info!(tenant=%scope.tenant_id,namespace=%scope.namespace,retain_days=policy.retain_for_ms/86_400_000,task_candidates=?preview.task_candidates,workflow_candidates=?preview.workflow_candidates,expired_session_candidates=?preview.expired_session_candidates,retiring_tasks=?preview.retiring_tasks,retiring_workflows=?preview.retiring_workflows,"retention preview only; bounded age candidates may have protective references; no records changed");
        return Ok(());
    }
    let artifacts =
        super::artifacts::load(artifact_config, std::sync::Arc::new(store.clone()), true).await?;
    let result = async {
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
        if let Some(artifacts) = &artifacts {
            let progress = artifacts.maintain(policy.batch_size.min(100), Instant::now() + Duration::from_secs(120)).await.map_err(|error| error.to_string())?;
            tracing::info!(inspected=progress.inspected,deleted=progress.deleted,pending=progress.pending,uncertain=progress.uncertain,"artifact maintenance batch");
        }
        completed += 1;
        // An empty/protected lane is not completion: persisted cursors rotate.
        tokio::task::yield_now().await;
    }
    tracing::info!(tenant=%scope.tenant_id,namespace=%scope.namespace,batches=completed,examined=total.examined,retired=total.retired,deleted_rows=total.deleted_rows,deleted_executions=total.deleted_executions,deleted_sessions=total.deleted_sessions,"retention maintenance completed; rerun to continue bounded collection");
    Ok(())
    }.await;
    if let Some(artifacts) = artifacts {
        super::artifacts::drain(artifacts.as_ref()).await;
    }
    result
}
