use super::*;

/// Locate lineage without locking; callers then lock the canonical root and
/// recheck the immediate task/workflow. No user-provided root is trusted.
pub(super) async fn canonical_root(
    tx: &mut PgConnection,
    s: &Scope,
    t: &ExecutionTarget,
) -> StoreResult<ExecutionTarget> {
    let workflow = match t {
        ExecutionTarget::Task { id } => {
            let row=sqlx::query("SELECT workflow_id FROM tasks WHERE task_id=$1 AND tenant_id=$2 AND namespace=$3 AND retiring_at_ms IS NULL").bind(id).bind(&s.tenant_id).bind(&s.namespace).fetch_optional(&mut *tx).await?.ok_or(ContractError::NotFound)?;
            row.try_get::<Option<String>, _>("workflow_id")?
        }
        ExecutionTarget::Workflow { id } => Some(id.clone()),
    };
    match workflow {
        Some(id) => {
            let root:String=sqlx::query_scalar("SELECT coalesce(root_workflow_id,workflow_id) FROM workflow_runs WHERE workflow_id=$1 AND tenant_id=$2 AND namespace=$3 AND retiring_at_ms IS NULL").bind(id).bind(&s.tenant_id).bind(&s.namespace).fetch_optional(tx).await?.ok_or(ContractError::NotFound)?;
            Ok(ExecutionTarget::Workflow { id: root })
        }
        None => Ok(t.clone()),
    }
}
pub(super) async fn lock_root(
    tx: &mut PgConnection,
    s: &Scope,
    r: &ExecutionTarget,
) -> StoreResult<()> {
    let sql = match r {
        ExecutionTarget::Task { .. } => {
            "SELECT task_id FROM tasks WHERE task_id=$1 AND tenant_id=$2 AND namespace=$3 AND retiring_at_ms IS NULL FOR NO KEY UPDATE"
        }
        ExecutionTarget::Workflow { .. } => {
            "SELECT workflow_id FROM workflow_runs WHERE workflow_id=$1 AND tenant_id=$2 AND namespace=$3 AND retiring_at_ms IS NULL FOR NO KEY UPDATE"
        }
    };
    let _: String = sqlx::query_scalar(sql)
        .bind(r.id())
        .bind(&s.tenant_id)
        .bind(&s.namespace)
        .fetch_optional(tx)
        .await?
        .ok_or(ContractError::NotFound)?;
    Ok(())
}
pub(super) async fn check_caller(
    store: &PostgresStore,
    tx: &mut PgConnection,
    a: &ArtifactAuthority,
    cap: AccessCapability,
    now: u64,
) -> StoreResult<()> {
    store.require_scope(a.scope())?;
    match a.caller() {
        ArtifactCaller::Verified(p) => {
            if matches!(a, ArtifactAuthority::Worker { .. }) {
                p.require_at(store.audit_audience()?, now)?;
                p.require(AccessCapability::WorkerExecute)?;
                if p.class() != PrincipalClass::Worker {
                    return Err(ContractError::Forbidden.into());
                }
            } else {
                store.require_operator(p, cap, now)?;
            }
        }
        ArtifactCaller::TrustedInstance(instance) => {
            instance.validate()?;
            let trusted:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM self_hosted_instance WHERE singleton AND instance_id=$1 AND tenant_id=$2 AND namespace=$3 AND access_authority_id IS NULL)")
                .bind(&instance.instance_id).bind(&instance.scope.tenant_id).bind(&instance.scope.namespace).fetch_one(tx).await?;
            if !trusted {
                return Err(ContractError::Forbidden.into());
            }
        }
    }
    Ok(())
}
pub(super) async fn authorize(
    store: &PostgresStore,
    tx: &mut PgConnection,
    a: &ArtifactAuthority,
    r: &ExecutionTarget,
    cap: AccessCapability,
) -> StoreResult<Option<ArtifactProducer>> {
    lock_root(tx, a.scope(), r).await?;
    let producer = if let ArtifactAuthority::Worker { lease, .. } = a {
        Some(worker(tx, a, lease, r).await?)
    } else {
        None
    };
    // Deliberately fresh after ALL lock waits, never transaction-start time.
    let now = db::now(tx).await?;
    check_caller(store, tx, a, cap, now).await?;
    Ok(producer)
}
pub(super) fn external_producer(a: &ArtifactAuthority) -> ArtifactProducer {
    match a.caller() {
        ArtifactCaller::Verified(p) => ArtifactProducer::VerifiedActor {
            identity: p.identity().clone(),
        },
        ArtifactCaller::TrustedInstance(i) => ArtifactProducer::TrustedInstance {
            instance_id: i.instance_id.clone(),
        },
    }
}
async fn worker(
    tx: &mut PgConnection,
    a: &ArtifactAuthority,
    o: &LeaseOwner,
    expected: &ExecutionTarget,
) -> StoreResult<ArtifactProducer> {
    o.scope.validate()?;
    if &o.scope != a.scope() {
        return Err(ContractError::Forbidden.into());
    }
    let owner_target = ExecutionTarget::Task {
        id: o.task_id.clone(),
    };
    if canonical_root(tx, &o.scope, &owner_target).await? != *expected {
        return Err(ContractError::Forbidden.into());
    }
    let workflow: Option<String> =
        sqlx::query_scalar("SELECT workflow_id FROM tasks WHERE task_id=$1")
            .bind(&o.task_id)
            .fetch_one(&mut *tx)
            .await?;
    let mut workflow_row = None;
    if let Some(id) = &workflow {
        workflow_row=Some(sqlx::query("SELECT state,current_activation_id,root_workflow_id FROM workflow_runs WHERE workflow_id=$1 AND retiring_at_ms IS NULL FOR NO KEY UPDATE").bind(id).fetch_optional(&mut *tx).await?.ok_or(ContractError::OwnershipLost)?);
    }
    let task=sqlx::query("SELECT state,current_attempt_id,cancel_requested_at_ms,workflow_id,workflow_activation_id FROM tasks WHERE task_id=$1 AND tenant_id=$2 AND namespace=$3 AND retiring_at_ms IS NULL FOR NO KEY UPDATE").bind(&o.task_id).bind(&o.scope.tenant_id).bind(&o.scope.namespace).fetch_optional(&mut *tx).await?.ok_or(ContractError::OwnershipLost)?;
    if task.try_get::<String, _>("state")? != "active"
        || task
            .try_get::<Option<String>, _>("current_attempt_id")?
            .as_deref()
            != Some(&o.attempt_id)
        || task
            .try_get::<Option<i64>, _>("cancel_requested_at_ms")?
            .is_some()
        || task.try_get::<Option<String>, _>("workflow_id")? != workflow
    {
        return Err(ContractError::OwnershipLost.into());
    }
    let attempt=sqlx::query("SELECT a.*,LEAST(a.deadline_ms,h.deadline_ms,h.stop_requested_at_ms,h.released_at_ms) AS effective_deadline,EXISTS(SELECT 1 FROM accepted_settlements s WHERE s.attempt_id=a.attempt_id) AS settled FROM attempts a LEFT JOIN queue_concurrency_holds h ON h.attempt_id=a.attempt_id WHERE a.task_id=$1 AND a.attempt_id=$2").bind(&o.task_id).bind(&o.attempt_id).fetch_optional(&mut *tx).await?.ok_or(ContractError::OwnershipLost)?;
    let now = db::now(tx).await?;
    if attempt.try_get::<String, _>("state")? != "active"
        || attempt.try_get::<String, _>("lease_id")? != o.lease_id
        || attempt.try_get::<i64, _>("generation")? != i64::from(o.generation)
        || attempt.try_get::<String, _>("worker_session_id")? != o.worker_session_id
        || attempt.try_get::<i64, _>("consumer_id")? != i64::from(o.consumer_id)
        || attempt.try_get::<bool, _>("settled")?
        || !attempt.try_get::<bool, _>("execution_may_have_started")?
    {
        return Err(ContractError::OwnershipLost.into());
    }
    for column in [
        "expires_at_ms",
        "authority_deadline_ms",
        "effective_deadline",
    ] {
        if now >= attempt.try_get::<i64, _>(column)? as u64 {
            return Err(ContractError::OwnershipLost.into());
        }
    }
    // A session read does not acquire a session lock after a task lock.
    let session=sqlx::query("SELECT expires_at_ms,admission_worker_bytes FROM worker_sessions WHERE session_id=$1 AND tenant_id=$2 AND namespace=$3").bind(&o.worker_session_id).bind(&o.scope.tenant_id).bind(&o.scope.namespace).fetch_optional(&mut *tx).await?.ok_or(ContractError::OwnershipLost)?;
    if now >= session.try_get::<i64, _>("expires_at_ms")? as u64 {
        return Err(ContractError::OwnershipLost.into());
    }
    if let Some(bytes) = session.try_get::<Option<Vec<u8>>, _>("admission_worker_bytes")? {
        let identity: AdmissionWorkerIdentity = codec::decode(&bytes)?;
        if !matches!(a.caller(),ArtifactCaller::Verified(p) if identity.matches_principal(p)) {
            return Err(ContractError::Forbidden.into());
        }
    }
    let activation = task.try_get::<Option<String>, _>("workflow_activation_id")?;
    if let Some(row) = workflow_row {
        let state: String = row.try_get("state")?;
        if !matches!(state.as_str(), "running" | "waiting") {
            return Err(ContractError::OwnershipLost.into());
        }
        if activation.is_some()
            && (state != "running"
                || row
                    .try_get::<Option<String>, _>("current_activation_id")?
                    .as_deref()
                    != Some(&o.task_id))
        {
            return Err(ContractError::OwnershipLost.into());
        }
    }
    if let Some(activation_id) = activation {
        Ok(ArtifactProducer::WorkflowActivation {
            workflow_id: workflow.ok_or_else(corrupt)?,
            activation_id,
        })
    } else {
        Ok(ArtifactProducer::Task {
            task_id: o.task_id.clone(),
        })
    }
}
pub(super) async fn authorize_upload(
    store: &PostgresStore,
    tx: &mut PgConnection,
    a: &ArtifactAuthority,
    id: &str,
) -> StoreResult<PgRow> {
    validate_text(id, 128)?;
    let initial = upload(tx, id, false).await?;
    if scope(&initial)? != *a.scope() {
        return Err(ContractError::NotFound.into());
    }
    let producer = authorize(
        store,
        tx,
        a,
        &root(&initial)?,
        AccessCapability::ArtifactWrite,
    )
    .await?
    .unwrap_or_else(|| external_producer(a));
    let b = binding(tx, &initial.try_get::<String, _>("artifact_id")?).await?;
    if codec::decode::<ArtifactProducer>(&b.try_get::<Vec<u8>, _>("producer_bytes")?)? != producer {
        return Err(ContractError::Forbidden.into());
    }
    let row = upload(tx, id, true).await?;
    authorize(store, tx, a, &root(&row)?, AccessCapability::ArtifactWrite).await?;
    Ok(row)
}
