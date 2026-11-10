//! Durable artifact catalog. Root -> immediate workflow/task -> artifact is the
//! lock order. Physical storage observations never confer execution authority.
use crate::{persistence as db, *};
use sqlx::{PgConnection, Row, postgres::PgRow};
mod authority;
mod maintenance;
mod mutations;
mod reads;
#[cfg(test)]
mod tests;
use authority::*;
pub(crate) use maintenance::{collect_retired_root, mark_retired_root};

impl ExecutionArtifactCatalog for PostgresStore {
    fn bind_store<'a>(
        &'a self,
        i: &'a ArtifactStoreIdentity,
        d: Instant,
    ) -> ContractFuture<'a, ()> {
        Box::pin(self.run_until(d, move || self.artifact_bind_store(i)))
    }
    fn prepare<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        c: &'a ArtifactPrepareCommand,
        d: Instant,
    ) -> ContractFuture<'a, ArtifactPrepareReply> {
        Box::pin(self.run_until(d, move || self.artifact_prepare(a, c)))
    }
    fn upload_status<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        id: &'a str,
        d: Instant,
    ) -> ContractFuture<'a, ArtifactUploadStatus> {
        Box::pin(self.run_until(d, move || self.artifact_status(a, id)))
    }
    fn replace<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        c: &'a ArtifactReplaceCommand,
        d: Instant,
    ) -> ContractFuture<'a, ArtifactReplacementReceipt> {
        Box::pin(self.run_until(d, move || self.artifact_replace(a, c)))
    }
    fn claim_ingress<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        s: &'a ArtifactSelection,
        d: Instant,
    ) -> ContractFuture<'a, ArtifactIngressClaim> {
        Box::pin(self.run_until(d, move || self.artifact_claim(a, s, d)))
    }
    fn release_ingress<'a>(
        &'a self,
        c: &'a ArtifactIngressClaim,
        d: Instant,
    ) -> ContractFuture<'a, ArtifactUploadStatus> {
        Box::pin(self.run_until(d, move || self.artifact_release(c)))
    }
    fn begin_write<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        c: &'a ArtifactIngressClaim,
        d: Instant,
    ) -> ContractFuture<'a, ArtifactBeginWrite> {
        Box::pin(self.run_until(d, move || self.artifact_begin(a, c, d)))
    }
    fn record_storage_observation<'a>(
        &'a self,
        id: &'a str,
        o: ArtifactStorageObservation,
        d: Instant,
    ) -> ContractFuture<'a, ()> {
        Box::pin(self.run_until(d, move || self.artifact_observe(id, o)))
    }
    fn accept<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        s: &'a ArtifactSelection,
        d: Instant,
    ) -> ContractFuture<'a, ArtifactManifest> {
        Box::pin(self.run_until(d, move || self.artifact_accept(a, s)))
    }
    fn resolve<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        id: &'a str,
        k: ArtifactReadKind,
        d: Instant,
    ) -> ContractFuture<'a, ArtifactRecord> {
        Box::pin(self.run_until(d, move || self.artifact_resolve(a, id, k)))
    }
    fn upload_object<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        s: &'a ArtifactSelection,
        d: Instant,
    ) -> ContractFuture<'a, ArtifactObjectDescriptor> {
        Box::pin(self.run_until(d, move || async {
            let mut conn = self.transaction_connection().await?;
            let mut tx = conn.begin_write().await?;
            let row = authorize_upload(self, &mut tx, a, &s.upload_id).await?;
            if selection(&row)? != *s {
                return Err(ContractError::Conflict.into());
            }
            let value = object(&row)?;
            tx.commit().await?;
            Ok(value)
        }))
    }
    fn list<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        q: &'a ArtifactListQuery,
        d: Instant,
    ) -> ContractFuture<'a, ArtifactPage> {
        Box::pin(self.run_until(d, move || self.artifact_list(a, q)))
    }
    fn claim_maintenance<'a>(
        &'a self,
        i: &'a ArtifactStoreIdentity,
        n: u32,
        d: Instant,
    ) -> ContractFuture<'a, Vec<ArtifactMaintenanceWork>> {
        Box::pin(self.run_until(d, move || self.artifact_maintenance(i, n)))
    }
    fn finish_delete<'a>(
        &'a self,
        l: &'a ArtifactCleanupLease,
        o: ArtifactDeleteObservation,
        d: Instant,
    ) -> ContractFuture<'a, ()> {
        Box::pin(self.run_until(d, move || self.artifact_finish_delete(l, o)))
    }
    fn maintenance_progress(&self, d: Instant) -> ContractFuture<'_, ArtifactMaintenanceProgress> {
        Box::pin(self.run_until(d, move || self.artifact_maintenance_progress()))
    }
}
fn corrupt() -> ContractError {
    ContractError::Unavailable("invalid execution artifact catalog record".into())
}
fn target(kind: String, id: String) -> Result<ExecutionTarget> {
    match kind.as_str() {
        "task" => Ok(ExecutionTarget::Task { id }),
        "workflow" => Ok(ExecutionTarget::Workflow { id }),
        _ => Err(corrupt()),
    }
}
fn root(row: &PgRow) -> StoreResult<ExecutionTarget> {
    Ok(target(row.try_get("root_kind")?, row.try_get("root_id")?)?)
}
fn scope(row: &PgRow) -> StoreResult<Scope> {
    Ok(Scope {
        tenant_id: row.try_get("tenant_id")?,
        namespace: row.try_get("namespace")?,
    })
}
fn selection(row: &PgRow) -> StoreResult<ArtifactSelection> {
    Ok(ArtifactSelection {
        artifact_id: row.try_get("artifact_id")?,
        upload_id: row.try_get("upload_id")?,
        revision: u64::try_from(row.try_get::<i64, _>("revision")?).map_err(|_| corrupt())?,
    })
}
fn object(row: &PgRow) -> StoreResult<ArtifactObjectDescriptor> {
    let value = ArtifactObjectDescriptor {
        store: ArtifactStoreIdentity {
            store_id: row.try_get("store_id")?,
            fingerprint: ledgence_worker_api::Digest(row.try_get("fingerprint")?),
        },
        artifact_id: row.try_get("artifact_id")?,
        upload_id: row.try_get("upload_id")?,
        key: row.try_get("object_key")?,
        metadata: codec::decode(&row.try_get::<Vec<u8>, _>("metadata_bytes")?)?,
    };
    value.validate().map_err(|_| corrupt())?;
    Ok(value)
}
async fn upload(tx: &mut PgConnection, id: &str, locked: bool) -> StoreResult<PgRow> {
    sqlx::query(if locked {
        "SELECT * FROM artifact_uploads WHERE upload_id=$1 FOR UPDATE"
    } else {
        "SELECT * FROM artifact_uploads WHERE upload_id=$1"
    })
    .bind(id)
    .fetch_optional(tx)
    .await?
    .ok_or(ContractError::NotFound.into())
}
async fn binding(tx: &mut PgConnection, id: &str) -> StoreResult<PgRow> {
    sqlx::query("SELECT * FROM execution_artifacts WHERE artifact_id=$1 FOR UPDATE")
        .bind(id)
        .fetch_optional(tx)
        .await?
        .ok_or(ContractError::NotFound.into())
}
async fn status(tx: &mut PgConnection, row: &PgRow) -> StoreResult<ArtifactUploadStatus> {
    let b = sqlx::query(
        "SELECT selected_upload_id,manifest_bytes FROM execution_artifacts WHERE artifact_id=$1",
    )
    .bind(row.try_get::<String, _>("artifact_id")?)
    .fetch_optional(&mut *tx)
    .await?;
    let selected = b.as_ref().is_some_and(|b| {
        b.get::<String, _>("selected_upload_id") == row.get::<String, _>("upload_id")
    });
    let reference = match b.and_then(|b| b.get::<Option<Vec<u8>>, _>("manifest_bytes")) {
        Some(bytes) => Some(codec::decode::<ArtifactManifest>(&bytes)?.reference),
        None => None,
    };
    let successor = match row.try_get::<Option<String>, _>("replaced_by")? {
        Some(id) => Some(selection(&upload(tx, &id, false).await?)?),
        None => None,
    };
    let stage: ArtifactUploadStage = codec::enum_value(row.try_get("stage")?)?;
    let now = db::now(tx).await?;
    let expired_prewrite = match stage {
        ArtifactUploadStage::Prepared => {
            now >= row.try_get::<i64, _>("prepare_expires_at_ms")? as u64
        }
        ArtifactUploadStage::Receiving => row
            .try_get::<Option<i64>, _>("ingress_expires_at_ms")?
            .is_some_and(|at| now >= at as u64),
        _ => false,
    };
    let retry = if selected && expired_prewrite {
        ArtifactRetryGuidance::Replace
    } else if !selected {
        ArtifactRetryGuidance::Unavailable
    } else {
        match stage {
            ArtifactUploadStage::Prepared => ArtifactRetryGuidance::Upload,
            ArtifactUploadStage::Receiving => ArtifactRetryGuidance::Wait,
            ArtifactUploadStage::Writing | ArtifactUploadStage::Uncertain => {
                ArtifactRetryGuidance::Reconcile
            }
            ArtifactUploadStage::Ready => ArtifactRetryGuidance::Complete,
            ArtifactUploadStage::ExpiredNoWrite => ArtifactRetryGuidance::Replace,
            ArtifactUploadStage::Retired => ArtifactRetryGuidance::Unavailable,
        }
    };
    let value = ArtifactUploadStatus {
        selection: selection(row)?,
        selected,
        stage,
        prepare_expires_at: row.try_get::<i64, _>("prepare_expires_at_ms")? as u64,
        ingress_expires_at: row
            .try_get::<Option<i64>, _>("ingress_expires_at_ms")?
            .map(|v| v as u64),
        dispatch_expires_at: row
            .try_get::<Option<i64>, _>("dispatch_expires_at_ms")?
            .map(|v| v as u64),
        successor,
        reference: if selected { reference } else { None },
        retry,
    };
    value.validate().map_err(|_| corrupt())?;
    Ok(value)
}
async fn expire_prewrite(
    tx: &mut PgConnection,
    s: &Scope,
    r: &ExecutionTarget,
    now: u64,
) -> StoreResult<()> {
    sqlx::query("UPDATE artifact_uploads SET stage='expired_no_write',ingress_token=NULL,ingress_expires_at_ms=NULL WHERE upload_id IN (SELECT upload_id FROM artifact_uploads WHERE tenant_id=$1 AND namespace=$2 AND root_kind=$3 AND root_id=$4 AND ((stage='prepared' AND prepare_expires_at_ms<=$5) OR (stage='receiving' AND ingress_expires_at_ms<=$5)) ORDER BY upload_id LIMIT 128 FOR UPDATE)")
        .bind(&s.tenant_id).bind(&s.namespace).bind(r.kind()).bind(r.id()).bind(codec::ms(now)?).execute(tx).await?;
    Ok(())
}
async fn quota(tx: &mut PgConnection, s: &Scope, r: &ExecutionTarget, now: u64) -> StoreResult<()> {
    expire_prewrite(tx, s, r, now).await?;
    let count:i64=sqlx::query_scalar("SELECT count(*) FROM artifact_uploads WHERE tenant_id=$1 AND namespace=$2 AND root_kind=$3 AND root_id=$4 AND NOT deleted AND stage IN ('prepared','receiving','writing','uncertain')")
        .bind(&s.tenant_id).bind(&s.namespace).bind(r.kind()).bind(r.id()).fetch_one(tx).await?;
    if count >= i64::from(ARTIFACT_ROOT_PENDING_LIMIT) {
        return Err(ContractError::Busy.into());
    }
    Ok(())
}
async fn insert_upload(
    tx: &mut PgConnection,
    s: &Scope,
    r: &ExecutionTarget,
    selection: &ArtifactSelection,
    m: &ArtifactMetadata,
    store: &ArtifactStoreIdentity,
    now: u64,
) -> StoreResult<()> {
    sqlx::query("INSERT INTO artifact_uploads(upload_id,artifact_id,tenant_id,namespace,root_kind,root_id,revision,store_id,fingerprint,object_key,metadata_bytes,stage,prepare_expires_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'prepared',$12)")
        .bind(&selection.upload_id).bind(&selection.artifact_id).bind(&s.tenant_id).bind(&s.namespace).bind(r.kind()).bind(r.id()).bind(selection.revision as i64).bind(&store.store_id).bind(&store.fingerprint.0).bind(format!("objects/{}/{}",selection.artifact_id,selection.upload_id)).bind(codec::encode(m)?).bind(codec::ms(now+ARTIFACT_PREPARE_TIMEOUT_MS)?).execute(tx).await?;
    Ok(())
}

impl PostgresStore {
    /// Artifact maintenance is installation-wide and must never accept an
    /// arbitrary scope from a legacy unbound retention invocation.
    pub fn require_artifact_scope(&self, scope: &Scope) -> Result<()> {
        scope.validate()?;
        match self.instance_scope.get() {
            Some(bound) if bound == scope => Ok(()),
            Some(_) => Err(ContractError::NotFound),
            None => Err(ContractError::Unavailable(
                "artifacts require an initialized bound instance".into(),
            )),
        }
    }
}
