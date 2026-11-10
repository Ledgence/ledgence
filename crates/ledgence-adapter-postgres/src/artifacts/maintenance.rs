use super::*;
impl PostgresStore {
    pub(super) async fn artifact_maintenance(
        &self,
        i: &ArtifactStoreIdentity,
        limit: u32,
    ) -> StoreResult<Vec<ArtifactMaintenanceWork>> {
        i.validate()?;
        if !(1..=100).contains(&limit) {
            return Err(ContractError::InvalidInput(
                "artifact maintenance limit must be between 1 and 100".into(),
            )
            .into());
        }
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_write().await?;
        let store=sqlx::query("SELECT store_id,fingerprint,maintenance_cursor_kind,maintenance_cursor_id,maintenance_upload_cursor,maintenance_lane FROM artifact_stores WHERE singleton FOR UPDATE").fetch_optional(&mut *tx).await?.ok_or(ContractError::NotFound)?;
        if store.try_get::<String, _>("store_id")? != i.store_id
            || store.try_get::<String, _>("fingerprint")? != i.fingerprint.0
        {
            return Err(ContractError::Conflict.into());
        }
        let now = db::now(&mut tx).await?;
        // Alternate two indexed lanes, so an endless replaced-object stream
        // cannot starve retired roots. The root cursor bounds roots examined,
        // even when unknown writes are absent and remain unresolved for years.
        let root_lane = store.try_get::<bool, _>("maintenance_lane")?;
        sqlx::query(
            "UPDATE artifact_stores SET maintenance_lane=NOT maintenance_lane WHERE singleton",
        )
        .execute(&mut *tx)
        .await?;
        let mut rows = Vec::new();
        if root_lane {
            let root_cursor_kind: String = store.try_get("maintenance_cursor_kind")?;
            let root_cursor_id: String = store.try_get("maintenance_cursor_id")?;
            let mut roots =
                retired_roots(&mut tx, &root_cursor_kind, &root_cursor_id, limit).await?;
            if roots.is_empty() && !root_cursor_kind.is_empty() {
                // One bounded wrap avoids wasting an entire scheduled run at
                // the end of the index. Never scan repeatedly until work exists.
                roots = retired_roots(&mut tx, "", "", limit).await?;
            }
            if roots.is_empty() {
                sqlx::query("UPDATE artifact_stores SET maintenance_cursor_kind='',maintenance_cursor_id='' WHERE singleton").execute(&mut *tx).await?;
            }
            for (kind, id, upload_cursor) in roots {
                let remaining = limit - rows.len() as u32;
                let mut batch =
                    retired_uploads(&mut tx, i, &kind, &id, &upload_cursor, now, remaining).await?;
                if batch.is_empty() && !upload_cursor.is_empty() {
                    batch = retired_uploads(&mut tx, i, &kind, &id, "", now, remaining).await?;
                }
                // Advance within the root even if an absent unresolved write
                // survives many spaced-out retention runs. Lease expiry must
                // not repeatedly put the same low-ID object at the front.
                let next = batch
                    .last()
                    .map(|row| row.try_get::<String, _>("upload_id"))
                    .transpose()?
                    .unwrap_or_default();
                sqlx::query("UPDATE artifact_roots SET maintenance_upload_cursor=$3 WHERE root_kind=$1 AND root_id=$2")
                    .bind(&kind).bind(&id).bind(next).execute(&mut *tx).await?;
                rows.extend(batch);
                // Only bounded roots already visited by the durable cursor are
                // considered for tombstone collection. Never rescan live roots.
                sqlx::query("DELETE FROM artifact_uploads WHERE upload_id IN (SELECT u.upload_id FROM artifact_uploads u WHERE u.root_kind=$1 AND u.root_id=$2 AND u.deleted AND NOT EXISTS(SELECT 1 FROM execution_artifacts b WHERE b.artifact_id=u.artifact_id) ORDER BY u.upload_id LIMIT $3 FOR UPDATE SKIP LOCKED)")
                    .bind(&kind).bind(&id).bind(i64::from(limit)).execute(&mut *tx).await?;
                sqlx::query("DELETE FROM artifact_roots r WHERE root_kind=$1 AND root_id=$2 AND retired_at_ms IS NOT NULL AND NOT EXISTS(SELECT 1 FROM artifact_uploads u WHERE u.root_kind=r.root_kind AND u.root_id=r.root_id) AND NOT EXISTS(SELECT 1 FROM execution_artifacts b WHERE b.root_kind=r.root_kind AND b.root_id=r.root_id)")
                    .bind(&kind).bind(&id).execute(&mut *tx).await?;
                sqlx::query("UPDATE artifact_stores SET maintenance_cursor_kind=$1,maintenance_cursor_id=$2 WHERE singleton").bind(&kind).bind(&id).execute(&mut *tx).await?;
                if rows.len() >= limit as usize {
                    break;
                }
            }
        } else {
            let cursor: String = store.try_get("maintenance_upload_cursor")?;
            rows = replaced_uploads(&mut tx, i, &cursor, now, limit).await?;
            if rows.is_empty() && !cursor.is_empty() {
                rows = replaced_uploads(&mut tx, i, "", now, limit).await?;
            }
            let next = rows
                .last()
                .map(|row| row.try_get::<String, _>("upload_id"))
                .transpose()?
                .unwrap_or_default();
            sqlx::query("UPDATE artifact_stores SET maintenance_upload_cursor=$1 WHERE singleton")
                .bind(next)
                .execute(&mut *tx)
                .await?;
        }
        let mut work = Vec::with_capacity(rows.len());
        for row in rows {
            let token = db::id(&mut tx, "cln").await?;
            let object = object(&row)?;
            let stage: String = row.try_get("stage")?;
            let quiescent = row.try_get::<bool, _>("quiescent")?;
            sqlx::query("UPDATE artifact_uploads SET cleanup_token=$2,cleanup_expires_at_ms=$3,stage=CASE WHEN stage IN ('prepared','receiving','expired_no_write') THEN 'retired' ELSE stage END,ingress_token=NULL,ingress_expires_at_ms=NULL WHERE upload_id=$1")
                .bind(&object.upload_id).bind(&token).bind(codec::ms(now+ARTIFACT_TRANSFER_TIMEOUT_MS)?).execute(&mut *tx).await?;
            let lease = ArtifactCleanupLease {
                upload_id: object.upload_id.clone(),
                token,
                object,
            };
            work.push(
                if quiescent
                    || matches!(
                        stage.as_str(),
                        "prepared" | "receiving" | "expired_no_write"
                    )
                {
                    ArtifactMaintenanceWork::Delete(lease)
                } else {
                    ArtifactMaintenanceWork::Inspect(lease)
                },
            );
        }
        tx.commit().await?;
        Ok(work)
    }
    pub(super) async fn artifact_finish_delete(
        &self,
        l: &ArtifactCleanupLease,
        o: ArtifactDeleteObservation,
    ) -> StoreResult<()> {
        let mut conn = self.transaction_connection().await?;
        let mut tx = conn.begin_write().await?;
        let row = match upload(&mut tx, &l.upload_id, true).await {
            Ok(row) => row,
            Err(StoreError::Contract(ContractError::NotFound)) => {
                tx.commit().await?;
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        if row.try_get::<bool, _>("deleted")? {
            tx.commit().await?;
            return Ok(());
        }
        if row
            .try_get::<Option<String>, _>("cleanup_token")?
            .as_deref()
            != Some(&l.token)
            || !row.try_get::<bool, _>("quiescent")?
            || object(&row)? != l.object
        {
            return Err(ContractError::Conflict.into());
        }
        sqlx::query("UPDATE artifact_uploads SET deleted=$2,stage=CASE WHEN $2 THEN 'retired' ELSE stage END,cleanup_token=NULL,cleanup_expires_at_ms=NULL WHERE upload_id=$1")
            .bind(&l.upload_id).bind(o==ArtifactDeleteObservation::Deleted).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    pub(super) async fn artifact_maintenance_progress(
        &self,
    ) -> StoreResult<ArtifactMaintenanceProgress> {
        let (pending,uncertain):(i64,i64)=sqlx::query_as("SELECT count(*)::bigint,count(*) FILTER(WHERE NOT quiescent)::bigint FROM (SELECT u.upload_id,u.quiescent FROM artifact_uploads u WHERE u.cleanup_eligible AND NOT u.deleted UNION SELECT u.upload_id,u.quiescent FROM artifact_roots r JOIN artifact_uploads u ON u.root_kind=r.root_kind AND u.root_id=r.root_id WHERE r.retired_at_ms IS NOT NULL AND NOT u.deleted) pending").fetch_one(&self.pool).await?;
        Ok(ArtifactMaintenanceProgress {
            pending: pending as u64,
            uncertain: uncertain as u64,
            ..Default::default()
        })
    }
}
pub(crate) async fn mark_retired_root(
    tx: &mut PgConnection,
    kind: &str,
    id: &str,
    now: u64,
) -> StoreResult<()> {
    sqlx::query("UPDATE artifact_roots SET retired_at_ms=$3 WHERE root_kind=$1 AND root_id=$2 AND retired_at_ms IS NULL").bind(kind).bind(id).bind(codec::ms(now)?).execute(tx).await?;
    Ok(())
}
/// Upload cleanup obligations have no cascading FK to metadata or executions.
pub(crate) async fn collect_retired_root(
    tx: &mut PgConnection,
    kind: &str,
    id: &str,
    remaining: u32,
) -> StoreResult<u32> {
    if remaining == 0 {
        return Ok(0);
    }
    Ok(sqlx::query("DELETE FROM execution_artifacts WHERE artifact_id IN (SELECT artifact_id FROM execution_artifacts WHERE root_kind=$1 AND root_id=$2 ORDER BY artifact_id LIMIT $3 FOR UPDATE SKIP LOCKED)").bind(kind).bind(id).bind(i64::from(remaining)).execute(tx).await?.rows_affected() as u32)
}

// Each cursor level uses at most one seek and one wrap. These indexed ranges
// return at most `limit` rows and never revisit healthy (non-retired) roots.
async fn retired_roots(
    tx: &mut PgConnection,
    cursor_kind: &str,
    cursor_id: &str,
    limit: u32,
) -> StoreResult<Vec<(String, String, String)>> {
    Ok(sqlx::query_as("SELECT root_kind,root_id,maintenance_upload_cursor FROM artifact_roots WHERE retired_at_ms IS NOT NULL AND (root_kind,root_id)>($1,$2) ORDER BY root_kind,root_id LIMIT $3")
        .bind(cursor_kind).bind(cursor_id).bind(i64::from(limit)).fetch_all(tx).await?)
}
async fn retired_uploads(
    tx: &mut PgConnection,
    identity: &ArtifactStoreIdentity,
    kind: &str,
    id: &str,
    cursor: &str,
    now: u64,
    limit: u32,
) -> StoreResult<Vec<PgRow>> {
    Ok(sqlx::query("SELECT * FROM artifact_uploads WHERE root_kind=$1 AND root_id=$2 AND store_id=$3 AND fingerprint=$4 AND NOT deleted AND (cleanup_expires_at_ms IS NULL OR cleanup_expires_at_ms<=$5) AND upload_id>$6 ORDER BY upload_id LIMIT $7 FOR UPDATE SKIP LOCKED")
        .bind(kind).bind(id).bind(&identity.store_id).bind(&identity.fingerprint.0).bind(codec::ms(now)?).bind(cursor).bind(i64::from(limit)).fetch_all(tx).await?)
}
async fn replaced_uploads(
    tx: &mut PgConnection,
    identity: &ArtifactStoreIdentity,
    cursor: &str,
    now: u64,
    limit: u32,
) -> StoreResult<Vec<PgRow>> {
    Ok(sqlx::query("SELECT * FROM artifact_uploads WHERE store_id=$1 AND fingerprint=$2 AND cleanup_eligible AND NOT deleted AND (cleanup_expires_at_ms IS NULL OR cleanup_expires_at_ms<=$3) AND upload_id>$4 ORDER BY upload_id LIMIT $5 FOR UPDATE SKIP LOCKED")
        .bind(&identity.store_id).bind(&identity.fingerprint.0).bind(codec::ms(now)?).bind(cursor).bind(i64::from(limit)).fetch_all(tx).await?)
}
