//! Offline, resumable sixteen-occurrence projection. No fresh historical authority.
use crate::*;
use sqlx::{Connection, PgConnection, PgPool, Row};
pub(super) const VERSION: i64 = 20261103030000;
pub(super) async fn verify_backfill(pool: &PgPool) -> Result<()> {
    let pending: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM workflow_approvals WHERE effect_retained_pending)",
    )
    .fetch_one(pool)
    .await
    .map_err(database_error)?;
    if pending {
        return Err(ContractError::Unavailable(
            "retained effect evidence upgrade is incomplete; run ledgence orchestrator migrate"
                .into(),
        ));
    }
    Ok(())
}
pub(super) async fn backfill(connection: &mut PgConnection) -> Result<()> {
    loop {
        let mut tx = connection.begin().await.map_err(database_error)?;
        let rows=sqlx::query("SELECT workflow_id,wait_key FROM workflow_approvals WHERE effect_retained_pending ORDER BY workflow_id,wait_key LIMIT 16 FOR UPDATE").fetch_all(&mut *tx).await.map_err(database_error)?;
        if rows.is_empty() {
            tx.commit().await.map_err(database_error)?;
            return Ok(());
        }
        let now = persistence::now(&mut tx)
            .await
            .map_err(StoreError::into_contract)?;
        for row in rows {
            let workflow: String = row.try_get("workflow_id").map_err(database_error)?;
            let key: String = row.try_get("wait_key").map_err(database_error)?;
            let mut r = workflow::effect_retained::ensure(&mut tx, &workflow, &key, now, None)
                .await
                .map_err(StoreError::into_contract)?;
            workflow::effect_retained::legacy_confirmation(&mut tx, &mut r, now)
                .await
                .map_err(StoreError::into_contract)?;
            let valid:Option<bool>=sqlx::query_scalar("UPDATE workflow_approvals SET effect_retained_pending=false WHERE workflow_id=$1 AND wait_key=$2 AND effect_retained_pending RETURNING workflow_id=$1 AND wait_key=$2 AND NOT effect_retained_pending")
                .bind(&workflow).bind(&key).fetch_optional(&mut *tx).await.map_err(database_error)?;
            if valid != Some(true) {
                return Err(ContractError::Unavailable(
                    "retained effect backfill marker was not committed".into(),
                ));
            }
        }
        tx.commit().await.map_err(database_error)?;
    }
}
