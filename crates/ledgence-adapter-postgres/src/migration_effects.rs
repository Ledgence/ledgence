//! Recover reported outcomes from the pre-confirmation worker during an offline
//! upgrade. The saved record and action use the same strict decoder and binding
//! check as a live confirmation, including JSON number classes.
use crate::{codec, database_error, workflow::approved_effects::confirmed_lookup};
use ledgence_orchestration_api::{
    ApprovalSnapshot, ApprovalStatus, ContractError, LocalStepRecord, Result,
};
use sqlx::{Connection, PgConnection, PgPool, Row, postgres::PgRow};

pub(super) const VERSION: i64 = 20261103000000;

pub(super) async fn verify_backfill(pool: &PgPool) -> Result<()> {
    let pending: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM workflow_approvals WHERE effect_evidence_state='legacy')",
    )
    .fetch_one(pool)
    .await
    .map_err(database_error)?;
    if pending {
        return Err(ContractError::Unavailable(
            "approved effect evidence upgrade is incomplete; run ledgence orchestrator migrate"
                .into(),
        ));
    }
    Ok(())
}

const LEGACY_PAGE: &str = "
    WITH pending AS MATERIALIZED (
        SELECT workflow_id, wait_key, effect_step_key, snapshot_bytes, effect_dispatch_attempt_id
        FROM workflow_approvals WHERE effect_evidence_state='legacy'
        ORDER BY workflow_id, wait_key LIMIT 16 FOR UPDATE
    )
    SELECT p.*, w.tenant_id, w.namespace, w.retiring_at_ms IS NOT NULL AS retiring,
           a.activation_id, r.record_bytes, accepted.attempt_id AS accepting_attempt
    FROM pending p
    JOIN workflow_runs w ON w.workflow_id=p.workflow_id
    LEFT JOIN attempts granted ON granted.attempt_id=p.effect_dispatch_attempt_id
    LEFT JOIN workflow_activations a ON a.task_id=granted.task_id AND a.workflow_id=p.workflow_id
    LEFT JOIN workflow_local_results r ON r.activation_id=a.activation_id AND r.step_key=p.effect_step_key
    LEFT JOIN attempts accepted ON accepted.attempt_id=r.attempt_id AND accepted.task_id=a.task_id";

pub(super) async fn backfill(connection: &mut PgConnection) -> Result<()> {
    loop {
        // The SQL migration commits a fail-closed marker before any Rust-side
        // inspection. A timeout rolls back this page. Re-running migrate resumes
        // with the next marked row; startup and retention reject partial upgrades.
        let mut tx = connection.begin().await.map_err(database_error)?;
        let rows = sqlx::query(LEGACY_PAGE)
            .fetch_all(&mut *tx)
            .await
            .map_err(database_error)?;
        if rows.is_empty() {
            tx.commit().await.map_err(database_error)?;
            return Ok(());
        }
        for row in rows {
            let workflow_id: &str = row.try_get("workflow_id").map_err(database_error)?;
            let key: &str = row.try_get("wait_key").map_err(database_error)?;
            let proof = historical_confirmation(&row, workflow_id, key)?;
            if proof.is_none() && row.try_get::<bool, _>("retiring").map_err(database_error)? {
                // Retirement can already have physically deleted graph/receipt
                // rows. Clearing the retirement marker cannot rebuild them.
                return Err(ContractError::Unavailable(format!(
                    "workflow {workflow_id} was already retiring with an unconfirmed approved dispatch; restore its pre-retirement backup before migrating"
                )));
            }
            let state = if proof.is_some() {
                "legacy_confirmed"
            } else {
                "open"
            };
            let (lookup, attempt) = proof.unzip();
            sqlx::query(
                "UPDATE workflow_approvals SET effect_evidence_state=$3, effect_lookup=$4,
                 effect_confirmed_attempt_id=$5 WHERE workflow_id=$1 AND wait_key=$2",
            )
            .bind(workflow_id)
            .bind(key)
            .bind(state)
            .bind(lookup)
            .bind(attempt)
            .execute(&mut *tx)
            .await
            .map_err(database_error)?;
        }
        tx.commit().await.map_err(database_error)?;
    }
}

// An unrecognized ordinary, malformed or mismatched journal row is never a
// provider receipt. Preserve the obligation for subsequent reconciliation.
fn historical_confirmation(
    row: &PgRow,
    workflow_id: &str,
    key: &str,
) -> Result<Option<(String, String)>> {
    let stored_snapshot: &[u8] = row.try_get("snapshot_bytes").map_err(database_error)?;
    let snapshot: ApprovalSnapshot = codec::decode(stored_snapshot)?;
    let activation: Option<&str> = row.try_get("activation_id").map_err(database_error)?;
    if snapshot.validate().is_err()
        || snapshot.workflow_id != workflow_id
        || snapshot.key != key
        || snapshot.scope.tenant_id
            != row
                .try_get::<&str, _>("tenant_id")
                .map_err(database_error)?
        || snapshot.scope.namespace
            != row
                .try_get::<&str, _>("namespace")
                .map_err(database_error)?
        || snapshot.status != ApprovalStatus::Approved
        || activation.is_none()
        || snapshot.resumed_activation_id.as_deref() != activation
    {
        return Ok(None);
    }
    let Some(attempt) = row
        .try_get::<Option<String>, _>("accepting_attempt")
        .map_err(database_error)?
    else {
        return Ok(None);
    };
    let bytes: &[u8] = row.try_get("record_bytes").map_err(database_error)?;
    let step_key: &str = row.try_get("effect_step_key").map_err(database_error)?;
    let Ok(record) = codec::decode::<LocalStepRecord>(bytes) else {
        return Ok(None);
    };
    Ok(confirmed_lookup(&record, step_key, &snapshot.action)
        .ok()
        .map(|lookup| (lookup.to_owned(), attempt)))
}
