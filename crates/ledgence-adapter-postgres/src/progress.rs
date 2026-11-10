//! Compact lifecycle observations from one repeatable-read database snapshot.
use crate::*;
use sqlx::{Row, postgres::PgRow};

const TASK_HISTORY_SQL: &str = "SELECT trunc(sequence)::text AS sequence_text,at_ms,reason,attempt_id,NULL::text AS activation_id FROM task_history WHERE task_id=$1 AND sequence>($2::text)::ldg_u64 ORDER BY sequence LIMIT $3";
const WORKFLOW_HISTORY_SQL: &str = "SELECT trunc(sequence)::text AS sequence_text,at_ms,reason,NULL::text AS attempt_id,activation_id FROM workflow_history WHERE workflow_id=$1 AND sequence>($2::text)::ldg_u64 ORDER BY sequence LIMIT $3";

impl ExecutionProgressReader for PostgresStore {
    fn execution_progress<'a>(
        &'a self,
        query: &'a ExecutionProgressQuery,
    ) -> ContractFuture<'a, ExecutionProgressPage> {
        Box::pin(self.run(move || async move {
            self.require_scope(&query.scope)?;
            let after = query.validate()?;
            let mut connection = self.transaction_connection().await?;
            let mut tx = connection.begin_read().await?;
            let page = read_page(&mut tx, query, after).await?;
            tx.commit().await?;
            Ok(page)
        }))
    }
}

async fn read_page(
    connection: &mut sqlx::PgConnection,
    query: &ExecutionProgressQuery,
    after: u64,
) -> StoreResult<ExecutionProgressPage> {
    let (owner_sql, history_sql) = match &query.target {
        ExecutionProgressTarget::Task { .. } => (
            "SELECT state,trunc(history_sequence)::text AS sequence_text FROM tasks WHERE task_id=$1 AND tenant_id=$2 AND namespace=$3 AND retiring_at_ms IS NULL",
            TASK_HISTORY_SQL,
        ),
        ExecutionProgressTarget::Workflow { .. } => (
            "SELECT state,trunc(history_sequence)::text AS sequence_text FROM workflow_runs WHERE workflow_id=$1 AND tenant_id=$2 AND namespace=$3 AND retiring_at_ms IS NULL",
            WORKFLOW_HISTORY_SQL,
        ),
    };
    // Only owner metadata is loaded. Inputs, outputs, descriptors and
    // settlement payloads are neither selected nor decoded.
    let owner = sqlx::query(owner_sql)
        .bind(query.target.id())
        .bind(&query.scope.tenant_id)
        .bind(&query.scope.namespace)
        .fetch_optional(&mut *connection)
        .await?
        .ok_or(ContractError::NotFound)?;
    let high_watermark = sequence(&owner)?;
    if after > high_watermark {
        return Err(ContractError::InvalidInput(
            "progress cursor is ahead of execution history".into(),
        )
        .into());
    }
    let state: String = owner.try_get("state")?;
    let terminal = match &query.target {
        ExecutionProgressTarget::Task { .. } => {
            let state: TaskState = codec::enum_value(state.clone())?;
            state.is_terminal()
        }
        ExecutionProgressTarget::Workflow { .. } => {
            let state: WorkflowState = codec::enum_value(state.clone())?;
            state.is_terminal()
        }
    };
    let rows = sqlx::query(history_sql)
        .bind(query.target.id())
        .bind(after.to_string())
        .bind(i64::from(query.limit))
        .fetch_all(&mut *connection)
        .await?;
    let events = rows.iter().map(event).collect::<StoreResult<Vec<_>>>()?;
    let consumed = events.last().map_or(after, |event| event.sequence.0);
    let page = ExecutionProgressPage {
        scope: query.scope.clone(),
        target: query.target.clone(),
        state,
        terminal,
        high_watermark: ProgressSequence(high_watermark),
        events,
        next_cursor: query.cursor_for(consumed)?,
        has_more: consumed < high_watermark,
    };
    page.validate_for(query)
        .map_err(|_| ContractError::Unavailable("inconsistent execution progress".into()))?;
    Ok(page)
}

fn sequence(row: &PgRow) -> StoreResult<u64> {
    let value: String = row.try_get("sequence_text")?;
    value
        .parse()
        .map_err(|_| ContractError::Unavailable("invalid progress sequence".into()).into())
}

fn event(row: &PgRow) -> StoreResult<ExecutionProgressEvent> {
    let at: i64 = row.try_get("at_ms")?;
    Ok(ExecutionProgressEvent {
        sequence: ProgressSequence(sequence(row)?),
        at: codec::timestamp(at)?,
        reason: row.try_get("reason")?,
        activation_id: row.try_get("activation_id")?,
        attempt_id: row.try_get("attempt_id")?,
    })
}

#[cfg(test)]
mod tests;
