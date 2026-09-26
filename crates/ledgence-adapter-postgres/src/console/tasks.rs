use super::*;
use sqlx::{Postgres, QueryBuilder};
use std::collections::HashMap;

pub(super) async fn list(
    connection: &mut PgConnection,
    scope: &Scope,
    query: &ConsoleQuery,
    filters: &TaskFilters,
    request: &ConsolePagination,
    position: Option<&ConsolePosition>,
    observed_at: u64,
) -> StoreResult<ConsolePage<ConsoleTaskSummary>> {
    let position = position
        .map(|p| {
            Ok::<_, ContractError>(TaskPosition {
                submitted_at: position_time(p, 0)? as u64,
                task_id: position_text(p, 1)?,
            })
        })
        .transpose()?;
    let legacy = TaskListQuery {
        filters: filters.clone(),
        limit: request.limit,
        cursor: None,
    };
    let rows = crate::discovery::query(scope, &legacy, position.as_ref())
        .build()
        .fetch_all(&mut *connection)
        .await?;
    // Resolve only the already bounded shortlist. Never select task input/event
    // or report payloads to populate table metadata.
    let ids = rows
        .iter()
        .map(|row| row.try_get::<String, _>("task_id"))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let descriptors=sqlx::query("SELECT task_id,CASE WHEN octet_length(descriptor_bytes)<=4096 THEN descriptor_bytes ELSE NULL END AS descriptor FROM tasks WHERE task_id=ANY($1) AND tenant_id=$2 AND namespace=$3 AND retiring_at_ms IS NULL")
        .bind(&ids).bind(&scope.tenant_id).bind(&scope.namespace).fetch_all(connection).await?;
    let mut by_id = descriptors
        .iter()
        .map(|r| {
            Ok::<_, StoreError>((
                r.try_get::<String, _>("task_id")?,
                descriptor(r, "descriptor")?,
            ))
        })
        .collect::<StoreResult<HashMap<_, _>>>()?;
    let items = rows
        .iter()
        .map(|row| {
            let task = codec::status(row)?;
            let descriptor = by_id
                .remove(&task.task_id)
                .ok_or_else(|| corrupt("missing shortlisted descriptor"))?;
            Ok(ConsoleTaskSummary {
                task: task.into(),
                descriptor,
            })
        })
        .collect::<StoreResult<Vec<_>>>()?;
    page(items, scope, query, request, observed_at)
}
const ATTEMPT_COLUMNS: &str = "a.task_id,a.attempt_id,a.generation,a.worker_session_id,a.consumer_id,a.state,a.execution_may_have_started,a.quiescence,a.finished_at_ms";
const TIMES: &str = " LEFT JOIN LATERAL (SELECT min(at_ms) FILTER(WHERE reason='claimed') AS claimed_at,min(at_ms) FILTER(WHERE reason='dispatch_authorized') AS dispatch_authorized_at FROM task_history h WHERE h.task_id=a.task_id AND h.attempt_id=a.attempt_id AND h.reason IN ('claimed','dispatch_authorized')) h ON true";
pub(super) async fn attempts(
    connection: &mut PgConnection,
    scope: &Scope,
    query: &ConsoleQuery,
    id: &str,
    request: &ConsolePagination,
    position: Option<&ConsolePosition>,
    observed_at: u64,
) -> StoreResult<ConsolePage<ConsoleAttemptSummary>> {
    require_task(connection, scope, id).await?;
    let mut sql: QueryBuilder<Postgres> =
        QueryBuilder::new("SELECT a.*,h.claimed_at,h.dispatch_authorized_at FROM (SELECT ");
    sql.push(ATTEMPT_COLUMNS)
        .push(" FROM attempts a WHERE a.task_id=")
        .push_bind(id.to_owned());
    if let Some(p) = position {
        sql.push(" AND (a.generation,a.attempt_id)<(")
            .push_bind(i64::try_from(position_number(p, 0)?).map_err(|_| {
                ContractError::InvalidInput("invalid attempt generation cursor".into())
            })?)
            .push(",")
            .push_bind(position_text(p, 1)?)
            .push(")");
    }
    sql.push(" ORDER BY a.generation DESC,a.attempt_id DESC LIMIT ")
        .push_bind(i64::from(request.limit) + 1)
        .push(") a")
        .push(TIMES)
        .push(" ORDER BY a.generation DESC,a.attempt_id DESC");
    let rows = sql.build().fetch_all(connection).await?;
    page(
        rows.iter().map(summary).collect::<StoreResult<Vec<_>>>()?,
        scope,
        query,
        request,
        observed_at,
    )
}
fn summary(row: &PgRow) -> StoreResult<ConsoleAttemptSummary> {
    Ok(ConsoleAttemptSummary {
        task_id: row.try_get("task_id")?,
        attempt_id: row.try_get("attempt_id")?,
        generation: u32::try_from(number(row, "generation")?).map_err(|_| corrupt("generation"))?,
        worker_session_id: row.try_get("worker_session_id")?,
        consumer_id: u32::try_from(number(row, "consumer_id")?).map_err(|_| corrupt("consumer"))?,
        state: enumeration(row, "state")?,
        execution_may_have_started: row.try_get("execution_may_have_started")?,
        quiescence: enumeration(row, "quiescence")?,
        claimed_at: optional_number(row, "claimed_at")?,
        dispatch_authorized_at: optional_number(row, "dispatch_authorized_at")?,
        finished_at: optional_number(row, "finished_at_ms")?,
    })
}
pub(super) async fn attempt(
    connection: &mut PgConnection,
    scope: &Scope,
    id: &str,
    observed_at: u64,
) -> StoreResult<ConsoleAttemptDetail> {
    let mut sql: QueryBuilder<Postgres> = QueryBuilder::new("SELECT ");
    sql.push(ATTEMPT_COLUMNS).push(",h.claimed_at,h.dispatch_authorized_at,CASE WHEN octet_length(t.descriptor_bytes)<=4096 THEN t.descriptor_bytes ELSE NULL END AS descriptor,s.accepted_command FROM attempts a JOIN tasks t ON t.task_id=a.task_id LEFT JOIN accepted_settlements s ON s.attempt_id=a.attempt_id").push(TIMES)
        .push(" WHERE a.attempt_id=").push_bind(id.to_owned()).push(" AND t.tenant_id=").push_bind(scope.tenant_id.clone()).push(" AND t.namespace=").push_bind(scope.namespace.clone()).push(" AND t.retiring_at_ms IS NULL");
    let row = sql
        .build()
        .fetch_optional(connection)
        .await?
        .ok_or(ContractError::NotFound)?;
    let mut detail = ConsoleAttemptDetail {
        attempt: summary(&row)?,
        descriptor: descriptor(&row, "descriptor")?,
        phase: None,
        error: None,
        application_error: None,
        cleanup_error: None,
        process_id: None,
        process_instance_id: None,
        reused_process: None,
        worker_elapsed_ms: None,
        observed_at,
    };
    if let Some(bytes) = row.try_get::<Option<Vec<u8>>, _>("accepted_command")? {
        // Inspect is explicit and bounded. Do not return output, owner, context
        // or report bytes; lists never select this column.
        let command = SettleCommand::decode(&bytes).map_err(|_| corrupt("settlement"))?;
        if command.owner.scope != *scope
            || command.owner.task_id != detail.attempt.task_id
            || command.owner.attempt_id != id
        {
            return Err(corrupt("settlement identity").into());
        }
        match command.report {
            AttemptReport::Completed(report) => {
                detail.process_id = Some(report.process_id);
                detail.reused_process = Some(report.reused_process);
                detail.worker_elapsed_ms = Some(ConsoleU64(report.elapsed_ms));
                if let ledgence_worker_api::ProgramOutcome::Failure { kind, message } =
                    report.outcome
                {
                    detail.application_error = Some(ApplicationError { kind, message });
                }
            }
            AttemptReport::Failed(failure) => {
                detail.phase = Some(failure.phase);
                detail.error = Some(failure.error);
                detail.cleanup_error = failure.cleanup_error;
            }
        }
    }
    Ok(detail)
}
