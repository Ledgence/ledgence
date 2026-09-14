//! A single statement snapshot over compact task metadata, with keyset pagination.

use crate::*;
use sqlx::{Postgres, QueryBuilder};

const STATUS_COLUMNS: &str = "t.tenant_id,t.namespace,t.task_id,t.run_id,t.queue,t.correlation_key,\
    t.state,t.attempt_count,t.current_attempt_id,t.submitted_at_ms,\
    t.available_at_ms,t.terminal_at_ms,t.cancel_requested_at_ms,t.next_expiry_ms";

/// Predicates are selected from fixed SQL fragments; all values are bound.
/// Omitting absent predicates lets PostgreSQL use the corresponding index range.
pub(super) fn query(
    scope: &Scope,
    request: &TaskListQuery,
    position: Option<&TaskPosition>,
) -> QueryBuilder<Postgres> {
    // Limit the ordered task shortlist before joining attempts. This also bounds
    // metadata hydration when the planner chooses a different join strategy.
    let mut sql = QueryBuilder::new("SELECT ");
    sql.push(STATUS_COLUMNS)
        .push(",a.attempt_id AS latest_attempt_id,a.state AS latest_attempt_state FROM (SELECT ")
        .push(STATUS_COLUMNS)
        .push(" FROM tasks t WHERE t.tenant_id=")
        .push_bind(&scope.tenant_id)
        .push(" AND t.namespace=")
        .push_bind(&scope.namespace);
    let filters = &request.filters;
    if let Some(state) = filters.state {
        sql.push(" AND t.state=").push_bind(match state {
            TaskState::Queued => "queued",
            TaskState::Active => "active",
            TaskState::Succeeded => "succeeded",
            TaskState::Failed => "failed",
            TaskState::Cancelled => "cancelled",
        });
    }
    if let Some(queue) = &filters.queue {
        sql.push(" AND t.queue=").push_bind(queue);
    }
    if let Some(key) = &filters.correlation_key {
        sql.push(" AND t.correlation_key=").push_bind(key);
    }
    if let Some(from) = filters.submitted_from {
        sql.push(" AND t.submitted_at_ms>=").push_bind(from as i64);
    }
    if let Some(until) = filters.submitted_until {
        sql.push(" AND t.submitted_at_ms<").push_bind(until as i64);
    }
    if let Some(position) = position {
        sql.push(" AND (t.submitted_at_ms,t.task_id)<(")
            .push_bind(position.submitted_at as i64)
            .push(",")
            .push_bind(&position.task_id)
            .push(")");
    }
    sql.push(" ORDER BY t.submitted_at_ms DESC,t.task_id DESC LIMIT ")
        .push_bind(i64::from(request.limit) + 1)
        .push(") t LEFT JOIN attempts a ON a.task_id=t.task_id AND a.generation=t.attempt_count ")
        .push("ORDER BY t.submitted_at_ms DESC,t.task_id DESC");
    sql
}

impl PostgresStore {
    pub(super) async fn list_tasks_once(
        &self,
        scope: &Scope,
        request: &TaskListQuery,
    ) -> StoreResult<TaskPage> {
        let position = request.validate(scope)?;
        let rows = query(scope, request, position.as_ref())
            .build()
            .fetch_all(&self.pool)
            .await?;
        let has_more = rows.len() > request.limit as usize;
        let items = rows
            .iter()
            .take(request.limit as usize)
            .map(codec::status)
            .collect::<Result<Vec<_>>>()?;
        let next_cursor = if has_more {
            let last = items
                .last()
                .ok_or_else(|| ContractError::Unavailable("invalid task discovery page".into()))?;
            Some(request.next_cursor(scope, &TaskPosition::from(last))?)
        } else {
            None
        };
        let page = TaskPage { items, next_cursor };
        page.validate(scope, request)?;
        Ok(page)
    }
}
