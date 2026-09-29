use super::*;
use sqlx::{Postgres, QueryBuilder};

pub(super) async fn list(
    connection: &mut PgConnection,
    scope: &Scope,
    query: &ConsoleQuery,
    filters: &ConsoleExecutionFilters,
    request: &ConsolePagination,
    position: Option<&ConsolePosition>,
    observed_at: u64,
) -> StoreResult<ConsolePage<ConsoleExecutionSummary>> {
    let rows = discovery_query(scope, filters, request.limit, position)?
        .build()
        .fetch_all(connection)
        .await?;
    let items = rows
        .iter()
        .map(|row| {
            Ok(ConsoleExecutionSummary {
                kind: enumeration(row, "kind")?,
                id: row.try_get("id")?,
                descriptor: descriptor(row, "descriptor")?,
                queue: row.try_get("queue")?,
                state: enumeration(row, "state")?,
                submitted_at: number(row, "submitted_at_ms")?,
                terminal_at: optional_number(row, "terminal_at_ms")?,
                correlation_key: row.try_get("correlation_key")?,
                parent_workflow_id: row.try_get("parent_workflow_id")?,
                root_workflow_id: row.try_get("root_workflow_id")?,
            })
        })
        .collect::<StoreResult<Vec<_>>>()?;
    page(items, scope, query, request, observed_at)
}

/// Seek independently in each typed index, merge at most 2*(limit+1) compact
/// records, then hydrate only the selected descriptors. C collation agrees with
/// the API's UTF-8 byte ordering even when IDs or submission timestamps collide.
pub(super) fn discovery_query(
    scope: &Scope,
    filters: &ConsoleExecutionFilters,
    limit: u32,
    position: Option<&ConsolePosition>,
) -> Result<QueryBuilder<Postgres>> {
    let mut sql = QueryBuilder::<Postgres>::new("WITH chosen AS (SELECT * FROM (");
    for (index, kind) in [ConsoleExecutionKind::Task, ConsoleExecutionKind::Workflow]
        .into_iter()
        .enumerate()
    {
        if index > 0 {
            sql.push(" UNION ALL ");
        }
        let task = kind == ConsoleExecutionKind::Task;
        let (table, id, parent, root) = if task {
            (
                "tasks",
                "task_id",
                "workflow_id",
                "CASE WHEN t.workflow_id IS NULL THEN NULL ELSE coalesce(t.root_workflow_id,t.workflow_id) END",
            )
        } else {
            (
                "workflow_runs",
                "workflow_id",
                "parent_workflow_id",
                "t.root_workflow_id",
            )
        };
        sql.push("(SELECT '")
            .push(kind.as_str())
            .push("'::text COLLATE \"C\" AS kind,t.")
            .push(id)
            .push(" AS id,t.queue,t.state,t.submitted_at_ms,t.terminal_at_ms,t.correlation_key,t.")
            .push(parent)
            .push(" AS parent_workflow_id,")
            .push(root)
            .push(" AS root_workflow_id FROM ")
            .push(table)
            .push(" t WHERE t.tenant_id=")
            .push_bind(scope.tenant_id.clone())
            .push(" AND t.namespace=")
            .push_bind(scope.namespace.clone())
            .push(" AND t.retiring_at_ms IS NULL");
        if task {
            sql.push(" AND t.workflow_activation_id IS NULL");
        }
        if filters.kind.is_some_and(|k| k != kind)
            || filters.state.is_some_and(|s| !s.supports(kind))
        {
            sql.push(" AND false");
        }
        if !filters.includes_children() {
            sql.push(" AND t.").push(parent).push(" IS NULL");
        }
        for (column, value) in [
            ("program_id", &filters.program_id),
            ("program_version", &filters.version),
            ("queue", &filters.queue),
            ("correlation_key", &filters.correlation_key),
            (id, &filters.execution_id),
        ] {
            if let Some(value) = value {
                sql.push(" AND t.")
                    .push(column)
                    .push("=")
                    .push_bind(value.clone());
            }
        }
        if let Some(state) = filters.state {
            sql.push(" AND t.state=").push_bind(codec::label(&state)?);
        }
        if let Some(at) = filters.submitted_from {
            sql.push(" AND t.submitted_at_ms>=").push_bind(at as i64);
        }
        if let Some(at) = filters.submitted_until {
            sql.push(" AND t.submitted_at_ms<").push_bind(at as i64);
        }
        if let Some(p) = position {
            let cursor_kind = position_text(p, 1)?;
            if !matches!(cursor_kind.as_str(), "task" | "workflow") {
                return Err(ContractError::InvalidInput(
                    "invalid execution cursor kind".into(),
                ));
            }
            match kind.as_str().cmp(cursor_kind.as_str()) {
                std::cmp::Ordering::Equal => {
                    sql.push(" AND (t.submitted_at_ms,t.")
                        .push(id)
                        .push(")<(")
                        .push_bind(position_time(p, 0)?)
                        .push(",")
                        .push_bind(position_text(p, 2)?)
                        .push(")");
                }
                ordering => {
                    sql.push(if ordering == std::cmp::Ordering::Less {
                        " AND t.submitted_at_ms<="
                    } else {
                        " AND t.submitted_at_ms<"
                    })
                    .push_bind(position_time(p, 0)?);
                }
            }
        }
        sql.push(" ORDER BY t.submitted_at_ms DESC,t.")
            .push(id)
            .push(" DESC LIMIT ")
            .push_bind(i64::from(limit) + 1)
            .push(")");
    }
    sql.push(") candidates ORDER BY submitted_at_ms DESC,kind COLLATE \"C\" DESC,id COLLATE \"C\" DESC LIMIT ")
        .push_bind(i64::from(limit)+1)
        .push(") SELECT chosen.*,CASE WHEN chosen.kind='task' THEN CASE WHEN octet_length(t.descriptor_bytes)<=4096 THEN t.descriptor_bytes END ELSE CASE WHEN octet_length(w.controller_bytes)<=4096 THEN w.controller_bytes END END AS descriptor FROM chosen LEFT JOIN tasks t ON chosen.kind='task' AND t.task_id=chosen.id LEFT JOIN workflow_runs w ON chosen.kind='workflow' AND w.workflow_id=chosen.id ORDER BY chosen.submitted_at_ms DESC,chosen.kind COLLATE \"C\" DESC,chosen.id COLLATE \"C\" DESC");
    Ok(sql)
}
