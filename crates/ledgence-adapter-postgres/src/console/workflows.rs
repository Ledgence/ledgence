use super::*;
use sqlx::{Postgres, QueryBuilder};
const SUMMARY_COLUMNS: &str = "w.workflow_id,w.state,trunc(w.revision)::text AS revision_text,w.current_activation_id,w.submitted_at_ms,w.terminal_at_ms,w.correlation_key,w.parent_workflow_id,w.root_workflow_id,w.queue";

pub(super) async fn query(
    connection: &mut PgConnection,
    scope: &Scope,
    query: &ConsoleQuery,
    position: Option<&ConsolePosition>,
    at: u64,
) -> StoreResult<ConsoleQueryReply> {
    match query {
        ConsoleQuery::Workflows {
            filters,
            page: request,
        } => Ok(ConsoleQueryReply::Workflows(
            list(connection, scope, query, filters, request, position, at).await?,
        )),
        ConsoleQuery::Workflow { workflow_id } => Ok(ConsoleQueryReply::Workflow(
            detail(connection, scope, workflow_id, at).await?,
        )),
        ConsoleQuery::Activations {
            workflow_id,
            page: request,
        } => {
            require_workflow(connection, scope, workflow_id).await?;
            let mut sql = QueryBuilder::<Postgres>::new(
                "SELECT a.workflow_id,a.activation_id,a.task_id,trunc(a.revision)::text AS revision_text,t.state,a.applied_at_ms,octet_length(a.error_bytes) AS error_size,CASE WHEN octet_length(a.error_bytes)<=",
            );
            sql.push_bind(WORKFLOW_ERROR_ENCODED_MAX_BYTES as i64)
                .push(" THEN a.error_bytes ELSE NULL END AS error_bytes FROM workflow_activations a JOIN tasks t ON t.task_id=a.task_id WHERE a.workflow_id=")
                .push_bind(workflow_id.clone())
                .push(" AND t.tenant_id=")
                .push_bind(scope.tenant_id.clone())
                .push(" AND t.namespace=")
                .push_bind(scope.namespace.clone())
                .push(" AND t.retiring_at_ms IS NULL");
            if let Some(p) = position {
                sql.push(" AND (a.revision,a.activation_id)>(CAST(")
                    .push_bind(position_number(p, 0)?.to_string())
                    .push(" AS numeric),")
                    .push_bind(position_text(p, 1)?)
                    .push(")");
            }
            sql.push(" ORDER BY a.revision,a.activation_id LIMIT ")
                .push_bind(i64::from(request.limit) + 1);
            let rows = sql.build().fetch_all(connection).await?;
            let items = rows
                .iter()
                .map(|r| {
                    if r.try_get::<Option<i32>, _>("error_size")?
                        .is_some_and(|n| i64::from(n) > WORKFLOW_ERROR_ENCODED_MAX_BYTES as i64)
                    {
                        return Err(corrupt("activation error size").into());
                    }
                    Ok(ConsoleActivation {
                        workflow_id: r.try_get("workflow_id")?,
                        activation_id: r.try_get("activation_id")?,
                        task_id: r.try_get("task_id")?,
                        revision: unsigned(r, "revision_text")?,
                        state: enumeration(r, "state")?,
                        applied_at: optional_number(r, "applied_at_ms")?,
                        error: r
                            .try_get::<Option<Vec<u8>>, _>("error_bytes")?
                            .map(|b| codec::decode(&b))
                            .transpose()?,
                    })
                })
                .collect::<StoreResult<Vec<_>>>()?;
            Ok(ConsoleQueryReply::Activations(page(
                items, scope, query, request, at,
            )?))
        }
        ConsoleQuery::Children {
            workflow_id,
            page: request,
        } => Ok(ConsoleQueryReply::Children(
            children(connection, scope, query, workflow_id, request, position, at).await?,
        )),
        ConsoleQuery::Waits {
            workflow_id,
            page: request,
        } => {
            let anchor = anchor(connection, scope, workflow_id).await?;
            let mut sql = QueryBuilder::<Postgres>::new(
                "SELECT workflow_id,wait_key,activation_id,kind,deadline_ms,registered_at_ms,closed_at_ms FROM workflow_waits WHERE workflow_id=",
            );
            sql.push_bind(workflow_id.clone());
            if let Some(p) = position {
                sql.push(" AND (registered_at_ms,wait_key)>(")
                    .push_bind(position_time(p, 0)?)
                    .push(",")
                    .push_bind(position_text(p, 1)?)
                    .push(")");
            }
            sql.push(" ORDER BY registered_at_ms,wait_key LIMIT ")
                .push_bind(i64::from(request.limit) + 1);
            let rows = sql.build().fetch_all(connection).await?;
            let items = rows
                .iter()
                .map(|r| {
                    Ok(ConsoleWorkflowWait {
                        workflow_id: r.try_get("workflow_id")?,
                        wait_key: r.try_get("wait_key")?,
                        activation_id: r.try_get("activation_id")?,
                        kind: enumeration(r, "kind")?,
                        deadline: optional_number(r, "deadline_ms")?,
                        registered_at: number(r, "registered_at_ms")?,
                        closed_at: optional_number(r, "closed_at_ms")?,
                    })
                })
                .collect::<StoreResult<Vec<_>>>()?;
            Ok(ConsoleQueryReply::Waits(ConsoleWorkflowWaits {
                page: page(items, scope, query, request, at)?,
                child_wait: anchor.child_wait,
                revision: anchor.revision,
            }))
        }
        ConsoleQuery::History {
            workflow_id,
            page: request,
        } => {
            require_workflow(connection, scope, workflow_id).await?;
            let mut sql = QueryBuilder::<Postgres>::new(
                "SELECT workflow_id,trunc(sequence)::text AS sequence_text,activation_id,at_ms,reason FROM workflow_history WHERE workflow_id=",
            );
            sql.push_bind(workflow_id.clone());
            if let Some(p) = position {
                sql.push(" AND sequence>CAST(")
                    .push_bind(position_number(p, 0)?.to_string())
                    .push(" AS numeric)");
            }
            sql.push(" ORDER BY sequence LIMIT ")
                .push_bind(i64::from(request.limit) + 1);
            let rows = sql.build().fetch_all(connection).await?;
            let items = rows
                .iter()
                .map(|r| {
                    Ok(ConsoleWorkflowHistory {
                        workflow_id: r.try_get("workflow_id")?,
                        sequence: unsigned(r, "sequence_text")?,
                        activation_id: r.try_get("activation_id")?,
                        at: number(r, "at_ms")?,
                        reason: r.try_get("reason")?,
                    })
                })
                .collect::<StoreResult<Vec<_>>>()?;
            Ok(ConsoleQueryReply::History(page(
                items, scope, query, request, at,
            )?))
        }
        ConsoleQuery::LocalSteps {
            workflow_id,
            activation_id,
            page: request,
        } => {
            require_workflow(connection, scope, workflow_id).await?;
            let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workflow_activations a JOIN tasks t ON t.task_id=a.task_id WHERE a.workflow_id=$1 AND a.activation_id=$2 AND t.tenant_id=$3 AND t.namespace=$4 AND t.retiring_at_ms IS NULL)")
                .bind(workflow_id).bind(activation_id).bind(&scope.tenant_id).bind(&scope.namespace).fetch_one(&mut *connection).await?;
            if !exists {
                return Err(ContractError::NotFound.into());
            }
            let mut sql = QueryBuilder::<Postgres>::new(
                "SELECT activation_id,step_key,callable,attempt_id,accepted_at_ms FROM workflow_local_results WHERE activation_id=",
            );
            sql.push_bind(activation_id.clone());
            if let Some(p) = position {
                sql.push(" AND step_key>").push_bind(position_text(p, 0)?);
            }
            sql.push(" ORDER BY step_key LIMIT ")
                .push_bind(i64::from(request.limit) + 1);
            let rows = sql.build().fetch_all(connection).await?;
            let items = rows
                .iter()
                .map(|r| {
                    Ok(ConsoleLocalStep {
                        workflow_id: workflow_id.clone(),
                        activation_id: r.try_get("activation_id")?,
                        step_key: r.try_get("step_key")?,
                        callable: r.try_get("callable")?,
                        attempt_id: r.try_get("attempt_id")?,
                        accepted_at: number(r, "accepted_at_ms")?,
                    })
                })
                .collect::<StoreResult<Vec<_>>>()?;
            Ok(ConsoleQueryReply::LocalSteps(page(
                items, scope, query, request, at,
            )?))
        }
        _ => Err(corrupt("unsupported workflow observation").into()),
    }
}

async fn require_workflow(
    connection: &mut PgConnection,
    scope: &Scope,
    id: &str,
) -> StoreResult<()> {
    let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND retiring_at_ms IS NULL)")
        .bind(&scope.tenant_id).bind(&scope.namespace).bind(id).fetch_one(connection).await?;
    if !exists {
        return Err(ContractError::NotFound.into());
    }
    Ok(())
}
fn summary(row: &PgRow) -> StoreResult<ConsoleWorkflowSummary> {
    Ok(ConsoleWorkflowSummary {
        workflow: ConsoleWorkflowStatus {
            workflow_id: row.try_get("workflow_id")?,
            state: enumeration(row, "state")?,
            revision: unsigned(row, "revision_text")?,
            activation_id: row.try_get("current_activation_id")?,
            submitted_at: number(row, "submitted_at_ms")?,
            terminal_at: optional_number(row, "terminal_at_ms")?,
            correlation_key: row.try_get("correlation_key")?,
            parent_workflow_id: row.try_get("parent_workflow_id")?,
            root_workflow_id: row.try_get("root_workflow_id")?,
        },
        controller: descriptor(row, "controller")?,
        queue: row.try_get("queue")?,
    })
}
async fn list(
    connection: &mut PgConnection,
    scope: &Scope,
    query: &ConsoleQuery,
    filters: &ConsoleWorkflowFilters,
    request: &ConsolePagination,
    position: Option<&ConsolePosition>,
    at: u64,
) -> StoreResult<ConsolePage<ConsoleWorkflowSummary>> {
    if let Some(parent) = &filters.parent_workflow_id {
        require_workflow(connection, scope, parent).await?;
    }
    let mut sql = QueryBuilder::<Postgres>::new(
        "SELECT chosen.*,CASE WHEN octet_length(r.controller_bytes)<=4096 THEN r.controller_bytes ELSE NULL END AS controller FROM (SELECT ",
    );
    sql.push(SUMMARY_COLUMNS)
        .push(" FROM workflow_runs w WHERE w.tenant_id=")
        .push_bind(scope.tenant_id.clone())
        .push(" AND w.namespace=")
        .push_bind(scope.namespace.clone())
        .push(" AND w.retiring_at_ms IS NULL");
    if let Some(state) = filters.state {
        sql.push(" AND w.state=").push_bind(codec::label(&state)?);
    }
    if let Some(key) = &filters.correlation_key {
        sql.push(" AND w.correlation_key=").push_bind(key.clone());
    }
    if let Some(from) = filters.submitted_from {
        sql.push(" AND w.submitted_at_ms>=").push_bind(from as i64);
    }
    if let Some(until) = filters.submitted_until {
        sql.push(" AND w.submitted_at_ms<").push_bind(until as i64);
    }
    if let Some(parent) = &filters.parent_workflow_id {
        sql.push(" AND w.parent_workflow_id=")
            .push_bind(parent.clone());
    }
    if filters.root_only {
        sql.push(" AND w.parent_workflow_id IS NULL");
    }
    if let Some(p) = position {
        sql.push(" AND (w.submitted_at_ms,w.workflow_id)<(")
            .push_bind(position_time(p, 0)?)
            .push(",")
            .push_bind(position_text(p, 1)?)
            .push(")");
    }
    sql.push(" ORDER BY w.submitted_at_ms DESC,w.workflow_id DESC LIMIT ").push_bind(i64::from(request.limit)+1)
        .push(") chosen JOIN workflow_runs r ON r.workflow_id=chosen.workflow_id ORDER BY chosen.submitted_at_ms DESC,chosen.workflow_id DESC");
    let rows = sql.build().fetch_all(connection).await?;
    page(
        rows.iter().map(summary).collect::<StoreResult<Vec<_>>>()?,
        scope,
        query,
        request,
        at,
    )
}
struct Anchor {
    revision: ConsoleU64,
    child_wait: Option<ConsoleChildWait>,
    external_wait_key: Option<String>,
}
fn decode_anchor(row: &PgRow) -> StoreResult<Anchor> {
    let external_wait_key: Option<String> = row.try_get("external_wait_key")?;
    let wait: Option<String> = row.try_get("wait_activation_id")?;
    let child_wait = if external_wait_key.is_none() {
        if let Some(activation_id) = wait {
            let bytes = row
                .try_get::<Option<Vec<u8>>, _>("wait_keys_bytes")?
                .ok_or_else(|| corrupt("child wait membership"))?;
            let command_keys: Vec<String> =
                decode_unique_json(&bytes, 65536).map_err(|_| corrupt("child wait membership"))?;
            Some(ConsoleChildWait {
                activation_id,
                command_keys,
            })
        } else {
            None
        }
    } else {
        None
    };
    Ok(Anchor {
        revision: unsigned(row, "revision_text")?,
        child_wait,
        external_wait_key,
    })
}
async fn anchor(connection: &mut PgConnection, scope: &Scope, id: &str) -> StoreResult<Anchor> {
    let row=sqlx::query("SELECT trunc(revision)::text AS revision_text,wait_activation_id,CASE WHEN octet_length(wait_keys_bytes)<=65536 THEN wait_keys_bytes ELSE NULL END AS wait_keys_bytes,external_wait_key FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND retiring_at_ms IS NULL")
        .bind(&scope.tenant_id).bind(&scope.namespace).bind(id).fetch_optional(connection).await?.ok_or(ContractError::NotFound)?;
    decode_anchor(&row)
}
async fn detail(
    connection: &mut PgConnection,
    scope: &Scope,
    id: &str,
    at: u64,
) -> StoreResult<ConsoleWorkflowDetail> {
    let mut sql = QueryBuilder::<Postgres>::new("SELECT ");
    sql.push(SUMMARY_COLUMNS).push(",CASE WHEN octet_length(w.controller_bytes)<=4096 THEN w.controller_bytes ELSE NULL END AS controller,w.continuation,w.wait_activation_id,CASE WHEN octet_length(w.wait_keys_bytes)<=65536 THEN w.wait_keys_bytes ELSE NULL END AS wait_keys_bytes,w.external_wait_key FROM workflow_runs w WHERE w.tenant_id=")
        .push_bind(scope.tenant_id.clone()).push(" AND w.namespace=").push_bind(scope.namespace.clone()).push(" AND w.workflow_id=").push_bind(id.to_owned()).push(" AND w.retiring_at_ms IS NULL");
    let row = sql
        .build()
        .fetch_optional(connection)
        .await?
        .ok_or(ContractError::NotFound)?;
    let anchor = decode_anchor(&row)?;
    Ok(ConsoleWorkflowDetail {
        summary: summary(&row)?,
        continuation: row.try_get("continuation")?,
        child_wait: anchor.child_wait,
        external_wait_key: anchor.external_wait_key,
        observed_at: at,
    })
}

async fn children(
    connection: &mut PgConnection,
    scope: &Scope,
    query: &ConsoleQuery,
    id: &str,
    request: &ConsolePagination,
    position: Option<&ConsolePosition>,
    at: u64,
) -> StoreResult<ConsolePage<ConsoleWorkflowChild>> {
    require_workflow(connection, scope, id).await?;
    let mut sql = children_query(scope, id, request.limit, position)?;
    let rows = sql.build().fetch_all(connection).await?;
    let items = rows
        .iter()
        .map(|r| {
            Ok(ConsoleWorkflowChild {
                workflow_id: id.to_owned(),
                creating_activation_id: r.try_get("activation_id")?,
                creating_revision: unsigned(r, "revision_text")?,
                kind: enumeration(r, "kind")?,
                command_key: r.try_get("command_key")?,
                target_id: r.try_get("target_id")?,
                task_state: r
                    .try_get::<Option<String>, _>("task_state")?
                    .map(|s| {
                        serde_json::from_value(serde_json::Value::String(s))
                            .map_err(|_| corrupt("task state"))
                    })
                    .transpose()?,
                workflow_state: r
                    .try_get::<Option<String>, _>("workflow_state")?
                    .map(|s| {
                        serde_json::from_value(serde_json::Value::String(s))
                            .map_err(|_| corrupt("workflow state"))
                    })
                    .transpose()?,
                consumed: r.try_get("consumed")?,
            })
        })
        .collect::<StoreResult<Vec<_>>>()?;
    page(items, scope, query, request, at)
}
/// Each branch seeks directly in a persisted index before hydrating status.
/// UNION ALL merges at most 2*(limit+1) metadata rows, independent of depth.
pub(super) fn children_query(
    scope: &Scope,
    id: &str,
    limit: u32,
    position: Option<&ConsolePosition>,
) -> Result<QueryBuilder<Postgres>> {
    let mut sql = QueryBuilder::<Postgres>::new("SELECT * FROM (");
    for (index, kind) in ["task", "workflow"].into_iter().enumerate() {
        if index > 0 {
            sql.push(" UNION ALL ");
        }
        sql.push("(SELECT l.creating_revision,trunc(l.creating_revision)::text AS revision_text,");
        if kind == "task" {
            sql.push("'task'::text AS kind,l.activation_id,l.command_key,l.task_id AS target_id,t.state AS task_state,NULL::text AS workflow_state,l.consumed FROM workflow_task_links l JOIN tasks t ON t.task_id=l.task_id WHERE NOT l.is_activation AND l.workflow_id=");
        } else {
            sql.push("'workflow'::text AS kind,l.creating_activation_id AS activation_id,l.command_key,l.child_workflow_id AS target_id,NULL::text AS task_state,t.state AS workflow_state,l.consumed FROM owned_workflow_links l JOIN workflow_runs t ON t.workflow_id=l.child_workflow_id WHERE l.parent_workflow_id=");
        }
        sql.push_bind(id.to_owned())
            .push(" AND t.tenant_id=")
            .push_bind(scope.tenant_id.clone())
            .push(" AND t.namespace=")
            .push_bind(scope.namespace.clone())
            .push(" AND t.retiring_at_ms IS NULL");
        let target = if kind == "task" {
            "l.task_id"
        } else {
            "l.child_workflow_id"
        };
        if let Some(p) = position {
            // Each branch has constant kind: reduce the mixed-kind cursor to
            // a direct range on its real (revision,key,id) index.
            let cursor_kind = position_text(p, 1)?;
            match kind.cmp(cursor_kind.as_str()) {
                std::cmp::Ordering::Equal => {
                    sql.push(" AND (l.creating_revision,l.command_key,")
                        .push(target)
                        .push(")>(CAST(")
                        .push_bind(position_number(p, 0)?.to_string())
                        .push(" AS numeric),")
                        .push_bind(position_text(p, 2)?)
                        .push(",")
                        .push_bind(position_text(p, 3)?)
                        .push(")");
                }
                ordering => {
                    sql.push(if ordering == std::cmp::Ordering::Greater {
                        " AND l.creating_revision>=CAST("
                    } else {
                        " AND l.creating_revision>CAST("
                    })
                    .push_bind(position_number(p, 0)?.to_string())
                    .push(" AS numeric)");
                }
            }
        }
        sql.push(" ORDER BY l.creating_revision,l.command_key,")
            .push(target)
            .push(" LIMIT ")
            .push_bind(i64::from(limit) + 1)
            .push(")");
    }
    sql.push(
        ") children ORDER BY creating_revision,kind COLLATE \"C\",command_key,target_id LIMIT ",
    )
    .push_bind(i64::from(limit) + 1);
    Ok(sql)
}
