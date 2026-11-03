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
            let mut sql = waits_query(workflow_id, request.limit, position)?;
            let rows = sql.build().fetch_all(connection).await?;
            let items = rows
                .iter()
                .map(|r| {
                    let native_effect_wait = r
                        .try_get::<Option<Vec<u8>>, _>("native_wait_bytes")?
                        .map(|bytes| {
                            native_wait(
                                &bytes,
                                &r.try_get::<String, _>("activation_id")?,
                                scope,
                                workflow_id,
                            )
                        })
                        .transpose()?;
                    let provider_wait = r
                        .try_get::<Option<Vec<u8>>, _>("provider_wait_bytes")?
                        .map(|bytes| {
                            provider_wait(
                                &bytes,
                                &r.try_get::<String, _>("activation_id")?,
                                scope,
                                workflow_id,
                            )
                        })
                        .transpose()?;
                    Ok(ConsoleWorkflowWait {
                        workflow_id: r.try_get("workflow_id")?,
                        wait_key: r.try_get("wait_key")?,
                        activation_id: r.try_get("activation_id")?,
                        kind: enumeration(r, "kind")?,
                        deadline: optional_number(r, "deadline_ms")?,
                        registered_at: number(r, "registered_at_ms")?,
                        closed_at: optional_number(r, "closed_at_ms")?,
                        native_effect_wait,
                        provider_wait,
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

/// Each namespace contributes at most limit+1 metadata rows. A same-text legacy
/// event and native wait remain distinct even when registered at the same time.
pub(super) fn waits_query(
    workflow_id: &str,
    limit: u32,
    position: Option<&ConsolePosition>,
) -> Result<QueryBuilder<Postgres>> {
    let mut sql = QueryBuilder::<Postgres>::new("SELECT * FROM (");
    for namespace in [None, Some("effect_resolution"), Some("provider_operation")] {
        let native = namespace.is_some();
        if native {
            sql.push(" UNION ALL ");
        }
        sql.push("(SELECT workflow_id,");
        if namespace == Some("provider_operation") {
            sql.push("logical_key AS wait_key,parking_activation_id AS activation_id,'provider_operation'::text COLLATE \"C\" AS kind,NULL::bigint AS deadline_ms,registered_at_ms,closed_at_ms,NULL::bytea AS native_wait_bytes,wait_bytes AS provider_wait_bytes FROM workflow_provider_waits WHERE workflow_id=");
        } else if native {
            sql.push("logical_key AS wait_key,parking_activation_id AS activation_id,'effect_resolution'::text COLLATE \"C\" AS kind,NULL::bigint AS deadline_ms,registered_at_ms,closed_at_ms,wait_bytes AS native_wait_bytes,NULL::bytea AS provider_wait_bytes FROM workflow_effect_waits WHERE workflow_id=");
        } else {
            sql.push("wait_key,activation_id,kind,deadline_ms,registered_at_ms,closed_at_ms,NULL::bytea AS native_wait_bytes,NULL::bytea AS provider_wait_bytes FROM workflow_waits WHERE workflow_id=");
        }
        sql.push_bind(workflow_id.to_owned());
        if let Some(p) = position {
            if native {
                // Constant namespace: reduce the cursor to a range the actual
                // index can seek, including arbitrarily many equal timestamps.
                match namespace
                    .expect("native namespace")
                    .cmp(position_text(p, 1)?.as_str())
                {
                    std::cmp::Ordering::Equal => {
                        sql.push(" AND (registered_at_ms,logical_key,parking_activation_id)>(")
                            .push_bind(position_time(p, 0)?)
                            .push(",")
                            .push_bind(position_text(p, 2)?)
                            .push(",")
                            .push_bind(position_text(p, 3)?)
                            .push(")");
                    }
                    order => {
                        sql.push(if order == std::cmp::Ordering::Greater {
                            " AND registered_at_ms>="
                        } else {
                            " AND registered_at_ms>"
                        })
                        .push_bind(position_time(p, 0)?);
                    }
                }
            } else {
                sql.push(" AND (registered_at_ms,kind,wait_key,activation_id)>(")
                    .push_bind(position_time(p, 0)?)
                    .push(",")
                    .push_bind(position_text(p, 1)?)
                    .push(",")
                    .push_bind(position_text(p, 2)?)
                    .push(",")
                    .push_bind(position_text(p, 3)?)
                    .push(")");
            }
        }
        sql.push(if native {
            " ORDER BY registered_at_ms,logical_key,parking_activation_id LIMIT "
        } else {
            " ORDER BY registered_at_ms,kind,wait_key,activation_id LIMIT "
        })
        .push_bind(i64::from(limit) + 1)
        .push(")");
    }
    sql.push(") waits ORDER BY registered_at_ms,kind COLLATE \"C\",wait_key,activation_id LIMIT ")
        .push_bind(i64::from(limit) + 1);
    Ok(sql)
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
    native_effect_wait: Option<ConsoleNativeEffectWait>,
    provider_wait: Option<ConsoleProviderWait>,
}
fn native_wait(
    bytes: &[u8],
    activation: &str,
    scope: &Scope,
    workflow_id: &str,
) -> StoreResult<ConsoleNativeEffectWait> {
    let wait: WorkflowWait =
        decode_unique_json(bytes, 4096).map_err(|_| corrupt("native wait metadata"))?;
    let WorkflowWait::EffectResolution {
        key,
        occurrence,
        after_resolution_id,
    } = wait
    else {
        return Err(corrupt("native wait kind").into());
    };
    let wait = ConsoleNativeEffectWait {
        parking_activation_id: activation.into(),
        key,
        occurrence,
        after_resolution_id,
    };
    wait.validate(workflow_id)?;
    if &wait.occurrence.scope != scope {
        return Err(corrupt("native wait scope").into());
    }
    Ok(wait)
}
fn provider_wait(
    bytes: &[u8],
    activation: &str,
    scope: &Scope,
    workflow_id: &str,
) -> StoreResult<ConsoleProviderWait> {
    let wait: ProviderWait =
        decode_unique_json(bytes, 4096).map_err(|_| corrupt("provider wait metadata"))?;
    let wait = ConsoleProviderWait {
        parking_activation_id: activation.into(),
        key: wait.key,
        occurrence: wait.occurrence,
        after_effect_revision: wait.after_effect_revision.map(ConsoleU64),
    };
    wait.validate(workflow_id)?;
    if &wait.occurrence.scope != scope {
        return Err(corrupt("provider wait scope").into());
    }
    Ok(wait)
}
fn decode_anchor(row: &PgRow, scope: &Scope, workflow_id: &str) -> StoreResult<Anchor> {
    let external_wait_key: Option<String> = row.try_get("external_wait_key")?;
    let wait: Option<String> = row.try_get("wait_activation_id")?;
    let native_activation: Option<String> = row.try_get("native_wait_activation_id")?;
    let provider_activation: Option<String> = row.try_get("provider_wait_activation_id")?;
    let provider_wait = if let Some(activation) = &provider_activation {
        if external_wait_key.is_some()
            || native_activation.is_some()
            || wait.as_ref() != Some(activation)
        {
            return Err(corrupt("provider wait anchor").into());
        }
        Some(provider_wait(
            &row.try_get::<Option<Vec<u8>>, _>("provider_wait_bytes")?
                .ok_or_else(|| corrupt("missing provider wait"))?,
            activation,
            scope,
            workflow_id,
        )?)
    } else {
        None
    };
    let native_effect_wait = if let Some(activation) = &native_activation {
        if external_wait_key.is_some() || wait.as_ref() != Some(activation) {
            return Err(corrupt("native wait anchor").into());
        }
        Some(native_wait(
            &row.try_get::<Option<Vec<u8>>, _>("native_wait_bytes")?
                .ok_or_else(|| corrupt("missing native wait"))?,
            activation,
            scope,
            workflow_id,
        )?)
    } else {
        None
    };
    let child_wait = if external_wait_key.is_none()
        && native_activation.is_none()
        && provider_activation.is_none()
    {
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
        native_effect_wait,
        provider_wait,
    })
}
async fn anchor(connection: &mut PgConnection, scope: &Scope, id: &str) -> StoreResult<Anchor> {
    let row=sqlx::query("SELECT trunc(w.revision)::text AS revision_text,w.wait_activation_id,CASE WHEN octet_length(w.wait_keys_bytes)<=65536 THEN w.wait_keys_bytes ELSE NULL END AS wait_keys_bytes,w.external_wait_key,w.native_wait_activation_id,n.wait_bytes AS native_wait_bytes,w.provider_wait_activation_id,p.wait_bytes AS provider_wait_bytes FROM workflow_runs w LEFT JOIN workflow_effect_waits n ON n.workflow_id=w.workflow_id AND n.parking_activation_id=w.native_wait_activation_id LEFT JOIN workflow_provider_waits p ON p.workflow_id=w.workflow_id AND p.parking_activation_id=w.provider_wait_activation_id WHERE w.tenant_id=$1 AND w.namespace=$2 AND w.workflow_id=$3 AND w.retiring_at_ms IS NULL")
        .bind(&scope.tenant_id).bind(&scope.namespace).bind(id).fetch_optional(connection).await?.ok_or(ContractError::NotFound)?;
    decode_anchor(&row, scope, id)
}
async fn detail(
    connection: &mut PgConnection,
    scope: &Scope,
    id: &str,
    at: u64,
) -> StoreResult<ConsoleWorkflowDetail> {
    let mut sql = QueryBuilder::<Postgres>::new("SELECT ");
    sql.push(SUMMARY_COLUMNS).push(",CASE WHEN octet_length(w.controller_bytes)<=4096 THEN w.controller_bytes ELSE NULL END AS controller,w.continuation,w.wait_activation_id,CASE WHEN octet_length(w.wait_keys_bytes)<=65536 THEN w.wait_keys_bytes ELSE NULL END AS wait_keys_bytes,w.external_wait_key,w.native_wait_activation_id,n.wait_bytes AS native_wait_bytes,w.provider_wait_activation_id,p.wait_bytes AS provider_wait_bytes FROM workflow_runs w LEFT JOIN workflow_effect_waits n ON n.workflow_id=w.workflow_id AND n.parking_activation_id=w.native_wait_activation_id LEFT JOIN workflow_provider_waits p ON p.workflow_id=w.workflow_id AND p.parking_activation_id=w.provider_wait_activation_id WHERE w.tenant_id=")
        .push_bind(scope.tenant_id.clone()).push(" AND w.namespace=").push_bind(scope.namespace.clone()).push(" AND w.workflow_id=").push_bind(id.to_owned()).push(" AND w.retiring_at_ms IS NULL");
    let row = sql
        .build()
        .fetch_optional(connection)
        .await?
        .ok_or(ContractError::NotFound)?;
    let anchor = decode_anchor(&row, scope, id)?;
    Ok(ConsoleWorkflowDetail {
        summary: summary(&row)?,
        continuation: row.try_get("continuation")?,
        child_wait: anchor.child_wait,
        external_wait_key: anchor.external_wait_key,
        native_effect_wait: anchor.native_effect_wait,
        provider_wait: anchor.provider_wait,
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
