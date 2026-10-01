//! One bounded metadata page from a repeatable-read transaction. No submitted
//! data, checkpoint, local output or accepted report is read for the graph.
use super::*;
use sqlx::{Postgres, QueryBuilder};
use std::collections::BTreeMap;
#[cfg(test)]
#[path = "explorer_tests.rs"]
mod tests;

pub(super) async fn query(
    connection: &mut PgConnection,
    scope: &Scope,
    query: &ConsoleQuery,
    position: Option<&ConsolePosition>,
    at: u64,
) -> StoreResult<ConsoleQueryReply> {
    match query {
        ConsoleQuery::Explorer {
            workflow_id,
            page: request,
        } => {
            let ConsoleQueryReply::Workflow(workflow) = workflows::query(
                connection,
                scope,
                &ConsoleQuery::Workflow {
                    workflow_id: workflow_id.clone(),
                },
                None,
                at,
            )
            .await?
            else {
                return Err(corrupt("workflow explorer header").into());
            };
            let mut sql = explorer_query(workflow_id, request.limit, position)?;
            let rows = sql.build().fetch_all(&mut *connection).await?;
            let mut items = rows
                .iter()
                .map(|row| {
                    let data: ConsoleExplorerData =
                        codec::decode(&row.try_get::<Vec<u8>, _>("metadata_bytes")?)?;
                    let activation_id: String = row.try_get("activation_id")?;
                    if data.kind() != row.try_get::<String, _>("kind")?
                        || data.key() != row.try_get::<String, _>("record_key")?
                    {
                        return Err(corrupt("explorer record identity").into());
                    }
                    Ok(ConsoleExplorerNode {
                        id: serde_json::to_string(&(
                            data.kind(),
                            workflow_id,
                            &activation_id,
                            data.key(),
                        ))
                        .map_err(|_| corrupt("explorer identity"))?,
                        activation_id,
                        revision: unsigned(row, "revision_text")?,
                        entrypoint: row.try_get("entrypoint")?,
                        relations: Vec::new(),
                        data,
                    })
                })
                .collect::<StoreResult<Vec<_>>>()?;
            hydrate(connection, scope, workflow_id, &mut items).await?;
            for node in &mut items {
                node.relations = node.derive_relations(workflow_id)?;
            }
            let page = bounded_page(items, &workflow, scope, query, request, at)?;
            Ok(ConsoleQueryReply::Explorer(ConsoleWorkflowExplorer {
                workflow,
                page,
                evidence: "retained_records_only".into(),
            }))
        }
        ConsoleQuery::WorkflowInput { workflow_id } => {
            let bytes: Vec<u8> = sqlx::query_scalar("SELECT submission_bytes FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND retiring_at_ms IS NULL")
                .bind(&scope.tenant_id).bind(&scope.namespace).bind(workflow_id).fetch_optional(connection).await?.ok_or(ContractError::NotFound)?;
            let command: SubmitCommand = codec::decode(&bytes)?;
            ledgence_orchestration_core::validate_submission(&command)?;
            if command.input.tenant_id != scope.tenant_id
                || command.input.namespace != scope.namespace
            {
                return Err(corrupt("workflow input scope").into());
            }
            Ok(ConsoleQueryReply::WorkflowInput(ConsoleWorkflowInput {
                workflow_id: workflow_id.clone(),
                data: command.input.data,
                observed_at: at,
            }))
        }
        ConsoleQuery::Ancestry { execution } => Ok(ConsoleQueryReply::Ancestry(
            ancestry(connection, scope, execution, at).await?,
        )),
        _ => Err(corrupt("unsupported explorer query").into()),
    }
}

fn bounded_page(
    mut items: Vec<ConsoleExplorerNode>,
    workflow: &ConsoleWorkflowDetail,
    scope: &Scope,
    query: &ConsoleQuery,
    request: &ConsolePagination,
    at: u64,
) -> StoreResult<ConsolePage<ConsoleExplorerNode>> {
    // Large fork/wait membership is legal. Respect the wire byte limit as well
    // as row count, reserving the maximum cursor and fixed response framing.
    let mut bytes = codec::encode(workflow)?.len() + TASK_CURSOR_MAX_BYTES + 512;
    let mut count = 0;
    for item in items.iter().take(request.limit as usize) {
        let size = codec::encode(item)?.len() + 1;
        if bytes + size > CONSOLE_METADATA_MAX_BYTES {
            break;
        }
        bytes += size;
        count += 1;
    }
    if count == 0 && !items.is_empty() {
        return Err(corrupt("explorer record exceeds page bound").into());
    }
    let more = count < items.len();
    items.truncate(count);
    let next_cursor = if more {
        Some(request.next_cursor(
            &query.binding(scope)?,
            &items.last().expect("nonempty bounded page").position(),
        )?)
    } else {
        None
    };
    Ok(ConsolePage {
        items,
        next_cursor,
        observed_at: at,
    })
}

pub(super) fn explorer_query(
    workflow: &str,
    limit: u32,
    position: Option<&ConsolePosition>,
) -> Result<QueryBuilder<Postgres>> {
    let mut sql = QueryBuilder::new(
        "SELECT activation_id,trunc(revision)::text AS revision_text,entrypoint,kind,record_key,metadata_bytes FROM workflow_explorer_records WHERE workflow_id=",
    );
    sql.push_bind(workflow.to_owned());
    if let Some(position) = position {
        let kind = position_text(position, 1)?;
        let key = position_text(position, 2)?;
        let record_key = if matches!(kind.as_str(), "entrypoint" | "child_wait") {
            if key != kind {
                return Err(ContractError::InvalidInput(
                    "invalid singleton explorer cursor".into(),
                ));
            }
            String::new()
        } else {
            key
        };
        sql.push(" AND (revision,kind,record_key)>(CAST(")
            .push_bind(position_number(position, 0)?.to_string())
            .push(" AS numeric),")
            .push_bind(kind)
            .push(",")
            .push_bind(record_key)
            .push(")");
    }
    sql.push(" ORDER BY revision,kind,record_key LIMIT ")
        .push_bind(i64::from(limit) + 1);
    Ok(sql)
}

async fn hydrate(
    connection: &mut PgConnection,
    scope: &Scope,
    workflow: &str,
    items: &mut [ConsoleExplorerNode],
) -> StoreResult<()> {
    let entrypoint_ids: Vec<_> = items
        .iter()
        .filter(|node| matches!(node.data, ConsoleExplorerData::Entrypoint { .. }))
        .map(|node| node.activation_id.clone())
        .collect();
    let task_ids: Vec<_> = items
        .iter()
        .filter_map(|node| match &node.data {
            ConsoleExplorerData::Child { execution, .. }
                if execution.kind == ConsoleExecutionKind::Task =>
            {
                Some(execution.id.clone())
            }
            _ => None,
        })
        .collect();
    let workflow_ids: Vec<_> = items
        .iter()
        .filter_map(|node| match &node.data {
            ConsoleExplorerData::Child { execution, .. }
                if execution.kind == ConsoleExecutionKind::Workflow =>
            {
                Some(execution.id.clone())
            }
            _ => None,
        })
        .collect();
    let wait_keys: Vec<_> = items
        .iter()
        .filter_map(|node| match &node.data {
            ConsoleExplorerData::ExternalWait { key, .. } => Some(key.clone()),
            _ => None,
        })
        .collect();
    let entrypoints: BTreeMap<String, PgRow> = if entrypoint_ids.is_empty() {
        BTreeMap::new()
    } else {
        sqlx::query("SELECT t.task_id,t.state,t.terminal_at_ms,a.applied_at_ms,a.error_bytes FROM tasks t LEFT JOIN workflow_activations a ON a.task_id=t.task_id WHERE t.task_id=ANY($1) AND t.tenant_id=$2 AND t.namespace=$3 AND t.retiring_at_ms IS NULL")
            .bind(entrypoint_ids).bind(&scope.tenant_id).bind(&scope.namespace).fetch_all(&mut *connection).await?.into_iter().map(|row| Ok((row.try_get("task_id")?, row))).collect::<StoreResult<_>>()?
    };
    let children: BTreeMap<(String, String), PgRow> = if task_ids.is_empty()
        && workflow_ids.is_empty()
    {
        BTreeMap::new()
    } else {
        sqlx::query("SELECT 'task' AS kind,task_id AS id,state,terminal_at_ms FROM tasks WHERE task_id=ANY($1) AND tenant_id=$3 AND namespace=$4 AND retiring_at_ms IS NULL UNION ALL SELECT 'workflow' AS kind,workflow_id AS id,state,terminal_at_ms FROM workflow_runs WHERE workflow_id=ANY($2) AND tenant_id=$3 AND namespace=$4 AND retiring_at_ms IS NULL")
            .bind(task_ids).bind(workflow_ids).bind(&scope.tenant_id).bind(&scope.namespace).fetch_all(&mut *connection).await?.into_iter().map(|row| Ok(((row.try_get("kind")?,row.try_get("id")?),row))).collect::<StoreResult<_>>()?
    };
    let waits: BTreeMap<String, PgRow> = if wait_keys.is_empty() {
        BTreeMap::new()
    } else {
        sqlx::query("SELECT wait_key,closed_at_ms FROM workflow_waits WHERE workflow_id=$1 AND wait_key=ANY($2)")
            .bind(workflow).bind(wait_keys).fetch_all(&mut *connection).await?.into_iter().map(|row| Ok((row.try_get("wait_key")?,row))).collect::<StoreResult<_>>()?
    };
    for node in items {
        match &mut node.data {
            ConsoleExplorerData::Entrypoint {
                state,
                availability,
                terminal_at,
                applied_at,
                error,
                decision_kind,
                resumed_activation_id,
                ..
            } => {
                if let Some(row) = entrypoints.get(&node.activation_id) {
                    *state = Some(enumeration(row, "state")?);
                    *availability = ConsoleEvidenceAvailability::Available;
                    *terminal_at = optional_number(row, "terminal_at_ms")?;
                    *applied_at = optional_number(row, "applied_at_ms")?;
                    *error = row
                        .try_get::<Option<Vec<u8>>, _>("error_bytes")?
                        .map(|bytes| codec::decode(&bytes))
                        .transpose()?;
                    if error.is_some() {
                        *decision_kind = None;
                        *resumed_activation_id = None;
                    }
                } else {
                    *state = None;
                    *availability = ConsoleEvidenceAvailability::Unavailable;
                }
            }
            ConsoleExplorerData::Child {
                execution,
                state,
                availability,
                terminal_at,
                ..
            } => {
                if let Some(row) =
                    children.get(&(codec::label(&execution.kind)?, execution.id.clone()))
                {
                    *state = Some(enumeration(row, "state")?);
                    *availability = ConsoleEvidenceAvailability::Available;
                    *terminal_at = optional_number(row, "terminal_at_ms")?;
                } else {
                    *state = None;
                    *availability = ConsoleEvidenceAvailability::Unavailable;
                }
            }
            ConsoleExplorerData::ExternalWait { key, closed_at, .. } => {
                if let Some(row) = waits.get(key) {
                    *closed_at = optional_number(row, "closed_at_ms")?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

async fn ancestry(
    connection: &mut PgConnection,
    scope: &Scope,
    execution: &ConsoleExecutionIdentity,
    at: u64,
) -> StoreResult<ConsoleAncestry> {
    let mut reverse = Vec::new();
    let workflow = if execution.kind == ConsoleExecutionKind::Task {
        let row = sqlx::query("SELECT workflow_id,CASE WHEN octet_length(descriptor_bytes)<=4096 THEN descriptor_bytes ELSE NULL END AS descriptor FROM tasks WHERE task_id=$1 AND tenant_id=$2 AND namespace=$3 AND retiring_at_ms IS NULL")
            .bind(&execution.id).bind(&scope.tenant_id).bind(&scope.namespace).fetch_optional(&mut *connection).await?.ok_or(ContractError::NotFound)?;
        reverse.push(ConsoleAncestor {
            execution: execution.clone(),
            program: Some(descriptor(&row, "descriptor")?.program),
            availability: ConsoleEvidenceAvailability::Available,
        });
        row.try_get::<Option<String>, _>("workflow_id")?
    } else {
        Some(execution.id.clone())
    };
    if let Some(workflow) = workflow {
        // Direct-parent lineage is at most 17 workflow rows. No descendant scan
        // or root lock, and missing ancestors are retained as unavailable IDs.
        let rows = sqlx::query("WITH RECURSIVE lineage AS (SELECT workflow_id,parent_workflow_id,controller_bytes,0 AS depth,ARRAY[workflow_id] AS visited FROM workflow_runs WHERE workflow_id=$1 AND tenant_id=$2 AND namespace=$3 AND retiring_at_ms IS NULL UNION ALL SELECT w.workflow_id,w.parent_workflow_id,w.controller_bytes,l.depth+1,l.visited||w.workflow_id FROM lineage l JOIN workflow_runs w ON w.workflow_id=l.parent_workflow_id WHERE l.depth<16 AND NOT w.workflow_id=ANY(l.visited) AND w.tenant_id=$2 AND w.namespace=$3 AND w.retiring_at_ms IS NULL) SELECT workflow_id,parent_workflow_id,CASE WHEN octet_length(controller_bytes)<=4096 THEN controller_bytes ELSE NULL END AS descriptor,depth FROM lineage ORDER BY depth")
            .bind(&workflow).bind(&scope.tenant_id).bind(&scope.namespace).fetch_all(connection).await?;
        if rows.is_empty() {
            if execution.kind == ConsoleExecutionKind::Workflow {
                return Err(ContractError::NotFound.into());
            }
            reverse.push(ConsoleAncestor {
                execution: ConsoleExecutionIdentity {
                    kind: ConsoleExecutionKind::Workflow,
                    id: workflow,
                },
                program: None,
                availability: ConsoleEvidenceAvailability::Unavailable,
            });
        } else {
            for row in &rows {
                reverse.push(ConsoleAncestor {
                    execution: ConsoleExecutionIdentity {
                        kind: ConsoleExecutionKind::Workflow,
                        id: row.try_get("workflow_id")?,
                    },
                    program: Some(descriptor(row, "descriptor")?.program),
                    availability: ConsoleEvidenceAvailability::Available,
                });
            }
            if let Some(parent) = rows
                .last()
                .expect("nonempty")
                .try_get::<Option<String>, _>("parent_workflow_id")?
            {
                if rows.len() >= 17
                    || reverse.iter().any(|item| {
                        item.execution.kind == ConsoleExecutionKind::Workflow
                            && item.execution.id == parent
                    })
                {
                    return Err(corrupt("workflow ancestry depth or cycle").into());
                }
                reverse.push(ConsoleAncestor {
                    execution: ConsoleExecutionIdentity {
                        kind: ConsoleExecutionKind::Workflow,
                        id: parent,
                    },
                    program: None,
                    availability: ConsoleEvidenceAvailability::Unavailable,
                });
            }
        }
    }
    reverse.reverse();
    Ok(ConsoleAncestry {
        execution: execution.clone(),
        path: reverse,
        observed_at: at,
    })
}
