//! Read one committed page of projected metadata, excluding retiring workflows.
use super::*;
use sqlx::{Postgres, QueryBuilder};

pub(super) fn select(
    query: &ApprovalInboxQuery,
    after: Option<ApprovalInboxPosition>,
) -> Result<QueryBuilder<Postgres>> {
    let mut sql = QueryBuilder::new(
        "SELECT a.workflow_id, a.wait_key, a.activation_id, a.program_id, a.program_version, \
         a.action_name, a.action_version, a.created_at_ms, a.deadline_ms, a.status \
         FROM workflow_approvals a WHERE a.tenant_id=",
    );
    sql.push_bind(&query.scope.tenant_id)
        .push(" AND a.namespace=")
        .push_bind(&query.scope.namespace);
    if let Some(status) = query.status {
        sql.push(" AND a.status=").push_bind(codec::label(&status)?);
    }
    if let Some(program) = &query.program_id {
        sql.push(" AND a.program_id=").push_bind(program);
    }
    if let Some(at) = query.due_before {
        sql.push(" AND a.deadline_ms < ").push_bind(codec::ms(at)?);
    }
    if let Some(position) = after {
        sql.push(" AND (a.deadline_ms,a.workflow_id,a.wait_key) > (")
            .push_bind(codec::ms(position.deadline)?)
            .push(",")
            .push_bind(position.workflow_id)
            .push(",")
            .push_bind(position.key)
            .push(")");
    }
    // Keep the scope, filters and seek on the approval index. The existence check
    // hides a retired owner immediately, even before its batched cleanup finishes.
    sql.push(" AND EXISTS (SELECT 1 FROM workflow_runs w WHERE w.workflow_id=a.workflow_id AND w.retiring_at_ms IS NULL) \
              ORDER BY a.deadline_ms, a.workflow_id, a.wait_key LIMIT ").push_bind(i64::from(query.limit)+1);
    Ok(sql)
}

impl PostgresStore {
    pub(super) async fn approval_inbox_once(
        &self,
        query: &ApprovalInboxQuery,
    ) -> StoreResult<ApprovalInboxPage> {
        self.require_scope(&query.scope)?;
        let mut sql = select(query, query.validate()?)?;
        let rows = sql.build().fetch_all(&self.pool).await?;
        let more = rows.len() > query.limit as usize;
        let items = rows
            .into_iter()
            .take(query.limit as usize)
            .map(|row| {
                Ok(ApprovalInboxItem {
                    workflow_id: row.try_get("workflow_id")?,
                    key: row.try_get("wait_key")?,
                    activation_id: row.try_get("activation_id")?,
                    program_id: row.try_get("program_id")?,
                    program_version: row.try_get("program_version")?,
                    action_name: row.try_get("action_name")?,
                    action_version: row.try_get("action_version")?,
                    status: codec::enum_value(row.try_get("status")?)?,
                    created_at: codec::timestamp(row.try_get("created_at_ms")?)?,
                    deadline: codec::timestamp(row.try_get("deadline_ms")?)?,
                })
            })
            .collect::<StoreResult<Vec<_>>>()?;
        let next_cursor = if more {
            Some(query.next_cursor(items.last().ok_or_else(|| corrupt("empty inbox page"))?)?)
        } else {
            None
        };
        let page = ApprovalInboxPage { items, next_cursor };
        page.validate_for(query)
            .map_err(|_| corrupt("approval inbox page"))?;
        Ok(page)
    }
}
