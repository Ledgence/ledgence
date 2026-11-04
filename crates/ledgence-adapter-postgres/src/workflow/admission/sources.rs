use super::*;

const QUERY: &str = include_str!("../../../queries/admission_open_sources.sql");

impl PostgresStore {
    pub(super) async fn resource_sources_once(
        &self,
        p: &VerifiedPrincipal,
        q: &ResourceSourceQuery,
    ) -> StoreResult<ResourceSourcePage> {
        let before = q.validate()?;
        self.admission_configured()?;
        self.require_scope(&q.scope)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_read().await?;
        self.require_operator(p, AccessCapability::SensitiveRead, db::now(&mut tx).await?)?;
        resource(&mut tx, &q.scope, &q.name, false).await?;
        let states: &[&str] = match q.state {
            Some(ResourceSourceState::PossiblyDispatched) => &["possibly_dispatched"],
            Some(ResourceSourceState::UnknownOccupied) => &["unknown_occupied"],
            None => &["possibly_dispatched", "unknown_occupied"],
        };
        // Positive positions are validated as signed bigint values by the port.
        let through = before.map_or(i64::MAX, |n| n as i64 - 1);
        let rows = sqlx::query(sqlx::AssertSqlSafe(QUERY))
            .bind(&q.scope.tenant_id)
            .bind(&q.scope.namespace)
            .bind(&q.name)
            .bind(states)
            .bind(through)
            .bind(i64::from(q.limit) + 1)
            .fetch_all(&mut *tx)
            .await?;
        let mut page = ResourceSourcePage {
            items: vec![],
            next_cursor: None,
        };
        let mut bytes = RESOURCE_SOURCE_CURSOR_MAX_BYTES + 64;
        for row in rows.iter().take(q.limit as usize) {
            let item = source(q, row)?;
            bytes += serde_json::to_vec(&item)
                .map_err(|_| corrupt("resource source encoding"))?
                .len()
                + 1;
            if bytes > ADMISSION_PAGE_MAX_BYTES {
                break;
            }
            page.items.push(item);
        }
        if rows.len() > page.items.len() {
            let last = page
                .items
                .last()
                .ok_or_else(|| corrupt("resource source exceeds page bound"))?;
            page.next_cursor = Some(q.next_cursor(last.sequence)?);
        }
        page.validate_for(q)?;
        self.require_operator(p, AccessCapability::SensitiveRead, db::now(&mut tx).await?)?;
        tx.commit().await?;
        Ok(page)
    }
}

fn source(q: &ResourceSourceQuery, row: &PgRow) -> StoreResult<ResourceSource> {
    let state = match row.try_get::<String, _>("state")?.as_str() {
        "possibly_dispatched" => ResourceSourceState::PossiblyDispatched,
        "unknown_occupied" => ResourceSourceState::UnknownOccupied,
        _ => return Err(corrupt("unexpected charged resource source state").into()),
    };
    let subject = if let Some(task_id) = row.try_get::<Option<String>, _>("task_id")? {
        ResourceSourceSubject::Task {
            task_id,
            generation: u32::try_from(row.try_get::<i64, _>("generation")?)
                .map_err(|_| corrupt("resource source task generation"))?,
        }
    } else {
        let workflow_id = row.try_get("workflow_id")?;
        let origin_activation_id = row.try_get("origin_activation_id")?;
        let key = row.try_get("logical_key")?;
        match row.try_get::<Option<String>, _>("approval_key")? {
            Some(approval_key) => ResourceSourceSubject::Provider {
                workflow_id,
                origin_activation_id,
                key,
                approval_activation_id: row.try_get("activation_id")?,
                approval_key,
            },
            None => ResourceSourceSubject::Workflow {
                workflow_id,
                origin_activation_id,
                key,
            },
        }
    };
    Ok(ResourceSource {
        scope: q.scope.clone(),
        resource: q.name.clone(),
        sequence: row.try_get::<i64, _>("seq")? as u64,
        revision: row.try_get::<i64, _>("revision")? as u64,
        updated_at: codec::timestamp(row.try_get("updated_at_ms")?)?,
        state,
        subject,
    })
}
