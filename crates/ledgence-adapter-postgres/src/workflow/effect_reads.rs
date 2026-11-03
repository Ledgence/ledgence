//! Bounded indexed projections over the independent ledger, including collected executions.
use super::*;
use sqlx::{Postgres, QueryBuilder};
impl EffectEvidenceReader for PostgresStore {
    fn effect_inbox<'a>(&'a self, q: &'a EffectInboxQuery) -> ContractFuture<'a, EffectInboxPage> {
        Box::pin(self.run(move || self.effect_inbox_once(q)))
    }
    fn inspect_effect<'a>(
        &'a self,
        q: &'a EffectInspectQuery,
    ) -> ContractFuture<'a, EffectEvidenceDetail> {
        Box::pin(self.run(move || self.inspect_effect_once(q)))
    }
}
impl PostgresStore {
    async fn effect_inbox_once(&self, q: &EffectInboxQuery) -> StoreResult<EffectInboxPage> {
        self.require_scope(&q.scope)?;
        let after = q.validate()?;
        let mut sql = QueryBuilder::<Postgres>::new(
            "SELECT summary_bytes FROM approved_effect_occurrences WHERE tenant_id=",
        );
        sql.push_bind(&q.scope.tenant_id)
            .push(" AND namespace=")
            .push_bind(&q.scope.namespace);
        match q.filter {
            EffectInboxFilter::All => {}
            EffectInboxFilter::Unconfirmed => {
                sql.push(" AND journal_state='unconfirmed'");
            }
            EffectInboxFilter::Confirmed => {
                sql.push(" AND journal_state='confirmed'");
            }
            EffectInboxFilter::Discrepant => {
                sql.push(" AND has_discrepancy");
            }
        }
        if let Some(id) = &q.workflow_id {
            sql.push(" AND workflow_id=").push_bind(id);
        }
        if let Some(p) = after {
            sql.push(" AND (updated_at_ms,workflow_id,activation_id,approval_key)<(")
                .push_bind(codec::ms(p.updated_at)?)
                .push(",")
                .push_bind(p.workflow_id)
                .push(",")
                .push_bind(p.activation_id)
                .push(",")
                .push_bind(p.approval_key)
                .push(")");
        }
        sql.push(" ORDER BY updated_at_ms DESC,workflow_id DESC,activation_id DESC,approval_key DESC LIMIT ").push_bind(i64::from(q.limit)+1);
        let rows = sql.build().fetch_all(&self.pool).await?;
        let more = rows.len() > q.limit as usize;
        let items: Vec<EffectEvidenceSummary> = rows
            .iter()
            .take(q.limit as usize)
            .map(|r| {
                codec::decode(
                    &r.try_get::<Vec<u8>, _>("summary_bytes")
                        .map_err(database_error)?,
                )
            })
            .collect::<Result<_>>()?;
        let next_cursor = if more {
            Some(q.next_cursor(items.last().ok_or_else(|| corrupt("empty effect inbox"))?)?)
        } else {
            None
        };
        let page = EffectInboxPage { items, next_cursor };
        page.validate_for(q).map_err(|_| corrupt("effect inbox"))?;
        Ok(page)
    }
    async fn inspect_effect_once(
        &self,
        q: &EffectInspectQuery,
    ) -> StoreResult<EffectEvidenceDetail> {
        let o = &q.occurrence;
        self.require_scope(&o.scope)?;
        let after = q.validate()?.unwrap_or(0);
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        // A bounded occurrence lock yields one consistent summary and timeline.
        // No execution table is read, and no audit/history scan is needed.
        let r = effect_retained::load(&mut tx, o)
            .await?
            .ok_or(ContractError::NotFound)?;
        let rows:Vec<Vec<u8>>=sqlx::query_scalar("SELECT entry_bytes FROM approved_effect_evidence_entries WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND activation_id=$4 AND approval_key=$5 AND sequence>$6::text::numeric ORDER BY sequence LIMIT $7")
            .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.activation_id).bind(&o.approval_key).bind(after.to_string()).bind(i64::from(q.limit)+1).fetch_all(&mut *tx).await?;
        let mut more = rows.len() > q.limit as usize;
        let mut page = EffectEvidenceDetail {
            binding: r.binding,
            summary: r.summary,
            entries: Vec::new(),
            next_cursor: None,
        };
        for bytes in rows.iter().take(q.limit as usize) {
            let entry: EffectEvidenceEntry = codec::decode(bytes)?;
            entry.validate(&o.scope)?;
            page.entries.push(entry);
            // Reserve a complete cursor before measuring, even on the final page.
            page.next_cursor =
                Some(q.next_cursor(page.entries.last().expect("just pushed").sequence())?);
            if codec::encode(&page)?.len() > EFFECT_EVIDENCE_PAGE_MAX_BYTES {
                page.entries.pop();
                more = true;
                if page.entries.is_empty() {
                    return Err(corrupt("effect entry cannot fit bounded detail").into());
                }
                break;
            }
        }
        page.next_cursor = if more {
            Some(
                q.next_cursor(
                    page.entries
                        .last()
                        .ok_or_else(|| corrupt("empty evidence page"))?
                        .sequence(),
                )?,
            )
        } else {
            None
        };
        page.validate_for(q).map_err(|_| corrupt("effect detail"))?;
        tx.commit().await?;
        Ok(page)
    }
}
