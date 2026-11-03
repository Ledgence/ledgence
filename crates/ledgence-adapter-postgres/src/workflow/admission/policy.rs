use super::*;
impl PostgresStore {
    pub(super) async fn put_resource_once(
        &self,
        p: &VerifiedPrincipal,
        c: &ResourcePutCommand,
    ) -> StoreResult<ResourcePutReceipt> {
        c.validate()?;
        self.admission_configured()?;
        self.require_scope(&c.scope)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        self.require_operator(
            p,
            AccessCapability::ResourceControl,
            db::now(&mut tx).await?,
        )?;
        // Creation and revision changes serialize only this scoped resource.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended(encode($1::bytea,'hex'),17))")
            .bind(canonical(&(&c.scope, &c.name))?)
            .execute(&mut *tx)
            .await?;
        let row=sqlx::query("SELECT * FROM admission_resources WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 FOR UPDATE").bind(&c.scope.tenant_id).bind(&c.scope.namespace).bind(&c.name).fetch_optional(&mut *tx).await?;
        lock_receipt(&mut tx, &c.scope, &c.request_id).await?;
        self.require_operator(
            p,
            AccessCapability::ResourceControl,
            db::now(&mut tx).await?,
        )?;
        if let Some(mut receipt) =
            receipt::<ResourcePutReceipt, _>(&mut tx, p, &c.scope, &c.request_id, "policy", c)
                .await?
        {
            receipt.already_accepted = true;
            tx.commit().await?;
            return Ok(receipt);
        }
        // The resource clock is sampled after acquiring its serialization lock.
        let now = db::now(&mut tx).await?;
        self.require_operator(p, AccessCapability::ResourceControl, now)?;
        let mut r = if let Some(row) = row {
            let mut r = resource_row(&row)?;
            if c.expected_revision != Some(r.record.revision) {
                return Err(ContractError::Conflict.into());
            }
            let bucket = core::change_admission_rate(
                r.credit
                    .map(|credit| core::AdmissionBucket {
                        credit,
                        high_water_at: r.high_water,
                    })
                    .as_ref(),
                r.record.policy.rate.as_ref(),
                c.policy.rate.as_ref(),
                now,
            )?;
            r.credit = bucket.as_ref().map(|b| b.credit);
            r.high_water = bucket
                .map(|b| b.high_water_at)
                .unwrap_or(now.max(r.high_water))
                .max(r.high_water);
            r.record = ResourceRecord {
                scope: c.scope.clone(),
                name: c.name.clone(),
                revision: r.record.revision + 1,
                policy: c.policy.clone(),
                updated_at: now.max(r.record.updated_at),
            };
            // All offers belong to a policy epoch. Started debt survives reductions.
            r.offered = 0;
            r.wake(now);
            save_resource(&mut tx, &r).await?;
            r
        } else {
            if c.expected_revision.is_some() {
                return Err(ContractError::Conflict.into());
            }
            let r = Resource {
                record: ResourceRecord {
                    scope: c.scope.clone(),
                    name: c.name.clone(),
                    revision: 1,
                    policy: c.policy.clone(),
                    updated_at: now,
                },
                credit: c.policy.rate.as_ref().map(ResourceRate::capacity),
                high_water: now,
                offered: 0,
                started: 0,
                unknown: 0,
                due: None,
                quarantined_due: None,
            };
            let valid:Option<bool>=sqlx::query_scalar("INSERT INTO admission_resources(tenant_id,namespace,resource_name,revision,record_bytes,credit,high_water_at_ms) VALUES($1,$2,$3,1,$4,$5,$6) RETURNING record_bytes=$4 AND credit IS NOT DISTINCT FROM $5 AND high_water_at_ms=$6")
                .bind(&c.scope.tenant_id).bind(&c.scope.namespace).bind(&c.name).bind(codec::encode(&r.record)?).bind(r.credit.map(|v|v as i64)).bind(codec::ms(now)?).fetch_optional(&mut *tx).await?;
            proof(valid, "resource creation suppressed or changed")?;
            r
        };
        r.refill(now)?;
        save_resource(&mut tx, &r).await?;
        let receipt = ResourcePutReceipt {
            resource: r.record,
            already_accepted: false,
        };
        record_receipt(
            &mut tx,
            p,
            &c.scope,
            &c.request_id,
            "policy",
            c,
            &receipt,
            AuditOperation::ResourcePolicyAccepted {
                name: c.name.clone(),
                request_id: c.request_id.clone(),
                revision: receipt.resource.revision,
                policy_digest: c.policy.digest()?,
                reason: c.reason.clone(),
            },
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(receipt)
    }
    pub(super) async fn resource_once(
        &self,
        p: &VerifiedPrincipal,
        q: &ResourceQuery,
    ) -> StoreResult<ResourceSnapshot> {
        q.validate()?;
        let quarantined =
            self.admission_configured()?.dispatch == AdmissionDispatchPolicy::Quarantined;
        self.require_scope(&q.scope)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_read().await?;
        self.require_operator(p, AccessCapability::MetadataRead, db::now(&mut tx).await?)?;
        let r = resource(&mut tx, &q.scope, &q.name, false)
            .await?
            .snapshot(quarantined);
        tx.commit().await?;
        Ok(r)
    }
    pub(super) async fn resources_once(
        &self,
        p: &VerifiedPrincipal,
        q: &ResourceListQuery,
    ) -> StoreResult<ResourcePage> {
        q.validate()?;
        let quarantined =
            self.admission_configured()?.dispatch == AdmissionDispatchPolicy::Quarantined;
        self.require_scope(&q.scope)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_read().await?;
        self.require_operator(p, AccessCapability::MetadataRead, db::now(&mut tx).await?)?;
        let rows=sqlx::query("SELECT * FROM admission_resources WHERE tenant_id=$1 AND namespace=$2 AND ($3::text IS NULL OR resource_name>$3) ORDER BY resource_name LIMIT $4").bind(&q.scope.tenant_id).bind(&q.scope.namespace).bind(&q.after).bind(i64::from(q.limit)+1).fetch_all(&mut *tx).await?;
        let more = rows.len() > q.limit as usize;
        let items = rows
            .iter()
            .take(q.limit as usize)
            .map(|r| resource_row(r).map(|v| v.snapshot(quarantined)))
            .collect::<StoreResult<Vec<_>>>()?;
        let next = if more {
            items.last().map(|v| v.resource.name.clone())
        } else {
            None
        };
        let page = ResourcePage { items, next };
        page.validate_for(q)?;
        tx.commit().await?;
        Ok(page)
    }
    pub(super) async fn inspect_admission_once(
        &self,
        p: &VerifiedPrincipal,
        q: &AdmissionQuery,
    ) -> StoreResult<AdmissionSnapshot> {
        q.validate()?;
        self.admission_configured()?;
        self.require_scope(&q.occurrence.scope)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        self.require_operator(p, AccessCapability::SensitiveRead, db::now(&mut tx).await?)?;
        let name = request_resource(&mut tx, &q.occurrence).await?;
        let mut r = resource(&mut tx, &q.occurrence.scope, &name, true).await?;
        let now = db::now(&mut tx).await?;
        let mut a = request(&mut tx, &q.occurrence).await?;
        if invalidate_offer(&mut a, &mut r, now)? {
            save_request(&mut tx, &a).await?;
            save_resource(&mut tx, &r).await?;
        }
        tx.commit().await?;
        Ok(a.snapshot)
    }
    pub(super) async fn release_admission_once(
        &self,
        p: &VerifiedPrincipal,
        c: &AdmissionReleaseCommand,
    ) -> StoreResult<AdmissionReleaseReceipt> {
        c.validate()?;
        self.admission_configured()?;
        self.require_scope(&c.occurrence.scope)?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        self.require_operator(
            p,
            AccessCapability::ResourceControl,
            db::now(&mut tx).await?,
        )?;
        // Workflow precedes resource so durable notification cannot race work application.
        lock_retained_workflow(&mut tx, &c.occurrence).await?;
        let name = request_resource(&mut tx, &c.occurrence).await?;
        let mut r = resource(&mut tx, &c.occurrence.scope, &name, true).await?;
        lock_receipt(&mut tx, &c.occurrence.scope, &c.request_id).await?;
        self.require_operator(
            p,
            AccessCapability::ResourceControl,
            db::now(&mut tx).await?,
        )?;
        if let Some(mut receipt) = receipt::<AdmissionReleaseReceipt, _>(
            &mut tx,
            p,
            &c.occurrence.scope,
            &c.request_id,
            "release",
            c,
        )
        .await?
        {
            receipt.already_accepted = true;
            tx.commit().await?;
            return Ok(receipt);
        }
        let now = db::now(&mut tx).await?;
        self.require_operator(p, AccessCapability::ResourceControl, now)?;
        let mut a = request(&mut tx, &c.occurrence).await?;
        if a.snapshot.revision != c.expected_revision
            || a.snapshot.source.as_ref().map(|v| &v.source_id) != Some(&c.source_id)
            || !matches!(
                a.snapshot.state,
                AdmissionState::PossiblyDispatched | AdmissionState::UnknownOccupied
            )
        {
            return Err(ContractError::Conflict.into());
        }
        r.started = r
            .started
            .checked_sub(1)
            .ok_or_else(|| corrupt("started occupancy underflow"))?;
        if a.snapshot.state == AdmissionState::UnknownOccupied {
            r.unknown = r
                .unknown
                .checked_sub(1)
                .ok_or_else(|| corrupt("unknown occupancy underflow"))?;
        }
        a.snapshot.state = AdmissionState::AdministrativelyReleased;
        a.check_at = None;
        a.bump(now)?;
        r.wake(now);
        r.refill(now)?;
        save_request(&mut tx, &a).await?;
        save_resource(&mut tx, &r).await?;
        wait::notify(&mut tx, &a, now).await?;
        let receipt = AdmissionReleaseReceipt {
            occurrence: c.occurrence.clone(),
            source_id: c.source_id.clone(),
            revision: a.snapshot.revision,
            accepted_at: now,
            already_accepted: false,
        };
        record_receipt(
            &mut tx,
            p,
            &c.occurrence.scope,
            &c.request_id,
            "release",
            c,
            &receipt,
            AuditOperation::AdmissionReleased {
                occurrence: c.occurrence.clone(),
                source_id: c.source_id.clone(),
                request_id: c.request_id.clone(),
                revision: receipt.revision,
                reason: c.reason.clone(),
            },
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(receipt)
    }
}
async fn lock_receipt(tx: &mut PgConnection, scope: &Scope, id: &str) -> StoreResult<()> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended(encode($1::bytea,'hex'),18))")
        .bind(canonical(&(scope, id))?)
        .execute(&mut *tx)
        .await?;
    Ok(())
}
async fn receipt<T: serde::de::DeserializeOwned, C: serde::Serialize>(
    tx: &mut PgConnection,
    p: &VerifiedPrincipal,
    scope: &Scope,
    id: &str,
    operation: &str,
    c: &C,
) -> StoreResult<Option<T>> {
    let row=sqlx::query("SELECT * FROM admission_operator_receipts WHERE tenant_id=$1 AND namespace=$2 AND request_id=$3").bind(&scope.tenant_id).bind(&scope.namespace).bind(id).fetch_optional(&mut *tx).await?;
    row.map(|row| {
        if row.try_get::<String, _>("operation")? != operation
            || row.try_get::<Vec<u8>, _>("command_bytes")? != canonical(c)?
            || row.try_get::<Vec<u8>, _>("actor_bytes")?
                != canonical(&(p.audience(), AuditActor::from_principal(p)))?
        {
            return Err(ContractError::Conflict.into());
        }
        Ok(codec::decode(&row.try_get::<Vec<u8>, _>("receipt_bytes")?)?)
    })
    .transpose()
}
#[allow(clippy::too_many_arguments)]
async fn record_receipt<C: serde::Serialize, R: serde::Serialize>(
    tx: &mut PgConnection,
    p: &VerifiedPrincipal,
    scope: &Scope,
    id: &str,
    operation: &str,
    c: &C,
    receipt: &R,
    audit: AuditOperation,
    now: u64,
) -> StoreResult<()> {
    let bytes = canonical(c)?;
    let event = AuditEvent {
        event_id: db::id(tx, "audit").await?,
        recorded_at: now,
        audience: p.audience().clone(),
        actor: AuditActor::from_principal(p),
        operation: audit,
    };
    crate::operator_audit::insert_event(tx, &event, Some(&bytes), Some(id), false).await?;
    let valid:Option<bool>=sqlx::query_scalar("INSERT INTO admission_operator_receipts(tenant_id,namespace,request_id,operation,command_bytes,receipt_bytes,actor_bytes,acceptance_event_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING operation=$4 AND command_bytes=$5 AND receipt_bytes=$6 AND actor_bytes=$7 AND acceptance_event_id=$8")
        .bind(&scope.tenant_id).bind(&scope.namespace).bind(id).bind(operation).bind(bytes).bind(codec::encode(receipt)?).bind(canonical(&(p.audience(),AuditActor::from_principal(p)))?).bind(&event.event_id).fetch_optional(&mut *tx).await?;
    proof(valid, "admission operator receipt")
}
