use super::*;
pub(in crate::workflow) struct Resource {
    pub record: ResourceRecord,
    pub credit: Option<u64>,
    pub high_water: u64,
    pub offered: u64,
    pub started: u64,
    pub unknown: u64,
    pub due: Option<u64>,
    pub quarantined_due: Option<u64>,
}
impl Resource {
    pub fn wake(&mut self, now: u64) {
        self.due = Some(now);
        self.quarantined_due = Some(now);
    }
    pub fn snapshot(&self, quarantined: bool) -> ResourceSnapshot {
        ResourceSnapshot {
            resource: self.record.clone(),
            offered: self.offered,
            occupied: self.offered + self.started,
            unknown: self.unknown,
            quarantined,
        }
    }
    pub fn refill(&mut self, now: u64) -> Result<()> {
        if let Some(rate) = &self.record.policy.rate {
            let b = core::refill_admission_bucket(
                &core::AdmissionBucket {
                    credit: self
                        .credit
                        .ok_or_else(|| corrupt("missing bucket credit"))?,
                    high_water_at: self.high_water,
                },
                rate,
                now,
            )?;
            self.credit = Some(b.credit);
            self.high_water = b.high_water_at;
        } else {
            self.high_water = self.high_water.max(now)
        }
        Ok(())
    }
    pub fn capacity(&self) -> Result<u32> {
        core::admission_offer_capacity(
            &self.record.policy,
            self.credit
                .map(|credit| core::AdmissionBucket {
                    credit,
                    high_water_at: self.high_water,
                })
                .as_ref(),
            self.offered,
            self.offered + self.started,
        )
    }
}
pub(in crate::workflow) fn resource_row(row: &PgRow) -> StoreResult<Resource> {
    let record: ResourceRecord = codec::decode(&row.try_get::<Vec<u8>, _>("record_bytes")?)?;
    record.validate()?;
    if row.try_get::<String, _>("tenant_id")? != record.scope.tenant_id
        || row.try_get::<String, _>("namespace")? != record.scope.namespace
        || row.try_get::<String, _>("resource_name")? != record.name
        || row.try_get::<i64, _>("revision")? != record.revision as i64
    {
        return Err(corrupt("resource projection").into());
    }
    let r = Resource {
        record,
        credit: row.try_get::<Option<i64>, _>("credit")?.map(|v| v as u64),
        high_water: row.try_get::<i64, _>("high_water_at_ms")? as u64,
        offered: row.try_get::<i64, _>("offered")? as u64,
        started: row.try_get::<i64, _>("started")? as u64,
        unknown: row.try_get::<i64, _>("unknown")? as u64,
        quarantined_due: row
            .try_get::<Option<i64>, _>("quarantined_due_ms")?
            .map(|v| v as u64),
        due: row
            .try_get::<Option<i64>, _>("next_due_ms")?
            .map(|v| v as u64),
    };
    if r.credit.is_some() != r.record.policy.rate.is_some() {
        return Err(corrupt("resource bucket mode").into());
    }
    r.snapshot(false).validate()?;
    Ok(r)
}
pub(in crate::workflow) async fn resource(
    tx: &mut PgConnection,
    scope: &Scope,
    name: &str,
    lock: bool,
) -> StoreResult<Resource> {
    let sql = if lock {
        "SELECT * FROM admission_resources WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 FOR UPDATE"
    } else {
        "SELECT * FROM admission_resources WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3"
    };
    let row = sqlx::query(sql)
        .bind(&scope.tenant_id)
        .bind(&scope.namespace)
        .bind(name)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(ContractError::NotFound)?;
    resource_row(&row)
}
pub(in crate::workflow) async fn save_resource(
    tx: &mut PgConnection,
    r: &Resource,
) -> StoreResult<()> {
    r.snapshot(false).validate()?;
    let valid:Option<bool>=sqlx::query_scalar("UPDATE admission_resources SET revision=$4,record_bytes=$5,credit=$6,high_water_at_ms=$7,offered=$8,started=$9,unknown=$10,next_due_ms=$11,quarantined_due_ms=$12 WHERE tenant_id=$1 AND namespace=$2 AND resource_name=$3 RETURNING revision=$4 AND record_bytes=$5 AND credit IS NOT DISTINCT FROM $6 AND high_water_at_ms=$7 AND offered=$8 AND started=$9 AND unknown=$10 AND next_due_ms IS NOT DISTINCT FROM $11 AND quarantined_due_ms IS NOT DISTINCT FROM $12")
        .bind(&r.record.scope.tenant_id).bind(&r.record.scope.namespace).bind(&r.record.name).bind(r.record.revision as i64).bind(codec::encode(&r.record)?).bind(r.credit.map(|v|v as i64)).bind(codec::ms(r.high_water)?).bind(r.offered as i64).bind(r.started as i64).bind(r.unknown as i64).bind(r.due.map(codec::ms).transpose()?).bind(r.quarantined_due.map(codec::ms).transpose()?).fetch_optional(&mut *tx).await?;
    proof(valid, "resource mutation suppressed or changed")
}
pub(in crate::workflow) struct Request {
    pub snapshot: AdmissionSnapshot,
    pub parking: Option<String>,
    pub generation: u64,
    pub check_at: Option<u64>,
    pub start_bytes: Option<Vec<u8>>,
    pub finish_bytes: Option<Vec<u8>>,
}
impl Request {
    pub fn bump(&mut self, now: u64) -> Result<()> {
        self.snapshot.revision = self
            .snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| corrupt("admission revision overflow"))?;
        self.snapshot.updated_at = self.snapshot.updated_at.max(now);
        self.snapshot.validate()
    }
}
pub(in crate::workflow) fn request_row(row: &PgRow) -> StoreResult<Request> {
    let s: AdmissionSnapshot = codec::decode(&row.try_get::<Vec<u8>, _>("snapshot_bytes")?)?;
    s.validate()?;
    let o = &s.occurrence;
    if row.try_get::<String, _>("tenant_id")? != o.scope.tenant_id
        || row.try_get::<String, _>("namespace")? != o.scope.namespace
        || row.try_get::<String, _>("workflow_id")? != o.workflow_id
        || row.try_get::<String, _>("origin_activation_id")? != o.origin_activation_id
        || row.try_get::<String, _>("logical_key")? != o.key
        || row.try_get::<String, _>("resource_name")? != s.binding.resource()
        || row.try_get::<String, _>("state")? != codec::label(&s.state)?
        || row.try_get::<i64, _>("revision")? != s.revision as i64
        || row.try_get::<i64, _>("created_at_ms")? != codec::ms(s.created_at)?
        || row.try_get::<i64, _>("updated_at_ms")? != codec::ms(s.updated_at)?
        || row.try_get::<i64, _>("deadline_at_ms")? != codec::ms(s.deadline_at)?
        || row.try_get::<Option<i64>, _>("offer_policy_revision")?
            != s.offer.as_ref().map(|v| v.policy_revision as i64)
        || row.try_get::<Option<i64>, _>("offer_expires_at_ms")?
            != s.offer.as_ref().map(|v| v.expires_at as i64)
    {
        return Err(corrupt("admission scalar projection").into());
    }
    Ok(Request {
        snapshot: s,
        parking: row.try_get("parking_activation_id")?,
        generation: row.try_get::<i64, _>("offer_generation")? as u64,
        check_at: row
            .try_get::<Option<i64>, _>("check_at_ms")?
            .map(|v| v as u64),
        start_bytes: row.try_get("start_command_bytes")?,
        finish_bytes: row.try_get("finish_command_bytes")?,
    })
}
pub(in crate::workflow) async fn request(
    tx: &mut PgConnection,
    o: &AdmissionOccurrence,
) -> StoreResult<Request> {
    let row=sqlx::query("SELECT * FROM admission_requests WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND origin_activation_id=$4 AND logical_key=$5 FOR UPDATE").bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.origin_activation_id).bind(&o.key).fetch_optional(&mut *tx).await?.ok_or(ContractError::NotFound)?;
    request_row(&row)
}
pub(in crate::workflow) async fn request_resource(
    tx: &mut PgConnection,
    o: &AdmissionOccurrence,
) -> StoreResult<String> {
    Ok(sqlx::query_scalar("SELECT resource_name FROM admission_requests WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND origin_activation_id=$4 AND logical_key=$5").bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.origin_activation_id).bind(&o.key).fetch_optional(&mut *tx).await?.ok_or(ContractError::NotFound)?)
}
pub(in crate::workflow) async fn save_request(
    tx: &mut PgConnection,
    r: &Request,
) -> StoreResult<()> {
    let s = &r.snapshot;
    s.validate()?;
    let o = &s.occurrence;
    let valid:Option<bool>=sqlx::query_scalar("UPDATE admission_requests SET state=$6,revision=$7,updated_at_ms=$8,parking_activation_id=$9,offer_generation=$10,offer_policy_revision=$11,offer_expires_at_ms=$12,check_at_ms=$13,snapshot_bytes=$14,start_command_bytes=$15,finish_command_bytes=$16 WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 AND origin_activation_id=$4 AND logical_key=$5 RETURNING state=$6 AND revision=$7 AND updated_at_ms=$8 AND parking_activation_id IS NOT DISTINCT FROM $9 AND offer_generation=$10 AND offer_policy_revision IS NOT DISTINCT FROM $11 AND offer_expires_at_ms IS NOT DISTINCT FROM $12 AND check_at_ms IS NOT DISTINCT FROM $13 AND snapshot_bytes=$14 AND start_command_bytes IS NOT DISTINCT FROM $15 AND finish_command_bytes IS NOT DISTINCT FROM $16")
        .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).bind(&o.origin_activation_id).bind(&o.key).bind(codec::label(&s.state)?).bind(s.revision as i64).bind(codec::ms(s.updated_at)?).bind(&r.parking).bind(r.generation as i64).bind(s.offer.as_ref().map(|v|v.policy_revision as i64)).bind(s.offer.as_ref().map(|v|v.expires_at as i64)).bind(r.check_at.map(codec::ms).transpose()?).bind(codec::encode(s)?).bind(&r.start_bytes).bind(&r.finish_bytes).fetch_optional(&mut *tx).await?;
    proof(valid, "admission mutation suppressed or changed")
}
/// Epoch invalidation cancels old shadow reservations without touching started debt.
pub(in crate::workflow) fn invalidate_offer(
    r: &mut Request,
    p: &mut Resource,
    now: u64,
) -> Result<bool> {
    if let Some(offer) = &r.snapshot.offer
        && (offer.policy_revision != p.record.revision
            || offer.expires_at <= now
            || !p.record.policy.enabled)
    {
        if offer.policy_revision == p.record.revision {
            p.offered = p
                .offered
                .checked_sub(1)
                .ok_or_else(|| corrupt("offer counter underflow"))?;
        }
        r.snapshot.offer = None;
        r.snapshot.state = AdmissionState::AwaitingRewait;
        r.bump(now)?;
        p.refill(now)?;
        p.wake(now);
        return Ok(true);
    }
    Ok(false)
}

/// If execution history still exists, serialize notification with cancellation
/// and work application. Missing history does not erase retained source authority.
pub(in crate::workflow) async fn lock_retained_workflow(
    tx: &mut PgConnection,
    o: &AdmissionOccurrence,
) -> StoreResult<()> {
    sqlx::query("SELECT workflow_id FROM workflow_runs WHERE tenant_id=$1 AND namespace=$2 AND workflow_id=$3 FOR NO KEY UPDATE")
        .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.workflow_id).fetch_optional(&mut *tx).await?;
    Ok(())
}
