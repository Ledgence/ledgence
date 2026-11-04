use super::*;
pub(super) struct Request {
    pub snapshot: TaskAdmissionSnapshot,
    pub sequence: i64,
    pub offer_generation: u64,
    pub available_at: u64,
    pub last_rewait_at: Option<u64>,
    pub check_at: Option<u64>,
    pub worker: Option<AdmissionWorkerIdentity>,
    pub start_bytes: Option<Vec<u8>>,
    pub close_bytes: Option<Vec<u8>>,
}
impl Request {
    pub fn bump(&mut self, now: u64) -> Result<()> {
        self.snapshot.revision = self
            .snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| corrupt("revision overflow"))?;
        self.snapshot.updated_at = self.snapshot.updated_at.max(now);
        self.snapshot.validate()
    }
}
pub(super) fn from_row(row: &PgRow) -> StoreResult<Request> {
    let snapshot: TaskAdmissionSnapshot =
        codec::decode(&row.try_get::<Vec<u8>, _>("snapshot_bytes")?)?;
    snapshot.validate()?;
    let o = &snapshot.occurrence;
    if row.try_get::<String, _>("tenant_id")? != o.scope.tenant_id
        || row.try_get::<String, _>("namespace")? != o.scope.namespace
        || row.try_get::<String, _>("task_id")? != o.task_id
        || row.try_get::<i64, _>("generation")? != i64::from(o.generation)
        || row.try_get::<String, _>("resource_name")? != snapshot.resource
        || row.try_get::<String, _>("state")? != codec::label(&snapshot.state)?
        || row.try_get::<i64, _>("revision")? != snapshot.revision as i64
        || row.try_get::<Option<String>, _>("owner_attempt_id")?
            != snapshot.owner.as_ref().map(|v| v.attempt_id.clone())
        || row.try_get::<Option<String>, _>("source_id")?
            != snapshot.source.as_ref().map(|v| v.source_id.clone())
        || row.try_get::<Option<i64>, _>("offer_policy_revision")?
            != snapshot.offer.as_ref().map(|v| v.policy_revision as i64)
        || row.try_get::<Option<i64>, _>("offer_expires_at_ms")?
            != snapshot.offer.as_ref().map(|v| v.expires_at as i64)
    {
        return Err(corrupt("scalar projection").into());
    }
    let request = Request {
        snapshot,
        sequence: row.try_get("request_seq")?,
        offer_generation: row.try_get::<i64, _>("offer_generation")? as u64,
        available_at: codec::timestamp(row.try_get("available_at_ms")?)?,
        last_rewait_at: row
            .try_get::<Option<i64>, _>("last_rewait_at_ms")?
            .map(codec::timestamp)
            .transpose()?,
        check_at: row
            .try_get::<Option<i64>, _>("check_at_ms")?
            .map(|v| v as u64),
        worker: row
            .try_get::<Option<Vec<u8>>, _>("worker_bytes")?
            .map(|v| codec::decode(&v))
            .transpose()?,
        start_bytes: row.try_get("start_command_bytes")?,
        close_bytes: row.try_get("close_command_bytes")?,
    };
    if request.snapshot.owner.is_some() != request.worker.is_some() {
        return Err(corrupt("preparation worker").into());
    }
    Ok(request)
}
pub(super) async fn request(
    tx: &mut PgConnection,
    o: &TaskAdmissionOccurrence,
) -> StoreResult<Request> {
    let row=sqlx::query("SELECT * FROM task_admission_requests WHERE tenant_id=$1 AND namespace=$2 AND task_id=$3 AND generation=$4 FOR UPDATE")
        .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.task_id).bind(i64::from(o.generation)).fetch_optional(tx).await?.ok_or(ContractError::NotFound)?;
    from_row(&row)
}
pub(super) async fn resource_name(
    tx: &mut PgConnection,
    o: &TaskAdmissionOccurrence,
) -> StoreResult<String> {
    Ok(sqlx::query_scalar("SELECT resource_name FROM task_admission_requests WHERE tenant_id=$1 AND namespace=$2 AND task_id=$3 AND generation=$4")
        .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.task_id).bind(i64::from(o.generation)).fetch_optional(tx).await?.ok_or(ContractError::NotFound)?)
}
pub(super) async fn save(tx: &mut PgConnection, a: &Request) -> StoreResult<()> {
    a.snapshot.validate()?;
    let s = &a.snapshot;
    let o = &s.occurrence;
    let valid:Option<bool>=sqlx::query_scalar("UPDATE task_admission_requests SET state=$5,revision=$6,updated_at_ms=$7,available_at_ms=$8,offer_generation=$9,offer_policy_revision=$10,offer_expires_at_ms=$11,check_at_ms=$12,owner_attempt_id=$13,worker_bytes=$14,source_id=$15,snapshot_bytes=$16,start_command_bytes=$17,close_command_bytes=$18,last_rewait_at_ms=$19 WHERE tenant_id=$1 AND namespace=$2 AND task_id=$3 AND generation=$4 RETURNING state=$5 AND revision=$6 AND updated_at_ms=$7 AND available_at_ms=$8 AND offer_generation=$9 AND offer_policy_revision IS NOT DISTINCT FROM $10 AND offer_expires_at_ms IS NOT DISTINCT FROM $11 AND check_at_ms IS NOT DISTINCT FROM $12 AND owner_attempt_id IS NOT DISTINCT FROM $13 AND worker_bytes IS NOT DISTINCT FROM $14 AND source_id IS NOT DISTINCT FROM $15 AND snapshot_bytes=$16 AND start_command_bytes IS NOT DISTINCT FROM $17 AND close_command_bytes IS NOT DISTINCT FROM $18 AND last_rewait_at_ms IS NOT DISTINCT FROM $19")
        .bind(&o.scope.tenant_id).bind(&o.scope.namespace).bind(&o.task_id).bind(i64::from(o.generation))
        .bind(codec::label(&s.state)?).bind(s.revision as i64).bind(codec::ms(s.updated_at)?).bind(codec::ms(a.available_at)?).bind(a.offer_generation as i64)
        .bind(s.offer.as_ref().map(|v|v.policy_revision as i64)).bind(s.offer.as_ref().map(|v|v.expires_at as i64)).bind(a.check_at.map(codec::ms).transpose()?)
        .bind(s.owner.as_ref().map(|v|v.attempt_id.as_str())).bind(a.worker.as_ref().map(codec::encode).transpose()?).bind(s.source.as_ref().map(|v|v.source_id.as_str())).bind(codec::encode(s)?).bind(&a.start_bytes).bind(&a.close_bytes).bind(a.last_rewait_at.map(codec::ms).transpose()?).fetch_optional(tx).await?;
    proof(valid, "mutation suppressed or changed")
}
pub(super) fn release_offer(a: &mut Request, r: &mut capacity::Resource, now: u64) -> Result<bool> {
    if !matches!(
        a.snapshot.state,
        TaskAdmissionState::Offered | TaskAdmissionState::Preparing
    ) {
        return Ok(false);
    }
    let offer = a
        .snapshot
        .offer
        .as_ref()
        .ok_or_else(|| corrupt("missing offer"))?;
    if offer.policy_revision == r.record.revision {
        r.offered = r
            .offered
            .checked_sub(1)
            .ok_or_else(|| corrupt("offer counter underflow"))?;
    }
    a.snapshot.state = if a.snapshot.owner.is_some() {
        TaskAdmissionState::ClosedBeforeStart
    } else {
        TaskAdmissionState::Waiting
    };
    // A claimed offer remains immutable evidence for a late Start/Close.
    if a.snapshot.owner.is_none() {
        a.snapshot.offer = None;
    }
    a.check_at = None;
    a.bump(now)?;
    r.refill(now)?;
    r.wake(now);
    Ok(true)
}
pub(super) fn release_started(
    a: &mut Request,
    r: &mut capacity::Resource,
    now: u64,
) -> Result<bool> {
    if !matches!(
        a.snapshot.state,
        TaskAdmissionState::PossiblyDispatched | TaskAdmissionState::UnknownOccupied
    ) {
        return Ok(false);
    }
    r.started = r
        .started
        .checked_sub(1)
        .ok_or_else(|| corrupt("started counter underflow"))?;
    if a.snapshot.state == TaskAdmissionState::UnknownOccupied {
        r.unknown = r
            .unknown
            .checked_sub(1)
            .ok_or_else(|| corrupt("unknown counter underflow"))?;
    }
    // The caller attaches the authenticated Close or operator release before
    // validating the final state. There is no unproved intermediate Finished.
    a.check_at = None;
    r.refill(now)?;
    r.wake(now);
    Ok(true)
}
pub(super) async fn ready(tx: &mut PgConnection, task: &str, value: bool) -> StoreResult<()> {
    let projection:Option<(bool,Option<String>,i64,i64,bool)>=sqlx::query_as("UPDATE tasks SET admission_ready=$2 WHERE task_id=$1 AND admission_resource IS NOT NULL RETURNING admission_ready=$2 AND state='queued' AND cancel_requested_at_ms IS NULL,dispatch_destination,attempt_count+1,available_at_ms,NOT EXISTS(SELECT 1 FROM dispatch_intents i WHERE i.task_id=$1)").bind(task).bind(value).fetch_optional(&mut *tx).await?;
    let (verified, destination, generation, available_at, was_absent) =
        projection.ok_or_else(|| corrupt("missing task delivery projection"))?;
    proof(Some(verified), "task delivery eligibility")?;
    let Some(destination) = destination else {
        return Ok(());
    };
    // A new offer needs a *new* publication, including when the task generation
    // is unchanged. An old same-generation row cannot satisfy this condition.
    if value && !was_absent {
        return Err(corrupt("new offer has a stale publication").into());
    }
    crate::dispatch_intents::sync_intent(tx, task).await?;
    let exact: bool = if value {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatch_intents WHERE task_id=$1 AND generation=$2 AND destination=$3 AND available_at_ms=$4 AND next_publish_at_ms=$4 AND publication_epoch=1 AND lease_token IS NULL AND lease_until_ms IS NULL AND last_confirmed_at_ms IS NULL)").bind(task).bind(generation).bind(destination).bind(available_at).fetch_one(tx).await?
    } else {
        sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM dispatch_intents WHERE task_id=$1)")
            .bind(task)
            .fetch_one(tx)
            .await?
    };
    proof(Some(exact), "admission publication handoff")
}
