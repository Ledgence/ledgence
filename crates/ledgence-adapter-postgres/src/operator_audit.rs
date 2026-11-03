//! Scoped metadata receipts. Execution/worker hot paths never consult this ledger.
use crate::{persistence as db, *};
use sqlx::{PgConnection, Row, postgres::PgRow};

impl OperatorAccess for PostgresStore {
    fn decide_verified<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ApprovalDecisionCommand,
        request: &'a OperatorRequest,
    ) -> ContractFuture<'a, AuditedDecision> {
        Box::pin(self.run(move || self.decide_approval_audited_once(principal, command, request)))
    }
    fn record_sensitive_response<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        observation: &'a SensitiveObservation,
    ) -> ContractFuture<'a, AuditReceipt> {
        Box::pin(self.run(move || self.observe_response_once(principal, observation)))
    }
    fn query_audit<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        query: &'a AuditQuery,
    ) -> ContractFuture<'a, AuditPage> {
        Box::pin(self.run(move || self.query_audit_once(principal, query)))
    }
}

impl PostgresStore {
    pub(crate) fn audit_audience(&self) -> Result<&AccessAudience> {
        self.instance_access.get().ok_or_else(|| {
            ContractError::Unavailable("operator audit requires initialized instance access".into())
        })
    }
    pub(crate) fn require_operator(
        &self,
        principal: &VerifiedPrincipal,
        capability: AccessCapability,
        now: u64,
    ) -> Result<()> {
        principal.require_at(self.audit_audience()?, now)?;
        principal.require(capability)?;
        if principal.class() == PrincipalClass::Worker {
            return Err(ContractError::Forbidden);
        }
        Ok(())
    }
    async fn observe_response_once(
        &self,
        principal: &VerifiedPrincipal,
        observation: &SensitiveObservation,
    ) -> StoreResult<AuditReceipt> {
        observation.validate()?;
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_write().await?;
        let now = db::now(&mut tx).await?;
        self.require_operator(principal, AccessCapability::SensitiveRead, now)?;
        let event = AuditEvent {
            event_id: observation.request.observation_id().into(),
            recorded_at: now,
            audience: principal.audience().clone(),
            actor: AuditActor::from_principal(principal),
            operation: AuditOperation::SensitiveResponse {
                read: observation.read,
                resource: observation.resource.clone(),
            },
        };
        let inserted = insert_event(
            &mut tx,
            &event,
            None,
            observation.request.request_id(),
            true,
        )
        .await?;
        let receipt = if inserted {
            AuditReceipt {
                event,
                already_recorded: false,
            }
        } else {
            let row = sqlx::query("SELECT * FROM operator_audit_events WHERE event_id=$1")
                .bind(observation.request.observation_id())
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| corrupt("observation was not persisted"))?;
            let event = decode_event(&row)?;
            let receipt = AuditReceipt {
                event,
                already_recorded: true,
            };
            receipt
                .validate_for(principal, observation)
                .map_err(|_| ContractError::Conflict)?;
            if row.try_get::<Option<String>, _>("request_id")?.as_deref()
                != observation.request.request_id()
            {
                return Err(ContractError::Conflict.into());
            }
            receipt
        };
        receipt.validate_for(principal, observation)?;
        // Recheck after an INSERT conflict wait. A receipt must not authorize a
        // response using a principal which expired while acquiring the ledger.
        self.require_operator(
            principal,
            AccessCapability::SensitiveRead,
            db::now(&mut tx).await?,
        )?;
        tx.commit().await?;
        Ok(receipt)
    }
    async fn query_audit_once(
        &self,
        principal: &VerifiedPrincipal,
        query: &AuditQuery,
    ) -> StoreResult<AuditPage> {
        let position = query.validate()?;
        self.require_scope(&query.scope)?;
        if query.scope != principal.audience().scope {
            return Err(ContractError::Forbidden.into());
        }
        let mut connection = self.transaction_connection().await?;
        let mut tx = connection.begin_read().await?;
        self.require_operator(
            principal,
            AccessCapability::MetadataRead,
            db::now(&mut tx).await?,
        )?;
        let sequence = position
            .as_ref()
            .map(|p| (p.sequence - 1) as i64)
            .unwrap_or(i64::MAX);
        let limit = i64::from(query.limit) + 1;
        let rows=match &query.filter {
            AuditFilter::Event{event_id}=>sqlx::query("SELECT * FROM operator_audit_events WHERE tenant_id=$1 AND namespace=$2 AND event_id=$3").bind(&query.scope.tenant_id).bind(&query.scope.namespace).bind(event_id).fetch_all(&mut *tx).await?,
            AuditFilter::All=>sqlx::query("SELECT * FROM operator_audit_events WHERE tenant_id=$1 AND namespace=$2 AND event_seq<=$3 ORDER BY event_seq DESC LIMIT $4")
                .bind(&query.scope.tenant_id).bind(&query.scope.namespace).bind(sequence).bind(limit).fetch_all(&mut *tx).await?,
            AuditFilter::Resource{resource_kind,resource_id}=>sqlx::query("SELECT * FROM operator_audit_events WHERE tenant_id=$1 AND namespace=$2 AND resource_kind=$3 AND resource_id=$4 AND event_seq<=$5 ORDER BY event_seq DESC LIMIT $6")
                .bind(&query.scope.tenant_id).bind(&query.scope.namespace).bind(kind(*resource_kind)).bind(resource_id).bind(sequence).bind(limit).fetch_all(&mut *tx).await?,
            AuditFilter::ApprovalAcceptance{workflow_id,key,..}=>sqlx::query("SELECT * FROM operator_audit_events WHERE tenant_id=$1 AND namespace=$2 AND resource_id=$3 AND operation_key=$4 AND event_kind='approval_accepted' LIMIT 2")
                .bind(&query.scope.tenant_id).bind(&query.scope.namespace).bind(workflow_id).bind(key).fetch_all(&mut *tx).await?,
        };
        let mut items = rows
            .iter()
            .map(decode_event)
            .collect::<StoreResult<Vec<_>>>()?;
        if matches!(query.filter, AuditFilter::ApprovalAcceptance { .. }) {
            // A different occurrence is a legitimate miss, not attribution for
            // the requested decision. The unique occurrence index bounds this.
            items.retain(|e| query.filter.matches(e));
        }
        let more = items.len() > query.limit as usize;
        items.truncate(query.limit as usize);
        let next_cursor = if more {
            let last = &rows[query.limit as usize - 1];
            Some(query.next_cursor(AuditPosition {
                sequence: sequence_of(last)?,
                event_id: items.last().expect("full audit page").event_id.clone(),
            })?)
        } else {
            None
        };
        let page = AuditPage { items, next_cursor };
        page.validate_for(query, principal.audience())?;
        self.require_operator(
            principal,
            AccessCapability::MetadataRead,
            db::now(&mut tx).await?,
        )?;
        tx.commit().await?;
        Ok(page)
    }
}

pub(crate) async fn insert_event(
    connection: &mut PgConnection,
    event: &AuditEvent,
    command: Option<&[u8]>,
    request_id: Option<&str>,
    replay: bool,
) -> StoreResult<bool> {
    event.validate()?;
    let (event_kind, key) = match &event.operation {
        AuditOperation::ApprovalAccepted { key, .. } => ("approval_accepted", Some(key.as_str())),
        AuditOperation::SensitiveResponse { .. } => ("sensitive_response", None),
        AuditOperation::ResourcePolicyAccepted { request_id, .. } => {
            ("resource_policy_accepted", Some(request_id.as_str()))
        }
        AuditOperation::AdmissionReleased { request_id, .. } => {
            ("admission_released", Some(request_id.as_str()))
        }
        AuditOperation::EffectResolved { resolution_id, .. } => {
            ("effect_resolved", Some(resolution_id.as_str()))
        }
    };
    if command.is_some() != (event_kind != "sensitive_response") {
        return Err(corrupt("audit command binding").into());
    }
    let (resource_kind, resource_id) = event.operation.target();
    let inserted:Option<bool>=sqlx::query_scalar("INSERT INTO operator_audit_events(event_id,event_kind,tenant_id,namespace,authority_id,instance_id,resource_kind,resource_id,operation_key,recorded_at_ms,event_bytes,command_digest,request_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,sha256($12::bytea),$13) ON CONFLICT(event_id) DO NOTHING RETURNING event_id=$1 AND event_kind=$2 AND tenant_id=$3 AND namespace=$4 AND authority_id=$5 AND instance_id=$6 AND resource_kind=$7 AND resource_id=$8 AND operation_key IS NOT DISTINCT FROM $9 AND recorded_at_ms=$10 AND event_bytes=$11 AND command_digest IS NOT DISTINCT FROM sha256($12::bytea) AND request_id IS NOT DISTINCT FROM $13")
        .bind(&event.event_id).bind(event_kind).bind(&event.audience.scope.tenant_id).bind(&event.audience.scope.namespace)
        .bind(&event.audience.authority_id).bind(&event.audience.instance_id).bind(kind(resource_kind)).bind(resource_id).bind(key)
        .bind(codec::ms(event.recorded_at)?).bind(codec::encode(event)?).bind(command).bind(request_id)
        .fetch_optional(connection).await?;
    match inserted {
        Some(true) => Ok(true),
        None if replay => Ok(false),
        _ => Err(corrupt("audit event identity was not persisted").into()),
    }
}

pub(crate) async fn load_acceptance(
    connection: &mut PgConnection,
    id: &str,
    bytes: &[u8],
) -> StoreResult<AuditEvent> {
    let row=sqlx::query("SELECT *,command_digest=sha256($2::bytea) AS command_matches FROM operator_audit_events WHERE event_id=$1")
        .bind(id).bind(bytes).fetch_optional(connection).await?.ok_or_else(||corrupt("missing approval acceptance event"))?;
    if row.try_get::<Option<bool>, _>("command_matches")? != Some(true) {
        return Err(corrupt("approval audit command mismatch").into());
    }
    let event = decode_event(&row)?;
    if !matches!(
        event.operation,
        AuditOperation::ApprovalAccepted { .. }
            | AuditOperation::EffectResolved { .. }
            | AuditOperation::ResourcePolicyAccepted { .. }
            | AuditOperation::AdmissionReleased { .. }
    ) {
        return Err(corrupt("approval audit event kind").into());
    }
    Ok(event)
}
fn sequence_of(row: &PgRow) -> StoreResult<u64> {
    let sequence = row.try_get::<i64, _>("event_seq")?;
    if sequence <= 0 {
        return Err(corrupt("audit sequence").into());
    }
    Ok(sequence as u64)
}
fn decode_event(row: &PgRow) -> StoreResult<AuditEvent> {
    let event: AuditEvent = codec::decode(&row.try_get::<Vec<u8>, _>("event_bytes")?)?;
    event.validate().map_err(|_| corrupt("audit event"))?;
    sequence_of(row)?;
    let (event_kind, key) = match &event.operation {
        AuditOperation::ApprovalAccepted { key, .. } => ("approval_accepted", Some(key.as_str())),
        AuditOperation::SensitiveResponse { .. } => ("sensitive_response", None),
        AuditOperation::ResourcePolicyAccepted { request_id, .. } => {
            ("resource_policy_accepted", Some(request_id.as_str()))
        }
        AuditOperation::AdmissionReleased { request_id, .. } => {
            ("admission_released", Some(request_id.as_str()))
        }
        AuditOperation::EffectResolved { resolution_id, .. } => {
            ("effect_resolved", Some(resolution_id.as_str()))
        }
    };
    let (resource_kind, resource_id) = event.operation.target();
    if row.try_get::<String, _>("event_id")? != event.event_id
        || row.try_get::<String, _>("event_kind")? != event_kind
        || row.try_get::<String, _>("tenant_id")? != event.audience.scope.tenant_id
        || row.try_get::<String, _>("namespace")? != event.audience.scope.namespace
        || row.try_get::<String, _>("authority_id")? != event.audience.authority_id
        || row.try_get::<String, _>("instance_id")? != event.audience.instance_id
        || row.try_get::<String, _>("resource_kind")? != kind(resource_kind)
        || row.try_get::<String, _>("resource_id")? != resource_id
        || row
            .try_get::<Option<String>, _>("operation_key")?
            .as_deref()
            != key
        || row.try_get::<i64, _>("recorded_at_ms")? != codec::ms(event.recorded_at)?
    {
        return Err(corrupt("audit projection mismatch").into());
    }
    Ok(event)
}
fn kind(kind: AuditResourceKind) -> &'static str {
    match kind {
        AuditResourceKind::Task => "task",
        AuditResourceKind::Resource => "resource",
        AuditResourceKind::Workflow => "workflow",
    }
}
fn corrupt(message: &str) -> ContractError {
    ContractError::Unavailable(format!("invalid operator audit storage: {message}"))
}
