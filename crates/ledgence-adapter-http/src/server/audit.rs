//! Typed payload exposure and the durable boundary before successful response bytes.
use super::*;
use ledgence_orchestration_api::console::ConsoleU64;
use serde::Deserialize;
use std::sync::Mutex;

enum Exposure {
    Missing,
    NoPayload,
    Sensitive(AuditResource),
}

/// One request owns both its principal and its exposure annotation. The inner
/// byte dispatchers cannot turn an unclassified sensitive response into success.
pub(super) struct RequestContext {
    principal: Option<VerifiedPrincipal>,
    request: Option<OperatorRequest>,
    policy: Policy,
    read: Option<AuditReadKind>,
    enforced: bool,
    exposure: Mutex<Exposure>,
    decision_committed: AtomicBool,
}
pub(super) struct PreparedResponse {
    bytes: Vec<u8>,
    context: RequestContext,
}
pub(super) fn enabled(server: &Server) -> bool {
    server
        .console
        .as_ref()
        .is_some_and(|console| console.operator_audit)
}
pub(super) fn service(server: &Server) -> Result<&dyn OperatorAccess> {
    if !enabled(server) {
        return Err(unavailable("verified operator audit is not configured"));
    }
    server
        .workflows
        .as_ref()
        .ok_or_else(|| unavailable("workflow service is unavailable"))?
        .operator_access()
}
impl RequestContext {
    pub(super) fn new(
        server: &Server,
        principal: Option<VerifiedPrincipal>,
        route: &Route,
        method: &str,
        request_id: &str,
    ) -> Result<Self> {
        let policy = route
            .policy(method)
            .ok_or_else(|| unavailable("unclassified request policy"))?;
        let read = route.audit_read(method);
        let enforced = enabled(server);
        if enforced && matches!(policy, Policy::Sensitive | Policy::Approval) && read.is_none() {
            return Err(unavailable(
                "sensitive response has no declared audit operation",
            ));
        }
        let request = if enforced && read.is_some() {
            let mut random = [0u8; 32];
            getrandom::fill(&mut random).map_err(|_| unavailable("audit identity unavailable"))?;
            let identity = random
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            Some(OperatorRequest::new(
                format!("obs_{identity}"),
                Some(request_id.into()),
            )?)
        } else {
            None
        };
        Ok(Self {
            principal,
            request,
            policy,
            read,
            enforced,
            exposure: Mutex::new(Exposure::Missing),
            decision_committed: AtomicBool::new(false),
        })
    }
    pub(super) fn audited(&self) -> bool {
        self.enforced
    }
    pub(super) fn principal(&self) -> Result<&VerifiedPrincipal> {
        self.principal
            .as_ref()
            .ok_or(ContractError::Unauthenticated)
    }
    pub(super) fn request(&self) -> Result<&OperatorRequest> {
        self.request
            .as_ref()
            .ok_or_else(|| unavailable("trusted audit request is missing"))
    }
    pub(super) fn recheck(&self, server: &Server) -> Result<()> {
        if let Some(principal) = &self.principal {
            access::recheck(server, principal, self.policy)?;
        }
        Ok(())
    }
    pub(super) fn decision_committed(&self) {
        self.decision_committed.store(true, Ordering::Release);
    }
    pub(super) fn failure(&self, failure: Failure) -> Failure {
        if self.decision_committed.load(Ordering::Acquire) {
            unavailable("decision may be accepted; response verification failed; reconcile the identical decision after restoring access").into()
        } else {
            failure
        }
    }
    pub(super) fn sensitive(&self, resource: AuditResource) -> Result<()> {
        self.mark(Exposure::Sensitive(resource))
    }
    pub(super) fn pending_result(&self) -> Result<()> {
        if !matches!(
            self.read,
            Some(AuditReadKind::TaskResult | AuditReadKind::WorkflowResult)
        ) {
            return Err(unavailable(
                "no-payload classification requires a pending result",
            ));
        }
        self.mark(Exposure::NoPayload)
    }
    fn mark(&self, exposure: Exposure) -> Result<()> {
        let mut current = self
            .exposure
            .lock()
            .map_err(|_| unavailable("response classification unavailable"))?;
        if !matches!(*current, Exposure::Missing) {
            return Err(unavailable("response classified more than once"));
        }
        *current = exposure;
        Ok(())
    }
    fn take_exposure(&self) -> Result<Exposure> {
        let exposure = std::mem::replace(
            &mut *self
                .exposure
                .lock()
                .map_err(|_| unavailable("response classification unavailable"))?,
            Exposure::Missing,
        );
        match exposure {
            Exposure::Missing => Err(unavailable(
                "sensitive response lacks an audit classification",
            )),
            Exposure::NoPayload
                if !matches!(
                    self.read,
                    Some(AuditReadKind::TaskResult | AuditReadKind::WorkflowResult)
                ) =>
            {
                Err(unavailable("invalid no-payload response classification"))
            }
            _ => Ok(exposure),
        }
    }
    pub(super) fn prepare(self, bytes: Vec<u8>) -> PreparedResponse {
        PreparedResponse {
            bytes,
            context: self,
        }
    }
}
pub(super) async fn release(
    server: &Server,
    prepared: PreparedResponse,
) -> std::result::Result<Vec<u8>, Failure> {
    let PreparedResponse { bytes, context } = prepared;
    let result = async {
        // Worker leases and ordinary commands retain their existing settlement
        // semantics. This boundary governs payload release, not their response.
        if !matches!(context.policy, Policy::Sensitive | Policy::Approval) {
            return Ok(bytes);
        }
        context.recheck(server)?;
        if !context.enforced {
            return Ok(bytes);
        }
        match context.take_exposure()? {
            Exposure::Missing => unreachable!("take_exposure rejects missing annotations"),
            Exposure::NoPayload => Ok(bytes),
            Exposure::Sensitive(resource) => {
                let observation = SensitiveObservation {
                    request: context.request()?.clone(),
                    read: context
                        .read
                        .ok_or_else(|| unavailable("audit operation missing"))?,
                    resource,
                };
                observation.validate()?;
                let principal = context.principal()?;
                let receipt = service(server)?
                    .record_sensitive_response(principal, &observation)
                    .await?;
                receipt.validate_for(principal, &observation)?;
                context.recheck(server)?;
                Ok(bytes)
            }
        }
    }
    .await;
    result.map_err(|failure| context.failure(failure))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(read: AuditReadKind) -> RequestContext {
        RequestContext {
            principal: None,
            request: None,
            policy: Policy::Sensitive,
            read: Some(read),
            enforced: true,
            exposure: Mutex::new(Exposure::Missing),
            decision_committed: AtomicBool::new(false),
        }
    }

    #[test]
    fn missing_repeated_or_unrelated_payload_annotations_fail_closed() {
        let pending = context(AuditReadKind::TaskResult);
        assert!(pending.take_exposure().is_err());
        pending.pending_result().unwrap();
        assert!(pending.pending_result().is_err());
        assert!(matches!(
            pending.take_exposure().unwrap(),
            Exposure::NoPayload
        ));
        assert!(pending.take_exposure().is_err());
        let inspection = context(AuditReadKind::TaskInspect);
        assert!(inspection.pending_result().is_err());
        // Even a future internal caller bypassing the marking helper cannot
        // release inspect bytes using the result-only no-payload annotation.
        inspection.mark(Exposure::NoPayload).unwrap();
        assert!(inspection.take_exposure().is_err());
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsoleQuery {
    filter: ConsoleFilter,
    cursor: Option<String>,
    limit: u32,
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ConsoleFilter {
    All,
    Event {
        event_id: String,
    },
    Resource {
        resource_kind: AuditResourceKind,
        resource_id: String,
    },
    ApprovalAcceptance {
        workflow_id: String,
        key: String,
        activation_id: String,
        revision: ConsoleU64,
        decision_id: String,
    },
}
impl From<ConsoleFilter> for AuditFilter {
    fn from(value: ConsoleFilter) -> Self {
        match value {
            ConsoleFilter::All => Self::All,
            ConsoleFilter::Event { event_id } => Self::Event { event_id },
            ConsoleFilter::Resource {
                resource_kind,
                resource_id,
            } => Self::Resource {
                resource_kind,
                resource_id,
            },
            ConsoleFilter::ApprovalAcceptance {
                workflow_id,
                key,
                activation_id,
                revision,
                decision_id,
            } => Self::ApprovalAcceptance {
                workflow_id,
                key,
                activation_id,
                revision: revision.0,
                decision_id,
            },
        }
    }
}
#[derive(Serialize)]
struct ConsolePage {
    items: Vec<ConsoleEvent>,
    next_cursor: Option<String>,
}
#[derive(Serialize)]
struct ConsoleEvent {
    event_id: String,
    recorded_at: Timestamp,
    audience: AccessAudience,
    actor: AuditActor,
    operation: ConsoleOperation,
}
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ConsoleOperation {
    EffectResolved {
        occurrence: EffectOccurrence,
        resolution_id: String,
        accepted_revision: ConsoleU64,
        basis_revision: ConsoleU64,
        supersedes_resolution_id: Option<String>,
        assessment_kind: EffectAssessmentKind,
    },
    ApprovalAccepted {
        workflow_id: String,
        key: String,
        activation_id: String,
        revision: ConsoleU64,
        decision_id: String,
        decision: ApprovalDecision,
    },
    SensitiveResponse {
        read: AuditReadKind,
        resource: AuditResource,
    },
}
impl From<AuditEvent> for ConsoleEvent {
    fn from(event: AuditEvent) -> Self {
        let operation = match event.operation {
            AuditOperation::EffectResolved {
                occurrence,
                resolution_id,
                accepted_revision,
                basis_revision,
                supersedes_resolution_id,
                assessment_kind,
            } => ConsoleOperation::EffectResolved {
                occurrence,
                resolution_id,
                accepted_revision: ConsoleU64(accepted_revision),
                basis_revision: ConsoleU64(basis_revision),
                supersedes_resolution_id,
                assessment_kind,
            },
            AuditOperation::ApprovalAccepted {
                workflow_id,
                key,
                activation_id,
                revision,
                decision_id,
                decision,
            } => ConsoleOperation::ApprovalAccepted {
                workflow_id,
                key,
                activation_id,
                revision: ConsoleU64(revision),
                decision_id,
                decision,
            },
            AuditOperation::SensitiveResponse { read, resource } => {
                ConsoleOperation::SensitiveResponse { read, resource }
            }
        };
        Self {
            event_id: event.event_id,
            recorded_at: event.recorded_at,
            audience: event.audience,
            actor: event.actor,
            operation,
        }
    }
}
pub(super) async fn query(
    server: &Server,
    bytes: Vec<u8>,
    scope: Option<&Scope>,
    context: &RequestContext,
) -> std::result::Result<Vec<u8>, Failure> {
    let operator = service(server)?;
    let principal = context.principal()?;
    context.recheck(server)?;
    let console = scope.is_some();
    let query: AuditQuery = if let Some(scope) = scope {
        let value: ConsoleQuery = server.decode(bytes, 16 * 1024).await?;
        AuditQuery {
            scope: scope.clone(),
            filter: value.filter.into(),
            cursor: value.cursor,
            limit: value.limit,
        }
    } else {
        server.decode(bytes, 16 * 1024).await?
    };
    query.validate()?;
    server.require_scope(&query.scope)?;
    let reply = operator.query_audit(principal, &query).await?;
    let audience = principal.audience().clone();
    let bytes = server
        .blocking(move || {
            reply.validate_for(&query, &audience)?;
            if console {
                encode_bounded(
                    &ConsolePage {
                        items: reply.items.into_iter().map(Into::into).collect(),
                        next_cursor: reply.next_cursor,
                    },
                    AUDIT_PAGE_MAX_BYTES,
                )
            } else {
                encode_bounded(&reply, AUDIT_PAGE_MAX_BYTES)
            }
        })
        .await?;
    context.recheck(server)?;
    Ok(bytes)
}
