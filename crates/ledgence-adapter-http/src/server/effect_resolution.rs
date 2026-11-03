//! Verified native effect assessments. Public receipts contain metadata only.
use super::*;
use ledgence_orchestration_api::console::ConsoleU64;
use serde::Deserialize;

pub(super) fn available(server: &Server) -> bool {
    evidence::available(server)
        && server
            .workflows
            .as_ref()
            .is_some_and(|service| service.effect_resolution().is_ok())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsoleOccurrence {
    workflow_id: String,
    activation_id: String,
    approval_key: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsoleCommand {
    occurrence: ConsoleOccurrence,
    resolution_id: String,
    expected_effect_revision: ConsoleU64,
    #[serde(deserialize_with = "required_option")]
    supersedes_resolution_id: Option<String>,
    action: ApprovalAction,
    assessment: EffectResolutionAssessment,
    reason: String,
    evidence_references: Vec<String>,
}
fn required_option<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::<T>::deserialize(d)
}

#[derive(Serialize)]
struct ConsoleReceipt {
    occurrence: EffectOccurrence,
    resolution_id: String,
    accepted_at: Timestamp,
    accepted_revision: ConsoleU64,
    basis_revision: ConsoleU64,
    supersedes_resolution_id: Option<String>,
    assessment_kind: EffectAssessmentKind,
    acceptance_event_id: String,
    already_accepted: bool,
}
impl From<EffectResolutionReceipt> for ConsoleReceipt {
    fn from(r: EffectResolutionReceipt) -> Self {
        Self {
            occurrence: r.occurrence,
            resolution_id: r.resolution_id,
            accepted_at: r.accepted_at,
            accepted_revision: ConsoleU64(r.accepted_revision),
            basis_revision: ConsoleU64(r.basis_revision),
            supersedes_resolution_id: r.supersedes_resolution_id,
            assessment_kind: r.assessment_kind,
            acceptance_event_id: r.acceptance_event_id,
            already_accepted: r.already_accepted,
        }
    }
}

pub(super) async fn post(
    server: &Server,
    bytes: Vec<u8>,
    scope: Option<&Scope>,
    context: &audit::RequestContext,
) -> std::result::Result<Vec<u8>, Failure> {
    let principal = context.principal()?;
    if !available(server) {
        return Err(unavailable("complete native effect resolution is unavailable").into());
    }
    let command: EffectResolutionCommand = if let Some(scope) = scope {
        let c: ConsoleCommand = server
            .decode(bytes, EFFECT_RESOLUTION_COMMAND_MAX_BYTES)
            .await?;
        EffectResolutionCommand {
            occurrence: EffectOccurrence {
                scope: scope.clone(),
                workflow_id: c.occurrence.workflow_id,
                activation_id: c.occurrence.activation_id,
                approval_key: c.occurrence.approval_key,
            },
            resolution_id: c.resolution_id,
            expected_effect_revision: c.expected_effect_revision.0,
            supersedes_resolution_id: c.supersedes_resolution_id,
            action: c.action,
            assessment: c.assessment,
            reason: c.reason,
            evidence_references: c.evidence_references,
        }
    } else {
        server
            .decode(bytes, EFFECT_RESOLUTION_COMMAND_MAX_BYTES)
            .await?
    };
    command.validate()?;
    server.require_scope(&command.occurrence.scope)?;
    context.recheck(server)?;
    let service = server
        .workflows
        .as_ref()
        .ok_or_else(|| unavailable("workflow service unavailable"))?;
    let accepted = service
        .effect_resolution()?
        .resolve_effect(principal, &command)
        .await?;
    context.decision_committed();
    // After possible commit, malformed proof is uncertain, never a definite
    // rejection that could invite submitting a different resolution command.
    accepted.validate_for(principal, &command).map_err(|_| {
        unavailable("invalid effect resolution proof; reconcile the identical command")
    })?;
    let console = scope.is_some();
    server.blocking(move || {
        if console { encode_bounded(&ConsoleReceipt::from(accepted.receipt), EFFECT_RESOLUTION_RECEIPT_MAX_BYTES) }
        else { encode_bounded(&accepted.receipt, EFFECT_RESOLUTION_RECEIPT_MAX_BYTES) }
    }).await.map_err(|_| unavailable("effect resolution response failed after possible commit; reconcile the identical command").into())
}
