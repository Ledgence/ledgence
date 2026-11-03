//! Authenticated worker evidence and bounded operator projections.
use super::*;
use ledgence_orchestration_api::console::ConsoleU64;
use serde::Deserialize;

pub(super) fn available(server: &Server) -> bool {
    access::control(server).is_some()
        && audit::enabled(server)
        && server.workflows.as_ref().is_some_and(|service| {
            service.approved_effect_evidence().is_ok()
                && service.effect_evidence_reader().is_ok()
                && service.operator_access().is_ok()
        })
}
fn invalid_reply() -> ContractError {
    unavailable("invalid effect evidence service response; reconcile the identical command")
}

pub(super) async fn post(
    server: &Server,
    path: &str,
    bytes: Vec<u8>,
    console_scope: Option<&Scope>,
    context: &audit::RequestContext,
) -> std::result::Result<Vec<u8>, Failure> {
    let service = server
        .workflows
        .as_ref()
        .ok_or_else(|| unavailable("workflow service unavailable"))?;
    if path.contains("/v2/") {
        let principal = context.principal()?;
        if !available(server) {
            return Err(unavailable("complete effect evidence v2 is unavailable").into());
        }
        let effects = service.approved_effect_evidence()?;
        match path.rsplit('/').next() {
            Some("claim") => {
                let command: EffectClaimV2Command = server
                    .decode(bytes, EFFECT_EVIDENCE_COMMAND_MAX_BYTES)
                    .await?;
                command.validate()?;
                server.require_scope(&command.owner.scope)?;
                context.recheck(server)?;
                let reply = effects.claim_v2(principal, &command).await?;
                // Once a trusted mutation returns, invalid metadata cannot be
                // represented as a definite denial of the accepted operation.
                reply
                    .validate_for(principal, &command)
                    .map_err(|_| invalid_reply())?;
                return server
                    .blocking(move || encode_bounded(&reply, EFFECT_EVIDENCE_COMMAND_MAX_BYTES))
                    .await;
            }
            Some("confirm") => {
                let command: EffectConfirmV2Command = server
                    .decode(bytes, EFFECT_EVIDENCE_COMMAND_MAX_BYTES)
                    .await?;
                command.validate()?;
                server.require_scope(&command.owner.scope)?;
                context.recheck(server)?;
                let reply = effects.confirm_v2(principal, &command).await?;
                reply
                    .validate_for(principal, &command)
                    .map_err(|_| invalid_reply())?;
                return server
                    .blocking(move || encode_bounded(&reply, 16 * 1024))
                    .await;
            }
            Some("observe") => {
                let command: EffectObservationCommand = server
                    .decode(bytes, EFFECT_EVIDENCE_COMMAND_MAX_BYTES)
                    .await?;
                command.validate()?;
                server.require_scope(&command.occurrence.scope)?;
                context.recheck(server)?;
                let reply = effects.observe(principal, &command).await?;
                reply
                    .validate_for(principal, &command)
                    .map_err(|_| invalid_reply())?;
                return server
                    .blocking(move || encode_bounded(&reply, 16 * 1024))
                    .await;
            }
            _ => return Err(ContractError::NotFound.into()),
        }
    }
    let reader = service.effect_evidence_reader()?;
    let console = console_scope.is_some();
    let maximum = EFFECT_EVIDENCE_CURSOR_MAX_BYTES + 4096;
    match path.rsplit('/').next() {
        Some("inbox") => {
            let query: EffectInboxQuery = if let Some(scope) = console_scope {
                let query: ConsoleInbox = server.decode(bytes, maximum).await?;
                EffectInboxQuery {
                    scope: scope.clone(),
                    filter: query.filter,
                    provider_filter: query.provider_filter,
                    workflow_id: query.workflow_id,
                    cursor: query.cursor,
                    limit: query.limit,
                }
            } else {
                server.decode(bytes, maximum).await?
            };
            query.validate()?;
            server.require_scope(&query.scope)?;
            let reply = reader.effect_inbox(&query).await?;
            server
                .blocking(move || {
                    reply.validate_for(&query).map_err(|_| invalid_reply())?;
                    if console {
                        encode_bounded(
                            &ConsoleInboxPage {
                                items: reply.items.into_iter().map(Into::into).collect(),
                                next_cursor: reply.next_cursor,
                            },
                            EFFECT_EVIDENCE_PAGE_MAX_BYTES,
                        )
                    } else {
                        encode_bounded(&reply, EFFECT_EVIDENCE_PAGE_MAX_BYTES)
                    }
                })
                .await
        }
        Some("inspect") => {
            if !audit::enabled(server) {
                return Err(
                    unavailable("effect inspection requires verified operator audit").into(),
                );
            }
            context.principal()?;
            audit::service(server)?;
            let query: EffectInspectQuery = if let Some(scope) = console_scope {
                let query: ConsoleInspect = server.decode(bytes, maximum).await?;
                EffectInspectQuery {
                    occurrence: EffectOccurrence {
                        scope: scope.clone(),
                        workflow_id: query.workflow_id,
                        activation_id: query.activation_id,
                        approval_key: query.approval_key,
                    },
                    cursor: query.cursor,
                    limit: query.limit,
                }
            } else {
                server.decode(bytes, maximum).await?
            };
            query.validate()?;
            server.require_scope(&query.occurrence.scope)?;
            let resource = AuditResource::Effect {
                workflow_id: query.occurrence.workflow_id.clone(),
                activation_id: query.occurrence.activation_id.clone(),
                approval_key: query.occurrence.approval_key.clone(),
            };
            let reply = reader.inspect_effect(&query).await?;
            let bytes = server
                .blocking(move || {
                    reply.validate_for(&query).map_err(|_| invalid_reply())?;
                    if console {
                        encode_console_detail(reply, &query)
                    } else {
                        encode_bounded(&reply, EFFECT_EVIDENCE_PAGE_MAX_BYTES)
                    }
                })
                .await?;
            context.sensitive(resource)?;
            Ok(bytes)
        }
        _ => Err(ContractError::NotFound.into()),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsoleInbox {
    filter: EffectInboxFilter,
    #[serde(default, deserialize_with = "super::approvals::optional_non_null")]
    provider_filter: Option<ProviderInboxFilter>,
    workflow_id: Option<String>,
    cursor: Option<String>,
    limit: u32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsoleInspect {
    workflow_id: String,
    activation_id: String,
    approval_key: String,
    cursor: Option<String>,
    limit: u32,
}
#[derive(Serialize)]
struct ConsoleInboxPage {
    items: Vec<ConsoleSummary>,
    next_cursor: Option<String>,
}
#[derive(Serialize)]
struct ConsoleSummary {
    occurrence: EffectOccurrence,
    program: ledgence_worker_api::ProgramRef,
    callable: String,
    action_version: String,
    dispatch_provenance: EffectDispatchProvenance,
    journal_state: EffectJournalState,
    has_discrepancy: bool,
    entry_count: ConsoleU64,
    effect_revision: ConsoleU64,
    created_at: Timestamp,
    updated_at: Timestamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    operator_resolution: Option<ConsoleResolutionSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<provider_console::Summary>,
}
impl From<EffectEvidenceSummary> for ConsoleSummary {
    fn from(value: EffectEvidenceSummary) -> Self {
        Self {
            occurrence: value.occurrence,
            program: value.program,
            callable: value.callable,
            action_version: value.action_version,
            dispatch_provenance: value.dispatch_provenance,
            journal_state: value.journal_state,
            has_discrepancy: value.has_discrepancy,
            entry_count: ConsoleU64(value.entry_count),
            effect_revision: ConsoleU64(value.effect_revision),
            created_at: value.created_at,
            updated_at: value.updated_at,
            operator_resolution: value.operator_resolution.map(Into::into),
            provider: value.provider.map(Into::into),
        }
    }
}
#[derive(Serialize)]
struct ConsoleResolutionReference {
    resolution_id: String,
    sequence: ConsoleU64,
    accepted_at: Timestamp,
    accepted_revision: ConsoleU64,
    basis_revision: ConsoleU64,
    supersedes_resolution_id: Option<String>,
    assessment_kind: EffectAssessmentKind,
    acceptance_event_id: String,
}
impl From<EffectResolutionReference> for ConsoleResolutionReference {
    fn from(v: EffectResolutionReference) -> Self {
        Self {
            resolution_id: v.resolution_id,
            sequence: ConsoleU64(v.sequence),
            accepted_at: v.accepted_at,
            accepted_revision: ConsoleU64(v.accepted_revision),
            basis_revision: ConsoleU64(v.basis_revision),
            supersedes_resolution_id: v.supersedes_resolution_id,
            assessment_kind: v.assessment_kind,
            acceptance_event_id: v.acceptance_event_id,
        }
    }
}
#[derive(Serialize)]
struct ConsoleResolutionSummary {
    reference: ConsoleResolutionReference,
    first_conflicting_entry_sequence: Option<ConsoleU64>,
}
impl From<EffectResolutionSummary> for ConsoleResolutionSummary {
    fn from(v: EffectResolutionSummary) -> Self {
        Self {
            reference: v.reference.into(),
            first_conflicting_entry_sequence: v.first_conflicting_entry_sequence.map(ConsoleU64),
        }
    }
}
#[derive(Serialize)]
struct ConsoleBinding {
    occurrence: EffectOccurrence,
    approval: super::approvals::ConsoleApproval,
    program: ledgence_orchestration_api::console::ConsoleProgramDescriptor,
    key: String,
    lookup: Option<String>,
    approval_acceptance_event_id: Option<String>,
}
impl From<EffectEvidenceBinding> for ConsoleBinding {
    fn from(value: EffectEvidenceBinding) -> Self {
        Self {
            occurrence: value.occurrence,
            approval: value.approval.into(),
            program: value.program.into(),
            key: value.key,
            lookup: value.lookup,
            approval_acceptance_event_id: value.approval_acceptance_event_id,
        }
    }
}
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ConsoleEntry {
    Assessment {
        sequence: ConsoleU64,
        recorded_at: Timestamp,
        recorded_revision: ConsoleU64,
        resolution_id: String,
        basis_revision: ConsoleU64,
        supersedes_resolution_id: Option<String>,
        assessment: EffectResolutionAssessment,
        reason: String,
        evidence_references: Vec<String>,
        actor: AuditActor,
        acceptance_event_id: String,
    },
    Claim {
        sequence: ConsoleU64,
        recorded_at: Timestamp,
        recorded_revision: ConsoleU64,
        claim_id: String,
        attempt_id: String,
        worker: EffectWorkerIdentity,
        initial_permission: ApprovedEffectDisposition,
    },
    Observation {
        sequence: ConsoleU64,
        recorded_at: Timestamp,
        recorded_revision: ConsoleU64,
        observation_id: String,
        source: EffectObservationSource,
        output: serde_json::Value,
    },
    #[serde(untagged)]
    Provider(provider_console::Entry),
}
impl From<EffectEvidenceEntry> for ConsoleEntry {
    fn from(value: EffectEvidenceEntry) -> Self {
        match value {
            EffectEvidenceEntry::ProviderDispatch {
                sequence,
                recorded_at,
                recorded_revision,
                claim_id,
                source_id,
                report_id,
                worker,
            } => Self::Provider(provider_console::Entry::Dispatch {
                sequence: ConsoleU64(sequence),
                recorded_at,
                recorded_revision: ConsoleU64(recorded_revision),
                claim_id,
                source_id,
                report_id,
                worker,
            }),
            EffectEvidenceEntry::ProviderProbeScheduled {
                sequence,
                recorded_at,
                recorded_revision,
                probe_id,
                generation,
                task_id,
                due_at,
            } => Self::Provider(provider_console::Entry::ProbeScheduled {
                sequence: ConsoleU64(sequence),
                recorded_at,
                recorded_revision: ConsoleU64(recorded_revision),
                probe_id,
                generation: ConsoleU64(generation),
                task_id,
                due_at,
            }),
            EffectEvidenceEntry::ProviderProbeSource {
                sequence,
                recorded_at,
                recorded_revision,
                probe_id,
                generation,
                task_id,
                attempt_id,
                source_id,
                report_id,
                worker,
            } => Self::Provider(provider_console::Entry::ProbeSource {
                sequence: ConsoleU64(sequence),
                recorded_at,
                recorded_revision: ConsoleU64(recorded_revision),
                probe_id,
                generation: ConsoleU64(generation),
                task_id,
                attempt_id,
                source_id,
                report_id,
                worker,
            }),
            EffectEvidenceEntry::ProviderReport {
                sequence,
                recorded_at,
                recorded_revision,
                source_id,
                source,
                report_id,
                worker,
                ingress,
                report,
            } => Self::Provider(provider_console::Entry::Report {
                sequence: ConsoleU64(sequence),
                recorded_at,
                recorded_revision: ConsoleU64(recorded_revision),
                source_id,
                source: source.into(),
                report_id,
                worker,
                ingress,
                report,
            }),
            EffectEvidenceEntry::ProviderProbeFinished {
                sequence,
                recorded_at,
                recorded_revision,
                probe_id,
                generation,
                task_id,
                completion,
            } => Self::Provider(provider_console::Entry::ProbeFinished {
                sequence: ConsoleU64(sequence),
                recorded_at,
                recorded_revision: ConsoleU64(recorded_revision),
                probe_id,
                generation: ConsoleU64(generation),
                task_id,
                completion: completion.into(),
            }),
            EffectEvidenceEntry::ProviderRecoveryStopped {
                sequence,
                recorded_at,
                recorded_revision,
                reason,
            } => Self::Provider(provider_console::Entry::RecoveryStopped {
                sequence: ConsoleU64(sequence),
                recorded_at,
                recorded_revision: ConsoleU64(recorded_revision),
                reason,
            }),
            EffectEvidenceEntry::Assessment {
                sequence,
                recorded_at,
                recorded_revision,
                resolution_id,
                basis_revision,
                supersedes_resolution_id,
                assessment,
                reason,
                evidence_references,
                actor,
                acceptance_event_id,
            } => Self::Assessment {
                sequence: ConsoleU64(sequence),
                recorded_at,
                recorded_revision: ConsoleU64(recorded_revision),
                resolution_id,
                basis_revision: ConsoleU64(basis_revision),
                supersedes_resolution_id,
                assessment,
                reason,
                evidence_references,
                actor,
                acceptance_event_id,
            },
            EffectEvidenceEntry::Claim {
                sequence,
                recorded_at,
                recorded_revision,
                claim_id,
                attempt_id,
                worker,
                initial_permission,
            } => Self::Claim {
                sequence: ConsoleU64(sequence),
                recorded_at,
                recorded_revision: ConsoleU64(recorded_revision),
                claim_id,
                attempt_id,
                worker,
                initial_permission,
            },
            EffectEvidenceEntry::Observation {
                sequence,
                recorded_at,
                recorded_revision,
                observation_id,
                source,
                output,
            } => Self::Observation {
                sequence: ConsoleU64(sequence),
                recorded_at,
                recorded_revision: ConsoleU64(recorded_revision),
                observation_id,
                source,
                output,
            },
        }
    }
}
#[derive(Serialize)]
struct ConsoleDetail {
    binding: ConsoleBinding,
    summary: ConsoleSummary,
    entries: Vec<ConsoleEntry>,
    next_cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    current_assessment: Option<ConsoleEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<ConsoleProviderDetail>,
}
#[derive(Serialize)]
struct ConsoleProviderDetail {
    current_report: Option<ConsoleEntry>,
}
fn encode_console_detail(
    value: EffectEvidenceDetail,
    query: &EffectInspectQuery,
) -> std::result::Result<Vec<u8>, Failure> {
    let mut detail = ConsoleDetail {
        binding: value.binding.into(),
        summary: value.summary.into(),
        entries: value.entries.into_iter().map(Into::into).collect(),
        next_cursor: value.next_cursor,
        current_assessment: value.current_assessment.map(Into::into),
        provider: value.provider.map(|value| ConsoleProviderDetail {
            current_report: value.current_report.map(|value| (*value).into()),
        }),
    };
    loop {
        let bytes = serde_json::to_vec(&detail).map_err(|_| invalid_reply())?;
        if bytes.len() <= EFFECT_EVIDENCE_PAGE_MAX_BYTES {
            return Ok(bytes);
        }
        detail.entries.pop();
        let last = detail.entries.last().ok_or_else(invalid_reply)?;
        let sequence = match last {
            ConsoleEntry::Claim { sequence, .. }
            | ConsoleEntry::Observation { sequence, .. }
            | ConsoleEntry::Assessment { sequence, .. } => sequence.0,
            ConsoleEntry::Provider(entry) => entry.sequence(),
        };
        detail.next_cursor = Some(query.next_cursor(sequence)?);
    }
}
