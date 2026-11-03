//! Bounded, scoped projections; execution credentials and frozen commands are never projected.
use crate::effect_evidence::{bounded, invalid, positive, time};
use crate::*;
use ledgence_worker_api::{ProgramDescriptor, ProgramRef};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectJournalState {
    Unconfirmed,
    Confirmed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectObservationIngress {
    LiveConfirm,
    LateObserve,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectDispatchProvenance {
    VerifiedClaim { claim_id: String },
    LegacyAttempt { attempt_id: String },
    LegacyFence,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectObservationSource {
    VerifiedWorkerClaim {
        claim_id: String,
        worker: EffectWorkerIdentity,
        reported_callback: EffectReportedCallback,
        ingress: EffectObservationIngress,
    },
    LegacyJournal {
        attempt_id: String,
        journal_accepted_at: Timestamp,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectEvidenceEntry {
    ProviderDispatch {
        sequence: u64,
        recorded_at: Timestamp,
        recorded_revision: u64,
        claim_id: String,
        source_id: String,
        report_id: String,
        worker: EffectWorkerIdentity,
    },
    ProviderProbeScheduled {
        sequence: u64,
        recorded_at: Timestamp,
        recorded_revision: u64,
        probe_id: String,
        generation: u64,
        task_id: String,
        due_at: Timestamp,
    },
    ProviderProbeSource {
        sequence: u64,
        recorded_at: Timestamp,
        recorded_revision: u64,
        probe_id: String,
        generation: u64,
        task_id: String,
        attempt_id: String,
        source_id: String,
        report_id: String,
        worker: EffectWorkerIdentity,
    },
    ProviderReport {
        sequence: u64,
        recorded_at: Timestamp,
        recorded_revision: u64,
        source_id: String,
        source: ProviderSourceReference,
        report_id: String,
        worker: EffectWorkerIdentity,
        ingress: ProviderReportIngress,
        report: Box<ProviderReport>,
    },
    ProviderProbeFinished {
        sequence: u64,
        recorded_at: Timestamp,
        recorded_revision: u64,
        probe_id: String,
        generation: u64,
        task_id: String,
        completion: ProviderProbeCompletion,
    },
    ProviderRecoveryStopped {
        sequence: u64,
        recorded_at: Timestamp,
        recorded_revision: u64,
        reason: ProviderRecoveryStopReason,
    },

    Assessment {
        sequence: u64,
        recorded_at: Timestamp,
        recorded_revision: u64,
        resolution_id: String,
        basis_revision: u64,
        #[serde(deserialize_with = "crate::observation::required_option")]
        supersedes_resolution_id: Option<String>,
        assessment: EffectResolutionAssessment,
        reason: String,
        evidence_references: Vec<String>,
        actor: AuditActor,
        acceptance_event_id: String,
    },
    Claim {
        sequence: u64,
        recorded_at: Timestamp,
        recorded_revision: u64,
        claim_id: String,
        attempt_id: String,
        worker: EffectWorkerIdentity,
        initial_permission: ApprovedEffectDisposition,
    },
    Observation {
        sequence: u64,
        recorded_at: Timestamp,
        recorded_revision: u64,
        observation_id: String,
        source: EffectObservationSource,
        #[serde(deserialize_with = "crate::observation::required_value")]
        output: Value,
    },
}
impl EffectEvidenceEntry {
    pub fn sequence(&self) -> u64 {
        match self {
            Self::ProviderDispatch { sequence, .. }
            | Self::ProviderProbeScheduled { sequence, .. }
            | Self::ProviderProbeSource { sequence, .. }
            | Self::ProviderReport { sequence, .. }
            | Self::ProviderProbeFinished { sequence, .. }
            | Self::ProviderRecoveryStopped { sequence, .. } => *sequence,
            Self::Claim { sequence, .. }
            | Self::Observation { sequence, .. }
            | Self::Assessment { sequence, .. } => *sequence,
        }
    }
    pub fn recorded_revision(&self) -> u64 {
        match self {
            Self::ProviderDispatch {
                recorded_revision, ..
            }
            | Self::ProviderProbeScheduled {
                recorded_revision, ..
            }
            | Self::ProviderProbeSource {
                recorded_revision, ..
            }
            | Self::ProviderReport {
                recorded_revision, ..
            }
            | Self::ProviderProbeFinished {
                recorded_revision, ..
            }
            | Self::ProviderRecoveryStopped {
                recorded_revision, ..
            } => *recorded_revision,
            Self::Assessment {
                recorded_revision, ..
            }
            | Self::Claim {
                recorded_revision, ..
            }
            | Self::Observation {
                recorded_revision, ..
            } => *recorded_revision,
        }
    }
    pub fn recorded_at(&self) -> u64 {
        match self {
            Self::ProviderDispatch { recorded_at, .. }
            | Self::ProviderProbeScheduled { recorded_at, .. }
            | Self::ProviderProbeSource { recorded_at, .. }
            | Self::ProviderReport { recorded_at, .. }
            | Self::ProviderProbeFinished { recorded_at, .. }
            | Self::ProviderRecoveryStopped { recorded_at, .. } => *recorded_at,
            Self::Claim { recorded_at, .. }
            | Self::Observation { recorded_at, .. }
            | Self::Assessment { recorded_at, .. } => *recorded_at,
        }
    }
    pub fn command_id(&self) -> &str {
        match self {
            Self::ProviderDispatch { claim_id, .. } => claim_id,
            Self::ProviderProbeScheduled { probe_id, .. }
            | Self::ProviderProbeFinished { probe_id, .. } => probe_id,
            Self::ProviderProbeSource { source_id, .. } => source_id,
            Self::ProviderReport { report_id, .. } => report_id,
            Self::ProviderRecoveryStopped { reason, .. } => reason.as_str(),
            Self::Claim { claim_id, .. } => claim_id,
            Self::Assessment { resolution_id, .. } => resolution_id,
            Self::Observation { observation_id, .. } => observation_id,
        }
    }
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ProviderDispatch { .. } => "provider_dispatch",
            Self::ProviderProbeScheduled { .. } => "provider_probe_scheduled",
            Self::ProviderProbeSource { .. } => "provider_probe_source",
            Self::ProviderReport { .. } => "provider_report",
            Self::ProviderProbeFinished { .. } => "provider_probe_finished",
            Self::ProviderRecoveryStopped { .. } => "provider_recovery_stopped",
            Self::Claim { .. } => "claim",
            Self::Assessment { .. } => "assessment",
            Self::Observation { .. } => "observation",
        }
    }
    pub fn validate(&self, scope: &Scope) -> Result<()> {
        positive(self.sequence())?;
        positive(self.recorded_revision())?;
        // The retained occurrence starts at revision one, before its first entry.
        // Metadata-only changes can advance the revision without adding an entry.
        if self.sequence() >= self.recorded_revision() {
            return Err(invalid());
        }
        time(self.recorded_at())?;
        validate_text(self.command_id(), 128)?;
        let validate_worker = |worker: &EffectWorkerIdentity| -> Result<()> {
            worker.validate()?;
            if &worker.audience.scope != scope {
                return Err(invalid());
            }
            Ok(())
        };
        match self {
            Self::ProviderDispatch {
                claim_id,
                source_id,
                report_id,
                worker,
                ..
            } => {
                for id in [claim_id, source_id, report_id] {
                    validate_text(id, 128)?;
                }
                validate_worker(worker)?;
            }
            Self::ProviderProbeScheduled {
                probe_id,
                generation,
                task_id,
                due_at,
                ..
            } => {
                validate_text(probe_id, 128)?;
                validate_text(task_id, 128)?;
                positive(*generation)?;
                time(*due_at)?;
            }
            Self::ProviderProbeSource {
                probe_id,
                generation,
                task_id,
                attempt_id,
                source_id,
                report_id,
                worker,
                ..
            } => {
                for id in [probe_id, task_id, attempt_id, source_id, report_id] {
                    validate_text(id, 128)?;
                }
                positive(*generation)?;
                validate_worker(worker)?;
            }
            Self::ProviderReport {
                source_id,
                source,
                report_id,
                worker,
                ingress,
                report,
                ..
            } => {
                validate_text(source_id, 128)?;
                validate_text(report_id, 128)?;
                source.validate()?;
                validate_worker(worker)?;
                report.validate()?;
                if matches!(
                    (source, ingress),
                    (
                        ProviderSourceReference::Dispatch { .. },
                        ProviderReportIngress::LiveProbe
                    ) | (
                        ProviderSourceReference::Probe { .. },
                        ProviderReportIngress::LiveDispatch
                    )
                ) {
                    return Err(invalid());
                }
            }
            Self::ProviderProbeFinished {
                probe_id,
                generation,
                task_id,
                completion,
                ..
            } => {
                validate_text(probe_id, 128)?;
                validate_text(task_id, 128)?;
                positive(*generation)?;
                if let ProviderProbeCompletion::Reported { report } = completion {
                    report.validate()?;
                    if report.sequence >= self.sequence()
                        || report.recorded_revision >= self.recorded_revision()
                    {
                        return Err(invalid());
                    }
                }
            }
            Self::ProviderRecoveryStopped { .. } => {}
            Self::Assessment {
                assessment,
                reason,
                evidence_references,
                actor,
                ..
            } => {
                crate::effect_resolution::assessment_fields(
                    assessment,
                    reason,
                    evidence_references,
                )?;
                actor.validate()?;
                self.resolution_reference()
                    .ok_or_else(invalid)?
                    .validate()?;
            }
            Self::Claim {
                attempt_id, worker, ..
            } => {
                validate_text(attempt_id, 128)?;
                validate_worker(worker)?;
            }
            Self::Observation { source, output, .. } => {
                ledgence_worker_api::validate_wire_value(output)?;
                bounded(output, WORKFLOW_LOCAL_RECORD_MAX_BYTES)?;
                match source {
                    EffectObservationSource::VerifiedWorkerClaim {
                        claim_id, worker, ..
                    } => {
                        validate_text(claim_id, 128)?;
                        validate_worker(worker)?;
                    }
                    EffectObservationSource::LegacyJournal {
                        attempt_id,
                        journal_accepted_at,
                    } => {
                        validate_text(attempt_id, 128)?;
                        time(*journal_accepted_at)?;
                    }
                }
            }
        }
        bounded(
            self,
            if self.kind().starts_with("provider_") {
                PROVIDER_ENTRY_MAX_BYTES
            } else if matches!(self, Self::Assessment { .. }) {
                EFFECT_RESOLUTION_ENTRY_MAX_BYTES
            } else {
                EFFECT_EVIDENCE_COMMAND_MAX_BYTES
            },
        )
    }
    pub fn provider_report_reference(&self) -> Option<ProviderReportReference> {
        if let Self::ProviderReport {
            sequence,
            recorded_at,
            recorded_revision,
            source_id,
            report_id,
            report,
            ..
        } = self
        {
            Some(ProviderReportReference {
                report_id: report_id.clone(),
                source_id: source_id.clone(),
                sequence: *sequence,
                recorded_revision: *recorded_revision,
                recorded_at: *recorded_at,
                knowledge: report.knowledge.kind(),
            })
        } else {
            None
        }
    }
    pub fn resolution_reference(&self) -> Option<EffectResolutionReference> {
        if let Self::Assessment {
            sequence,
            recorded_at,
            recorded_revision,
            resolution_id,
            basis_revision,
            supersedes_resolution_id,
            assessment,
            acceptance_event_id,
            ..
        } = self
        {
            Some(EffectResolutionReference {
                resolution_id: resolution_id.clone(),
                sequence: *sequence,
                accepted_at: *recorded_at,
                accepted_revision: *recorded_revision,
                basis_revision: *basis_revision,
                supersedes_resolution_id: supersedes_resolution_id.clone(),
                assessment_kind: assessment.kind(),
                acceptance_event_id: acceptance_event_id.clone(),
            })
        } else {
            None
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectEvidenceBinding {
    pub occurrence: EffectOccurrence,
    pub approval: ApprovalSnapshot,
    pub program: ProgramDescriptor,
    pub key: String,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub lookup: Option<String>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub approval_acceptance_event_id: Option<String>,
}
impl EffectEvidenceBinding {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()?;
        self.approval.validate()?;
        self.program.validate()?;
        if self.approval.scope != self.occurrence.scope
            || self.approval.workflow_id != self.occurrence.workflow_id
            || self.approval.key != self.occurrence.approval_key
            || self.approval.resumed_activation_id.as_deref()
                != Some(&self.occurrence.activation_id)
            || self.approval.status != ApprovalStatus::Approved
            || self.key != approved_effect_step_key(&self.occurrence.approval_key)
        {
            return Err(invalid());
        }
        if let Some(provider) = &self.approval.provider_binding
            && (provider.program != self.program || self.lookup.is_some())
        {
            return Err(invalid());
        }
        if let Some(lookup) = &self.lookup {
            validate_text(lookup, 512)?;
            if lookup == &self.approval.action.name {
                return Err(invalid());
            }
        }
        if let Some(id) = &self.approval_acceptance_event_id {
            validate_text(id, 128)?;
        }
        bounded(self, APPROVAL_SNAPSHOT_MAX_BYTES + 16 * 1024)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectEvidenceSummary {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::effect_resolution::present"
    )]
    pub provider: Option<ProviderEffectSummary>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::effect_resolution::present"
    )]
    pub operator_resolution: Option<EffectResolutionSummary>,
    pub occurrence: EffectOccurrence,
    pub program: ProgramRef,
    pub callable: String,
    pub action_version: String,
    pub dispatch_provenance: EffectDispatchProvenance,
    pub journal_state: EffectJournalState,
    pub has_discrepancy: bool,
    pub entry_count: u64,
    pub effect_revision: u64,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}
impl EffectEvidenceSummary {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()?;
        self.program.validate()?;
        validate_text(&self.callable, 512)?;
        validate_text(&self.action_version, 128)?;
        time(self.created_at)?;
        time(self.updated_at)?;
        positive(self.effect_revision)?;
        if self.created_at > self.updated_at || self.entry_count >= self.effect_revision {
            return Err(invalid());
        }
        if let Some(provider) = &self.provider {
            provider.validate(self.entry_count, self.effect_revision)?;
            if self.journal_state != EffectJournalState::Unconfirmed
                || (provider.handling == ProviderHandlingState::OperatorClosed)
                    != self.operator_resolution.is_some()
            {
                return Err(invalid());
            }
        }
        if let Some(resolution) = &self.operator_resolution {
            resolution.validate(self.entry_count, self.effect_revision)?;
            if self.provider.is_none()
                && resolution.first_conflicting_entry_sequence.is_some()
                && resolution.reference.assessment_kind != EffectAssessmentKind::RecordSuccess
            {
                return Err(invalid());
            }
        }
        match &self.dispatch_provenance {
            EffectDispatchProvenance::VerifiedClaim { claim_id } => validate_text(claim_id, 128)?,
            EffectDispatchProvenance::LegacyAttempt { attempt_id } => {
                validate_text(attempt_id, 128)?
            }
            EffectDispatchProvenance::LegacyFence => {}
        }
        Ok(())
    }
    pub fn position(&self) -> EffectInboxPosition {
        EffectInboxPosition {
            updated_at: self.updated_at,
            workflow_id: self.occurrence.workflow_id.clone(),
            activation_id: self.occurrence.activation_id.clone(),
            approval_key: self.occurrence.approval_key.clone(),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectInboxFilter {
    All,
    Unconfirmed,
    Confirmed,
    Discrepant,
    Disputed,
}
impl EffectInboxFilter {
    pub fn matches(self, item: &EffectEvidenceSummary) -> bool {
        match self {
            Self::All => true,
            Self::Unconfirmed => item.journal_state == EffectJournalState::Unconfirmed,
            Self::Confirmed => item.journal_state == EffectJournalState::Confirmed,
            Self::Discrepant => item.has_discrepancy,
            Self::Disputed => item
                .operator_resolution
                .as_ref()
                .is_some_and(|r| r.first_conflicting_entry_sequence.is_some()),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectInboxQuery {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::effect_resolution::present"
    )]
    pub provider_filter: Option<ProviderInboxFilter>,
    pub scope: Scope,
    pub filter: EffectInboxFilter,
    pub workflow_id: Option<String>,
    pub cursor: Option<String>,
    pub limit: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectInboxPosition {
    pub updated_at: Timestamp,
    pub workflow_id: String,
    pub activation_id: String,
    pub approval_key: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InboxCursor {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::effect_resolution::present"
    )]
    provider_filter: Option<ProviderInboxFilter>,
    version: u8,
    scope: Scope,
    filter: EffectInboxFilter,
    workflow_id: Option<String>,
    position: EffectInboxPosition,
}
impl EffectInboxQuery {
    pub fn validate(&self) -> Result<Option<EffectInboxPosition>> {
        if self.provider_filter.is_some() && self.filter != EffectInboxFilter::All {
            return Err(invalid());
        }
        self.scope.validate()?;
        if let Some(id) = &self.workflow_id {
            validate_text(id, 128)?;
        }
        if !(1..=100).contains(&self.limit) {
            return Err(invalid());
        }
        let Some(text) = &self.cursor else {
            return Ok(None);
        };
        let c: InboxCursor = decode_cursor(text)?;
        if c.version != if self.provider_filter.is_some() { 2 } else { 1 }
            || c.provider_filter != self.provider_filter
            || c.scope != self.scope
            || c.filter != self.filter
            || c.workflow_id != self.workflow_id
        {
            return Err(invalid());
        }
        time(c.position.updated_at)?;
        for id in [
            &c.position.workflow_id,
            &c.position.activation_id,
            &c.position.approval_key,
        ] {
            validate_text(id, 128)?;
        }
        if self
            .workflow_id
            .as_ref()
            .is_some_and(|id| id != &c.position.workflow_id)
        {
            return Err(invalid());
        }
        Ok(Some(c.position))
    }
    pub fn next_cursor(&self, item: &EffectEvidenceSummary) -> Result<String> {
        encode_cursor(&InboxCursor {
            provider_filter: self.provider_filter,
            version: if self.provider_filter.is_some() { 2 } else { 1 },
            scope: self.scope.clone(),
            filter: self.filter,
            workflow_id: self.workflow_id.clone(),
            position: item.position(),
        })
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectInboxPage {
    pub items: Vec<EffectEvidenceSummary>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub next_cursor: Option<String>,
}
impl EffectInboxPage {
    pub fn validate_for(&self, query: &EffectInboxQuery) -> Result<()> {
        let mut prior = query.validate()?;
        if self.items.len() > query.limit as usize {
            return Err(invalid());
        }
        let mut ids = std::collections::HashSet::new();
        for item in &self.items {
            item.validate()?;
            let pos = item.position();
            if item.occurrence.scope != query.scope
                || !query.filter.matches(item)
                || query
                    .provider_filter
                    .is_some_and(|filter| !filter.matches(item))
                || query
                    .workflow_id
                    .as_ref()
                    .is_some_and(|id| id != &item.occurrence.workflow_id)
                || prior.as_ref().is_some_and(|p| pos >= *p)
                || !ids.insert((
                    &item.occurrence.workflow_id,
                    &item.occurrence.activation_id,
                    &item.occurrence.approval_key,
                ))
            {
                return Err(invalid());
            }
            prior = Some(pos);
        }
        if let Some(next) = &self.next_cursor {
            let last = self.items.last().ok_or_else(invalid)?;
            if query.next_cursor(last)? != *next || query.cursor.as_ref() == Some(next) {
                return Err(invalid());
            }
        }
        bounded(self, EFFECT_EVIDENCE_PAGE_MAX_BYTES)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectInspectQuery {
    pub occurrence: EffectOccurrence,
    pub cursor: Option<String>,
    pub limit: u32,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InspectCursor {
    version: u8,
    occurrence: EffectOccurrence,
    after_sequence: u64,
}
impl EffectInspectQuery {
    pub fn validate(&self) -> Result<Option<u64>> {
        self.occurrence.validate()?;
        if !(1..=4).contains(&self.limit) {
            return Err(invalid());
        }
        let Some(text) = &self.cursor else {
            return Ok(None);
        };
        let c: InspectCursor = decode_cursor(text)?;
        if c.version != 1 || c.occurrence != self.occurrence {
            return Err(invalid());
        }
        positive(c.after_sequence)?;
        Ok(Some(c.after_sequence))
    }
    pub fn next_cursor(&self, sequence: u64) -> Result<String> {
        positive(sequence)?;
        encode_cursor(&InspectCursor {
            version: 1,
            occurrence: self.occurrence.clone(),
            after_sequence: sequence,
        })
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectEvidenceDetail {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::effect_resolution::present"
    )]
    pub provider: Option<ProviderEffectDetail>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::effect_resolution::present"
    )]
    pub current_assessment: Option<EffectEvidenceEntry>,
    pub binding: EffectEvidenceBinding,
    pub summary: EffectEvidenceSummary,
    pub entries: Vec<EffectEvidenceEntry>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub next_cursor: Option<String>,
}
impl EffectEvidenceDetail {
    pub fn validate_for(&self, query: &EffectInspectQuery) -> Result<()> {
        let after = query.validate()?.unwrap_or(0);
        self.binding.validate()?;
        self.summary.validate()?;
        if self.binding.occurrence != query.occurrence
            || self.summary.occurrence != query.occurrence
            || self.summary.program != self.binding.program.program
            || self.summary.callable != self.binding.approval.action.name
            || self.summary.action_version != self.binding.approval.action.version
            || self.entries.len() > query.limit as usize
        {
            return Err(invalid());
        }
        match (&self.current_assessment, &self.summary.operator_resolution) {
            (None, None) => {}
            (Some(entry), Some(resolution)) => {
                entry.validate(&query.occurrence.scope)?;
                if entry.resolution_reference().as_ref() != Some(&resolution.reference) {
                    return Err(invalid());
                }
            }
            _ => return Err(invalid()),
        }
        match (
            &self.provider,
            &self.summary.provider,
            &self.binding.approval.provider_binding,
        ) {
            (None, None, None) => {}
            (Some(detail), Some(summary), Some(binding)) => {
                if binding.program != self.binding.program
                    || summary.binding_digest != binding.digest(&self.binding.approval.action)?
                    || self.binding.lookup.is_some()
                {
                    return Err(invalid());
                }
                match (&detail.current_report, &summary.selected_report) {
                    (None, None) => {}
                    (Some(entry), Some(reference)) => {
                        entry.validate(&query.occurrence.scope)?;
                        if let EffectEvidenceEntry::ProviderReport { report, .. } = entry.as_ref()
                            && report.reported_target != binding.target
                        {
                            return Err(invalid());
                        }
                        if entry.provider_report_reference().as_ref() != Some(reference) {
                            return Err(invalid());
                        }
                    }
                    _ => return Err(invalid()),
                }
            }
            _ => return Err(invalid()),
        }
        if after > self.summary.entry_count {
            return Err(invalid());
        }
        let mut sequence = after;
        let mut revision = 0;
        let mut ids = std::collections::HashSet::new();
        for e in &self.entries {
            e.validate(&query.occurrence.scope)?;
            if let Some(provider_binding) = &self.binding.approval.provider_binding {
                if let EffectEvidenceEntry::ProviderReport { report, .. } = e
                    && report.reported_target != provider_binding.target
                {
                    return Err(invalid());
                }
                if let Some(summary) = &self.summary.provider {
                    for reference in summary
                        .selected_report
                        .iter()
                        .chain(&summary.conflicting_reports)
                    {
                        if e.sequence() == reference.sequence
                            && e.provider_report_reference().as_ref() != Some(reference)
                        {
                            return Err(invalid());
                        }
                    }
                }
            }

            if e.kind().starts_with("provider_") && self.provider.is_none() {
                return Err(invalid());
            }
            if let Some(current) = self
                .provider
                .as_ref()
                .and_then(|p| p.current_report.as_ref())
                && e.sequence() == current.sequence()
                && canonical_json_bytes(&serde_json::to_value(e).map_err(|_| invalid())?)?
                    != canonical_json_bytes(&serde_json::to_value(current).map_err(|_| invalid())?)?
            {
                return Err(invalid());
            }
            if matches!(e, EffectEvidenceEntry::Assessment { .. })
                && self
                    .current_assessment
                    .as_ref()
                    .is_none_or(|current| e.sequence() > current.sequence())
            {
                return Err(invalid());
            }
            if let Some(current) = &self.current_assessment
                && e.sequence() == current.sequence()
                && canonical_json_bytes(&serde_json::to_value(e).map_err(|_| invalid())?)?
                    != canonical_json_bytes(&serde_json::to_value(current).map_err(|_| invalid())?)?
            {
                return Err(invalid());
            }
            if sequence.checked_add(1) != Some(e.sequence())
                || e.sequence() > self.summary.entry_count
                || e.recorded_revision() <= revision
                || e.recorded_revision() > self.summary.effect_revision
                || !ids.insert((e.kind(), e.command_id()))
            {
                return Err(invalid());
            }
            sequence = e.sequence();
            revision = e.recorded_revision();
        }
        if self.next_cursor.is_some() != (sequence < self.summary.entry_count) {
            return Err(invalid());
        }
        if let Some(next) = &self.next_cursor
            && (self.entries.is_empty()
                || query.next_cursor(sequence)? != *next
                || sequence >= self.summary.entry_count)
        {
            return Err(invalid());
        }
        bounded(self, EFFECT_EVIDENCE_PAGE_MAX_BYTES)
    }
}
fn encode_cursor(value: &impl Serialize) -> Result<String> {
    let bytes = serde_json::to_vec(value).map_err(|_| invalid())?;
    let encoded = crate::cursor::encode(&bytes);
    if encoded.len() > EFFECT_EVIDENCE_CURSOR_MAX_BYTES {
        return Err(invalid());
    }
    Ok(encoded)
}
fn decode_cursor<T: serde::de::DeserializeOwned>(text: &str) -> Result<T> {
    let bytes =
        crate::cursor::decode(text, EFFECT_EVIDENCE_CURSOR_MAX_BYTES).ok_or_else(invalid)?;
    Ok(decode_unique_json(
        &bytes,
        EFFECT_EVIDENCE_CURSOR_MAX_BYTES / 2,
    )?)
}
