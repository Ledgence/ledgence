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
            Self::Claim { sequence, .. } | Self::Observation { sequence, .. } => *sequence,
        }
    }
    pub fn recorded_revision(&self) -> u64 {
        match self {
            Self::Claim {
                recorded_revision, ..
            }
            | Self::Observation {
                recorded_revision, ..
            } => *recorded_revision,
        }
    }
    pub fn recorded_at(&self) -> u64 {
        match self {
            Self::Claim { recorded_at, .. } | Self::Observation { recorded_at, .. } => *recorded_at,
        }
    }
    pub fn command_id(&self) -> &str {
        match self {
            Self::Claim { claim_id, .. } => claim_id,
            Self::Observation { observation_id, .. } => observation_id,
        }
    }
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Claim { .. } => "claim",
            Self::Observation { .. } => "observation",
        }
    }
    pub fn validate(&self, scope: &Scope) -> Result<()> {
        positive(self.sequence())?;
        positive(self.recorded_revision())?;
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
        bounded(self, EFFECT_EVIDENCE_COMMAND_MAX_BYTES)
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
}
impl EffectInboxFilter {
    pub fn matches(self, item: &EffectEvidenceSummary) -> bool {
        match self {
            Self::All => true,
            Self::Unconfirmed => item.journal_state == EffectJournalState::Unconfirmed,
            Self::Confirmed => item.journal_state == EffectJournalState::Confirmed,
            Self::Discrepant => item.has_discrepancy,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectInboxQuery {
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
    version: u8,
    scope: Scope,
    filter: EffectInboxFilter,
    workflow_id: Option<String>,
    position: EffectInboxPosition,
}
impl EffectInboxQuery {
    pub fn validate(&self) -> Result<Option<EffectInboxPosition>> {
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
        if c.version != 1
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
            version: 1,
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
        if after > self.summary.entry_count {
            return Err(invalid());
        }
        let mut sequence = after;
        let mut revision = 0;
        let mut ids = std::collections::HashSet::new();
        for e in &self.entries {
            e.validate(&query.occurrence.scope)?;
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
