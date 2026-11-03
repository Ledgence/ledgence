//! Payload-free provider summaries and bounded retained report references.
use crate::effect_evidence::{positive, time};
use crate::provider_binding::provider_invalid;
use crate::*;
use ledgence_worker_api::Digest;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderReportReference {
    pub report_id: String,
    pub source_id: String,
    pub sequence: u64,
    pub recorded_revision: u64,
    pub recorded_at: Timestamp,
    pub knowledge: ProviderKnowledgeKind,
}
impl ProviderReportReference {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.report_id, 128)?;
        validate_text(&self.source_id, 128)?;
        positive(self.sequence)?;
        positive(self.recorded_revision)?;
        time(self.recorded_at)?;
        if self.sequence >= self.recorded_revision
            || self.knowledge == ProviderKnowledgeKind::Disputed
        {
            return Err(provider_invalid());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderHandlingState {
    Unresolved,
    ProviderTerminal,
    OperatorClosed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRecoveryStopReason {
    ProviderTerminal,
    OperatorAssessment,
    WorkflowCancelled,
    WorkflowFailed,
    ActionRequired,
    LookupUnavailable,
    Disputed,
    BudgetExhausted,
    Deadline,
    RecoveryFailed,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderReconciliationSummary {
    pub queue: String,
    pub checks_used: u32,
    pub max_checks: u32,
    pub generation: u64,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub active_probe_id: Option<String>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub active_task_id: Option<String>,
    pub first_due_at: Timestamp,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub next_due_at: Option<Timestamp>,
    pub deadline_at: Timestamp,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub stopped_reason: Option<ProviderRecoveryStopReason>,
}
impl ProviderReconciliationSummary {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.queue, 128)?;
        time(self.first_due_at)?;
        time(self.deadline_at)?;
        for id in [&self.active_probe_id, &self.active_task_id]
            .into_iter()
            .flatten()
        {
            validate_text(id, 128)?;
        }
        if let Some(due) = self.next_due_at {
            time(due)?;
            if due > self.deadline_at {
                return Err(provider_invalid());
            }
        }
        if !(1..=256).contains(&self.max_checks)
            || self.checks_used > self.max_checks
            || self.generation != u64::from(self.checks_used)
            || self.active_probe_id.is_some() != self.active_task_id.is_some()
            || self.active_task_id.is_some() && self.generation == 0
            || self.first_due_at >= self.deadline_at
            || self.stopped_reason.is_some() && self.next_due_at.is_some()
        {
            return Err(provider_invalid());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderEffectSummary {
    pub binding_digest: Digest,
    pub knowledge: ProviderKnowledgeKind,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub selected_report: Option<ProviderReportReference>,
    pub conflicting_reports: Vec<ProviderReportReference>,
    pub reconciliation: ProviderReconciliationSummary,
    pub handling: ProviderHandlingState,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub candidate_revision: Option<u64>,
}
impl ProviderEffectSummary {
    pub fn validate(&self, entry_count: u64, effect_revision: u64) -> Result<()> {
        self.binding_digest.validate()?;
        self.reconciliation.validate()?;
        for reference in self.selected_report.iter().chain(&self.conflicting_reports) {
            reference.validate()?;
            if reference.sequence > entry_count || reference.recorded_revision > effect_revision {
                return Err(provider_invalid());
            }
        }
        if let Some(revision) = self.candidate_revision {
            positive(revision)?;
            if revision > effect_revision {
                return Err(provider_invalid());
            }
        }
        if self.knowledge == ProviderKnowledgeKind::Disputed {
            if self.conflicting_reports.len() != 2
                || self.selected_report.is_some()
                || self.conflicting_reports[0].sequence >= self.conflicting_reports[1].sequence
            {
                return Err(provider_invalid());
            }
        } else if !self.conflicting_reports.is_empty()
            || self
                .selected_report
                .as_ref()
                .is_some_and(|r| r.knowledge != self.knowledge)
            || self.selected_report.is_none() && self.knowledge != ProviderKnowledgeKind::Unknown
        {
            return Err(provider_invalid());
        }
        let terminal = matches!(
            self.knowledge,
            ProviderKnowledgeKind::Succeeded
                | ProviderKnowledgeKind::Failed
                | ProviderKnowledgeKind::Cancelled
                | ProviderKnowledgeKind::Disputed
        );
        if (self.handling == ProviderHandlingState::ProviderTerminal && !terminal)
            || (self.handling == ProviderHandlingState::Unresolved
                && terminal
                && self.knowledge != ProviderKnowledgeKind::Disputed)
        {
            return Err(provider_invalid());
        }
        Ok(())
    }
    pub fn needs_review(&self, operator_conflict: bool) -> bool {
        operator_conflict
            || self.handling != ProviderHandlingState::OperatorClosed
                && self.knowledge == ProviderKnowledgeKind::Disputed
            || self.handling == ProviderHandlingState::Unresolved
                && matches!(
                    self.reconciliation.stopped_reason,
                    Some(
                        ProviderRecoveryStopReason::ActionRequired
                            | ProviderRecoveryStopReason::LookupUnavailable
                            | ProviderRecoveryStopReason::BudgetExhausted
                            | ProviderRecoveryStopReason::Deadline
                            | ProviderRecoveryStopReason::RecoveryFailed
                    )
                )
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderEffectDetail {
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub current_report: Option<Box<EffectEvidenceEntry>>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderInboxFilter {
    All,
    Pending,
    NeedsReview,
    Succeeded,
    Failed,
    Cancelled,
    Disputed,
}
impl ProviderInboxFilter {
    pub fn matches(self, item: &EffectEvidenceSummary) -> bool {
        let Some(provider) = &item.provider else {
            return false;
        };
        let conflict = item
            .operator_resolution
            .as_ref()
            .is_some_and(|r| r.first_conflicting_entry_sequence.is_some());
        match self {
            Self::All => true,
            Self::Pending => {
                matches!(
                    provider.knowledge,
                    ProviderKnowledgeKind::Unknown | ProviderKnowledgeKind::Pending
                ) && provider.handling == ProviderHandlingState::Unresolved
                    && provider.reconciliation.stopped_reason.is_none()
                    && !provider.needs_review(conflict)
            }
            Self::NeedsReview => provider.needs_review(conflict),
            Self::Succeeded => provider.knowledge == ProviderKnowledgeKind::Succeeded,
            Self::Failed => provider.knowledge == ProviderKnowledgeKind::Failed,
            Self::Cancelled => provider.knowledge == ProviderKnowledgeKind::Cancelled,
            Self::Disputed => provider.knowledge == ProviderKnowledgeKind::Disputed || conflict,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderProbeCompletion {
    Reported { report: ProviderReportReference },
    Unreported { failure: ProviderProbeFailureKind },
}

impl ProviderRecoveryStopReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProviderTerminal => "provider_terminal",
            Self::OperatorAssessment => "operator_assessment",
            Self::WorkflowCancelled => "workflow_cancelled",
            Self::WorkflowFailed => "workflow_failed",
            Self::ActionRequired => "action_required",
            Self::LookupUnavailable => "lookup_unavailable",
            Self::Disputed => "disputed",
            Self::BudgetExhausted => "budget_exhausted",
            Self::Deadline => "deadline",
            Self::RecoveryFailed => "recovery_failed",
        }
    }
}
