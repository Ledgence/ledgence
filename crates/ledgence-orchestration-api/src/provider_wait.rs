//! One-shot durable provider rendezvous; it never grants a mutation or occupies a worker.
use crate::effect_evidence::{positive, time};
use crate::provider_binding::{provider_bound, provider_invalid};
use crate::*;
use serde_json::Value;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderWait {
    pub key: String,
    pub occurrence: EffectOccurrence,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub after_effect_revision: Option<u64>,
}
impl ProviderWait {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.key, 128)?;
        self.occurrence.validate()?;
        if let Some(revision) = self.after_effect_revision {
            positive(revision)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderSelection {
    ProviderSucceeded {
        report_ref: ProviderReportReference,
        #[serde(deserialize_with = "crate::observation::required_value")]
        result: Value,
    },
    ProviderFailed {
        report_ref: ProviderReportReference,
        failure: ApplicationError,
    },
    ProviderCancelled {
        report_ref: ProviderReportReference,
        reason: String,
    },
    OperatorAssessment {
        reference: Box<EffectResolutionReference>,
        outcome: EffectResolutionOutcome,
    },
    Disputed {
        first_conflicting_entry_sequence: u64,
    },
    NeedsReview {
        reason: ProviderRecoveryStopReason,
        checks_used: u32,
        deadline_at: Timestamp,
    },
}
impl ProviderSelection {
    pub fn validate(&self, selected_revision: u64, selected_at: Timestamp) -> Result<()> {
        match self {
            Self::ProviderSucceeded { report_ref, result } => {
                report_ref.validate()?;
                if report_ref.knowledge != ProviderKnowledgeKind::Succeeded {
                    return Err(provider_invalid());
                }
                ledgence_worker_api::validate_wire_value(result)?;
                provider_bound(result, PROVIDER_RESULT_MAX_BYTES)?;
            }
            Self::ProviderFailed {
                report_ref,
                failure,
            } => {
                report_ref.validate()?;
                if report_ref.knowledge != ProviderKnowledgeKind::Failed {
                    return Err(provider_invalid());
                }
                crate::workflow::validate_workflow_error(failure)?;
            }
            Self::ProviderCancelled { report_ref, reason } => {
                report_ref.validate()?;
                if report_ref.knowledge != ProviderKnowledgeKind::Cancelled {
                    return Err(provider_invalid());
                }
                validate_text(reason, 4096)?;
            }
            Self::OperatorAssessment { reference, outcome } => {
                reference.validate()?;
                outcome.validate()?;
                if reference.accepted_revision > selected_revision
                    || reference.accepted_at > selected_at
                {
                    return Err(provider_invalid());
                }
                let matches = matches!(
                    (reference.assessment_kind, outcome),
                    (
                        EffectAssessmentKind::RecordSuccess,
                        EffectResolutionOutcome::RecordedSuccess { .. }
                    ) | (
                        EffectAssessmentKind::RecordFailure,
                        EffectResolutionOutcome::RecordedFailure { .. }
                    ) | (
                        EffectAssessmentKind::CloseUnknown,
                        EffectResolutionOutcome::ClosedUnknown
                    ) | (
                        EffectAssessmentKind::RecordSuccess | EffectAssessmentKind::RecordFailure,
                        EffectResolutionOutcome::Disputed { .. }
                    )
                );
                if !matches {
                    return Err(provider_invalid());
                }
                if let EffectResolutionOutcome::Disputed {
                    first_conflicting_entry_sequence,
                } = outcome
                    && (*first_conflicting_entry_sequence >= selected_revision
                        || *first_conflicting_entry_sequence <= reference.sequence)
                {
                    return Err(provider_invalid());
                }
            }
            Self::Disputed {
                first_conflicting_entry_sequence,
            } => {
                positive(*first_conflicting_entry_sequence)?;
                if *first_conflicting_entry_sequence >= selected_revision {
                    return Err(provider_invalid());
                }
            }
            Self::NeedsReview {
                reason,
                checks_used,
                deadline_at,
            } => {
                time(*deadline_at)?;
                if *checks_used > 256
                    || !matches!(
                        reason,
                        ProviderRecoveryStopReason::ActionRequired
                            | ProviderRecoveryStopReason::LookupUnavailable
                            | ProviderRecoveryStopReason::BudgetExhausted
                            | ProviderRecoveryStopReason::Deadline
                            | ProviderRecoveryStopReason::RecoveryFailed
                    )
                    || *reason == ProviderRecoveryStopReason::Deadline && *deadline_at > selected_at
                {
                    return Err(provider_invalid());
                }
            }
        }
        if let Self::ProviderSucceeded { report_ref, .. }
        | Self::ProviderFailed { report_ref, .. }
        | Self::ProviderCancelled { report_ref, .. } = self
            && (report_ref.recorded_revision > selected_revision
                || report_ref.recorded_at > selected_at)
        {
            return Err(provider_invalid());
        }
        Ok(())
    }
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ProviderSucceeded { .. } => "provider_succeeded",
            Self::ProviderFailed { .. } => "provider_failed",
            Self::ProviderCancelled { .. } => "provider_cancelled",
            Self::OperatorAssessment { .. } => "operator_assessment",
            Self::Disputed { .. } => "disputed",
            Self::NeedsReview { .. } => "needs_review",
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderWake {
    pub key: String,
    pub occurrence: EffectOccurrence,
    pub parking_activation_id: String,
    pub parking_revision: u64,
    pub resumed_activation_id: String,
    pub resumed_revision: u64,
    pub selected_at: Timestamp,
    pub selected_effect_revision: u64,
    pub selection: ProviderSelection,
}
impl ProviderWake {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.key, 128)?;
        self.occurrence.validate()?;
        validate_text(&self.parking_activation_id, 128)?;
        validate_text(&self.resumed_activation_id, 128)?;
        time(self.selected_at)?;
        positive(self.selected_effect_revision)?;
        if self.parking_activation_id == self.resumed_activation_id
            || self.parking_revision.checked_add(1) != Some(self.resumed_revision)
        {
            return Err(provider_invalid());
        }
        self.selection
            .validate(self.selected_effect_revision, self.selected_at)?;
        provider_bound(self, PROVIDER_WAKE_MAX_BYTES)
    }
}
