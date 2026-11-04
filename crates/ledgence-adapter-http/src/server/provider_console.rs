//! Explicit Console projections preserve application JSON and stringify counters.
use super::*;
use ledgence_orchestration_api::console::{ConsoleProgramDescriptor, ConsoleU64};
use serde::Deserialize;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Binding {
    pub v: u8,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "super::approvals::optional_non_null"
    )]
    pub admission: Option<ProviderAdmissionRequirement>,
    pub business: ProviderBusinessIdentity,
    pub contract: ProviderContract,
    pub target: ProviderTarget,
    #[serde(deserialize_with = "super::approvals::required_option")]
    pub deduplication: Option<ProviderDeduplication>,
    pub credential_reference: String,
    pub reconciliation: ProviderReconciliationPolicy,
    pub program: ConsoleProgramDescriptor,
}
impl From<ProviderBinding> for Binding {
    fn from(v: ProviderBinding) -> Self {
        Self {
            v: v.v,
            admission: v.admission,
            business: v.business,
            contract: v.contract,
            target: v.target,
            deduplication: v.deduplication,
            credential_reference: v.credential_reference,
            reconciliation: v.reconciliation,
            program: v.program.into(),
        }
    }
}
impl Binding {
    pub fn into_core(self) -> ProviderBinding {
        ProviderBinding {
            v: self.v,
            admission: self.admission,
            business: self.business,
            contract: self.contract,
            target: self.target,
            deduplication: self.deduplication,
            credential_reference: self.credential_reference,
            reconciliation: self.reconciliation,
            program: ledgence_worker_api::ProgramDescriptor {
                program: self.program.program,
                digest: self.program.digest,
                size: self.program.size.0,
            },
        }
    }
}
#[derive(Serialize)]
pub(super) struct Reference {
    report_id: String,
    source_id: String,
    sequence: ConsoleU64,
    recorded_revision: ConsoleU64,
    recorded_at: Timestamp,
    knowledge: ProviderKnowledgeKind,
}
impl From<ProviderReportReference> for Reference {
    fn from(v: ProviderReportReference) -> Self {
        Self {
            report_id: v.report_id,
            source_id: v.source_id,
            sequence: ConsoleU64(v.sequence),
            recorded_revision: ConsoleU64(v.recorded_revision),
            recorded_at: v.recorded_at,
            knowledge: v.knowledge,
        }
    }
}
#[derive(Serialize)]
struct Reconciliation {
    queue: String,
    checks_used: u32,
    max_checks: u32,
    generation: ConsoleU64,
    active_probe_id: Option<String>,
    active_task_id: Option<String>,
    first_due_at: Timestamp,
    next_due_at: Option<Timestamp>,
    deadline_at: Timestamp,
    stopped_reason: Option<ProviderRecoveryStopReason>,
}
impl From<ProviderReconciliationSummary> for Reconciliation {
    fn from(v: ProviderReconciliationSummary) -> Self {
        Self {
            queue: v.queue,
            checks_used: v.checks_used,
            max_checks: v.max_checks,
            generation: ConsoleU64(v.generation),
            active_probe_id: v.active_probe_id,
            active_task_id: v.active_task_id,
            first_due_at: v.first_due_at,
            next_due_at: v.next_due_at,
            deadline_at: v.deadline_at,
            stopped_reason: v.stopped_reason,
        }
    }
}
#[derive(Serialize)]
pub(super) struct Summary {
    binding_digest: ledgence_worker_api::Digest,
    knowledge: ProviderKnowledgeKind,
    selected_report: Option<Reference>,
    conflicting_reports: Vec<Reference>,
    reconciliation: Reconciliation,
    handling: ProviderHandlingState,
    candidate_revision: Option<ConsoleU64>,
}
impl From<ProviderEffectSummary> for Summary {
    fn from(v: ProviderEffectSummary) -> Self {
        Self {
            binding_digest: v.binding_digest,
            knowledge: v.knowledge,
            selected_report: v.selected_report.map(Into::into),
            conflicting_reports: v.conflicting_reports.into_iter().map(Into::into).collect(),
            reconciliation: v.reconciliation.into(),
            handling: v.handling,
            candidate_revision: v.candidate_revision.map(ConsoleU64),
        }
    }
}
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum Source {
    Dispatch {
        claim_id: String,
    },
    Probe {
        probe_id: String,
        generation: ConsoleU64,
        task_id: String,
        attempt_id: String,
    },
}
impl From<ProviderSourceReference> for Source {
    fn from(v: ProviderSourceReference) -> Self {
        match v {
            ProviderSourceReference::Dispatch { claim_id } => Self::Dispatch { claim_id },
            ProviderSourceReference::Probe {
                probe_id,
                generation,
                task_id,
                attempt_id,
            } => Self::Probe {
                probe_id,
                generation: ConsoleU64(generation),
                task_id,
                attempt_id,
            },
        }
    }
}
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum Completion {
    Reported { report: Reference },
    Unreported { failure: ProviderProbeFailureKind },
}
impl From<ProviderProbeCompletion> for Completion {
    fn from(v: ProviderProbeCompletion) -> Self {
        match v {
            ProviderProbeCompletion::Reported { report } => Self::Reported {
                report: report.into(),
            },
            ProviderProbeCompletion::Unreported { failure } => Self::Unreported { failure },
        }
    }
}
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum Entry {
    #[serde(rename = "provider_dispatch")]
    Dispatch {
        sequence: ConsoleU64,
        recorded_at: Timestamp,
        recorded_revision: ConsoleU64,
        claim_id: String,
        source_id: String,
        report_id: String,
        worker: EffectWorkerIdentity,
    },
    #[serde(rename = "provider_probe_scheduled")]
    ProbeScheduled {
        sequence: ConsoleU64,
        recorded_at: Timestamp,
        recorded_revision: ConsoleU64,
        probe_id: String,
        generation: ConsoleU64,
        task_id: String,
        due_at: Timestamp,
    },
    #[serde(rename = "provider_probe_source")]
    ProbeSource {
        sequence: ConsoleU64,
        recorded_at: Timestamp,
        recorded_revision: ConsoleU64,
        probe_id: String,
        generation: ConsoleU64,
        task_id: String,
        attempt_id: String,
        source_id: String,
        report_id: String,
        worker: EffectWorkerIdentity,
    },
    #[serde(rename = "provider_report")]
    Report {
        sequence: ConsoleU64,
        recorded_at: Timestamp,
        recorded_revision: ConsoleU64,
        source_id: String,
        source: Source,
        report_id: String,
        worker: EffectWorkerIdentity,
        ingress: ProviderReportIngress,
        report: Box<ProviderReport>,
    },
    #[serde(rename = "provider_probe_finished")]
    ProbeFinished {
        sequence: ConsoleU64,
        recorded_at: Timestamp,
        recorded_revision: ConsoleU64,
        probe_id: String,
        generation: ConsoleU64,
        task_id: String,
        completion: Completion,
    },
    #[serde(rename = "provider_recovery_stopped")]
    RecoveryStopped {
        sequence: ConsoleU64,
        recorded_at: Timestamp,
        recorded_revision: ConsoleU64,
        reason: ProviderRecoveryStopReason,
    },
}
impl Entry {
    pub fn sequence(&self) -> u64 {
        match self {
            Self::Dispatch { sequence, .. }
            | Self::ProbeScheduled { sequence, .. }
            | Self::ProbeSource { sequence, .. }
            | Self::Report { sequence, .. }
            | Self::ProbeFinished { sequence, .. }
            | Self::RecoveryStopped { sequence, .. } => sequence.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    fn binding() -> ProviderBinding {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../../sdk/python-client/tests/fixtures/provider-effect.json"
        ))
        .unwrap();
        serde_json::from_value(fixture["binding"]["approval"]["provider_binding"].clone()).unwrap()
    }
    #[test]
    fn binding_roundtrip_preserves_frozen_contract_and_numeric_durations() {
        let value = binding();
        let encoded = serde_json::to_value(Binding::from(value.clone())).unwrap();
        assert!(encoded["program"]["size"].is_string());
        assert!(encoded["deduplication"]["horizon"]["duration_ms"].is_number());
        assert!(encoded["reconciliation"]["max_elapsed_ms"].is_number());
        let restored: Binding = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(restored.into_core(), value);
        let mut missing = encoded;
        missing.as_object_mut().unwrap().remove("deduplication");
        assert!(serde_json::from_value::<Binding>(missing).is_err());
    }
    #[test]
    fn governed_binding_roundtrip_preserves_reviewed_resource_and_rejects_null_requirement() {
        let mut original = binding();
        let legacy = serde_json::to_value(Binding::from(original.clone())).unwrap();
        assert!(legacy.get("admission").is_none());
        original.v = 2;
        original.admission = Some(ProviderAdmissionRequirement {
            resource: "export-dispatch".into(),
        });
        let encoded = serde_json::to_value(Binding::from(original.clone())).unwrap();
        let restored: Binding = serde_json::from_value(encoded.clone()).unwrap();
        let restored = restored.into_core();
        restored.validate().unwrap();
        assert_eq!(restored, original);
        let mut invalid = encoded;
        invalid["admission"] = Value::Null;
        assert!(serde_json::from_value::<Binding>(invalid).is_err());
    }
    #[test]
    fn report_projection_stringifies_only_platform_counters() {
        let report = ProviderReport {
            usage: None,
            reported_target: binding().target,
            provider_reference: None,
            provider_request_id: None,
            knowledge: ProviderKnowledge::Succeeded {
                result: json!({"max":u64::MAX,"fraction":1.0,"negative":-0.0}),
            },
        };
        let worker = EffectWorkerIdentity {
            audience: AccessAudience {
                authority_id: "authority".into(),
                instance_id: "instance".into(),
                scope: Scope {
                    tenant_id: "acme".into(),
                    namespace: "billing".into(),
                },
            },
            issuer: "issuer".into(),
            subject: "worker".into(),
            credential_id: "credential".into(),
        };
        let encoded = serde_json::to_value(Entry::Report {
            sequence: ConsoleU64(u64::MAX - 1),
            recorded_at: 1,
            recorded_revision: ConsoleU64(u64::MAX),
            source_id: "source".into(),
            source: Source::Probe {
                probe_id: "probe".into(),
                generation: ConsoleU64(u64::MAX),
                task_id: "task".into(),
                attempt_id: "attempt".into(),
            },
            report_id: "report".into(),
            worker,
            ingress: ProviderReportIngress::EvidenceOnly,
            report: Box::new(report.clone()),
        })
        .unwrap();
        assert_eq!(encoded["sequence"], (u64::MAX - 1).to_string());
        assert_eq!(encoded["recorded_revision"], u64::MAX.to_string());
        assert_eq!(encoded["source"]["generation"], u64::MAX.to_string());
        assert_eq!(encoded["report"], serde_json::to_value(report).unwrap());
        assert!(encoded["report"]["knowledge"]["result"]["max"].is_u64());
        assert!(encoded["report"]["knowledge"]["result"]["fraction"].is_f64());
    }
}
