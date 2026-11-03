//! Typed callback knowledge, authenticated source receipts and complete provider ports.
use crate::effect_evidence::{positive, time};
use crate::provider_binding::{provider_bound, provider_digest, provider_invalid};
use crate::*;
use ledgence_worker_api::Digest;
use serde_json::Value;

pub trait ProviderOperations: Send + Sync {
    fn claim_provider<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ProviderClaimCommand,
    ) -> ContractFuture<'a, ProviderClaimReceipt>;
    fn report_dispatch<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ProviderDispatchReportCommand,
    ) -> ContractFuture<'a, ProviderReportReply>;
    fn probe_context<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ProviderProbeContextCommand,
    ) -> ContractFuture<'a, ProviderProbeContext>;
    fn report_probe<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ProviderProbeReportCommand,
    ) -> ContractFuture<'a, ProviderReportReply>;
    fn observe_provider<'a>(
        &'a self,
        principal: &'a VerifiedPrincipal,
        command: &'a ProviderObservationCommand,
    ) -> ContractFuture<'a, ProviderReportReply>;
}
/// Returning this capability promises atomic task/outbox creation, typed checkpoint
/// coordination and marker-aware cancellation/finalization/retention integration.
pub trait ProviderOperationStore: ProviderOperations {
    fn advance_provider_operations<'a>(
        &'a self,
        query: &'a ProviderMaintenanceQuery,
    ) -> ContractFuture<'a, ProviderMaintenanceProgress>;
    fn apply_provider_work<'a>(
        &'a self,
        work: &'a WorkflowWork,
        resolved_children: &'a [ResolvedWorkflowChild],
    ) -> ContractFuture<'a, WorkflowProgress>;
}
pub trait ProviderOperationClient: Send + Sync {
    fn claim_provider<'a>(
        &'a self,
        command: &'a ProviderClaimCommand,
    ) -> ContractFuture<'a, ProviderClaimReceipt>;
    fn report_dispatch<'a>(
        &'a self,
        command: &'a ProviderDispatchReportCommand,
    ) -> ContractFuture<'a, ProviderReportReply>;
    fn probe_context<'a>(
        &'a self,
        command: &'a ProviderProbeContextCommand,
    ) -> ContractFuture<'a, ProviderProbeContext>;
    fn report_probe<'a>(
        &'a self,
        command: &'a ProviderProbeReportCommand,
    ) -> ContractFuture<'a, ProviderReportReply>;
    fn observe_provider<'a>(
        &'a self,
        command: &'a ProviderObservationCommand,
    ) -> ContractFuture<'a, ProviderReportReply>;
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderOperationTicket {
    pub occurrence: EffectOccurrence,
    pub binding_digest: Digest,
}
impl ProviderOperationTicket {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()?;
        Ok(self.binding_digest.validate()?)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderClaimCommand {
    pub owner: LeaseOwner,
    pub workflow_id: String,
    pub approval_key: String,
    pub claim_id: String,
    pub report_id: String,
    pub action: ApprovalAction,
    pub binding: ProviderBinding,
}
impl ProviderClaimCommand {
    pub fn occurrence(&self) -> EffectOccurrence {
        EffectOccurrence {
            scope: self.owner.scope.clone(),
            workflow_id: self.workflow_id.clone(),
            activation_id: self.owner.task_id.clone(),
            approval_key: self.approval_key.clone(),
        }
    }
    pub fn ticket(&self) -> Result<ProviderOperationTicket> {
        Ok(ProviderOperationTicket {
            occurrence: self.occurrence(),
            binding_digest: self.binding.digest(&self.action)?,
        })
    }
    pub fn validate(&self) -> Result<()> {
        self.owner.validate()?;
        self.occurrence().validate()?;
        validate_text(&self.claim_id, 128)?;
        validate_text(&self.report_id, 128)?;
        self.binding.validate_action(&self.action)?;
        provider_bound(self, PROVIDER_COMMAND_MAX_BYTES)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDispatchSource {
    pub source_id: String,
    pub claim_id: String,
    pub report_id: String,
    pub ticket: ProviderOperationTicket,
    pub worker: EffectWorkerIdentity,
    pub accepted_at: Timestamp,
    pub first_possible_dispatch_at: Timestamp,
    pub reconciliation_deadline: Timestamp,
}
impl ProviderDispatchSource {
    pub fn validate(&self) -> Result<()> {
        for id in [&self.source_id, &self.claim_id, &self.report_id] {
            validate_text(id, 128)?;
        }
        self.ticket.validate()?;
        self.worker.validate()?;
        time(self.accepted_at)?;
        time(self.first_possible_dispatch_at)?;
        time(self.reconciliation_deadline)?;
        if self.accepted_at != self.first_possible_dispatch_at
            || self.reconciliation_deadline <= self.accepted_at
            || self.worker.audience.scope != self.ticket.occurrence.scope
        {
            return Err(provider_invalid());
        }
        provider_bound(self, PROVIDER_RECEIPT_MAX_BYTES)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderClaimReceipt {
    Dispatch {
        ticket: ProviderOperationTicket,
        source: Box<ProviderDispatchSource>,
    },
    Existing {
        audience: AccessAudience,
        ticket: ProviderOperationTicket,
    },
    ExistingOperation {
        audience: AccessAudience,
        ticket: ProviderOperationTicket,
    },
}
impl ProviderClaimReceipt {
    pub fn ticket(&self) -> &ProviderOperationTicket {
        match self {
            Self::Dispatch { ticket, .. }
            | Self::Existing { ticket, .. }
            | Self::ExistingOperation { ticket, .. } => ticket,
        }
    }
    pub fn validate(&self) -> Result<()> {
        self.ticket().validate()?;
        match self {
            Self::Dispatch { ticket, source } => {
                source.validate()?;
                if ticket != &source.ticket {
                    return Err(provider_invalid());
                }
            }
            Self::Existing { audience, ticket } | Self::ExistingOperation { audience, ticket } => {
                audience.validate()?;
                if audience.scope != ticket.occurrence.scope {
                    return Err(provider_invalid());
                }
            }
        }
        provider_bound(self, PROVIDER_RECEIPT_MAX_BYTES)
    }
    pub fn matches(&self, command: &ProviderClaimCommand) -> Result<bool> {
        self.validate()?;
        command.validate()?;
        let expected = command.ticket()?;
        let ticket = self.ticket();
        if ticket.binding_digest != expected.binding_digest
            || ticket.occurrence.scope != expected.occurrence.scope
        {
            return Ok(false);
        }
        Ok(match self {
            Self::Dispatch { source, .. } => {
                ticket == &expected
                    && source.claim_id == command.claim_id
                    && source.report_id == command.report_id
            }
            Self::Existing { .. } => ticket == &expected,
            Self::ExistingOperation { .. } => ticket.occurrence != expected.occurrence,
        })
    }
    pub fn validate_worker(&self, worker: &EffectWorkerIdentity) -> Result<()> {
        self.validate()?;
        let same = match self {
            Self::Dispatch { source, .. } => source.worker.same_worker(worker),
            Self::Existing { audience, .. } | Self::ExistingOperation { audience, .. } => {
                audience == &worker.audience
            }
        };
        if same {
            Ok(())
        } else {
            Err(provider_reply_error())
        }
    }
    pub fn validate_for(
        &self,
        principal: &VerifiedPrincipal,
        command: &ProviderClaimCommand,
    ) -> Result<()> {
        if !self.matches(command).unwrap_or(false) {
            return Err(provider_reply_error());
        }
        self.validate_worker(&EffectWorkerIdentity::from_principal(principal)?)
            .map_err(|_| provider_reply_error())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKnowledgeKind {
    Unknown,
    Pending,
    RequiresAction,
    Succeeded,
    Failed,
    Cancelled,
    Disputed,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderKnowledge {
    Unknown {
        reason: String,
    },
    Pending,
    RequiresAction {
        action_kind: String,
    },
    Succeeded {
        #[serde(deserialize_with = "crate::observation::required_value")]
        result: Value,
    },
    Failed {
        failure: ApplicationError,
    },
    Cancelled {
        reason: String,
    },
}
impl ProviderKnowledge {
    pub fn kind(&self) -> ProviderKnowledgeKind {
        match self {
            Self::Unknown { .. } => ProviderKnowledgeKind::Unknown,
            Self::Pending => ProviderKnowledgeKind::Pending,
            Self::RequiresAction { .. } => ProviderKnowledgeKind::RequiresAction,
            Self::Succeeded { .. } => ProviderKnowledgeKind::Succeeded,
            Self::Failed { .. } => ProviderKnowledgeKind::Failed,
            Self::Cancelled { .. } => ProviderKnowledgeKind::Cancelled,
        }
    }
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded { .. } | Self::Failed { .. } | Self::Cancelled { .. }
        )
    }
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Unknown { reason } | Self::Cancelled { reason } => validate_text(reason, 4096),
            Self::Pending => Ok(()),
            Self::RequiresAction { action_kind } => validate_text(action_kind, 128),
            Self::Succeeded { result } => {
                ledgence_worker_api::validate_wire_value(result)?;
                provider_bound(result, PROVIDER_RESULT_MAX_BYTES)
            }
            Self::Failed { failure } => crate::workflow::validate_workflow_error(failure),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderReport {
    pub reported_target: ProviderTarget,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub provider_reference: Option<String>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub provider_request_id: Option<String>,
    pub knowledge: ProviderKnowledge,
}
impl ProviderReport {
    pub fn validate(&self) -> Result<()> {
        self.reported_target.validate()?;
        for value in [&self.provider_reference, &self.provider_request_id]
            .into_iter()
            .flatten()
        {
            validate_text(value, 1024)?;
        }
        self.knowledge.validate()?;
        provider_bound(self, PROVIDER_REPORT_MAX_BYTES)
    }
    pub fn digest(&self) -> Result<Digest> {
        self.validate()?;
        provider_digest(self)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderProbeAssignment {
    pub occurrence: EffectOccurrence,
    pub probe_id: String,
    pub generation: u64,
}
impl ProviderProbeAssignment {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()?;
        validate_text(&self.probe_id, 128)?;
        positive(self.generation)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderProbeContextCommand {
    pub owner: LeaseOwner,
    pub probe: ProviderProbeAssignment,
}
impl ProviderProbeContextCommand {
    pub fn validate(&self) -> Result<()> {
        self.owner.validate()?;
        self.probe.validate()?;
        if self.owner.scope != self.probe.occurrence.scope {
            return Err(provider_invalid());
        }
        provider_bound(self, PROVIDER_COMMAND_MAX_BYTES)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderProbeContext {
    pub ticket: ProviderOperationTicket,
    pub binding: ProviderBinding,
    pub action: ApprovalAction,
    pub probe_id: String,
    pub generation: u64,
    pub task_id: String,
    pub attempt_id: String,
    pub source_id: String,
    pub report_id: String,
    pub worker: EffectWorkerIdentity,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub provider_reference: Option<String>,
    pub check_timeout_ms: u64,
}
impl ProviderProbeContext {
    pub fn validate(&self) -> Result<()> {
        self.ticket.validate()?;
        self.binding.validate_action(&self.action)?;
        self.worker.validate()?;
        for id in [
            &self.probe_id,
            &self.task_id,
            &self.attempt_id,
            &self.source_id,
            &self.report_id,
        ] {
            validate_text(id, 128)?;
        }
        positive(self.generation)?;
        if let Some(reference) = &self.provider_reference {
            validate_text(reference, 1024)?;
        }
        if self.worker.audience.scope != self.ticket.occurrence.scope
            || self.ticket.binding_digest != self.binding.digest(&self.action)?
            || self.check_timeout_ms != self.binding.reconciliation.check_timeout_ms
            || self.binding.contract.lookup == ProviderLookup::Unavailable
            || (self.binding.contract.lookup == ProviderLookup::ByReference
                && self.provider_reference.is_none())
        {
            return Err(provider_invalid());
        }
        provider_bound(self, PROVIDER_CONTEXT_MAX_BYTES)
    }
    pub fn matches(&self, command: &ProviderProbeContextCommand) -> Result<bool> {
        self.validate()?;
        command.validate()?;
        Ok(self.ticket.occurrence == command.probe.occurrence
            && self.probe_id == command.probe.probe_id
            && self.generation == command.probe.generation
            && self.task_id == command.owner.task_id
            && self.attempt_id == command.owner.attempt_id)
    }
    pub fn validate_for(
        &self,
        principal: &VerifiedPrincipal,
        command: &ProviderProbeContextCommand,
    ) -> Result<()> {
        if self.matches(command).unwrap_or(false) && self.worker.matches_principal(principal) {
            Ok(())
        } else {
            Err(provider_reply_error())
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderSourceReference {
    Dispatch {
        claim_id: String,
    },
    Probe {
        probe_id: String,
        generation: u64,
        task_id: String,
        attempt_id: String,
    },
}
impl ProviderSourceReference {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Dispatch { claim_id } => validate_text(claim_id, 128),
            Self::Probe {
                probe_id,
                generation,
                task_id,
                attempt_id,
            } => {
                for id in [probe_id, task_id, attempt_id] {
                    validate_text(id, 128)?;
                }
                positive(*generation)
            }
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderReportIngress {
    LiveDispatch,
    LiveProbe,
    EvidenceOnly,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDispatchReportCommand {
    pub owner: LeaseOwner,
    pub ticket: ProviderOperationTicket,
    pub source_id: String,
    pub report_id: String,
    pub report: ProviderReport,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderProbeReportCommand {
    pub owner: LeaseOwner,
    pub ticket: ProviderOperationTicket,
    pub generation: u64,
    pub source_id: String,
    pub report_id: String,
    pub report: ProviderReport,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderObservationCommand {
    pub ticket: ProviderOperationTicket,
    pub source_id: String,
    pub report_id: String,
    pub report: ProviderReport,
}
/// Shared receipt binding; the source's stored owner remains server-side.
pub trait ProviderReportedCommand: Serialize {
    fn ticket(&self) -> &ProviderOperationTicket;
    fn source_id(&self) -> &str;
    fn report_id(&self) -> &str;
    fn report(&self) -> &ProviderReport;
    fn ingress(&self) -> ProviderReportIngress;
    fn validate(&self) -> Result<()>;
    fn source_matches(&self, source: &ProviderSourceReference) -> bool;
}
macro_rules! report_accessors {
    () => {
        fn ticket(&self) -> &ProviderOperationTicket {
            &self.ticket
        }
        fn source_id(&self) -> &str {
            &self.source_id
        }
        fn report_id(&self) -> &str {
            &self.report_id
        }
        fn report(&self) -> &ProviderReport {
            &self.report
        }
    };
}
fn report_fields(command: &impl ProviderReportedCommand) -> Result<()> {
    command.ticket().validate()?;
    validate_text(command.source_id(), 128)?;
    validate_text(command.report_id(), 128)?;
    command.report().validate()?;
    provider_bound(command, PROVIDER_COMMAND_MAX_BYTES)
}
impl ProviderReportedCommand for ProviderDispatchReportCommand {
    report_accessors!();
    fn ingress(&self) -> ProviderReportIngress {
        ProviderReportIngress::LiveDispatch
    }
    fn validate(&self) -> Result<()> {
        self.owner.validate()?;
        if self.owner.scope != self.ticket.occurrence.scope
            || self.owner.task_id != self.ticket.occurrence.activation_id
        {
            return Err(provider_invalid());
        }
        report_fields(self)
    }
    fn source_matches(&self, source: &ProviderSourceReference) -> bool {
        matches!(source, ProviderSourceReference::Dispatch { .. })
    }
}
impl ProviderReportedCommand for ProviderProbeReportCommand {
    report_accessors!();
    fn ingress(&self) -> ProviderReportIngress {
        ProviderReportIngress::LiveProbe
    }
    fn validate(&self) -> Result<()> {
        self.owner.validate()?;
        positive(self.generation)?;
        if self.owner.scope != self.ticket.occurrence.scope {
            return Err(provider_invalid());
        }
        report_fields(self)
    }
    fn source_matches(&self, source: &ProviderSourceReference) -> bool {
        matches!(source,ProviderSourceReference::Probe{generation,task_id,attempt_id,..} if *generation==self.generation&&task_id==&self.owner.task_id&&attempt_id==&self.owner.attempt_id)
    }
}
impl ProviderReportedCommand for ProviderObservationCommand {
    report_accessors!();
    fn ingress(&self) -> ProviderReportIngress {
        ProviderReportIngress::EvidenceOnly
    }
    fn validate(&self) -> Result<()> {
        report_fields(self)
    }
    fn source_matches(&self, _: &ProviderSourceReference) -> bool {
        true
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderReportReceipt {
    pub ticket: ProviderOperationTicket,
    pub source_id: String,
    pub source: ProviderSourceReference,
    pub report_id: String,
    pub report_digest: Digest,
    pub worker: EffectWorkerIdentity,
    pub accepted_at: Timestamp,
    pub recorded_revision: u64,
    pub accepted_via: ProviderReportIngress,
    pub already_accepted: bool,
}
impl ProviderReportReceipt {
    pub fn validate(&self) -> Result<()> {
        self.ticket.validate()?;
        validate_text(&self.source_id, 128)?;
        self.source.validate()?;
        validate_text(&self.report_id, 128)?;
        self.report_digest.validate()?;
        self.worker.validate()?;
        time(self.accepted_at)?;
        positive(self.recorded_revision)?;
        if self.worker.audience.scope != self.ticket.occurrence.scope
            || matches!(
                (&self.source, self.accepted_via),
                (
                    ProviderSourceReference::Dispatch { .. },
                    ProviderReportIngress::LiveProbe
                ) | (
                    ProviderSourceReference::Probe { .. },
                    ProviderReportIngress::LiveDispatch
                )
            )
        {
            return Err(provider_invalid());
        }
        provider_bound(self, PROVIDER_RECEIPT_MAX_BYTES)
    }
    pub fn matches(&self, command: &impl ProviderReportedCommand) -> Result<bool> {
        self.validate()?;
        command.validate()?;
        Ok(self.ticket == *command.ticket()
            && self.source_id == command.source_id()
            && self.report_id == command.report_id()
            && self.report_digest == command.report().digest()?
            && command.source_matches(&self.source))
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderReportReply {
    Accepted {
        receipt: ProviderReportReceipt,
    },
    ExistingReport {
        original_receipt: ProviderReportReceipt,
    },
}
impl ProviderReportReply {
    pub fn receipt(&self) -> &ProviderReportReceipt {
        match self {
            Self::Accepted { receipt } => receipt,
            Self::ExistingReport { original_receipt } => original_receipt,
        }
    }
    pub fn validate(&self) -> Result<()> {
        self.receipt().validate()?;
        provider_bound(self, PROVIDER_RECEIPT_MAX_BYTES)
    }
    pub fn matches(&self, command: &impl ProviderReportedCommand) -> Result<bool> {
        self.validate()?;
        if !self.receipt().matches(command)? {
            return Ok(false);
        }
        Ok(match self {
            Self::Accepted { receipt } => receipt.accepted_via == command.ingress(),
            Self::ExistingReport { original_receipt } => {
                original_receipt.accepted_via != command.ingress()
            }
        })
    }
    pub fn validate_for(
        &self,
        principal: &VerifiedPrincipal,
        command: &impl ProviderReportedCommand,
    ) -> Result<()> {
        if self.matches(command).unwrap_or(false)
            && self.receipt().worker.matches_principal(principal)
        {
            Ok(())
        } else {
            Err(provider_reply_error())
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderProbeFailureKind {
    IncompatibleWorker,
    ContractMismatch,
    ContextInvalid,
    ResultInvalid,
    InvocationTimeout,
    InvocationFailed,
    PreparationFailed,
    WorkerLost,
    Cancelled,
    MissingReport,
}
impl ProviderProbeFailureKind {
    pub fn validate_settlement(self, report: &AttemptReport) -> Result<()> {
        use ledgence_worker_api::{ErrorKind as E, Phase as P};
        let AttemptReport::Failed(failure) = report else {
            return Err(provider_invalid());
        };
        let phase = failure.phase;
        let kind = failure.error.kind;
        let valid = match self {
            Self::IncompatibleWorker => {
                kind == E::Incompatible
                    && matches!(phase, P::Admission | P::Preparation | P::Startup)
            }
            Self::ContractMismatch => {
                matches!(kind, E::Integrity | E::Incompatible)
                    && matches!(phase, P::Admission | P::Preparation | P::Execution)
            }
            Self::ContextInvalid => {
                matches!(kind, E::Protocol | E::InvalidInput)
                    && matches!(phase, P::Admission | P::Execution)
            }
            Self::ResultInvalid => kind == E::Protocol && phase == P::Execution,
            Self::InvocationTimeout => kind == E::TimedOut && phase == P::Execution,
            Self::InvocationFailed => {
                matches!(kind, E::Runtime | E::Io | E::Unavailable | E::Cancelled)
                    && matches!(phase, P::Execution | P::Cleanup)
            }
            Self::PreparationFailed => {
                kind != E::Incompatible
                    && matches!(phase, P::Admission | P::Preparation | P::Startup)
            }
            Self::WorkerLost | Self::Cancelled | Self::MissingReport => false,
        };
        if valid {
            Ok(())
        } else {
            Err(provider_invalid())
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderProbeRuntimeRejection {
    ContractMismatch,
    ContextInvalid,
    ResultInvalid,
    InvocationFailed,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderProbeRuntimeResult {
    Report {
        v: u8,
        report: Box<ProviderReport>,
    },
    Rejected {
        v: u8,
        failure: ProviderProbeRuntimeRejection,
    },
}
impl ProviderProbeRuntimeResult {
    pub fn validate(&self) -> Result<()> {
        let v = match self {
            Self::Report { v, report } => {
                report.validate()?;
                v
            }
            Self::Rejected { v, .. } => v,
        };
        if *v != 1 {
            return Err(provider_invalid());
        }
        provider_bound(self, PROVIDER_RUNTIME_RESULT_MAX_BYTES)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderMaintenanceCursor {
    pub due_at: Timestamp,
    pub occurrence: EffectOccurrence,
}
#[derive(Debug, Clone)]
pub struct ProviderMaintenanceQuery {
    pub cursor: Option<ProviderMaintenanceCursor>,
    pub limit: u32,
}
impl ProviderMaintenanceQuery {
    pub fn validate(&self) -> Result<()> {
        if !(1..=PROVIDER_MAX_MAINTENANCE_BATCH).contains(&self.limit) {
            return Err(provider_invalid());
        }
        if let Some(cursor) = &self.cursor {
            time(cursor.due_at)?;
            cursor.occurrence.validate()?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Default)]
pub struct ProviderMaintenanceProgress {
    pub examined: u32,
    pub allocated: u32,
    pub stopped: u32,
    pub skipped_busy: u32,
    pub next_cursor: Option<ProviderMaintenanceCursor>,
}
pub(crate) fn provider_reply_error() -> ContractError {
    ContractError::Unavailable("invalid provider operation response".into())
}
