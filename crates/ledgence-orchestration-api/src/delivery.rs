use crate::*;
use ledgence_worker_api::{CloudEvent, ExecutionFailure, ExecutionReport, ProgramDescriptor};

/// UTC milliseconds since the Unix epoch, supplied by the authoritative store.
pub type Timestamp = u64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub tenant_id: String,
    pub namespace: String,
}
impl Scope {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.tenant_id, 128)?;
        validate_text(&self.namespace, 128)
    }
}

/// Origin and processing contexts are separate values, not interchangeable IDs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceContext {
    pub traceparent: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitCommand {
    pub idempotency_key: String,
    pub input: SubmitTask,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_trace: Option<TraceContext>,
}
impl SubmitCommand {
    /// Decode the portable command at a byte boundary. HTTP adapters may obtain
    /// its idempotency key from a header, but must use the same strict JSON path.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let command: Self = decode_unique_json(bytes, SUBMISSION_MAX_BYTES)?;
        command.input.validate()?;
        validate_text(&command.idempotency_key, 255)?;
        Ok(command)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Queued,
    Active,
    Succeeded,
    Failed,
    Cancelled,
}
impl TaskState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptState {
    Active,
    Succeeded,
    Failed,
    Cancelled,
    Lost,
}

/// Transaction-loaded scheduling record. Inputs and the descriptor are immutable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSnapshot {
    pub task_id: String,
    pub run_id: String,
    pub idempotency_key: String,
    pub input: SubmitTask,
    pub descriptor: ProgramDescriptor,
    pub origin_trace: Option<TraceContext>,
    pub state: TaskState,
    pub submitted_at: Timestamp,
    pub available_at: Timestamp,
    pub terminal_at: Option<Timestamp>,
    pub current_attempt_id: Option<String>,
    pub attempt_count: u32,
    pub cancel_requested_at: Option<Timestamp>,
}
impl TaskSnapshot {
    pub fn scope(&self) -> Scope {
        Scope {
            tenant_id: self.input.tenant_id.clone(),
            namespace: self.input.namespace.clone(),
        }
    }
}

/// Sessions are issued by the service and never recreated by acquisition.
/// Unknown/expired session IDs are rejected even after old cursor deletion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerSession {
    pub id: String,
    pub scope: Scope,
    pub queue: String,
    pub concurrency: u32,
    pub expires_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcquireCommand {
    pub scope: Scope,
    pub queue: String,
    pub worker_session_id: String,
    pub consumer_id: u32,
    pub sequence: u64,
}

/// Compact replay state: only the latest completed poll per consumer is retained.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerCursor {
    pub command: AcquireCommand,
    pub assignment: Option<AttemptRef>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptRef {
    pub task_id: String,
    pub attempt_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseOwner {
    pub scope: Scope,
    pub task_id: String,
    pub attempt_id: String,
    pub lease_id: String,
    pub generation: u32,
    pub worker_session_id: String,
    pub consumer_id: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub owner: LeaseOwner,
    pub expires_at: Timestamp,
}

/// Current authority sampled for this response; never replay a cached TTL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Authority {
    pub owner: LeaseOwner,
    pub expires_at: Timestamp,
    pub remaining_ms: u64,
    /// Remaining user-work budget; cleanup/reporting can retain a longer lease.
    pub execution_remaining_ms: u64,
    pub renew_sequence: u64,
    pub cancel_requested: bool,
    pub dispatch_allowed: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Assignment {
    pub descriptor: ProgramDescriptor,
    pub event: CloudEvent,
    pub lease: Lease,
    pub authority: Authority,
    pub attempt_deadline: Timestamp,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "disposition", rename_all = "snake_case")]
pub enum AcquireReply {
    Empty {
        sequence: u64,
    },
    Assigned {
        sequence: u64,
        assignment: Box<Assignment>,
    },
    OwnershipLost {
        sequence: u64,
        assignment: AttemptRef,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RenewIntent {
    KeepAlive,
    Dispatch,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RenewCommand {
    pub owner: LeaseOwner,
    pub sequence: u64,
    pub intent: RenewIntent,
}

/// Required invocation cleanup is done; a healthy warm process may still exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Quiescence {
    Confirmed,
    Unconfirmed,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "report", rename_all = "snake_case")]
pub enum AttemptReport {
    Completed(ExecutionReport),
    Failed(ExecutionFailure),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettleCommand {
    pub owner: LeaseOwner,
    pub operation_id: String,
    pub report: AttemptReport,
    pub quiescence: Quiescence,
    pub processing_trace: Option<TraceContext>,
}
impl SettleCommand {
    /// Preserve JSON numbers and reject duplicate keys before constructing a
    /// report. The lifecycle core also checks its identity against the attempt.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let command: Self = decode_unique_json(bytes, SETTLEMENT_MAX_BYTES)?;
        validate_text(&command.operation_id, 128)?;
        if let AttemptReport::Completed(report) = &command.report
            && let ledgence_worker_api::ProgramOutcome::Success { output } = &report.outcome
        {
            ledgence_worker_api::validate_wire_value(output)?;
        }
        Ok(command)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementReceipt {
    pub operation_id: String,
    pub task_id: String,
    pub attempt_id: String,
    pub accepted_at: Timestamp,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptedSettlement {
    pub command: SettleCommand,
    pub receipt: SettlementReceipt,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettleReply {
    pub receipt: SettlementReceipt,
    pub already_accepted: bool,
    /// A report can be recorded while required cleanup is still outstanding.
    pub task_state: TaskState,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttemptSnapshot {
    pub event: CloudEvent,
    pub descriptor: ProgramDescriptor,
    pub lease: Lease,
    pub deadline: Timestamp,
    pub authority_deadline: Timestamp,
    pub state: AttemptState,
    pub execution_may_have_started: bool,
    pub last_renewal: Option<RenewCommand>,
    pub quiescence: Quiescence,
    pub settlement: Option<AcceptedSettlement>,
    pub finished_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionReason {
    Submitted,
    Claimed,
    DispatchAuthorized,
    CancelRequested,
    Cancelled,
    ReportAccepted,
    CleanupConfirmed,
    Succeeded,
    Failed,
    RetryScheduled,
    LeaseExpired,
}
/// Persist with the accompanying records. Per-task ordering is assigned under
/// the same task lock; heartbeat renewals do not append history rows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEvent {
    pub task_id: String,
    pub attempt_id: Option<String>,
    pub at: Timestamp,
    pub reason: TransitionReason,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordedHistoryEvent {
    pub sequence: u64,
    pub event: HistoryEvent,
}

/// Service boundary implemented by future transport adapters. A successful
/// mutation reply is permitted only after durable transactional acceptance.
pub trait TaskService: Send + Sync {
    fn open_session<'a>(
        &'a self,
        scope: &'a Scope,
        queue: &'a str,
        concurrency: u32,
    ) -> ContractFuture<'a, WorkerSession>;
    fn extend_session<'a>(
        &'a self,
        worker_session_id: &'a str,
    ) -> ContractFuture<'a, WorkerSession>;
    fn submit<'a>(&'a self, command: &'a SubmitCommand) -> ContractFuture<'a, TaskSnapshot>;
    fn inspect<'a>(
        &'a self,
        scope: &'a Scope,
        task_id: &'a str,
    ) -> ContractFuture<'a, TaskSnapshot>;
    fn inspect_attempt<'a>(
        &'a self,
        scope: &'a Scope,
        task_id: &'a str,
        attempt_id: &'a str,
    ) -> ContractFuture<'a, AttemptSnapshot>;
    /// Return at most 100 ordered records after the supplied sequence. Large
    /// application outcomes are fetched through inspect_attempt, not history.
    fn history<'a>(
        &'a self,
        scope: &'a Scope,
        task_id: &'a str,
        after_sequence: u64,
    ) -> ContractFuture<'a, Vec<RecordedHistoryEvent>>;
    fn acquire<'a>(&'a self, command: &'a AcquireCommand) -> ContractFuture<'a, AcquireReply>;
    fn renew<'a>(&'a self, command: &'a RenewCommand) -> ContractFuture<'a, Authority>;
    fn settle<'a>(&'a self, command: &'a SettleCommand) -> ContractFuture<'a, SettleReply>;
    fn confirm_quiescence<'a>(&'a self, owner: &'a LeaseOwner) -> ContractFuture<'a, TaskState>;
    fn cancel<'a>(&'a self, scope: &'a Scope, task_id: &'a str) -> ContractFuture<'a, TaskState>;
}
