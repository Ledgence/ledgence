//! Atomic persistence operations used by the application service.
//!
//! Implementations own their transaction boundaries, authoritative clock, and
//! generated identities. A successful mutation reply follows durable commit of
//! all affected records and history. A database failure is never an empty queue
//! or proof that ownership was lost; an uncertain commit requires reconciliation
//! using the original operation identity.

use crate::*;
use ledgence_worker_api::ProgramDescriptor;

/// Persistence boundary for complete single-task lifecycle operations.
///
/// Adapters load related records consistently, invoke the lifecycle core under
/// the required locks, and atomically persist its complete transition. These
/// operations must not perform external program resolution inside transactions.
pub trait TaskStore: Send + Sync {
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
    /// Read an already accepted submission before contacting the program store.
    fn lookup_submission<'a>(
        &'a self,
        scope: &'a Scope,
        idempotency_key: &'a str,
    ) -> ContractFuture<'a, Option<TaskSnapshot>>;
    /// Atomically accept this binding or replay the concurrently accepted winner.
    ///
    /// Scoped submission-key uniqueness is authoritative. A matching winner's
    /// input, descriptor, and origin context remain unchanged; different
    /// normalized input conflicts. The descriptor supplied by a losing caller
    /// must never replace the accepted one.
    fn accept_resolved_submission<'a>(
        &'a self,
        command: &'a SubmitCommand,
        descriptor: &'a ProgramDescriptor,
    ) -> ContractFuture<'a, TaskSnapshot>;
    /// Read one bounded page of matching committed task statuses in descending
    /// submission-time/task-ID order. Each page has its own read snapshot.
    fn list_tasks<'a>(
        &'a self,
        scope: &'a Scope,
        query: &'a TaskListQuery,
    ) -> ContractFuture<'a, TaskPage>;
    /// Read compact scheduling metadata without application payloads.
    fn status<'a>(&'a self, scope: &'a Scope, task_id: &'a str) -> ContractFuture<'a, TaskStatus>;
    /// Read task metadata and its logical outcome from one consistent snapshot.
    fn result<'a>(&'a self, scope: &'a Scope, task_id: &'a str) -> ContractFuture<'a, TaskResult>;
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
    /// Read at most 100 ordered history records after the supplied sequence.
    fn history<'a>(
        &'a self,
        scope: &'a Scope,
        task_id: &'a str,
        after_sequence: u64,
    ) -> ContractFuture<'a, Vec<RecordedHistoryEvent>>;
    /// Probe under short atomic storage locks. Pending must roll back every
    /// mutation and release its connection before returning. The deadline bounds
    /// connection admission, contention, retries and commit acknowledgement.
    fn probe_acquisition<'a>(
        &'a self,
        command: &'a AcquireCommand,
        finish_empty: bool,
        deadline: std::time::Instant,
    ) -> ContractFuture<'a, AcquisitionProbe>;

    /// Immediate completion convenience for storage consumers and adapter tests.
    fn acquire<'a>(&'a self, command: &'a AcquireCommand) -> ContractFuture<'a, AcquireReply> {
        Box::pin(async move {
            let deadline = std::time::Instant::now()
                + std::time::Duration::from_millis(CONTROL_REQUEST_TIMEOUT_MS);
            match self.probe_acquisition(command, true, deadline).await? {
                AcquisitionProbe::Completed { reply, .. } => Ok(reply),
                AcquisitionProbe::Pending { .. } => Err(ContractError::Unavailable(
                    "store returned Pending from final acquisition probe".into(),
                )),
            }
        })
    }
    fn renew<'a>(&'a self, command: &'a RenewCommand) -> ContractFuture<'a, Authority>;
    fn settle<'a>(&'a self, command: &'a SettleCommand) -> ContractFuture<'a, SettleReply>;
    fn confirm_quiescence<'a>(&'a self, owner: &'a LeaseOwner) -> ContractFuture<'a, TaskState>;
    fn cancel<'a>(&'a self, scope: &'a Scope, task_id: &'a str) -> ContractFuture<'a, TaskState>;
}

/// Maximum number of task candidates shortlisted by one recovery operation.
pub const MAX_RECOVERY_BATCH: u32 = 100;

/// Committed progress from one bounded recovery operation.
///
/// Counts do not imply the expired queue is exhausted: other candidates may
/// remain, including locked tasks skipped by this operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryProgress {
    /// Shortlisted candidates examined, including candidates subsequently skipped.
    pub examined: u32,
    /// Expiry transitions successfully committed.
    pub expired: u32,
}

/// Internal maintenance boundary, independent of worker/client delivery calls.
pub trait RecoveryStore: Send + Sync {
    /// Recover at most `limit` candidates, where `1 <= limit <= MAX_RECOVERY_BATCH`.
    ///
    /// Each candidate is rechecked with authoritative time under its task lock.
    /// Repeated/concurrent scans must not duplicate finalization or history.
    /// Query duration is bounded separately from the shortlist size. A later
    /// error may follow already committed task transitions; retries are safe.
    fn expire_batch(&self, limit: u32) -> ContractFuture<'_, RecoveryProgress>;
}
