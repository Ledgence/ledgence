//! Portable contracts for task orchestration, resumable workflows and worker delivery.
//!
//! Submission, leases, durable settlement, completion callbacks, workflow events,
//! child tasks and retention describe transactional operations. They provide no
//! persistence or transport by themselves. Adapters must apply the full operation
//! before acknowledging it and preserve replay and ownership semantics.
//!
//! See the [delivery contract](https://github.com/Ledgence/ledgence/blob/main/docs/delivery-contract.md)
//! and [workflow contract](https://github.com/Ledgence/ledgence/blob/main/docs/workflows.md).

pub use ledgence_worker_api::TraceContext;

mod run_budget;
pub use run_budget::*;
mod queue_policies;
pub use queue_policies::*;
mod operator_audit;
pub use operator_audit::*;
mod access;
mod admission;
pub use access::*;
pub use admission::*;
mod acquisition;
mod completion;
pub mod console;
mod cursor;
mod delivery;
mod discovery;
pub mod instance;
pub use instance::*;
mod approval_inbox;
mod approved_effects;
mod dispatch;
mod effect_evidence;
mod effect_evidence_reads;
mod effect_resolution;
mod observation;
mod provider_admission;
mod provider_binding;
mod provider_operation;
mod provider_reads;
mod provider_wait;
mod retention;
mod storage;
mod submission;
mod task_admission;
mod workflow;
mod workflow_approvals;
mod workflow_children;
mod workflow_events;
mod workflow_forks;
pub use acquisition::*;
pub use approval_inbox::*;
pub use approved_effects::*;
pub use completion::*;
pub use delivery::*;
pub use discovery::*;
pub use dispatch::*;
pub use effect_evidence::*;
pub use effect_evidence_reads::*;
pub use effect_resolution::*;
pub use observation::*;
pub use provider_admission::*;
pub use provider_binding::*;
pub use provider_operation::*;
pub use provider_reads::*;
pub use provider_wait::*;
pub use retention::*;
pub use storage::*;
pub use submission::*;
pub use task_admission::*;
pub use workflow::*;
pub use workflow_approvals::*;
pub use workflow_children::*;
pub use workflow_events::*;
pub use workflow_forks::*;

use ledgence_worker_api::{Error, ErrorKind};
use serde::{Deserialize, Serialize};
use std::{fmt, future::Future, pin::Pin};

/// Expected operation rejection or an adapter failure. Backend errors must not
/// be translated into successful empty acquisitions or lost ownership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "code", content = "message", rename_all = "snake_case")]
pub enum ContractError {
    /// Missing, invalid or expired transport credentials. No operation was authorized.
    Unauthenticated,
    /// The verified principal lacks permission. No operation was authorized.
    Forbidden,
    InvalidInput(String),
    RunBudgetDenied(RunBudgetDenial),
    /// Integrated acquisition rejected because the queue requires external
    /// dispatch. This exact operation granted no authority and committed no
    /// consumer-cursor mutation. Existing completed sequences must replay before
    /// checking the route. Only a timely confirmed response permits stopping
    /// reconciliation; a transport timeout remains an unknown outcome.
    ExternalDispatchRequired,
    /// A queue receive positively returned invalid transport data, before any
    /// targeted claim or cursor mutation was issued. This stops new broker
    /// admission without acknowledging the record. Adapters must not use this
    /// for timeouts, network failures, or errors from claim/acknowledgment calls.
    InvalidQueueDelivery(String),
    Conflict,
    OwnershipLost,
    UnknownSession,
    SessionExpired,
    ObsoleteOperation,
    OutOfOrder,
    Busy,
    NotFound,
    Unavailable(String),
}
impl From<Error> for ContractError {
    fn from(error: Error) -> Self {
        Self::InvalidInput(error.to_string())
    }
}
impl fmt::Display for ContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ContractError {}
pub type Result<T> = std::result::Result<T, ContractError>;
pub type ContractFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Fixed-delay retry policy, including the initial attempt in `max_attempts`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub retry_delay_ms: u64,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            retry_delay_ms: 5_000,
        }
    }
}
impl RetryPolicy {
    pub fn validate(&self) -> ledgence_worker_api::Result<()> {
        if !(1..=1_000).contains(&self.max_attempts) || self.retry_delay_ms > 86_400_000 {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "retry policy exceeds supported limits",
            ));
        }
        Ok(())
    }
}

/// Initial server limits. All times are milliseconds; none is a concurrency knob.
pub const LEASE_DURATION_MS: u64 = 60_000;
pub const RENEW_INTERVAL_MS: u64 = 15_000;
pub const LEASE_SAFETY_MARGIN_MS: u64 = 5_000;
pub const LONG_POLL_WAIT_MS: u64 = 20_000;
pub const CONTROL_REQUEST_TIMEOUT_MS: u64 = 30_000;
pub const CLEANUP_GRACE_MS: u64 = 30_000;
pub const SESSION_VALIDITY_MS: u64 = 86_400_000;
pub const TERMINAL_RETENTION_MS: u64 = 90 * 86_400_000;
pub const SETTLEMENT_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Identifier/reference text stored in indexed platform columns.
pub fn validate_text(value: &str, maximum: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > maximum
        || value.chars().any(|character| {
            let code = u32::from(character);
            character.is_control() || (0xfdd0..=0xfdef).contains(&code) || (code & 0xfffe) == 0xfffe
        })
    {
        return Err(ContractError::InvalidInput(
            "invalid platform identifier/reference".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod provider_tests;
