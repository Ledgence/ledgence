//! Portable contracts for one-task orchestration and worker delivery.
//!
//! These types describe transactional operations; they provide no persistence
//! or transport by themselves. Adapters must apply the full operation before
//! acknowledging it. See `docs/delivery-contract.md` for replay and retention.

pub use ledgence_worker_api::TraceContext;

mod acquisition;
mod delivery;
mod storage;
mod submission;
pub use acquisition::*;
pub use delivery::*;
pub use storage::*;
pub use submission::*;

use ledgence_worker_api::{Error, ErrorKind};
use serde::{Deserialize, Serialize};
use std::{fmt, future::Future, pin::Pin};

/// Expected operation rejection or an adapter failure. Backend errors must not
/// be translated into successful empty acquisitions or lost ownership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "code", content = "message", rename_all = "snake_case")]
pub enum ContractError {
    InvalidInput(String),
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
