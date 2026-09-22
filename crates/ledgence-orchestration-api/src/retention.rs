//! Operator-triggered retention; no execution is expired merely by reading it.
use crate::{ContractError, ContractFuture, Result, Scope};
use serde::{Deserialize, Serialize};
use std::time::Instant;

pub const MIN_RETENTION_MS: u64 = 90 * 24 * 60 * 60 * 1000;
pub const MAX_RETENTION_BATCH: u32 = 256;

/// Applies to terminal executions and the latest terminal callback activity.
/// Existing retiring records continue physical collection under any later policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionPolicy {
    pub retain_for_ms: u64,
    /// Dependent-row work budget, not an execution count. Small task ledgers
    /// may share this budget across tables within their single-target transaction.
    pub batch_size: u32,
}
impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            retain_for_ms: MIN_RETENTION_MS,
            batch_size: 128,
        }
    }
}
impl RetentionPolicy {
    pub fn validate(&self) -> Result<()> {
        if !(MIN_RETENTION_MS..=253_402_300_799_999).contains(&self.retain_for_ms)
            || !(1..=MAX_RETENTION_BATCH).contains(&self.batch_size)
        {
            return Err(ContractError::InvalidInput(
                "retention requires at least 90 days and a batch size from 1 through 256".into(),
            ));
        }
        Ok(())
    }
}

/// One bounded transaction. Zero removed rows does not mean the database has no
/// retained records: protection, cursor rotation, or a new retirement can occur.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionProgress {
    pub examined: u32,
    pub retired: u32,
    pub deleted_rows: u32,
    pub deleted_executions: u32,
    pub deleted_sessions: u32,
}

/// Bounded age candidates only; protective references can defer collection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionPreview {
    pub task_candidates: Vec<String>,
    pub workflow_candidates: Vec<String>,
    pub retiring_tasks: Vec<String>,
    pub retiring_workflows: Vec<String>,
    pub expired_session_candidates: Vec<String>,
}

pub trait RetentionStore: Send + Sync {
    fn retention_preview<'a>(
        &'a self,
        scope: &'a Scope,
        policy: &'a RetentionPolicy,
        deadline: Instant,
    ) -> ContractFuture<'a, RetentionPreview>;

    /// Cooperating calls must preserve live cursors, active execution trees and
    /// unfinished delivery/reconciliation obligations. Crash/retry can repeat a
    /// batch; progress counts only the transaction acknowledged by this call.
    fn retain_batch<'a>(
        &'a self,
        scope: &'a Scope,
        policy: &'a RetentionPolicy,
        deadline: Instant,
    ) -> ContractFuture<'a, RetentionProgress>;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retention_cannot_shorten_the_documented_receipt_lifetime() {
        assert!(RetentionPolicy::default().validate().is_ok());
        for retain_for_ms in [0, MIN_RETENTION_MS - 1, u64::MAX] {
            assert!(
                RetentionPolicy {
                    retain_for_ms,
                    ..Default::default()
                }
                .validate()
                .is_err()
            );
        }
        for batch_size in [0, MAX_RETENTION_BATCH + 1, u32::MAX] {
            assert!(
                RetentionPolicy {
                    batch_size,
                    ..Default::default()
                }
                .validate()
                .is_err()
            );
        }
    }
}
