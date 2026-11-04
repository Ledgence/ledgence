//! Durable admission of standalone task entrypoint invocations.
//!
//! Capacity offers authorize preparation only. A successful first Start grants
//! one bounded invocation; replay never returns executable permission. Closing
//! an unused preparation fences the entire attempt against delayed Starts.
use crate::*;
use ledgence_worker_api::{Digest, ProgramDescriptor};
use sha2::{Digest as _, Sha256};

mod authority;
mod ledger;
mod ports;
pub use authority::*;
pub use ledger::*;
pub use ports::*;

/// CloudEvents integer extensions are signed 32-bit integers.
pub const MAX_TASK_ATTEMPT_GENERATION: u32 = i32::MAX as u32;

pub const TASK_ADMISSION_FEATURE: &str = "task_admission.v1";
pub const TASK_ADMISSION_DEFERRAL_BACKOFF_MS: u64 = 1_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdmissionRequirement {
    pub resource: String,
}
impl TaskAdmissionRequirement {
    pub fn validate(&self) -> Result<()> {
        crate::admission::name(&self.resource)
    }
}

/// Immutable identity for one assignment generation, including preparation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdmissionOccurrence {
    pub scope: Scope,
    pub task_id: String,
    pub generation: u32,
}
impl TaskAdmissionOccurrence {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        validate_text(&self.task_id, 128)?;
        if self.generation == 0 || self.generation > MAX_TASK_ATTEMPT_GENERATION {
            return Err(invalid("task admission generation must be positive"));
        }
        Ok(())
    }
    pub fn matches_owner(&self, owner: &LeaseOwner) -> bool {
        self.scope == owner.scope
            && self.task_id == owner.task_id
            && self.generation == owner.generation
    }
}
impl From<&LeaseOwner> for TaskAdmissionOccurrence {
    fn from(owner: &LeaseOwner) -> Self {
        Self {
            scope: owner.scope.clone(),
            task_id: owner.task_id.clone(),
            generation: owner.generation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdmissionBinding {
    pub occurrence: TaskAdmissionOccurrence,
    pub resource: String,
    pub program: ProgramDescriptor,
}
impl TaskAdmissionBinding {
    pub fn validate(&self) -> Result<()> {
        self.occurrence.validate()?;
        crate::admission::name(&self.resource)?;
        self.program.validate()?;
        Ok(())
    }
    pub fn digest(&self) -> Result<Digest> {
        self.validate()?;
        let bytes = canonical_json_bytes(
            &serde_json::to_value(self).map_err(|_| invalid("task admission binding encoding"))?,
        )?;
        Ok(Digest(format!("sha256:{:x}", Sha256::digest(bytes))))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdmissionSessionCommand {
    pub scope: Scope,
    pub queue: String,
    pub concurrency: u32,
}
impl TaskAdmissionSessionCommand {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        validate_text(&self.queue, 128)?;
        if self.concurrency == 0 {
            return Err(invalid("task admission session concurrency exceeds bounds"));
        }
        Ok(())
    }
    pub fn matches(&self, session: &WorkerSession) -> bool {
        session.scope == self.scope
            && session.queue == self.queue
            && session.concurrency == self.concurrency
            && session
                .features
                .iter()
                .any(|feature| feature == TASK_ADMISSION_FEATURE)
    }
}

/// Server-persisted preparation requirement, never an invocation permission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdmissionAssignment {
    pub offer_generation: u64,
    pub resource: String,
}
impl TaskAdmissionAssignment {
    pub fn validate(&self) -> Result<()> {
        crate::admission::positive(self.offer_generation)?;
        crate::admission::name(&self.resource)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAdmissionOffer {
    pub generation: u64,
    pub policy_revision: u64,
    pub expires_at: Timestamp,
}
impl TaskAdmissionOffer {
    pub fn validate(&self) -> Result<()> {
        crate::admission::positive(self.generation)?;
        crate::admission::positive(self.policy_revision)?;
        time(self.expires_at)
    }
}

pub(crate) fn is_zero(value: &u32) -> bool {
    *value == 0
}
pub fn task_retry_attempts(
    attempt_count: u32,
    admission_deferrals: u32,
    governed: bool,
) -> Result<u32> {
    if attempt_count > MAX_TASK_ATTEMPT_GENERATION {
        return Err(invalid(
            "task attempt generation exceeds CloudEvents integer range",
        ));
    }
    if !governed && admission_deferrals != 0 {
        return Err(invalid("ungoverned task has admission deferrals"));
    }
    attempt_count
        .checked_sub(admission_deferrals)
        .ok_or_else(|| invalid("task admission deferrals exceed attempts"))
}
fn time(value: u64) -> Result<()> {
    if value > 253_402_300_799_999 {
        return Err(invalid("task admission timestamp exceeds bounds"));
    }
    Ok(())
}
fn bound<T: Serialize>(value: &T) -> Result<()> {
    crate::submission::check_encoded_size(value, ADMISSION_RESPONSE_MAX_BYTES, "task admission")
        .map_err(Into::into)
}
fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}

#[cfg(test)]
#[path = "task_admission_tests.rs"]
mod tests;
