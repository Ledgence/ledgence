//! Explicit, payload-free console observations. The browser never supplies Scope.
//!
//! These contracts are additive: existing SDK wire models retain their original
//! representations. Pagination is live keyset traversal, not a frozen snapshot.

mod catalog;
mod executions;
mod explorer;
mod measurements;
mod pagination;
mod query;
mod task;
mod workers;
mod workflow;
pub use catalog::*;
pub use executions::*;
pub use explorer::*;
pub use measurements::*;
pub use pagination::*;
pub use query::*;
pub use task::*;
pub use workers::*;
pub use workflow::*;

use crate::*;
use ledgence_worker_api::{Digest, ProgramDescriptor, ProgramRef};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Version 2 adds unified execution discovery and the durable workflow explorer.
/// Static assets and server must advertise the same contract version.
pub const CONSOLE_CONTRACT_VERSION: u32 = 2;
pub const CONSOLE_QUERY_MAX_BYTES: usize = 16 * 1024;
pub const CONSOLE_METADATA_MAX_BYTES: usize = 2 * 1024 * 1024;
pub const CONSOLE_MAX_TIMESTAMP: Timestamp = 253_402_300_799_999;

/// A full unsigned integer serialized as a canonical decimal string. Never sent
/// through a JavaScript Number, even when the current value happens to be small.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConsoleU64(pub u64);
impl Serialize for ConsoleU64 {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}
impl<'de> Deserialize<'de> for ConsoleU64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        let number = text.parse::<u64>().map_err(serde::de::Error::custom)?;
        if number.to_string() != text {
            return Err(serde::de::Error::custom(
                "expected canonical unsigned decimal string",
            ));
        }
        Ok(Self(number))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleProgramDescriptor {
    pub program: ProgramRef,
    pub digest: Digest,
    pub size: ConsoleU64,
}
impl From<ProgramDescriptor> for ConsoleProgramDescriptor {
    fn from(value: ProgramDescriptor) -> Self {
        Self {
            program: value.program,
            digest: value.digest,
            size: ConsoleU64(value.size),
        }
    }
}
impl ConsoleProgramDescriptor {
    pub fn validate(&self) -> Result<()> {
        ProgramDescriptor {
            program: self.program.clone(),
            digest: self.digest.clone(),
            size: self.size.0,
        }
        .validate()
        .map_err(Into::into)
    }
}

/// Small public configuration. Binding, database/store addresses and credentials
/// are deliberately absent from this type rather than redacted after encoding.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleConfig {
    pub contract_version: u32,
    pub server_version: String,
    pub instance_id: String,
    pub instance_name: String,
    pub capabilities: ConsoleCapabilities,
    pub suggested_queues: Vec<String>,
    pub limits: ConsoleLimits,
    pub polling: ConsolePolling,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleCapabilities {
    pub executions: bool,
    pub workflows: bool,
    pub programs: bool,
    pub workers: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleLimits {
    pub default_page_size: u32,
    pub max_page_size: u32,
    pub metadata_max_bytes: u32,
    pub submission_max_bytes: u32,
    pub input_max_bytes: u32,
    pub max_visible_workflow_nodes: u32,
    pub max_detailed_worker_slots: u32,
}
impl Default for ConsoleLimits {
    fn default() -> Self {
        Self {
            default_page_size: 50,
            max_page_size: 100,
            metadata_max_bytes: CONSOLE_METADATA_MAX_BYTES as u32,
            submission_max_bytes: SUBMISSION_MAX_BYTES as u32,
            input_max_bytes: SUBMISSION_DATA_MAX_BYTES as u32,
            max_visible_workflow_nodes: 100,
            max_detailed_worker_slots: 1024,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsolePolling {
    pub lists_ms: u32,
    pub active_task_ms: u32,
    pub waiting_workflow_ms: u32,
    pub workers_ms: u32,
    pub catalog_stale_ms: u32,
    pub worker_fresh_ms: u32,
    pub worker_recent_ms: u32,
}
impl Default for ConsolePolling {
    fn default() -> Self {
        Self {
            lists_ms: 5000,
            active_task_ms: 2000,
            waiting_workflow_ms: 5000,
            workers_ms: 5000,
            catalog_stale_ms: 60000,
            worker_fresh_ms: 15000,
            worker_recent_ms: 60000,
        }
    }
}

pub(super) fn timestamp(value: Timestamp) -> Result<()> {
    if value > CONSOLE_MAX_TIMESTAMP {
        return Err(invalid("console timestamp out of range"));
    }
    Ok(())
}
pub(super) fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}
pub(super) fn inconsistent(message: &str) -> ContractError {
    ContractError::Unavailable(message.into())
}
pub(super) fn metadata_size(value: &impl Serialize) -> Result<()> {
    crate::submission::check_encoded_size(value, CONSOLE_METADATA_MAX_BYTES, "console metadata")
        .map_err(Into::into)
}
