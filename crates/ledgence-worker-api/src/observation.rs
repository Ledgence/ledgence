//! Readonly local process ownership, separate from delivery authority.
//!
//! Process slots and consumers are independent resources governed by one capacity.
//! A reporter supplies session identity and freshness. A snapshot does not renew
//! leases or certify a task's durable result.

use crate::{Digest, ProgramRef};
use serde::{Deserialize, Serialize};

/// Larger embedder pools retain their capacity but report summaries only.
pub const WORKER_OBSERVATION_MAX_SLOTS: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerObservationSnapshot {
    pub configured_concurrency: usize,
    pub accepting: bool,
    /// Reserved consumers, including acquisition, preparation and settlement.
    pub active_consumers: usize,
    pub occupied_process_slots: usize,
    pub detail_state: WorkerObservationDetailState,
    /// All slots including empty entries, or no entries when detail is unavailable.
    pub slots: Vec<SlotObservation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerObservationDetailState {
    Available,
    UnsupportedCapacity,
    /// A reporter cannot supply a complete bounded snapshot; never truncation.
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessSlotState {
    Empty,
    Starting,
    Warm,
    Executing,
    Retiring,
    CleanupPending,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerObservationScope {
    pub tenant_id: String,
    pub namespace: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationObservation {
    pub task_id: String,
    pub attempt_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlotObservation {
    /// Stable within one Worker lifetime; a connected session namespaces this ID.
    pub slot_id: usize,
    pub state: ProcessSlotState,
    /// New per startup and retained through reuse; never derived from PID.
    pub process_instance_id: Option<String>,
    pub process_id: Option<u32>,
    pub program: Option<ProgramRef>,
    pub digest: Option<Digest>,
    pub scope: Option<WorkerObservationScope>,
    /// Only the currently attached invocation, never a previous warm reuse.
    pub invocation: Option<InvocationObservation>,
}
