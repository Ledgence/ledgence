use super::*;
use ledgence_worker_api::{
    ProcessSlotState, WorkerObservationDetailState, WorkerObservationSnapshot,
};

pub const WORKER_OBSERVATION_MAX_BYTES: usize = 2 * 1024 * 1024;
/// The connected reporter supplies session identity; this is observation, never
/// authorization, session renewal, lease renewal or a durable task outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerObservationCommand {
    pub schema_version: u32,
    pub worker_session_id: String,
    pub scope: Scope,
    pub sequence: ConsoleU64,
    pub display_name: Option<String>,
    pub snapshot: WorkerObservationSnapshot,
}
impl WorkerObservationCommand {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.worker_session_id, 128)?;
        self.scope.validate()?;
        if let Some(name) = &self.display_name {
            validate_text(name, 128)?;
        }
        let s = &self.snapshot;
        if self.schema_version != 1
            || self.sequence.0 == 0
            || s.configured_concurrency == 0
            || s.configured_concurrency > u32::MAX as usize
            || s.active_consumers > s.configured_concurrency
            || s.occupied_process_slots > s.configured_concurrency
        {
            return Err(invalid("invalid worker observation summary"));
        }
        match s.detail_state {
            WorkerObservationDetailState::Available => {
                if s.configured_concurrency > 1024 || s.slots.len() != s.configured_concurrency {
                    return Err(invalid("worker observation must include every slot"));
                }
                let mut ids = std::collections::BTreeSet::new();
                let mut process_ids = std::collections::BTreeSet::new();
                let mut occupied = 0;
                for slot in &s.slots {
                    if slot.slot_id >= s.configured_concurrency || !ids.insert(slot.slot_id) {
                        return Err(invalid("invalid or duplicate worker slot"));
                    }
                    if slot.state != ProcessSlotState::Empty {
                        occupied += 1;
                    }
                    if let Some(id) = &slot.process_instance_id {
                        validate_text(id, 128)?;
                        if !process_ids.insert(id) {
                            return Err(invalid("duplicate process identity"));
                        }
                    }
                    if let Some(program) = &slot.program {
                        program.validate()?;
                    }
                    if let Some(digest) = &slot.digest {
                        digest.validate()?;
                    }
                    if let Some(scope) = &slot.scope
                        && (scope.tenant_id != self.scope.tenant_id
                            || scope.namespace != self.scope.namespace)
                    {
                        return Err(invalid("worker observation contains a different binding"));
                    }
                    if let Some(invocation) = &slot.invocation {
                        validate_text(&invocation.task_id, 128)?;
                        validate_text(&invocation.attempt_id, 128)?;
                    }
                    if slot.program.is_some() != slot.digest.is_some()
                        || (slot.state == ProcessSlotState::Empty
                            && (slot.process_instance_id.is_some()
                                || slot.process_id.is_some()
                                || slot.program.is_some()
                                || slot.scope.is_some()
                                || slot.invocation.is_some()))
                        || (slot.state == ProcessSlotState::Warm
                            && (slot.invocation.is_some()
                                || slot.process_instance_id.is_none()
                                || slot.program.is_none()
                                || slot.scope.is_none()))
                        || (slot.state == ProcessSlotState::Executing
                            && (slot.invocation.is_none()
                                || slot.process_instance_id.is_none()
                                || slot.program.is_none()
                                || slot.scope.is_none()))
                    {
                        return Err(invalid("inconsistent worker slot state"));
                    }
                }
                if occupied != s.occupied_process_slots {
                    return Err(invalid("worker occupied slot count differs from details"));
                }
            }
            WorkerObservationDetailState::UnsupportedCapacity => {
                if s.configured_concurrency <= 1024 || !s.slots.is_empty() {
                    return Err(invalid("invalid unsupported worker capacity"));
                }
            }
            WorkerObservationDetailState::Unavailable => {
                if !s.slots.is_empty() {
                    return Err(invalid("unavailable detail cannot contain partial slots"));
                }
            }
        }
        crate::submission::check_encoded_size(
            self,
            WORKER_OBSERVATION_MAX_BYTES,
            "worker observation",
        )?;
        Ok(())
    }
    /// Slot order is observational, not semantic. Normalization makes reordered
    /// exact retransmissions idempotent without moving their server receipt time.
    pub fn normalized_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut normalized = self.clone();
        normalized.snapshot.slots.sort_by_key(|s| s.slot_id);
        Ok(canonical_json_bytes(
            &serde_json::to_value(normalized).map_err(|_| invalid("invalid worker observation"))?,
        )?)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerObservationReceipt {
    pub worker_session_id: String,
    pub sequence: ConsoleU64,
    pub received_at: Timestamp,
    pub already_received: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerObservationFreshness {
    Fresh,
    Stale,
    NoRecentReport,
    Unavailable,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleWorkerSummary {
    pub worker_session_id: String,
    pub display_name: Option<String>,
    pub queue: String,
    pub capacity: u32,
    pub session_expires_at: Timestamp,
    pub session_expired: bool,
    pub snapshot_sequence: Option<ConsoleU64>,
    pub received_at: Option<Timestamp>,
    pub accepting: Option<bool>,
    pub active_consumers: Option<u32>,
    pub occupied_process_slots: Option<u32>,
    pub detail_state: Option<WorkerObservationDetailState>,
    pub freshness: WorkerObservationFreshness,
}
impl ConsoleWorkerSummary {
    pub fn validate_at(&self, observed_at: Timestamp) -> Result<()> {
        self.validate()?;
        timestamp(observed_at)?;
        if self.session_expired != (self.session_expires_at <= observed_at) {
            return Err(invalid("inconsistent worker session expiry"));
        }
        let expected = match self.received_at {
            None => WorkerObservationFreshness::Unavailable,
            Some(at) => {
                if at > observed_at {
                    return Err(invalid("worker receipt is after observation"));
                }
                match observed_at - at {
                    0..=15000 => WorkerObservationFreshness::Fresh,
                    15001..=60000 => WorkerObservationFreshness::Stale,
                    _ => WorkerObservationFreshness::NoRecentReport,
                }
            }
        };
        if self.freshness != expected {
            return Err(invalid("inconsistent worker freshness"));
        }
        Ok(())
    }
}
impl ConsoleRecord for ConsoleWorkerSummary {
    fn position(&self) -> ConsolePosition {
        vec![ConsoleKey::Text(self.worker_session_id.clone())]
    }
    fn validate(&self) -> Result<()> {
        validate_text(&self.worker_session_id, 128)?;
        validate_text(&self.queue, 128)?;
        timestamp(self.session_expires_at)?;
        if let Some(name) = &self.display_name {
            validate_text(name, 128)?;
        }
        if let Some(at) = self.received_at {
            timestamp(at)?;
        }
        let reported = self.snapshot_sequence.is_some();
        if self.capacity == 0
            || [
                self.received_at.is_some(),
                self.accepting.is_some(),
                self.active_consumers.is_some(),
                self.occupied_process_slots.is_some(),
                self.detail_state.is_some(),
            ]
            .into_iter()
            .any(|v| v != reported)
            || self.active_consumers.is_some_and(|v| v > self.capacity)
            || self
                .occupied_process_slots
                .is_some_and(|v| v > self.capacity)
        {
            return Err(invalid("inconsistent worker summary"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerLinkDiagnostic {
    AuthorityMismatch,
    Unavailable,
}
/// Public projection deliberately excludes internal binding. A mismatched task
/// reference remains observable as a bounded diagnostic, without a clickable ID.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleWorkerSlot {
    pub slot_id: u32,
    pub state: ProcessSlotState,
    pub process_instance_id: Option<String>,
    pub process_id: Option<u32>,
    pub program: Option<ProgramRef>,
    pub digest: Option<Digest>,
    pub task_id: Option<String>,
    pub attempt_id: Option<String>,
    pub consumer_id: Option<u32>,
    pub link_diagnostic: Option<WorkerLinkDiagnostic>,
}
impl ConsoleRecord for ConsoleWorkerSlot {
    fn position(&self) -> ConsolePosition {
        vec![ConsoleKey::Number(ConsoleU64(u64::from(self.slot_id)))]
    }
    fn validate(&self) -> Result<()> {
        if self.slot_id >= 1024 {
            return Err(invalid("invalid detailed worker slot"));
        }
        if let Some(id) = &self.process_instance_id {
            validate_text(id, 128)?;
        }
        if let Some(p) = &self.program {
            p.validate()?;
        }
        if let Some(d) = &self.digest {
            d.validate()?;
        }
        for id in [&self.task_id, &self.attempt_id].into_iter().flatten() {
            validate_text(id, 128)?;
        }
        if self.program.is_some() != self.digest.is_some()
            || self.task_id.is_some() != self.attempt_id.is_some()
            || (self.link_diagnostic.is_some()
                && (self.task_id.is_some() || self.consumer_id.is_some()))
        {
            return Err(invalid("inconsistent worker slot links"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleWorkerDetail {
    pub worker: ConsoleWorkerSummary,
    pub slots: ConsolePage<ConsoleWorkerSlot>,
}
#[derive(Debug, Clone)]
pub enum WorkerObservationQuery {
    Workers {
        queue: Option<String>,
        page: ConsolePagination,
    },
    Inspect {
        worker_session_id: String,
        page: ConsolePagination,
    },
}
impl WorkerObservationQuery {
    pub fn page(&self) -> &ConsolePagination {
        match self {
            Self::Workers { page, .. } | Self::Inspect { page, .. } => page,
        }
    }
    pub fn binding(&self, scope: &Scope) -> Result<ConsoleCursorBinding> {
        scope.validate()?;
        let (endpoint, parent, filters, numeric_keys) = match self {
            Self::Workers { queue, .. } => {
                if let Some(q) = queue {
                    validate_text(q, 128)?;
                }
                (
                    "workers",
                    vec![],
                    serde_json::json!({"queue":queue}),
                    vec![false],
                )
            }
            Self::Inspect {
                worker_session_id, ..
            } => {
                validate_text(worker_session_id, 128)?;
                (
                    "workers/inspect",
                    vec![worker_session_id.clone()],
                    serde_json::Value::Null,
                    vec![true],
                )
            }
        };
        Ok(ConsoleCursorBinding {
            endpoint,
            scope: scope.clone(),
            parent,
            filters,
            descending: false,
            numeric_keys,
        })
    }
    pub fn validate(&self, scope: &Scope) -> Result<Option<ConsolePosition>> {
        self.page().validate(&self.binding(scope)?)
    }
}
#[derive(Debug, Clone)]
pub enum WorkerObservationReply {
    Workers(ConsolePage<ConsoleWorkerSummary>),
    Inspect(Box<ConsoleWorkerDetail>),
}
impl WorkerObservationReply {
    pub fn validate(&self, scope: &Scope, query: &WorkerObservationQuery) -> Result<()> {
        query.validate(scope)?;
        let binding = query.binding(scope)?;
        let mismatch = || inconsistent("inconsistent worker observation");
        match (self, query) {
            (Self::Workers(reply), WorkerObservationQuery::Workers { queue, page }) => {
                reply.validate(page, &binding)?;
                for worker in &reply.items {
                    worker
                        .validate_at(reply.observed_at)
                        .map_err(|_| mismatch())?;
                    if queue.as_ref().is_some_and(|q| q != &worker.queue) {
                        return Err(mismatch());
                    }
                }
                Ok(())
            }
            (
                Self::Inspect(reply),
                WorkerObservationQuery::Inspect {
                    worker_session_id,
                    page,
                },
            ) => {
                reply.slots.validate(page, &binding)?;
                reply
                    .worker
                    .validate_at(reply.slots.observed_at)
                    .map_err(|_| mismatch())?;
                if &reply.worker.worker_session_id != worker_session_id
                    || reply
                        .slots
                        .items
                        .iter()
                        .any(|slot| slot.slot_id >= reply.worker.capacity)
                    || (reply.worker.detail_state != Some(WorkerObservationDetailState::Available)
                        && (!reply.slots.items.is_empty() || reply.slots.next_cursor.is_some()))
                {
                    return Err(mismatch());
                }
                metadata_size(reply).map_err(|_| mismatch())
            }
            _ => Err(mismatch()),
        }
    }
}
pub trait WorkerObservationStore: Send + Sync {
    fn record_observation<'a>(
        &'a self,
        command: &'a WorkerObservationCommand,
    ) -> ContractFuture<'a, WorkerObservationReceipt>;
    fn query_workers<'a>(
        &'a self,
        scope: &'a Scope,
        query: &'a WorkerObservationQuery,
    ) -> ContractFuture<'a, WorkerObservationReply>;
}
pub trait WorkerObservationPublisher: Send + Sync {
    fn publish_observation<'a>(
        &'a self,
        command: &'a WorkerObservationCommand,
    ) -> ContractFuture<'a, WorkerObservationReceipt>;
}
pub trait WorkerObservationService: WorkerObservationPublisher {
    fn query_workers<'a>(
        &'a self,
        query: &'a WorkerObservationQuery,
    ) -> ContractFuture<'a, WorkerObservationReply>;
}
