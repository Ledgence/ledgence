//! Acquisition over integrated delivery or an individually acknowledged broker.

use ledgence_orchestration_api::*;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;

/// Idle transport polling does not consume a durable acquisition sequence.
#[derive(Debug, Clone)]
pub enum SourceReply {
    Idle,
    Completed(AcquireReply),
    /// Durable nonauthority handoff consumes its sequence without idle pacing.
    Discarded {
        sequence: u64,
    },
    /// Fail-stop before any uncertain claim for this sequence. The broker record
    /// remains unacknowledged; this is not durable quarantine or task settlement.
    Stopped {
        error: ContractError,
    },
}

/// Supply acquisition while the delivery driver retains its existing ownership,
/// execution, renewal, settlement, and cleanup responsibilities. Implementations
/// must retain selected claim identity across cancellation of an acquire future.
pub trait AcquisitionSource: Send + Sync {
    fn start_session(&self, _session: &WorkerSession) -> Result<()> {
        Ok(())
    }
    fn acquire<'a>(
        &'a self,
        command: &'a AcquireCommand,
        options: AcquireOptions,
    ) -> ContractFuture<'a, SourceReply>;
    /// Resolve an acquisition during shutdown. The default keeps reconciling
    /// through `acquire`, since an uncertain exchange may have committed a claim.
    /// A source may return Idle only when no claim can have started and any
    /// already-issued transport receive has finished or reached its deadline.
    fn drain<'a>(
        &'a self,
        command: &'a AcquireCommand,
        options: AcquireOptions,
    ) -> ContractFuture<'a, SourceReply> {
        self.acquire(command, options)
    }
    /// Called only after every consumer has stopped. No new claims may start;
    /// task state remains responsible for recovery of lost session authority.
    fn finish_session(&self, _worker_session_id: &str) {}
}

pub(crate) struct ServiceAcquisitionSource {
    pub service: Arc<dyn TaskService>,
}
impl AcquisitionSource for ServiceAcquisitionSource {
    fn acquire<'a>(
        &'a self,
        command: &'a AcquireCommand,
        options: AcquireOptions,
    ) -> ContractFuture<'a, SourceReply> {
        Box::pin(async move {
            match self.service.acquire(command, options).await {
                Ok(reply) => Ok(SourceReply::Completed(reply)),
                Err(error @ ContractError::ExternalDispatchRequired) => {
                    Ok(SourceReply::Stopped { error })
                }
                Err(error) => Err(error),
            }
        })
    }
}

/// Shares one broker adapter across exactly N logical consumer slots. Each slot
/// receives at most one record at a time; publication can be batched independently.
/// A retained slot contains copied transport/claim identities, never references
/// to worker ownership, processes, execution sessions, or program artifacts.
pub struct BrokerAcquisitionSource {
    queue: Arc<dyn AckQueue>,
    service: Arc<dyn TaskService>,
    limits: QueueLimits,
    registry: Mutex<Registry>,
}
#[derive(Default)]
struct Registry {
    session: Option<WorkerSession>,
    slots: Vec<Arc<AsyncMutex<Slot>>>,
    stopped: Option<ContractError>,
}
struct Slot {
    sequence: u64,
    /// Owned transport I/O survives cancellation of the outer acquire future.
    /// It retains its original deadline and never overlaps a second receive.
    receiving: Option<ContractFuture<'static, Vec<QueueDelivery>>>,
    pending: Option<Pending>,
}
struct Pending {
    record: PublishedDispatch,
    command: ClaimCommand,
    receipt: String,
    /// Set before the first claim future is awaited; cancellation never resets it.
    claim_started: bool,
    /// Durable handoff was observed, whether or not the caller received our reply.
    completed: bool,
    /// Retained before awaiting acknowledgment, including cancellation or panic.
    /// A later reconciliation refreshes the claim but cannot be held by delete again.
    ack_attempted: bool,
}
impl BrokerAcquisitionSource {
    pub fn new(queue: Arc<dyn AckQueue>, service: Arc<dyn TaskService>) -> Result<Self> {
        let limits = queue.limits();
        limits.validate()?;
        Ok(Self {
            queue,
            service,
            limits,
            registry: Mutex::new(Registry::default()),
        })
    }

    fn slot(&self, command: &AcquireCommand) -> Result<Arc<AsyncMutex<Slot>>> {
        let registry = self.registry.lock().unwrap_or_else(|p| p.into_inner());
        let session = registry.session.as_ref().ok_or_else(|| {
            ContractError::InvalidInput("broker source has no active worker session".into())
        })?;
        if command.worker_session_id != session.id
            || command.scope != session.scope
            || command.queue != session.queue
            || command.consumer_id >= session.concurrency
            || command.sequence == 0
        {
            return Err(ContractError::InvalidInput(
                "broker acquisition does not match its registered consumer".into(),
            ));
        }
        Ok(registry.slots[command.consumer_id as usize].clone())
    }

    fn stopped(&self) -> Option<ContractError> {
        self.registry
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .stopped
            .clone()
    }

    fn stop(&self, message: &str) -> SourceReply {
        self.stop_error(ContractError::InvalidInput(message.into()))
    }

    fn stop_error(&self, error: ContractError) -> SourceReply {
        let mut registry = self.registry.lock().unwrap_or_else(|p| p.into_inner());
        let error = registry.stopped.get_or_insert(error).clone();
        SourceReply::Stopped { error }
    }

    async fn acquire_once(
        &self,
        command: &AcquireCommand,
        options: AcquireOptions,
        draining: bool,
    ) -> Result<SourceReply> {
        options.validate()?;
        let slot = self.slot(command)?;
        let mut slot = slot.lock().await;
        if command.sequence != slot.sequence {
            if slot.sequence.checked_add(1) == Some(command.sequence)
                && slot
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.completed)
            {
                slot.pending = None;
                slot.sequence = command.sequence;
            } else {
                return Err(ContractError::InvalidInput(
                    "broker acquisition sequence changed before durable completion".into(),
                ));
            }
        }
        if let Some(pending) = &slot.pending
            && pending.command.acquisition != *command
        {
            return Err(ContractError::Conflict);
        }
        if let Some(error) = self.stopped()
            && slot.receiving.is_none()
            && !slot
                .pending
                .as_ref()
                .is_some_and(|pending| pending.claim_started)
        {
            return Ok(SourceReply::Stopped { error });
        }
        if draining
            && slot.receiving.is_none()
            && !slot
                .pending
                .as_ref()
                .is_some_and(|pending| pending.claim_started)
        {
            return Ok(SourceReply::Idle);
        }
        if slot.pending.is_none() {
            if slot.receiving.is_none() {
                let queue = self.queue.clone();
                let wait = options.max_wait;
                let deadline = options.deadline;
                slot.receiving = Some(Box::pin(async move {
                    super::exchange_until(deadline, || queue.receive(1, wait, deadline)).await
                }));
            }
            let deliveries = slot.receiving.as_mut().expect("receive retained").await;
            slot.receiving = None;
            let deliveries = match deliveries {
                Err(error @ ContractError::InvalidQueueDelivery(_)) => {
                    return Ok(self.stop_error(error));
                }
                result => result?,
            };
            if draining {
                // No task claim has started. Leave any returned records
                // unacknowledged for broker redelivery and durable intent repair.
                return Ok(SourceReply::Idle);
            }
            if deliveries.len() > 1 {
                return Ok(self.stop("broker returned more records than the reserved capacity"));
            }
            let Some(delivery) = deliveries.into_iter().next() else {
                return Ok(SourceReply::Idle);
            };
            if delivery.validate(self.limits).is_err() {
                return Ok(self.stop("broker record or receipt exceeds configured bounds; record remains unacknowledged"));
            }
            let record = match PublishedDispatch::decode(&delivery.body) {
                Ok(record) => record,
                Err(_) => {
                    return Ok(
                        self.stop("malformed broker dispatch; record remains unacknowledged")
                    );
                }
            };
            let claim = ClaimCommand {
                acquisition: command.clone(),
                dispatch: record.dispatch.clone(),
            };
            if claim.validate().is_err() {
                return Ok(self.stop("broker dispatch does not match the worker route; record remains unacknowledged"));
            }
            // Save before polling any operation that could commit task authority.
            slot.pending = Some(Pending {
                record,
                command: claim,
                receipt: delivery.receipt,
                claim_started: false,
                completed: false,
                ack_attempted: false,
            });
        }
        let pending = slot.pending.as_mut().expect("selected record retained");
        pending.claim_started = true;
        // Reconcile even an already observed reply through the original claim:
        // replayed authority needs current deadlines, never a cached lease TTL.
        let reply = self.service.claim_dispatch(&pending.command).await?;
        reply.validate_reply_against(&pending.command)?;
        pending.completed = true;
        if !pending.ack_attempted {
            // Save before constructing or polling adapter I/O. If this await
            // consumes the acquisition deadline, the next call must refresh
            // authority and return without repeating a full-budget delete.
            pending.ack_attempted = true;
            let receipts = [pending.receipt.clone()];
            match self.queue.acknowledge(&receipts, options.deadline).await {
                Ok(results)
                    if results.len() == 1
                        && results[0].receipt == pending.receipt
                        && results[0].confirmed => {}
                Ok(_) => tracing::warn!(
                    task_id = %pending.command.dispatch.task_id,
                    publication_id = %pending.record.publication_id,
                    "broker acknowledgment unconfirmed after durable handoff"
                ),
                Err(error) => tracing::warn!(
                    task_id = %pending.command.dispatch.task_id,
                    publication_id = %pending.record.publication_id,
                    error = %error,
                    "broker acknowledgment failed after durable handoff"
                ),
            }
        }
        // An acknowledgment outage must not prevent execution of a task whose
        // recovery is already durable. Redelivery is reconciled by task identity.
        let reply = match reply.disposition {
            ClaimDisposition::Claimed { reply } => SourceReply::Completed(reply),
            ClaimDisposition::AlreadyHandedOff { .. }
            | ClaimDisposition::TerminalOrSuperseded
            | ClaimDisposition::Deferred { .. } => SourceReply::Discarded {
                sequence: command.sequence,
            },
        };
        Ok(reply)
    }
}
impl AcquisitionSource for BrokerAcquisitionSource {
    fn start_session(&self, session: &WorkerSession) -> Result<()> {
        session.scope.validate()?;
        validate_text(&session.queue, 128)?;
        validate_text(&session.id, 128)?;
        if session.concurrency == 0 {
            return Err(ContractError::InvalidInput(
                "broker session needs consumers".into(),
            ));
        }
        let mut registry = self.registry.lock().unwrap_or_else(|p| p.into_inner());
        if registry.session.is_some() {
            return Err(ContractError::InvalidInput(
                "broker source already has a worker session".into(),
            ));
        }
        registry.slots = (0..session.concurrency)
            .map(|_| {
                Arc::new(AsyncMutex::new(Slot {
                    sequence: 1,
                    receiving: None,
                    pending: None,
                }))
            })
            .collect();
        registry.session = Some(session.clone());
        registry.stopped = None;
        Ok(())
    }

    fn acquire<'a>(
        &'a self,
        command: &'a AcquireCommand,
        options: AcquireOptions,
    ) -> ContractFuture<'a, SourceReply> {
        Box::pin(self.acquire_once(command, options, false))
    }

    fn drain<'a>(
        &'a self,
        command: &'a AcquireCommand,
        options: AcquireOptions,
    ) -> ContractFuture<'a, SourceReply> {
        Box::pin(self.acquire_once(command, options, true))
    }

    fn finish_session(&self, worker_session_id: &str) {
        let mut registry = self.registry.lock().unwrap_or_else(|p| p.into_inner());
        if registry
            .session
            .as_ref()
            .is_some_and(|session| session.id == worker_session_id)
        {
            *registry = Registry::default();
        }
    }
}
