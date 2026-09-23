//! Local acquisition budgets and optional advisory wake signals.

use crate::*;
use std::time::{Duration, Instant};

/// A wait preference and the enclosing local exchange deadline. Neither is part
/// of durable acquisition identity; only the nonzero wait preference is sent
/// over a transport. Every caller must keep its original deadline across probes.
#[derive(Debug, Clone, Copy)]
pub struct AcquireOptions {
    pub max_wait: Duration,
    pub deadline: Instant,
}
impl AcquireOptions {
    pub fn new(max_wait: Duration, deadline: Instant) -> Result<Self> {
        let options = Self { max_wait, deadline };
        options.validate()?;
        Ok(options)
    }

    pub fn immediate(deadline: Instant) -> Self {
        Self {
            max_wait: Duration::ZERO,
            deadline,
        }
    }

    pub fn for_wait(max_wait: Duration) -> Result<Self> {
        Self::new(
            max_wait,
            Instant::now() + Duration::from_millis(CONTROL_REQUEST_TIMEOUT_MS),
        )
    }

    pub fn validate(&self) -> Result<()> {
        if self.max_wait > Duration::from_millis(LONG_POLL_WAIT_MS) {
            return Err(ContractError::InvalidInput(
                "acquisition wait exceeds 20000 ms".into(),
            ));
        }
        Ok(())
    }
}

/// Internal provenance prevents replayed assignments from implying more backlog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquisitionCompletion {
    Replayed,
    Claimed,
    FinalizedEmpty,
}

#[derive(Debug, Clone)]
pub enum AcquisitionProbe {
    Completed {
        reply: AcquireReply,
        kind: AcquisitionCompletion,
    },
    Pending {
        session_remaining_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcquisitionQueue {
    pub scope: Scope,
    pub queue: String,
}
impl From<&AcquireCommand> for AcquisitionQueue {
    fn from(command: &AcquireCommand) -> Self {
        Self {
            scope: command.scope.clone(),
            queue: command.queue.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcquisitionKey {
    pub queue: AcquisitionQueue,
    pub worker_session_id: String,
    pub consumer_id: u32,
    pub sequence: u64,
}
impl From<&AcquireCommand> for AcquisitionKey {
    fn from(command: &AcquireCommand) -> Self {
        Self {
            queue: command.into(),
            worker_session_id: command.worker_session_id.clone(),
            consumer_id: command.consumer_id,
            sequence: command.sequence,
        }
    }
}

/// Hints carry identities only. They never grant execution authority or cache a
/// reply. A rescan follows notification subscription/reconnection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AcquisitionHint {
    QueueChanged(AcquisitionQueue),
    AcquisitionCompleted(AcquisitionKey),
    Rescan,
}

/// Optional adapter-to-service wake port. Implementations must return promptly
/// without network I/O and bound retained interests. Periodic fallback remains
/// necessary even when a transport delivers these hints.
pub trait AcquisitionWake: Send + Sync {
    fn wake(&self, hint: AcquisitionHint);
}
