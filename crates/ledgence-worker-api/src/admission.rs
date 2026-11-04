//! Optional host-owned permission to begin an already prepared invocation.
//! No provider SDK, orchestration identity, or Python helper is part of this port.
use crate::{Error, ErrorKind, ExecutionContext, ExecutionResult, PortFuture, Result, RunControl};
use std::time::Instant;

/// Local, monotonic send window. This grants one invocation, not a renewed lease.
#[derive(Debug)]
pub struct InvocationPermit {
    pub start_before: Instant,
}
impl InvocationPermit {
    pub fn check(&self) -> Result<()> {
        if Instant::now() >= self.start_before {
            Err(Error::new(
                ErrorKind::TimedOut,
                "invocation permission expired before dispatch",
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone)]
pub struct InvocationReady {
    pub context: ExecutionContext,
    pub process_id: u32,
    pub reused_process: bool,
}

pub enum InvocationAdmissionDecision {
    Invoke(InvocationPermit),
    /// No user invocation is permitted. The delivery owner must still close its
    /// durable preparation and establish whether deferral is accepted.
    DoNotInvoke,
}

/// Called once, after artifact and process preparation, by the owned supervisor.
/// An uncertain reply must never be retried to manufacture execution permission.
pub trait InvocationAdmission: Send + Sync {
    fn ready<'a>(
        &'a self,
        ready: &'a InvocationReady,
        control: RunControl,
    ) -> PortFuture<'a, InvocationAdmissionDecision>;
}

/// A local no-invocation result is not a durable deferral or capacity release.
pub enum AdmittedExecutionResult {
    Executed(ExecutionResult),
    NotInvoked { context: Box<ExecutionContext> },
}
