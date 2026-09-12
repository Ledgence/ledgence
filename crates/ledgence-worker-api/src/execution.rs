//! Portable invocation requests and observations shared by workers and delivery adapters.
//!
//! A report describes local execution; it does not certify durable orchestration
//! acceptance or exactly-once application effects.

use crate::{
    CloudEvent, Digest, Error, InvocationIdentity, ProgramDescriptor, ProgramOutcome, ProgramRef,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRequest {
    pub descriptor: ProgramDescriptor,
    pub event: CloudEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionContext {
    #[serde(flatten)]
    pub identity: InvocationIdentity,
    pub program: ProgramRef,
    pub digest: Digest,
}
impl From<&ExecutionRequest> for ExecutionContext {
    fn from(request: &ExecutionRequest) -> Self {
        Self {
            identity: InvocationIdentity::from(&request.event),
            program: request.descriptor.program.clone(),
            digest: request.descriptor.digest.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReport {
    #[serde(flatten)]
    pub context: Box<ExecutionContext>,
    pub process_id: u32,
    pub reused_process: bool,
    pub outcome: ProgramOutcome,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Admission,
    Preparation,
    Startup,
    Execution,
    Cleanup,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionFailure {
    #[serde(flatten)]
    pub context: Box<ExecutionContext>,
    pub error: Error,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_error: Option<Error>,
    pub phase: Phase,
    /// Conservative: a lost response must not be retried inside the runtime.
    pub execution_may_have_started: bool,
}
impl std::fmt::Display for ExecutionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.phase, self.error)
    }
}
impl std::error::Error for ExecutionFailure {}
pub type ExecutionResult = std::result::Result<ExecutionReport, ExecutionFailure>;
