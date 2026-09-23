//! Portable invocation requests and observations shared by workers and delivery adapters.
//!
//! A report describes local execution; it does not certify durable orchestration
//! acceptance or exactly-once application effects.

use crate::{
    CloudEvent, Digest, Error, InvocationIdentity, ProgramDescriptor, ProgramOutcome, ProgramRef,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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

/// Ephemeral runtime input. The carrier belongs to the current execution span;
/// it does not modify the durable request or its immutable CloudEvent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeInvocation {
    pub event: CloudEvent,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processing_context: Option<crate::TraceContext>,
    /// Optional platform context, separate from the wholly user-owned event data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extension: Option<RuntimeExtension>,
}
impl From<CloudEvent> for RuntimeInvocation {
    fn from(event: CloudEvent) -> Self {
        Self {
            event,
            processing_context: None,
            extension: None,
        }
    }
}

/// Versioned, runtime-neutral context for an opt-in interactive invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeExtension {
    pub schema: String,
    pub payload: Value,
}
impl RuntimeExtension {
    pub fn validate(&self) -> crate::Result<()> {
        validate_name(&self.schema, "runtime extension schema")?;
        crate::validate_runtime_payload(&self.payload, crate::RUNTIME_EXTENSION_MAX_BYTES)
    }
}

/// One invocation-scoped request. The host, not these fields, supplies authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeRequest {
    pub id: u64,
    pub operation: String,
    pub payload: Value,
}
impl RuntimeRequest {
    pub fn validate(&self) -> crate::Result<()> {
        if self.id == 0 {
            return Err(Error::new(
                crate::ErrorKind::InvalidInput,
                "runtime request ID must be positive",
            ));
        }
        validate_name(&self.operation, "runtime request operation")?;
        crate::validate_runtime_payload(&self.payload, crate::RUNTIME_REQUEST_MAX_BYTES)
    }
}

/// An acknowledgement for exactly one request; its result schema belongs to the handler.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeReply {
    pub id: u64,
    pub result: Value,
}

fn validate_name(value: &str, label: &str) -> crate::Result<()> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(Error::new(
            crate::ErrorKind::InvalidInput,
            format!("{label} must contain 1..=256 bytes without control characters"),
        ));
    }
    Ok(())
}
