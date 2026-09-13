use ledgence_orchestration_api::{AcquireCommand, ContractError, Scope};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenSession {
    pub scope: Scope,
    pub queue: String,
    pub concurrency: u32,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExtendSession {
    pub worker_session_id: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Cancel {
    pub scope: Scope,
    pub task_id: String,
}

/// Transport preferences are explicit fields, separate from durable identity.
/// `u64` plus `default` accepts omission while rejecting null and non-integers.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcquisitionRequest {
    pub scope: Scope,
    pub queue: String,
    pub worker_session_id: String,
    pub consumer_id: u32,
    pub sequence: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub wait_ms: u64,
}

impl AcquisitionRequest {
    #[cfg(feature = "client")]
    pub fn new(command: &AcquireCommand, wait_ms: u64) -> Self {
        Self {
            scope: command.scope.clone(),
            queue: command.queue.clone(),
            worker_session_id: command.worker_session_id.clone(),
            consumer_id: command.consumer_id,
            sequence: command.sequence,
            wait_ms,
        }
    }

    #[cfg(feature = "server")]
    pub fn into_command(self) -> AcquireCommand {
        AcquireCommand {
            scope: self.scope,
            queue: self.queue,
            worker_session_id: self.worker_session_id,
            consumer_id: self.consumer_id,
            sequence: self.sequence,
        }
    }
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

pub(crate) fn error_status(error: &ContractError) -> u16 {
    match error {
        ContractError::InvalidInput(_) => 400,
        ContractError::NotFound | ContractError::UnknownSession => 404,
        ContractError::Conflict
        | ContractError::OwnershipLost
        | ContractError::ObsoleteOperation
        | ContractError::OutOfOrder
        | ContractError::Busy => 409,
        ContractError::SessionExpired => 410,
        ContractError::Unavailable(_) => 503,
    }
}

pub(crate) fn error_code(error: &ContractError) -> &'static str {
    match error {
        ContractError::InvalidInput(_) => "invalid_input",
        ContractError::NotFound => "not_found",
        ContractError::UnknownSession => "unknown_session",
        ContractError::Conflict => "conflict",
        ContractError::OwnershipLost => "ownership_lost",
        ContractError::ObsoleteOperation => "obsolete_operation",
        ContractError::OutOfOrder => "out_of_order",
        ContractError::Busy => "busy",
        ContractError::SessionExpired => "session_expired",
        ContractError::Unavailable(_) => "unavailable",
    }
}

pub(crate) fn is_json(content_type: &str) -> bool {
    let mut fields = content_type.split(';');
    if !fields
        .next()
        .unwrap_or("")
        .trim()
        .eq_ignore_ascii_case("application/json")
    {
        return false;
    }
    match (fields.next(), fields.next()) {
        (None, None) => true,
        (Some(parameter), None) => {
            let Some((name, value)) = parameter.split_once('=') else {
                return false;
            };
            let value = value.trim();
            let value = if value.starts_with('"') {
                value
                    .strip_prefix('"')
                    .and_then(|value| value.strip_suffix('"'))
                    .unwrap_or("")
            } else {
                value
            };
            name.trim().eq_ignore_ascii_case("charset") && value.eq_ignore_ascii_case("utf-8")
        }
        _ => false,
    }
}

pub(crate) fn unavailable(message: impl Into<String>) -> ContractError {
    ContractError::Unavailable(message.into())
}
