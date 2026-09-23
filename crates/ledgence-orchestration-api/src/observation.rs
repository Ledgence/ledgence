//! Compact, coherent observations of logical tasks, independent of worker reports.

use crate::*;
use ledgence_worker_api::{Error, Phase};
use serde::Deserializer;
use serde_json::Value;

/// Maximum encoded compact status response, excluding HTTP headers.
pub const TASK_STATUS_MAX_BYTES: usize = 16 * 1024;

/// Scheduling metadata without application input, output, or package payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskStatus {
    pub scope: Scope,
    pub task_id: String,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_activation_id: Option<String>,
    pub queue: String,
    #[serde(deserialize_with = "required_option")]
    pub correlation_key: Option<String>,
    pub state: TaskState,
    pub attempt_count: u32,
    #[serde(deserialize_with = "required_option")]
    pub current_attempt_id: Option<String>,
    /// Last allocated attempt, never inferred cancellation provenance.
    #[serde(deserialize_with = "required_option")]
    pub latest_attempt_id: Option<String>,
    pub submitted_at: Timestamp,
    pub available_at: Timestamp,
    #[serde(deserialize_with = "required_option")]
    pub terminal_at: Option<Timestamp>,
    #[serde(deserialize_with = "required_option")]
    pub cancel_requested_at: Option<Timestamp>,
}

impl TaskStatus {
    /// Validate an adapter-produced observation before exposing it to a caller.
    pub fn validate(&self) -> Result<()> {
        let invalid = || observation_error("inconsistent task status");
        self.scope.validate().map_err(|_| invalid())?;
        for value in [&self.task_id, &self.run_id, &self.queue] {
            validate_text(value, 128).map_err(|_| invalid())?;
        }
        for value in [&self.workflow_id, &self.workflow_activation_id]
            .into_iter()
            .flatten()
        {
            validate_text(value, 128).map_err(|_| invalid())?;
        }
        if let Some(activation) = &self.workflow_activation_id
            && (self.workflow_id.is_none() || activation != &self.task_id)
        {
            return Err(invalid());
        }
        if let Some(value) = &self.correlation_key
            && (value.len() > 512 || value.chars().any(char::is_control))
        {
            return Err(invalid());
        }
        for value in [&self.current_attempt_id, &self.latest_attempt_id]
            .into_iter()
            .flatten()
        {
            validate_text(value, 128).map_err(|_| invalid())?;
        }
        if self.attempt_count > 1000
            || (self.attempt_count == 0) != self.latest_attempt_id.is_none()
            || (self.state == TaskState::Active) != self.current_attempt_id.is_some()
            || (self.state == TaskState::Active
                && self.current_attempt_id != self.latest_attempt_id)
            || self.state.is_terminal() != self.terminal_at.is_some()
            || (self.state == TaskState::Cancelled && self.cancel_requested_at.is_none())
            || (self.cancel_requested_at.is_some()
                && !matches!(self.state, TaskState::Active | TaskState::Cancelled))
            || (matches!(self.state, TaskState::Succeeded | TaskState::Failed)
                && self.attempt_count == 0)
        {
            return Err(invalid());
        }
        // Matches the existing stored/core four-digit RFC3339 timestamp range.
        if [
            Some(self.submitted_at),
            Some(self.available_at),
            self.terminal_at,
            self.cancel_requested_at,
        ]
        .into_iter()
        .flatten()
        .any(|at| at > 253_402_300_799_999)
        {
            return Err(invalid());
        }
        crate::submission::check_encoded_size(self, TASK_STATUS_MAX_BYTES, "task status")
            .map_err(|_| invalid())
    }
}

/// One coherent task observation. Pending is distinct from successful JSON null.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskResult {
    pub task: TaskStatus,
    #[serde(deserialize_with = "required_option")]
    pub outcome: Option<TaskOutcome>,
}

/// Terminal scheduling outcome. Cancellation never attributes an earlier attempt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskOutcome {
    Succeeded {
        attempt_id: String,
        quiescence: Quiescence,
        execution_may_have_started: bool,
        #[serde(deserialize_with = "required_value")]
        output: Value,
    },
    Failed {
        attempt_id: String,
        quiescence: Quiescence,
        execution_may_have_started: bool,
        failure: TaskFailure,
    },
    Cancelled {},
}

/// Application error identifiers are user-defined, unlike worker error kinds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationError {
    pub kind: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskFailure {
    Application {
        error: ApplicationError,
    },
    Execution {
        #[serde(deserialize_with = "required_error")]
        error: Error,
        phase: Phase,
        #[serde(deserialize_with = "required_cleanup_error")]
        cleanup_error: Option<Error>,
    },
    AttemptLost {},
}

impl TaskResult {
    /// Reject contradictory backend or transport results without inventing state.
    pub fn validate(&self) -> Result<()> {
        self.task.validate()?;
        let invalid = || observation_error("inconsistent task outcome");
        match (&self.outcome, self.task.state) {
            (None, TaskState::Queued | TaskState::Active)
            | (Some(TaskOutcome::Cancelled {}), TaskState::Cancelled) => Ok(()),
            (
                Some(TaskOutcome::Succeeded {
                    attempt_id,
                    output,
                    execution_may_have_started,
                    ..
                }),
                TaskState::Succeeded,
            ) => {
                if self.task.latest_attempt_id.as_ref() != Some(attempt_id)
                    || !execution_may_have_started
                {
                    return Err(invalid());
                }
                validate_task_output(output, self.task.workflow_activation_id.is_some())
                    .map_err(|_| invalid())
            }
            (
                Some(TaskOutcome::Failed {
                    attempt_id,
                    quiescence,
                    execution_may_have_started,
                    failure,
                }),
                TaskState::Failed,
            ) => {
                if self.task.latest_attempt_id.as_ref() != Some(attempt_id)
                    || (matches!(failure, TaskFailure::AttemptLost {})
                        && *quiescence != Quiescence::Unconfirmed)
                    || (matches!(failure, TaskFailure::Application { .. })
                        && !execution_may_have_started)
                {
                    return Err(invalid());
                }
                Ok(())
            }
            _ => Err(invalid()),
        }?;
        if let Some(outcome) = &self.outcome {
            // Projection removes report/owner context, so every valid compact
            // outcome fits within its original 8 MiB settlement budget.
            crate::submission::check_encoded_size(outcome, SETTLEMENT_MAX_BYTES, "task outcome")
                .map_err(|_| invalid())?;
        }
        Ok(())
    }
}

pub(crate) fn required_option<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn observation_error(message: &str) -> ContractError {
    ContractError::Unavailable(message.into())
}

pub(crate) fn required_value<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Value, D::Error> {
    Value::deserialize(deserializer)
}

#[cfg(test)]
#[path = "observation_tests.rs"]
mod tests;

// Keep the existing portable worker Error type without inheriting its older,
// permissive unknown-field response decoding in this additive strict contract.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictError {
    kind: ledgence_worker_api::ErrorKind,
    message: String,
}
impl From<StrictError> for Error {
    fn from(value: StrictError) -> Self {
        Self {
            kind: value.kind,
            message: value.message,
        }
    }
}
fn required_error<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Error, D::Error> {
    StrictError::deserialize(deserializer).map(Into::into)
}
fn required_cleanup_error<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Error>, D::Error> {
    Option::<StrictError>::deserialize(deserializer).map(|value| value.map(Into::into))
}
