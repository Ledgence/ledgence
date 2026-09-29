//! Optional observations; never authority for a workflow decision or local replay.
use crate::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const INVOCATION_OBSERVATIONS_MAX_BYTES: usize = 128 * 1024;

/// CPU deltas cover the runtime process and all its threads during this invocation,
/// excluding subprocesses. RSS is explicitly the process lifetime high-water mark;
/// warm-process reuse means it may come from an earlier invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationObservations {
    pub runtime_started_at_ms: u64,
    pub runtime_elapsed_us: u64,
    pub process_cpu_user_us: Option<u64>,
    pub process_cpu_system_us: Option<u64>,
    pub process_lifetime_peak_rss_bytes: Option<u64>,
    pub local_steps: Vec<LocalStepObservation>,
    pub local_steps_truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalStepObservation {
    pub key: String,
    pub callable: String,
    pub started_at_ms: u64,
    pub elapsed_us: u64,
    pub state: LocalStepObservedState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalStepObservedState {
    /// Callable returned a valid value; this does not establish durable acceptance.
    Returned,
    Failed,
    Cancelled,
    /// Previously accepted output was reused without invoking the callable.
    Replayed,
}

impl InvocationObservations {
    pub fn validate(&self) -> Result<()> {
        let invalid = || Error::new(ErrorKind::InvalidInput, "invalid invocation observations");
        if self.runtime_started_at_ms > 253_402_300_799_999 || self.local_steps.len() > 128 {
            return Err(invalid());
        }
        let mut keys = BTreeSet::new();
        for step in &self.local_steps {
            if step.started_at_ms > 253_402_300_799_999 || !keys.insert(&step.key) {
                return Err(invalid());
            }
            for (text, max) in [(&step.key, 128), (&step.callable, 512)] {
                if text.is_empty() || text.len() > max || text.chars().any(char::is_control) {
                    return Err(invalid());
                }
            }
        }
        let value = serde_json::to_value(self).map_err(|_| invalid())?;
        crate::validate_runtime_payload(&value, INVOCATION_OBSERVATIONS_MAX_BYTES)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample() -> InvocationObservations {
        InvocationObservations {
            runtime_started_at_ms: 1,
            runtime_elapsed_us: 10,
            process_cpu_user_us: None,
            process_cpu_system_us: None,
            process_lifetime_peak_rss_bytes: None,
            local_steps: vec![],
            local_steps_truncated: false,
        }
    }
    #[test]
    fn unknown_resources_stay_absent_and_local_observations_are_bounded() {
        let mut value = sample();
        value.validate().unwrap();
        assert!(serde_json::to_value(&value).unwrap()["process_cpu_user_us"].is_null());
        let step = LocalStepObservation {
            key: "tests:0".into(),
            callable: "program:tests".into(),
            started_at_ms: 1,
            elapsed_us: 5,
            state: LocalStepObservedState::Returned,
        };
        value.local_steps = vec![step.clone()];
        value.validate().unwrap();
        value.local_steps.push(step.clone());
        assert!(
            value.validate().is_err(),
            "duplicate logical occurrences are invalid"
        );
        value.local_steps = (0..129)
            .map(|n| LocalStepObservation {
                key: format!("step:{n}"),
                ..step.clone()
            })
            .collect();
        assert!(value.validate().is_err());
        value.local_steps.clear();
        value.runtime_started_at_ms = u64::MAX;
        assert!(value.validate().is_err());
    }
}
