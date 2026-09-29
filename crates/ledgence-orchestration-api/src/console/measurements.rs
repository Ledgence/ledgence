use super::*;
use ledgence_worker_api::{InvocationObservations, LocalStepObservation, LocalStepObservedState};

/// Explicit attempt inspection, separate from legacy exact-field Console DTOs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleAttemptObservations {
    pub task_id: String,
    pub attempt_id: String,
    pub observations: Option<ConsoleInvocationObservations>,
    pub observed_at: Timestamp,
}
impl ConsoleAttemptObservations {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.task_id, 128)?;
        validate_text(&self.attempt_id, 128)?;
        timestamp(self.observed_at)?;
        if let Some(value) = &self.observations {
            InvocationObservations::from(value.clone()).validate()?;
        }
        metadata_size(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleInvocationObservations {
    pub runtime_started_at_ms: Timestamp,
    pub runtime_elapsed_us: ConsoleU64,
    pub process_cpu_user_us: Option<ConsoleU64>,
    pub process_cpu_system_us: Option<ConsoleU64>,
    pub process_lifetime_peak_rss_bytes: Option<ConsoleU64>,
    pub local_steps: Vec<ConsoleAttemptLocalObservation>,
    pub local_steps_truncated: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleAttemptLocalObservation {
    pub key: String,
    pub callable: String,
    pub started_at_ms: Timestamp,
    pub elapsed_us: ConsoleU64,
    pub state: LocalStepObservedState,
}
impl From<InvocationObservations> for ConsoleInvocationObservations {
    fn from(v: InvocationObservations) -> Self {
        Self {
            runtime_started_at_ms: v.runtime_started_at_ms,
            runtime_elapsed_us: ConsoleU64(v.runtime_elapsed_us),
            process_cpu_user_us: v.process_cpu_user_us.map(ConsoleU64),
            process_cpu_system_us: v.process_cpu_system_us.map(ConsoleU64),
            process_lifetime_peak_rss_bytes: v.process_lifetime_peak_rss_bytes.map(ConsoleU64),
            local_steps: v
                .local_steps
                .into_iter()
                .map(|s| ConsoleAttemptLocalObservation {
                    key: s.key,
                    callable: s.callable,
                    started_at_ms: s.started_at_ms,
                    elapsed_us: ConsoleU64(s.elapsed_us),
                    state: s.state,
                })
                .collect(),
            local_steps_truncated: v.local_steps_truncated,
        }
    }
}
impl From<ConsoleInvocationObservations> for InvocationObservations {
    fn from(v: ConsoleInvocationObservations) -> Self {
        Self {
            runtime_started_at_ms: v.runtime_started_at_ms,
            runtime_elapsed_us: v.runtime_elapsed_us.0,
            process_cpu_user_us: v.process_cpu_user_us.map(|n| n.0),
            process_cpu_system_us: v.process_cpu_system_us.map(|n| n.0),
            process_lifetime_peak_rss_bytes: v.process_lifetime_peak_rss_bytes.map(|n| n.0),
            local_steps: v
                .local_steps
                .into_iter()
                .map(|s| LocalStepObservation {
                    key: s.key,
                    callable: s.callable,
                    started_at_ms: s.started_at_ms,
                    elapsed_us: s.elapsed_us.0,
                    state: s.state,
                })
                .collect(),
            local_steps_truncated: v.local_steps_truncated,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn measurement_counters_round_trip_without_javascript_precision_loss() {
        let observations = InvocationObservations {
            runtime_started_at_ms: 100,
            runtime_elapsed_us: u64::MAX,
            process_cpu_user_us: Some(u64::MAX),
            process_cpu_system_us: None,
            process_lifetime_peak_rss_bytes: Some(u64::MAX),
            local_steps: vec![],
            local_steps_truncated: true,
        };
        let dto = ConsoleInvocationObservations::from(observations.clone());
        let json = serde_json::to_value(&dto).unwrap();
        assert_eq!(json["runtime_elapsed_us"], u64::MAX.to_string());
        assert!(json["process_cpu_system_us"].is_null());
        assert_eq!(InvocationObservations::from(dto), observations);
    }
}
