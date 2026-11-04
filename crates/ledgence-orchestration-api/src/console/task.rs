use super::*;
use ledgence_worker_api::{Error, Phase};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleProviderProbeAssignment {
    pub occurrence: EffectOccurrence,
    pub probe_id: String,
    pub generation: ConsoleU64,
}
impl From<ProviderProbeAssignment> for ConsoleProviderProbeAssignment {
    fn from(value: ProviderProbeAssignment) -> Self {
        Self {
            occurrence: value.occurrence,
            probe_id: value.probe_id,
            generation: ConsoleU64(value.generation),
        }
    }
}
impl ConsoleProviderProbeAssignment {
    pub fn into_core(self) -> ProviderProbeAssignment {
        ProviderProbeAssignment {
            occurrence: self.occurrence,
            probe_id: self.probe_id,
            generation: self.generation.0,
        }
    }
}

/// Scope-free status; every field is selected explicitly from the legacy model.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleTaskStatus {
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub admission: Option<TaskAdmissionRequirement>,
    pub admission_deferrals: u32,
    pub task_id: String,
    pub run_id: String,
    pub workflow_id: Option<String>,
    pub workflow_activation_id: Option<String>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub provider_probe: Option<ConsoleProviderProbeAssignment>,
    pub queue: String,
    pub correlation_key: Option<String>,
    pub state: TaskState,
    pub attempt_count: u32,
    pub current_attempt_id: Option<String>,
    pub latest_attempt_id: Option<String>,
    pub submitted_at: Timestamp,
    pub available_at: Timestamp,
    pub terminal_at: Option<Timestamp>,
    pub cancel_requested_at: Option<Timestamp>,
}
impl From<TaskStatus> for ConsoleTaskStatus {
    fn from(v: TaskStatus) -> Self {
        Self {
            admission: v.admission,
            admission_deferrals: v.admission_deferrals,
            task_id: v.task_id,
            run_id: v.run_id,
            workflow_id: v.workflow_id,
            workflow_activation_id: v.workflow_activation_id,
            provider_probe: v.provider_probe.map(Into::into),
            queue: v.queue,
            correlation_key: v.correlation_key,
            state: v.state,
            attempt_count: v.attempt_count,
            current_attempt_id: v.current_attempt_id,
            latest_attempt_id: v.latest_attempt_id,
            submitted_at: v.submitted_at,
            available_at: v.available_at,
            terminal_at: v.terminal_at,
            cancel_requested_at: v.cancel_requested_at,
        }
    }
}
impl ConsoleTaskStatus {
    pub fn validate(&self) -> Result<()> {
        TaskStatus {
            admission: self.admission.clone(),
            admission_deferrals: self.admission_deferrals,
            scope: self
                .provider_probe
                .as_ref()
                .map(|probe| probe.occurrence.scope.clone())
                .unwrap_or_else(|| Scope {
                    tenant_id: "console-validation".into(),
                    namespace: "console-validation".into(),
                }),
            task_id: self.task_id.clone(),
            run_id: self.run_id.clone(),
            workflow_id: self.workflow_id.clone(),
            workflow_activation_id: self.workflow_activation_id.clone(),
            provider_probe: self
                .provider_probe
                .clone()
                .map(ConsoleProviderProbeAssignment::into_core),
            queue: self.queue.clone(),
            correlation_key: self.correlation_key.clone(),
            state: self.state,
            attempt_count: self.attempt_count,
            current_attempt_id: self.current_attempt_id.clone(),
            latest_attempt_id: self.latest_attempt_id.clone(),
            submitted_at: self.submitted_at,
            available_at: self.available_at,
            terminal_at: self.terminal_at,
            cancel_requested_at: self.cancel_requested_at,
        }
        .validate()
    }
    pub fn matches(&self, filters: &TaskFilters) -> bool {
        filters.state.is_none_or(|v| v == self.state)
            && filters.queue.as_ref().is_none_or(|v| v == &self.queue)
            && filters
                .correlation_key
                .as_ref()
                .is_none_or(|v| Some(v) == self.correlation_key.as_ref())
            && filters
                .submitted_from
                .is_none_or(|v| self.submitted_at >= v)
            && filters
                .submitted_until
                .is_none_or(|v| self.submitted_at < v)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleTaskSummary {
    pub task: ConsoleTaskStatus,
    pub descriptor: ConsoleProgramDescriptor,
}
impl ConsoleRecord for ConsoleTaskSummary {
    fn position(&self) -> ConsolePosition {
        vec![
            ConsoleKey::Number(ConsoleU64(self.task.submitted_at)),
            ConsoleKey::Text(self.task.task_id.clone()),
        ]
    }
    fn validate(&self) -> Result<()> {
        self.task.validate()?;
        self.descriptor.validate()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleSubmitTask {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::effect_resolution::present"
    )]
    pub admission: Option<TaskAdmissionRequirement>,
    pub program: ProgramRef,
    pub queue: String,
    #[serde(default)]
    pub correlation_key: Option<String>,
    #[serde(deserialize_with = "crate::observation::required_value")]
    pub data: Value,
    #[serde(default)]
    pub retry_policy: RetryPolicy,
    #[serde(default = "attempt_timeout")]
    pub attempt_timeout_ms: u64,
}
const fn attempt_timeout() -> u64 {
    300_000
}
impl ConsoleSubmitTask {
    pub fn into_submission(self, scope: &Scope) -> Result<SubmitTask> {
        let value = SubmitTask {
            admission: self.admission,
            tenant_id: scope.tenant_id.clone(),
            namespace: scope.namespace.clone(),
            program: self.program,
            queue: self.queue,
            correlation_key: self.correlation_key,
            data: self.data,
            retry_policy: self.retry_policy,
            attempt_timeout_ms: self.attempt_timeout_ms,
        };
        value.validate()?;
        Ok(value)
    }
}
impl From<SubmitTask> for ConsoleSubmitTask {
    fn from(value: SubmitTask) -> Self {
        ConsoleTaskInput::from(value).into()
    }
}

/// Recorded input is an observation: the nullable admission field is always
/// present. A submission command instead omits the field to opt out.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleTaskInput {
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub admission: Option<TaskAdmissionRequirement>,
    pub program: ProgramRef,
    pub queue: String,
    pub correlation_key: Option<String>,
    #[serde(deserialize_with = "crate::observation::required_value")]
    pub data: Value,
    pub retry_policy: RetryPolicy,
    pub attempt_timeout_ms: u64,
}
impl From<SubmitTask> for ConsoleTaskInput {
    fn from(value: SubmitTask) -> Self {
        Self {
            admission: value.admission,
            program: value.program,
            queue: value.queue,
            correlation_key: value.correlation_key,
            data: value.data,
            retry_policy: value.retry_policy,
            attempt_timeout_ms: value.attempt_timeout_ms,
        }
    }
}
impl From<ConsoleTaskInput> for ConsoleSubmitTask {
    fn from(value: ConsoleTaskInput) -> Self {
        Self {
            admission: value.admission,
            program: value.program,
            queue: value.queue,
            correlation_key: value.correlation_key,
            data: value.data,
            retry_policy: value.retry_policy,
            attempt_timeout_ms: value.attempt_timeout_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleTaskDetail {
    pub admission_deferrals: u32,
    pub task_id: String,
    pub run_id: String,
    pub workflow_id: Option<String>,
    pub workflow_activation_id: Option<String>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub provider_probe: Option<ConsoleProviderProbeAssignment>,
    pub parent_workflow_id: Option<String>,
    pub root_workflow_id: Option<String>,
    pub input: ConsoleTaskInput,
    pub descriptor: ConsoleProgramDescriptor,
    pub idempotency_key: String,
    pub origin_trace: Option<TraceContext>,
    pub state: TaskState,
    pub attempt_count: u32,
    pub current_attempt_id: Option<String>,
    pub submitted_at: Timestamp,
    pub available_at: Timestamp,
    pub terminal_at: Option<Timestamp>,
    pub cancel_requested_at: Option<Timestamp>,
    pub observed_at: Timestamp,
}
impl ConsoleTaskDetail {
    /// Validate only facts present in the snapshot; the last allocated attempt
    /// is deliberately not inferred from the currently active attempt.
    pub fn validate(&self) -> Result<()> {
        ConsoleSubmitTask::from(self.input.clone()).into_submission(&Scope {
            tenant_id: "console-validation".into(),
            namespace: "console-validation".into(),
        })?;
        self.descriptor.validate()?;
        validate_text(&self.task_id, 128)?;
        validate_text(&self.run_id, 128)?;
        validate_text(&self.idempotency_key, 255)?;
        validate_workflow_lineage(
            self.workflow_id.as_deref(),
            self.parent_workflow_id.as_deref(),
            self.root_workflow_id.as_deref(),
        )?;
        if let Some(trace) = &self.origin_trace {
            trace.validate()?;
        }
        if let Some(probe) = &self.provider_probe {
            probe.clone().into_core().validate()?;
            if self.workflow_id.as_ref() != Some(&probe.occurrence.workflow_id)
                || self.workflow_activation_id.is_some()
            {
                return Err(inconsistent("inconsistent provider probe identity"));
            }
        }
        if let Some(id) = &self.current_attempt_id {
            validate_text(id, 128)?;
        }
        if let Some(id) = &self.workflow_activation_id {
            validate_text(id, 128)?;
            if self.workflow_id.is_none() || id != &self.task_id {
                return Err(inconsistent("inconsistent workflow activation identity"));
            }
        }
        let charged = task_retry_attempts(
            self.attempt_count,
            self.admission_deferrals,
            self.input.admission.is_some(),
        )
        .map_err(|_| inconsistent("inconsistent task admission counters"))?;
        if self.input.admission.is_some()
            && (self.workflow_id.is_some()
                || self.workflow_activation_id.is_some()
                || self.provider_probe.is_some())
        {
            return Err(inconsistent("task admission requires a standalone task"));
        }
        if self.descriptor.program != self.input.program
            || charged > self.input.retry_policy.max_attempts
            || (self.state == TaskState::Active) != self.current_attempt_id.is_some()
            || (matches!(
                self.state,
                TaskState::Active | TaskState::Succeeded | TaskState::Failed
            ) && self.attempt_count == 0)
            || self.state.is_terminal() != self.terminal_at.is_some()
            || (self.state == TaskState::Cancelled && self.cancel_requested_at.is_none())
            || (self.cancel_requested_at.is_some()
                && !matches!(self.state, TaskState::Active | TaskState::Cancelled))
        {
            return Err(inconsistent("inconsistent task detail"));
        }
        for at in [
            Some(self.submitted_at),
            Some(self.available_at),
            self.terminal_at,
            self.cancel_requested_at,
            Some(self.observed_at),
        ]
        .into_iter()
        .flatten()
        {
            timestamp(at)?;
        }
        metadata_size(self)
    }
    pub fn from_snapshot(v: TaskSnapshot, observed_at: Timestamp) -> Self {
        Self {
            admission_deferrals: v.admission_deferrals,
            task_id: v.task_id,
            run_id: v.run_id,
            workflow_id: v.workflow_id,
            workflow_activation_id: v.workflow_activation_id,
            provider_probe: v.provider_probe.map(Into::into),
            parent_workflow_id: v.parent_workflow_id,
            root_workflow_id: v.root_workflow_id,
            input: v.input.into(),
            descriptor: v.descriptor.into(),
            idempotency_key: v.idempotency_key,
            origin_trace: v.origin_trace,
            state: v.state,
            attempt_count: v.attempt_count,
            current_attempt_id: v.current_attempt_id,
            submitted_at: v.submitted_at,
            available_at: v.available_at,
            terminal_at: v.terminal_at,
            cancel_requested_at: v.cancel_requested_at,
            observed_at,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleObservedTaskStatus {
    pub task: ConsoleTaskStatus,
    pub observed_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleTaskResult {
    pub task: ConsoleTaskStatus,
    pub outcome: Option<TaskOutcome>,
    pub observed_at: Timestamp,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleAttemptSummary {
    pub task_id: String,
    pub attempt_id: String,
    pub generation: u32,
    pub worker_session_id: String,
    pub consumer_id: u32,
    pub state: AttemptState,
    pub execution_may_have_started: bool,
    pub quiescence: Quiescence,
    pub claimed_at: Option<Timestamp>,
    pub dispatch_authorized_at: Option<Timestamp>,
    pub finished_at: Option<Timestamp>,
}
impl ConsoleRecord for ConsoleAttemptSummary {
    fn position(&self) -> ConsolePosition {
        vec![
            ConsoleKey::Number(ConsoleU64(u64::from(self.generation))),
            ConsoleKey::Text(self.attempt_id.clone()),
        ]
    }
    fn validate(&self) -> Result<()> {
        for id in [&self.task_id, &self.attempt_id, &self.worker_session_id] {
            validate_text(id, 128)?;
        }
        if !(1..=MAX_TASK_ATTEMPT_GENERATION).contains(&self.generation) {
            return Err(invalid("invalid attempt generation"));
        }
        for at in [
            self.claimed_at,
            self.dispatch_authorized_at,
            self.finished_at,
        ]
        .into_iter()
        .flatten()
        {
            timestamp(at)?;
        }
        Ok(())
    }
}
/// No lease identifier, renewal command, event payload or settlement owner.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleAttemptDetail {
    pub attempt: ConsoleAttemptSummary,
    pub descriptor: ConsoleProgramDescriptor,
    pub phase: Option<Phase>,
    pub error: Option<Error>,
    pub application_error: Option<ApplicationError>,
    pub cleanup_error: Option<Error>,
    pub process_id: Option<u32>,
    /// Not recorded by older workers. A PID must never be substituted here.
    pub process_instance_id: Option<String>,
    pub reused_process: Option<bool>,
    /// Includes preparation; this is not a process execution duration.
    pub worker_elapsed_ms: Option<ConsoleU64>,
    pub observed_at: Timestamp,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleTaskHistory {
    pub sequence: ConsoleU64,
    pub task_id: String,
    pub attempt_id: Option<String>,
    pub at: Timestamp,
    pub reason: TransitionReason,
}
impl ConsoleRecord for ConsoleTaskHistory {
    fn position(&self) -> ConsolePosition {
        vec![ConsoleKey::Number(self.sequence)]
    }
    fn validate(&self) -> Result<()> {
        validate_text(&self.task_id, 128)?;
        if let Some(id) = &self.attempt_id {
            validate_text(id, 128)?;
        }
        timestamp(self.at)
    }
}
