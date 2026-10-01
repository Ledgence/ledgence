use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleWorkflowStatus {
    pub workflow_id: String,
    pub state: WorkflowState,
    pub revision: ConsoleU64,
    pub activation_id: Option<String>,
    pub submitted_at: Timestamp,
    pub terminal_at: Option<Timestamp>,
    pub correlation_key: Option<String>,
    pub parent_workflow_id: Option<String>,
    pub root_workflow_id: Option<String>,
}
impl From<WorkflowSnapshot> for ConsoleWorkflowStatus {
    fn from(v: WorkflowSnapshot) -> Self {
        Self {
            workflow_id: v.workflow_id,
            state: v.state,
            revision: ConsoleU64(v.revision),
            activation_id: v.activation_id,
            submitted_at: v.submitted_at,
            terminal_at: v.terminal_at,
            correlation_key: v.correlation_key,
            parent_workflow_id: v.parent_workflow_id,
            root_workflow_id: v.root_workflow_id,
        }
    }
}
impl ConsoleWorkflowStatus {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.workflow_id, 128)?;
        for id in [
            &self.activation_id,
            &self.parent_workflow_id,
            &self.root_workflow_id,
        ]
        .into_iter()
        .flatten()
        {
            validate_text(id, 128)?;
        }
        timestamp(self.submitted_at)?;
        if let Some(at) = self.terminal_at {
            timestamp(at)?;
        }
        if self.state.is_terminal() != self.terminal_at.is_some()
            || self.parent_workflow_id.is_some() != self.root_workflow_id.is_some()
        {
            return Err(invalid("inconsistent workflow status"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleWorkflowSummary {
    pub workflow: ConsoleWorkflowStatus,
    pub controller: ConsoleProgramDescriptor,
    pub queue: String,
}
impl ConsoleRecord for ConsoleWorkflowSummary {
    fn position(&self) -> ConsolePosition {
        vec![
            ConsoleKey::Number(ConsoleU64(self.workflow.submitted_at)),
            ConsoleKey::Text(self.workflow.workflow_id.clone()),
        ]
    }
    fn validate(&self) -> Result<()> {
        self.workflow.validate()?;
        self.controller.validate()?;
        validate_text(&self.queue, 128)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleChildWait {
    pub activation_id: String,
    pub command_keys: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleWorkflowDetail {
    pub summary: ConsoleWorkflowSummary,
    pub continuation: String,
    pub child_wait: Option<ConsoleChildWait>,
    pub external_wait_key: Option<String>,
    pub observed_at: Timestamp,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleWorkflowResult {
    pub workflow: ConsoleWorkflowStatus,
    pub outcome: Option<WorkflowOutcome>,
    pub observed_at: Timestamp,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleWorkflowFilters {
    pub state: Option<WorkflowState>,
    pub correlation_key: Option<String>,
    pub submitted_from: Option<Timestamp>,
    pub submitted_until: Option<Timestamp>,
    pub parent_workflow_id: Option<String>,
    #[serde(default)]
    pub root_only: bool,
}
impl ConsoleWorkflowFilters {
    pub fn validate(&self) -> Result<()> {
        TaskFilters {
            state: None,
            queue: None,
            correlation_key: self.correlation_key.clone(),
            submitted_from: self.submitted_from,
            submitted_until: self.submitted_until,
        }
        .validate()?;
        if let Some(id) = &self.parent_workflow_id {
            validate_text(id, 128)?;
            if self.root_only {
                return Err(invalid("root_only and parent_workflow_id are exclusive"));
            }
        }
        Ok(())
    }
    pub fn matches(&self, item: &ConsoleWorkflowSummary) -> bool {
        let w = &item.workflow;
        self.state.is_none_or(|s| s == w.state)
            && self
                .correlation_key
                .as_ref()
                .is_none_or(|k| Some(k) == w.correlation_key.as_ref())
            && self.submitted_from.is_none_or(|at| w.submitted_at >= at)
            && self.submitted_until.is_none_or(|at| w.submitted_at < at)
            && self
                .parent_workflow_id
                .as_ref()
                .is_none_or(|id| Some(id) == w.parent_workflow_id.as_ref())
            && (!self.root_only || w.parent_workflow_id.is_none())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleActivation {
    pub workflow_id: String,
    pub activation_id: String,
    pub task_id: String,
    pub revision: ConsoleU64,
    pub state: TaskState,
    pub applied_at: Option<Timestamp>,
    pub error: Option<ApplicationError>,
}
impl ConsoleRecord for ConsoleActivation {
    fn position(&self) -> ConsolePosition {
        vec![
            ConsoleKey::Number(self.revision),
            ConsoleKey::Text(self.activation_id.clone()),
        ]
    }
    fn validate(&self) -> Result<()> {
        for id in [&self.workflow_id, &self.activation_id, &self.task_id] {
            validate_text(id, 128)?;
        }
        if self.activation_id != self.task_id {
            return Err(invalid("inconsistent activation task"));
        }
        if let Some(at) = self.applied_at {
            timestamp(at)?;
        }
        if let Some(error) = &self.error {
            validate_workflow_error(error)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleChildKind {
    Task,
    Workflow,
}
impl ConsoleChildKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Task => "task",
            Self::Workflow => "workflow",
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleWorkflowChild {
    pub workflow_id: String,
    pub creating_activation_id: String,
    pub creating_revision: ConsoleU64,
    pub kind: ConsoleChildKind,
    pub command_key: String,
    pub target_id: String,
    pub task_state: Option<TaskState>,
    pub workflow_state: Option<WorkflowState>,
    pub consumed: bool,
}
impl ConsoleRecord for ConsoleWorkflowChild {
    fn position(&self) -> ConsolePosition {
        vec![
            ConsoleKey::Number(self.creating_revision),
            ConsoleKey::Text(self.kind.as_str().into()),
            ConsoleKey::Text(self.command_key.clone()),
            ConsoleKey::Text(self.target_id.clone()),
        ]
    }
    fn validate(&self) -> Result<()> {
        for id in [
            &self.workflow_id,
            &self.creating_activation_id,
            &self.command_key,
            &self.target_id,
        ] {
            validate_text(id, 128)?;
        }
        if (self.kind == ConsoleChildKind::Task)
            != (self.task_state.is_some() && self.workflow_state.is_none())
            || (self.kind == ConsoleChildKind::Workflow)
                != (self.workflow_state.is_some() && self.task_state.is_none())
        {
            return Err(invalid("inconsistent workflow child state"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleWaitKind {
    Event,
    Timer,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleWorkflowWait {
    pub workflow_id: String,
    pub wait_key: String,
    pub activation_id: String,
    pub kind: ConsoleWaitKind,
    pub deadline: Option<Timestamp>,
    pub registered_at: Timestamp,
    pub closed_at: Option<Timestamp>,
}
impl ConsoleRecord for ConsoleWorkflowWait {
    fn position(&self) -> ConsolePosition {
        vec![
            ConsoleKey::Number(ConsoleU64(self.registered_at)),
            ConsoleKey::Text(self.wait_key.clone()),
        ]
    }
    fn validate(&self) -> Result<()> {
        for id in [&self.workflow_id, &self.wait_key, &self.activation_id] {
            validate_text(id, 128)?;
        }
        for at in [Some(self.registered_at), self.deadline, self.closed_at]
            .into_iter()
            .flatten()
        {
            timestamp(at)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleWorkflowWaits {
    pub page: ConsolePage<ConsoleWorkflowWait>,
    pub child_wait: Option<ConsoleChildWait>,
    pub revision: ConsoleU64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleWorkflowHistory {
    pub workflow_id: String,
    pub sequence: ConsoleU64,
    pub activation_id: Option<String>,
    pub at: Timestamp,
    pub reason: String,
}
impl ConsoleRecord for ConsoleWorkflowHistory {
    fn position(&self) -> ConsolePosition {
        vec![ConsoleKey::Number(self.sequence)]
    }
    fn validate(&self) -> Result<()> {
        validate_text(&self.workflow_id, 128)?;
        validate_text(&self.reason, 128)?;
        if let Some(id) = &self.activation_id {
            validate_text(id, 128)?;
        }
        timestamp(self.at)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleLocalStep {
    pub workflow_id: String,
    pub activation_id: String,
    pub step_key: String,
    pub callable: String,
    pub attempt_id: String,
    pub accepted_at: Timestamp,
}
impl ConsoleRecord for ConsoleLocalStep {
    fn position(&self) -> ConsolePosition {
        vec![ConsoleKey::Text(self.step_key.clone())]
    }
    fn validate(&self) -> Result<()> {
        for id in [
            &self.workflow_id,
            &self.activation_id,
            &self.step_key,
            &self.attempt_id,
        ] {
            validate_text(id, 128)?;
        }
        validate_text(&self.callable, 512)?;
        timestamp(self.accepted_at)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleObservedWorkflowStatus {
    pub workflow: ConsoleWorkflowStatus,
    pub observed_at: Timestamp,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleWorkflowEvent {
    pub workflow_id: String,
    pub key: String,
    pub event: WorkflowEvent,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleWorkflowEventReceipt {
    pub workflow_id: String,
    pub key: String,
    pub event_id: String,
    pub event_source: String,
    pub accepted_at: Timestamp,
    pub already_accepted: bool,
}
impl From<WorkflowEventReceipt> for ConsoleWorkflowEventReceipt {
    fn from(v: WorkflowEventReceipt) -> Self {
        Self {
            workflow_id: v.workflow_id,
            key: v.key,
            event_id: v.event_id,
            event_source: v.event_source,
            accepted_at: v.accepted_at,
            already_accepted: v.already_accepted,
        }
    }
}
