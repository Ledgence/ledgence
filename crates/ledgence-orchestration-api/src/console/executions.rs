use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleExecutionKind {
    Task,
    Workflow,
}
impl ConsoleExecutionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Task => "task",
            Self::Workflow => "workflow",
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleExecutionIdentity {
    pub kind: ConsoleExecutionKind,
    pub id: String,
}
impl ConsoleExecutionIdentity {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.id, 128)
    }
}
/// Preserve durable states rather than relabelling active tasks as running or
/// treating failing/cancelling workflows as terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsoleExecutionState {
    Queued,
    Active,
    Running,
    Waiting,
    Failing,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
}
impl ConsoleExecutionState {
    pub fn supports(self, kind: ConsoleExecutionKind) -> bool {
        match self {
            Self::Queued | Self::Active => kind == ConsoleExecutionKind::Task,
            Self::Running | Self::Waiting | Self::Failing | Self::Cancelling => {
                kind == ConsoleExecutionKind::Workflow
            }
            _ => true,
        }
    }
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleExecutionSummary {
    pub kind: ConsoleExecutionKind,
    pub id: String,
    pub descriptor: ConsoleProgramDescriptor,
    pub queue: String,
    pub state: ConsoleExecutionState,
    pub submitted_at: Timestamp,
    pub terminal_at: Option<Timestamp>,
    pub correlation_key: Option<String>,
    /// Direct durable owner, including for a task inside a root workflow.
    pub parent_workflow_id: Option<String>,
    /// Present for children; absent on root submissions.
    pub root_workflow_id: Option<String>,
}
impl ConsoleRecord for ConsoleExecutionSummary {
    fn position(&self) -> ConsolePosition {
        vec![
            ConsoleKey::Number(ConsoleU64(self.submitted_at)),
            ConsoleKey::Text(self.kind.as_str().into()),
            ConsoleKey::Text(self.id.clone()),
        ]
    }
    fn validate(&self) -> Result<()> {
        validate_text(&self.id, 128)?;
        validate_text(&self.queue, 128)?;
        self.descriptor.validate()?;
        timestamp(self.submitted_at)?;
        if let Some(at) = self.terminal_at {
            timestamp(at)?;
        }
        TaskFilters {
            correlation_key: self.correlation_key.clone(),
            ..TaskFilters::default()
        }
        .validate()?;
        for id in [&self.parent_workflow_id, &self.root_workflow_id]
            .into_iter()
            .flatten()
        {
            validate_text(id, 128)?;
        }
        if !self.state.supports(self.kind)
            || self.state.is_terminal() != self.terminal_at.is_some()
            || self.parent_workflow_id.is_some() != self.root_workflow_id.is_some()
            || self.terminal_at.is_some_and(|at| at < self.submitted_at)
            || (self.kind == ConsoleExecutionKind::Workflow
                && [&self.parent_workflow_id, &self.root_workflow_id]
                    .into_iter()
                    .flatten()
                    .any(|id| id == &self.id))
        {
            return Err(invalid("inconsistent execution summary"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsoleExecutionFilters {
    pub kind: Option<ConsoleExecutionKind>,
    pub state: Option<ConsoleExecutionState>,
    pub program_id: Option<String>,
    pub version: Option<String>,
    pub queue: Option<String>,
    pub correlation_key: Option<String>,
    pub execution_id: Option<String>,
    #[serde(default)]
    pub include_children: bool,
    pub submitted_from: Option<Timestamp>,
    pub submitted_until: Option<Timestamp>,
}
impl ConsoleExecutionFilters {
    /// Program histories and exact ID searches deliberately expand root scope.
    /// A raw ID may match both kinds; callers can also supply a typed kind.
    pub fn includes_children(&self) -> bool {
        self.include_children || self.program_id.is_some() || self.execution_id.is_some()
    }
    pub fn validate(&self) -> Result<()> {
        TaskFilters {
            state: None,
            queue: self.queue.clone(),
            correlation_key: self.correlation_key.clone(),
            submitted_from: self.submitted_from,
            submitted_until: self.submitted_until,
        }
        .validate()?;
        for at in [self.submitted_from, self.submitted_until]
            .into_iter()
            .flatten()
        {
            timestamp(at)?;
        }
        if let Some(id) = &self.execution_id {
            validate_text(id, 128)?;
        }
        match (&self.program_id, &self.version) {
            (Some(id), version) => ProgramRef {
                id: id.clone(),
                version: version.clone().unwrap_or_else(|| "validation".into()),
            }
            .validate()?,
            (None, Some(_)) => return Err(invalid("version requires program_id")),
            (None, None) => (),
        }
        if let (Some(kind), Some(state)) = (self.kind, self.state)
            && !state.supports(kind)
        {
            return Err(invalid("execution state is incompatible with kind"));
        }
        Ok(())
    }
    pub fn matches(&self, value: &ConsoleExecutionSummary) -> bool {
        self.kind.is_none_or(|v| v == value.kind)
            && self.state.is_none_or(|v| v == value.state)
            && self
                .program_id
                .as_ref()
                .is_none_or(|v| v == &value.descriptor.program.id)
            && self
                .version
                .as_ref()
                .is_none_or(|v| v == &value.descriptor.program.version)
            && self.queue.as_ref().is_none_or(|v| v == &value.queue)
            && self
                .correlation_key
                .as_ref()
                .is_none_or(|v| Some(v) == value.correlation_key.as_ref())
            && self.execution_id.as_ref().is_none_or(|v| v == &value.id)
            && (self.includes_children() || value.parent_workflow_id.is_none())
            && self
                .submitted_from
                .is_none_or(|at| value.submitted_at >= at)
            && self
                .submitted_until
                .is_none_or(|at| value.submitted_at < at)
    }
}
