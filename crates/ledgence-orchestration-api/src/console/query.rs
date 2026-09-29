use super::*;
use serde_json::Value;

/// A closed set of bounded metadata reads. Application services bind Scope;
/// adapter queries cannot add a browser-supplied tenant or arbitrary sort order.
#[derive(Debug, Clone)]
pub enum ConsoleQuery {
    Explorer {
        workflow_id: String,
        page: ConsolePagination,
    },
    WorkflowInput {
        workflow_id: String,
    },
    Ancestry {
        execution: ConsoleExecutionIdentity,
    },
    AttemptObservations {
        attempt_id: String,
    },
    Executions {
        filters: ConsoleExecutionFilters,
        page: ConsolePagination,
    },
    Tasks {
        filters: TaskFilters,
        page: ConsolePagination,
    },
    Attempts {
        task_id: String,
        page: ConsolePagination,
    },
    Attempt {
        attempt_id: String,
    },
    Workflows {
        filters: ConsoleWorkflowFilters,
        page: ConsolePagination,
    },
    Workflow {
        workflow_id: String,
    },
    Activations {
        workflow_id: String,
        page: ConsolePagination,
    },
    Children {
        workflow_id: String,
        page: ConsolePagination,
    },
    Waits {
        workflow_id: String,
        page: ConsolePagination,
    },
    History {
        workflow_id: String,
        page: ConsolePagination,
    },
    LocalSteps {
        workflow_id: String,
        activation_id: String,
        page: ConsolePagination,
    },
}
impl ConsoleQuery {
    pub fn pagination(&self) -> Option<&ConsolePagination> {
        match self {
            Self::Executions { page, .. }
            | Self::Explorer { page, .. }
            | Self::Tasks { page, .. }
            | Self::Attempts { page, .. }
            | Self::Workflows { page, .. }
            | Self::Activations { page, .. }
            | Self::Children { page, .. }
            | Self::Waits { page, .. }
            | Self::History { page, .. }
            | Self::LocalSteps { page, .. } => Some(page),
            Self::Attempt { .. }
            | Self::Workflow { .. }
            | Self::WorkflowInput { .. }
            | Self::Ancestry { .. }
            | Self::AttemptObservations { .. } => None,
        }
    }
    pub fn binding(&self, scope: &Scope) -> Result<ConsoleCursorBinding> {
        scope.validate()?;
        let (endpoint, parent, filters, descending, numeric_keys) = match self {
            Self::Explorer { workflow_id, .. } => (
                "workflows/explorer",
                vec![workflow_id.clone()],
                Ok(Value::Null),
                false,
                vec![true, false, false],
            ),
            Self::WorkflowInput { workflow_id } => (
                "workflows/input",
                vec![workflow_id.clone()],
                Ok(Value::Null),
                false,
                vec![],
            ),
            Self::Ancestry { execution } => (
                "executions/ancestry",
                vec![execution.id.clone()],
                serde_json::to_value(execution.kind),
                false,
                vec![],
            ),
            Self::AttemptObservations { attempt_id } => (
                "attempts/observations",
                vec![attempt_id.clone()],
                Ok(Value::Null),
                false,
                vec![],
            ),
            Self::Executions { filters, .. } => {
                filters.validate()?;
                (
                    "executions",
                    vec![],
                    serde_json::to_value(filters),
                    true,
                    vec![true, false, false],
                )
            }
            Self::Tasks { filters, .. } => {
                filters.validate()?;
                (
                    "tasks",
                    vec![],
                    serde_json::to_value(filters),
                    true,
                    vec![true, false],
                )
            }
            Self::Attempts { task_id, .. } => (
                "tasks/attempts",
                vec![task_id.clone()],
                Ok(Value::Null),
                true,
                vec![true, false],
            ),
            Self::Attempt { attempt_id } => (
                "attempts/inspect",
                vec![attempt_id.clone()],
                Ok(Value::Null),
                false,
                vec![],
            ),
            Self::Workflows { filters, .. } => {
                filters.validate()?;
                (
                    "workflows",
                    vec![],
                    serde_json::to_value(filters),
                    true,
                    vec![true, false],
                )
            }
            Self::Workflow { workflow_id } => (
                "workflows/inspect",
                vec![workflow_id.clone()],
                Ok(Value::Null),
                false,
                vec![],
            ),
            Self::Activations { workflow_id, .. } => (
                "workflows/activations",
                vec![workflow_id.clone()],
                Ok(Value::Null),
                false,
                vec![true, false],
            ),
            Self::Children { workflow_id, .. } => (
                "workflows/children",
                vec![workflow_id.clone()],
                Ok(Value::Null),
                false,
                vec![true, false, false, false],
            ),
            Self::Waits { workflow_id, .. } => (
                "workflows/waits",
                vec![workflow_id.clone()],
                Ok(Value::Null),
                false,
                vec![true, false],
            ),
            Self::History { workflow_id, .. } => (
                "workflows/history",
                vec![workflow_id.clone()],
                Ok(Value::Null),
                false,
                vec![true],
            ),
            Self::LocalSteps {
                workflow_id,
                activation_id,
                ..
            } => (
                "workflows/local-steps",
                vec![workflow_id.clone(), activation_id.clone()],
                Ok(Value::Null),
                false,
                vec![false],
            ),
        };
        for id in &parent {
            validate_text(id, 128)?;
        }
        Ok(ConsoleCursorBinding {
            endpoint,
            scope: scope.clone(),
            parent,
            filters: filters.map_err(|_| invalid("invalid console filters"))?,
            descending,
            numeric_keys,
        })
    }
    pub fn validate(&self, scope: &Scope) -> Result<Option<ConsolePosition>> {
        let binding = self.binding(scope)?;
        self.pagination()
            .map_or(Ok(None), |page| page.validate(&binding))
    }
}

/// Internal result variants are unwrapped by HTTP into the corresponding explicit
/// response DTO. A mismatched or invalid adapter reply is an availability error.
#[derive(Debug, Clone)]
pub enum ConsoleQueryReply {
    Explorer(ConsoleWorkflowExplorer),
    WorkflowInput(ConsoleWorkflowInput),
    Ancestry(ConsoleAncestry),
    AttemptObservations(ConsoleAttemptObservations),
    Executions(ConsolePage<ConsoleExecutionSummary>),
    Tasks(ConsolePage<ConsoleTaskSummary>),
    Attempts(ConsolePage<ConsoleAttemptSummary>),
    Attempt(ConsoleAttemptDetail),
    Workflows(ConsolePage<ConsoleWorkflowSummary>),
    Workflow(ConsoleWorkflowDetail),
    Activations(ConsolePage<ConsoleActivation>),
    Children(ConsolePage<ConsoleWorkflowChild>),
    Waits(ConsoleWorkflowWaits),
    History(ConsolePage<ConsoleWorkflowHistory>),
    LocalSteps(ConsolePage<ConsoleLocalStep>),
}
impl ConsoleQueryReply {
    pub fn validate(&self, scope: &Scope, query: &ConsoleQuery) -> Result<()> {
        query.validate(scope)?;
        let binding = query.binding(scope)?;
        let mismatch = || inconsistent("console adapter returned inconsistent observation");
        match (self, query) {
            (Self::Explorer(reply), ConsoleQuery::Explorer { workflow_id, page }) => {
                reply.validate(workflow_id, page, &binding)?
            }
            (Self::WorkflowInput(reply), ConsoleQuery::WorkflowInput { workflow_id }) => {
                reply.validate(workflow_id)?
            }
            (Self::Ancestry(reply), ConsoleQuery::Ancestry { execution }) => {
                reply.validate(execution)?
            }
            (
                Self::AttemptObservations(reply),
                ConsoleQuery::AttemptObservations { attempt_id },
            ) => {
                reply.validate()?;
                if &reply.attempt_id != attempt_id {
                    return Err(mismatch());
                }
            }
            (Self::Executions(reply), ConsoleQuery::Executions { filters, page }) => {
                reply.validate(page, &binding)?;
                if reply.items.iter().any(|r| !filters.matches(r)) {
                    return Err(mismatch());
                }
            }
            (Self::Tasks(reply), ConsoleQuery::Tasks { filters, page }) => {
                reply.validate(page, &binding)?;
                if reply.items.iter().any(|r| !r.task.matches(filters)) {
                    return Err(mismatch());
                }
            }
            (Self::Attempts(reply), ConsoleQuery::Attempts { task_id, page }) => {
                reply.validate(page, &binding)?;
                if reply.items.iter().any(|r| &r.task_id != task_id) {
                    return Err(mismatch());
                }
            }
            (Self::Attempt(reply), ConsoleQuery::Attempt { attempt_id }) => {
                reply.attempt.validate().map_err(|_| mismatch())?;
                reply.descriptor.validate().map_err(|_| mismatch())?;
                timestamp(reply.observed_at).map_err(|_| mismatch())?;
                metadata_size(reply).map_err(|_| mismatch())?;
                if &reply.attempt.attempt_id != attempt_id {
                    return Err(mismatch());
                }
            }
            (Self::Workflows(reply), ConsoleQuery::Workflows { filters, page }) => {
                reply.validate(page, &binding)?;
                if reply.items.iter().any(|r| !filters.matches(r)) {
                    return Err(mismatch());
                }
            }
            (Self::Workflow(reply), ConsoleQuery::Workflow { workflow_id }) => {
                reply.summary.validate().map_err(|_| mismatch())?;
                timestamp(reply.observed_at).map_err(|_| mismatch())?;
                validate_text(&reply.continuation, 128).map_err(|_| mismatch())?;
                validate_child_wait(reply.child_wait.as_ref()).map_err(|_| mismatch())?;
                if let Some(key) = &reply.external_wait_key {
                    validate_text(key, 128).map_err(|_| mismatch())?;
                }
                metadata_size(reply).map_err(|_| mismatch())?;
                if &reply.summary.workflow.workflow_id != workflow_id {
                    return Err(mismatch());
                }
            }
            (Self::Activations(reply), ConsoleQuery::Activations { workflow_id, page }) => {
                reply.validate(page, &binding)?;
                if reply.items.iter().any(|r| &r.workflow_id != workflow_id) {
                    return Err(mismatch());
                }
            }
            (Self::Children(reply), ConsoleQuery::Children { workflow_id, page }) => {
                reply.validate(page, &binding)?;
                if reply.items.iter().any(|r| &r.workflow_id != workflow_id) {
                    return Err(mismatch());
                }
            }
            (Self::Waits(reply), ConsoleQuery::Waits { workflow_id, page }) => {
                reply.page.validate(page, &binding)?;
                validate_child_wait(reply.child_wait.as_ref()).map_err(|_| mismatch())?;
                metadata_size(reply).map_err(|_| mismatch())?;
                if reply
                    .page
                    .items
                    .iter()
                    .any(|r| &r.workflow_id != workflow_id)
                {
                    return Err(mismatch());
                }
            }
            (Self::History(reply), ConsoleQuery::History { workflow_id, page }) => {
                reply.validate(page, &binding)?;
                if reply.items.iter().any(|r| &r.workflow_id != workflow_id) {
                    return Err(mismatch());
                }
            }
            (
                Self::LocalSteps(reply),
                ConsoleQuery::LocalSteps {
                    workflow_id,
                    activation_id,
                    page,
                },
            ) => {
                reply.validate(page, &binding)?;
                if reply
                    .items
                    .iter()
                    .any(|r| &r.workflow_id != workflow_id || &r.activation_id != activation_id)
                {
                    return Err(mismatch());
                }
            }
            _ => return Err(mismatch()),
        }
        Ok(())
    }
}
fn validate_child_wait(wait: Option<&ConsoleChildWait>) -> Result<()> {
    if let Some(wait) = wait {
        validate_text(&wait.activation_id, 128)?;
        if wait.command_keys.len() > WORKFLOW_MAX_COMMANDS {
            return Err(invalid("too many child wait keys"));
        }
        let mut keys = std::collections::BTreeSet::new();
        for key in &wait.command_keys {
            validate_text(key, 128)?;
            if !keys.insert(key) {
                return Err(invalid("duplicate child wait key"));
            }
        }
    }
    Ok(())
}
/// Consistent, bounded reads; no acquisition, expiry, renewal or workflow wake.
pub trait ConsoleQueryStore: Send + Sync {
    fn query_console<'a>(
        &'a self,
        scope: &'a Scope,
        query: &'a ConsoleQuery,
    ) -> ContractFuture<'a, ConsoleQueryReply>;
}
/// Scope is immutable application context, never a browser parameter.
pub trait ConsoleQueryService: Send + Sync {
    fn query_console<'a>(
        &'a self,
        query: &'a ConsoleQuery,
    ) -> ContractFuture<'a, ConsoleQueryReply>;
}
