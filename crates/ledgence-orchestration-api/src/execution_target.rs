//! A task or workflow identity shared by execution-scoped capabilities.
use crate::{Result, validate_text};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutionTarget {
    Task { id: String },
    Workflow { id: String },
}
impl ExecutionTarget {
    pub fn id(&self) -> &str {
        match self {
            Self::Task { id } | Self::Workflow { id } => id,
        }
    }
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Task { .. } => "task",
            Self::Workflow { .. } => "workflow",
        }
    }
    pub fn validate(&self) -> Result<()> {
        validate_text(self.id(), 128)
    }
}
