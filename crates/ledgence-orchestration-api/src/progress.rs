//! Bounded resumable lifecycle observations. Progress is not an execution command
//! or an application-output stream. The cursor never grants authorization.
use crate::*;
use serde::{Deserializer, Serializer, de::Error as _};

pub const EXECUTION_PROGRESS_MAX_LIMIT: u32 = 100;
pub const EXECUTION_PROGRESS_QUERY_MAX_BYTES: usize = 8192;
pub const EXECUTION_PROGRESS_PAGE_MAX_BYTES: usize = 256 * 1024;
pub const EXECUTION_PROGRESS_CURSOR_MAX_BYTES: usize = 4096;
const MAX_TIMESTAMP: Timestamp = 253_402_300_799_999;

/// Full-width sequence on the wire: a canonical decimal string, safe for browsers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProgressSequence(pub u64);
impl Serialize for ProgressSequence {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}
impl<'de> Deserialize<'de> for ProgressSequence {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        let value = text
            .parse::<u64>()
            .map_err(|_| D::Error::custom("invalid progress sequence"))?;
        if value.to_string() != text {
            return Err(D::Error::custom("noncanonical progress sequence"));
        }
        Ok(Self(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutionProgressTarget {
    Task { id: String },
    Workflow { id: String },
}
impl ExecutionProgressTarget {
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
    fn terminal(&self, state: &str) -> Result<bool> {
        let value = serde_json::Value::String(state.to_owned());
        match self {
            Self::Task { .. } => {
                let state: TaskState = serde_json::from_value(value)
                    .map_err(|_| invalid("invalid task progress state"))?;
                Ok(matches!(
                    state,
                    TaskState::Succeeded | TaskState::Failed | TaskState::Cancelled
                ))
            }
            Self::Workflow { .. } => {
                let state: WorkflowState = serde_json::from_value(value)
                    .map_err(|_| invalid("invalid workflow progress state"))?;
                Ok(state.is_terminal())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionProgressQuery {
    pub scope: Scope,
    pub target: ExecutionProgressTarget,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: u32,
}
fn default_limit() -> u32 {
    EXECUTION_PROGRESS_MAX_LIMIT
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    scope: Scope,
    target: ExecutionProgressTarget,
    after: ProgressSequence,
}
impl ExecutionProgressQuery {
    /// Validate the request and return its exclusive consumed-through position.
    pub fn validate(&self) -> Result<u64> {
        self.scope.validate()?;
        self.target.validate()?;
        if !(1..=EXECUTION_PROGRESS_MAX_LIMIT).contains(&self.limit) {
            return Err(invalid("progress limit must be between 1 and 100"));
        }
        crate::submission::check_encoded_size(
            self,
            EXECUTION_PROGRESS_QUERY_MAX_BYTES,
            "progress query",
        )?;
        let Some(text) = &self.cursor else {
            return Ok(0);
        };
        let bytes = crate::cursor::decode(text, EXECUTION_PROGRESS_CURSOR_MAX_BYTES)
            .ok_or_else(|| invalid("invalid progress cursor"))?;
        let cursor: Cursor = decode_unique_json(&bytes, EXECUTION_PROGRESS_CURSOR_MAX_BYTES)?;
        if cursor.version != 1
            || cursor.scope != self.scope
            || cursor.target != self.target
            || self.cursor_for(cursor.after.0)? != *text
        {
            return Err(invalid("progress cursor does not match this execution"));
        }
        Ok(cursor.after.0)
    }
    /// Adapter helper. Clients treat returned cursors as opaque checkpoints.
    pub fn cursor_for(&self, after: u64) -> Result<String> {
        self.scope.validate()?;
        self.target.validate()?;
        let bytes = serde_json::to_vec(&Cursor {
            version: 1,
            scope: self.scope.clone(),
            target: self.target.clone(),
            after: ProgressSequence(after),
        })
        .map_err(|_| invalid("could not encode progress cursor"))?;
        let cursor = crate::cursor::encode(&bytes);
        if cursor.len() > EXECUTION_PROGRESS_CURSOR_MAX_BYTES {
            return Err(invalid("progress cursor exceeds bound"));
        }
        Ok(cursor)
    }
}

/// Identity is (scope, target kind, target ID, sequence), not its timestamp.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionProgressEvent {
    pub sequence: ProgressSequence,
    pub at: Timestamp,
    pub reason: String,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub activation_id: Option<String>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub attempt_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionProgressPage {
    pub scope: Scope,
    pub target: ExecutionProgressTarget,
    pub state: String,
    /// Logical outcome only. Late operational events can still be appended.
    pub terminal: bool,
    /// Owner state, this tail and events must come from the same read snapshot.
    pub high_watermark: ProgressSequence,
    pub events: Vec<ExecutionProgressEvent>,
    /// Always present, even on an empty page. Never advances past returned events.
    pub next_cursor: String,
    pub has_more: bool,
}
impl ExecutionProgressPage {
    pub fn validate_for(&self, query: &ExecutionProgressQuery) -> Result<()> {
        let mut position = query.validate()?;
        if self.scope != query.scope
            || self.target != query.target
            || self.terminal != self.target.terminal(&self.state)?
            || self.events.len() > query.limit as usize
            || position > self.high_watermark.0
        {
            return Err(invalid("inconsistent progress snapshot"));
        }
        for event in &self.events {
            if position.checked_add(1) != Some(event.sequence.0)
                || event.sequence.0 > self.high_watermark.0
                || event.at > MAX_TIMESTAMP
            {
                return Err(invalid("inconsistent progress event position"));
            }
            validate_text(&event.reason, 128)?;
            for id in [&event.activation_id, &event.attempt_id]
                .into_iter()
                .flatten()
            {
                validate_text(id, 128)?;
            }
            match &self.target {
                ExecutionProgressTarget::Task { .. } => {
                    if event.activation_id.is_some() {
                        return Err(invalid("task progress has workflow activation"));
                    }
                    serde_json::from_value::<TransitionReason>(serde_json::Value::String(
                        event.reason.clone(),
                    ))
                    .map_err(|_| invalid("invalid task progress reason"))?;
                }
                ExecutionProgressTarget::Workflow { .. } => {
                    if event.attempt_id.is_some() {
                        return Err(invalid("workflow progress has task attempt"));
                    }
                }
            }
            position = event.sequence.0;
        }
        if self.next_cursor != query.cursor_for(position)?
            || self.has_more != (position < self.high_watermark.0)
            || (self.events.is_empty() && self.has_more)
        {
            return Err(invalid("inconsistent progress continuation"));
        }
        crate::submission::check_encoded_size(
            self,
            EXECUTION_PROGRESS_PAGE_MAX_BYTES,
            "progress page",
        )?;
        Ok(())
    }
}

/// Complete optional capability: bounded metadata read without recovery, side
/// effects, retained connections, worker slots, or implicit descendant traversal.
pub trait ExecutionProgressReader: Send + Sync {
    fn execution_progress<'a>(
        &'a self,
        query: &'a ExecutionProgressQuery,
    ) -> ContractFuture<'a, ExecutionProgressPage>;
}
fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn query() -> ExecutionProgressQuery {
        ExecutionProgressQuery {
            scope: Scope {
                tenant_id: "acme".into(),
                namespace: "demo".into(),
            },
            target: ExecutionProgressTarget::Task {
                id: "task_1".into(),
            },
            cursor: None,
            limit: 100,
        }
    }
    #[test]
    fn decimal_sequences_reject_lossy_or_noncanonical_json() {
        for value in [0, 9_007_199_254_740_993, u64::MAX] {
            let text = serde_json::to_string(&ProgressSequence(value)).unwrap();
            assert_eq!(
                serde_json::from_str::<ProgressSequence>(&text).unwrap().0,
                value
            );
        }
        for text in [
            "1",
            "true",
            "null",
            "\"01\"",
            "\"+1\"",
            "\"18446744073709551616\"",
        ] {
            assert!(serde_json::from_str::<ProgressSequence>(text).is_err());
        }
    }
    #[test]
    fn cursor_binds_kind_id_scope_and_version() {
        let original = query();
        let mut other = original.clone();
        other.cursor = Some(original.cursor_for(u64::MAX).unwrap());
        assert_eq!(other.validate().unwrap(), u64::MAX);
        other.target = ExecutionProgressTarget::Workflow {
            id: "task_1".into(),
        };
        assert!(other.validate().is_err());
        other.target = original.target.clone();
        other.scope.namespace = "other".into();
        assert!(other.validate().is_err());
        other = original.clone();
        other.cursor = Some(crate::cursor::encode(br#"{"version":2,"scope":{"tenant_id":"acme","namespace":"demo"},"target":{"kind":"task","id":"task_1"},"after":"0"}"#));
        assert!(other.validate().is_err());
    }
    #[test]
    fn page_rejects_skipped_or_invented_positions_and_terminal_claims() {
        let query = query();
        let mut page = ExecutionProgressPage {
            scope: query.scope.clone(),
            target: query.target.clone(),
            state: "active".into(),
            terminal: false,
            high_watermark: ProgressSequence(2),
            events: vec![ExecutionProgressEvent {
                sequence: ProgressSequence(1),
                at: 1,
                reason: "submitted".into(),
                activation_id: None,
                attempt_id: None,
            }],
            next_cursor: query.cursor_for(1).unwrap(),
            has_more: true,
        };
        page.validate_for(&query).unwrap();
        page.next_cursor = query.cursor_for(2).unwrap();
        assert!(page.validate_for(&query).is_err());
        page.next_cursor = query.cursor_for(1).unwrap();
        page.terminal = true;
        assert!(page.validate_for(&query).is_err());
        page.state = "succeeded".into();
        page.validate_for(&query).unwrap(); // Terminal can still have unread history.
        page.events[0].sequence = ProgressSequence(2);
        assert!(page.validate_for(&query).is_err());
    }
    #[test]
    fn event_requires_explicit_nullable_identity_fields() {
        let complete = serde_json::json!({"sequence":"1", "at":1, "reason":"submitted", "activation_id":null, "attempt_id":null});
        assert!(serde_json::from_value::<ExecutionProgressEvent>(complete.clone()).is_ok());
        for field in ["activation_id", "attempt_id"] {
            let mut missing = complete.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<ExecutionProgressEvent>(missing).is_err());
        }
    }
    #[test]
    fn caught_up_cursor_is_preserved_at_full_width_tail() {
        let mut query = query();
        query.cursor = Some(query.cursor_for(u64::MAX).unwrap());
        let mut page = ExecutionProgressPage {
            scope: query.scope.clone(),
            target: query.target.clone(),
            state: "active".into(),
            terminal: false,
            high_watermark: ProgressSequence(u64::MAX),
            events: vec![],
            next_cursor: query.cursor.clone().unwrap(),
            has_more: false,
        };
        page.validate_for(&query).unwrap();
        page.has_more = true;
        assert!(page.validate_for(&query).is_err());
    }
}
