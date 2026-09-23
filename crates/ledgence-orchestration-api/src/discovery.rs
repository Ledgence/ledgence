//! Bounded discovery of committed task metadata using live keyset pagination.

use crate::*;
use serde::Deserializer;

pub const TASK_LIST_DEFAULT_LIMIT: u32 = 50;
pub const TASK_LIST_MAX_LIMIT: u32 = 100;
pub const TASK_CURSOR_MAX_BYTES: usize = 8192;
pub const TASK_PAGE_MAX_BYTES: usize = 2 * 1024 * 1024;
const MAX_TIMESTAMP: Timestamp = 253_402_300_799_999;

/// Exact metadata filters. Submission bounds are inclusive from, exclusive until.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskFilters {
    pub state: Option<TaskState>,
    pub queue: Option<String>,
    pub submitted_from: Option<Timestamp>,
    pub submitted_until: Option<Timestamp>,
    /// An empty string matches empty correlation metadata; absence applies no filter.
    pub correlation_key: Option<String>,
}

impl TaskFilters {
    pub fn validate(&self) -> Result<()> {
        if let Some(queue) = &self.queue {
            validate_text(queue, 128)?;
        }
        if let Some(key) = &self.correlation_key
            && (key.len() > 512 || key.chars().any(char::is_control))
        {
            return Err(invalid("invalid correlation key filter"));
        }
        if [self.submitted_from, self.submitted_until]
            .into_iter()
            .flatten()
            .any(|at| at > MAX_TIMESTAMP)
            || matches!((self.submitted_from, self.submitted_until), (Some(from), Some(until)) if from >= until)
        {
            return Err(invalid("invalid submission time range"));
        }
        Ok(())
    }

    /// Whether a status satisfies every requested exact filter.
    pub fn matches(&self, task: &TaskStatus) -> bool {
        self.state.is_none_or(|state| state == task.state)
            && self.queue.as_ref().is_none_or(|queue| queue == &task.queue)
            && self
                .submitted_from
                .is_none_or(|from| task.submitted_at >= from)
            && self
                .submitted_until
                .is_none_or(|until| task.submitted_at < until)
            && self
                .correlation_key
                .as_ref()
                .is_none_or(|key| task.correlation_key.as_ref() == Some(key))
    }
}

/// One bounded read. Cursors bind the scope and filters, but allow a new page size.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskListQuery {
    #[serde(default)]
    pub filters: TaskFilters,
    #[serde(default = "default_limit")]
    pub limit: u32,
    pub cursor: Option<String>,
}

impl Default for TaskListQuery {
    fn default() -> Self {
        Self {
            filters: TaskFilters::default(),
            limit: TASK_LIST_DEFAULT_LIMIT,
            cursor: None,
        }
    }
}

/// Immutable seek key, ordered by submission time and then UTF-8 task ID bytes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskPosition {
    pub submitted_at: Timestamp,
    pub task_id: String,
}
impl From<&TaskStatus> for TaskPosition {
    fn from(task: &TaskStatus) -> Self {
        Self {
            submitted_at: task.submitted_at,
            task_id: task.task_id.clone(),
        }
    }
}
impl TaskPosition {
    fn validate(&self, filters: &TaskFilters) -> Result<()> {
        validate_text(&self.task_id, 128)?;
        if self.submitted_at > MAX_TIMESTAMP
            || filters
                .submitted_from
                .is_some_and(|from| self.submitted_at < from)
            || filters
                .submitted_until
                .is_some_and(|until| self.submitted_at >= until)
        {
            return Err(invalid("invalid cursor position"));
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u32,
    scope: Scope,
    filters: TaskFilters,
    position: TaskPosition,
}

impl TaskListQuery {
    /// Reject invalid bounds or a cursor from another scope or filter set.
    pub fn validate(&self, scope: &Scope) -> Result<Option<TaskPosition>> {
        scope.validate()?;
        self.filters.validate()?;
        if !(1..=TASK_LIST_MAX_LIMIT).contains(&self.limit) {
            return Err(invalid("task list limit must be between 1 and 100"));
        }
        self.cursor
            .as_deref()
            .map(|cursor| self.decode_cursor(scope, cursor))
            .transpose()
    }

    /// Encode the last returned position. This token is opaque, not a snapshot.
    pub fn next_cursor(&self, scope: &Scope, position: &TaskPosition) -> Result<String> {
        self.validate(scope)?;
        position.validate(&self.filters)?;
        let bytes = serde_json::to_vec(&Cursor {
            version: 1,
            scope: scope.clone(),
            filters: self.filters.clone(),
            position: position.clone(),
        })
        .map_err(|_| invalid("could not encode task cursor"))?;
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut cursor = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            cursor.push(char::from(HEX[usize::from(byte >> 4)]));
            cursor.push(char::from(HEX[usize::from(byte & 15)]));
        }
        if cursor.len() > TASK_CURSOR_MAX_BYTES {
            return Err(invalid("task cursor exceeds size limit"));
        }
        Ok(cursor)
    }

    fn decode_cursor(&self, scope: &Scope, text: &str) -> Result<TaskPosition> {
        if text.is_empty() || text.len() > TASK_CURSOR_MAX_BYTES || !text.len().is_multiple_of(2) {
            return Err(invalid("invalid task cursor"));
        }
        let digit = |byte| match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err(invalid("invalid task cursor")),
        };
        let bytes = text
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| Ok((digit(pair[0])? << 4) | digit(pair[1])?))
            .collect::<Result<Vec<u8>>>()?;
        let cursor: Cursor = decode_unique_json(&bytes, TASK_CURSOR_MAX_BYTES / 2)
            .map_err(|_| invalid("invalid task cursor"))?;
        if cursor.version != 1 || cursor.scope != *scope || cursor.filters != self.filters {
            return Err(invalid("task cursor does not match scope or filters"));
        }
        cursor.position.validate(&self.filters)?;
        Ok(cursor.position)
    }
}

/// A coherent committed view for this read; later pages use later read snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskPage {
    pub items: Vec<TaskStatus>,
    #[serde(deserialize_with = "required_option")]
    pub next_cursor: Option<String>,
}
impl TaskPage {
    /// Validate an adapter response, including strict descending continuation.
    pub fn validate(&self, scope: &Scope, query: &TaskListQuery) -> Result<()> {
        let mut previous = query.validate(scope)?;
        let inconsistent = || ContractError::Unavailable("inconsistent task page".into());
        if self.items.len() > query.limit as usize {
            return Err(inconsistent());
        }
        let mut ids = std::collections::HashSet::with_capacity(self.items.len());
        for task in &self.items {
            task.validate()?;
            let position = TaskPosition::from(task);
            if task.scope != *scope
                || !query.filters.matches(task)
                || previous
                    .as_ref()
                    .is_some_and(|previous| position >= *previous)
                || !ids.insert(&task.task_id)
            {
                return Err(inconsistent());
            }
            previous = Some(position);
        }
        if let Some(cursor) = &self.next_cursor {
            let position = query
                .decode_cursor(scope, cursor)
                .map_err(|_| inconsistent())?;
            if self.items.len() != query.limit as usize
                || self.items.last().map(TaskPosition::from).as_ref() != Some(&position)
            {
                return Err(inconsistent());
            }
        }
        crate::submission::check_encoded_size(self, TASK_PAGE_MAX_BYTES, "task page")
            .map_err(|_| inconsistent())
    }
}

fn default_limit() -> u32 {
    TASK_LIST_DEFAULT_LIMIT
}
fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}
fn required_option<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
#[path = "discovery_tests.rs"]
mod tests;
