use super::*;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsolePagination {
    /// Maximum items; a response may stop earlier to fit its metadata byte bound.
    #[serde(default = "default_limit")]
    pub limit: u32,
    pub cursor: Option<String>,
}
const fn default_limit() -> u32 {
    50
}
impl Default for ConsolePagination {
    fn default() -> Self {
        Self {
            limit: default_limit(),
            cursor: None,
        }
    }
}

/// Typed positions keep numeric order independent of the decimal-string wire
/// representation; text keys compare UTF-8 bytes, matching database COLLATE C.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ConsoleKey {
    Number(ConsoleU64),
    Text(String),
}
pub type ConsolePosition = Vec<ConsoleKey>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u32,
    endpoint: String,
    scope: Scope,
    parent: Vec<String>,
    filters: Value,
    position: ConsolePosition,
}

/// Bound by the application, never accepted as an HTTP request body. `filters`
/// is the validated metadata filter DTO; it must contain no application data.
#[derive(Debug, Clone)]
pub struct ConsoleCursorBinding {
    pub endpoint: &'static str,
    pub scope: Scope,
    pub parent: Vec<String>,
    pub filters: Value,
    pub descending: bool,
    /// True means a numeric position component, false means a text component.
    pub numeric_keys: Vec<bool>,
}
impl ConsoleCursorBinding {
    fn validate_position(&self, position: &ConsolePosition) -> Result<()> {
        self.scope.validate()?;
        if position.len() != self.numeric_keys.len() || position.is_empty() || position.len() > 4 {
            return Err(invalid("invalid console cursor position"));
        }
        for (key, number) in position.iter().zip(&self.numeric_keys) {
            match (key, number) {
                (ConsoleKey::Number(_), true) => (),
                (ConsoleKey::Text(text), false) => validate_text(text, 512)?,
                _ => return Err(invalid("invalid console cursor key type")),
            }
        }
        Ok(())
    }
}
impl ConsolePagination {
    pub fn validate(&self, binding: &ConsoleCursorBinding) -> Result<Option<ConsolePosition>> {
        if !(1..=100).contains(&self.limit) {
            return Err(invalid("console page size must be between 1 and 100"));
        }
        self.cursor
            .as_deref()
            .map(|text| self.decode(binding, text))
            .transpose()
    }
    pub fn next_cursor(
        &self,
        binding: &ConsoleCursorBinding,
        position: &ConsolePosition,
    ) -> Result<String> {
        binding.validate_position(position)?;
        let bytes = serde_json::to_vec(&Cursor {
            version: 1,
            endpoint: binding.endpoint.into(),
            scope: binding.scope.clone(),
            parent: binding.parent.clone(),
            filters: binding.filters.clone(),
            position: position.clone(),
        })
        .map_err(|_| invalid("could not encode console cursor"))?;
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let text: String = bytes
            .into_iter()
            .flat_map(|byte| {
                [
                    char::from(HEX[usize::from(byte >> 4)]),
                    char::from(HEX[usize::from(byte & 15)]),
                ]
            })
            .collect();
        if text.len() > TASK_CURSOR_MAX_BYTES {
            return Err(invalid("console cursor too long"));
        }
        Ok(text)
    }
    fn decode(&self, binding: &ConsoleCursorBinding, text: &str) -> Result<ConsolePosition> {
        if text.is_empty() || text.len() > TASK_CURSOR_MAX_BYTES || !text.len().is_multiple_of(2) {
            return Err(invalid("invalid console cursor"));
        }
        let digit = |byte| match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err(invalid("invalid console cursor")),
        };
        let bytes = text
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| Ok((digit(pair[0])? << 4) | digit(pair[1])?))
            .collect::<Result<Vec<_>>>()?;
        let cursor: Cursor = decode_unique_json(&bytes, TASK_CURSOR_MAX_BYTES / 2)?;
        if cursor.version != 1
            || cursor.endpoint != binding.endpoint
            || cursor.scope != binding.scope
            || cursor.parent != binding.parent
            || cursor.filters != binding.filters
        {
            return Err(invalid(
                "console cursor does not match endpoint, instance, parent or filters",
            ));
        }
        binding.validate_position(&cursor.position)?;
        Ok(cursor.position)
    }
}

/// Every item validates its own invariants and provides its persisted sort key.
pub trait ConsoleRecord: Serialize {
    fn position(&self) -> ConsolePosition;
    fn validate(&self) -> Result<()>;
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsolePage<T> {
    pub items: Vec<T>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub next_cursor: Option<String>,
    pub observed_at: Timestamp,
}
impl<T: ConsoleRecord> ConsolePage<T> {
    pub fn validate(&self, page: &ConsolePagination, binding: &ConsoleCursorBinding) -> Result<()> {
        let error = || inconsistent("inconsistent console page");
        timestamp(self.observed_at).map_err(|_| error())?;
        let mut previous = page.validate(binding)?;
        if self.items.len() > page.limit as usize {
            return Err(error());
        }
        for item in &self.items {
            item.validate().map_err(|_| error())?;
            let position = item.position();
            binding.validate_position(&position).map_err(|_| error())?;
            if previous.as_ref().is_some_and(|last| {
                if binding.descending {
                    position >= *last
                } else {
                    position <= *last
                }
            }) {
                return Err(error());
            }
            previous = Some(position);
        }
        if let Some(cursor) = &self.next_cursor
            && (self.items.is_empty()
                || Some(page.decode(binding, cursor).map_err(|_| error())?) != previous)
        {
            return Err(error());
        }

        metadata_size(self).map_err(|_| error())
    }
}
