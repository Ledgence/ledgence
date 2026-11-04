//! Audited discovery of charged occupancy independent of execution retention.
use super::*;
use crate::queue_policies::decimal;

pub const RESOURCE_SOURCE_CURSOR_MAX_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceSourceState {
    PossiblyDispatched,
    UnknownOccupied,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceSourceSubject {
    Workflow {
        workflow_id: String,
        origin_activation_id: String,
        key: String,
    },
    Provider {
        workflow_id: String,
        origin_activation_id: String,
        key: String,
        approval_activation_id: String,
        approval_key: String,
    },
    Task {
        task_id: String,
        generation: u32,
    },
}
impl ResourceSourceSubject {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Task {
                task_id,
                generation,
            } => {
                validate_text(task_id, 128)?;
                if !(1..=MAX_TASK_ATTEMPT_GENERATION).contains(generation) {
                    return Err(invalid("invalid resource source task generation"));
                }
            }
            Self::Workflow {
                workflow_id,
                origin_activation_id,
                key,
            }
            | Self::Provider {
                workflow_id,
                origin_activation_id,
                key,
                ..
            } => {
                for id in [workflow_id, origin_activation_id, key] {
                    validate_text(id, 128)?;
                }
                if let Self::Provider {
                    approval_activation_id,
                    approval_key,
                    ..
                } = self
                {
                    validate_text(approval_activation_id, 128)?;
                    validate_text(approval_key, 128)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSource {
    pub scope: Scope,
    pub resource: String,
    #[serde(with = "decimal")]
    pub sequence: u64,
    #[serde(with = "decimal")]
    pub revision: u64,
    pub updated_at: Timestamp,
    pub state: ResourceSourceState,
    pub subject: ResourceSourceSubject,
}
impl ResourceSource {
    fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        name(&self.resource)?;
        sequence(self.sequence)?;
        positive(self.revision)?;
        time(self.updated_at)?;
        self.subject.validate()
    }
}

fn sequence(value: u64) -> Result<()> {
    if (1..=i64::MAX as u64).contains(&value) {
        Ok(())
    } else {
        Err(invalid("invalid resource source sequence"))
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    v: u8,
    scope: Scope,
    name: String,
    #[serde(deserialize_with = "crate::observation::required_option")]
    state: Option<ResourceSourceState>,
    #[serde(with = "decimal")]
    sequence: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSourceQuery {
    pub scope: Scope,
    pub name: String,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub state: Option<ResourceSourceState>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub cursor: Option<String>,
    pub limit: u32,
}
impl ResourceSourceQuery {
    pub fn validate(&self) -> Result<Option<u64>> {
        self.scope.validate()?;
        name(&self.name)?;
        if !(1..=100).contains(&self.limit) {
            return Err(invalid("invalid resource source page size"));
        }
        let Some(cursor) = self.cursor.as_ref() else {
            return Ok(None);
        };
        let bytes = crate::cursor::decode(cursor, RESOURCE_SOURCE_CURSOR_MAX_BYTES)
            .ok_or_else(|| invalid("invalid resource source cursor"))?;
        let decoded: Cursor = decode_unique_json(&bytes, RESOURCE_SOURCE_CURSOR_MAX_BYTES / 2)?;
        if decoded.v != 1
            || decoded.scope != self.scope
            || decoded.name != self.name
            || decoded.state != self.state
        {
            return Err(invalid(
                "resource source cursor belongs to another scope, resource or state",
            ));
        }
        sequence(decoded.sequence)?;
        Ok(Some(decoded.sequence))
    }
    pub fn next_cursor(&self, position: u64) -> Result<String> {
        sequence(position)?;
        let bytes = serde_json::to_vec(&Cursor {
            v: 1,
            scope: self.scope.clone(),
            name: self.name.clone(),
            state: self.state,
            sequence: position,
        })
        .map_err(|_| invalid("could not encode resource source cursor"))?;
        let cursor = crate::cursor::encode(&bytes);
        if cursor.len() > RESOURCE_SOURCE_CURSOR_MAX_BYTES {
            return Err(invalid("resource source cursor too large"));
        }
        Ok(cursor)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSourcePage {
    pub items: Vec<ResourceSource>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub next_cursor: Option<String>,
}
impl ResourceSourcePage {
    pub fn validate(&self) -> Result<()> {
        if self.items.len() > 100 {
            return Err(invalid("resource source page too large"));
        }
        let mut previous = None;
        for item in &self.items {
            item.validate()?;
            if previous.is_some_and(|seq| seq <= item.sequence) {
                return Err(invalid("resource sources must descend by unique sequence"));
            }
            previous = Some(item.sequence);
        }
        bound(self, ADMISSION_PAGE_MAX_BYTES)
    }
    pub fn validate_for(&self, q: &ResourceSourceQuery) -> Result<()> {
        self.validate()?;
        let before = q.validate()?;
        if self.items.len() > q.limit as usize
            || self.items.iter().any(|i| {
                i.scope != q.scope
                    || i.resource != q.name
                    || q.state.is_some_and(|s| i.state != s)
                    || before.is_some_and(|s| i.sequence >= s)
            })
        {
            return Err(invalid("resource source page disagrees with query"));
        }
        if let Some(cursor) = &self.next_cursor {
            let last = self
                .items
                .last()
                .ok_or_else(|| invalid("empty resource source page with cursor"))?;
            if *cursor != q.next_cursor(last.sequence)? {
                return Err(invalid("invalid resource source continuation"));
            }
        }
        Ok(())
    }
}
