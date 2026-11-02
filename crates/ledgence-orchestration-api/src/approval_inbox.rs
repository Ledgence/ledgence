//! Scoped, payload-free discovery of persisted approval requests.
use crate::*;
use ledgence_worker_api::ProgramRef;

fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}

pub const APPROVAL_INBOX_MAX_LIMIT: u32 = 100;
pub const APPROVAL_INBOX_CURSOR_MAX_BYTES: usize = 8192;
pub const APPROVAL_INBOX_PAGE_MAX_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalInboxQuery {
    pub scope: Scope,
    pub status: Option<ApprovalStatus>,
    pub program_id: Option<String>,
    pub cursor: Option<String>,
    pub limit: u32,
}

/// Stable seek tuple, ordered by deadline then UTF-8 workflow ID and wait key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalInboxPosition {
    pub deadline: Timestamp,
    pub workflow_id: String,
    pub key: String,
}

impl ApprovalInboxPosition {
    fn validate(&self) -> Result<()> {
        crate::workflow_approvals::timestamp(self.deadline)?;
        validate_text(&self.workflow_id, 128)?;
        validate_text(&self.key, 128)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    scope: Scope,
    status: Option<ApprovalStatus>,
    program_id: Option<String>,
    position: ApprovalInboxPosition,
}

impl ApprovalInboxQuery {
    /// A cursor binds scope and filters but not page size or mutable status rows.
    pub fn validate(&self) -> Result<Option<ApprovalInboxPosition>> {
        self.scope.validate()?;
        if let Some(id) = &self.program_id {
            ProgramRef {
                id: id.clone(),
                version: "1".into(),
            }
            .validate()?;
        }
        if !(1..=APPROVAL_INBOX_MAX_LIMIT).contains(&self.limit) {
            return Err(invalid("approval inbox limit must be between 1 and 100"));
        }
        let Some(text) = &self.cursor else {
            return Ok(None);
        };
        let error = || invalid("invalid or mismatched approval inbox cursor");
        let bytes =
            crate::cursor::decode(text, APPROVAL_INBOX_CURSOR_MAX_BYTES).ok_or_else(error)?;
        let cursor: Cursor =
            decode_unique_json(&bytes, APPROVAL_INBOX_CURSOR_MAX_BYTES / 2).map_err(|_| error())?;
        if cursor.version != 1
            || cursor.scope != self.scope
            || cursor.status != self.status
            || cursor.program_id != self.program_id
        {
            return Err(error());
        }
        cursor.position.validate()?;
        Ok(Some(cursor.position))
    }

    pub fn next_cursor(&self, item: &ApprovalInboxItem) -> Result<String> {
        let encoded = serde_json::to_vec(&Cursor {
            version: 1,
            scope: self.scope.clone(),
            status: self.status,
            program_id: self.program_id.clone(),
            position: item.position(),
        })
        .map_err(|_| invalid("could not encode approval inbox cursor"))?;
        if encoded.len() > APPROVAL_INBOX_CURSOR_MAX_BYTES / 2 {
            return Err(invalid("approval inbox cursor exceeds size limit"));
        }
        Ok(crate::cursor::encode(&encoded))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalInboxItem {
    pub workflow_id: String,
    pub key: String,
    pub activation_id: String,
    pub program_id: String,
    pub program_version: String,
    pub action_name: String,
    pub action_version: String,
    pub created_at: Timestamp,
    pub deadline: Timestamp,
    pub status: ApprovalStatus,
}
impl ApprovalInboxItem {
    fn position(&self) -> ApprovalInboxPosition {
        ApprovalInboxPosition {
            deadline: self.deadline,
            workflow_id: self.workflow_id.clone(),
            key: self.key.clone(),
        }
    }
    fn validate(&self) -> Result<()> {
        self.position().validate()?;
        validate_text(&self.activation_id, 128)?;
        ProgramRef {
            id: self.program_id.clone(),
            version: self.program_version.clone(),
        }
        .validate()?;
        validate_text(&self.action_name, 512)?;
        validate_text(&self.action_version, 128)?;
        if self.created_at > self.deadline
            || self.deadline - self.created_at > WORKFLOW_MAX_DELAY_MS
        {
            return Err(invalid("invalid approval inbox timestamps"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalInboxPage {
    pub items: Vec<ApprovalInboxItem>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub next_cursor: Option<String>,
}
impl ApprovalInboxPage {
    pub fn validate_for(&self, query: &ApprovalInboxQuery) -> Result<()> {
        let inconsistent = || ContractError::Unavailable("inconsistent approval inbox page".into());
        let mut previous = query.validate()?;
        if self.items.len() > query.limit as usize {
            return Err(inconsistent());
        }
        let mut ids = std::collections::HashSet::with_capacity(self.items.len());
        for item in &self.items {
            item.validate().map_err(|_| inconsistent())?;
            let position = item.position();
            if previous.as_ref().is_some_and(|p| &position <= p)
                || query.status.is_some_and(|s| s != item.status)
                || query
                    .program_id
                    .as_ref()
                    .is_some_and(|p| p != &item.program_id)
                || !ids.insert((&item.workflow_id, &item.key))
            {
                return Err(inconsistent());
            }
            previous = Some(position);
        }
        if let Some(cursor) = &self.next_cursor
            && (self.items.len() != query.limit as usize
                || *cursor != query.next_cursor(self.items.last().ok_or_else(inconsistent)?)?)
        {
            return Err(inconsistent());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn query() -> ApprovalInboxQuery {
        ApprovalInboxQuery {
            scope: Scope {
                tenant_id: "a".into(),
                namespace: "ns".into(),
            },
            status: Some(ApprovalStatus::Pending),
            program_id: Some("agent".into()),
            limit: 1,
            cursor: None,
        }
    }
    fn item(key: &str) -> ApprovalInboxItem {
        ApprovalInboxItem {
            workflow_id: "wf".into(),
            key: key.into(),
            activation_id: "a1".into(),
            program_id: "agent".into(),
            program_version: "1".into(),
            action_name: "send".into(),
            action_version: "1".into(),
            created_at: 0,
            deadline: 100,
            status: ApprovalStatus::Pending,
        }
    }
    #[test]
    fn filters_and_scope_bind_a_cursor_and_limit_can_change_between_pages() {
        let first = query();
        let cursor = first.next_cursor(&item("a")).unwrap();
        let continuation = ApprovalInboxQuery {
            cursor: Some(cursor),
            limit: 2,
            ..first.clone()
        };
        assert_eq!(
            continuation.validate().unwrap().unwrap(),
            item("a").position()
        );
        for bad in [
            ApprovalInboxQuery {
                program_id: None,
                ..continuation.clone()
            },
            ApprovalInboxQuery {
                status: None,
                ..continuation.clone()
            },
            ApprovalInboxQuery {
                scope: Scope {
                    namespace: "foreign".into(),
                    ..first.scope.clone()
                },
                ..continuation.clone()
            },
            ApprovalInboxQuery {
                limit: 101,
                ..continuation.clone()
            },
            ApprovalInboxQuery {
                cursor: Some("f".repeat(APPROVAL_INBOX_CURSOR_MAX_BYTES + 2)),
                ..continuation.clone()
            },
        ] {
            assert!(bad.validate().is_err());
        }
        let ok = ApprovalInboxPage {
            items: vec![item("b"), item("c")],
            next_cursor: Some(continuation.next_cursor(&item("c")).unwrap()),
        };
        ok.validate_for(&continuation).unwrap();
        for bad in [
            ApprovalInboxPage {
                items: vec![item("a")],
                next_cursor: None,
            },
            ApprovalInboxPage {
                items: vec![item("c"), item("b")],
                next_cursor: None,
            },
            ApprovalInboxPage {
                items: vec![
                    item("b"),
                    ApprovalInboxItem {
                        deadline: 101,
                        ..item("b")
                    },
                ],
                next_cursor: None,
            },
            ApprovalInboxPage {
                items: vec![ApprovalInboxItem {
                    status: ApprovalStatus::Expired,
                    ..item("c")
                }],
                next_cursor: None,
            },
            ApprovalInboxPage {
                items: vec![ApprovalInboxItem {
                    program_id: "other".into(),
                    ..item("c")
                }],
                next_cursor: None,
            },
            ApprovalInboxPage {
                next_cursor: continuation.cursor.clone(),
                ..ok.clone()
            },
        ] {
            assert!(bad.validate_for(&continuation).is_err());
        }
    }
}
