use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueHoldState {
    Reserved,
    Running,
    Uncertain,
    Released,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueHoldReleaseKind {
    Confirmed,
    NeverStarted,
    Operator,
    Timeout,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueHoldSnapshot {
    pub scope: Scope,
    pub queue: String,
    pub task_id: String,
    pub attempt_id: String,
    pub worker_session_id: String,
    pub consumer_id: u32,
    #[serde(with = "decimal")]
    pub revision: u64,
    pub state: QueueHoldState,
    pub acquired_at: Timestamp,
    #[serde(with = "decimal")]
    pub policy_revision: u64,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub grace_ms: Option<u32>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub force_at: Option<Timestamp>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub deadline: Option<Timestamp>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub stop_requested_at: Option<Timestamp>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub released_at: Option<Timestamp>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub release_kind: Option<QueueHoldReleaseKind>,
}
impl QueueHoldSnapshot {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        for value in [
            &self.queue,
            &self.task_id,
            &self.attempt_id,
            &self.worker_session_id,
        ] {
            validate_text(value, 128)?;
        }
        positive_revision(self.revision)?;
        positive_revision(self.policy_revision)?;
        timestamp(self.acquired_at)?;
        if self
            .grace_ms
            .is_some_and(|g| g == 0 || g > i32::MAX as u32 || self.deadline.is_none())
        {
            return Err(invalid_queue("queue hold grace requires a valid deadline"));
        }
        let scheduled = match (self.stop_requested_at, self.grace_ms) {
            (Some(t), Some(g)) => Some(
                t.checked_add(u64::from(g))
                    .ok_or_else(|| invalid_queue("queue hold force time overflow"))?,
            ),
            _ => None,
        };
        if self.force_at != scheduled {
            return Err(invalid_queue("inconsistent queue forced-release schedule"));
        }

        for t in [
            self.deadline,
            self.stop_requested_at,
            self.released_at,
            self.force_at,
        ]
        .into_iter()
        .flatten()
        {
            timestamp(t)?;
            if t < self.acquired_at {
                return Err(invalid_queue("queue hold event predates acquisition"));
            }
        }
        if self
            .stop_requested_at
            .is_some_and(|t| self.deadline.is_none_or(|v| v > t))
            || (self.state == QueueHoldState::Released) != self.released_at.is_some()
            || self.released_at.is_some() != self.release_kind.is_some()
            || self
                .released_at
                .zip(self.stop_requested_at)
                .is_some_and(|(release, stop)| release < stop)
            || (self.release_kind == Some(QueueHoldReleaseKind::Timeout)
                && !(self
                    .released_at
                    .zip(self.force_at)
                    .is_some_and(|(r, f)| r >= f)))
        {
            return Err(invalid_queue("queue hold lifecycle is inconsistent"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueHoldList {
    pub scope: Scope,
    pub queue: String,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub after: Option<String>,
    pub limit: u32,
    pub include_released: bool,
}
impl QueueHoldList {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        validate_text(&self.queue, 128)?;
        page_input(self.after.as_deref(), self.limit)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueHoldPage {
    pub items: Vec<QueueHoldSnapshot>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub next: Option<String>,
}
impl QueueHoldPage {
    pub fn validate_for(&self, q: &QueueHoldList) -> Result<()> {
        q.validate()?;
        if self.items.len() > q.limit as usize {
            return Err(invalid_queue("queue holds page too large"));
        }
        let mut last = q.after.as_deref();
        for item in &self.items {
            item.validate()?;
            if item.scope != q.scope
                || item.queue != q.queue
                || (!q.include_released && item.state == QueueHoldState::Released)
                || last.is_some_and(|x| x >= item.attempt_id.as_str())
            {
                return Err(invalid_queue(
                    "invalid queue holds page scope, order or state",
                ));
            }
            last = Some(&item.attempt_id);
        }
        validate_next(
            self.next.as_deref(),
            self.items.last().map(|v| v.attempt_id.as_str()),
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueHoldRelease {
    pub scope: Scope,
    pub queue: String,
    pub task_id: String,
    pub attempt_id: String,
    #[serde(with = "decimal")]
    pub expected_revision: u64,
    pub request_id: String,
    pub reason: String,
}
impl QueueHoldRelease {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        for value in [
            &self.queue,
            &self.task_id,
            &self.attempt_id,
            &self.request_id,
        ] {
            validate_text(value, 128)?;
        }
        positive_revision(self.expected_revision)?;
        revision_after(self.expected_revision)?;
        validate_text(&self.reason, 1024)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueHoldReceipt {
    pub hold: QueueHoldSnapshot,
    pub already_accepted: bool,
}
impl QueueHoldReceipt {
    pub fn validate_for(&self, c: &QueueHoldRelease) -> Result<()> {
        c.validate()?;
        self.hold.validate()?;
        let h = &self.hold;
        if h.scope != c.scope
            || h.queue != c.queue
            || h.attempt_id != c.attempt_id
            || h.task_id != c.task_id
            || h.revision != revision_after(c.expected_revision)?
            || h.release_kind != Some(QueueHoldReleaseKind::Operator)
        {
            return Err(ContractError::Conflict);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueHoldClose {
    pub queue: String,
    pub owner: LeaseOwner,
}
impl QueueHoldClose {
    pub fn validate(&self) -> Result<()> {
        self.owner.validate()?;
        validate_text(&self.queue, 128)
    }
    /// Digest includes the complete source, including the opaque lease. It may be
    /// compared without returning the credential in an inspection or receipt.
    pub fn digest(&self) -> Result<ledgence_worker_api::Digest> {
        self.validate()?;
        crate::admission::digest(self)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueHoldCloseReceipt {
    pub source_digest: ledgence_worker_api::Digest,
    pub queue: String,
    pub task_id: String,
    pub attempt_id: String,
    #[serde(with = "decimal")]
    pub revision: u64,
    pub release_kind: QueueHoldReleaseKind,
    pub already_closed: bool,
}
impl QueueHoldCloseReceipt {
    pub fn new(
        c: &QueueHoldClose,
        revision: u64,
        release_kind: QueueHoldReleaseKind,
        already_closed: bool,
    ) -> Result<Self> {
        let value = Self {
            source_digest: c.digest()?,
            queue: c.queue.clone(),
            task_id: c.owner.task_id.clone(),
            attempt_id: c.owner.attempt_id.clone(),
            revision,
            release_kind,
            already_closed,
        };
        value.validate_for(c)?;
        Ok(value)
    }
    pub fn validate_for(&self, c: &QueueHoldClose) -> Result<()> {
        c.validate()?;
        positive_revision(self.revision)?;
        if self.source_digest != c.digest()?
            || self.queue != c.queue
            || self.task_id != c.owner.task_id
            || self.attempt_id != c.owner.attempt_id
            || (!self.already_closed && self.release_kind != QueueHoldReleaseKind::Confirmed)
        {
            return Err(ContractError::Conflict);
        }
        Ok(())
    }
}
