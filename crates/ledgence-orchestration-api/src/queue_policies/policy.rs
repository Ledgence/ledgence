use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueueTimeoutRelease {
    Confirm,
    AfterGrace { grace_ms: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueHoldTimeout {
    pub after_ms: u32,
    pub release: QueueTimeoutRelease,
}
impl QueueHoldTimeout {
    pub fn validate(&self) -> Result<()> {
        let valid = |v| (1..=i32::MAX as u32).contains(&v);
        if !valid(self.after_ms)
            || matches!(self.release,QueueTimeoutRelease::AfterGrace{grace_ms} if !valid(grace_ms))
        {
            return Err(invalid_queue(
                "queue timeout and grace must be between 1 and 2147483647 milliseconds",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueConcurrencyPolicy {
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub max_concurrency: Option<u32>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub timeout: Option<QueueHoldTimeout>,
}
impl QueueConcurrencyPolicy {
    pub fn validate(&self) -> Result<()> {
        if self
            .max_concurrency
            .is_some_and(|v| v > QUEUE_CONCURRENCY_MAX)
        {
            return Err(invalid_queue("queue concurrency exceeds 1000000"));
        }
        if let Some(timeout) = self.timeout {
            timeout.validate()?;
        }
        Ok(())
    }
    pub fn digest(&self) -> Result<ledgence_worker_api::Digest> {
        self.validate()?;
        crate::admission::digest(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueuePolicyRecord {
    pub scope: Scope,
    pub queue: String,
    #[serde(with = "decimal")]
    pub revision: u64,
    pub policy: QueueConcurrencyPolicy,
    pub updated_at: Timestamp,
}
impl QueuePolicyRecord {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        validate_text(&self.queue, 128)?;
        positive_revision(self.revision)?;
        self.policy.validate()?;
        timestamp(self.updated_at)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueuePolicySnapshot {
    pub record: QueuePolicyRecord,
    #[serde(with = "decimal")]
    pub occupied: u64,
    #[serde(with = "decimal")]
    pub uncertain: u64,
}
impl QueuePolicySnapshot {
    pub fn validate(&self) -> Result<()> {
        self.record.validate()?;
        if self.uncertain > self.occupied {
            return Err(invalid_queue("uncertain queue holds exceed occupied count"));
        }
        Ok(())
    }
    pub fn validate_for(&self, q: &QueuePolicyQuery) -> Result<()> {
        self.validate()?;
        q.validate()?;
        if self.record.scope != q.scope || self.record.queue != q.queue {
            return Err(ContractError::Conflict);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueuePolicyPut {
    pub scope: Scope,
    pub queue: String,
    pub request_id: String,
    #[serde(with = "optional_decimal")]
    pub expected_revision: Option<u64>,
    pub policy: QueueConcurrencyPolicy,
    pub reason: String,
}
impl QueuePolicyPut {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        validate_text(&self.queue, 128)?;
        validate_text(&self.request_id, 128)?;
        validate_text(&self.reason, 1024)?;
        if let Some(v) = self.expected_revision {
            positive_revision(v)?;
            revision_after(v)?;
        }
        self.policy.validate()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueuePolicyReceipt {
    pub record: QueuePolicyRecord,
    pub already_accepted: bool,
}
impl QueuePolicyReceipt {
    pub fn validate_for(&self, c: &QueuePolicyPut) -> Result<()> {
        self.record.validate()?;
        c.validate()?;
        if self.record.scope != c.scope
            || self.record.queue != c.queue
            || self.record.policy != c.policy
            || self.record.revision != revision_after(c.expected_revision.unwrap_or(0))?
        {
            return Err(ContractError::Conflict);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueuePolicyQuery {
    pub scope: Scope,
    pub queue: String,
}
impl QueuePolicyQuery {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        validate_text(&self.queue, 128)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueuePolicyList {
    pub scope: Scope,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub after: Option<String>,
    pub limit: u32,
}
impl QueuePolicyList {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        page_input(self.after.as_deref(), self.limit)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueuePolicyPage {
    pub items: Vec<QueuePolicySnapshot>,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub next: Option<String>,
}
impl QueuePolicyPage {
    pub fn validate_for(&self, q: &QueuePolicyList) -> Result<()> {
        q.validate()?;
        if self.items.len() > q.limit as usize {
            return Err(invalid_queue("queue policy page too large"));
        }
        let mut last = q.after.as_deref();
        for item in &self.items {
            item.validate()?;
            let key = item.record.queue.as_str();
            if q.scope != item.record.scope || last.is_some_and(|before| before >= key) {
                return Err(invalid_queue("invalid queue policy page order or scope"));
            }
            last = Some(key);
        }
        validate_next(
            self.next.as_deref(),
            self.items.last().map(|v| v.record.queue.as_str()),
        )
    }
}
