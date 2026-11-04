//! Run-wide authority for instrumented work. Only the durable store resolves a subject's root.
use crate::queue_policies::{decimal, optional_decimal};
use crate::*;

fn invalid(reason: &str) -> ContractError {
    ContractError::InvalidInput(reason.into())
}

/// A budgeted external mutation may be handed to user code only inside this short window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunBudgetGrant {
    pub authorized_at: Timestamp,
    pub expires_at: Timestamp,
}
impl RunBudgetGrant {
    pub fn new(authorized_at: Timestamp, latest_expiry: Timestamp) -> Result<Self> {
        let grant = Self {
            authorized_at,
            expires_at: latest_expiry.min(authorized_at.saturating_add(5_000)),
        };
        grant.validate()?;
        Ok(grant)
    }
    pub fn validate(&self) -> Result<()> {
        crate::workflow_approvals::timestamp(self.authorized_at)?;
        crate::workflow_approvals::timestamp(self.expires_at)?;
        if self.expires_at <= self.authorized_at || self.expires_at - self.authorized_at > 5_000 {
            return Err(invalid("invalid run budget grant window"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunBudgetPolicy {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::effect_resolution::present"
    )]
    pub max_calls: Option<u32>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::effect_resolution::present"
    )]
    pub max_delegations: Option<u32>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::effect_resolution::present"
    )]
    pub deadline_at_ms: Option<Timestamp>,
}
impl RunBudgetPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.max_calls.is_none()
            && self.max_delegations.is_none()
            && self.deadline_at_ms.is_none()
        {
            return Err(invalid("run budget requires a limit or deadline"));
        }
        if self.max_calls.is_some_and(|n| n > 1_000_000)
            || self.max_delegations.is_some_and(|n| n > 1_000_000)
        {
            return Err(invalid("run budget limit exceeds 1000000"));
        }
        if let Some(t) = self.deadline_at_ms {
            crate::workflow_approvals::timestamp(t)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunBudgetSubject {
    Task { task_id: String },
    Workflow { workflow_id: String },
}
impl RunBudgetSubject {
    pub fn validate(&self) -> Result<()> {
        validate_text(
            match self {
                Self::Task { task_id } => task_id,
                Self::Workflow { workflow_id } => workflow_id,
            },
            128,
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunBudgetAction {
    Call,
    Delegation,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunBudgetOutcome {
    Authorized,
    Denied,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunBudgetDenial {
    /// The owner or an ancestor has durably stopped new work for this subtree.
    Cancelled,
    CallsExhausted,
    DelegationsExhausted,
    DeadlineExceeded,
}
impl RunBudgetDenial {
    pub const fn code(self) -> &'static str {
        match self {
            Self::CallsExhausted => "budget_calls_exhausted",
            Self::DelegationsExhausted => "budget_delegations_exhausted",
            Self::DeadlineExceeded => "budget_deadline_exceeded",
            Self::Cancelled => "budget_cancelled",
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunBudgetUsage {
    Unknown {},
    Reported {
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::effect_resolution::present"
        )]
        input_tokens: Option<u64>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::effect_resolution::present"
        )]
        output_tokens: Option<u64>,
    },
    Estimated {
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::effect_resolution::present"
        )]
        input_tokens: Option<u64>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::effect_resolution::present"
        )]
        output_tokens: Option<u64>,
    },
}
impl RunBudgetUsage {
    pub fn validate(&self) -> Result<()> {
        let (input, output) = match self {
            Self::Unknown {} => return Ok(()),
            Self::Reported {
                input_tokens,
                output_tokens,
            }
            | Self::Estimated {
                input_tokens,
                output_tokens,
            } => (input_tokens, output_tokens),
        };
        if input.is_none() && output.is_none() {
            return Err(invalid("usage requires a reported or estimated quantity"));
        }
        for n in [*input, *output].into_iter().flatten() {
            crate::admission::safe(n)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunBudgetQuery {
    pub scope: Scope,
    pub subject: RunBudgetSubject,
}
impl RunBudgetQuery {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        self.subject.validate()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunBudgetEntriesQuery {
    pub scope: Scope,
    pub subject: RunBudgetSubject,
    #[serde(with = "optional_decimal")]
    pub before: Option<u64>,
    pub limit: u32,
}
impl RunBudgetEntriesQuery {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        self.subject.validate()?;
        if self.before == Some(0) || !(1..=100).contains(&self.limit) {
            return Err(invalid("invalid run budget cursor or limit"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunBudgetSnapshot {
    pub scope: Scope,
    pub subject: RunBudgetSubject,
    pub root: RunBudgetSubject,
    pub policy: RunBudgetPolicy,
    #[serde(with = "decimal")]
    pub calls: u64,
    #[serde(with = "decimal")]
    pub delegations: u64,
    pub observed_at: Timestamp,
}
impl RunBudgetSnapshot {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        self.subject.validate()?;
        self.root.validate()?;
        self.policy.validate()?;
        crate::workflow_approvals::timestamp(self.observed_at)?;
        if self
            .policy
            .max_calls
            .is_some_and(|m| self.calls > u64::from(m))
            || self
                .policy
                .max_delegations
                .is_some_and(|m| self.delegations > u64::from(m))
        {
            return Err(invalid("recorded use exceeds immutable run budget"));
        }
        validate_root(&self.subject, &self.root)
    }
    pub fn validate_for(&self, q: &RunBudgetQuery) -> Result<()> {
        self.validate()?;
        q.validate()?;
        if q.scope != self.scope || q.subject != self.subject {
            return Err(invalid("run budget snapshot mismatch"));
        }
        Ok(())
    }
}
fn validate_root(subject: &RunBudgetSubject, root: &RunBudgetSubject) -> Result<()> {
    if matches!(root, RunBudgetSubject::Task { .. }) && subject != root {
        return Err(invalid("a standalone task budget cannot own descendants"));
    }
    Ok(())
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunBudgetEntry {
    pub scope: Scope,
    pub root: RunBudgetSubject,
    #[serde(with = "decimal")]
    pub sequence: u64,
    pub subject: RunBudgetSubject,
    pub action: RunBudgetAction,
    pub outcome: RunBudgetOutcome,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub reason: Option<RunBudgetDenial>,
    pub source_id: String,
    /// Original server-recorded approval that authorized this physical mutation.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::effect_resolution::present"
    )]
    pub approval: Option<EffectOccurrence>,
    pub usage: RunBudgetUsage,
    pub recorded_at: Timestamp,
}
impl RunBudgetEntry {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        self.subject.validate()?;
        self.root.validate()?;
        validate_root(&self.subject, &self.root)?;
        validate_text(&self.source_id, 128)?;
        if let Some(approval) = &self.approval {
            approval.validate()?;
            if self.action != RunBudgetAction::Call
                || self.scope != approval.scope
                || !matches!(&self.subject,RunBudgetSubject::Workflow{workflow_id} if workflow_id==&approval.workflow_id)
            {
                return Err(invalid("budget decision does not own its approval"));
            }
        }
        self.usage.validate()?;
        crate::workflow_approvals::timestamp(self.recorded_at)?;
        if self.sequence == 0
            || (self.outcome == RunBudgetOutcome::Denied) != self.reason.is_some()
            || (self.outcome == RunBudgetOutcome::Denied
                || self.action == RunBudgetAction::Delegation)
                && !matches!(self.usage, RunBudgetUsage::Unknown {})
            || matches!(
                (self.action, self.reason),
                (
                    RunBudgetAction::Call,
                    Some(RunBudgetDenial::DelegationsExhausted)
                ) | (
                    RunBudgetAction::Delegation,
                    Some(RunBudgetDenial::CallsExhausted)
                )
            )
        {
            return Err(invalid("inconsistent run budget decision"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunBudgetPage {
    pub scope: Scope,
    pub subject: RunBudgetSubject,
    pub root: RunBudgetSubject,
    pub items: Vec<RunBudgetEntry>,
    #[serde(with = "optional_decimal")]
    pub next: Option<u64>,
}
impl RunBudgetPage {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        self.subject.validate()?;
        self.root.validate()?;
        validate_root(&self.subject, &self.root)?;
        if self.items.len() > 100 {
            return Err(invalid("oversized run budget page"));
        }
        let mut previous = None;
        for entry in &self.items {
            entry.validate()?;
            if entry.scope != self.scope
                || entry.root != self.root
                || previous.is_some_and(|seq| entry.sequence >= seq)
            {
                return Err(invalid("inconsistent run budget entry page"));
            }
            previous = Some(entry.sequence);
        }
        if self.next.is_some() && self.next != previous {
            return Err(invalid("invalid run budget cursor"));
        }
        crate::admission::bound(self, ADMISSION_PAGE_MAX_BYTES)
    }
    pub fn validate_for(&self, q: &RunBudgetEntriesQuery) -> Result<()> {
        self.validate()?;
        q.validate()?;
        if q.scope != self.scope
            || q.subject != self.subject
            || self.items.len() > q.limit as usize
            || self
                .items
                .first()
                .is_some_and(|entry| q.before.is_some_and(|seq| entry.sequence >= seq))
        {
            return Err(invalid("run budget page mismatch"));
        }
        Ok(())
    }
}
/// Complete read capability of a store which enforces budgeted submissions, delegation and Start atomically.
pub trait RunBudgetOperations: Send + Sync {
    fn run_budget<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        q: &'a RunBudgetQuery,
    ) -> ContractFuture<'a, RunBudgetSnapshot>;
    fn list_run_budget_entries<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        q: &'a RunBudgetEntriesQuery,
    ) -> ContractFuture<'a, RunBudgetPage>;
}
pub trait RunBudgetClient: Send + Sync {
    fn run_budget<'a>(&'a self, q: &'a RunBudgetQuery) -> ContractFuture<'a, RunBudgetSnapshot>;
    fn list_run_budget_entries<'a>(
        &'a self,
        q: &'a RunBudgetEntriesQuery,
    ) -> ContractFuture<'a, RunBudgetPage>;
}
