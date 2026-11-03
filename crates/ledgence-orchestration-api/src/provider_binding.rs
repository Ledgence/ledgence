//! Immutable, reviewed provider intent. Credentials remain references, never secrets.
use crate::*;
use ledgence_worker_api::{Digest, ProgramDescriptor};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;

pub const PROVIDER_BINDING_REQUEST_MAX_BYTES: usize = 12 * 1024;
pub const PROVIDER_BINDING_MAX_BYTES: usize = 16 * 1024;
pub const PROVIDER_RESULT_MAX_BYTES: usize = 128 * 1024;
pub const PROVIDER_REPORT_MAX_BYTES: usize = 144 * 1024;
pub const PROVIDER_COMMAND_MAX_BYTES: usize = 256 * 1024;
pub const PROVIDER_ENTRY_MAX_BYTES: usize = 256 * 1024;
pub const PROVIDER_RECEIPT_MAX_BYTES: usize = 16 * 1024;
pub const PROVIDER_CONTEXT_MAX_BYTES: usize = 64 * 1024;
pub const PROVIDER_WAKE_MAX_BYTES: usize = 160 * 1024;
pub const PROVIDER_RUNTIME_RESULT_MAX_BYTES: usize = 160 * 1024;
pub const PROVIDER_MAX_MAINTENANCE_BATCH: u32 = 16;
pub const PROVIDER_ATTEMPT_TIMEOUT_MS: u64 = 60_000;
pub const PROVIDER_REPORT_RESERVE_MS: u64 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderBusinessIdentity {
    pub namespace: String,
    pub key: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderLookup {
    ByBinding,
    ByReference,
    Unavailable,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderContract {
    pub adapter_id: String,
    pub version: String,
    pub digest: Digest,
    pub completion_criterion: String,
    pub dispatch_callable: String,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub reconcile_callable: Option<String>,
    pub lookup: ProviderLookup,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderTarget {
    pub provider: String,
    pub environment: String,
    pub account: String,
    pub api_variant: String,
    pub operation: String,
    pub endpoint_identity: String,
    pub dimensions: BTreeMap<String, String>,
}
impl ProviderTarget {
    pub fn validate(&self) -> Result<()> {
        for value in [&self.provider, &self.api_variant, &self.operation] {
            validate_text(value, 128)?;
        }
        validate_text(&self.environment, 64)?;
        validate_text(&self.account, 256)?;
        validate_text(&self.endpoint_identity, 512)?;
        dimensions(&self.dimensions)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDeduplicationDomain {
    pub provider: String,
    pub namespace: String,
    pub dimensions: BTreeMap<String, String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderDeduplicationHorizon {
    Unknown,
    MinimumDuration { duration_ms: u64 },
    ResourceLifetime,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDeduplication {
    pub domain: ProviderDeduplicationDomain,
    pub key: String,
    pub horizon: ProviderDeduplicationHorizon,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderReconciliationPolicy {
    pub queue: String,
    pub initial_delay_ms: u64,
    pub interval_ms: u64,
    pub maximum_interval_ms: u64,
    pub max_checks: u32,
    pub max_elapsed_ms: u64,
    pub check_timeout_ms: u64,
}
impl ProviderReconciliationPolicy {
    pub fn for_queue(queue: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            initial_delay_ms: 30_000,
            interval_ms: 5_000,
            maximum_interval_ms: 300_000,
            max_checks: 12,
            max_elapsed_ms: 3_600_000,
            check_timeout_ms: 10_000,
        }
    }
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.queue, 128)?;
        if !(1_000..=86_400_000).contains(&self.initial_delay_ms)
            || !(1_000..=86_400_000).contains(&self.interval_ms)
            || self.maximum_interval_ms < self.interval_ms
            || self.maximum_interval_ms > 86_400_000
            || !(1..=256).contains(&self.max_checks)
            || !(60_000..=604_800_000).contains(&self.max_elapsed_ms)
            || self.initial_delay_ms >= self.max_elapsed_ms
            || !(1_000..=30_000).contains(&self.check_timeout_ms)
        {
            return Err(provider_invalid());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderBindingRequest {
    pub v: u8,
    pub business: ProviderBusinessIdentity,
    pub contract: ProviderContract,
    pub target: ProviderTarget,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub deduplication: Option<ProviderDeduplication>,
    pub credential_reference: String,
    pub reconciliation: ProviderReconciliationPolicy,
}
impl ProviderBindingRequest {
    pub fn validate(&self) -> Result<()> {
        if self.v != 1 {
            return Err(provider_invalid());
        }
        validate_text(&self.business.namespace, 128)?;
        validate_text(&self.business.key, 256)?;
        validate_text(&self.contract.adapter_id, 128)?;
        validate_text(&self.contract.version, 128)?;
        self.contract.digest.validate()?;
        validate_text(&self.contract.completion_criterion, 1024)?;
        if self.contract.completion_criterion.trim().is_empty() {
            return Err(provider_invalid());
        }
        validate_text(&self.contract.dispatch_callable, 512)?;
        match (&self.contract.reconcile_callable, self.contract.lookup) {
            (None, ProviderLookup::Unavailable) => {}
            (Some(callable), ProviderLookup::ByBinding | ProviderLookup::ByReference) => {
                validate_text(callable, 512)?;
                if callable == &self.contract.dispatch_callable {
                    return Err(provider_invalid());
                }
            }
            _ => return Err(provider_invalid()),
        }
        self.target.validate()?;
        if let Some(dedup) = &self.deduplication {
            validate_text(&dedup.domain.provider, 128)?;
            validate_text(&dedup.domain.namespace, 128)?;
            validate_text(&dedup.key, 256)?;
            dimensions(&dedup.domain.dimensions)?;
            if dedup.domain.provider != self.target.provider {
                return Err(provider_invalid());
            }
            if let ProviderDeduplicationHorizon::MinimumDuration { duration_ms } = dedup.horizon
                && !(1..=WORKFLOW_MAX_DELAY_MS).contains(&duration_ms)
            {
                return Err(provider_invalid());
            }
        }
        validate_text(&self.credential_reference, 256)?;
        self.reconciliation.validate()?;
        provider_bound(self, PROVIDER_BINDING_REQUEST_MAX_BYTES)
    }
    pub fn resolve(&self, program: ProgramDescriptor) -> Result<ProviderBinding> {
        self.validate()?;
        let result = ProviderBinding {
            v: self.v,
            business: self.business.clone(),
            contract: self.contract.clone(),
            target: self.target.clone(),
            deduplication: self.deduplication.clone(),
            credential_reference: self.credential_reference.clone(),
            reconciliation: self.reconciliation.clone(),
            program,
        };
        result.validate()?;
        Ok(result)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderBinding {
    pub v: u8,
    pub business: ProviderBusinessIdentity,
    pub contract: ProviderContract,
    pub target: ProviderTarget,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub deduplication: Option<ProviderDeduplication>,
    pub credential_reference: String,
    pub reconciliation: ProviderReconciliationPolicy,
    pub program: ProgramDescriptor,
}
impl ProviderBinding {
    pub fn request(&self) -> ProviderBindingRequest {
        ProviderBindingRequest {
            v: self.v,
            business: self.business.clone(),
            contract: self.contract.clone(),
            target: self.target.clone(),
            deduplication: self.deduplication.clone(),
            credential_reference: self.credential_reference.clone(),
            reconciliation: self.reconciliation.clone(),
        }
    }
    pub fn validate(&self) -> Result<()> {
        self.request().validate()?;
        self.program.validate()?;
        provider_bound(self, PROVIDER_BINDING_MAX_BYTES)
    }
    pub fn validate_action(&self, action: &ApprovalAction) -> Result<()> {
        self.validate()?;
        action.validate()?;
        if action.name != self.contract.dispatch_callable || action.version != self.contract.version
        {
            return Err(provider_invalid());
        }
        Ok(())
    }
    pub fn digest(&self, action: &ApprovalAction) -> Result<Digest> {
        self.validate_action(action)?;
        provider_digest(&(self, action))
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderDispatchPolicy {
    Enabled,
    DeclaredRestoreQuarantine,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderOperationConfig {
    pub v: u8,
    pub audience: AccessAudience,
    pub dispatch: ProviderDispatchPolicy,
}
impl ProviderOperationConfig {
    pub fn validate(&self) -> Result<()> {
        self.audience.validate()?;
        if self.v == 1 {
            Ok(())
        } else {
            Err(provider_invalid())
        }
    }
    pub fn validate_for(&self, audience: &AccessAudience) -> Result<()> {
        self.validate()?;
        if &self.audience == audience {
            Ok(())
        } else {
            Err(ContractError::Conflict)
        }
    }
}
pub(crate) fn provider_invalid() -> ContractError {
    ContractError::InvalidInput("invalid provider operation contract".into())
}
pub(crate) fn provider_bound<T: Serialize>(value: &T, maximum: usize) -> Result<()> {
    Ok(crate::submission::check_encoded_size(
        value,
        maximum,
        "provider operation",
    )?)
}
pub(crate) fn provider_digest<T: Serialize>(value: &T) -> Result<Digest> {
    let value = serde_json::to_value(value).map_err(|_| provider_invalid())?;
    let bytes = crate::canonical_json_bytes(&value)?;
    Ok(Digest(format!("sha256:{:x}", Sha256::digest(bytes))))
}
fn dimensions(values: &BTreeMap<String, String>) -> Result<()> {
    if values.len() > 8 {
        return Err(provider_invalid());
    }
    for (key, value) in values {
        validate_text(key, 128)?;
        validate_text(value, 256)?;
    }
    Ok(())
}
