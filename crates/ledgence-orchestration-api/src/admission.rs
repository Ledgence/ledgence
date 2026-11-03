//! Provider-neutral, checkpointed admission. A ready offer is never dispatch permission.
use crate::*;
use ledgence_worker_api::{Digest, ProgramDescriptor, validate_wire_value};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

mod commands;
mod lifecycle;
mod ports;
pub use commands::*;
pub use lifecycle::*;
pub use ports::*;

pub const ADMISSION_MAX_MAINTENANCE_BATCH: u32 = 128;
pub const ADMISSION_ARGUMENTS_MAX_BYTES: usize = 64 * 1024;
pub const ADMISSION_OUTPUT_MAX_BYTES: usize = 64 * 1024;
pub const ADMISSION_COMMAND_MAX_BYTES: usize = 160 * 1024;
pub const ADMISSION_RESPONSE_MAX_BYTES: usize = 256 * 1024;
pub const ADMISSION_PAGE_MAX_BYTES: usize = 256 * 1024;
pub const ADMISSION_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionDispatchPolicy {
    Enabled,
    Quarantined,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionConfig {
    pub v: u8,
    pub audience: AccessAudience,
    pub dispatch: AdmissionDispatchPolicy,
}
impl AdmissionConfig {
    pub fn validate_for(&self, audience: &AccessAudience) -> Result<()> {
        self.audience.validate()?;
        if self.v != 1 || &self.audience != audience {
            return Err(invalid("admission configuration audience or version"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRate {
    pub numerator: u32,
    pub period_ms: u32,
    pub burst: u32,
}
impl ResourceRate {
    pub fn validate(&self) -> Result<()> {
        if !(1..=1_000_000).contains(&self.numerator)
            || !(1..=86_400_000).contains(&self.period_ms)
            || !(1..=1_000_000).contains(&self.burst)
        {
            return Err(invalid("resource rate exceeds supported bounds"));
        }
        safe(self.capacity())
    }
    pub fn capacity(&self) -> u64 {
        u64::from(self.period_ms) * u64::from(self.burst)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcePolicy {
    pub enabled: bool,
    pub max_occupancy: u32,
    #[serde(deserialize_with = "crate::observation::required_option")]
    pub rate: Option<ResourceRate>,
    pub offer_timeout_ms: u32,
    pub start_timeout_ms: u32,
}
impl ResourcePolicy {
    pub fn validate(&self) -> Result<()> {
        if !(1..=1_000_000).contains(&self.max_occupancy)
            || !(1_000..=300_000).contains(&self.offer_timeout_ms)
            || !(100..=30_000).contains(&self.start_timeout_ms)
        {
            return Err(invalid("resource policy exceeds supported bounds"));
        }
        if let Some(rate) = &self.rate {
            rate.validate()?;
        }
        Ok(())
    }
    pub fn digest(&self) -> Result<Digest> {
        self.validate()?;
        digest(self)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRecord {
    pub scope: Scope,
    pub name: String,
    pub revision: u64,
    pub policy: ResourcePolicy,
    pub updated_at: Timestamp,
}
impl ResourceRecord {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        name(&self.name)?;
        positive(self.revision)?;
        self.policy.validate()?;
        time(self.updated_at)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceSnapshot {
    pub resource: ResourceRecord,
    pub offered: u64,
    /// Includes unstarted offers and started/unknown sources.
    pub occupied: u64,
    pub unknown: u64,
    pub quarantined: bool,
}
impl ResourceSnapshot {
    pub fn validate(&self) -> Result<()> {
        self.resource.validate()?;
        for n in [self.offered, self.occupied, self.unknown] {
            safe(n)?;
        }
        if self
            .offered
            .checked_add(self.unknown)
            .is_none_or(|n| n > self.occupied)
        {
            return Err(invalid("inconsistent resource occupancy"));
        }
        // A reduction can intentionally leave outstanding debt above the policy.
        Ok(())
    }
    pub fn matches(&self, query: &ResourceQuery) -> bool {
        self.resource.scope == query.scope && self.resource.name == query.name
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionOccurrence {
    pub scope: Scope,
    pub workflow_id: String,
    pub origin_activation_id: String,
    pub key: String,
}
impl AdmissionOccurrence {
    pub fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        for id in [&self.workflow_id, &self.origin_activation_id, &self.key] {
            validate_text(id, 128)?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionOperationKind {
    Model,
    Tool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionOperation {
    pub callable: String,
    pub kind: AdmissionOperationKind,
    pub version: String,
    #[serde(deserialize_with = "crate::observation::required_value")]
    pub arguments: Value,
}
impl AdmissionOperation {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.callable, 512)?;
        validate_text(&self.version, 128)?;
        let Some((module, function)) = self.callable.split_once(':') else {
            return Err(invalid("admission callable requires module:qualname"));
        };
        if !dotted_identifier(module) || !dotted_identifier(function) || !self.arguments.is_object()
        {
            return Err(invalid("invalid admission operation binding"));
        }
        validate_wire_value(&self.arguments)?;
        bound(&self.arguments, ADMISSION_ARGUMENTS_MAX_BYTES)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionBinding {
    pub resource: String,
    pub program: ProgramDescriptor,
    pub operation: AdmissionOperation,
}
impl AdmissionBinding {
    pub fn validate(&self) -> Result<()> {
        name(&self.resource)?;
        self.program.validate()?;
        self.operation.validate()
    }
    pub fn digest(&self) -> Result<Digest> {
        self.validate()?;
        digest(self)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AdmissionWaitRequest {
    New {
        resource: String,
        operation: AdmissionOperation,
    },
    Existing {
        occurrence: AdmissionOccurrence,
    },
}
impl AdmissionWaitRequest {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::New {
                resource,
                operation,
            } => {
                name(resource)?;
                operation.validate()
            }
            Self::Existing { occurrence } => occurrence.validate(),
        }
    }
}
fn dotted_identifier(value: &str) -> bool {
    value.split('.').all(|part| {
        let mut chars = part.chars();
        chars
            .next()
            .is_some_and(|c| c == '_' || c.is_ascii_alphabetic())
            && chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
    })
}
pub(crate) fn name(value: &str) -> Result<()> {
    validate_text(value, 128)?;
    if !value
        .bytes()
        .all(|c| c.is_ascii_alphanumeric() || b"_.:-".contains(&c))
    {
        return Err(invalid("invalid resource name"));
    }
    Ok(())
}
fn safe(n: u64) -> Result<()> {
    if n > ADMISSION_SAFE_INTEGER {
        Err(invalid("admission integer exceeds safe range"))
    } else {
        Ok(())
    }
}
pub(crate) fn positive(n: u64) -> Result<()> {
    safe(n)?;
    if n == 0 {
        Err(invalid("admission integer must be positive"))
    } else {
        Ok(())
    }
}
fn time(n: Timestamp) -> Result<()> {
    if n > 253_402_300_799_999 {
        Err(invalid("admission timestamp exceeds supported range"))
    } else {
        Ok(())
    }
}
fn invalid(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}
fn bound<T: Serialize>(value: &T, bytes: usize) -> Result<()> {
    crate::submission::check_encoded_size(value, bytes, "admission value").map_err(Into::into)
}
fn digest<T: Serialize>(value: &T) -> Result<Digest> {
    let json = serde_json::to_value(value).map_err(|_| invalid("admission encoding"))?;
    Ok(Digest(format!(
        "sha256:{:x}",
        Sha256::digest(canonical_json_bytes(&json)?)
    )))
}
fn canonical_eq<T: Serialize>(a: &T, b: &T) -> bool {
    let bytes = |v: &T| {
        serde_json::to_value(v)
            .map_err(|_| invalid("admission encoding"))
            .and_then(|v| canonical_json_bytes(&v).map_err(Into::into))
    };
    matches!((bytes(a),bytes(b)),(Ok(a),Ok(b)) if a==b)
}
