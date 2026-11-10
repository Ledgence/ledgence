//! Portable authentication results. Credentials and policy implementations belong
//! in adapters; caller-supplied labels never establish a verified identity.
use crate::{ContractError, Result, Scope, SelfHostedInstanceContext, validate_text};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalIdentity {
    pub issuer: String,
    pub subject: String,
}
impl PrincipalIdentity {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.issuer, 256)?;
        validate_text(&self.subject, 128)
    }
}

/// An installation's immutable authority and compatibility scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessAudience {
    pub authority_id: String,
    pub instance_id: String,
    pub scope: Scope,
}
impl AccessAudience {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.authority_id, 128)?;
        validate_text(&self.instance_id, 128)?;
        self.scope.validate()
    }
    pub fn require_instance(&self, instance: &SelfHostedInstanceContext) -> Result<()> {
        self.validate()?;
        instance.validate()?;
        if self.instance_id != instance.instance_id || self.scope != instance.scope {
            return Err(ContractError::Forbidden);
        }
        Ok(())
    }
    pub fn issuer(&self) -> String {
        format!("urn:ledgence:authority:{}", self.authority_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalClass {
    Operator,
    Service,
    Worker,
}
impl PrincipalClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Operator => "operator",
            Self::Service => "service",
            Self::Worker => "worker",
        }
    }
    pub fn validate_capabilities(self, capabilities: &[AccessCapability]) -> Result<()> {
        let mut seen = HashSet::new();
        if capabilities.is_empty() || capabilities.len() > 10 {
            return Err(invalid());
        }
        for capability in capabilities {
            if !seen.insert(capability)
                || (*capability == AccessCapability::ResourceControl && self != Self::Operator)
                || (self == Self::Worker) != (*capability == AccessCapability::WorkerExecute)
            {
                return Err(invalid());
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessCapability {
    MetadataRead,
    SensitiveRead,
    ExecutionSubmit,
    ExecutionControl,
    ProgramPublish,
    ArtifactWrite,
    ApprovalDecide,
    EffectResolve,
    WorkerExecute,
    ResourceControl,
}
impl AccessCapability {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MetadataRead => "metadata_read",
            Self::SensitiveRead => "sensitive_read",
            Self::ExecutionSubmit => "execution_submit",
            Self::ExecutionControl => "execution_control",
            Self::ProgramPublish => "program_publish",
            Self::ArtifactWrite => "artifact_write",
            Self::ApprovalDecide => "approval_decide",
            Self::EffectResolve => "effect_resolve",
            Self::WorkerExecute => "worker_execute",
            Self::ResourceControl => "resource_control",
        }
    }
}

/// A trusted verifier's assertion, never a deserializable request body. It does
/// not itself establish durable decision attribution or an audit receipt.
///
/// ```compile_fail
/// use ledgence_orchestration_api::VerifiedPrincipal;
/// let _: VerifiedPrincipal = serde_json::from_str("{}").unwrap();
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedPrincipal {
    identity: PrincipalIdentity,
    audience: AccessAudience,
    credential_id: String,
    class: PrincipalClass,
    expires_at_ms: u64,
    capabilities: Vec<AccessCapability>,
}
impl VerifiedPrincipal {
    /// Called only by trusted authentication adapters after credential checking.
    /// This validates the assertion's shape; it does not verify a credential.
    pub fn new(
        identity: PrincipalIdentity,
        audience: AccessAudience,
        credential_id: String,
        class: PrincipalClass,
        expires_at_ms: u64,
        capabilities: Vec<AccessCapability>,
    ) -> Result<Self> {
        identity.validate()?;
        audience.validate()?;
        validate_text(&credential_id, 128)?;
        class.validate_capabilities(&capabilities)?;
        if expires_at_ms == 0 || expires_at_ms > i64::MAX as u64 {
            return Err(invalid());
        }
        Ok(Self {
            identity,
            audience,
            credential_id,
            class,
            expires_at_ms,
            capabilities,
        })
    }
    pub fn identity(&self) -> &PrincipalIdentity {
        &self.identity
    }
    pub fn audience(&self) -> &AccessAudience {
        &self.audience
    }
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }
    pub fn class(&self) -> PrincipalClass {
        self.class
    }
    pub fn expires_at_ms(&self) -> u64 {
        self.expires_at_ms
    }
    pub fn capabilities(&self) -> &[AccessCapability] {
        &self.capabilities
    }
    pub fn require(&self, capability: AccessCapability) -> Result<()> {
        if self.capabilities.contains(&capability) {
            Ok(())
        } else {
            Err(ContractError::Forbidden)
        }
    }
    /// `now_ms` is the authentication adapter's checked wall clock, independent
    /// of database-authoritative orchestration timestamps.
    pub fn require_at(&self, audience: &AccessAudience, now_ms: u64) -> Result<()> {
        if now_ms >= self.expires_at_ms {
            return Err(ContractError::Unauthenticated);
        }
        if &self.audience != audience {
            return Err(ContractError::Forbidden);
        }
        Ok(())
    }
}

/// Credential verification must complete before dispatch. Implementations must
/// return no credentials in diagnostics and recheck expiry on every call.
pub trait CredentialVerifier: Send + Sync {
    fn audience(&self) -> &AccessAudience;
    fn verify(&self, credential: &str, now_ms: u64) -> Result<VerifiedPrincipal>;
}

fn invalid() -> ContractError {
    ContractError::InvalidInput("invalid access principal or capabilities".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn audience() -> AccessAudience {
        AccessAudience {
            authority_id: "authority".into(),
            instance_id: "instance".into(),
            scope: Scope {
                tenant_id: "tenant".into(),
                namespace: "namespace".into(),
            },
        }
    }
    #[test]
    fn class_boundaries_and_duplicate_capabilities_are_rejected() {
        for capabilities in [
            vec![],
            vec![AccessCapability::WorkerExecute],
            vec![
                AccessCapability::MetadataRead,
                AccessCapability::MetadataRead,
            ],
        ] {
            assert!(
                PrincipalClass::Operator
                    .validate_capabilities(&capabilities)
                    .is_err()
            );
            assert!(
                PrincipalClass::Service
                    .validate_capabilities(&capabilities)
                    .is_err()
            );
        }
        assert!(
            PrincipalClass::Worker
                .validate_capabilities(&[AccessCapability::MetadataRead])
                .is_err()
        );
        assert!(
            PrincipalClass::Worker
                .validate_capabilities(&[
                    AccessCapability::WorkerExecute,
                    AccessCapability::MetadataRead
                ])
                .is_err()
        );
        PrincipalClass::Worker
            .validate_capabilities(&[AccessCapability::WorkerExecute])
            .unwrap();
    }
    #[test]
    fn verified_assertions_recheck_expiry_permission_and_all_audience_fields() {
        let audience = audience();
        let principal = VerifiedPrincipal::new(
            PrincipalIdentity {
                issuer: audience.issuer(),
                subject: "alice".into(),
            },
            audience.clone(),
            "credential".into(),
            PrincipalClass::Operator,
            100,
            vec![AccessCapability::MetadataRead],
        )
        .unwrap();
        principal.require_at(&audience, 99).unwrap();
        assert_eq!(
            principal.require_at(&audience, 100),
            Err(ContractError::Unauthenticated)
        );
        assert_eq!(
            principal.require(AccessCapability::SensitiveRead),
            Err(ContractError::Forbidden)
        );
        for field in 0..4 {
            let mut changed = audience.clone();
            match field {
                0 => changed.authority_id.push('x'),
                1 => changed.instance_id.push('x'),
                2 => changed.scope.tenant_id.push('x'),
                _ => changed.scope.namespace.push('x'),
            }
            assert_eq!(
                principal.require_at(&changed, 99),
                Err(ContractError::Forbidden)
            );
        }
    }
}
