//! Fixed server-side compatibility binding for a self-hosted installation.
use crate::{ContractError, Result, Scope, validate_text};
use serde::{Deserialize, Serialize};

/// Public instance identity and private compatibility binding. Browser requests
/// never select this scope; HTTP console projections omit it explicitly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelfHostedInstanceContext {
    pub instance_id: String,
    pub name: String,
    pub scope: Scope,
}
impl SelfHostedInstanceContext {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.instance_id, 128)?;
        validate_text(&self.name, 128)?;
        self.scope.validate()
    }
    pub fn require_scope(&self, scope: &Scope) -> Result<()> {
        scope.validate()?;
        if scope != &self.scope {
            return Err(ContractError::NotFound);
        }
        Ok(())
    }
}

/// Server configuration only. Its binding and origins are never a browser scope
/// selector; adapters expose a separate public configuration projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelfHostedInstanceConfig {
    pub instance_id: String,
    pub name: String,
    #[serde(default = "default_scope")]
    pub scope: Scope,
    #[serde(default)]
    pub suggested_queues: Vec<String>,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
}
fn default_scope() -> Scope {
    Scope {
        tenant_id: "default".into(),
        namespace: "default".into(),
    }
}
impl SelfHostedInstanceConfig {
    pub fn context(&self) -> SelfHostedInstanceContext {
        SelfHostedInstanceContext {
            instance_id: self.instance_id.clone(),
            name: self.name.clone(),
            scope: self.scope.clone(),
        }
    }
    pub fn validate(&self) -> Result<()> {
        self.context().validate()?;
        if self.suggested_queues.len() > 100 || self.allowed_origins.len() > 16 {
            return Err(ContractError::InvalidInput(
                "instance configuration exceeds collection limits".into(),
            ));
        }
        let mut queues = std::collections::HashSet::new();
        for queue in &self.suggested_queues {
            validate_text(queue, 128)?;
            if !queues.insert(queue) {
                return Err(ContractError::InvalidInput(
                    "duplicate suggested queue".into(),
                ));
            }
        }
        let mut origins = std::collections::HashSet::new();
        for origin in &self.allowed_origins {
            validate_text(origin, 2048)?;
            if !origins.insert(origin) {
                return Err(ContractError::InvalidInput(
                    "duplicate allowed origin".into(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn configuration_is_strict_and_defaults_to_one_binding() {
        let config: SelfHostedInstanceConfig =
            serde_json::from_str(r#"{"instance_id":"local","name":"Local instance"}"#).unwrap();
        config.validate().unwrap();
        assert_eq!(config.scope, default_scope());
        assert!(
            serde_json::from_str::<SelfHostedInstanceConfig>(
                r#"{"instance_id":"local","name":"Local","tenant_id":"other"}"#
            )
            .is_err()
        );
        assert!(
            config
                .context()
                .require_scope(&Scope {
                    tenant_id: "other".into(),
                    namespace: "default".into()
                })
                .is_err()
        );
    }
}
