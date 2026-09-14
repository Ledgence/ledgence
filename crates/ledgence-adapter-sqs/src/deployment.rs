//! Explicit single-route SQS executable configuration. Credentials stay outside this file.

use crate::SqsOptions;
use ledgence_orchestration_api::{ContractError, DispatchRoute, Result, Scope, decode_unique_json};
use serde::Deserialize;
use std::{fs::File, io::Read, path::Path, time::Duration};

pub const CONFIG_MAX_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone)]
pub struct DeliveryConfig {
    pub route: DispatchRoute,
    pub sqs: SqsOptions,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    route: DispatchRoute,
    sqs: SqsConfiguration,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SqsConfiguration {
    region: String,
    queue_url: String,
    #[serde(default)]
    endpoint_url: Option<String>,
    #[serde(default)]
    local_credentials: bool,
    #[serde(default = "operation_timeout_ms")]
    operation_timeout_ms: u64,
    #[serde(default = "visibility_timeout_seconds")]
    visibility_timeout_seconds: u64,
}
const fn operation_timeout_ms() -> u64 {
    5000
}
const fn visibility_timeout_seconds() -> u64 {
    60
}

impl DeliveryConfig {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let config: Configuration = decode_unique_json(bytes, CONFIG_MAX_BYTES)?;
        let result = Self {
            route: config.route,
            sqs: SqsOptions {
                region: config.sqs.region,
                queue_url: config.sqs.queue_url,
                endpoint_url: config.sqs.endpoint_url,
                local_credentials: config.sqs.local_credentials,
                operation_timeout: Duration::from_millis(config.sqs.operation_timeout_ms),
                visibility_timeout: Duration::from_secs(config.sqs.visibility_timeout_seconds),
            },
        };
        result.validate()?;
        Ok(result)
    }

    /// Reads at most the configured bound plus one byte, including whitespace.
    /// Executables run this finite-size disk operation outside async lease timers.
    pub fn load(path: &Path) -> Result<Self> {
        let metadata = std::fs::metadata(path).map_err(|_| {
            ContractError::InvalidInput("cannot inspect delivery configuration file".into())
        })?;
        if !metadata.is_file() || metadata.len() > CONFIG_MAX_BYTES as u64 {
            return Err(ContractError::InvalidInput(
                "delivery configuration must be a regular file of at most 16 KiB".into(),
            ));
        }
        let mut bytes = Vec::new();
        File::open(path)
            .map_err(|_| {
                ContractError::InvalidInput("cannot open delivery configuration file".into())
            })?
            .take(CONFIG_MAX_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| {
                ContractError::InvalidInput("cannot read delivery configuration file".into())
            })?;
        Self::decode(&bytes)
    }

    pub fn validate(&self) -> Result<()> {
        self.route.validate()?;
        self.sqs.validate()
    }

    pub fn validate_worker(&self, scope: &Scope, queue: &str) -> Result<()> {
        self.validate()?;
        if &self.route.scope != scope || self.route.queue != queue {
            return Err(ContractError::InvalidInput(
                "worker scope and queue must match the delivery route exactly".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const VALID: &str = r#"{"route":{"scope":{"tenant_id":"tenant","namespace":"billing"},"queue":"queue","destination":"primary"},"sqs":{"region":"us-east-1","queue_url":"https://sqs.us-east-1.amazonaws.com/123456789012/tasks"}}"#;

    #[test]
    fn strict_configuration_defaults_and_explicit_units() {
        let config = DeliveryConfig::decode(VALID.as_bytes()).unwrap();
        assert_eq!(config.sqs.operation_timeout, Duration::from_secs(5));
        assert_eq!(config.sqs.visibility_timeout, Duration::from_secs(60));
        assert!(!config.sqs.local_credentials);
        config
            .validate_worker(&config.route.scope, "queue")
            .unwrap();
        assert!(
            config
                .validate_worker(&config.route.scope, "another")
                .is_err()
        );
        assert!(
            config
                .validate_worker(
                    &Scope {
                        tenant_id: "another".into(),
                        namespace: "billing".into()
                    },
                    "queue"
                )
                .is_err()
        );
        let extended = VALID.replace(
            "\"region\":",
            "\"operation_timeout_ms\":1234,\"visibility_timeout_seconds\":45,\"region\":",
        );
        let config = DeliveryConfig::decode(extended.as_bytes()).unwrap();
        assert_eq!(config.sqs.operation_timeout, Duration::from_millis(1234));
        assert_eq!(config.sqs.visibility_timeout, Duration::from_secs(45));
    }

    #[test]
    fn configuration_rejects_unknown_duplicate_credential_and_invalid_bounds() {
        for text in [
            VALID.replacen('{', "{\"unknown\":true,", 1),
            VALID.replace("\"region\":", "\"region\":\"us-east-2\",\"region\":"),
            VALID.replace("\"region\":", "\"access_key\":\"not-accepted\",\"region\":"),
            VALID.replace("\"region\":", "\"operation_timeout_ms\":0,\"region\":"),
            VALID.replace("\"region\":", "\"operation_timeout_ms\":30001,\"region\":"),
            VALID.replace("\"region\":", "\"operation_timeout_ms\":1.5,\"region\":"),
            VALID.replace(
                "\"region\":",
                "\"visibility_timeout_seconds\":29,\"region\":",
            ),
            VALID.replace("\"region\":", "\"local_credentials\":true,\"region\":"),
        ] {
            assert!(DeliveryConfig::decode(text.as_bytes()).is_err(), "{text}");
        }
        let mut padded = VALID.as_bytes().to_vec();
        padded.resize(CONFIG_MAX_BYTES, b' ');
        DeliveryConfig::decode(&padded).unwrap();
        padded.push(b' ');
        assert!(DeliveryConfig::decode(&padded).is_err());
    }

    #[test]
    fn configuration_load_is_bounded_and_rejects_non_files() {
        let path =
            std::env::temp_dir().join(format!("ledgence-delivery-config-{}", std::process::id()));
        std::fs::write(&path, VALID).unwrap();
        let result = DeliveryConfig::load(&path);
        std::fs::remove_file(&path).unwrap();
        result.unwrap();
        assert!(DeliveryConfig::load(&std::env::temp_dir()).is_err());
    }
}
