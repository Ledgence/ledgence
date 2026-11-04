//! Single-route and multi-route SQS executable configuration. Credentials stay outside this file.

use crate::{SqsOptions, SqsQueue};
use ledgence_orchestration_api::{
    ContractError, DispatchRoute, MAX_DISPATCH_ROUTES, Result, Scope, decode_unique_json,
};
use serde::Deserialize;
use std::{collections::HashSet, fs::File, io::Read, path::Path, time::Duration};

pub const CONFIG_MAX_BYTES: usize = 16 * 1024;
const ROUTES_MAX_BYTES: usize = CONFIG_MAX_BYTES * MAX_DISPATCH_ROUTES;

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

impl TryFrom<Configuration> for DeliveryConfig {
    type Error = ContractError;
    fn try_from(config: Configuration) -> Result<Self> {
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
}

impl DeliveryConfig {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Self::try_from(decode_unique_json::<Configuration>(
            bytes,
            CONFIG_MAX_BYTES,
        )?)
    }
    /// Reads at most the configured bound plus one byte, including whitespace.
    /// Executables run this finite-size disk operation outside async lease timers.
    pub fn load(path: &Path) -> Result<Self> {
        Self::decode(&read_file(path, CONFIG_MAX_BYTES)?)
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

fn invalid(reason: &str) -> ContractError {
    ContractError::InvalidInput(reason.into())
}

fn read_file(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let metadata = std::fs::metadata(path)
        .map_err(|_| invalid("cannot inspect delivery configuration file"))?;
    if !metadata.is_file() || metadata.len() > limit as u64 {
        return Err(invalid(
            "delivery configuration requires a bounded regular file",
        ));
    }
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|_| invalid("cannot open delivery configuration file"))?
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid("cannot read delivery configuration file"))?;
    Ok(bytes)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RouteList {
    routes: Vec<Configuration>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Document {
    Single(Configuration),
    Multiple(RouteList),
}

/// An immutable set of SQS Standard routes. The legacy one-route document
/// remains valid; the list form is bounded and binds one instance scope.
pub struct DeliveryRoutes(Vec<DeliveryConfig>);

impl DeliveryRoutes {
    pub fn load(path: &Path) -> Result<Self> {
        Self::decode(&read_file(path, ROUTES_MAX_BYTES)?)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let configs = match decode_unique_json::<Document>(bytes, ROUTES_MAX_BYTES)? {
            Document::Single(config) if bytes.len() <= CONFIG_MAX_BYTES => vec![config],
            Document::Single(_) => {
                return Err(invalid("single-route configuration exceeds 16 KiB"));
            }
            Document::Multiple(list) => list.routes,
        };
        if !(1..=MAX_DISPATCH_ROUTES).contains(&configs.len()) {
            return Err(invalid("configure between 1 and 64 SQS routes"));
        }
        let routes = configs
            .into_iter()
            .map(DeliveryConfig::try_from)
            .collect::<Result<Vec<_>>>()?;
        let scope = &routes[0].route.scope;
        let mut queues = HashSet::new();
        let mut destinations = HashSet::new();
        let mut urls = HashSet::new();
        for cfg in &routes {
            if &cfg.route.scope != scope {
                return Err(invalid("all SQS routes must have the same scope"));
            }
            if !queues.insert(&cfg.route.queue)
                || !destinations.insert(&cfg.route.destination)
                || !urls.insert(crate::validate_url(&cfg.sqs.queue_url)?)
            {
                return Err(invalid(
                    "SQS routes require distinct queue names, destinations and queue URLs",
                ));
            }
        }
        Ok(Self(routes))
    }

    pub fn scope(&self) -> &Scope {
        &self.0[0].route.scope
    }

    pub fn validate_worker(&self, scope: &Scope, queues: &[String]) -> Result<()> {
        let names: HashSet<_> = queues.iter().collect();
        if self.scope() != scope
            || names.len() != queues.len()
            || names.len() != self.0.len()
            || self.0.iter().any(|cfg| !names.contains(&cfg.route.queue))
        {
            return Err(invalid(
                "worker scope and planned queues must match the delivery routes exactly",
            ));
        }
        Ok(())
    }

    /// Verify all bound queues before the executable registers sessions or
    /// activates routes. Use the ARN returned by SQS to catch URL aliases that
    /// cannot be detected syntactically (including custom endpoint aliases).
    pub async fn connect(self) -> Result<Vec<(DispatchRoute, SqsQueue)>> {
        let identify = self.0.len() > 1;
        let mut identities = HashSet::new();
        let mut connected = Vec::with_capacity(self.0.len());
        for cfg in self.0 {
            let queue = SqsQueue::connect_route(cfg.sqs, identify).await?;
            if let Some(arn) = &queue.queue_arn
                && !identities.insert(arn.clone())
            {
                return Err(invalid("multiple SQS routes resolve to the same queue ARN"));
            }
            connected.push((cfg.route, queue));
        }
        Ok(connected)
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

    fn route(name: &str) -> serde_json::Value {
        let mut value: serde_json::Value = serde_json::from_str(VALID).unwrap();
        value["route"]["queue"] = name.into();
        value["route"]["destination"] = format!("dest-{name}").into();
        value["sqs"]["queue_url"] = format!("https://queue.example/{name}").into();
        value
    }

    #[test]
    fn route_sets_preserve_legacy_limits_and_bind_an_exact_unordered_worker_set() {
        let one = DeliveryRoutes::decode(VALID.as_bytes()).unwrap();
        one.validate_worker(one.scope(), &["queue".into()]).unwrap();
        let mut padded = VALID.as_bytes().to_vec();
        padded.resize(CONFIG_MAX_BYTES + 1, b' ');
        assert!(DeliveryRoutes::decode(&padded).is_err());
        let entries: Vec<_> = (0..MAX_DISPATCH_ROUTES)
            .map(|i| route(&format!("q{i}")))
            .collect();
        let mut file = serde_json::to_vec(&serde_json::json!({"routes":entries})).unwrap();
        file.resize(ROUTES_MAX_BYTES, b' ');
        let set = DeliveryRoutes::decode(&file).unwrap();
        let names: Vec<_> = (0..MAX_DISPATCH_ROUTES)
            .rev()
            .map(|i| format!("q{i}"))
            .collect();
        set.validate_worker(set.scope(), &names).unwrap();
        assert!(set.validate_worker(set.scope(), &names[1..]).is_err());
        let mut wrong = names.clone();
        wrong[0] = wrong[1].clone();
        assert!(set.validate_worker(set.scope(), &wrong).is_err());
        wrong[0] = "elsewhere".into();
        assert!(set.validate_worker(set.scope(), &wrong).is_err());
        assert!(
            set.validate_worker(
                &Scope {
                    tenant_id: "foreign".into(),
                    namespace: "other".into()
                },
                &names
            )
            .is_err()
        );
        file.push(b' ');
        assert!(DeliveryRoutes::decode(&file).is_err());
    }

    #[test]
    fn ambiguous_route_sets_fail_before_any_network_access() {
        let a = route("a");
        let b = route("b");
        let mut scope = b.clone();
        scope["route"]["scope"]["namespace"] = "other".into();
        let mut same_queue = b.clone();
        same_queue["route"]["queue"] = a["route"]["queue"].clone();
        let mut alias = b.clone();
        alias["route"]["destination"] = a["route"]["destination"].clone();
        let mut url = b.clone();
        url["sqs"]["queue_url"] = "https://QUEUE.example:443/a".into();
        use serde_json::json;
        for v in [
            json!({"routes":[]}),
            json!({"routes": vec![a.clone();MAX_DISPATCH_ROUTES+1]}),
            json!({"routes":[a.clone()], "route":a["route"]}),
            json!({"routes":[a.clone(),scope]}),
            json!({"routes":[a.clone(),same_queue]}),
            json!({"routes":[a.clone(),alias]}),
            json!({"routes":[a,url]}),
        ] {
            assert!(DeliveryRoutes::decode(&serde_json::to_vec(&v).unwrap()).is_err());
        }
        assert!(DeliveryRoutes::decode(br#"{"routes":[],"routes":[]}"#).is_err());
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
