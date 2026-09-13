use std::{collections::BTreeMap, fmt};

/// Explicit, bounded OTLP/HTTP configuration. No SDK environment defaults are used.
#[derive(Clone, Debug)]
pub struct Config {
    pub(crate) endpoint: Option<String>,
    pub(crate) service_name: String,
    pub(crate) service_version: String,
    pub(crate) instance_id: String,
    pub(crate) environment: Option<String>,
    pub(crate) root_ratio: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(pub(crate) String);
impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ConfigError {}

impl Config {
    pub fn from_env(service_name: &str, service_version: &str) -> Result<Self, ConfigError> {
        Self::from_variables(
            service_name,
            service_version,
            std::env::vars().filter(|(k, _)| k.starts_with("OTEL_")),
        )
    }

    /// Parse a supplied environment snapshot without modifying process-global variables.
    pub fn from_variables(
        service_name: &str,
        service_version: &str,
        variables: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, ConfigError> {
        let vars: BTreeMap<_, _> = variables.into_iter().collect();
        let allowed = [
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
            "OTEL_EXPORTER_OTLP_PROTOCOL",
            "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
            "OTEL_SERVICE_NAME",
            "OTEL_RESOURCE_ATTRIBUTES",
            "OTEL_TRACES_SAMPLER",
            "OTEL_TRACES_SAMPLER_ARG",
            "OTEL_SDK_DISABLED",
        ];
        for key in vars.keys() {
            if !allowed.contains(&key.as_str()) {
                return Err(ConfigError(format!("unsupported telemetry setting: {key}")));
            }
        }
        for key in [
            "OTEL_EXPORTER_OTLP_PROTOCOL",
            "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        ] {
            if vars.get(key).is_some_and(|v| v != "http/protobuf") {
                return Err(ConfigError(format!("{key} must be http/protobuf")));
            }
        }
        let disabled = match vars.get("OTEL_SDK_DISABLED").map(String::as_str) {
            None | Some("false") => false,
            Some("true") => true,
            _ => {
                return Err(ConfigError(
                    "OTEL_SDK_DISABLED must be true or false".into(),
                ));
            }
        };
        let endpoint = vars.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").cloned();
        if let Some(endpoint) = &endpoint {
            let url = reqwest::Url::parse(endpoint).map_err(|_| {
                ConfigError(
                    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT must be an absolute HTTP(S) URL".into(),
                )
            })?;
            if !["http", "https"].contains(&url.scheme())
                || url.host_str().is_none()
                || url.fragment().is_some()
                || !url.username().is_empty()
                || url.password().is_some()
                || endpoint.len() > 2048
            {
                return Err(ConfigError("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT must be an HTTP(S) URL without user information or fragment (at most 2048 bytes)".into()));
            }
        }
        let sampler = vars
            .get("OTEL_TRACES_SAMPLER")
            .map(String::as_str)
            .unwrap_or("parentbased_traceidratio");
        if !["parentbased_traceidratio", "parentbased_always_on"].contains(&sampler) {
            return Err(ConfigError(
                "OTEL_TRACES_SAMPLER must be parentbased_traceidratio or parentbased_always_on"
                    .into(),
            ));
        }
        let root_ratio = vars.get("OTEL_TRACES_SAMPLER_ARG").map_or(Ok(1.0), |v| {
            v.parse::<f64>().map_err(|_| {
                ConfigError("OTEL_TRACES_SAMPLER_ARG must be a finite ratio from 0 to 1".into())
            })
        })?;
        if !root_ratio.is_finite()
            || !(0.0..=1.0).contains(&root_ratio)
            || (sampler == "parentbased_always_on" && root_ratio != 1.0)
        {
            return Err(ConfigError("OTEL_TRACES_SAMPLER_ARG must be a finite ratio from 0 to 1 (1 for parentbased_always_on)".into()));
        }
        let mut instance_id = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let mut environment = None;
        if let Some(attributes) = vars.get("OTEL_RESOURCE_ATTRIBUTES") {
            for item in attributes.split(',').filter(|item| !item.is_empty()) {
                let (key, value) = item.split_once('=').ok_or_else(|| {
                    ConfigError("OTEL_RESOURCE_ATTRIBUTES requires key=value entries".into())
                })?;
                bounded(value, "resource attribute")?;
                match key {
                    "service.instance.id" => instance_id = value.to_owned(),
                    "deployment.environment.name" => environment = Some(value.to_owned()),
                    _ => {
                        return Err(ConfigError(format!(
                            "unsupported OTEL_RESOURCE_ATTRIBUTES key: {key}"
                        )));
                    }
                }
            }
        }
        let service_name = vars
            .get("OTEL_SERVICE_NAME")
            .map(String::as_str)
            .unwrap_or(service_name);
        bounded(service_name, "service name")?;
        bounded(service_version, "service version")?;
        Ok(Self {
            endpoint: if disabled { None } else { endpoint },
            service_name: service_name.into(),
            service_version: service_version.into(),
            instance_id,
            environment,
            root_ratio,
        })
    }

    pub fn enabled(&self) -> bool {
        self.endpoint.is_some()
    }
}

fn bounded(value: &str, name: &str) -> Result<(), ConfigError> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        Err(ConfigError(format!(
            "{name} must contain 1 to 256 bytes without control characters"
        )))
    } else {
        Ok(())
    }
}
