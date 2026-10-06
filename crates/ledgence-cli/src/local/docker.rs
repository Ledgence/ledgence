use super::{State, kit::Distribution, operational};
use crate::args::invalid;
use ledgence_orchestration_api::Result;
use serde_json::Value;
use std::{
    path::Path,
    process::{Command, Stdio},
};

pub(super) struct Docker {
    pub context: String,
    pub endpoint: String,
    pub platform: String,
}

impl Docker {
    pub fn connect(context: Option<&str>) -> Result<Self> {
        if context.is_none() && std::env::var_os("DOCKER_HOST").is_some() {
            return Err(invalid(
                "DOCKER_HOST is set; select a named local Docker context with --context when creating the installation",
            ));
        }
        let context = match context {
            Some(value) => value.to_owned(),
            None => output(
                Command::new("docker").args(["context", "show"]),
                "select Docker context",
            )?,
        };
        if context.is_empty()
            || context.starts_with('-')
            || context.chars().any(char::is_whitespace)
        {
            return Err(invalid("invalid Docker context name"));
        }
        let mut docker = Self {
            context,
            endpoint: String::new(),
            platform: String::new(),
        };
        let endpoint = output(
            docker.command().args([
                "context",
                "inspect",
                &docker.context,
                "--format",
                "{{json .Endpoints.docker.Host}}",
            ]),
            "inspect Docker context",
        )?;
        docker.endpoint = serde_json::from_str(&endpoint)
            .map_err(|_| operational("Docker returned an invalid context endpoint"))?;
        if !docker.endpoint.starts_with("unix://") && !docker.endpoint.starts_with("npipe://") {
            return Err(invalid(
                "ledgence local requires a local Docker endpoint; use the distribution's Compose files directly for a remote engine",
            ));
        }
        let version = output(
            docker.command().args(["compose", "version", "--short"]),
            "check Docker Compose (2.23.1 or newer is required)",
        )?;
        if !supported_compose(&version) {
            return Err(invalid(format!(
                "Docker Compose 2.23.1 or newer is required; found {version}"
            )));
        }
        let info = output(
            docker.command().args(["info", "--format", "{{json .}}"]),
            "connect to Docker; start Docker Engine or Docker Desktop",
        )?;
        let info: Value = serde_json::from_str(&info)
            .map_err(|_| operational("Docker returned invalid engine information"))?;
        let architecture = match info["Architecture"].as_str() {
            Some("amd64" | "x86_64") => "amd64",
            Some("arm64" | "aarch64") => "arm64",
            _ => {
                return Err(invalid(
                    "local distribution supports Docker engines on amd64 or arm64",
                ));
            }
        };
        if info["OSType"].as_str() != Some("linux") {
            return Err(invalid(
                "the local distribution requires Docker Linux containers",
            ));
        }
        docker.platform = format!("linux/{architecture}");
        Ok(docker)
    }

    pub fn validate(&self, distribution: &Distribution) -> Result<()> {
        if !distribution.platforms.contains(&self.platform) {
            return Err(invalid(format!(
                "distribution does not support Docker engine platform {}",
                self.platform
            )));
        }
        Ok(())
    }

    fn command(&self) -> Command {
        let mut command = Command::new("docker");
        command.args(["--context", &self.context]);
        command
            .env_remove("DOCKER_HOST")
            .env_remove("DOCKER_CONTEXT");
        command
    }

    pub fn compose(&self, directory: &Path, state: &State) -> Command {
        let mut command = self.command();
        // A shell's unrelated Compose settings must not change the saved stack.
        for (key, _) in std::env::vars_os() {
            if key.to_str().is_some_and(|key| key.starts_with("COMPOSE_")) {
                command.env_remove(key);
            }
        }
        command
            .args([
                "compose",
                "--project-name",
                &state.project,
                "--project-directory",
            ])
            .arg(directory)
            .arg("--env-file")
            .arg(directory.join(".ledgence.env"))
            .arg("--file")
            .arg(directory.join("compose.yaml"))
            .env("LEDGENCE_PORT", state.port.to_string())
            .env("LEDGENCE_CONCURRENCY", state.concurrency.to_string())
            .env_remove("DOCKER_DEFAULT_PLATFORM");
        command
    }

    pub fn owns_port(&self, directory: &Path, state: &State) -> Result<bool> {
        let text = output(
            self.compose(directory, state)
                .args(["ps", "--all", "--format", "json"]),
            "inspect existing local containers",
        )?;
        let rows: Vec<Value> = if text.is_empty() {
            Vec::new()
        } else if text.starts_with('[') {
            serde_json::from_str(&text)
                .map_err(|_| operational("invalid Docker Compose status response"))?
        } else {
            text.lines()
                .map(serde_json::from_str)
                .collect::<std::result::Result<_, _>>()
                .map_err(|_| operational("invalid Docker Compose status response"))?
        };
        Ok(rows.iter().any(|row| {
            row["Service"] == "orchestrator"
                && row["State"] == "running"
                && row["Publishers"].as_array().is_some_and(|ports| {
                    ports.iter().any(|port| {
                        port["PublishedPort"].as_u64() == Some(u64::from(state.port))
                            && port["TargetPort"].as_u64() == Some(8080)
                            && port["URL"].as_str() == Some("127.0.0.1")
                    })
                })
        }))
    }
}

fn output(command: &mut Command, action: &str) -> Result<String> {
    let output = command
        .stdin(Stdio::null())
        .output()
        .map_err(|error| operational(format!("cannot {action}: {error}")))?;
    if !output.status.success() {
        return Err(operational(format!(
            "cannot {action}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .map_err(|_| operational(format!("invalid UTF-8 while attempting to {action}")))
}

fn supported_compose(version: &str) -> bool {
    let version = version.trim_start_matches('v');
    if version.contains("-rc") || version.contains("-beta") || version.contains("-alpha") {
        return false;
    }
    let mut parts = version.split('-').next().unwrap_or("").split('.');
    let values: Vec<u32> = parts
        .by_ref()
        .map(str::parse)
        .collect::<std::result::Result<_, _>>()
        .unwrap_or_default();
    values.len() == 3 && (values[0], values[1], values[2]) >= (2, 23, 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compose_requires_supported_stable_version() {
        for version in ["2.23.1", "v2.40.0", "v2.23.1-desktop.1", "3.0.0"] {
            assert!(supported_compose(version));
        }
        for version in [
            "1.29.2",
            "2.23.0",
            "2.23.1-rc.1",
            "2.23",
            "latest",
            "2.23.1.0",
        ] {
            assert!(!supported_compose(version));
        }
    }
}
