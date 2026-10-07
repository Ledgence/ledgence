use super::{invalid, operational};
use ledgence_orchestration_api::Result;
use serde_json::Value;
use std::{ffi::OsString, path::Path, process::Stdio, time::Duration};
use tokio::{io::AsyncReadExt, process::Command};

pub(super) trait Process {
    async fn execute(&self, arguments: &[OsString], deadline: Duration) -> Result<Vec<u8>>;
}
pub(super) struct DockerProcess;
impl Process for DockerProcess {
    async fn execute(&self, arguments: &[OsString], deadline: Duration) -> Result<Vec<u8>> {
        let mut command = Command::new("docker");
        command
            .args(arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .env_remove("DOCKER_HOST")
            .env_remove("DOCKER_CONTEXT")
            .env_remove("DOCKER_DEFAULT_PLATFORM")
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|_| {
            operational("cannot start Docker; install Docker and start its local engine")
        })?;
        let mut stdout = child.stdout.take().expect("stdout is piped");
        let operation = async {
            let mut bytes = Vec::new();
            (&mut stdout)
                .take(1024 * 1024 + 1)
                .read_to_end(&mut bytes)
                .await
                .map_err(|_| operational("cannot read Docker result"))?;
            if bytes.len() > 1024 * 1024 {
                return Err(operational("Docker response exceeded its output limit"));
            }
            let status = child
                .wait()
                .await
                .map_err(|_| operational("cannot wait for Docker"))?;
            if !status.success() {
                // Only relay adapter-owned diagnostics, never arbitrary Docker
                // stderr, environment variables, package URLs or source text.
                let message = serde_json::from_slice::<Value>(&bytes)
                    .ok()
                    .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_owned));
                const MESSAGES: &[&str] = &[
                    "builder runtime differs from requested OS, architecture or CPython version",
                    "dependency installation failed: require complete == pins, matching SHA256 hashes and compatible wheels for the target",
                    "application and dependencies have colliding package paths",
                    "preparation produced a symlink or special file",
                    "prepared package exceeds entry limit",
                    "prepared package exceeds expansion limit",
                ];
                return Err(operational(message.filter(|v| MESSAGES.contains(&v.as_str())).unwrap_or_else(||
                    "Docker preparation failed; check the selected local context, runtime image, platform and pinned wheel requirements".into())));
            }
            Ok(bytes)
        };
        tokio::time::timeout(deadline, operation)
            .await
            .map_err(|_| operational("Docker operation timed out"))?
    }
}
pub(super) struct Preparation<'a> {
    pub input: &'a Path,
    pub control: &'a Path,
    pub output: &'a Path,
    pub image: &'a str,
    pub platform: &'a str,
    pub timeout: Duration,
    pub name: &'a str,
}
pub(super) struct Docker<'a, P> {
    process: &'a P,
    context: String,
}
impl<'a, P: Process> Docker<'a, P> {
    pub async fn connect(
        process: &'a P,
        context: Option<&str>,
        docker_host_present: bool,
    ) -> Result<Self> {
        if context.is_none() && docker_host_present {
            return Err(invalid(
                "DOCKER_HOST is set; select a named local Docker context with --context",
            ));
        }
        let context = match context {
            Some(context) => context.to_owned(),
            None => String::from_utf8(
                process
                    .execute(&["context".into(), "show".into()], Duration::from_secs(20))
                    .await?,
            )
            .map_err(|_| operational("Docker returned invalid context text"))?
            .trim()
            .to_owned(),
        };
        if context.is_empty()
            || context.starts_with('-')
            || context.chars().any(char::is_whitespace)
        {
            return Err(invalid("invalid Docker context name"));
        }
        let docker = Self { process, context };
        let endpoint: String = serde_json::from_slice(
            &docker
                .call(
                    &[
                        "context".into(),
                        "inspect".into(),
                        docker.context.clone().into(),
                        "--format".into(),
                        "{{json .Endpoints.docker.Host}}".into(),
                    ],
                    Duration::from_secs(20),
                )
                .await?,
        )
        .map_err(|_| operational("Docker returned an invalid context endpoint"))?;
        if !endpoint.starts_with("unix://") && !endpoint.starts_with("npipe://") {
            return Err(invalid(
                "program build requires a local Docker engine; remote contexts cannot access staged host inputs",
            ));
        }
        Ok(docker)
    }
    async fn call(&self, arguments: &[OsString], deadline: Duration) -> Result<Vec<u8>> {
        let mut args = vec!["--context".into(), self.context.clone().into()];
        args.extend_from_slice(arguments);
        self.process.execute(&args, deadline).await
    }
    pub async fn prepare(
        &self,
        request: Preparation<'_>,
        cancellation: &mut Cancellation,
    ) -> Result<()> {
        let Preparation {
            input,
            control,
            output,
            image,
            platform,
            timeout,
            name,
        } = request;
        let cidfile = output
            .parent()
            .ok_or_else(|| invalid("staging output requires a parent"))?
            .join("container.id");
        let label = format!("com.ledgence.build={name}");
        let mut args: Vec<OsString> = [
            "run",
            "--rm",
            "--name",
            name,
            "--platform",
            platform,
            "--read-only",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--pids-limit",
            "64",
            "--memory",
            "1g",
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,size=536870912",
            "--env",
            "PYTHONDONTWRITEBYTECODE=1",
        ]
        .into_iter()
        .map(Into::into)
        .collect();
        args.extend([
            "--cidfile".into(),
            cidfile.as_os_str().to_owned(),
            "--label".into(),
            label.clone().into(),
        ]);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let metadata = output
                .metadata()
                .map_err(|_| operational("cannot inspect staging owner"))?;
            args.extend([
                "--user".into(),
                format!("{}:{}", metadata.uid(), metadata.gid()).into(),
            ]);
        }
        for (source, destination, readonly) in [
            (input, "/input", true),
            (control, "/control", true),
            (output, "/output", false),
        ] {
            // --mount uses commas as separators even with structured argv.
            if source.as_os_str().as_encoded_bytes().contains(&b',') {
                return Err(invalid("Docker staging parent paths cannot contain commas"));
            }
            let mut mount = OsString::from("type=bind,source=");
            mount.push(source);
            mount.push(format!(
                ",target={destination}{}",
                if readonly { ",readonly" } else { "" }
            ));
            args.extend(["--mount".into(), mount]);
        }
        args.extend(
            [
                "--entrypoint",
                "python3",
                image,
                "-I",
                "-B",
                "/control/prepare.py",
            ]
            .into_iter()
            .map(Into::into),
        );
        let prepared = tokio::select! {
            biased;
            _ = cancellation.wait() => Err(operational("program build cancelled; prepared output was not published")),
            result = self.call(&args, timeout) => result.map(|_| ()) ,
        };
        // A timeout or signal kills the attached CLI, not necessarily its
        // container. Never remove by name: creation can fail on a name collision.
        // Docker's exclusive cidfile or this invocation's label proves ownership.
        if prepared.is_err() && self.cleanup(&cidfile, &label).await.is_err() {
            return Err(operational(format!(
                "build did not complete and container cleanup was unconfirmed; inspect only container {name}"
            )));
        }
        prepared
    }
    async fn cleanup(&self, cidfile: &Path, label: &str) -> Result<()> {
        let list = || {
            [
                "container".into(),
                "ls".into(),
                "--all".into(),
                "--no-trunc".into(),
                "--filter".into(),
                format!("label={label}").into(),
                "--format".into(),
                "{{.ID}}".into(),
            ]
        };
        let ids = match super::files::read_regular(cidfile, 256) {
            Ok(bytes) => bytes,
            Err(_) => self.call(&list(), Duration::from_secs(20)).await?,
        };
        let text = std::str::from_utf8(&ids)
            .map_err(|_| operational("invalid Docker container identity"))?;
        if text.split_whitespace().count() > 1 {
            return Err(operational(
                "ambiguous Docker build ownership; no containers removed",
            ));
        }
        for id in text.split_whitespace() {
            if id.len() != 64
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(operational("invalid Docker container identity"));
            }
            if self
                .call(
                    &["rm".into(), "--force".into(), id.into()],
                    Duration::from_secs(20),
                )
                .await
                .is_err()
            {
                // Docker --rm can already have removed a failed container.
                if !self
                    .call(&list(), Duration::from_secs(20))
                    .await?
                    .iter()
                    .all(u8::is_ascii_whitespace)
                {
                    return Err(operational("Docker container remains after cleanup"));
                }
            }
        }
        Ok(())
    }
}
/// Keep subscriptions alive across every phase, including blocking verification.
/// Tokio's process signal interception persists after a subscription is dropped.
pub(super) struct Cancellation {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}
impl Cancellation {
    pub fn new() -> Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Self {
                interrupt: signal(SignalKind::interrupt())
                    .map_err(|_| operational("cannot subscribe to build cancellation"))?,
                terminate: signal(SignalKind::terminate())
                    .map_err(|_| operational("cannot subscribe to build cancellation"))?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }
    pub async fn wait(&mut self) {
        #[cfg(unix)]
        {
            tokio::select! { _ = self.interrupt.recv() => {}, _ = self.terminate.recv() => {} }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}
