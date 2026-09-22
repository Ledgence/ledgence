//! Supervised reusable CPython sessions. The core owns pool capacity and reuse policy.
//!
//! A detached actor owns every child until it is reaped. Dropping a caller future
//! cancels that exchange; it never transfers the live child back to the pool.

use ledgence_worker_api::{
    DEFAULT_RUNTIME_FRAME_MAX_BYTES, Error, ErrorKind, ExecutionRuntime, ExecutionSession,
    PortFuture, PreparedArtifact, ProgramOutcome, RUNTIME_REQUEST_MAX_BYTES, Result, RunControl,
    RuntimeInvocation, RuntimeRequest, RuntimeRequestHandler, StartOutcome,
    validate_runtime_payload, validate_wire_value,
};
#[cfg(test)]
mod guard_tests;
mod logs;

use logs::LogForwarder;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    future::Future,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    sync::{Mutex, mpsc, oneshot, watch},
    task::JoinHandle,
};
use tracing::instrument::WithSubscriber;

#[derive(Debug, Clone)]
pub struct SubprocessConfig {
    /// Maximum encoded bytes including the newline, in either protocol direction.
    /// The default supports the submission/delivery profile. Explicit local
    /// overrides can be smaller or larger and may be incompatible with delivery.
    pub max_frame_bytes: usize,
    /// Maximum stderr bytes emitted to tracing between invocation starts.
    /// The pipe continues draining after this allowance is exhausted.
    pub max_log_bytes: usize,
    pub startup_timeout: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for SubprocessConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_RUNTIME_FRAME_MAX_BYTES,
            max_log_bytes: 64 * 1024,
            startup_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(2),
        }
    }
}

#[derive(Clone)]
pub struct SubprocessRuntime {
    python: PathBuf,
    runner: PathBuf,
    config: SubprocessConfig,
    owners: ResourceRegistry,
}

type SharedResources = Arc<Mutex<Resources>>;
type ResourceRegistry = Arc<StdMutex<Vec<SharedResources>>>;

/// Kept independently of supervisor futures, including if an actor panics.
struct Resources {
    child: Child,
    artifact: Option<PreparedArtifact>,
    workspace: Option<PathBuf>,
    logs: Option<JoinHandle<()>>,
    reaped: bool,
    /// The signal outcome is committed before wait() can suspend. Reaping may
    /// be interrupted, but an already observed signal must never be forgotten.
    group_signal_attempted: bool,
    group_error: Option<GroupTerminationError>,
    #[cfg(test)]
    before_reap: Option<Arc<tokio::sync::Semaphore>>,
    #[cfg(test)]
    group_signal_attempts: usize,
}

struct GroupTerminationError {
    error: Error,
    /// After reaping, this identifier is only valid for a read-only existence
    /// check. It must never be used to deliver another signal.
    #[cfg(unix)]
    group_id: nix::unistd::Pid,
}

impl SubprocessRuntime {
    /// The interpreter is a host dependency; the runner is the absolute SDK bootstrap.
    pub fn new(python: impl Into<PathBuf>, runner: impl Into<PathBuf>) -> Self {
        Self {
            python: python.into(),
            runner: runner.into(),
            config: SubprocessConfig::default(),
            owners: Arc::new(StdMutex::new(Vec::new())),
        }
    }
    pub fn with_config(mut self, config: SubprocessConfig) -> Self {
        self.config = config;
        self
    }
}

impl ExecutionRuntime for SubprocessRuntime {
    fn start<'a>(
        &'a self,
        artifact: PreparedArtifact,
        control: RunControl,
    ) -> PortFuture<'a, StartOutcome> {
        Box::pin(async move {
            control.check()?;
            artifact.manifest().validate_host()?;
            if self.config.max_frame_bytes < 256
                || self.config.startup_timeout.is_zero()
                || self.config.shutdown_timeout.is_zero()
                || Instant::now()
                    .checked_add(self.config.startup_timeout)
                    .is_none()
                || Instant::now()
                    .checked_add(self.config.shutdown_timeout)
                    .is_none()
            {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid subprocess limits",
                ));
            }
            let runner = std::fs::canonicalize(&self.runner)?;
            let root = std::fs::canonicalize(artifact.root())?;
            // Resolve explicit relative paths before changing the child's working directory.
            let python = if self.python.components().count() > 1 {
                std::fs::canonicalize(&self.python)?
            } else {
                self.python.clone()
            };
            let workspace = tempfile::Builder::new()
                .prefix("ledgence-session-")
                .tempdir()?;
            let mut command = Command::new(python);
            command
                .args(["-I", "-S", "-B"])
                .current_dir(workspace.path())
                .arg(runner)
                .arg("--package-root")
                .arg(root)
                .arg("--handler")
                .arg(&artifact.manifest().handler)
                .arg("--python-version")
                .arg(&artifact.manifest().runtime.python)
                .arg("--max-input-bytes")
                .arg(self.config.max_frame_bytes.to_string())
                .arg("--max-output-bytes")
                .arg(self.config.max_frame_bytes.to_string())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
            // Keep v1 launches compatible with old helpers that do not know this flag.
            if artifact.manifest().runtime.protocol >= 2 {
                command
                    .arg("--protocol-version")
                    .arg(artifact.manifest().runtime.protocol.to_string());
            }
            #[cfg(unix)]
            command.process_group(0);
            let mut child = command
                .spawn()
                .map_err(|e| Error::new(ErrorKind::Runtime, format!("cannot start Python: {e}")))?;
            let pid = child.id().expect("newly spawned child has a PID");
            let stdin = child.stdin.take().expect("configured piped stdin");
            let stdout = child.stdout.take().expect("configured piped stdout");
            let stderr = child.stderr.take().expect("configured piped stderr");
            let owner = Arc::new(Mutex::new(Resources {
                child,
                artifact: Some(artifact),
                workspace: Some(workspace.keep()),
                logs: None,
                reaped: false,
                group_signal_attempted: false,
                group_error: None,
                #[cfg(test)]
                before_reap: None,
                #[cfg(test)]
                group_signal_attempts: 0,
            }));
            self.owners
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(owner.clone());
            let (commands, receiver) = mpsc::channel(1);
            let (started, startup) = oneshot::channel();
            let (terminated, termination) = watch::channel(None);
            // There is no suspension between spawn and transferring ownership to the actor.
            tokio::spawn(
                supervise(
                    owner.clone(),
                    self.owners.clone(),
                    stdin,
                    stdout,
                    stderr,
                    self.config.clone(),
                    control,
                    receiver,
                    started,
                    terminated,
                )
                .with_current_subscriber(),
            );
            let session = Box::new(Session {
                pid,
                commands: Some(commands),
                termination,
                owner,
                owners: self.owners.clone(),
                cleanup: None,
            });
            finish_startup(session, startup).await
        })
    }
}

struct Session {
    pid: u32,
    commands: Option<mpsc::Sender<SessionCommand>>,
    termination: watch::Receiver<Option<Result<()>>>,
    owner: SharedResources,
    owners: ResourceRegistry,
    cleanup: Option<Result<()>>,
}

async fn finish_startup(
    session: Box<Session>,
    startup: oneshot::Receiver<Result<()>>,
) -> Result<StartOutcome> {
    match startup.await {
        Ok(Ok(())) => Ok(StartOutcome::Ready(session)),
        Ok(Err(error)) => {
            let confirmed = matches!(*session.termination.borrow(), Some(Ok(())));
            if confirmed {
                Err(error)
            } else {
                Ok(StartOutcome::CleanupRequired { error, session })
            }
        }
        Err(_) => Ok(StartOutcome::CleanupRequired {
            error: Error::new(
                ErrorKind::Runtime,
                "subprocess startup supervisor stopped before confirming ownership cleanup",
            ),
            session,
        }),
    }
}

enum SessionCommand {
    Invoke {
        invocation: RuntimeInvocation,
        control: RunControl,
        handler: Option<Arc<dyn RuntimeRequestHandler>>,
        reply: oneshot::Sender<Result<ProgramOutcome>>,
    },
    Close,
}

impl ExecutionSession for Session {
    fn pid(&self) -> u32 {
        self.pid
    }
    fn execute<'a>(
        &'a mut self,
        invocation: RuntimeInvocation,
        control: RunControl,
    ) -> PortFuture<'a, ProgramOutcome> {
        self.dispatch(invocation, control, None)
    }
    fn execute_with_requests<'a>(
        &'a mut self,
        invocation: RuntimeInvocation,
        control: RunControl,
        handler: Arc<dyn RuntimeRequestHandler>,
    ) -> PortFuture<'a, ProgramOutcome> {
        self.dispatch(invocation, control, Some(handler))
    }
    fn close(&mut self) -> PortFuture<'_, ()> {
        Box::pin(async move {
            if matches!(self.cleanup, Some(Ok(()))) {
                return Ok(());
            }
            if let Some(sender) = self.commands.take() {
                let _ = sender.send(SessionCommand::Close).await;
            }
            let observed = loop {
                if let Some(result) = self.termination.borrow().clone() {
                    break result;
                }
                if self.termination.changed().await.is_err() {
                    break Err(retired());
                }
            };
            // The shared record also survives a supervisor panic. A cancelled
            // close future merely unlocks it; later close calls can finish cleanup.
            let result = if observed.is_err() {
                let mut resources = self.owner.lock().await;
                let result = terminate(&mut resources).await;
                release_confirmed(&self.owner, &self.owners, &resources, &result);
                result
            } else {
                observed
            };
            self.cleanup = Some(result.clone());
            result
        })
    }
}

impl Session {
    fn dispatch<'a>(
        &'a mut self,
        invocation: RuntimeInvocation,
        control: RunControl,
        handler: Option<Arc<dyn RuntimeRequestHandler>>,
    ) -> PortFuture<'a, ProgramOutcome> {
        Box::pin(async move {
            control.check()?;
            let sender = self.commands.as_ref().ok_or_else(retired)?;
            let (reply, response) = oneshot::channel();
            sender
                .send(SessionCommand::Invoke {
                    invocation,
                    control,
                    handler,
                    reply,
                })
                .await
                .map_err(|_| retired())?;
            response.await.map_err(|_| retired())?
        })
    }
}

fn retired() -> Error {
    Error::new(
        ErrorKind::Runtime,
        "subprocess session is retired; invocation was not retried",
    )
}
fn protocol(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Protocol, message)
}

#[allow(clippy::too_many_arguments)]
async fn supervise(
    owner: SharedResources,
    owners: ResourceRegistry,
    mut stdin: ChildStdin,
    stdout: ChildStdout,
    stderr: ChildStderr,
    config: SubprocessConfig,
    control: RunControl,
    mut commands: mpsc::Receiver<SessionCommand>,
    mut started: oneshot::Sender<Result<()>>,
    terminated: watch::Sender<Option<Result<()>>>,
) {
    let mut resources = owner.lock().await;
    let pid = resources.child.id().expect("actor owns a new child");
    let budget = Arc::new(AtomicUsize::new(config.max_log_bytes));
    resources.logs = Some(tokio::spawn(
        drain_logs(
            stderr,
            budget.clone(),
            pid,
            resources
                .artifact
                .as_ref()
                .expect("owned artifact")
                .digest()
                .0
                .clone(),
        )
        .with_current_subscriber(),
    ));
    let artifact = resources.artifact.as_ref().expect("owned artifact");
    let version = artifact.manifest().runtime.protocol;
    let mut logs = LogForwarder::new(pid, artifact.digest().0.clone(), budget.clone());
    let mut frames = FrameReader::new(stdout, config.max_frame_bytes);
    let startup = guarded(
        async {
            let frame: Ready = serde_json::from_value(frames.next().await?)
                .map_err(|e| protocol(format!("invalid ready frame: {e}")))?;
            if frame.v != version
                || frame.kind != "ready"
                || frame.pid != pid
                || frame.python_version
                    != resources
                        .artifact
                        .as_ref()
                        .expect("owned artifact")
                        .manifest()
                        .runtime
                        .python
            {
                return Err(protocol(
                    "Python ready frame did not match runtime, protocol, or PID",
                ));
            }
            Ok(())
        },
        &control,
        &mut started,
        Some(config.startup_timeout),
    )
    .await;
    if let Err(error) = startup {
        let cleanup = terminate(&mut resources).await;
        release_confirmed(&owner, &owners, &resources, &cleanup);
        let _ = terminated.send(Some(cleanup));
        let _ = started.send(Err(error));
        return;
    }
    if started.send(Ok(())).is_err() {
        let cleanup = terminate(&mut resources).await;
        release_confirmed(&owner, &owners, &resources, &cleanup);
        let _ = terminated.send(Some(cleanup));
        return;
    }

    loop {
        // Read unexpected output/EOF while idle, without reaping first: the owned
        // child's PID cannot be reused before process-group cleanup is requested.
        let command = tokio::select! {
            biased;
            command = commands.recv() => command,
            unsolicited = frames.next() => {
                if let Ok(frame) = &unsolicited
                    && logs.accept(frame, version) {
                        // Always yield, including an idle flood from user background work.
                        tokio::task::yield_now().await;
                        continue;
                    }
                tracing::warn!(subprocess_pid = pid, error = ?unsolicited, "subprocess exited or sent unsolicited protocol data");
                None
            }
        };
        match command {
            Some(SessionCommand::Invoke {
                invocation,
                control,
                handler,
                mut reply,
            }) => {
                budget.store(config.max_log_bytes, Ordering::Release);
                let result = guarded(
                    invoke(
                        &mut stdin,
                        &mut frames,
                        &invocation,
                        version,
                        &mut logs,
                        config.max_frame_bytes,
                        RequestDispatch {
                            handler: handler.as_deref(),
                            control: &control,
                        },
                    ),
                    &control,
                    &mut reply,
                    None,
                )
                .await;
                if result.is_err() {
                    let cleanup = terminate(&mut resources).await;
                    release_confirmed(&owner, &owners, &resources, &cleanup);
                    let _ = terminated.send(Some(cleanup));
                    let _ = reply.send(result);
                    return;
                }
                if reply.send(result).is_err() {
                    let cleanup = terminate(&mut resources).await;
                    release_confirmed(&owner, &owners, &resources, &cleanup);
                    let _ = terminated.send(Some(cleanup));
                    return;
                }
            }
            Some(SessionCommand::Close) => {
                // The closing acknowledgement leaves Python alive, waiting on
                // stdin. Keep stdin owned until group signaling and reaping are
                // complete, avoiding Darwin's zombie-only group signaling race.
                let _ = tokio::time::timeout(config.shutdown_timeout, async {
                    write_frame(
                        &mut stdin,
                        &json!({"v": version, "type": "shutdown"}),
                        config.max_frame_bytes,
                    )
                    .await?;
                    let closing = next_control(&mut frames, version, &mut logs).await?;
                    if closing != json!({"v": version, "type": "closing"}) {
                        return Err(protocol("invalid shutdown response"));
                    }
                    Ok(())
                })
                .await;
                let cleanup = terminate(&mut resources).await;
                release_confirmed(&owner, &owners, &resources, &cleanup);
                let _ = terminated.send(Some(cleanup));
                return;
            }
            None => {
                let cleanup = terminate(&mut resources).await;
                release_confirmed(&owner, &owners, &resources, &cleanup);
                let _ = terminated.send(Some(cleanup));
                return;
            }
        }
    }
}

async fn guarded<T, F: Future<Output = Result<T>>>(
    operation: F,
    control: &RunControl,
    reply: &mut oneshot::Sender<Result<T>>,
    local_timeout: Option<Duration>,
) -> Result<T> {
    control.check()?;
    let deadline = local_timeout
        .map(|timeout| (Instant::now() + timeout).min(control.deadline()))
        .unwrap_or(control.deadline());
    let caller_dropped = || {
        Error::new(
            ErrorKind::Cancelled,
            "caller dropped subprocess operation; execution may have started",
        )
    };
    tokio::select! {
        biased;
        _ = reply.closed() => Err(caller_dropped()),
        error = stop_signal(control, deadline) => Err(error),
        result = operation => {
            // A synchronous poll (including final frame decoding) can cross a
            // deadline or cancellation without allowing the stop branch to run.
            // Reject that completion before the actor permits session reuse.
            if reply.is_closed() {
                return Err(caller_dropped());
            }
            check_stop(control, deadline)?;
            result
        },
    }
}

fn check_stop(control: &RunControl, deadline: Instant) -> Result<()> {
    control.check()?;
    if Instant::now() >= deadline {
        return Err(Error::new(
            ErrorKind::TimedOut,
            "subprocess startup deadline expired",
        ));
    }
    Ok(())
}

async fn stop_signal(control: &RunControl, deadline: Instant) -> Error {
    loop {
        if let Err(error) = check_stop(control, deadline) {
            return error;
        }
        tokio::time::sleep(
            Duration::from_millis(10).min(deadline.saturating_duration_since(Instant::now())),
        )
        .await;
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Ready {
    v: u32,
    #[serde(rename = "type")]
    kind: String,
    pid: u32,
    python_version: String,
}

struct RequestDispatch<'a> {
    handler: Option<&'a dyn RuntimeRequestHandler>,
    control: &'a RunControl,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestFrame {
    v: u32,
    #[serde(rename = "type")]
    kind: String,
    event_id: String,
    attempt_id: String,
    id: u64,
    operation: String,
    payload: Value,
}

async fn invoke(
    stdin: &mut ChildStdin,
    frames: &mut FrameReader,
    invocation: &RuntimeInvocation,
    version: u32,
    logs: &mut LogForwarder,
    limit: usize,
    dispatch: RequestDispatch<'_>,
) -> Result<ProgramOutcome> {
    if (dispatch.handler.is_some() || invocation.extension.is_some()) && version != 3 {
        return Err(Error::new(
            ErrorKind::Incompatible,
            "interactive execution requires runtime protocol 3",
        ));
    }
    if invocation.extension.is_some() && dispatch.handler.is_none() {
        return Err(Error::new(
            ErrorKind::Incompatible,
            "runtime extension requires interactive execution",
        ));
    }
    if let Some(extension) = &invocation.extension {
        extension.validate()?;
    }
    let event = &invocation.event;
    let mut request = json!({"v": version, "type": "invoke", "event_id": event.id(), "attempt_id": event.attempt_id(), "event": event.value()});
    if version >= 2 {
        if let Some(context) = &invocation.processing_context {
            context.validate()?;
        }
        request["processing_context"] = serde_json::to_value(&invocation.processing_context)
            .map_err(|error| protocol(format!("cannot encode processing context: {error}")))?;
    }
    if let Some(extension) = &invocation.extension {
        request["extension"] = serde_json::to_value(extension)
            .map_err(|error| protocol(format!("cannot encode runtime extension: {error}")))?;
    }
    write_frame(stdin, &request, limit).await?;
    let mut expected_id = 1_u64;
    let response = loop {
        let response = next_control(frames, version, logs).await?;
        if response.get("type").and_then(Value::as_str) != Some("runtime_request") {
            break response;
        }
        let handler = dispatch
            .handler
            .filter(|_| version == 3)
            .ok_or_else(|| protocol("runtime request outside interactive execution"))?;
        let frame: RequestFrame = serde_json::from_value(response)
            .map_err(|error| protocol(format!("invalid runtime request: {error}")))?;
        if frame.v != version
            || frame.kind != "runtime_request"
            || frame.event_id != event.id()
            || frame.attempt_id != event.attempt_id()
            || frame.id != expected_id
        {
            return Err(protocol(
                "runtime request protocol, invocation identity, or sequence mismatch",
            ));
        }
        let request = RuntimeRequest {
            id: frame.id,
            operation: frame.operation,
            payload: frame.payload,
        };
        request
            .validate()
            .map_err(|error| protocol(format!("invalid runtime request: {error}")))?;
        dispatch.control.check()?;
        // Exactly one decoded request and callback are held at a time. The
        // surrounding guard cancels this await and retires the process on a
        // deadline or lost caller; no late acknowledgement reaches warm reuse.
        let reply = handler.handle(request, dispatch.control.clone()).await?;
        dispatch.control.check()?;
        if reply.id != expected_id {
            return Err(protocol("runtime reply ID does not match its request"));
        }
        validate_runtime_payload(&reply.result, RUNTIME_REQUEST_MAX_BYTES)
            .map_err(|error| protocol(format!("invalid runtime reply: {error}")))?;
        write_frame(
            stdin,
            &json!({ "v": version, "type": "runtime_reply",
            "event_id": event.id(), "attempt_id": event.attempt_id(),
            "id": reply.id, "result": reply.result }),
            limit,
        )
        .await?;
        expected_id = expected_id
            .checked_add(1)
            .ok_or_else(|| protocol("runtime request sequence exhausted"))?;
        tokio::task::yield_now().await;
    };
    if response.get("v").and_then(Value::as_u64) != Some(u64::from(version))
        || response.get("type").and_then(Value::as_str) != Some("result")
        || response.get("event_id").and_then(Value::as_str) != Some(event.id())
        || response.get("attempt_id").and_then(Value::as_str) != Some(event.attempt_id())
    {
        return Err(protocol("result protocol or invocation identity mismatch"));
    }
    match response.get("status").and_then(Value::as_str) {
        Some("success") => {
            let output = response
                .get("output")
                .cloned()
                .ok_or_else(|| protocol("success result lacks output"))?;
            if dispatch.handler.is_some() {
                validate_runtime_payload(&output, limit)?;
            } else {
                validate_wire_value(&output)
                    .map_err(|error| protocol(format!("invalid program output: {error}")))?;
            }
            Ok(ProgramOutcome::Success { output })
        }
        Some("error") => {
            let error = response
                .get("error")
                .ok_or_else(|| protocol("error result lacks error"))?;
            let kind = error
                .get("kind")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| protocol("invalid program error kind"))?;
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .ok_or_else(|| protocol("invalid program error message"))?;
            Ok(ProgramOutcome::Failure {
                kind: kind.to_owned(),
                message: message.to_owned(),
            })
        }
        Some("runtime_error") if version == 3 && dispatch.handler.is_some() => {
            let error = response
                .get("error")
                .ok_or_else(|| protocol("runtime error result lacks error"))?;
            let kind = error
                .get("kind")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| protocol("runtime error result lacks kind"))?;
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .ok_or_else(|| protocol("runtime error result lacks message"))?;
            Err(Error::new(ErrorKind::Runtime, format!("{kind}: {message}")))
        }
        _ => Err(protocol("unknown program result status")),
    }
}

async fn next_control(
    frames: &mut FrameReader,
    version: u32,
    logs: &mut LogForwarder,
) -> Result<Value> {
    let mut consecutive = 0;
    loop {
        let frame = frames.next().await?;
        if !logs.accept(&frame, version) {
            return Ok(frame);
        }
        consecutive += 1;
        if consecutive == 16 {
            // An always-readable telemetry pipe must not starve cancellation,
            // deadlines, lease renewal, or other consumer tasks.
            tokio::task::yield_now().await;
            consecutive = 0;
        }
    }
}

async fn write_frame(stdin: &mut ChildStdin, value: &Value, limit: usize) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)
        .map_err(|e| protocol(format!("cannot encode protocol frame: {e}")))?;
    if bytes.len() >= limit {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "invocation exceeds subprocess frame limit",
        ));
    }
    bytes.push(b'\n');
    stdin.write_all(&bytes).await.map_err(|e| {
        Error::new(
            ErrorKind::Runtime,
            format!("subprocess dispatch failed; execution may have started: {e}"),
        )
    })?;
    stdin.flush().await?;
    Ok(())
}

struct FrameReader {
    reader: BufReader<ChildStdout>,
    pending: Vec<u8>,
    limit: usize,
}
impl FrameReader {
    fn new(stdout: ChildStdout, limit: usize) -> Self {
        Self {
            reader: BufReader::new(stdout),
            pending: Vec::new(),
            limit,
        }
    }
    async fn next(&mut self) -> Result<Value> {
        loop {
            let buffer = self.reader.fill_buf().await?;
            if buffer.is_empty() {
                return Err(Error::new(
                    ErrorKind::Runtime,
                    "subprocess closed protocol output; execution result is unavailable",
                ));
            }
            let newline = buffer.iter().position(|b| *b == b'\n');
            let used = newline.map_or(buffer.len(), |i| i + 1);
            if used > self.limit.saturating_sub(self.pending.len()) {
                return Err(protocol("subprocess frame exceeded configured limit"));
            }
            self.pending.extend_from_slice(&buffer[..used]);
            self.reader.consume(used);
            if newline.is_some() {
                let parsed = ledgence_worker_api::decode_json(&self.pending)
                    .map_err(|e| protocol(format!("invalid subprocess JSON frame: {e}")));
                self.pending.clear();
                return parsed;
            }
        }
    }
}

async fn drain_logs(mut stderr: ChildStderr, budget: Arc<AtomicUsize>, pid: u32, digest: String) {
    let mut buffer = [0u8; 8192];
    loop {
        let count = match stderr.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(count) => count,
        };
        let allowance = budget
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                Some(remaining.saturating_sub(count))
            })
            .unwrap_or(0);
        let emitted = allowance.min(count);
        if emitted > 0 {
            tracing::info!(target: "ledgence::program_log", subprocess_pid = pid, artifact_digest = %digest, stream = "stderr", text = %String::from_utf8_lossy(&buffer[..emitted]), "program log");
        }
        if allowance > 0 && count > allowance {
            tracing::warn!(target: "ledgence::program_log", subprocess_pid = pid, artifact_digest = %digest, truncated = true, "program log allowance exhausted; continuing to drain");
        }
    }
}

async fn terminate(resources: &mut Resources) -> Result<()> {
    // Signal the owned group BEFORE waiting/reaping. Never deliver a signal
    // after wait() releases the child's PID to the operating system.
    if !resources.reaped && !resources.group_signal_attempted {
        #[cfg(test)]
        {
            resources.group_signal_attempts += 1;
        }
        #[cfg(unix)]
        let group_error = resources.child.id().and_then(|pid| {
            use nix::{
                errno::Errno,
                sys::signal::{Signal, killpg},
                unistd::Pid,
            };
            let pid = i32::try_from(pid).ok().filter(|pid| *pid > 1)?;
            let group_id = Pid::from_raw(pid);
            match killpg(group_id, Signal::SIGKILL) {
                Ok(()) | Err(Errno::ESRCH) => None,
                Err(error) => Some(GroupTerminationError {
                    error: Error::new(
                        ErrorKind::Runtime,
                        format!("cannot terminate subprocess group: {error}"),
                    ),
                    group_id,
                }),
            }
        });
        #[cfg(not(unix))]
        let group_error: Option<GroupTerminationError> = None;
        resources.group_error = group_error;
        resources.group_signal_attempted = true;
        let _ = resources.child.start_kill();
    }
    if !resources.reaped {
        #[cfg(test)]
        if let Some(gate) = resources.before_reap.as_ref() {
            gate.acquire()
                .await
                .expect("test reap gate remains open")
                .forget();
            resources.before_reap = None;
        }
        resources.child.wait().await.map_err(Error::from)?;
        resources.reaped = true;
    }
    // Darwin can reject the initial killpg with EPERM when the group contains
    // only an unreaped zombie. Confirm that the group is absent after reaping;
    // neither EPERM nor a successful existence check confirms cleanup. Signal 0
    // is read-only, including if this numeric PGID has since been reused.
    #[cfg(unix)]
    if resources.group_error.as_ref().is_some_and(|failure| {
        matches!(
            nix::sys::signal::killpg(failure.group_id, None),
            Err(nix::errno::Errno::ESRCH)
        )
    }) {
        resources.group_error = None;
    }
    // Escaped descendants can retain stderr. They must not hold the actor forever.
    if let Some(logs) = resources.logs.as_mut()
        && tokio::time::timeout(Duration::from_millis(200), &mut *logs)
            .await
            .is_err()
    {
        logs.abort();
        let _ = logs.await;
    }
    resources.logs = None;
    if let Some(failure) = &resources.group_error {
        return Err(failure.error.clone());
    }
    if let Some(path) = resources.workspace.as_ref() {
        std::fs::remove_dir_all(path)
            .or_else(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(error)
                }
            })
            .map_err(|error| {
                Error::new(
                    ErrorKind::Io,
                    format!(
                        "cannot remove subprocess working directory {}: {error}",
                        path.display()
                    ),
                )
            })?;
    }
    resources.workspace = None;
    resources.artifact = None;
    Ok(())
}

fn release_confirmed(
    owner: &SharedResources,
    owners: &ResourceRegistry,
    resources: &Resources,
    result: &Result<()>,
) {
    if result.is_ok() {
        owners
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|candidate| !Arc::ptr_eq(candidate, owner));
    } else {
        tracing::error!(error = ?result.as_ref().err(), working_directory = ?resources.workspace, "retaining child, artifact, and working-directory ownership until cleanup is confirmed");
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use ledgence_worker_api::{Digest, Platform, ProgramManifest, ProgramRef, PythonRuntime};

    fn cleanup_resources(child: Child) -> (tempfile::TempDir, Resources, std::sync::Weak<()>) {
        let artifact_root = tempfile::tempdir().unwrap();
        let pin = Arc::new(());
        let weak = Arc::downgrade(&pin);
        let artifact = PreparedArtifact::new(
            artifact_root.path().to_owned(),
            ProgramManifest {
                schema_version: 1,
                program: ProgramRef {
                    id: "cleanup".into(),
                    version: "v1".into(),
                },
                runtime: PythonRuntime {
                    kind: "python".into(),
                    python: "3.12".into(),
                    protocol: 1,
                },
                handler: "program:handle".into(),
                platform: Platform {
                    os: std::env::consts::OS.into(),
                    arch: std::env::consts::ARCH.into(),
                },
            },
            Digest(format!("sha256:{}", "0".repeat(64))),
            pin,
        );
        let resources = Resources {
            child,
            artifact: Some(artifact),
            workspace: Some(tempfile::tempdir().unwrap().keep()),
            logs: None,
            reaped: false,
            group_signal_attempted: false,
            group_error: None,
            before_reap: None,
            group_signal_attempts: 0,
        };
        (artifact_root, resources, weak)
    }

    #[tokio::test]
    async fn spontaneously_exited_child_is_confirmed_after_interrupted_reaping() {
        let child = Command::new("/bin/sh")
            .args(["-c", "exit 23"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap();
        // Observe exit without wait/try_wait, so the group still contains the
        // unreaped child when the first cleanup signal is attempted.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let status = Command::new("ps")
                    .args(["-o", "stat=", "-p", &pid.to_string()])
                    .output()
                    .await
                    .unwrap();
                if String::from_utf8_lossy(&status.stdout)
                    .trim()
                    .starts_with('Z')
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("owned child should exit without being reaped");
        let (_artifact_root, mut resources, pin) = cleanup_resources(child);
        let workspace = resources.workspace.clone().unwrap();
        let before_reap = Arc::new(tokio::sync::Semaphore::new(0));
        resources.before_reap = Some(before_reap.clone());
        let first_poll = {
            let mut cleanup = Box::pin(terminate(&mut resources));
            std::future::poll_fn(|cx| std::task::Poll::Ready(cleanup.as_mut().poll(cx))).await
        };
        assert!(first_poll.is_pending());
        assert!(!resources.reaped);
        assert_eq!(resources.group_signal_attempts, 1);
        #[cfg(target_os = "macos")]
        if let Some(failure) = resources.group_error.as_ref() {
            assert_eq!(
                failure.error.message,
                "cannot terminate subprocess group: EPERM: Operation not permitted"
            );
        }
        assert!(pin.upgrade().is_some());
        assert!(workspace.exists());

        before_reap.add_permits(1);
        terminate(&mut resources).await.unwrap();
        terminate(&mut resources).await.unwrap();
        assert!(resources.reaped);
        assert!(resources.group_error.is_none());
        assert_eq!(resources.group_signal_attempts, 1);
        assert!(pin.upgrade().is_none());
        assert!(!workspace.exists());
        assert_eq!(
            nix::sys::signal::killpg(nix::unistd::Pid::from_raw(pid as i32), None),
            Err(nix::errno::Errno::ESRCH)
        );
    }

    #[tokio::test]
    async fn failed_group_signal_retains_ownership_until_remaining_group_disappears() {
        let child = Command::new("/bin/sleep")
            .arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let group_id = nix::unistd::Pid::from_raw(child.id().unwrap() as i32);
        let mut member = Command::new("/bin/sleep")
            .arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(group_id.as_raw())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let (_artifact_root, mut resources, pin) = cleanup_resources(child);
        let workspace = resources.workspace.clone().unwrap();
        // Model a failed group signal followed by the direct-child fallback.
        // A permission failure must not become success merely because the
        // direct child is reaped while another group member remains alive.
        let error = Error::new(ErrorKind::Runtime, "group signal permission denied");
        resources.group_signal_attempted = true;
        resources.group_signal_attempts = 1;
        resources.group_error = Some(GroupTerminationError {
            error: error.clone(),
            group_id,
        });
        resources.child.start_kill().unwrap();
        for _ in 0..2 {
            assert_eq!(terminate(&mut resources).await.unwrap_err(), error);
            assert!(resources.reaped);
            assert_eq!(resources.group_signal_attempts, 1);
            assert!(member.try_wait().unwrap().is_none());
            assert!(pin.upgrade().is_some());
            assert!(workspace.exists());
        }
        // The test still owns this child handle. Only its owner may kill/reap
        // it; runtime retries must restrict themselves to read-only checks.
        member.kill().await.unwrap();
        terminate(&mut resources).await.unwrap();
        assert!(pin.upgrade().is_none());
        assert!(!workspace.exists());
        assert!(resources.group_error.is_none());
        assert_eq!(resources.group_signal_attempts, 1);
    }

    #[tokio::test]
    async fn lost_startup_channel_returns_real_cleanup_ownership() {
        let artifact_root = tempfile::tempdir().unwrap();
        let pin = Arc::new(());
        let weak = Arc::downgrade(&pin);
        let artifact = PreparedArtifact::new(
            artifact_root.path().to_owned(),
            ProgramManifest {
                schema_version: 1,
                program: ProgramRef {
                    id: "startup".into(),
                    version: "v1".into(),
                },
                runtime: PythonRuntime {
                    kind: "python".into(),
                    python: "3.12".into(),
                    protocol: 1,
                },
                handler: "program:handle".into(),
                platform: Platform {
                    os: std::env::consts::OS.into(),
                    arch: std::env::consts::ARCH.into(),
                },
            },
            Digest(format!("sha256:{}", "0".repeat(64))),
            pin,
        );
        let workspace = tempfile::tempdir().unwrap().keep();
        let child = Command::new("/bin/sleep")
            .arg("30")
            .current_dir(&workspace)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap();
        let owner = Arc::new(Mutex::new(Resources {
            child,
            artifact: Some(artifact),
            workspace: Some(workspace.clone()),
            logs: None,
            reaped: false,
            group_signal_attempted: false,
            group_error: None,
            before_reap: None,
            group_signal_attempts: 0,
        }));
        let owners = Arc::new(StdMutex::new(vec![owner.clone()]));
        let (sender, receiver) = mpsc::channel(1);
        let (started, startup) = oneshot::channel();
        let (terminated, termination) = watch::channel(None);
        // Simulate a lost supervisor before it reports startup or cleanup.
        drop(receiver);
        drop(started);
        drop(terminated);
        let session = Box::new(Session {
            pid,
            commands: Some(sender),
            termination,
            owner,
            owners: owners.clone(),
            cleanup: None,
        });
        let mut session = match finish_startup(session, startup).await {
            Ok(StartOutcome::CleanupRequired { session, .. }) => session,
            _ => panic!("lost startup channel must preserve cleanup ownership"),
        };
        assert!(weak.upgrade().is_some());
        assert!(workspace.exists());
        assert!(nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None).is_ok());
        session.close().await.unwrap();
        assert!(owners.lock().unwrap().is_empty());
        assert!(weak.upgrade().is_none());
        assert!(!workspace.exists());
        assert!(nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None).is_err());
    }

    #[tokio::test]
    async fn cancelled_fallback_close_preserves_signal_progress_until_reaping_finishes() {
        let artifact_root = tempfile::tempdir().unwrap();
        let pin = Arc::new(());
        let weak = Arc::downgrade(&pin);
        let artifact = PreparedArtifact::new(
            artifact_root.path().to_owned(),
            ProgramManifest {
                schema_version: 1,
                program: ProgramRef {
                    id: "cleanup".into(),
                    version: "v1".into(),
                },
                runtime: PythonRuntime {
                    kind: "python".into(),
                    python: "3.12".into(),
                    protocol: 1,
                },
                handler: "program:handle".into(),
                platform: Platform {
                    os: std::env::consts::OS.into(),
                    arch: std::env::consts::ARCH.into(),
                },
            },
            Digest(format!("sha256:{}", "0".repeat(64))),
            pin,
        );
        let workspace = tempfile::tempdir().unwrap().keep();
        let child = Command::new("/bin/sleep")
            .arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let pid = child.id().unwrap();
        let before_reap = Arc::new(tokio::sync::Semaphore::new(0));
        let owner = Arc::new(Mutex::new(Resources {
            child,
            artifact: Some(artifact),
            workspace: Some(workspace.clone()),
            logs: None,
            reaped: false,
            group_signal_attempted: false,
            group_error: None,
            before_reap: Some(before_reap.clone()),
            group_signal_attempts: 0,
        }));
        let owners = Arc::new(StdMutex::new(vec![owner.clone()]));
        let (_terminated, termination) = watch::channel(Some(Err(retired())));
        let mut session = Session {
            pid,
            commands: None,
            termination,
            owner: owner.clone(),
            owners: owners.clone(),
            cleanup: None,
        };
        // Poll the actual fallback close through group signaling, then cancel at
        // an exact pre-reap suspension instead of relying on scheduler timing.
        let first_poll = {
            let mut close = session.close();
            std::future::poll_fn(|cx| std::task::Poll::Ready(close.as_mut().poll(cx))).await
        };
        assert!(first_poll.is_pending());
        {
            let resources = owner.lock().await;
            assert!(resources.group_signal_attempted);
            assert!(resources.group_error.is_none());
            assert!(!resources.reaped);
            assert_eq!(resources.group_signal_attempts, 1);
        }
        assert!(weak.upgrade().is_some());
        assert!(workspace.exists());
        // The killed child may now be a zombie. Darwin can reject a second
        // killpg with EPERM; the retry must preserve the first successful result.
        tokio::time::sleep(Duration::from_millis(100)).await;
        before_reap.add_permits(1);
        session.close().await.unwrap();
        session.close().await.unwrap();
        {
            let resources = owner.lock().await;
            assert!(resources.reaped);
            assert!(resources.group_error.is_none());
            assert_eq!(resources.group_signal_attempts, 1);
        }
        assert!(owners.lock().unwrap().is_empty());
        assert!(weak.upgrade().is_none());
        assert!(!workspace.exists());
        assert!(nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None).is_err());
    }
}
