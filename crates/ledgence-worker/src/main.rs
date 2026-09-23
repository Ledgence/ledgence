//! Composition root for local fixture execution and connected worker delivery.

mod composition;
mod connect;
mod output;
mod signals;
mod telemetry;

use ledgence_adapter_artifact::{ArtifactLimits, publish_directory};
use ledgence_worker_api::*;
use ledgence_worker_core::{ExecutionRequest, Worker};
use output::{Outputs, Sink};
use serde::{Deserialize, Serialize};
use serde_json::json;
use signals::{ShutdownSignals, forced_exit};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Mutex, task::JoinSet};

const HELP: &str = "Ledgence worker\n\nCommands:\n  example --directory DIR --python EXE\n  publish --source DIR --store DIR\n  run --tasks FILE --store DIR_OR_URL --cache DIR --python EXE --runner BOOTSTRAP [--concurrency N] [--timeout-ms MS]\n  connect --server URL --tenant ID --namespace ID --queue NAME --store DIR_OR_URL --cache DIR --python EXE --runner BOOTSTRAP [--concurrency N] [--acquire-wait-ms MS] [--delivery-config FILE]\n\nrun consumes a local JSON task fixture. connect acquires tasks through HTTP,\nrenews leases, and reconciles durable results. One concurrency setting controls\nconsumers and the reusable process pool. The first shutdown signal drains;\na second signal forces exit with unresolved work.\n";

fn main() -> std::process::ExitCode {
    let mut outputs = match Outputs::new() {
        Ok(outputs) => outputs,
        // No output service exists if descriptor setup failed. Exit without a
        // fallback blocking write to the same unavailable destination.
        Err(_) => return std::process::ExitCode::FAILURE,
    };
    let telemetry = match telemetry::Telemetry::start("ledgence-worker", outputs.stderr.clone()) {
        Ok(telemetry) => telemetry,
        Err(error) => {
            // Existing nonblocking log sink; bounded startup diagnostic.
            use std::io::Write;
            use tracing_subscriber::fmt::MakeWriter;
            let _ = writeln!(
                outputs.stderr.make_writer(),
                "{}",
                json!({"level":"ERROR", "message":error})
            );
            if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                runtime.block_on(async {
                    let _ = tokio::time::timeout(Duration::from_secs(1), outputs.finish()).await;
                });
            }
            return std::process::ExitCode::FAILURE;
        }
    };
    let trace = telemetry.bridge();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            return std::process::ExitCode::FAILURE;
        }
    };
    let result = runtime.block_on(async {
        let mut signals = ShutdownSignals::new()?;
        let mut interrupted = false;
        let result = dispatch(&outputs.stdout, &mut signals, &mut interrupted, trace).await;
        if signals::FORCE_EXIT.load(Ordering::Acquire) {
            return result;
        }
        let stderr = outputs.stderr.clone();
        let diagnostic = result.as_ref().err().map(ToString::to_string);
        let finalization = async {
            telemetry.finish().await;
            if let Some(message) = diagnostic {
                let _ = stderr
                    .line(json!({"level":"ERROR", "message":message}).to_string())
                    .await;
            }
            outputs.finish().await
        };
        tokio::pin!(finalization);
        // Signal subscriptions outlive dispatch, including its final report
        // and log drain. An output stall must never disable forced shutdown.
        let output_result = loop {
            tokio::select! {
                biased;
                signal = signals.recv() => {
                    signal?;
                    if interrupted { return Err(forced_exit()); }
                    interrupted = true;
                }
                finished = &mut finalization => break finished,
            }
        };
        result?;
        output_result?;
        if interrupted {
            return Err(Error::new(ErrorKind::Cancelled, "execution interrupted"));
        }
        Ok(())
    });
    if signals::FORCE_EXIT.load(Ordering::Acquire) {
        runtime.block_on(async {
            if let Err(error) = &result {
                let _ = tokio::time::timeout(
                    Duration::from_millis(100),
                    outputs
                        .stderr
                        .line(json!({"level":"ERROR", "message":error.to_string()}).to_string()),
                )
                .await;
            }
            outputs.abort();
            let _ = tokio::time::timeout(Duration::from_millis(100), outputs.finish()).await;
        });
    }
    drop(outputs);
    if signals::FORCE_EXIT.load(Ordering::Acquire) {
        // Only a second explicit shutdown signal permits abandoning unresolved
        // blocking work. Normal exits retain the runtime until ownership clears.
        runtime.shutdown_timeout(Duration::ZERO);
    } else {
        drop(runtime);
    }
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(_) => std::process::ExitCode::FAILURE,
    }
}

async fn dispatch(
    output: &Sink,
    signals: &mut ShutdownSignals,
    interrupted: &mut bool,
    trace: Arc<dyn TraceBridge>,
) -> Result<()> {
    let mut args = std::env::args().skip(1);
    let Some(command) = args.next() else {
        return output.write(HELP.as_bytes().to_vec()).await;
    };
    if ["--help", "-h", "help"].contains(&command.as_str()) {
        return output.write(HELP.as_bytes().to_vec()).await;
    }
    let mut options = HashMap::new();
    while let Some(key) = args.next() {
        if !key.starts_with("--") {
            return Err(input("expected a --name value option"));
        }
        let value = args
            .next()
            .ok_or_else(|| input(format!("missing value for {key}")))?;
        if options.insert(key.clone(), value).is_some() {
            return Err(input(format!("duplicate option {key}")));
        }
    }
    match command.as_str() {
        "connect" => {
            let config = connect::ConnectOptions::parse(options)?;
            connect::run(config, output, signals, interrupted, trace).await
        }
        "example" => {
            let directory = required(&mut options, "--directory")?;
            let python = required(&mut options, "--python")?;
            check_empty(options)?;
            make_example(Path::new(&directory), &python, output).await
        }
        "publish" => {
            let source = required(&mut options, "--source")?;
            let store = required(&mut options, "--store")?;
            check_empty(options)?;
            let descriptor = publish_directory(source, store, &ArtifactLimits::default())?;
            output
                .line(serde_json::to_string(&descriptor).map_err(|e| input(e.to_string()))?)
                .await
        }
        "run" => {
            let config = RunOptions {
                tasks: required(&mut options, "--tasks")?.into(),
                store: required(&mut options, "--store")?,
                cache: required(&mut options, "--cache")?.into(),
                python: required(&mut options, "--python")?.into(),
                runner: required(&mut options, "--runner")?.into(),
                concurrency: number(options.remove("--concurrency"), 4, "concurrency")?,
                timeout_ms: number(options.remove("--timeout-ms"), 30_000, "timeout-ms")?,
            };
            check_empty(options)?;
            if config.concurrency > 1024 || config.timeout_ms > 86_400_000 {
                return Err(input(
                    "concurrency must be at most 1024 and timeout at most one day",
                ));
            }
            run(config, output, signals, interrupted, trace).await
        }
        _ => Err(input(format!("unknown command {command}; use --help"))),
    }
}

#[derive(Clone)]
struct RunOptions {
    tasks: PathBuf,
    store: String,
    cache: PathBuf,
    python: PathBuf,
    runner: PathBuf,
    concurrency: usize,
    timeout_ms: u64,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmittedTask {
    program: ProgramRef,
    event: CloudEvent,
}

async fn prepare(
    config: RunOptions,
    stop: Arc<AtomicBool>,
    trace: Arc<dyn TraceBridge>,
) -> Result<(Worker, VecDeque<ExecutionRequest>)> {
    let concurrency = config.concurrency;
    // Disk inspection may block; keep the signal-driving task responsive and
    // retain this operation until it completes, including after cancellation.
    let (parts, tasks) = tokio::task::spawn_blocking(move || -> Result<_> {
        let parts = composition::WorkerParts::new(
            &config.store,
            &config.cache,
            &config.python,
            &config.runner,
        )?;
        let tasks: Vec<SubmittedTask> = decode_json(&read_bounded(&config.tasks, 8 * 1024 * 1024)?)
            .map_err(|e| input(format!("invalid task fixture: {e}")))?;
        if tasks.len() > 1000 {
            return Err(input("task fixture is limited to 1000 invocations"));
        }
        Ok((parts, tasks))
    })
    .await
    .map_err(|error| Error::new(ErrorKind::Io, format!("preparation failed: {error}")))??;
    let store = parts.store.clone();
    let mut attempts = HashSet::new();
    let mut releases = HashMap::<ProgramRef, ProgramDescriptor>::new();
    let mut bindings = HashMap::new();
    let mut assignments = VecDeque::new();
    // This controlled adapter resolves and binds descriptors before dispatch.
    // Real orchestration must persist this binding at logical-task scope.
    for task in tasks {
        if stop.load(Ordering::Acquire) {
            return Err(Error::new(
                ErrorKind::Cancelled,
                "execution interrupted during preparation",
            ));
        }
        task.program.validate()?;
        let attempt_key = (
            task.event.tenant_id().to_owned(),
            task.event.namespace().to_owned(),
            task.event.attempt_id().to_owned(),
        );
        if !attempts.insert(attempt_key) {
            return Err(input("task fixture contains a duplicate attempt identity"));
        }
        let descriptor = if let Some(descriptor) = releases.get(&task.program) {
            descriptor.clone()
        } else {
            let descriptor = store.resolve(&task.program).await?;
            releases.insert(task.program.clone(), descriptor.clone());
            descriptor
        };
        let task_key = (
            task.event.tenant_id().to_owned(),
            task.event.namespace().to_owned(),
            task.event.task_id().to_owned(),
        );
        if let Some(bound) = bindings.insert(task_key, descriptor.clone())
            && bound != descriptor
        {
            return Err(input("one logical task cannot change its bound program"));
        }
        assignments.push_back(ExecutionRequest {
            descriptor,
            event: task.event,
        });
    }
    let worker = parts.worker(concurrency)?.with_trace_bridge(trace);
    Ok((worker, assignments))
}

async fn run(
    config: RunOptions,
    output: &Sink,
    signals: &mut ShutdownSignals,
    interrupted: &mut bool,
    trace: Arc<dyn TraceBridge>,
) -> Result<()> {
    // Both subscriptions are already installed before any child can start.
    let stop = Arc::new(AtomicBool::new(false));
    let mut preparation = tokio::spawn(prepare(config.clone(), stop.clone(), trace));
    let prepared = loop {
        tokio::select! {
            biased;
            signal = signals.recv() => {
                signal?;
                if *interrupted { return Err(forced_exit()); }
                *interrupted = true;
                stop.store(true, Ordering::Release);
            }
            result = &mut preparation => {
                break result.map_err(|error| Error::new(ErrorKind::Runtime, format!("preparation supervisor failed: {error}")))?;
            }
        }
    };
    if *interrupted {
        if let Ok((worker, _)) = prepared {
            finish_shutdown(&worker, signals, interrupted).await?;
        }
        return Err(Error::new(
            ErrorKind::Cancelled,
            "execution interrupted during preparation",
        ));
    }
    let (worker, assignments) = prepared?;
    let queue = Arc::new(Mutex::new(assignments));
    let failures = Arc::new(AtomicUsize::new(0));
    let mut consumers = JoinSet::new();
    for _ in 0..config.concurrency {
        let worker = worker.clone();
        let queue = queue.clone();
        let stop = stop.clone();
        let failures = failures.clone();
        let output = output.clone();
        consumers.spawn(async move {
            while !stop.load(Ordering::Acquire) {
                let Some(request) = queue.lock().await.pop_front() else {
                    break;
                };
                let result = worker
                    .execute(
                        request,
                        RunControl::new(Duration::from_millis(config.timeout_ms)),
                    )
                    .await;
                let report = match result {
                    Ok(report) => {
                        if matches!(report.outcome, ProgramOutcome::Failure { .. }) {
                            failures.fetch_add(1, Ordering::Relaxed);
                        }
                        json!({"report": report})
                    }
                    Err(failure) => {
                        failures.fetch_add(1, Ordering::Relaxed);
                        json!({"failure":failure})
                    }
                };
                if let Err(error) = output.line(report.to_string()).await {
                    stop.store(true, Ordering::Release);
                    return Err(error);
                }
            }
            Ok::<(), Error>(())
        });
    }
    loop {
        tokio::select! {
            joined = consumers.join_next() => {
                match joined {
                    None => break,
                    Some(Ok(Ok(()))) => {},
                    Some(failed) => {
                        let error = match failed {
                            Ok(Err(error)) => error.to_string(),
                            Err(error) => error.to_string(),
                            Ok(Ok(())) => unreachable!(),
                        };
                        tracing::error!(%error, "consumer failed");
                        failures.fetch_add(1, Ordering::Relaxed);
                        stop.store(true, Ordering::Release);
                        // Delivery failed after execution. Do not retry effects,
                        // and cancel siblings before waiting for their consumers.
                        finish_shutdown(&worker, signals, interrupted).await?;
                    }
                }
            }
            signal = signals.recv() => {
                signal?;
                if *interrupted { return Err(forced_exit()); }
                *interrupted = true;
                stop.store(true, Ordering::Release);
                tokio::select! {
                    result = worker.shutdown(Duration::ZERO, Duration::from_secs(35)) => {
                        if let Err(error) = result {
                            tracing::error!(%error, "initial shutdown incomplete; retaining worker for final cleanup");
                        }
                    }
                    signal = signals.recv() => {
                        signal?;
                        return Err(forced_exit());
                    }
                }
            }
        }
    }
    finish_shutdown(&worker, signals, interrupted).await?;
    let failed = failures.load(Ordering::Relaxed);
    if *interrupted {
        return Err(Error::new(ErrorKind::Cancelled, "execution interrupted"));
    }
    if failed != 0 {
        return Err(Error::new(
            ErrorKind::Runtime,
            format!("{failed} invocation(s) failed; see JSON reports"),
        ));
    }
    Ok(())
}

/// Keep the async runtime and retained process handles alive while cleanup is
/// unresolved. A further interrupt is an explicit forced exit, reported as such.
async fn finish_shutdown(
    worker: &Worker,
    signals: &mut ShutdownSignals,
    interrupted: &mut bool,
) -> Result<()> {
    let mut last_error = None;
    loop {
        let cleanup = tokio::select! {
            result = worker.shutdown(Duration::ZERO, Duration::from_secs(5)) => result,
            signal = signals.recv() => {
                signal?;
                if *interrupted { return Err(forced_exit()); }
                *interrupted = true;
                continue;
            }
        };
        match cleanup {
            Ok(()) => return Ok(()),
            Err(error) => {
                let message = error.to_string();
                if last_error.as_ref() != Some(&message) {
                    tracing::error!(%error, "shutdown incomplete; retaining process ownership and retrying; a further shutdown signal forces exit");
                    last_error = Some(message);
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {},
                    signal = signals.recv() => {
                        signal?;
                        if *interrupted { return Err(forced_exit()); }
                        *interrupted = true;
                    }
                }
            }
        }
    }
}

async fn make_example(directory: &Path, python: &str, sink: &Sink) -> Result<()> {
    if directory.exists() {
        return Err(input("example directory already exists"));
    }
    let mut command = tokio::process::Command::new(python);
    command.args(["-I", "-S", "-c", "import json,sys;print(json.dumps({'version':'%d.%d'%sys.version_info[:2],'implementation':sys.implementation.name}))"]).kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(5), command.output())
        .await
        .map_err(|_| input("Python version probe timed out"))??;
    if !output.status.success() {
        return Err(input("Python version probe failed"));
    }
    let info: serde_json::Value =
        serde_json::from_slice(&output.stdout).map_err(|e| input(e.to_string()))?;
    if info["implementation"] != "cpython" {
        return Err(input("the first runtime requires CPython"));
    }
    let manifest = ProgramManifest {
        schema_version: 1,
        program: ProgramRef {
            id: "hello".into(),
            version: "1.0.0".into(),
        },
        runtime: PythonRuntime {
            kind: "python".into(),
            python: info["version"]
                .as_str()
                .ok_or_else(|| input("invalid Python version"))?
                .into(),
            protocol: 2,
        },
        handler: "program:handle".into(),
        platform: Platform {
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
        },
    };
    manifest.validate_host()?;
    let program = directory.join("program");
    std::fs::create_dir_all(&program)?;
    std::fs::write(
        program.join("ledgence-program.json"),
        serde_json::to_vec_pretty(&manifest).map_err(|e| input(e.to_string()))?,
    )?;
    std::fs::write(
        program.join("program.py"),
        include_str!("../../../examples/python/program.py"),
    )?;
    let mut tasks = Vec::new();
    for n in 1..=2 {
        tasks.push(SubmittedTask { program:manifest.program.clone(), event:CloudEvent::new(json!({
            "specversion":"1.0","id":format!("evt_example_{n}"),"source":"urn:ledgence:example","type":"com.ledgence.task.invocation.requested.v1", "subject":format!("tasks/task_example_{n}"),"time":"2026-09-12T14:00:00Z","datacontenttype":"application/json",
            "traceparent":format!("00-0af7651916cd43dd8448eb211c80319c-{n:016x}-01"),
            "ldgtenantid":"tenant_example","ldgnamespace":"demo","ldgrunid":"run_example","ldgtaskid":format!("task_example_{n}"),"ldgattemptid":format!("att_example_{n}_1"),"ldgattemptno":1,
            "data":{"invoice_id":format!("INV-{n}"),"amount":42}
        }))? });
    }
    std::fs::write(
        directory.join("tasks.json"),
        serde_json::to_vec_pretty(&tasks).map_err(|e| input(e.to_string()))?,
    )?;
    sink.line(json!({"program":program,"tasks":directory.join("tasks.json"),"python":manifest.runtime.python}).to_string()).await
}

fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    if !std::fs::symlink_metadata(path)?.is_file() {
        return Err(input("task fixture must be a regular file"));
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use nix::fcntl::OFlag;
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags((OFlag::O_NONBLOCK | OFlag::O_NOFOLLOW).bits());
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(input("task fixture must be a regular file"));
    }
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(input("task fixture exceeds size limit"));
    }
    Ok(bytes)
}
fn required(options: &mut HashMap<String, String>, key: &str) -> Result<String> {
    options
        .remove(key)
        .ok_or_else(|| input(format!("missing {key}")))
}
fn check_empty(options: HashMap<String, String>) -> Result<()> {
    if options.is_empty() {
        Ok(())
    } else {
        Err(input(format!(
            "unknown options: {:?}",
            options.keys().collect::<Vec<_>>()
        )))
    }
}
fn number<T: std::str::FromStr + PartialEq + Default>(
    value: Option<String>,
    fallback: T,
    name: &str,
) -> Result<T> {
    let parsed = match value {
        Some(value) => value
            .parse()
            .map_err(|_| input(format!("invalid {name}")))?,
        None => fallback,
    };
    if parsed == T::default() {
        return Err(input(format!("{name} must be positive")));
    }
    Ok(parsed)
}
fn input(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::InvalidInput, message)
}
