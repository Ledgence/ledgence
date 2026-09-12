//! Composition root and local execution fixture for the first worker milestone.

use ledgence_adapter_artifact::{
    ArtifactLimits, FileArtifactCache, FileProgramStore, HttpProgramStore, publish_directory,
};
use ledgence_adapter_subprocess::SubprocessRuntime;
use ledgence_worker_api::*;
use ledgence_worker_core::{ExecutionRequest, Worker, WorkerConfig};
use serde::{Deserialize, Serialize};
use serde_json::json;
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

const HELP: &str = "Ledgence worker foundation\n\nCommands:\n  example --directory DIR --python EXE\n  publish --source DIR --store DIR\n  run --tasks FILE --store DIR_OR_URL --cache DIR --python EXE --runner BOOTSTRAP [--concurrency N] [--timeout-ms MS]\n\nThe run command consumes a local JSON task fixture. A production orchestration\ntransport, distributed leases, and durable settlement are not implemented yet.\n";

#[tokio::main]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .json()
        .init();
    match dispatch().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn dispatch() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let Some(command) = args.next() else {
        print!("{HELP}");
        return Ok(());
    };
    if ["--help", "-h", "help"].contains(&command.as_str()) {
        print!("{HELP}");
        return Ok(());
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
        "example" => {
            let directory = required(&mut options, "--directory")?;
            let python = required(&mut options, "--python")?;
            check_empty(options)?;
            make_example(Path::new(&directory), &python).await
        }
        "publish" => {
            let source = required(&mut options, "--source")?;
            let store = required(&mut options, "--store")?;
            check_empty(options)?;
            let descriptor = publish_directory(source, store, &ArtifactLimits::default())?;
            println!(
                "{}",
                serde_json::to_string(&descriptor).map_err(|e| input(e.to_string()))?
            );
            Ok(())
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
            run(config).await
        }
        _ => Err(input(format!("unknown command {command}; use --help"))),
    }
}

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

async fn run(config: RunOptions) -> Result<()> {
    let limits = ArtifactLimits::default();
    let store: Arc<dyn ProgramStore> =
        if config.store.starts_with("http://") || config.store.starts_with("https://") {
            Arc::new(HttpProgramStore::new(&config.store, limits.clone())?)
        } else {
            Arc::new(FileProgramStore::new(&config.store, limits.clone())?)
        };
    let cache = Arc::new(FileArtifactCache::new(config.cache, limits)?);
    let runtime = Arc::new(SubprocessRuntime::new(config.python, config.runner));
    let tasks: Vec<SubmittedTask> =
        serde_json::from_slice(&read_bounded(&config.tasks, 8 * 1024 * 1024)?)
            .map_err(|e| input(format!("invalid task fixture: {e}")))?;
    if tasks.len() > 1000 {
        return Err(input("task fixture is limited to 1000 invocations"));
    }
    let mut attempts = HashSet::new();
    let mut releases = HashMap::<ProgramRef, ProgramDescriptor>::new();
    let mut bindings = HashMap::new();
    let mut assignments = VecDeque::new();
    // This controlled adapter resolves and binds descriptors before dispatch.
    // Real orchestration must persist this binding at logical-task scope.
    for task in tasks {
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
    let worker = Worker::new(
        WorkerConfig {
            concurrency: config.concurrency,
            ..WorkerConfig::default()
        },
        store,
        cache,
        runtime,
    )?;
    let queue = Arc::new(Mutex::new(assignments));
    let stop = Arc::new(AtomicBool::new(false));
    let failures = Arc::new(AtomicUsize::new(0));
    let mut consumers = JoinSet::new();
    for _ in 0..config.concurrency {
        let worker = worker.clone();
        let queue = queue.clone();
        let stop = stop.clone();
        let failures = failures.clone();
        consumers.spawn(async move {
            while !stop.load(Ordering::Acquire) {
                let Some(request) = queue.lock().await.pop_front() else {
                    break;
                };
                let event_id = request.event.id().to_owned();
                let attempt_id = request.event.attempt_id().to_owned();
                let result = worker
                    .execute(
                        request,
                        RunControl::new(Duration::from_millis(config.timeout_ms)),
                    )
                    .await;
                let output = match result {
                    Ok(report) => {
                        if matches!(report.outcome, ProgramOutcome::Failure { .. }) {
                            failures.fetch_add(1, Ordering::Relaxed);
                        }
                        json!({"report": report})
                    }
                    Err(failure) => {
                        failures.fetch_add(1, Ordering::Relaxed);
                        json!({"event_id":event_id,"attempt_id":attempt_id,"failure":failure})
                    }
                };
                println!("{output}");
            }
        });
    }
    let mut interrupted = false;
    loop {
        tokio::select! {
            joined = consumers.join_next() => {
                match joined {
                    None => break,
                    Some(Ok(())) => {},
                    Some(Err(error)) => {
                        tracing::error!(%error, "consumer failed");
                        failures.fetch_add(1, Ordering::Relaxed);
                        stop.store(true, Ordering::Release);
                    }
                }
            }
            signal = tokio::signal::ctrl_c() => {
                if let Err(error) = signal { tracing::error!(%error, "cannot receive interrupt signal"); }
                if interrupted { return Err(forced_exit()); }
                interrupted = true;
                stop.store(true, Ordering::Release);
                tokio::select! {
                    result = worker.shutdown(Duration::ZERO, Duration::from_secs(35)) => {
                        if let Err(error) = result {
                            tracing::error!(%error, "initial shutdown incomplete; retaining worker for final cleanup");
                        }
                    }
                    signal = tokio::signal::ctrl_c() => {
                        signal.map_err(Error::from)?;
                        return Err(forced_exit());
                    }
                }
            }
        }
    }
    finish_shutdown(&worker, interrupted).await?;
    let failed = failures.load(Ordering::Relaxed);
    if interrupted {
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
async fn finish_shutdown(worker: &Worker, interrupted: bool) -> Result<()> {
    let mut last_error = None;
    loop {
        let cleanup = tokio::select! {
            result = worker.shutdown(Duration::ZERO, Duration::from_secs(5)) => result,
            signal = tokio::signal::ctrl_c(), if interrupted || last_error.is_some() => {
                signal.map_err(Error::from)?;
                return Err(forced_exit());
            }
        };
        match cleanup {
            Ok(()) => return Ok(()),
            Err(error) => {
                let message = error.to_string();
                if last_error.as_ref() != Some(&message) {
                    tracing::error!(%error, "shutdown incomplete; retaining process ownership and retrying; Ctrl-C forces an exit with cleanup unresolved");
                    last_error = Some(message);
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {},
                    signal = tokio::signal::ctrl_c() => {
                        signal.map_err(Error::from)?;
                        return Err(forced_exit());
                    }
                }
            }
        }
    }
}

fn forced_exit() -> Error {
    Error::new(
        ErrorKind::Runtime,
        "forced exit requested with process cleanup unresolved",
    )
}

async fn make_example(directory: &Path, python: &str) -> Result<()> {
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
            protocol: 1,
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
    println!(
        "{}",
        json!({"program":program,"tasks":directory.join("tasks.json"),"python":manifest.runtime.python})
    );
    Ok(())
}

fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
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
