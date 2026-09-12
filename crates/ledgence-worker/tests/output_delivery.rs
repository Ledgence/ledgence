//! Output backpressure must not own the execution scheduler or child lifecycle.
#![cfg(unix)]

use nix::{
    sys::signal::{Signal, kill, killpg},
    unistd::Pid,
};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::{ExitStatus, Output, Stdio},
    time::Duration,
};
use tempfile::TempDir;
use tokio::{
    io::AsyncReadExt,
    process::{Child, ChildStderr, ChildStdout, Command},
};

fn python() -> String {
    std::env::var("LEDGENCE_PYTHON").unwrap_or_else(|_| "python3".into())
}

async fn command_output(args: &[&str]) -> Output {
    let output = tokio::time::timeout(
        Duration::from_secs(30),
        Command::new(env!("CARGO_BIN_EXE_ledgence-worker"))
            .args(args)
            .env("RUST_LOG", "warn")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("fixture command must finish")
    .unwrap();
    assert!(
        output.status.success(),
        "fixture command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

struct Fixture {
    _temp: TempDir,
    example: PathBuf,
    store: PathBuf,
    cache: PathBuf,
    markers: PathBuf,
}

impl Fixture {
    async fn new(modes: &[&str]) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let example = temp.path().join("example");
        command_output(&[
            "example",
            "--directory",
            example.to_str().unwrap(),
            "--python",
            &python(),
        ])
        .await;
        let markers = temp.path().join("markers");
        std::fs::create_dir(&markers).unwrap();
        let source = format!(
            r#"import os, pathlib, sys, time
MARKERS = pathlib.Path({})
def handle(event):
    mode = event['data']['mode']
    name = event['data']['name']
    (MARKERS / (name + '.pid')).write_text(str(os.getpid()))
    if mode == 'large_after_sleep_started':
        while not (MARKERS / 'task_1.pid').exists():
            time.sleep(0.01)
    if mode == 'sleep':
        time.sleep(60)
    if mode == 'log_sleep':
        sys.stderr.write('x' * 200000)
        sys.stderr.flush()
        (MARKERS / 'logs_flushed').write_text('ready')
        time.sleep(60)
    return {{'task_id': event['ldgtaskid'], 'value': 'x' * 200000}}
"#,
            serde_json::to_string(markers.to_str().unwrap()).unwrap()
        );
        std::fs::write(example.join("program/program.py"), source).unwrap();
        let tasks_path = example.join("tasks.json");
        let templates: Vec<Value> =
            serde_json::from_slice(&std::fs::read(&tasks_path).unwrap()).unwrap();
        let tasks: Vec<Value> = modes
            .iter()
            .enumerate()
            .map(|(index, mode)| {
                let mut task = templates[0].clone();
                task["event"]["id"] = format!("evt_output_{index}").into();
                task["event"]["ldgtaskid"] = format!("task_output_{index}").into();
                task["event"]["ldgattemptid"] = format!("att_output_{index}").into();
                task["event"]["data"] = json!({"mode": mode, "name": format!("task_{index}")});
                task
            })
            .collect();
        std::fs::write(tasks_path, serde_json::to_vec(&tasks).unwrap()).unwrap();
        let store = temp.path().join("store");
        command_output(&[
            "publish",
            "--source",
            example.join("program").to_str().unwrap(),
            "--store",
            store.to_str().unwrap(),
        ])
        .await;
        let cache = temp.path().join("cache");
        Self {
            _temp: temp,
            example,
            store,
            cache,
            markers,
        }
    }

    fn spawn(&self, concurrency: &str, timeout_ms: &str, logs: bool) -> RunningWorker {
        let runner = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../sdk/python/ledgence_worker/bootstrap.py");
        let mut child = Command::new(env!("CARGO_BIN_EXE_ledgence-worker"))
            .args(["run", "--tasks"])
            .arg(self.example.join("tasks.json"))
            .arg("--store")
            .arg(&self.store)
            .arg("--cache")
            .arg(&self.cache)
            .arg("--python")
            .arg(python())
            .arg("--runner")
            .arg(runner)
            .args(["--concurrency", concurrency, "--timeout-ms", timeout_ms])
            .env("TOKIO_WORKER_THREADS", "1")
            .env("RUST_LOG", if logs { "info" } else { "warn" })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        RunningWorker {
            child,
            stdout,
            stderr,
            markers: self.markers.clone(),
        }
    }

    async fn pid(&self, index: usize) -> i32 {
        let path = self.markers.join(format!("task_{index}.pid"));
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Ok(text) = std::fs::read_to_string(&path)
                    && let Ok(pid) = text.parse::<i32>()
                {
                    return pid;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("program must reach its expected invocation")
    }
}

/// All intentional unread pipes stay owned here. On failure, kill only this
/// fixture's worker and the process groups its programs recorded.
struct RunningWorker {
    child: Child,
    stdout: Option<ChildStdout>,
    stderr: Option<ChildStderr>,
    markers: PathBuf,
}

impl RunningWorker {
    fn signal(&self, signal: Signal) {
        let pid = self.child.id().expect("worker must still be running");
        kill(Pid::from_raw(pid as i32), signal).unwrap();
    }

    async fn wait_without_draining(&mut self, limit: Duration) -> ExitStatus {
        tokio::time::timeout(limit, self.child.wait())
            .await
            .expect("output backpressure must not prevent bounded worker exit")
            .unwrap()
    }

    async fn read_finished_output(&mut self, status: ExitStatus) -> Output {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), async {
            if let Some(mut reader) = self.stdout.take() {
                reader.read_to_end(&mut stdout).await.unwrap();
            }
            if let Some(mut reader) = self.stderr.take() {
                reader.read_to_end(&mut stderr).await.unwrap();
            }
        })
        .await
        .expect("finished worker must release its output descriptors");
        Output {
            status,
            stdout,
            stderr,
        }
    }
}

impl Drop for RunningWorker {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        if let Ok(entries) = std::fs::read_dir(&self.markers) {
            for entry in entries.flatten() {
                if entry.path().extension().is_some_and(|ext| ext == "pid")
                    && let Ok(text) = std::fs::read_to_string(entry.path())
                    && let Ok(pid) = text.parse::<i32>()
                {
                    let _ = killpg(Pid::from_raw(pid), Signal::SIGKILL);
                }
            }
        }
    }
}

async fn assert_stopped(pid: i32, limit: Duration) {
    tokio::time::timeout(limit, async {
        loop {
            let output = Command::new("ps")
                .args(["-p", &pid.to_string(), "-o", "stat="])
                .kill_on_drop(true)
                .output()
                .await
                .unwrap();
            let state = String::from_utf8_lossy(&output.stdout);
            if state.trim().is_empty() || state.trim().starts_with('Z') {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("owned process {pid} must stop despite output backpressure"));
}

#[tokio::test]
async fn unread_stdout_preserves_another_invocations_deadline_and_cleanup() {
    let fixture = Fixture::new(&["large_after_sleep_started", "sleep"]).await;
    let mut worker = fixture.spawn("2", "1500", false);
    let first_pid = fixture.pid(0).await;
    let sleeping_pid = fixture.pid(1).await;
    assert_stopped(sleeping_pid, Duration::from_secs(4)).await;
    assert!(
        worker.child.try_wait().unwrap().is_none(),
        "invocation deadline must progress before the blocked output delivery deadline"
    );
    let status = worker.wait_without_draining(Duration::from_secs(8)).await;
    assert!(
        !status.success(),
        "undelivered reports must fail the command"
    );
    assert_stopped(first_pid, Duration::from_secs(2)).await;
    let output = worker.read_finished_output(status).await;
    assert!(
        output.stdout.len() < 200000,
        "the result pipe must have stayed unread and blocked"
    );
}

#[tokio::test]
async fn unread_stderr_does_not_block_program_drain_or_first_signal_cleanup() {
    let fixture = Fixture::new(&["log_sleep"]).await;
    let mut worker = fixture.spawn("1", "60000", true);
    let pid = fixture.pid(0).await;
    tokio::time::timeout(Duration::from_secs(4), async {
        while !fixture.markers.join("logs_flushed").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("program stderr must keep draining when the CLI log sink stalls");
    worker.signal(Signal::SIGTERM);
    assert_stopped(pid, Duration::from_secs(3)).await;
    let status = worker.wait_without_draining(Duration::from_secs(8)).await;
    assert!(!status.success());
    let output = worker.read_finished_output(status).await;
    assert!(
        !output.stderr.is_empty(),
        "the stalled log pipe must contain logs"
    );
}

#[tokio::test]
async fn second_signal_exits_promptly_while_report_output_is_blocked() {
    let fixture = Fixture::new(&["large_after_sleep_started", "sleep"]).await;
    let mut worker = fixture.spawn("2", "60000", false);
    let first_pid = fixture.pid(0).await;
    let sleeping_pid = fixture.pid(1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    worker.signal(Signal::SIGTERM);
    assert_stopped(sleeping_pid, Duration::from_secs(3)).await;
    assert_stopped(first_pid, Duration::from_secs(2)).await;
    assert!(worker.child.try_wait().unwrap().is_none());
    // Different signal kinds avoid Unix coalescing of repeated signals.
    worker.signal(Signal::SIGINT);
    let status = worker.wait_without_draining(Duration::from_secs(2)).await;
    assert!(!status.success());
    let output = worker.read_finished_output(status).await;
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("forced exit"),
        "second-signal output loss must be explicit: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn resumed_stdout_delivers_complete_reports_exactly_once() {
    let fixture = Fixture::new(&["large", "large"]).await;
    let mut worker = fixture.spawn("1", "10000", false);
    let pid = fixture.pid(0).await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(worker.child.try_wait().unwrap().is_none());
    let mut stdout_reader = worker.stdout.take().unwrap();
    let mut stderr_reader = worker.stderr.take().unwrap();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let status = tokio::time::timeout(Duration::from_secs(8), async {
        let (status, out, err) = tokio::join!(
            worker.child.wait(),
            stdout_reader.read_to_end(&mut stdout),
            stderr_reader.read_to_end(&mut stderr),
        );
        out.unwrap();
        err.unwrap();
        status.unwrap()
    })
    .await
    .expect("resuming the report reader must allow successful output delivery");
    assert!(status.success(), "{}", String::from_utf8_lossy(&stderr));
    let text = String::from_utf8(stdout).unwrap();
    assert!(text.ends_with('\n'));
    let reports: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).expect("each report must be one complete JSON line"))
        .collect();
    assert_eq!(reports.len(), 2, "reports must be emitted exactly once");
    for (index, record) in reports.iter().enumerate() {
        let report = &record["report"];
        assert_eq!(report["task_id"], format!("task_output_{index}"));
        assert_eq!(report["outcome"]["status"], "success");
        assert_eq!(
            report["outcome"]["output"]["task_id"],
            format!("task_output_{index}")
        );
        assert_eq!(report["outcome"]["output"]["value"], "x".repeat(200000));
        assert_eq!(report["process_id"], pid);
    }
    assert_eq!(reports[1]["report"]["reused_process"], true);
    assert_stopped(pid, Duration::from_secs(2)).await;
}

#[tokio::test]
async fn closed_stdout_reader_fails_and_cleans_running_sibling() {
    let fixture = Fixture::new(&["large_after_sleep_started", "sleep"]).await;
    let mut worker = fixture.spawn("2", "60000", false);
    drop(worker.stdout.take());
    let first_pid = fixture.pid(0).await;
    let sleeping_pid = fixture.pid(1).await;
    let status = worker.wait_without_draining(Duration::from_secs(8)).await;
    assert!(
        !status.success(),
        "closed report destination must fail the command"
    );
    assert_stopped(first_pid, Duration::from_secs(2)).await;
    assert_stopped(sleeping_pid, Duration::from_secs(2)).await;
    let output = worker.read_finished_output(status).await;
    assert!(
        !output.stderr.is_empty(),
        "report-delivery failure must be visible when stderr works"
    );
}
