//! Exporter backpressure must not take ownership of executable signal handling.
#![cfg(all(unix, feature = "otel"))]

use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};
use tokio::process::{Child, Command};

async fn fixture_command(args: &[&str]) {
    let output = tokio::time::timeout(
        Duration::from_secs(20),
        Command::new(env!("CARGO_BIN_EXE_ledgence-worker"))
            .args(args)
            .env_remove("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
            .env("RUST_LOG", "warn")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

struct Fixture {
    directory: tempfile::TempDir,
    cache: PathBuf,
    child: Child,
}
impl Fixture {
    async fn start(endpoint: &str) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let example = directory.path().join("example");
        let store = directory.path().join("store");
        let cache = directory.path().join("cache");
        let python = std::env::var("LEDGENCE_PYTHON").unwrap_or_else(|_| "python3".into());
        fixture_command(&[
            "example",
            "--directory",
            example.to_str().unwrap(),
            "--python",
            &python,
        ])
        .await;
        let marker = directory.path().join("program.pid");
        std::fs::write(example.join("program/program.py"), format!(
            "import os, pathlib, time\nMARKER = pathlib.Path({})\ndef handle(event):\n    MARKER.write_text(str(os.getpid()))\n    time.sleep(10)\n    return 42\n",
            serde_json::to_string(marker.to_str().unwrap()).unwrap())).unwrap();
        let tasks_path = example.join("tasks.json");
        let mut tasks: Vec<Value> =
            serde_json::from_slice(&std::fs::read(&tasks_path).unwrap()).unwrap();
        tasks.truncate(1);
        std::fs::write(&tasks_path, serde_json::to_vec(&tasks).unwrap()).unwrap();
        fixture_command(&[
            "publish",
            "--source",
            example.join("program").to_str().unwrap(),
            "--store",
            store.to_str().unwrap(),
        ])
        .await;
        let runner = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../sdk/python/ledgence_worker/bootstrap.py");
        let child = Command::new(env!("CARGO_BIN_EXE_ledgence-worker"))
            .arg("run")
            .arg("--tasks")
            .arg(tasks_path)
            .arg("--store")
            .arg(store)
            .arg("--cache")
            .arg(&cache)
            .arg("--python")
            .arg(python)
            .arg("--runner")
            .arg(runner)
            .args(["--concurrency", "1", "--timeout-ms", "30000"])
            .env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", endpoint)
            .env("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
            .env("OTEL_TRACES_SAMPLER_ARG", "1")
            .env("RUST_LOG", "info")
            .stdout(Stdio::from(
                std::fs::File::create(directory.path().join("stdout.log")).unwrap(),
            ))
            .stderr(Stdio::from(
                std::fs::File::create(directory.path().join("stderr.log")).unwrap(),
            ))
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        Self {
            directory,
            cache,
            child,
        }
    }
    fn stderr(&self) -> String {
        std::fs::read_to_string(self.directory.path().join("stderr.log")).unwrap()
    }
    fn assert_running(&mut self) {
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "worker exited early: {}",
            self.stderr()
        );
    }
    fn signal(&self, signal: Signal) {
        kill(
            Pid::from_raw(self.child.id().unwrap().try_into().unwrap()),
            signal,
        )
        .unwrap();
    }
    async fn ready_pid(&mut self) -> i32 {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                self.assert_running();
                if let Ok(pid) = std::fs::read_to_string(self.directory.path().join("program.pid"))
                    && let Ok(pid) = pid.parse()
                {
                    return pid;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("real Python handler must start")
    }
    async fn wait_for_execution_cleanup(&mut self, pid: i32) {
        let cache_lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.cache.join(".lock"))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                self.assert_running();
                // Releasing the cache owner proves the local dispatch's Worker
                // and its process pins have dropped. Only final drains remain.
                if cache_lock.try_lock().is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("business ownership must clear before exporter recovery");
        assert_eq!(
            kill(Pid::from_raw(pid), None),
            Err(nix::errno::Errno::ESRCH),
            "owned Python process must be reaped before telemetry drain finishes"
        );
        let output = std::fs::read_to_string(self.directory.path().join("stdout.log")).unwrap();
        let records: Vec<Value> = output
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            records.len(),
            1,
            "the cancelled invocation has exactly one report"
        );
        assert!(records[0].get("failure").is_some());
    }
}

async fn stalled_shutdown(force: bool) {
    let mut collector = StalledCollector::start("ledgence.program.execute").await;
    let mut fixture = Fixture::start(&collector.endpoint).await;
    let pid = fixture.ready_pid().await;
    let stopped_at = Instant::now();
    fixture.signal(Signal::SIGTERM);
    collector.wait_until_stalled().await;
    fixture.wait_for_execution_cleanup(pid).await;
    fixture.assert_running();
    if force {
        let forced_at = Instant::now();
        fixture.signal(Signal::SIGINT);
        let status = tokio::time::timeout(Duration::from_secs(1), fixture.child.wait())
            .await
            .expect("second signal must not wait for OTLP completion")
            .unwrap();
        assert!(forced_at.elapsed() < Duration::from_secs(1));
        assert_eq!(status.code(), Some(1), "{}", fixture.stderr());
        assert!(
            fixture.stderr().contains("forced exit"),
            "{}",
            fixture.stderr()
        );
    } else {
        let status = tokio::time::timeout(Duration::from_secs(4), fixture.child.wait())
            .await
            .expect("single-signal exporter drain must be bounded")
            .unwrap();
        assert!(stopped_at.elapsed() < Duration::from_secs(4));
        assert_eq!(status.code(), Some(1), "{}", fixture.stderr());
        assert!(
            !fixture.stderr().contains("forced exit"),
            "{}",
            fixture.stderr()
        );
        assert!(
            fixture.stderr().contains("execution interrupted"),
            "{}",
            fixture.stderr()
        );
    }
}

#[tokio::test]
async fn second_signal_forces_worker_exit_during_stalled_exporter_drain() {
    stalled_shutdown(true).await;
}

#[tokio::test]
async fn single_signal_finishes_worker_cleanup_and_bounded_exporter_drain() {
    stalled_shutdown(false).await;
}
/// Receives real bounded OTLP HTTP bytes. Only the batch containing the named
/// completed span is held without a response; earlier setup batches succeed.
struct StalledCollector {
    endpoint: String,
    observed: Option<tokio::sync::oneshot::Receiver<()>>,
    server: tokio::task::JoinHandle<()>,
}
impl StalledCollector {
    async fn start(span_name: &'static str) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/traces", listener.local_addr().unwrap());
        let (send, observed) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let header_end = loop {
                    if let Some(offset) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        break offset + 4;
                    }
                    let mut chunk = [0; 4096];
                    let count = socket.read(&mut chunk).await.unwrap();
                    assert_ne!(count, 0);
                    bytes.extend_from_slice(&chunk[..count]);
                    assert!(bytes.len() <= 1024 * 1024);
                };
                let headers = String::from_utf8(bytes[..header_end].to_vec())
                    .unwrap()
                    .to_ascii_lowercase();
                assert!(headers.starts_with("post /v1/traces http/1.1"));
                assert!(headers.contains("application/x-protobuf"));
                let size = headers
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("content-length:")
                            .map(|size| size.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                assert!(size <= 1024 * 1024);
                while bytes.len() < header_end + size {
                    let mut chunk = [0; 4096];
                    let count = socket.read(&mut chunk).await.unwrap();
                    assert_ne!(count, 0);
                    bytes.extend_from_slice(&chunk[..count]);
                }
                let body = &bytes[header_end..header_end + size];
                if body
                    .windows(span_name.len())
                    .any(|part| part == span_name.as_bytes())
                {
                    let _ = send.send(());
                    // Keep the actual peer open until test teardown; no network
                    // response, release signal, or collector recovery can aid exit.
                    std::future::pending::<()>().await;
                    drop(socket);
                    return;
                }
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
            }
        });
        Self {
            endpoint,
            observed: Some(observed),
            server,
        }
    }
    async fn wait_until_stalled(&mut self) {
        tokio::time::timeout(Duration::from_secs(10), self.observed.take().unwrap())
            .await
            .expect("expected completed span must reach the collector")
            .unwrap();
    }
}
impl Drop for StalledCollector {
    fn drop(&mut self) {
        self.server.abort();
    }
}
