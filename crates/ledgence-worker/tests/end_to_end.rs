use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::Output,
    time::Duration,
};
use tempfile::TempDir;

fn python() -> String {
    std::env::var("LEDGENCE_PYTHON").unwrap_or_else(|_| "python3".into())
}

async fn invoke(args: &[&str]) -> Output {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_ledgence-worker"));
    command
        .args(args)
        .env("RUST_LOG", "warn")
        .kill_on_drop(true);
    tokio::time::timeout(Duration::from_secs(30), command.output())
        .await
        .expect("CLI command must complete within its deadline")
        .unwrap()
}

fn success(output: &Output) {
    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn write_json(path: impl AsRef<Path>, value: &Value) {
    std::fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

struct Fixture {
    _temp: TempDir,
    example: PathBuf,
    store: PathBuf,
    cache: PathBuf,
    runner: PathBuf,
}
impl Fixture {
    async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let example = temp.path().join("example");
        success(
            &invoke(&[
                "example",
                "--directory",
                example.to_str().unwrap(),
                "--python",
                &python(),
            ])
            .await,
        );
        let store = temp.path().join("store");
        let cache = temp.path().join("cache");
        let runner = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../sdk/python/ledgence_worker/bootstrap.py");
        Self {
            _temp: temp,
            example,
            store,
            cache,
            runner,
        }
    }
    async fn publish(&self) -> Output {
        invoke(&[
            "publish",
            "--source",
            self.example.join("program").to_str().unwrap(),
            "--store",
            self.store.to_str().unwrap(),
        ])
        .await
    }
    async fn run(&self) -> Output {
        self.run_with_timeout("30000").await
    }
    async fn run_with_timeout(&self, timeout: &str) -> Output {
        invoke(&[
            "run",
            "--tasks",
            self.example.join("tasks.json").to_str().unwrap(),
            "--store",
            self.store.to_str().unwrap(),
            "--cache",
            self.cache.to_str().unwrap(),
            "--python",
            &python(),
            "--runner",
            self.runner.to_str().unwrap(),
            "--concurrency",
            "1",
            "--timeout-ms",
            timeout,
        ])
        .await
    }
}

#[tokio::test]
async fn published_dependencies_are_downloaded_cached_and_reused_with_immutable_versions() {
    let fixture = Fixture::new().await;
    let program = fixture.example.join("program");
    std::fs::create_dir(program.join("bundled_dependency")).unwrap();
    std::fs::write(
        program.join("bundled_dependency/__init__.py"),
        "VERSION = 'first'\n",
    )
    .unwrap();
    std::fs::write(program.join("program.py"), "import os\nfrom bundled_dependency import VERSION\ndef handle(event):\n    return {'event': event, 'version': VERSION, 'pid': os.getpid()}\n").unwrap();
    let first = fixture.publish().await;
    success(&first);
    let first_descriptor: Value = serde_json::from_slice(&first.stdout).unwrap();
    let repeated = fixture.publish().await;
    success(&repeated);
    assert_eq!(
        first_descriptor,
        serde_json::from_slice::<Value>(&repeated.stdout).unwrap()
    );
    // A version cannot be silently overwritten with new bytes.
    std::fs::write(
        program.join("bundled_dependency/__init__.py"),
        "VERSION = 'second'\n",
    )
    .unwrap();
    assert!(!fixture.publish().await.status.success());
    let manifest_path = program.join("ledgence-program.json");
    let mut manifest: Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    manifest["program"]["version"] = "2.0.0".into();
    write_json(manifest_path, &manifest);
    let second = fixture.publish().await;
    success(&second);
    let second_descriptor: Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_ne!(first_descriptor["digest"], second_descriptor["digest"]);
    let tasks_path = fixture.example.join("tasks.json");
    let mut tasks: Value = serde_json::from_slice(&std::fs::read(&tasks_path).unwrap()).unwrap();
    let mut third = tasks[0].clone();
    third["program"]["version"] = "2.0.0".into();
    third["event"]["id"] = "evt_third".into();
    third["event"]["ldgtaskid"] = "task_third".into();
    third["event"]["ldgattemptid"] = "att_third".into();
    third["event"]["data"] = json!({"business": [null, true, {"anything": "user-owned"}]});
    tasks.as_array_mut().unwrap().push(third);
    write_json(tasks_path, &tasks);
    let result = fixture.run().await;
    success(&result);
    let reports: Vec<Value> = String::from_utf8(result.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap()["report"].clone())
        .collect();
    assert_eq!(reports.len(), 3);
    for (index, report) in reports.iter().enumerate() {
        assert_eq!(report["outcome"]["status"], "success");
        assert_eq!(report["outcome"]["output"]["event"], tasks[index]["event"]);
        assert_eq!(report["outcome"]["output"]["pid"], report["process_id"]);
        assert_eq!(
            report["outcome"]["output"]["version"],
            if index < 2 { "first" } else { "second" }
        );
        assert_eq!(
            &report["digest"],
            if index < 2 {
                &first_descriptor["digest"]
            } else {
                &second_descriptor["digest"]
            }
        );
    }
    assert_eq!(reports[0]["process_id"], reports[1]["process_id"]);
    assert_eq!(reports[1]["reused_process"], true);
    assert_ne!(reports[1]["process_id"], reports[2]["process_id"]);
    // Reopen the persistent cache in a new worker; task-to-descriptor binding and
    // package verification still apply, while the old process is never reused.
    std::fs::remove_dir_all(fixture.store.join("blobs")).unwrap();
    success(&fixture.run().await);
}

#[tokio::test]
async fn duplicate_attempts_and_changed_task_bindings_are_rejected_before_dispatch() {
    let fixture = Fixture::new().await;
    success(&fixture.publish().await);
    let tasks_path = fixture.example.join("tasks.json");
    let mut tasks: Value = serde_json::from_slice(&std::fs::read(&tasks_path).unwrap()).unwrap();
    tasks[1]["event"]["ldgattemptid"] = tasks[0]["event"]["ldgattemptid"].clone();
    write_json(&tasks_path, &tasks);
    let duplicate = fixture.run().await;
    assert!(!duplicate.status.success());
    assert!(
        duplicate.stdout.is_empty(),
        "no invocation may start before fixture validation completes"
    );
    assert!(String::from_utf8_lossy(&duplicate.stderr).contains("duplicate attempt"));
    let manifest_path = fixture.example.join("program/ledgence-program.json");
    let mut manifest: Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    manifest["program"]["version"] = "2.0.0".into();
    write_json(manifest_path, &manifest);
    success(&fixture.publish().await);
    tasks[1]["event"]["ldgattemptid"] = "att_different".into();
    tasks[1]["event"]["ldgtaskid"] = tasks[0]["event"]["ldgtaskid"].clone();
    tasks[1]["program"]["version"] = "2.0.0".into();
    write_json(tasks_path, &tasks);
    let changed = fixture.run().await;
    assert!(!changed.status.success());
    assert!(changed.stdout.is_empty());
    assert!(String::from_utf8_lossy(&changed.stderr).contains("bound program"));
}

#[tokio::test]
async fn bundled_executable_and_empty_namespace_survive_publication_and_cache_reopen() {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new().await;
    let program = fixture.example.join("program");
    std::fs::create_dir(program.join("empty_namespace")).unwrap();
    std::fs::write(
        program.join("helper"),
        "#!/bin/sh\nprintf packaged-helper\n",
    )
    .unwrap();
    std::fs::set_permissions(
        program.join("helper"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    std::fs::write(program.join("program.py"), "import pathlib, subprocess\nimport empty_namespace\ndef handle(event):\n    return subprocess.check_output([str(pathlib.Path(__file__).parent / 'helper')], text=True)\n").unwrap();
    success(&fixture.publish().await);
    for _ in 0..2 {
        let output = fixture.run().await;
        success(&output);
        for line in String::from_utf8(output.stdout).unwrap().lines() {
            let report: Value = serde_json::from_str(line).unwrap();
            assert_eq!(report["report"]["outcome"]["output"], "packaged-helper");
        }
    }
}

#[tokio::test]
async fn failures_keep_full_scope_program_and_trace_in_json_and_logs() {
    let fixture = Fixture::new().await;
    std::fs::write(
        fixture.example.join("program/program.py"),
        "import time\ndef handle(event):\n    time.sleep(120)\n",
    )
    .unwrap();
    let published = fixture.publish().await;
    success(&published);
    let descriptor: Value = serde_json::from_slice(&published.stdout).unwrap();
    let path = fixture.example.join("tasks.json");
    let original: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let mut tasks = json!([original[0].clone(), original[0].clone()]);
    tasks[0]["event"]["tracestate"] = "test=first".into();
    tasks[1]["event"]["source"] = "urn:other:source".into();
    tasks[1]["event"]["ldgtenantid"] = "tenant_other".into();
    tasks[1]["event"]["ldgnamespace"] = "other".into();
    tasks[1]["event"]["ldgrunid"] = "run_other".into();
    tasks[1]["event"]["traceparent"] =
        "00-11111111111111111111111111111111-2222222222222222-01".into();
    tasks[1]["event"]["tracestate"] = "test=second".into();
    write_json(path, &tasks);
    let output = fixture.run_with_timeout("1000").await;
    assert!(!output.status.success());
    let reports: Vec<Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap()["failure"].clone())
        .collect();
    assert_eq!(reports.len(), 2);
    for (index, failure) in reports.iter().enumerate() {
        for (report_key, event_key) in [
            ("source", "source"),
            ("event_id", "id"),
            ("tenant_id", "ldgtenantid"),
            ("namespace", "ldgnamespace"),
            ("run_id", "ldgrunid"),
            ("task_id", "ldgtaskid"),
            ("attempt_id", "ldgattemptid"),
            ("attempt_no", "ldgattemptno"),
            ("traceparent", "traceparent"),
            ("tracestate", "tracestate"),
        ] {
            assert_eq!(
                failure[report_key], tasks[index]["event"][event_key],
                "missing {report_key}: {failure}"
            );
        }
        assert_eq!(failure["program"], tasks[index]["program"]);
        assert_eq!(failure["digest"], descriptor["digest"]);
    }
    let logs = String::from_utf8(output.stderr).unwrap();
    assert!(
        logs.contains("tenant_other") && logs.contains("run_other") && logs.contains("test=second"),
        "failure logs lost scope: {logs}"
    );
}

#[cfg(unix)]
struct ProcessCleanup(Option<PathBuf>);
#[cfg(unix)]
impl Drop for ProcessCleanup {
    fn drop(&mut self) {
        if let Some(path) = &self.0
            && let Ok(bytes) = std::fs::read(path)
            && let Ok(pids) = serde_json::from_slice::<Vec<i32>>(&bytes)
        {
            let _ = nix::sys::signal::killpg(
                nix::unistd::Pid::from_raw(pids[0]),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }
}

#[cfg(unix)]
async fn signal_stops_owned_processes(signal: nix::sys::signal::Signal, during_start: bool) {
    let fixture = Fixture::new().await;
    let marker = fixture._temp.path().join("owned-processes.json");
    let mut cleanup = ProcessCleanup(Some(marker.clone()));
    let start = format!(
        "import os, time, json\ndef block():\n    child = os.fork()\n    if child == 0:\n        time.sleep(120)\n        os._exit(0)\n    with open({}, 'w') as file:\n        json.dump([os.getpid(), child], file)\n    time.sleep(120)\n",
        serde_json::to_string(marker.to_str().unwrap()).unwrap()
    );
    let source = if during_start {
        format!("{start}\nblock()\ndef handle(event):\n    return None\n")
    } else {
        format!("{start}\ndef handle(event):\n    block()\n")
    };
    std::fs::write(fixture.example.join("program/program.py"), source).unwrap();
    success(&fixture.publish().await);
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_ledgence-worker"));
    command
        .args(["run", "--tasks"])
        .arg(fixture.example.join("tasks.json"))
        .arg("--store")
        .arg(&fixture.store)
        .arg("--cache")
        .arg(&fixture.cache)
        .arg("--python")
        .arg(python())
        .arg("--runner")
        .arg(&fixture.runner)
        .args(["--concurrency", "1", "--timeout-ms", "120000"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let child = command.spawn().unwrap();
    let cli_pid = child.id().unwrap();
    let pids = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Ok(bytes) = std::fs::read(&marker)
                && let Ok(pids) = serde_json::from_slice::<Vec<i32>>(&bytes)
            {
                break pids;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Python must reach the intended lifecycle phase");
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(cli_pid as i32), signal).unwrap();
    let output = tokio::time::timeout(Duration::from_secs(8), child.wait_with_output())
        .await
        .expect("one shutdown signal must complete owned cleanup")
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("execution interrupted"),
        "CLI must handle the signal: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    for pid in pids {
        let state = tokio::process::Command::new("ps")
            .args(["-p", &pid.to_string(), "-o", "stat="])
            .output()
            .await
            .unwrap();
        let state = String::from_utf8_lossy(&state.stdout);
        assert!(
            state.trim().is_empty() || state.trim().starts_with('Z'),
            "owned process {pid} remains running: {state}"
        );
    }
    cleanup.0 = None;
    let reports = String::from_utf8(output.stdout).unwrap();
    assert!(
        reports.lines().count() <= 1,
        "shutdown must stop admitting the queued second task"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn sigterm_cleans_active_program_and_descendant() {
    signal_stops_owned_processes(nix::sys::signal::Signal::SIGTERM, false).await;
}
#[cfg(unix)]
#[tokio::test]
async fn sigterm_cleans_starting_program_and_descendant() {
    signal_stops_owned_processes(nix::sys::signal::Signal::SIGTERM, true).await;
}
#[cfg(unix)]
#[tokio::test]
async fn sigint_cleans_active_program_and_descendant() {
    signal_stops_owned_processes(nix::sys::signal::Signal::SIGINT, false).await;
}
#[cfg(unix)]
#[tokio::test]
async fn sigint_cleans_starting_program_and_descendant() {
    signal_stops_owned_processes(nix::sys::signal::Signal::SIGINT, true).await;
}

#[cfg(unix)]
#[tokio::test]
async fn second_shutdown_signal_forces_exit_while_fetch_is_retained() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let fixture = Fixture::new().await;
    let published = fixture.publish().await;
    success(&published);
    let descriptor = published.stdout;
    let size = serde_json::from_slice::<Value>(&descriptor).unwrap()["size"]
        .as_u64()
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (started, wait_started) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut started = Some(started);
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut first = String::new();
            stream.read_line(&mut first).await.unwrap();
            loop {
                let mut line = String::new();
                stream.read_line(&mut line).await.unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
            }
            let mut stream = stream.into_inner();
            if first.contains("descriptor.json") {
                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            descriptor.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
                stream.write_all(&descriptor).await.unwrap();
            } else {
                stream
                    .write_all(
                        format!("HTTP/1.1 200 OK\r\nContent-Length: {size}\r\n\r\n").as_bytes(),
                    )
                    .await
                    .unwrap();
                started.take().unwrap().send(()).unwrap();
                std::future::pending::<()>().await;
            }
        }
    });
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_ledgence-worker"));
    command
        .args(["run", "--tasks"])
        .arg(fixture.example.join("tasks.json"))
        .args(["--store", &url])
        .arg("--cache")
        .arg(&fixture.cache)
        .arg("--python")
        .arg(python())
        .arg("--runner")
        .arg(&fixture.runner)
        .args(["--concurrency", "1"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().unwrap();
    let pid = nix::unistd::Pid::from_raw(child.id().unwrap() as i32);
    tokio::time::timeout(Duration::from_secs(8), wait_started)
        .await
        .expect("must reach an admitted fetch")
        .unwrap();
    nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM).unwrap();
    // Give the first delivery time to be observed; standard Unix signals may coalesce.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        child.try_wait().unwrap().is_none(),
        "first signal must retain pending fetch ownership"
    );
    nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGINT).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output()).await;
    server.abort();
    let _ = server.await;
    let output = result
        .expect("a further shutdown signal must force exit")
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("forced exit requested"));
}

#[cfg(unix)]
#[tokio::test]
async fn fifo_task_fixture_is_rejected_without_blocking_the_cli() {
    use nix::{sys::stat::Mode, unistd::mkfifo};
    let fixture = Fixture::new().await;
    success(&fixture.publish().await);
    let tasks = fixture.example.join("tasks.json");
    std::fs::remove_file(&tasks).unwrap();
    mkfifo(&tasks, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
    let output = tokio::time::timeout(Duration::from_secs(5), fixture.run())
        .await
        .expect("a FIFO task fixture must be rejected promptly");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("regular file"));
}

#[cfg(unix)]
#[tokio::test]
async fn first_shutdown_signal_waits_for_preparation_then_skips_dispatch() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let fixture = Fixture::new().await;
    let published = fixture.publish().await;
    success(&published);
    let descriptor = published.stdout;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (started, wait_started) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        let mut first = String::new();
        stream.read_line(&mut first).await.unwrap();
        assert!(first.contains("descriptor.json"));
        loop {
            let mut line = String::new();
            stream.read_line(&mut line).await.unwrap();
            if line == "\r\n" || line.is_empty() {
                break;
            }
        }
        started.send(()).unwrap();
        released.await.unwrap();
        let mut stream = stream.into_inner();
        stream
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    descriptor.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        stream.write_all(&descriptor).await.unwrap();
    });
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_ledgence-worker"));
    command
        .args(["run", "--tasks"])
        .arg(fixture.example.join("tasks.json"))
        .args(["--store", &url])
        .arg("--cache")
        .arg(&fixture.cache)
        .arg("--python")
        .arg(python())
        .arg("--runner")
        .arg(&fixture.runner)
        .args(["--concurrency", "1"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().unwrap();
    tokio::time::timeout(Duration::from_secs(8), wait_started)
        .await
        .expect("must reach descriptor resolution")
        .unwrap();
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(child.id().unwrap() as i32),
        nix::sys::signal::Signal::SIGTERM,
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        child.try_wait().unwrap().is_none(),
        "preparation must retain owned I/O after cancellation"
    );
    release.send(()).unwrap();
    let output = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output())
        .await
        .expect("shutdown must finish after resolution ends")
        .unwrap();
    server.await.unwrap();
    assert!(!output.status.success());
    assert!(
        output.stdout.is_empty(),
        "interrupted preflight must not dispatch tasks"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("execution interrupted during preparation")
    );
}

#[tokio::test]
async fn integer_input_outside_wire_range_is_rejected_before_execution() {
    let fixture = Fixture::new().await;
    success(&fixture.publish().await);
    let path = fixture.example.join("tasks.json");
    let mut tasks: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    tasks[0]["event"]["data"] = "REPLACE_WITH_INTEGER".into();
    let encoded = serde_json::to_string(&tasks)
        .unwrap()
        .replace("\"REPLACE_WITH_INTEGER\"", "18446744073709551616");
    std::fs::write(path, encoded).unwrap();
    let output = fixture.run().await;
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("64-bit range"));
}
