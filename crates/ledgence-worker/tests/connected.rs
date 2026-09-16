#![cfg(unix)]

use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use serde_json::{Value, json};
use std::{collections::HashSet, path::Path, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::Child,
    sync::oneshot,
    task::JoinSet,
};

type Request = (String, Value);

async fn read_request(socket: &mut TcpStream) -> Request {
    read_request_or_closed(socket)
        .await
        .expect("complete fixture request")
}

async fn read_request_or_closed(socket: &mut TcpStream) -> Option<Request> {
    let mut bytes = Vec::new();
    let end = loop {
        if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break index + 4;
        }
        let mut buffer = [0; 4096];
        let count = match socket.read(&mut buffer).await {
            Ok(0) => return None,
            Ok(count) => count,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
                ) =>
            {
                return None;
            }
            Err(error) => panic!("fixture request read failed: {error}"),
        };
        bytes.extend_from_slice(&buffer[..count]);
    };
    let header = String::from_utf8(bytes[..end].to_vec()).unwrap();
    let size = header
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    while bytes.len() < end + size {
        let mut buffer = [0; 4096];
        let count = match socket.read(&mut buffer).await {
            Ok(0) => return None,
            Ok(count) => count,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
                ) =>
            {
                return None;
            }
            Err(error) => panic!("fixture request read failed: {error}"),
        };
        bytes.extend_from_slice(&buffer[..count]);
    }
    Some((
        header.lines().next().unwrap().into(),
        serde_json::from_slice(&bytes[end..end + size]).unwrap(),
    ))
}

async fn next_request(
    listener: &TcpListener,
    readers: &mut JoinSet<(TcpStream, Option<Request>)>,
) -> (TcpStream, Request) {
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (mut socket, _) = accepted.unwrap();
                readers.spawn(async move {
                    let request = read_request_or_closed(&mut socket).await;
                    (socket, request)
                });
            }
            completed = readers.join_next(), if !readers.is_empty() => {
                let (socket, request) = completed.unwrap().unwrap();
                if let Some(request) = request { return (socket, request); }
            }
        }
    }
}

async fn reply(socket: &mut TcpStream, body: Value) {
    try_reply(socket, body).await.unwrap();
}

async fn try_reply(socket: &mut TcpStream, body: Value) -> std::io::Result<()> {
    let body = body.to_string();
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nRequest-Id: req-connect\r\nCache-Control: no-store\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(header.as_bytes()).await?;
    socket.write_all(body.as_bytes()).await?;
    socket.shutdown().await
}

fn spawn_worker(directory: &Path, server: &str, concurrency: &str) -> Child {
    let store = directory.join("store");
    std::fs::create_dir_all(&store).unwrap();
    let runner =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../sdk/python/ledgence_worker/bootstrap.py");
    tokio::process::Command::new(env!("CARGO_BIN_EXE_ledgence-worker"))
        .args([
            "connect",
            "--server",
            server,
            "--tenant",
            "tenant",
            "--namespace",
            "billing",
            "--queue",
            "queue",
            "--store",
            store.to_str().unwrap(),
            "--cache",
            directory.join("cache").to_str().unwrap(),
            "--python",
            &std::env::var("LEDGENCE_PYTHON").unwrap_or_else(|_| "python3".into()),
            "--runner",
            runner.to_str().unwrap(),
            "--concurrency",
            concurrency,
        ])
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap()
}

fn signal(child: &Child, value: Signal) {
    kill(Pid::from_raw(child.id().unwrap() as i32), value).unwrap();
}

async fn register(socket: &mut TcpStream, concurrency: u32) {
    let request = read_request(socket).await;
    register_request(socket, request, concurrency).await;
}

async fn register_request(socket: &mut TcpStream, (route, body): Request, concurrency: u32) {
    assert_eq!(route, "POST /v1/worker-sessions HTTP/1.1");
    assert_eq!(
        body,
        json!({"scope":{"tenant_id":"tenant","namespace":"billing"}, "queue":"queue", "concurrency":concurrency})
    );
    reply(socket, json!({"id":"session-connect", "scope":body["scope"], "queue":"queue", "concurrency":concurrency, "expires_at": 9999999999999_u64})).await;
}

#[tokio::test]
async fn connected_worker_registers_exact_concurrency_and_drains_on_first_signal() {
    let directory = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = format!("http://{}", listener.local_addr().unwrap());
    // A cancelled client exchange can leave an idle or partial TCP request.
    // Keep one open throughout the test: it must not serialize registration,
    // acquisition, or reconciliation behind its unfinished HTTP body.
    let mut partial = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    partial
        .write_all(b"POST /v1/acquisitions HTTP/1.1\r\n")
        .await
        .unwrap();
    let (ready, acquired) = oneshot::channel();
    let transport = tokio::spawn(async move {
        let mut readers = JoinSet::new();
        let (mut socket, request) = next_request(&listener, &mut readers).await;
        register_request(&mut socket, request, 3).await;
        let mut consumers = HashSet::new();
        let mut ready = Some(ready);
        loop {
            let (mut socket, (route, body)) = next_request(&listener, &mut readers).await;
            assert_eq!(route, "POST /v1/acquisitions HTTP/1.1");
            assert_eq!(body["worker_session_id"], "session-connect");
            let consumer = body["consumer_id"].as_u64().unwrap();
            assert!(consumer < 3);
            consumers.insert(consumer);
            if let Err(error) = try_reply(
                &mut socket,
                json!({"disposition":"empty", "sequence":body["sequence"]}),
            )
            .await
            {
                // Shutdown may cancel an earlier exchange while its exact
                // sequence is reconciled over another connection.
                assert!(
                    matches!(
                        error.kind(),
                        std::io::ErrorKind::BrokenPipe
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::ConnectionAborted
                            | std::io::ErrorKind::NotConnected
                    ),
                    "{error}"
                );
            }
            if consumers.len() == 3
                && let Some(ready) = ready.take()
            {
                ready.send(()).unwrap();
            }
        }
    });
    let worker = spawn_worker(directory.path(), &server, "3");
    tokio::time::timeout(Duration::from_secs(10), acquired)
        .await
        .expect("all N consumers must poll")
        .unwrap();
    signal(&worker, Signal::SIGTERM);
    let output = tokio::time::timeout(Duration::from_secs(5), worker.wait_with_output())
        .await
        .expect("idle worker should drain promptly")
        .unwrap();
    assert!(!output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["delivery"]["session_id"], "session-connect");
    assert_eq!(report["delivery"]["finished"], true);
    assert_eq!(report["delivery"]["settled_attempts"], 0);
    assert!(!String::from_utf8_lossy(&output.stderr).contains("forced exit"));
    drop(partial);
    transport.abort();
    let _ = transport.await;
}

#[tokio::test]
async fn repeated_shutdown_wait_retains_unknown_acquisition_until_explicit_second_signal() {
    let directory = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = format!("http://{}", listener.local_addr().unwrap());
    let (ready, acquired) = oneshot::channel();
    let transport = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        register(&mut socket, 1).await;
        let (mut socket, _) = listener.accept().await.unwrap();
        let (route, _) = read_request(&mut socket).await;
        assert_eq!(route, "POST /v1/acquisitions HTTP/1.1");
        ready.send(()).unwrap();
        // Keep an actual response pending. The remote operation could have
        // committed; ending a five-second shutdown wait cannot release it.
        std::future::pending::<()>().await;
        drop(socket);
    });
    let mut worker = spawn_worker(directory.path(), &server, "1");
    tokio::time::timeout(Duration::from_secs(10), acquired)
        .await
        .unwrap()
        .unwrap();
    signal(&worker, Signal::SIGTERM);
    tokio::time::sleep(Duration::from_millis(5500)).await;
    assert!(
        worker.try_wait().unwrap().is_none(),
        "first shutdown timeout must retain driver ownership"
    );
    signal(&worker, Signal::SIGINT);
    let output = tokio::time::timeout(Duration::from_secs(3), worker.wait_with_output())
        .await
        .expect("second signal must remain responsive")
        .unwrap();
    assert!(!output.status.success());
    assert!(
        output.stdout.is_empty(),
        "no completed-delivery report may be invented"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("shutdown pending"), "{stderr}");
    assert!(stderr.contains("forced exit"), "{stderr}");
    transport.abort();
    let _ = transport.await;
}

#[tokio::test]
async fn connect_rejects_a_second_concurrency_setting_and_out_of_range_capacity() {
    for option in ["--pollers", "--timeout-ms"] {
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_ledgence-worker"))
            .args([
                "connect",
                "--server",
                "http://127.0.0.1:1",
                "--tenant",
                "tenant",
                "--namespace",
                "billing",
                "--queue",
                "queue",
                "--store",
                "/unused",
                "--cache",
                "/unused",
                "--python",
                "python3",
                "--runner",
                "/unused",
                option,
                "2",
            ])
            .kill_on_drop(true)
            .output()
            .await
            .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("unknown options"));
    }
    for concurrency in ["0", "1025"] {
        let temp = tempfile::tempdir().unwrap();
        let output = tokio::time::timeout(
            Duration::from_secs(5),
            spawn_worker(temp.path(), "http://127.0.0.1:1", concurrency).wait_with_output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("concurrency"));
    }
}

#[tokio::test]
async fn short_python_attempt_executes_after_fifteen_seconds_of_acquisition_wait() {
    use ledgence_orchestration_api::{AttemptReport, SettleCommand};
    use ledgence_worker_api::ProgramOutcome;

    let directory = tempfile::tempdir().unwrap();
    let example = directory.path().join("example");
    let python = std::env::var("LEDGENCE_PYTHON").unwrap_or_else(|_| "python3".into());
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_ledgence-worker"))
        .args([
            "example",
            "--directory",
            example.to_str().unwrap(),
            "--python",
            &python,
        ])
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    std::fs::write(
        example.join("program/program.py"),
        "def handle(event):\n    return {'received': event['data'], 'executed': True}\n",
    )
    .unwrap();
    let store = directory.path().join("store");
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_ledgence-worker"))
        .args([
            "publish",
            "--source",
            example.join("program").to_str().unwrap(),
            "--store",
            store.to_str().unwrap(),
        ])
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let descriptor: Value = serde_json::from_slice(&output.stdout).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = format!("http://{}", listener.local_addr().unwrap());
    let (settled, report) = oneshot::channel();
    let transport = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        register(&mut socket, 1).await;
        let (mut socket, _) = listener.accept().await.unwrap();
        let (route, body) = read_request(&mut socket).await;
        assert_eq!(route, "POST /v1/acquisitions HTTP/1.1");
        assert_eq!(body["wait_ms"], 20_000);
        // The clock for the new attempt starts only after this actual HTTP wait.
        tokio::time::sleep(Duration::from_secs(15)).await;
        let claimed = std::time::Instant::now();
        let owner = json!({
            "scope": body["scope"], "task_id": "task_short", "attempt_id": "att_short",
            "lease_id": "lease_short", "generation": 1,
            "worker_session_id": body["worker_session_id"], "consumer_id": body["consumer_id"]
        });
        let authority = |sequence: u64, dispatch: bool| {
            let elapsed = u64::try_from(claimed.elapsed().as_millis()).unwrap();
            json!({"owner": owner, "expires_at": 100_000,
                "remaining_ms": 40_000_u64.saturating_sub(elapsed),
                "execution_remaining_ms": 10_000_u64.saturating_sub(elapsed),
                "renew_sequence": sequence, "cancel_requested": false, "dispatch_allowed": dispatch})
        };
        reply(&mut socket, json!({"disposition": "assigned", "sequence": body["sequence"], "assignment": {
            "descriptor": descriptor,
            "event": {"specversion":"1.0", "id":"evt_short", "source":"urn:ledgence:orchestrator",
                "type":"com.ledgence.task.invocation.requested.v1", "datacontenttype":"application/json",
                "ldgtenantid":"tenant", "ldgnamespace":"billing", "ldgrunid":"run_short",
                "ldgtaskid":"task_short", "ldgattemptid":"att_short", "ldgattemptno":1,
                "data":{"business_id":"short-after-wait"}},
            "lease":{"owner":owner,"expires_at":100_000}, "authority":authority(0,false),
            "attempt_deadline":70_000
        }})).await;
        let mut settled = Some(settled);
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (route, body) = read_request(&mut socket).await;
            match route.as_str() {
                "POST /v1/renewals HTTP/1.1" => {
                    assert_eq!(body["owner"], owner);
                    reply(
                        &mut socket,
                        authority(
                            body["sequence"].as_u64().unwrap(),
                            body["intent"] == "dispatch",
                        ),
                    )
                    .await;
                }
                "POST /v1/settlements HTTP/1.1" => {
                    let command =
                        SettleCommand::decode(&serde_json::to_vec(&body).unwrap()).unwrap();
                    reply(&mut socket, json!({"receipt":{"operation_id":body["operation_id"], "task_id":"task_short", "attempt_id":"att_short", "accepted_at":70_000}, "already_accepted":false, "task_state":"succeeded"})).await;
                    settled.take().unwrap().send(command).unwrap();
                }
                "POST /v1/acquisitions HTTP/1.1" => {
                    reply(
                        &mut socket,
                        json!({"disposition":"empty", "sequence":body["sequence"]}),
                    )
                    .await;
                }
                _ => panic!("unexpected worker exchange: {route}"),
            }
        }
    });
    let worker = spawn_worker(directory.path(), &server, "1");
    let command = tokio::time::timeout(Duration::from_secs(25), report)
        .await
        .expect("worker must report after the long wait and short execution")
        .unwrap();
    let AttemptReport::Completed(report) = command.report else {
        panic!("fresh short Python attempt failed: {:?}", command.report);
    };
    assert!(report.process_id > 0);
    assert!(
        matches!(report.outcome, ProgramOutcome::Success { output } if output == json!({"received":{"business_id":"short-after-wait"}, "executed":true}))
    );
    signal(&worker, Signal::SIGTERM);
    let output = tokio::time::timeout(Duration::from_secs(5), worker.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    let status: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(status["delivery"]["settled_attempts"], 1);
    assert_eq!(status["delivery"]["finished"], true);
    transport.abort();
    let _ = transport.await;
}
