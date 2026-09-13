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
};

async fn read_request(socket: &mut TcpStream) -> (String, Value) {
    let mut bytes = Vec::new();
    let end = loop {
        if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break index + 4;
        }
        let mut buffer = [0; 4096];
        let count = socket.read(&mut buffer).await.unwrap();
        assert_ne!(count, 0);
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
        let count = socket.read(&mut buffer).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&buffer[..count]);
    }
    (
        header.lines().next().unwrap().into(),
        serde_json::from_slice(&bytes[end..end + size]).unwrap(),
    )
}

async fn reply(socket: &mut TcpStream, body: Value) {
    let body = body.to_string();
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nRequest-Id: req-connect\r\nCache-Control: no-store\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(header.as_bytes()).await.unwrap();
    socket.write_all(body.as_bytes()).await.unwrap();
    socket.shutdown().await.unwrap();
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
            "python3",
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
    let (route, body) = read_request(socket).await;
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
    let (ready, acquired) = oneshot::channel();
    let transport = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        register(&mut socket, 3).await;
        let mut consumers = HashSet::new();
        let mut ready = Some(ready);
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (route, body) = read_request(&mut socket).await;
            assert_eq!(route, "POST /v1/acquisitions HTTP/1.1");
            assert_eq!(body["worker_session_id"], "session-connect");
            let consumer = body["consumer_id"].as_u64().unwrap();
            assert!(consumer < 3);
            consumers.insert(consumer);
            reply(
                &mut socket,
                json!({"disposition":"empty", "sequence":body["sequence"]}),
            )
            .await;
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
