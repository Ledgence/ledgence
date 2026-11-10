use ledgence_orchestration_api::*;
use serde_json::Value;
use std::{process::Stdio, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::Command,
};

async fn read_query(socket: &mut TcpStream) -> ExecutionProgressQuery {
    let mut bytes = Vec::new();
    let (end, length) = loop {
        let mut buffer = [0; 4096];
        let count = socket.read(&mut buffer).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&buffer[..count]);
        assert!(bytes.len() < 16384);
        if let Some(end) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
            let header = String::from_utf8_lossy(&bytes[..end]);
            assert!(header.starts_with("POST /v1/executions/progress HTTP/1.1\r\n"));
            let length = header
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            break (end + 4, length);
        }
    };
    while bytes.len() < end + length {
        let mut buffer = [0; 4096];
        let count = socket.read(&mut buffer).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&buffer[..count]);
        assert!(bytes.len() < 16384);
    }
    serde_json::from_slice(&bytes[end..end + length]).unwrap()
}
fn query(kind: &str) -> ExecutionProgressQuery {
    ExecutionProgressQuery {
        scope: Scope {
            tenant_id: "acme".into(),
            namespace: "billing".into(),
        },
        target: if kind == "task" {
            ExecutionProgressTarget::Task {
                id: "execution".into(),
            }
        } else {
            ExecutionProgressTarget::Workflow {
                id: "execution".into(),
            }
        },
        cursor: None,
        limit: 1,
        wait_ms: 0,
    }
}
async fn respond(socket: &mut TcpStream, query: &ExecutionProgressQuery, sequence: u64, tail: u64) {
    let workflow = query.target.kind() == "workflow";
    let page = ExecutionProgressPage {
        scope: query.scope.clone(),
        target: query.target.clone(),
        state: "succeeded".into(),
        terminal: true,
        high_watermark: ProgressSequence(tail),
        events: vec![ExecutionProgressEvent {
            sequence: ProgressSequence(sequence),
            at: 1,
            reason: if workflow {
                "decision_applied"
            } else {
                "cleanup_confirmed"
            }
            .into(),
            activation_id: workflow.then(|| "activation".into()),
            attempt_id: (!workflow).then(|| "attempt".into()),
        }],
        next_cursor: query.cursor_for(sequence).unwrap(),
        has_more: sequence < tail,
    };
    respond_page(socket, query, &page).await;
}
async fn respond_page(
    socket: &mut TcpStream,
    query: &ExecutionProgressQuery,
    page: &ExecutionProgressPage,
) {
    page.validate_for(query).unwrap();
    let body = serde_json::to_vec(page).unwrap();
    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nRequest-Id: req-progress\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await.unwrap();
    socket.write_all(&body).await.unwrap();
    socket.shutdown().await.unwrap();
}
fn command(server: &str, kind: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ledgence"));
    command
        .args([
            kind,
            "progress",
            "--server",
            server,
            "--tenant",
            "acme",
            "--namespace",
            "billing",
            &format!("--{kind}"),
            "execution",
            "--limit",
            "1",
        ])
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}
fn diagnostic(stderr: &[u8]) -> Value {
    serde_json::from_slice(stderr).unwrap()
}

#[tokio::test]
async fn progress_cli_follows_terminal_backlog_and_resumes_full_width_cursors_for_both_kinds() {
    for kind in ["task", "workflow"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = format!("http://{}", listener.local_addr().unwrap());
        let after = 9_007_199_254_740_993;
        let mut expected = query(kind);
        expected.cursor = Some(expected.cursor_for(after).unwrap());
        let cursor = expected.cursor.clone().unwrap();
        let exchange = tokio::spawn(async move {
            for sequence in [after + 1, after + 2] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let received = read_query(&mut socket).await;
                assert_eq!(received, expected);
                respond(&mut socket, &received, sequence, after + 2).await;
                expected.cursor = Some(expected.cursor_for(sequence).unwrap());
            }
        });
        let output = tokio::time::timeout(
            Duration::from_secs(5),
            command(&server, kind)
                .args([
                    "--cursor",
                    &cursor,
                    "--follow",
                    "--poll-interval-ms",
                    "10000",
                ])
                .output(),
        )
        .await
        .expect("terminal backlog must drain without polling delay")
        .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let pages: Vec<Value> = output
            .stdout
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0]["events"][0]["sequence"], (after + 1).to_string());
        assert_eq!(pages[1]["high_watermark"], (after + 2).to_string());
        assert_eq!(diagnostic(&output.stderr)["request_id"], "req-progress");
        exchange.await.unwrap();
    }
}

#[tokio::test]
async fn one_page_does_not_poll_even_when_history_has_more() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = format!("http://{}", listener.local_addr().unwrap());
    let exchange = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let received = read_query(&mut socket).await;
        assert_eq!(received, query("task"));
        respond(&mut socket, &received, 1, 2).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err()
        );
    });
    let output = tokio::time::timeout(Duration::from_secs(5), command(&server, "task").output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let page: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(page["has_more"], true);
    exchange.await.unwrap();
}

#[tokio::test]
async fn progress_cli_uses_bounded_server_wait_only_after_it_catches_up() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = format!("http://{}", listener.local_addr().unwrap());
    let exchange = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let first = read_query(&mut socket).await;
        assert_eq!(first.wait_ms, 0);
        let empty = ExecutionProgressPage {
            scope: first.scope.clone(),
            target: first.target.clone(),
            state: "active".into(),
            terminal: false,
            high_watermark: ProgressSequence(0),
            events: Vec::new(),
            next_cursor: first.cursor_for(0).unwrap(),
            has_more: false,
        };
        respond_page(&mut socket, &first, &empty).await;
        let (mut socket, _) = listener.accept().await.unwrap();
        let following = read_query(&mut socket).await;
        assert_eq!(following.cursor, Some(empty.next_cursor));
        assert!(following.wait_ms > 0 && following.wait_ms <= 8750);
        respond(&mut socket, &following, 1, 1).await;
    });
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        command(&server, "task")
            .args([
                "--follow",
                "--poll-interval-ms",
                "250",
                "--timeout-ms",
                "10000",
            ])
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
    assert_eq!(
        output
            .stdout
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .count(),
        2
    );
    exchange.await.unwrap();
}

#[tokio::test]
async fn progress_cli_explicit_single_page_wait_preserves_the_wire_option() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = format!("http://{}", listener.local_addr().unwrap());
    let exchange = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let received = read_query(&mut socket).await;
        assert_eq!(received.wait_ms, 18000);
        respond(&mut socket, &received, 1, 1).await;
    });
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        command(&server, "task")
            .args(["--wait-ms", "18000"])
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
    exchange.await.unwrap();
}

#[cfg(unix)]
fn full_pipe() -> (std::os::fd::OwnedFd, std::os::fd::OwnedFd) {
    use nix::{
        fcntl::{FcntlArg, FdFlag, OFlag, fcntl},
        unistd::{pipe, write},
    };
    let (reader, writer) = pipe().unwrap();
    for fd in [&reader, &writer] {
        fcntl(fd, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC)).unwrap();
    }
    fcntl(&writer, FcntlArg::F_SETFL(OFlag::O_NONBLOCK)).unwrap();
    loop {
        match write(&writer, &[b'x'; 4096]) {
            Ok(0) => panic!("pipe must accept some bytes"),
            Ok(_) => (),
            Err(nix::errno::Errno::EAGAIN) => break,
            Err(error) => panic!("cannot fill test pipe: {error}"),
        }
    }
    // Exhaust a possible final short slot as well, independent of pipe capacity.
    loop {
        match write(&writer, b"x") {
            Ok(1) => (),
            Err(nix::errno::Errno::EAGAIN) => break,
            other => panic!("cannot fill final pipe slot: {other:?}"),
        }
    }
    (reader, writer)
}

#[cfg(unix)]
#[tokio::test]
async fn timeout_and_sigterm_exit_with_full_stdout_without_next_read_or_execution_mutation() {
    for terminate in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = format!("http://{}", listener.local_addr().unwrap());
        let (sent, received) = tokio::sync::oneshot::channel();
        let exchange = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let received = read_query(&mut socket).await;
            respond(&mut socket, &received, 1, 2).await;
            sent.send(()).unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(800), listener.accept())
                    .await
                    .is_err()
            );
        });
        let (_reader, writer) = full_pipe();
        let mut command = command(&server, "task");
        command
            .args([
                "--follow",
                "--timeout-ms",
                if terminate { "10000" } else { "400" },
            ])
            .stdout(Stdio::from(writer));
        let child = command.spawn().unwrap();
        let pid = child.id().unwrap();
        tokio::time::timeout(Duration::from_secs(5), received)
            .await
            .expect("observer must request its first page")
            .unwrap();
        if terminate {
            tokio::time::sleep(Duration::from_millis(100)).await;
            nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid as i32),
                nix::sys::signal::Signal::SIGTERM,
            )
            .unwrap();
        }
        let output = tokio::time::timeout(Duration::from_secs(2), child.wait_with_output())
            .await
            .expect("observation must stop even while stdout has no reader")
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        let error = diagnostic(&output.stderr);
        assert_eq!(error["error"]["code"], "unavailable");
        assert_eq!(error["outcome_may_be_unknown"], false);
        assert_eq!(error["request_id"], "req-progress");
        let message = error["error"]["message"].as_str().unwrap();
        assert!(
            message.contains(if terminate {
                "interrupted"
            } else {
                "timed out"
            }),
            "{message}"
        );
        exchange.await.unwrap();
    }
}
