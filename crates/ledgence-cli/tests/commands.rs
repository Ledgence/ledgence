use ledgence_orchestration_api::SubmitCommand;
use serde_json::{Value, json};
use std::{process::Output, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn invoke(args: &[&str]) -> Output {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_ledgence"));
    command.args(args).kill_on_drop(true);
    tokio::time::timeout(Duration::from_secs(10), command.output())
        .await
        .expect("operator command must complete")
        .unwrap()
}

async fn request(socket: &mut TcpStream) -> (String, Vec<u8>) {
    let mut bytes = Vec::new();
    let header_end = loop {
        if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break index + 4;
        }
        let mut buffer = [0; 4096];
        let count = socket.read(&mut buffer).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&buffer[..count]);
    };
    let header = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
    let size = header
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    while bytes.len() < header_end + size {
        let mut buffer = [0; 4096];
        let count = socket.read(&mut buffer).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&buffer[..count]);
    }
    (header, bytes[header_end..header_end + size].to_vec())
}

async fn respond(socket: &mut TcpStream, status: &str, body: &str) {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nRequest-Id: req-cli-test\r\nCache-Control: no-store\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(header.as_bytes()).await.unwrap();
    socket.write_all(body.as_bytes()).await.unwrap();
    socket.shutdown().await.unwrap();
}

fn diagnostic(output: &Output) -> Value {
    let records: Vec<Value> = output
        .stderr
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("each stderr record is complete JSON"))
        .collect();
    records
        .into_iter()
        .find(|record| record.get("request_id").is_some() || record.get("error").is_some())
        .expect("operator diagnostic is separate from optional tracing logs")
}

#[tokio::test]
async fn cancellation_returns_only_result_json_and_separate_request_id() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = format!("http://{}", listener.local_addr().unwrap());
    let exchange = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let (header, body) = request(&mut socket).await;
        assert!(header.starts_with("POST /v1/tasks/cancel HTTP/1.1\r\n"));
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"scope":{"tenant_id":"tenant", "namespace":"billing"}, "task_id":".."})
        );
        respond(&mut socket, "200 OK", "\"cancelled\"").await;
    });
    let output = invoke(&[
        "task",
        "cancel",
        "--server",
        &server,
        "--tenant",
        "tenant",
        "--namespace",
        "billing",
        "--task",
        "..",
    ])
    .await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"\"cancelled\"\n");
    assert_eq!(diagnostic(&output), json!({"request_id":"req-cli-test"}));
    exchange.await.unwrap();
}

#[tokio::test]
async fn inspection_queries_preserve_unusual_ids_and_history_cursor() {
    for operation in ["inspect", "status", "result", "attempt", "history"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = format!("http://{}", listener.local_addr().unwrap());
        let exchange = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (header, body) = request(&mut socket).await;
            let target = header.lines().next().unwrap();
            assert!(
                target.contains("tenant_id=tenant+%2B%25%2F%C3%A9"),
                "{target}"
            );
            assert!(target.contains("namespace=+billing+"), "{target}");
            assert!(target.contains("task_id=.."), "{target}");
            assert!(body.is_empty());
            match operation {
                "inspect" => assert!(target.starts_with("GET /v1/tasks/inspect?")),
                "status" => assert!(target.starts_with("GET /v1/tasks/status?")),
                "result" => assert!(target.starts_with("GET /v1/tasks/result?")),
                "attempt" => {
                    assert!(target.starts_with("GET /v1/attempts/inspect?"));
                    assert!(target.contains("attempt_id=+.%2F%2B%25+"));
                }
                _ => {
                    assert!(target.starts_with("GET /v1/tasks/history?"));
                    assert!(target.contains("after_sequence=18446744073709551615"));
                }
            }
            respond(&mut socket, "404 Not Found", "{\"code\":\"not_found\"}").await;
        });
        let mut args = vec![
            "task",
            operation,
            "--server",
            &server,
            "--tenant",
            "tenant +%/é",
            "--namespace",
            " billing ",
            "--task",
            "..",
        ];
        if operation == "attempt" {
            args.extend(["--attempt", " ./+% "]);
        }
        if operation == "history" {
            args.extend(["--after", "18446744073709551615"]);
        }
        let output = invoke(&args).await;
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert_eq!(diagnostic(&output)["error"]["code"], "not_found");
        assert_eq!(diagnostic(&output)["request_id"], "req-cli-test");
        exchange.await.unwrap();
    }
}

#[tokio::test]
async fn submission_keeps_supplied_key_and_wire_values_without_retry() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("submit.json");
    let original = r#"{"idempotency_key":"  key +%/é  ","input":{"tenant_id":"tenant","namespace":"billing","queue":"queue","program":{"id":"hello","version":"1.0.0"},"data":{"small":-9223372036854775808,"large":18446744073709551615,"zero":-0.0,"nul":"\u0000"}}}"#;
    std::fs::write(&path, original).unwrap();
    let expected = SubmitCommand::decode(original.as_bytes()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = format!("http://{}", listener.local_addr().unwrap());
    let exchange = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let (header, body) = request(&mut socket).await;
        assert!(header.starts_with("POST /v1/tasks HTTP/1.1\r\n"));
        let actual = SubmitCommand::decode(&body).unwrap();
        assert_eq!(actual.idempotency_key, expected.idempotency_key);
        assert_eq!(
            actual.input.canonical_bytes().unwrap(),
            expected.input.canonical_bytes().unwrap()
        );
        // An actual socket close makes acceptance uncertain. The CLI must not
        // manufacture a new key or issue an automatic second exchange.
        drop(socket);
        assert!(
            tokio::time::timeout(Duration::from_millis(400), listener.accept())
                .await
                .is_err()
        );
    });
    let output = invoke(&[
        "task",
        "submit",
        "--server",
        &server,
        "--file",
        path.to_str().unwrap(),
    ])
    .await;
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(diagnostic(&output)["outcome_may_be_unknown"], true);
    assert_eq!(diagnostic(&output)["error"]["code"], "unavailable");
    exchange.await.unwrap();
}

#[tokio::test]
async fn task_commands_reject_success_replies_for_another_operation_without_retry() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("submit.json");
    let command = json!({
        "idempotency_key": "original-key",
        "input": {
            "tenant_id": "tenant", "namespace": "billing", "queue": "queue",
            "program": {"id":"hello", "version":"1.0.0"}, "data":{"amount":100}
        }
    });
    std::fs::write(&path, command.to_string()).unwrap();
    for (operation, mismatch) in [
        ("submit", "key"),
        ("submit", "input"),
        ("inspect", "task"),
        ("inspect", "scope"),
    ] {
        let mut reply = json!({
            "task_id":"wanted-task", "run_id":"run", "idempotency_key":"original-key",
            "input":command["input"],
            "descriptor": {
                "program": {"id":"hello", "version":"1.0.0"},
                "digest":format!("sha256:{}", "a".repeat(64)), "size":123
            },
            "origin_trace":null, "state":"queued", "submitted_at":10,
            "available_at":10, "terminal_at":null, "current_attempt_id":null,
            "attempt_count":0, "cancel_requested_at":null
        });
        match mismatch {
            "key" => reply["idempotency_key"] = "other-key".into(),
            "input" => reply["input"]["data"]["amount"] = 50.into(),
            "task" => reply["task_id"] = "other-task".into(),
            "scope" => reply["input"]["tenant_id"] = "other-tenant".into(),
            _ => unreachable!(),
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = format!("http://{}", listener.local_addr().unwrap());
        let exchange = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (header, _) = request(&mut socket).await;
            assert!(header.starts_with(if operation == "submit" {
                "POST /v1/tasks HTTP/1.1\r\n"
            } else {
                "GET /v1/tasks/inspect?"
            }));
            respond(&mut socket, "200 OK", &reply.to_string()).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(400), listener.accept())
                    .await
                    .is_err()
            );
        });
        let mut args = vec!["task", operation, "--server", &server];
        if operation == "submit" {
            args.extend(["--file", path.to_str().unwrap()]);
        } else {
            args.extend([
                "--tenant",
                "tenant",
                "--namespace",
                "billing",
                "--task",
                "wanted-task",
            ]);
        }
        let output = invoke(&args).await;
        assert_eq!(output.status.code(), Some(1), "{operation}/{mismatch}");
        assert!(output.stdout.is_empty(), "{operation}/{mismatch}");
        let error = diagnostic(&output);
        assert_eq!(error["outcome_may_be_unknown"], true);
        assert_eq!(error["error"]["code"], "unavailable");
        assert_eq!(error["request_id"], "req-cli-test");
        exchange.await.unwrap();
    }
}

#[tokio::test]
async fn submission_rejects_original_byte_errors_before_contacting_server() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("submit.json");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = format!("http://{}", listener.local_addr().unwrap());
    for data in [
        r#"{"key":1,"\u006bey":2}"#,
        "18446744073709551616",
        "-9223372036854775809",
    ] {
        std::fs::write(&path, format!(r#"{{"idempotency_key":"stable","input":{{"tenant_id":"tenant","namespace":"billing","queue":"queue","program":{{"id":"hello","version":"1.0.0"}},"data":{data}}}}}"#)).unwrap();
        let output = invoke(&[
            "task",
            "submit",
            "--server",
            &server,
            "--file",
            path.to_str().unwrap(),
        ])
        .await;
        assert_eq!(
            output.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stdout.is_empty());
        assert_eq!(diagnostic(&output)["request_id"], Value::Null);
    }
    std::fs::write(
        &path,
        vec![b' '; ledgence_orchestration_api::SUBMISSION_MAX_BYTES + 1],
    )
    .unwrap();
    let output = invoke(&[
        "task",
        "submit",
        "--server",
        &server,
        "--file",
        path.to_str().unwrap(),
    ])
    .await;
    assert_eq!(output.status.code(), Some(2));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn invalid_options_fail_as_usage_without_output() {
    for args in [
        vec!["task", "unknown", "--server", "http://localhost"],
        vec![
            "task",
            "submit",
            "--server",
            "http://localhost",
            "--file",
            "a",
            "--file",
            "b",
        ],
        vec!["task", "submit", "--server", "http://localhost", "--file"],
        vec![
            "task",
            "submit",
            "--server",
            "http://localhost",
            "--file",
            "a",
            "--idempotency-key",
            "replacement",
        ],
        vec![
            "task",
            "history",
            "--server",
            "http://localhost",
            "--tenant",
            "a",
            "--namespace",
            "b",
            "--task",
            "c",
            "--after",
            "+1",
        ],
        vec![
            "task",
            "history",
            "--server",
            "http://localhost",
            "--tenant",
            "a",
            "--namespace",
            "b",
            "--task",
            "c",
            "--after",
            "18446744073709551616",
        ],
    ] {
        let output = invoke(&args).await;
        assert_eq!(output.status.code(), Some(2), "{args:?}");
        assert!(output.stdout.is_empty());
        assert_eq!(diagnostic(&output)["error"]["code"], "invalid_input");
    }
}

#[tokio::test]
async fn task_list_encodes_exact_filters_and_prints_one_page() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = format!("http://{}", listener.local_addr().unwrap());
    let exchange = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let (header, body) = request(&mut socket).await;
        assert!(header.starts_with("GET /v1/tasks?"));
        for value in [
            "tenant_id=tenant",
            "namespace=billing",
            "state=failed",
            "queue=+q%2B%25%2F%C3%A9",
            "correlation_key=",
            "submitted_from=0",
            "submitted_until=42",
            "limit=2",
        ] {
            assert!(
                header.lines().next().unwrap().contains(value),
                "{value}: {header}"
            );
        }
        assert!(body.is_empty());
        respond(&mut socket, "200 OK", r#"{"items":[],"next_cursor":null}"#).await;
    });
    let output = invoke(&[
        "task",
        "list",
        "--server",
        &server,
        "--tenant",
        "tenant",
        "--namespace",
        "billing",
        "--state",
        "failed",
        "--queue",
        " q+%/é",
        "--correlation-key",
        "",
        "--submitted-from",
        "0",
        "--submitted-until",
        "42",
        "--limit",
        "2",
    ])
    .await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        json!({"items":[],"next_cursor":null})
    );
    assert_eq!(diagnostic(&output), json!({"request_id":"req-cli-test"}));
    exchange.await.unwrap();
}

#[tokio::test]
async fn task_list_rejects_invalid_options_without_network() {
    for extra in [
        vec!["--task", "x"],
        vec!["--limit", "0"],
        vec!["--limit", "101"],
        vec!["--limit", "+2"],
        vec!["--state", "done"],
        vec!["--submitted-from", "2", "--submitted-until", "1"],
        vec!["--cursor", "aa"],
        vec!["--queue", ""],
        vec!["--limit", "2", "--limit", "3"],
    ] {
        let mut args = vec![
            "task",
            "list",
            "--server",
            "http://127.0.0.1:1",
            "--tenant",
            "tenant",
            "--namespace",
            "billing",
        ];
        args.extend(extra);
        let output = invoke(&args).await;
        assert_eq!(
            output.status.code(),
            Some(2),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stdout.is_empty());
    }
}

fn approval_command() -> Value {
    json!({"scope":{"tenant_id":"tenant","namespace":"billing"},"workflow_id":"wf:1 / é","key":"refund:1",
        "activation_id":"activation:1","revision":18446744073709551615u64,
        "action":{"name":"app:refund","version":"1","arguments":{"amount":50,"values":[1,1.0,-0.0,18446744073709551615u64]}},
        "decision_id":"review:1","decision":"approve","reviewer":"operator","reason":null})
}
#[tokio::test]
async fn approval_decision_file_is_sent_once_and_can_be_replayed_after_lost_response() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("decision.json");
    let command = approval_command();
    std::fs::write(&path, serde_json::to_vec(&command).unwrap()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = format!("http://{}", listener.local_addr().unwrap());
    let expected = command.clone();
    let exchange = tokio::spawn(async move {
        let mut first_body = None;
        for index in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (headers, body) = request(&mut socket).await;
            assert!(headers.starts_with("POST /v1/workflows/approvals/decide HTTP/1.1\r\n"));
            assert_eq!(
                serde_json::from_slice::<Value>(&body).unwrap().to_string(),
                expected.to_string()
            );
            if index == 0 {
                first_body = Some(body);
                drop(socket);
                continue;
            }
            assert_eq!(Some(body), first_body);
            let mut proposal = expected.clone();
            for field in ["decision_id", "decision", "reviewer", "reason"] {
                proposal.as_object_mut().unwrap().remove(field);
            }
            proposal["proposed_arguments"] = json!({"amount":100});
            proposal["created_at"] = json!(1);
            proposal["deadline"] = json!(1000);
            proposal["status"] = json!("approved");
            proposal["resumed_activation_id"] = json!("activation:2");
            proposal["decision"] = json!({"decision_id":"review:1","decision":"approve","reviewer":"operator","reason":null,"decided_at":50});
            respond(
                &mut socket,
                "200 OK",
                &json!({"approval":proposal,"already_accepted":true}).to_string(),
            )
            .await;
        }
    });
    let args = [
        "approval",
        "decide",
        "--server",
        &server,
        "--file",
        path.to_str().unwrap(),
    ];
    let first = invoke(&args).await;
    assert_eq!(first.status.code(), Some(1));
    assert!(first.stdout.is_empty());
    assert_eq!(diagnostic(&first)["outcome_may_be_unknown"], true);
    let replay = invoke(&args).await;
    assert!(
        replay.status.success(),
        "{}",
        String::from_utf8_lossy(&replay.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&replay.stdout).unwrap()["already_accepted"],
        true
    );
    exchange.await.unwrap();
}
#[tokio::test]
async fn approval_decisions_reject_duplicate_json_before_network() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("decision.json");
    let command = approval_command()
        .to_string()
        .replacen("{", "{\"decision\":\"reject\",", 1);
    std::fs::write(&path, command).unwrap();
    let result = invoke(&[
        "approval",
        "decide",
        "--server",
        "http://127.0.0.1:1",
        "--file",
        path.to_str().unwrap(),
    ])
    .await;
    assert_eq!(result.status.code(), Some(2));
    assert!(result.stdout.is_empty());
    assert_eq!(diagnostic(&result)["outcome_may_be_unknown"], false);
}
#[tokio::test]
async fn approval_reads_keep_exact_scope_key_and_cursor() {
    for operation in ["list", "inspect"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = format!("http://{}", listener.local_addr().unwrap());
        let exchange = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (header, body) = request(&mut socket).await;
            assert!(header.starts_with(&format!(
                "POST /v1/workflows/approvals/{operation} HTTP/1.1\r\n"
            )));
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["workflow_id"], "wf:1 / é");
            assert_eq!(
                body["scope"],
                json!({"tenant_id":"tenant","namespace":"billing"})
            );
            if operation == "list" {
                assert_eq!(body["after_key"], "refund:1");
                assert_eq!(body["limit"], 2);
            } else {
                assert_eq!(body["key"], "refund:1");
            }
            respond(&mut socket, "404 Not Found", "{\"code\":\"not_found\"}").await;
        });
        let mut args = vec![
            "approval",
            operation,
            "--server",
            &server,
            "--tenant",
            "tenant",
            "--namespace",
            "billing",
            "--workflow",
            "wf:1 / é",
        ];
        if operation == "list" {
            args.extend(["--after-key", "refund:1", "--limit", "2"]);
        } else {
            args.extend(["--key", "refund:1"]);
        }
        assert_eq!(invoke(&args).await.status.code(), Some(1));
        exchange.await.unwrap();
    }
}
