#![cfg(feature = "otel")]

use serde_json::{Value, json};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

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
        assert!(bytes.len() <= 64 * 1024);
    };
    let headers = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
    let size = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    assert!(size <= 256 * 1024);
    while bytes.len() < header_end + size {
        let mut buffer = [0; 4096];
        let count = socket.read(&mut buffer).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&buffer[..count]);
    }
    (headers, bytes[header_end..header_end + size].to_vec())
}

async fn respond(socket: &mut TcpStream, status: &str, content_type: &str, body: &[u8]) {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nRequest-Id: req-cli-observation\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(header.as_bytes()).await.unwrap();
    socket.write_all(body).await.unwrap();
    socket.shutdown().await.unwrap();
}

#[tokio::test]
async fn trace_logs_diagnostics_and_collector_failures_preserve_complete_json_records() {
    for success in [true, false] {
        let api = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = format!("http://{}", api.local_addr().unwrap());
        let collector = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/traces", collector.local_addr().unwrap());
        let api_exchange = tokio::spawn(async move {
            let (mut socket, _) = api.accept().await.unwrap();
            let (headers, body) = request(&mut socket).await;
            assert!(headers.starts_with("POST /v1/tasks/cancel HTTP/1.1"));
            assert!(
                headers
                    .to_ascii_lowercase()
                    .contains("\r\ntraceparent: 00-")
            );
            assert_eq!(
                serde_json::from_slice::<Value>(&body).unwrap()["task_id"],
                "task"
            );
            if success {
                respond(&mut socket, "200 OK", "application/json", b"\"cancelled\"").await;
            } else {
                // The encoded diagnostic exceeds PIPE_BUF. It must remain one
                // record while the separate trace-export thread emits warnings.
                let error = json!({"code": "invalid_input", "message": "\u{0000}".repeat(1024)});
                respond(
                    &mut socket,
                    "400 Bad Request",
                    "application/json",
                    &serde_json::to_vec(&error).unwrap(),
                )
                .await;
            }
        });
        let export = tokio::spawn(async move {
            let (mut socket, _) = collector.accept().await.unwrap();
            let (headers, body) = request(&mut socket).await;
            assert!(headers.starts_with("POST /v1/traces HTTP/1.1"));
            assert!(
                headers
                    .to_ascii_lowercase()
                    .contains("application/x-protobuf")
            );
            assert!(!body.is_empty());
            respond(
                &mut socket,
                "503 Service Unavailable",
                "application/x-protobuf",
                b"",
            )
            .await;
        });
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_ledgence"));
        command
            .args([
                "task",
                "cancel",
                "--server",
                &server,
                "--tenant",
                "tenant",
                "--namespace",
                "billing",
                "--task",
                "task",
            ])
            .env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", endpoint)
            .env("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
            .env("OTEL_TRACES_SAMPLER_ARG", "1")
            .env("RUST_LOG", "info")
            .kill_on_drop(true);
        let output = tokio::time::timeout(Duration::from_secs(8), command.output())
            .await
            .expect("collector failure must not stall the operator command")
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(if success { 0 } else { 2 }),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        if success {
            assert_eq!(output.stdout, b"\"cancelled\"\n");
        } else {
            assert!(output.stdout.is_empty());
        }
        let records: Vec<Value> = output
            .stderr
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| {
                serde_json::from_slice(line).unwrap_or_else(|error| {
                    panic!(
                        "mixed stderr JSON record: {error}: {}",
                        String::from_utf8_lossy(line)
                    )
                })
            })
            .collect();
        let diagnostic = records
            .iter()
            .find(|record| record["request_id"] == "req-cli-observation")
            .expect("the command diagnostic retains the response request ID");
        if !success {
            assert_eq!(diagnostic["error"]["message"], "\u{0000}".repeat(1024));
        }
        let exchange = records
            .iter()
            .find(|record| record["fields"]["message"] == "orchestration HTTP exchange")
            .expect("HTTP tracing log should coexist with command diagnostic");
        assert_eq!(exchange["trace_id"].as_str().unwrap().len(), 32);
        assert!(
            records
                .iter()
                .any(|record| record["target"] == "ledgence::telemetry"),
            "collector failure should produce a bounded diagnostic"
        );
        api_exchange.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), export)
            .await
            .unwrap()
            .unwrap();
    }
}
