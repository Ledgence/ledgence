//! Exercise the public command's structured diagnostics over real HTTP.
use serde_json::{Value, json};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

fn status(stage: &str, malformed: bool) -> Value {
    let mut value = json!({
        "selection": {"artifact_id":"art_report","upload_id":"upload_1","revision":1},
        "selected": true, "stage": stage, "prepare_expires_at": 120000,
        "ingress_expires_at": null, "dispatch_expires_at": null, "successor": null,
        "reference": null, "retry": "upload"
    });
    if stage != "prepared" {
        value["retry"] = json!(if stage == "receiving" {
            "wait"
        } else {
            "reconcile"
        });
        if stage == "receiving" {
            value["ingress_expires_at"] = json!(120000);
        } else if !malformed {
            value["dispatch_expires_at"] = json!(120000);
        }
    }
    value
}
async fn request(socket: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let header_end = loop {
        if let Some(end) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
            break end + 4;
        }
        let mut buffer = [0; 4096];
        let count = socket.read(&mut buffer).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&buffer[..count]);
        assert!(bytes.len() < 16384);
    };
    let header = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
    let length = header
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    while bytes.len() < header_end + length {
        let mut buffer = [0; 4096];
        let count = socket.read(&mut buffer).await.unwrap();
        assert_ne!(count, 0);
        bytes.extend_from_slice(&buffer[..count]);
    }
    if header
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        // This regression deliberately uploads an empty regular file. Consume
        // its terminal chunk before replying, avoiding a reset with unread IO.
        while bytes.len() < header_end + 5 {
            let mut buffer = [0; 4096];
            let count = socket.read(&mut buffer).await.unwrap();
            assert_ne!(count, 0);
            bytes.extend_from_slice(&buffer[..count]);
        }
        assert_eq!(&bytes[header_end..], b"0\r\n\r\n");
    }
    header
}
async fn respond(socket: &mut TcpStream, upload: Value) {
    let body = serde_json::to_vec(&json!({"upload":upload})).unwrap();
    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nRequest-Id: req-artifact-cli\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await.unwrap();
    socket.write_all(&body).await.unwrap();
    socket.shutdown().await.unwrap();
}

#[tokio::test]
async fn artifact_cli_reports_pending_receipt_uncertainty_and_preserves_recovery_identity() {
    for (stage, replay, malformed) in [
        ("uncertain", false, false),
        ("writing", false, false),
        ("uncertain", true, false),
        ("receiving", false, false),
        ("uncertain", false, true),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("empty");
        std::fs::write(&source, b"").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = format!("http://{}", listener.local_addr().unwrap());
        let exchange = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert!(
                request(&mut socket)
                    .await
                    .starts_with("POST /v1/artifacts/prepare HTTP/1.1")
            );
            respond(
                &mut socket,
                status(if replay { stage } else { "prepared" }, false),
            )
            .await;
            if !replay {
                let (mut socket, _) = listener.accept().await.unwrap();
                assert!(
                    request(&mut socket)
                        .await
                        .starts_with("PUT /v1/artifacts/uploads/upload_1/content HTTP/1.1")
                );
                respond(&mut socket, status(stage, malformed)).await;
            }
        });
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_ledgence"));
        command
            .args([
                "artifacts",
                "put",
                "--server",
                &server,
                "--tenant",
                "acme",
                "--namespace",
                "test",
                "--task",
                "task",
                "--key",
                "report",
                "--file",
            ])
            .arg(source)
            .kill_on_drop(true);
        let output = tokio::time::timeout(Duration::from_secs(10), command.output())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), exchange)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let diagnostic: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(diagnostic["request_id"], "req-artifact-cli");
        assert_eq!(
            diagnostic["outcome_may_be_unknown"],
            !malformed && stage != "receiving"
        );
        assert_eq!(
            diagnostic["artifact_upload"],
            status("prepared", false)["selection"]
        );
        if !malformed {
            assert!(
                diagnostic["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("status/reconcile/replace")
            );
        }
    }
}
