use crate::operations::ToolDispatcher;
use ledgence_adapter_http::{HttpProgramCatalogService, HttpTaskService};
use ledgence_orchestration_api::*;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn query(after: u64) -> ExecutionProgressQuery {
    let mut query = ExecutionProgressQuery {
        scope: Scope {
            tenant_id: "acme".into(),
            namespace: "billing".into(),
        },
        target: ExecutionProgressTarget::Workflow {
            id: "workflow".into(),
        },
        cursor: None,
        limit: 100,
        wait_ms: 0,
    };
    query.cursor = Some(query.cursor_for(after).unwrap());
    query
}
#[tokio::test]
async fn progress_tool_uses_scoped_http_port_preserves_cursor_and_has_no_unknown_mutation() {
    let after = 9_007_199_254_740_993;
    let query = query(after);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = HttpTaskService::new(&format!("http://{}", listener.local_addr().unwrap()))
        .unwrap()
        .with_bearer_token("operator-token")
        .unwrap();
    let catalog = HttpProgramCatalogService::new(http.clone(), query.scope.clone()).unwrap();
    let dispatcher = ToolDispatcher::new(
        Arc::new(http.clone()),
        Arc::new(http),
        Arc::new(catalog),
        query.scope.clone(),
        true,
    )
    .unwrap();
    let expected = query.clone();
    let server = tokio::spawn(async move {
        for success in [true, false] {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let (header_end, length) = loop {
                let mut buffer = [0; 4096];
                let count = stream.read(&mut buffer).await.unwrap();
                assert_ne!(count, 0);
                bytes.extend_from_slice(&buffer[..count]);
                if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                    let header = String::from_utf8_lossy(&bytes[..end]);
                    let length = header
                        .lines()
                        .find_map(|line| {
                            line.split_once(':')
                                .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                                .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    assert!(header.starts_with("POST /v1/executions/progress HTTP/1.1"));
                    assert!(
                        header
                            .to_ascii_lowercase()
                            .contains("authorization: bearer operator-token")
                    );
                    break (end + 4, length);
                }
            };
            while bytes.len() < header_end + length {
                let mut buffer = [0; 4096];
                let count = stream.read(&mut buffer).await.unwrap();
                assert_ne!(count, 0);
                bytes.extend_from_slice(&buffer[..count]);
            }
            let received: ExecutionProgressQuery =
                serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
            assert_eq!(received, expected);
            let reply = if success {
                json!(ExecutionProgressPage {
                    scope: expected.scope.clone(),
                    target: expected.target.clone(),
                    state: "waiting".into(),
                    terminal: false,
                    high_watermark: ProgressSequence(after + 1),
                    events: vec![ExecutionProgressEvent {
                        sequence: ProgressSequence(after + 1),
                        at: 5,
                        reason: "decision_applied".into(),
                        activation_id: Some("activation".into()),
                        attempt_id: None
                    }],
                    next_cursor: expected.cursor_for(after + 1).unwrap(),
                    has_more: false,
                })
            } else {
                json!({"code":"unavailable","message":"storage unavailable"})
            };
            let body = serde_json::to_vec(&reply).unwrap();
            let status = if success {
                "200 OK"
            } else {
                "503 Service Unavailable"
            };
            stream.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nRequest-Id: progress-test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
            stream.shutdown().await.unwrap();
        }
    });
    let args = json!({"target":query.target,"cursor":query.cursor});
    let result: Value = dispatcher
        .call("ledgence_execution_progress", &args)
        .await
        .unwrap();
    assert_eq!(result["events"][0]["sequence"], (after + 1).to_string());
    assert_eq!(result["high_watermark"], (after + 1).to_string());
    let error = dispatcher
        .call("ledgence_execution_progress", &args)
        .await
        .unwrap_err();
    assert_eq!(error.code, "unavailable");
    assert!(!error.outcome_unknown);
    server.await.unwrap();
}
