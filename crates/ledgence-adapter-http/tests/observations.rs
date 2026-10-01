#![cfg(all(feature = "client", feature = "server"))]

use axum::{Router, body::Bytes, extract::State, response::IntoResponse, routing::post};
use ledgence_adapter_http::HttpTaskService;
use ledgence_orchestration_api::{Scope, console::*};
use ledgence_worker_api::{
    ProcessSlotState, SlotObservation, WorkerObservationDetailState, WorkerObservationSnapshot,
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

struct Server {
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
#[derive(Default)]
struct Replies {
    received: Mutex<Vec<Value>>,
    response: Mutex<Value>,
}
async fn handler(State(replies): State<Arc<Replies>>, command: Bytes) -> impl IntoResponse {
    replies
        .received
        .lock()
        .unwrap()
        .push(serde_json::from_slice(&command).unwrap());
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&*replies.response.lock().unwrap()).unwrap(),
    )
}
async fn start(replies: Arc<Replies>) -> Server {
    let router = Router::new()
        .route("/v1/worker-observations", post(handler))
        .with_state(replies);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Server { url, task }
}
fn command() -> WorkerObservationCommand {
    WorkerObservationCommand {
        schema_version: 1,
        worker_session_id: "session-observation".into(),
        scope: Scope {
            tenant_id: "tenant".into(),
            namespace: "billing".into(),
        },
        sequence: ConsoleU64(u64::MAX),
        display_name: Some("Worker é".into()),
        snapshot: WorkerObservationSnapshot {
            configured_concurrency: 1,
            accepting: true,
            active_consumers: 0,
            occupied_process_slots: 0,
            detail_state: WorkerObservationDetailState::Available,
            slots: vec![SlotObservation {
                slot_id: 0,
                state: ProcessSlotState::Empty,
                process_instance_id: None,
                process_id: None,
                program: None,
                digest: None,
                scope: None,
                invocation: None,
            }],
        },
    }
}
fn receipt() -> Value {
    json!({"worker_session_id":"session-observation","sequence":u64::MAX.to_string(),"received_at":1234,"already_received":false})
}

#[tokio::test]
async fn publisher_round_trip_preserves_the_normalized_contract_and_full_sequence() {
    let replies = Arc::new(Replies::default());
    *replies.response.lock().unwrap() = receipt();
    let server = start(replies.clone()).await;
    let client = HttpTaskService::new(&server.url).unwrap();
    let command = command();
    let reply = client.publish_observation(&command).await.unwrap();
    assert_eq!(reply.sequence, command.sequence);
    assert_eq!(reply.received_at, 1234);
    assert_eq!(
        replies.received.lock().unwrap().as_slice(),
        [serde_json::to_value(command).unwrap()]
    );
}

#[tokio::test]
async fn publisher_rejects_invalid_input_before_sending_and_invalid_receipts() {
    let replies = Arc::new(Replies::default());
    let server = start(replies.clone()).await;
    let client = HttpTaskService::new(&server.url).unwrap();
    let mut invalid = command();
    invalid.snapshot.occupied_process_slots = 1;
    assert!(client.publish_observation(&invalid).await.is_err());
    assert!(replies.received.lock().unwrap().is_empty());
    for (field, value) in [
        ("worker_session_id", json!("other-session")),
        ("sequence", json!("1")),
        ("received_at", json!(CONSOLE_MAX_TIMESTAMP + 1)),
        ("sequence", json!(u64::MAX)),
        ("worker_session_id", json!("x".repeat(8192))),
    ] {
        let mut response = receipt();
        response[field] = value;
        *replies.response.lock().unwrap() = response;
        assert!(
            client.publish_observation(&command()).await.is_err(),
            "accepted invalid {field}"
        );
    }
    assert_eq!(replies.received.lock().unwrap().len(), 5);
}

#[tokio::test]
async fn publisher_has_a_bounded_response_deadline() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new().route(
        "/v1/worker-observations",
        post(|| async { std::future::pending::<String>().await }),
    );
    let _server = Server {
        url: url.clone(),
        task: tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        }),
    };
    let client = HttpTaskService::with_timeout(&url, Duration::from_millis(40)).unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        client.publish_observation(&command()),
    )
    .await
    .unwrap();
    assert!(result.is_err());
}
