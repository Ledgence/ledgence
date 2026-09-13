#![cfg(all(feature = "client", feature = "server"))]

use ledgence_adapter_http::{HttpTaskService, RESPONSE_MAX_BYTES, server};
use ledgence_orchestration_api::*;
use ledgence_worker_api::{
    CloudEvent, Digest, ExecutionContext, ExecutionReport, ExecutionRequest, ProgramDescriptor,
    ProgramOutcome, ProgramRef,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Default)]
struct Mock {
    calls: Mutex<Vec<(&'static str, Value)>>,
    replies: Mutex<HashMap<&'static str, Result<Value>>>,
}
impl Mock {
    fn set<T: Serialize>(&self, operation: &'static str, result: Result<T>) {
        self.replies.lock().unwrap().insert(
            operation,
            result.map(|value| serde_json::to_value(value).unwrap()),
        );
    }
    fn reply<T: DeserializeOwned>(
        &self,
        operation: &'static str,
        input: impl Serialize,
    ) -> Result<T> {
        self.calls
            .lock()
            .unwrap()
            .push((operation, serde_json::to_value(input).unwrap()));
        let value = self
            .replies
            .lock()
            .unwrap()
            .get(operation)
            .cloned()
            .unwrap_or(Err(ContractError::NotFound))?;
        Ok(serde_json::from_value(value).unwrap())
    }
}
impl TaskService for Mock {
    fn open_session<'a>(
        &'a self,
        scope: &'a Scope,
        queue: &'a str,
        concurrency: u32,
    ) -> ContractFuture<'a, WorkerSession> {
        Box::pin(async move {
            self.reply(
                "open",
                json!({"scope":scope,"queue":queue,"concurrency":concurrency}),
            )
        })
    }
    fn extend_session<'a>(&'a self, id: &'a str) -> ContractFuture<'a, WorkerSession> {
        Box::pin(async move { self.reply("extend", id) })
    }
    fn submit<'a>(&'a self, command: &'a SubmitCommand) -> ContractFuture<'a, TaskSnapshot> {
        Box::pin(async move { self.reply("submit", command) })
    }
    fn inspect<'a>(&'a self, scope: &'a Scope, task: &'a str) -> ContractFuture<'a, TaskSnapshot> {
        Box::pin(async move { self.reply("inspect", json!([scope, task])) })
    }
    fn inspect_attempt<'a>(
        &'a self,
        scope: &'a Scope,
        task: &'a str,
        attempt: &'a str,
    ) -> ContractFuture<'a, AttemptSnapshot> {
        Box::pin(async move { self.reply("attempt", json!([scope, task, attempt])) })
    }
    fn history<'a>(
        &'a self,
        scope: &'a Scope,
        task: &'a str,
        after: u64,
    ) -> ContractFuture<'a, Vec<RecordedHistoryEvent>> {
        Box::pin(async move { self.reply("history", json!([scope, task, after])) })
    }
    fn acquire<'a>(&'a self, command: &'a AcquireCommand) -> ContractFuture<'a, AcquireReply> {
        Box::pin(async move { self.reply("acquire", command) })
    }
    fn renew<'a>(&'a self, command: &'a RenewCommand) -> ContractFuture<'a, Authority> {
        Box::pin(async move { self.reply("renew", command) })
    }
    fn settle<'a>(&'a self, command: &'a SettleCommand) -> ContractFuture<'a, SettleReply> {
        Box::pin(async move { self.reply("settle", command) })
    }
    fn confirm_quiescence<'a>(&'a self, owner: &'a LeaseOwner) -> ContractFuture<'a, TaskState> {
        Box::pin(async move { self.reply("confirm", owner) })
    }
    fn cancel<'a>(&'a self, scope: &'a Scope, task: &'a str) -> ContractFuture<'a, TaskState> {
        Box::pin(async move { self.reply("cancel", json!([scope, task])) })
    }
}

struct Running {
    url: String,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Running {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn start(router: axum::Router) -> Running {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Running { url, task }
}
fn scope() -> Scope {
    Scope {
        tenant_id: " tenant + % / é ".into(),
        namespace: "..".into(),
    }
}
fn descriptor() -> ProgramDescriptor {
    ProgramDescriptor {
        program: ProgramRef {
            id: "invoice".into(),
            version: "1.0.0".into(),
        },
        digest: Digest(format!("sha256:{}", "a".repeat(64))),
        size: 123,
    }
}
fn submit(data: Value) -> SubmitCommand {
    SubmitCommand {
        idempotency_key: " idempotency +%/é ".into(),
        origin_trace: None,
        input: SubmitTask {
            tenant_id: scope().tenant_id,
            namespace: scope().namespace,
            queue: "queue".into(),
            program: descriptor().program,
            correlation_key: Some("invoice:42".into()),
            data,
            retry_policy: RetryPolicy::default(),
            attempt_timeout_ms: 300_000,
        },
    }
}
fn task(data: Value) -> TaskSnapshot {
    let submit = submit(data);
    TaskSnapshot {
        task_id: "..".into(),
        run_id: "run".into(),
        idempotency_key: submit.idempotency_key,
        input: submit.input,
        descriptor: descriptor(),
        origin_trace: None,
        state: TaskState::Active,
        submitted_at: 10,
        available_at: 10,
        terminal_at: None,
        current_attempt_id: Some("attempt +%é/ ".into()),
        attempt_count: 1,
        cancel_requested_at: None,
    }
}
fn owner() -> LeaseOwner {
    LeaseOwner {
        scope: scope(),
        task_id: "..".into(),
        attempt_id: "attempt +%é/ ".into(),
        lease_id: "lease".into(),
        generation: 1,
        worker_session_id: "session".into(),
        consumer_id: 0,
    }
}
fn event(data: Value) -> CloudEvent {
    CloudEvent::new(json!({"specversion":"1.0","id":"event","source":"urn:ledgence:orchestrator",
        "type":"com.ledgence.task.invocation.requested.v1","datacontenttype":"application/json",
        "ldgtenantid":scope().tenant_id,"ldgnamespace":scope().namespace,"ldgrunid":"run", "ldgtaskid":"..",
        "ldgattemptid":owner().attempt_id,"ldgattemptno":1,"data":data})).unwrap()
}
fn settlement(output: Value) -> SettleCommand {
    SettleCommand {
        owner: owner(),
        operation_id: "settle".into(),
        quiescence: Quiescence::Confirmed,
        processing_trace: None,
        report: AttemptReport::Completed(ExecutionReport {
            context: Box::new(ExecutionContext::from(&ExecutionRequest {
                descriptor: descriptor(),
                event: event(Value::Null),
            })),
            process_id: 42,
            reused_process: true,
            outcome: ProgramOutcome::Success { output },
            elapsed_ms: 12,
        }),
    }
}
fn receipt() -> SettlementReceipt {
    SettlementReceipt {
        operation_id: "settle".into(),
        task_id: "..".into(),
        attempt_id: owner().attempt_id,
        accepted_at: 100,
    }
}
fn attempt(data: Value, command: SettleCommand) -> AttemptSnapshot {
    AttemptSnapshot {
        event: event(data),
        descriptor: descriptor(),
        lease: Lease {
            owner: owner(),
            expires_at: 60000,
        },
        deadline: 300000,
        authority_deadline: 300000,
        state: AttemptState::Succeeded,
        execution_may_have_started: true,
        last_renewal: Some(RenewCommand {
            owner: owner(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        }),
        quiescence: Quiescence::Confirmed,
        settlement: Some(AcceptedSettlement {
            command,
            receipt: receipt(),
        }),
        finished_at: Some(100),
    }
}
fn authority() -> Authority {
    Authority {
        owner: owner(),
        expires_at: 60000,
        remaining_ms: 59000,
        execution_remaining_ms: 299000,
        renew_sequence: 1,
        cancel_requested: false,
        dispatch_allowed: true,
    }
}
fn acquisition() -> AcquireCommand {
    AcquireCommand {
        scope: scope(),
        queue: "queue".into(),
        worker_session_id: "session".into(),
        consumer_id: 0,
        sequence: 1,
    }
}
fn exact(value: impl Serialize) -> Vec<u8> {
    serde_json::to_vec(&value).unwrap()
}

#[tokio::test]
async fn all_methods_roundtrip_scoped_opaque_ids_and_lossless_application_values() {
    let data = decode_unique_json::<Value>(br#"[18446744073709551615,-9223372036854775808,9007199254740993,1,1.0,-0.0,2.291712365432881e-09,{"nul":"\u0000"}]"#, 1000).unwrap();
    let mock = Arc::new(Mock::default());
    let session = WorkerSession {
        id: "session".into(),
        scope: scope(),
        queue: "queue".into(),
        concurrency: 3,
        expires_at: 86400000,
    };
    mock.set("open", Ok(session.clone()));
    mock.set("extend", Ok(session));
    mock.set("submit", Ok(task(data.clone())));
    mock.set("inspect", Ok(task(data.clone())));
    let attempt = attempt(data.clone(), settlement(data.clone()));
    mock.set("attempt", Ok(attempt.clone()));
    let history = vec![RecordedHistoryEvent {
        sequence: u64::MAX,
        event: HistoryEvent {
            task_id: "..".into(),
            attempt_id: Some(owner().attempt_id),
            at: 100,
            reason: TransitionReason::Succeeded,
        },
    }];
    mock.set("history", Ok(history.clone()));
    let assignment = Assignment {
        descriptor: descriptor(),
        event: event(data.clone()),
        lease: attempt.lease.clone(),
        authority: authority(),
        attempt_deadline: 300000,
    };
    mock.set(
        "acquire",
        Ok(AcquireReply::Assigned {
            sequence: 1,
            assignment: Box::new(assignment.clone()),
        }),
    );
    mock.set("renew", Ok(authority()));
    mock.set(
        "settle",
        Ok(SettleReply {
            receipt: receipt(),
            already_accepted: true,
            task_state: TaskState::Succeeded,
        }),
    );
    mock.set("confirm", Ok(TaskState::Succeeded));
    mock.set("cancel", Ok(TaskState::Active));
    let running = start(server::router(mock.clone())).await;
    let metadata = Arc::new(Mutex::new(Vec::new()));
    let observed = metadata.clone();
    let client = HttpTaskService::new(&running.url)
        .unwrap()
        .with_observer(move |meta| observed.lock().unwrap().push(meta.clone()));
    assert_eq!(
        client
            .open_session(&scope(), "queue", 3)
            .await
            .unwrap()
            .concurrency,
        3
    );
    assert_eq!(
        client.extend_session("session").await.unwrap().id,
        "session"
    );
    assert_eq!(
        exact(client.submit(&submit(data.clone())).await.unwrap()),
        exact(task(data.clone()))
    );
    for task_id in [
        ".",
        "..",
        " leading trailing ",
        "slash/plus+percent%é",
        "%2F",
    ] {
        client.inspect(&scope(), task_id).await.unwrap();
        assert_eq!(
            mock.calls.lock().unwrap().last().unwrap().1,
            json!([scope(), task_id])
        );
    }
    assert_eq!(
        exact(
            client
                .inspect_attempt(&scope(), "..", &owner().attempt_id)
                .await
                .unwrap()
        ),
        exact(attempt)
    );
    assert_eq!(
        exact(client.history(&scope(), "..", u64::MAX - 1).await.unwrap()),
        exact(history)
    );
    assert_eq!(
        exact(client.acquire(&acquisition()).await.unwrap()),
        exact(AcquireReply::Assigned {
            sequence: 1,
            assignment: Box::new(assignment)
        })
    );
    assert_eq!(
        client
            .renew(&RenewCommand {
                owner: owner(),
                sequence: 1,
                intent: RenewIntent::Dispatch
            })
            .await
            .unwrap(),
        authority()
    );
    assert!(
        client
            .settle(&settlement(data.clone()))
            .await
            .unwrap()
            .already_accepted
    );
    assert_eq!(
        client.confirm_quiescence(&owner()).await.unwrap(),
        TaskState::Succeeded
    );
    assert_eq!(
        client.cancel(&scope(), "..").await.unwrap(),
        TaskState::Active
    );
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls.len(), 15);
    assert_eq!(
        exact(&calls.iter().find(|(name, _)| *name == "submit").unwrap().1),
        exact(serde_json::to_value(submit(data.clone())).unwrap())
    );
    assert_eq!(
        exact(&calls.iter().find(|(name, _)| *name == "settle").unwrap().1),
        exact(serde_json::to_value(settlement(data)).unwrap())
    );
    let records = metadata.lock().unwrap();
    assert_eq!(records.len(), 15);
    let ids: std::collections::HashSet<_> = records
        .iter()
        .map(|record| record.request_id.as_ref().unwrap())
        .collect();
    assert_eq!(ids.len(), 15);
}

#[tokio::test]
async fn every_domain_error_and_successful_empty_or_lost_disposition_is_preserved() {
    let mock = Arc::new(Mock::default());
    let running = start(server::router(mock.clone())).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    for error in [
        ContractError::InvalidInput("invalid".into()),
        ContractError::Conflict,
        ContractError::OwnershipLost,
        ContractError::UnknownSession,
        ContractError::SessionExpired,
        ContractError::ObsoleteOperation,
        ContractError::OutOfOrder,
        ContractError::Busy,
        ContractError::NotFound,
        ContractError::Unavailable("unavailable".into()),
    ] {
        mock.set::<Value>("acquire", Err(error.clone()));
        assert_eq!(client.acquire(&acquisition()).await.unwrap_err(), error);
    }
    for reply in [
        AcquireReply::Empty { sequence: 1 },
        AcquireReply::OwnershipLost {
            sequence: 1,
            assignment: AttemptRef {
                task_id: "..".into(),
                attempt_id: owner().attempt_id,
            },
        },
    ] {
        mock.set("acquire", Ok(&reply));
        assert_eq!(
            exact(client.acquire(&acquisition()).await.unwrap()),
            exact(reply)
        );
    }
}

#[tokio::test]
async fn strict_original_bytes_reject_duplicates_number_overflow_and_unknown_fields_before_mutation()
 {
    let mock = Arc::new(Mock::default());
    let running = start(server::router(mock.clone())).await;
    let client = reqwest::Client::new();
    let valid = String::from_utf8(exact(submit(json!("REPLACE")))).unwrap();
    let mut malformed = vec![];
    for replacement in [
        r#"{"a":1,"a":2}"#,
        r#"[{"a":{"x":1,"\u0078":1}}]"#,
        "18446744073709551616",
        "-9223372036854775809",
        "1e400",
        "NaN",
    ] {
        malformed.push(valid.replace("\"REPLACE\"", replacement).into_bytes());
    }
    malformed.push(
        valid
            .replace(
                "\"idempotency_key\":",
                "\"idempotency_key\":\"other\",\"idempotency_key\":",
            )
            .into_bytes(),
    );
    malformed.push(
        valid
            .replace("\"input\":", "\"unexpected\":true,\"input\":")
            .into_bytes(),
    );
    malformed.push([valid.as_bytes(), b" {}"].concat());
    malformed.push(vec![0xff, 0xfe]);
    for bytes in malformed {
        let response = client
            .post(format!("{}/v1/tasks", running.url))
            .header("Content-Type", "application/json")
            .body(bytes)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert!(response.headers().contains_key("request-id"));
    }
    assert!(mock.calls.lock().unwrap().is_empty());
    let bad = String::from_utf8(exact(settlement(json!("REPLACE"))))
        .unwrap()
        .replace("\"REPLACE\"", r#"{"x":1,"\u0078":2}"#);
    assert_eq!(
        client
            .post(format!("{}/v1/settlements", running.url))
            .header("Content-Type", "application/json")
            .body(bad)
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert!(mock.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn query_parameters_are_strictly_decoded_once_and_routes_have_distinct_errors() {
    let mock = Arc::new(Mock::default());
    let running = start(server::router(mock.clone())).await;
    let client = reqwest::Client::new();
    for query in [
        "tenant_id=a&namespace=b&task_id=x&task_id=y",
        "tenant_id=a&namespace=b&task_id=%",
        "tenant_id=a&namespace=b&task_id=%GG",
        "tenant_id=a&namespace=b&task_id=%ff",
        "tenant_id=a&namespace=b&task_id=x&unknown=z",
        "tenant_id=a&namespace=b&task_id=x&%74ask_id=z",
        "tenant_id=a&task_id=x",
        "tenant_id=a&namespace=b&task_id=x&",
        "tenant_id=a&namespace=b&task_id=x&after_sequence=1",
    ] {
        assert_eq!(
            client
                .get(format!("{}/v1/tasks/inspect?{query}", running.url))
                .send()
                .await
                .unwrap()
                .status(),
            400,
            "{query}"
        );
    }
    assert!(mock.calls.lock().unwrap().is_empty());
    let response = client
        .get(format!("{}/missing", running.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
    assert_eq!(
        response.text().await.unwrap(),
        r#"{"code":"route_not_found"}"#
    );
    let response = client
        .get(format!("{}/v1/acquisitions", running.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 405);
    assert_eq!(response.headers()["allow"], "POST");
    assert_eq!(
        response.text().await.unwrap(),
        r#"{"code":"method_not_allowed"}"#
    );
}

#[tokio::test]
async fn depth_64_survives_submission_and_settlement_but_65_is_rejected() {
    let mock = Arc::new(Mock::default());
    let running = start(server::router(mock.clone())).await;
    let client = reqwest::Client::new();
    let mut data = Value::Null;
    for _ in 0..64 {
        data = json!([data]);
    }
    mock.set("submit", Ok(task(data.clone())));
    mock.set(
        "settle",
        Ok(SettleReply {
            receipt: receipt(),
            already_accepted: false,
            task_state: TaskState::Succeeded,
        }),
    );
    for (path, bytes) in [
        ("tasks", exact(submit(data.clone()))),
        ("settlements", exact(settlement(data.clone()))),
    ] {
        assert_eq!(
            client
                .post(format!("{}/v1/{path}", running.url))
                .header("content-type", "application/json")
                .body(bytes)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
    data = json!([data]);
    for (path, bytes) in [
        ("tasks", exact(submit(data.clone()))),
        ("settlements", exact(settlement(data.clone()))),
    ] {
        assert_eq!(
            client
                .post(format!("{}/v1/{path}", running.url))
                .header("content-type", "application/json")
                .body(bytes)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
    }
    assert_eq!(mock.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn exchange_trace_headers_never_replace_durable_origin_or_processing_context() {
    let mock = Arc::new(Mock::default());
    mock.set("submit", Ok(task(Value::Null)));
    mock.set(
        "settle",
        Ok(SettleReply {
            receipt: receipt(),
            already_accepted: false,
            task_state: TaskState::Succeeded,
        }),
    );
    let running = start(server::router(mock.clone())).await;
    let client = reqwest::Client::new();
    let origin = TraceContext {
        traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into(),
        tracestate: Some("vendor=value".into()),
    };
    let mut command = submit(Value::Null);
    command.origin_trace = Some(origin.clone());
    let mut report = settlement(Value::Null);
    report.processing_trace = Some(origin);
    let mut ids = Vec::new();
    for trace in [
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "malformed",
    ] {
        for (path, bytes) in [("tasks", exact(&command)), ("settlements", exact(&report))] {
            let response = client
                .post(format!("{}/v1/{path}", running.url))
                .header("content-type", "application/json; charset=utf-8")
                .header("traceparent", trace)
                .header("tracestate", "different=exchange")
                .header("request-id", "client-supplied-id")
                .body(bytes)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            ids.push(
                response.headers()["request-id"]
                    .to_str()
                    .unwrap()
                    .to_owned(),
            );
        }
    }
    client
        .post(format!("{}/v1/tasks", running.url))
        .header("content-type", "application/json")
        .header(
            "traceparent",
            &command.origin_trace.as_ref().unwrap().traceparent,
        )
        .body(exact(submit(Value::Null)))
        .send()
        .await
        .unwrap();
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls[0].1, calls[2].1);
    assert_eq!(calls[1].1, calls[3].1);
    assert_eq!(
        calls[0].1["origin_trace"],
        serde_json::to_value(command.origin_trace).unwrap()
    );
    assert!(calls[4].1.get("origin_trace").is_none());
    assert_eq!(
        ids.iter().collect::<std::collections::HashSet<_>>().len(),
        4
    );
    assert!(ids.iter().all(|id| id != "client-supplied-id"));
}

#[tokio::test]
async fn shutdown_admission_rejection_keeps_transport_metadata_and_skips_service() {
    let mock = Arc::new(Mock::default());
    let stopping = Arc::new(AtomicBool::new(true));
    let running = start(server::router_with_admission(
        mock.clone(),
        stopping.clone(),
    ))
    .await;
    let response = reqwest::Client::new()
        .post(format!("{}/v1/acquisitions", running.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    assert!(response.headers().contains_key("request-id"));
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert!(mock.calls.lock().unwrap().is_empty());
    stopping.store(false, Ordering::Release);
    mock.set("acquire", Ok(AcquireReply::Empty { sequence: 1 }));
    HttpTaskService::new(&running.url)
        .unwrap()
        .acquire(&acquisition())
        .await
        .unwrap();
}

async fn raw_response(
    response: Vec<u8>,
    delay_after_headers: Option<Duration>,
) -> (Running, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let accepted = count.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            accepted.fetch_add(1, Ordering::SeqCst);
            let response = response.clone();
            tokio::spawn(async move {
                read_request(&mut stream).await;
                if let Some(delay) = delay_after_headers {
                    let end = response
                        .windows(4)
                        .position(|part| part == b"\r\n\r\n")
                        .unwrap()
                        + 4;
                    stream.write_all(&response[..end]).await.unwrap();
                    tokio::time::sleep(delay).await;
                    let _ = stream.write_all(&response[end..]).await;
                } else {
                    let _ = stream.write_all(&response).await;
                }
                let _ = stream.shutdown().await;
            });
        }
    });
    (Running { url, task }, count)
}
fn response(status: u16, content_type: &str, bytes: &[u8]) -> Vec<u8> {
    [format!("HTTP/1.1 {status} Test\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nRequest-Id: proxy_request\r\nConnection: close\r\n\r\n", bytes.len()).as_bytes(), bytes].concat()
}

#[tokio::test]
async fn malformed_or_mismatched_replies_are_uncertain_and_never_domain_absence() {
    let mut replies = vec![
        response(404, "application/json", br#"{"code":"ownership_lost"}"#),
        response(409, "application/json", br#"{"code":"not_found"}"#),
        response(503, "text/html", b"proxy failed"),
        response(204, "application/json", b""),
        response(
            200,
            "application/json",
            br#"{"disposition":"empty","sequence":1,"sequence":2}"#,
        ),
        response(
            200,
            "application/json",
            br#"{"disposition":"empty","sequence":18446744073709551616}"#,
        ),
        response(404, "application/json", br#"{"code":"route_not_found"}"#),
        response(409, "application/json", br#"{"code":"new_unknown_error"}"#),
        response(
            404,
            "application/json",
            br#"{"code":"not_found","code":"not_found"}"#,
        ),
        response(200, "application/json", br#"{"disposition":"empty""#),
        response(503, "application/json", &vec![b' '; 65537]),
    ];
    let mut truncated = response(
        200,
        "application/json",
        br#"{"disposition":"empty","sequence":1}"#,
    );
    truncated.truncate(truncated.len() - 3);
    replies.push(truncated);
    for bytes in replies {
        let (running, _) = raw_response(bytes, None).await;
        assert!(matches!(
            HttpTaskService::new(&running.url)
                .unwrap()
                .acquire(&acquisition())
                .await,
            Err(ContractError::Unavailable(_))
        ));
    }
}

#[tokio::test]
async fn redirects_and_retryable_responses_do_not_trigger_an_extra_exchange() {
    let busy = String::from_utf8(response(
        503,
        "application/json",
        &exact(ContractError::Unavailable("busy".into())),
    ))
    .unwrap()
    .replace("Connection: close", "Retry-After: 0\r\nConnection: close")
    .into_bytes();
    for (bytes, expected) in [
        (b"HTTP/1.1 307 Redirect\r\nLocation: /v1/acquisitions\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(), None),
        (busy, Some(ContractError::Unavailable("busy".into()))),
    ] {
        let (running, count) = raw_response(bytes, None).await;
        let error = HttpTaskService::new(&running.url).unwrap().acquire(&acquisition()).await.unwrap_err();
        assert!(matches!(error, ContractError::Unavailable(_)));
        if let Some(expected) = expected { assert_eq!(error, expected); }
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn stalled_body_and_late_authority_responses_do_not_escape_exchange_budget() {
    let bytes = response(200, "application/json", &exact(authority()));
    let (running, count) = raw_response(bytes, Some(Duration::from_millis(250))).await;
    let client = HttpTaskService::with_timeout(&running.url, Duration::from_millis(80)).unwrap();
    let started = tokio::time::Instant::now();
    let result = client
        .renew(&RenewCommand {
            owner: owner(),
            sequence: 1,
            intent: RenewIntent::Dispatch,
        })
        .await;
    assert!(matches!(result, Err(ContractError::Unavailable(_))));
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn oversized_success_responses_are_bounded_even_without_content_length() {
    for bytes in [
        format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", RESPONSE_MAX_BYTES + 1).into_bytes(),
        [b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n".as_slice(), &vec![b' '; RESPONSE_MAX_BYTES + 1]].concat(),
    ] {
        let (running, _) = raw_response(bytes, None).await;
        assert!(matches!(HttpTaskService::new(&running.url).unwrap().acquire(&acquisition()).await, Err(ContractError::Unavailable(_))));
    }
}

#[tokio::test]
async fn exact_request_limits_and_valid_snapshot_larger_than_eight_mib_roundtrip() {
    let mock = Arc::new(Mock::default());
    let running = start(server::router(mock.clone())).await;
    let raw = reqwest::Client::new();
    let client = HttpTaskService::new(&running.url).unwrap();
    let data = Value::String("x".repeat(SUBMISSION_DATA_MAX_BYTES - 2));
    mock.set("submit", Ok(task(data.clone())));
    mock.set(
        "settle",
        Ok(SettleReply {
            receipt: receipt(),
            already_accepted: false,
            task_state: TaskState::Succeeded,
        }),
    );
    let mut largest_report = settlement(Value::String(String::new()));
    let overhead = exact(&largest_report).len();
    let AttemptReport::Completed(report) = &mut largest_report.report else {
        unreachable!()
    };
    report.outcome = ProgramOutcome::Success {
        output: Value::String("r".repeat(SETTLEMENT_MAX_BYTES - overhead)),
    };
    let encoded = exact(&largest_report);
    assert_eq!(encoded.len(), SETTLEMENT_MAX_BYTES);
    SettleCommand::decode(&encoded).unwrap();
    client.settle(&largest_report).await.unwrap();
    let large_snapshot = attempt(data.clone(), largest_report);
    let expected = exact(&large_snapshot);
    assert!(expected.len() > 9 * 1024 * 1024);
    assert!(expected.len() < RESPONSE_MAX_BYTES);
    mock.set("attempt", Ok(large_snapshot));
    assert_eq!(
        exact(
            client
                .inspect_attempt(&scope(), "..", &owner().attempt_id)
                .await
                .unwrap()
        ),
        expected
    );
    for (path, mut body, limit) in [
        ("tasks", exact(submit(data)), SUBMISSION_MAX_BYTES),
        ("settlements", encoded, SETTLEMENT_MAX_BYTES),
    ] {
        body.resize(limit, b' ');
        assert_eq!(
            raw.post(format!("{}/v1/{path}", running.url))
                .header("Content-Type", "application/json")
                .body(body.clone())
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        body.push(b' ');
        assert_eq!(
            raw.post(format!("{}/v1/{path}", running.url))
                .header("Content-Type", "application/json")
                .body(body.clone())
                .send()
                .await
                .unwrap()
                .status(),
            413
        );
        // Both exact and one-byte-over limits must work without Content-Length.
        assert_eq!(
            chunked_status(&running.url, path, &body[..limit]).await,
            200
        );
        assert_eq!(chunked_status(&running.url, path, &body).await, 413);
    }
}

#[tokio::test]
async fn one_stalled_consumer_does_not_serialize_an_independent_renewal() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let first_received = Arc::new(tokio::sync::Notify::new());
    let observed = first_received.clone();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        read_request(&mut first).await;
        observed.notify_one();
        let (mut second, _) = listener.accept().await.unwrap();
        read_request(&mut second).await;
        second
            .write_all(&response(200, "application/json", &exact(authority())))
            .await
            .unwrap();
        second.shutdown().await.unwrap();
        let mut closed = [0u8; 1];
        let _ = first.read(&mut closed).await;
    });
    let client = HttpTaskService::with_timeout(&url, Duration::from_millis(300)).unwrap();
    let other = client.clone();
    let acquisition = tokio::spawn(async move { other.acquire(&acquisition()).await });
    first_received.notified().await;
    let renewal = client
        .renew(&RenewCommand {
            owner: owner(),
            sequence: 1,
            intent: RenewIntent::KeepAlive,
        })
        .await
        .unwrap();
    assert_eq!(renewal, authority());
    assert!(matches!(
        acquisition.await.unwrap(),
        Err(ContractError::Unavailable(_))
    ));
    server.await.unwrap();
}

#[test]
fn client_configuration_rejects_unusable_urls_and_unbounded_budgets() {
    for url in [
        "file:///tmp/tasks",
        "https://user:password@example.com",
        "http://example.com?query=x",
        "http://example.com#fragment",
        "not a URL",
    ] {
        assert!(HttpTaskService::new(url).is_err());
    }
    for duration in [Duration::ZERO, Duration::from_secs(31)] {
        assert!(HttpTaskService::with_timeout("http://127.0.0.1", duration).is_err());
    }
    assert!(HttpTaskService::new("https://example.com/base/").is_ok());
}

#[tokio::test]
async fn invalid_response_application_depth_is_not_accepted_by_inspection() {
    let mut data = Value::Null;
    for _ in 0..65 {
        data = json!([data]);
    }
    for (route, body) in [
        ("task", exact(task(data.clone()))),
        ("attempt", exact(attempt(Value::Null, settlement(data)))),
    ] {
        let (running, _) = raw_response(response(200, "application/json", &body), None).await;
        let client = HttpTaskService::new(&running.url).unwrap();
        let error = if route == "task" {
            client.inspect(&scope(), "..").await.unwrap_err()
        } else {
            client
                .inspect_attempt(&scope(), "..", &owner().attempt_id)
                .await
                .unwrap_err()
        };
        assert!(matches!(error, ContractError::Unavailable(_)));
    }
}

#[tokio::test]
async fn unsupported_request_media_and_duplicate_encoding_are_rejected_before_mutation() {
    let mock = Arc::new(Mock::default());
    let running = start(server::router(mock.clone())).await;
    let client = reqwest::Client::new();
    for content_type in [
        "text/plain",
        "application/json; charset=iso-8859-1",
        "application/json; bad=value",
        "application/json; charset=utf-8; charset=utf-8",
    ] {
        let response = client
            .post(format!("{}/v1/acquisitions", running.url))
            .header("content-type", content_type)
            .body(exact(acquisition()))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 415);
    }
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        "content-type",
        reqwest::header::HeaderValue::from_static("application/json"),
    );
    headers.append(
        "content-encoding",
        reqwest::header::HeaderValue::from_static("identity"),
    );
    headers.append(
        "content-encoding",
        reqwest::header::HeaderValue::from_static("gzip"),
    );
    assert_eq!(
        client
            .post(format!("{}/v1/acquisitions", running.url))
            .headers(headers)
            .body(exact(acquisition()))
            .send()
            .await
            .unwrap()
            .status(),
        415
    );
    assert!(mock.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn missing_diagnostic_request_id_does_not_invalidate_accepted_result() {
    let body = exact(SettleReply {
        receipt: receipt(),
        already_accepted: true,
        task_state: TaskState::Succeeded,
    });
    let bytes = String::from_utf8(response(200, "application/json", &body))
        .unwrap()
        .replace("Request-Id: proxy_request\r\n", "")
        .into_bytes();
    let (running, _) = raw_response(bytes, None).await;
    let observed = Arc::new(AtomicBool::new(false));
    let notified = observed.clone();
    let client = HttpTaskService::new(&running.url)
        .unwrap()
        .with_observer(move |metadata| {
            assert!(metadata.request_id.is_none());
            assert_eq!(metadata.status, Some(200));
            notified.store(true, Ordering::SeqCst);
        });
    assert!(
        client
            .settle(&settlement(Value::Null))
            .await
            .unwrap()
            .already_accepted
    );
    assert!(observed.load(Ordering::SeqCst));
}

async fn read_request(stream: &mut tokio::net::TcpStream) {
    let mut bytes = Vec::new();
    loop {
        let mut chunk = [0; 4096];
        let read = stream.read(&mut chunk).await.unwrap();
        assert_ne!(read, 0, "request ended before the complete body");
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            let headers = std::str::from_utf8(&bytes[..end]).unwrap();
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            if bytes.len() >= end + 4 + length {
                return;
            }
        }
    }
}

async fn chunked_status(url: &str, path: &str, body: &[u8]) -> u16 {
    let address = url.strip_prefix("http://").unwrap();
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let headers = format!(
        "POST /v1/{path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes()).await.unwrap();
    let _ = stream.write_all(body).await;
    let _ = stream.write_all(b"\r\n0\r\n\r\n").await;
    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response).await;
    std::str::from_utf8(&response)
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn exact_response_budget_includes_whitespace_with_or_without_content_length() {
    let mut body = exact(AcquireReply::Empty { sequence: 1 });
    body.resize(RESPONSE_MAX_BYTES, b' ');
    for length_header in [true, false] {
        let mut bytes = response(200, "application/json", &body);
        if !length_header {
            let prefix = String::from_utf8(bytes)
                .unwrap()
                .replace(&format!("Content-Length: {}\r\n", body.len()), "");
            bytes = prefix.into_bytes();
        }
        let (running, _) = raw_response(bytes, None).await;
        assert!(matches!(
            HttpTaskService::new(&running.url)
                .unwrap()
                .acquire(&acquisition())
                .await
                .unwrap(),
            AcquireReply::Empty { sequence: 1 }
        ));
    }
}

#[derive(Default)]
struct ExchangeTraceBridge {
    next: AtomicUsize,
    parents: Mutex<Vec<Option<TraceContext>>>,
}
impl ledgence_worker_api::TraceBridge for ExchangeTraceBridge {
    fn set_parent(&self, _: &tracing::Span, parent: Option<&TraceContext>) {
        self.parents.lock().unwrap().push(parent.cloned());
    }
    fn add_link(&self, _: &tracing::Span, _: &TraceContext) {
        panic!("HTTP exchanges do not add causal links");
    }
    fn context(&self, _: &tracing::Span) -> Option<TraceContext> {
        let id = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        Some(TraceContext {
            traceparent: format!("00-4bf92f3577b34da6a3ce929d0e0e4736-{id:016x}-00"),
            tracestate: Some("transport=unsampled".into()),
        })
    }
}

#[tokio::test]
async fn tracing_bridge_propagates_a_new_exchange_without_rewriting_command_origins() {
    let mock = Arc::new(Mock::default());
    mock.set("submit", Ok(task(Value::Null)));
    let bridge = Arc::new(ExchangeTraceBridge::default());
    let running = start(server::router_with_observability(
        mock.clone(),
        Arc::new(AtomicBool::new(false)),
        bridge.clone(),
    ))
    .await;
    let client = HttpTaskService::new(&running.url)
        .unwrap()
        .with_trace_bridge(bridge.clone());
    let mut command = submit(json!({"caller": "owned"}));
    command.origin_trace = Some(TraceContext {
        traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into(),
        tracestate: Some("origin=immutable".into()),
    });
    client.submit(&command).await.unwrap();
    client.submit(&command).await.unwrap();
    let disabled = HttpTaskService::new(&running.url).unwrap();
    disabled.submit(&command).await.unwrap();
    let parents = bridge.parents.lock().unwrap();
    assert_eq!(parents.len(), 3);
    let first = parents[0].as_ref().unwrap();
    let second = parents[1].as_ref().unwrap();
    assert_ne!(first.traceparent, second.traceparent);
    assert!(first.traceparent.ends_with("-00"));
    assert_eq!(first.tracestate.as_deref(), Some("transport=unsampled"));
    assert_eq!(
        parents[2], None,
        "no-op client must not invent transport IDs"
    );
    assert_eq!(bridge.next.load(Ordering::Relaxed), 2);
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert!(
        calls
            .iter()
            .all(|(_, value)| *value == serde_json::to_value(&command).unwrap())
    );
}
