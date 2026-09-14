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
    acquisition_options: Mutex<Vec<AcquireOptions>>,
    acquisition_gate: Mutex<Option<Arc<tokio::sync::Notify>>>,
    acquisition_entered: tokio::sync::Notify,
    active_acquisitions: AtomicUsize,
    acquisition_left: tokio::sync::Notify,
}
struct ActiveAcquisition<'a>(&'a Mock);
impl Drop for ActiveAcquisition<'_> {
    fn drop(&mut self) {
        self.0.active_acquisitions.fetch_sub(1, Ordering::SeqCst);
        self.0.acquisition_left.notify_one();
    }
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
    fn list_tasks<'a>(
        &'a self,
        scope: &'a Scope,
        query: &'a TaskListQuery,
    ) -> ContractFuture<'a, TaskPage> {
        Box::pin(async move { self.reply("list", json!({"scope":scope,"query":query})) })
    }
    fn inspect<'a>(&'a self, scope: &'a Scope, task: &'a str) -> ContractFuture<'a, TaskSnapshot> {
        Box::pin(async move { self.reply("inspect", json!([scope, task])) })
    }
    fn status<'a>(&'a self, scope: &'a Scope, task_id: &'a str) -> ContractFuture<'a, TaskStatus> {
        Box::pin(async move { self.reply("status", json!({"scope":scope,"task_id":task_id})) })
    }
    fn result<'a>(&'a self, scope: &'a Scope, task_id: &'a str) -> ContractFuture<'a, TaskResult> {
        Box::pin(async move { self.reply("result", json!({"scope":scope,"task_id":task_id})) })
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
    fn acquire<'a>(
        &'a self,
        command: &'a AcquireCommand,
        options: AcquireOptions,
    ) -> ContractFuture<'a, AcquireReply> {
        Box::pin(async move {
            self.active_acquisitions.fetch_add(1, Ordering::SeqCst);
            let _active = ActiveAcquisition(self);
            self.acquisition_options.lock().unwrap().push(options);
            self.acquisition_entered.notify_one();
            let gate = self.acquisition_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.notified().await;
            }
            self.reply("acquire", command)
        })
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
fn immediate() -> AcquireOptions {
    AcquireOptions::for_wait(Duration::ZERO).unwrap()
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
        exact(client.acquire(&acquisition(), immediate()).await.unwrap()),
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
        assert_eq!(
            client
                .acquire(&acquisition(), immediate())
                .await
                .unwrap_err(),
            error
        );
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
            exact(client.acquire(&acquisition(), immediate()).await.unwrap()),
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
        for route in ["inspect", "status", "result"] {
            assert_eq!(
                client
                    .get(format!("{}/v1/tasks/{route}?{query}", running.url))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                400,
                "{query}"
            );
        }
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
        .acquire(&acquisition(), immediate())
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
                .acquire(&acquisition(), immediate())
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
        let error = HttpTaskService::new(&running.url).unwrap().acquire(&acquisition(), immediate()).await.unwrap_err();
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
        assert!(matches!(HttpTaskService::new(&running.url).unwrap().acquire(&acquisition(), immediate()).await, Err(ContractError::Unavailable(_))));
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
    let acquisition = tokio::spawn(async move { other.acquire(&acquisition(), immediate()).await });
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
                .acquire(&acquisition(), immediate())
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

#[tokio::test]
async fn acquisition_wait_is_strict_and_separate_from_durable_identity() {
    let mock = Arc::new(Mock::default());
    mock.set("acquire", Ok(AcquireReply::Empty { sequence: 1 }));
    let running = start(server::router(mock.clone())).await;
    let client = reqwest::Client::new();
    let command = serde_json::to_value(acquisition()).unwrap();
    for wait in [None, Some(0), Some(1), Some(LONG_POLL_WAIT_MS)] {
        let mut body = command.clone();
        if let Some(wait) = wait {
            body["wait_ms"] = json!(wait);
        }
        let response = client
            .post(format!("{}/v1/acquisitions", running.url))
            .header("content-type", "application/json")
            .body(exact(body))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            serde_json::from_slice::<Value>(&response.bytes().await.unwrap()).unwrap(),
            json!({"disposition":"empty","sequence":1})
        );
        let options = *mock.acquisition_options.lock().unwrap().last().unwrap();
        assert_eq!(options.max_wait, Duration::from_millis(wait.unwrap_or(0)));
        assert_eq!(mock.calls.lock().unwrap().last().unwrap().1, command);
    }
    let base = String::from_utf8(exact(&command)).unwrap();
    let prefix = base.strip_suffix('}').unwrap();
    for fields in [
        "\"wait_ms\":null",
        "\"wait_ms\":-1",
        "\"wait_ms\":-0",
        "\"wait_ms\":1.0",
        "\"wait_ms\":1e0",
        "\"wait_ms\":\"1\"",
        "\"wait_ms\":true",
        "\"wait_ms\":[]",
        "\"wait_ms\":{}",
        "\"wait_ms\":20001",
        "\"wait_ms\":18446744073709551615",
        "\"wait_ms\":18446744073709551616",
        "\"wait_ms\":0,\"wait_ms\":0",
        "\"wait_ms\":0,\"unknown\":1",
    ] {
        let response = client
            .post(format!("{}/v1/acquisitions", running.url))
            .header("content-type", "application/json")
            .body(format!("{prefix},{fields}}}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400, "accepted {fields}");
        assert!(response.headers().contains_key("request-id"));
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert!(matches!(
            serde_json::from_slice::<ContractError>(&response.bytes().await.unwrap()).unwrap(),
            ContractError::InvalidInput(_)
        ));
    }
    assert_eq!(mock.calls.lock().unwrap().len(), 4);
    assert_eq!(mock.acquisition_options.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn immediate_client_omits_wait_and_remains_compatible_with_strict_legacy_server() {
    async fn legacy(body: axum::body::Bytes) -> axum::response::Response {
        use axum::{http::StatusCode, response::IntoResponse};
        match decode_unique_json::<AcquireCommand>(&body, SUBMISSION_MAX_BYTES) {
            Ok(command) => (
                StatusCode::OK,
                [("content-type", "application/json")],
                exact(AcquireReply::Empty {
                    sequence: command.sequence,
                }),
            )
                .into_response(),
            Err(_) => (
                StatusCode::BAD_REQUEST,
                [("content-type", "application/json")],
                exact(ContractError::InvalidInput(
                    "legacy strict acquisition".into(),
                )),
            )
                .into_response(),
        }
    }
    let running =
        start(axum::Router::new().route("/v1/acquisitions", axum::routing::post(legacy))).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    assert!(matches!(
        client.acquire(&acquisition(), immediate()).await.unwrap(),
        AcquireReply::Empty { sequence: 1 }
    ));
    // Do not silently downgrade a rejected wait preference or retry the command.
    assert!(matches!(
        client
            .acquire(
                &acquisition(),
                AcquireOptions::for_wait(Duration::from_millis(1)).unwrap(),
            )
            .await,
        Err(ContractError::InvalidInput(_))
    ));
}

#[tokio::test]
async fn acquisition_client_sends_wait_without_rewriting_command_or_reply() {
    let mock = Arc::new(Mock::default());
    mock.set("acquire", Ok(AcquireReply::Empty { sequence: 1 }));
    let running = start(server::router(mock.clone())).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    for wait in [
        Duration::ZERO,
        Duration::from_millis(1),
        Duration::from_secs(20),
    ] {
        let options = AcquireOptions::for_wait(wait).unwrap();
        assert!(matches!(
            client.acquire(&acquisition(), options).await.unwrap(),
            AcquireReply::Empty { sequence: 1 }
        ));
        assert_eq!(
            mock.acquisition_options
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .max_wait,
            wait
        );
    }
    assert!(
        mock.calls
            .lock()
            .unwrap()
            .iter()
            .all(|(kind, command)| *kind == "acquire"
                && *command == serde_json::to_value(acquisition()).unwrap())
    );
}

#[tokio::test]
async fn acquisition_deadline_covers_the_whole_response_transfer() {
    let (running, count) = raw_response(
        response(
            200,
            "application/json",
            &exact(AcquireReply::Empty { sequence: 1 }),
        ),
        Some(Duration::from_secs(1)),
    )
    .await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let capture = observed.clone();
    let client = HttpTaskService::new(&running.url)
        .unwrap()
        .with_observer(move |metadata| capture.lock().unwrap().push(metadata.clone()));
    let options = AcquireOptions::new(
        Duration::from_secs(20),
        std::time::Instant::now() + Duration::from_millis(100),
    )
    .unwrap();
    let result = tokio::time::timeout(
        Duration::from_millis(750),
        client.acquire(&acquisition(), options),
    )
    .await
    .expect("caller deadline must cap the configured thirty-second client timeout");
    assert!(matches!(result, Err(ContractError::Unavailable(_))));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let observed = observed.lock().unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].status, Some(200));
}

#[tokio::test]
async fn expired_or_invalid_acquisition_options_do_not_send_a_request() {
    let (running, count) = raw_response(
        response(
            200,
            "application/json",
            &exact(AcquireReply::Empty { sequence: 1 }),
        ),
        None,
    )
    .await;
    let client = HttpTaskService::new(&running.url).unwrap();
    let expired = AcquireOptions::immediate(std::time::Instant::now());
    assert!(matches!(
        client.acquire(&acquisition(), expired).await,
        Err(ContractError::Unavailable(_))
    ));
    let invalid = AcquireOptions {
        max_wait: Duration::from_millis(LONG_POLL_WAIT_MS + 1),
        deadline: std::time::Instant::now() + Duration::from_secs(30),
    };
    assert!(matches!(
        client.acquire(&acquisition(), invalid).await,
        Err(ContractError::InvalidInput(_))
    ));
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn waiting_acquisitions_release_json_capacity_for_control_calls() {
    let mock = Arc::new(Mock::default());
    mock.set("acquire", Ok(AcquireReply::Empty { sequence: 1 }));
    mock.set("renew", Ok(authority()));
    let gate = Arc::new(tokio::sync::Notify::new());
    *mock.acquisition_gate.lock().unwrap() = Some(gate.clone());
    let running = start(server::router(mock.clone())).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    let mut acquisitions = Vec::new();
    for consumer in 0..8 {
        let client = client.clone();
        acquisitions.push(tokio::spawn(async move {
            let mut command = acquisition();
            command.consumer_id = consumer;
            client
                .acquire(
                    &command,
                    AcquireOptions::for_wait(Duration::from_secs(20)).unwrap(),
                )
                .await
        }));
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if mock.acquisition_options.lock().unwrap().len() == 8 {
                break;
            }
            mock.acquisition_entered.notified().await;
        }
    })
    .await
    .expect("all waiters must enter without retaining the four JSON permits");
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        client.renew(&RenewCommand {
            owner: owner(),
            sequence: 1,
            intent: RenewIntent::KeepAlive,
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result, authority());
    gate.notify_waiters();
    for acquisition in acquisitions {
        assert!(matches!(
            acquisition.await.unwrap().unwrap(),
            AcquireReply::Empty { sequence: 1 }
        ));
    }
}

#[derive(Default)]
struct EntryCapture {
    entered: tokio::sync::Notify,
    observed: Mutex<Option<std::time::Instant>>,
}
impl ledgence_worker_api::TraceBridge for EntryCapture {
    fn set_parent(&self, _: &tracing::Span, _: Option<&TraceContext>) {
        *self.observed.lock().unwrap() = Some(std::time::Instant::now());
        self.entered.notify_one();
    }
    fn add_link(&self, _: &tracing::Span, _: &TraceContext) {}
    fn context(&self, _: &tracing::Span) -> Option<TraceContext> {
        None
    }
}

#[tokio::test]
async fn server_acquisition_deadline_starts_before_request_body_transfer() {
    let mock = Arc::new(Mock::default());
    mock.set("acquire", Ok(AcquireReply::Empty { sequence: 1 }));
    let entry = Arc::new(EntryCapture::default());
    let running = start(server::router_with_observability(
        mock.clone(),
        Arc::new(AtomicBool::new(false)),
        entry.clone(),
    ))
    .await;
    let mut stream = tokio::net::TcpStream::connect(running.url.strip_prefix("http://").unwrap())
        .await
        .unwrap();
    let mut body = serde_json::to_value(acquisition()).unwrap();
    body["wait_ms"] = json!(LONG_POLL_WAIT_MS);
    let bytes = exact(body);
    let before = std::time::Instant::now();
    stream.write_all(format!(
        "POST /v1/acquisitions HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        bytes.len()
    ).as_bytes()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), entry.entered.notified())
        .await
        .unwrap();
    let entered = entry.observed.lock().unwrap().unwrap();
    assert!(mock.acquisition_options.lock().unwrap().is_empty());
    stream.write_all(&bytes).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    assert!(response.starts_with(b"HTTP/1.1 200"));
    let options = mock.acquisition_options.lock().unwrap()[0];
    assert_eq!(options.max_wait, Duration::from_secs(20));
    assert!(options.deadline >= before + Duration::from_secs(30));
    assert!(
        options.deadline <= entered + Duration::from_secs(30),
        "body transfer or decoding restarted the server deadline"
    );
}

#[tokio::test]
async fn disconnected_waiting_http_request_drops_its_service_future() {
    let mock = Arc::new(Mock::default());
    *mock.acquisition_gate.lock().unwrap() = Some(Arc::new(tokio::sync::Notify::new()));
    let running = start(server::router(mock.clone())).await;
    let mut stream = tokio::net::TcpStream::connect(running.url.strip_prefix("http://").unwrap())
        .await
        .unwrap();
    let mut body = serde_json::to_value(acquisition()).unwrap();
    body["wait_ms"] = json!(LONG_POLL_WAIT_MS);
    let bytes = exact(body);
    stream.write_all(format!(
        "POST /v1/acquisitions HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        bytes.len()
    ).as_bytes()).await.unwrap();
    stream.write_all(&bytes).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), mock.acquisition_entered.notified())
        .await
        .unwrap();
    assert_eq!(mock.active_acquisitions.load(Ordering::SeqCst), 1);
    drop(stream);
    tokio::time::timeout(Duration::from_secs(1), mock.acquisition_left.notified())
        .await
        .expect("disconnected request must release its owned service future");
    assert_eq!(mock.active_acquisitions.load(Ordering::SeqCst), 0);
    assert!(
        mock.calls.lock().unwrap().is_empty(),
        "cancellation must not fabricate a completed reply"
    );
}

fn compact_status(state: TaskState) -> TaskStatus {
    let allocated = state != TaskState::Queued;
    TaskStatus {
        scope: scope(),
        task_id: "..".into(),
        run_id: "run".into(),
        queue: "queue".into(),
        correlation_key: None,
        state,
        attempt_count: u32::from(allocated),
        current_attempt_id: (state == TaskState::Active).then(|| "attempt".into()),
        latest_attempt_id: allocated.then(|| "attempt".into()),
        submitted_at: 1,
        available_at: 1,
        terminal_at: state.is_terminal().then_some(2),
        cancel_requested_at: (state == TaskState::Cancelled).then_some(2),
    }
}

#[tokio::test]
async fn task_observation_routes_preserve_pending_null_and_terminal_evidence() {
    let mock = Arc::new(Mock::default());
    let running = start(server::router(mock.clone())).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    let outcomes = [
        (TaskState::Queued, None),
        (TaskState::Active, None),
        (
            TaskState::Succeeded,
            Some(TaskOutcome::Succeeded {
                attempt_id: "attempt".into(),
                quiescence: Quiescence::Unconfirmed,
                execution_may_have_started: true,
                output: Value::Null,
            }),
        ),
        (
            TaskState::Failed,
            Some(TaskOutcome::Failed {
                attempt_id: "attempt".into(),
                quiescence: Quiescence::Unconfirmed,
                execution_may_have_started: true,
                failure: TaskFailure::AttemptLost {},
            }),
        ),
        (TaskState::Cancelled, Some(TaskOutcome::Cancelled {})),
    ];
    for (state, outcome) in outcomes {
        let status = compact_status(state);
        let result = TaskResult {
            task: status.clone(),
            outcome,
        };
        mock.set("status", Ok(&status));
        mock.set("result", Ok(&result));
        assert_eq!(client.status(&scope(), "..").await.unwrap(), status);
        assert_eq!(client.result(&scope(), "..").await.unwrap(), result);
        let mut url = reqwest::Url::parse(&format!("{}/v1/tasks/result", running.url)).unwrap();
        url.query_pairs_mut().extend_pairs([
            ("tenant_id", scope().tenant_id.as_str()),
            ("namespace", scope().namespace.as_str()),
            ("task_id", ".."),
        ]);
        let query = reqwest::Client::new().get(url).send().await.unwrap();
        assert_eq!(query.status(), 200);
        assert_eq!(query.headers()["cache-control"], "no-store");
        assert!(query.headers().contains_key("request-id"));
        assert_eq!(query.bytes().await.unwrap().as_ref(), exact(&result));
    }
    assert_eq!(mock.calls.lock().unwrap().len(), 15);
    for route in ["status", "result"] {
        for error in [
            ContractError::NotFound,
            ContractError::Unavailable("offline".into()),
        ] {
            mock.set::<TaskStatus>(route, Err(error.clone()));
            let actual = if route == "status" {
                client.status(&scope(), "..").await.unwrap_err()
            } else {
                client.result(&scope(), "..").await.unwrap_err()
            };
            assert_eq!(actual, error);
        }
    }
}

#[tokio::test]
async fn result_rejects_fabricated_outcomes_and_response_identity_changes() {
    let pending = json!({"task":compact_status(TaskState::Active), "outcome":null});
    let mut cases = Vec::new();
    let mut premature = pending.clone();
    premature["outcome"] = json!({"kind":"succeeded","attempt_id":"attempt","quiescence":"confirmed","execution_may_have_started":true,"output":null});
    cases.push(premature);
    let mut wrong_task = pending.clone();
    wrong_task["task"]["task_id"] = json!("another");
    cases.push(wrong_task);
    let mut wrong_scope = pending.clone();
    wrong_scope["task"]["scope"]["namespace"] = json!("another");
    cases.push(wrong_scope);
    let mut missing_outcome = pending.clone();
    missing_outcome.as_object_mut().unwrap().remove("outcome");
    cases.push(missing_outcome);
    let mut missing_latest = pending;
    missing_latest["task"]["latest_attempt_id"] = Value::Null;
    cases.push(missing_latest);
    let mut cancelled = json!({"task":compact_status(TaskState::Cancelled),"outcome":{"kind":"cancelled","attempt_id":"attempt"}});
    cases.push(cancelled.clone());
    cancelled["outcome"] = Value::Null;
    cases.push(cancelled);
    for case in cases {
        let (running, _) =
            raw_response(response(200, "application/json", &exact(&case)), None).await;
        let client = HttpTaskService::new(&running.url).unwrap();
        assert!(
            matches!(
                client.result(&scope(), "..").await,
                Err(ContractError::Unavailable(_))
            ),
            "accepted invalid case: {case}"
        );
    }
}

#[tokio::test]
async fn status_has_a_compact_body_bound_and_server_checks_adapter_results() {
    let maximum = ledgence_adapter_http::STATUS_MAX_BYTES;
    let mut status = exact(compact_status(TaskState::Queued));
    status.resize(maximum + 1, b' ');
    for bytes in [
        response(200, "application/json", &status),
        [
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
                .as_slice(),
            status.as_slice(),
        ]
        .concat(),
    ] {
        let (running, _) = raw_response(bytes, None).await;
        assert!(matches!(
            HttpTaskService::new(&running.url)
                .unwrap()
                .status(&scope(), "..")
                .await,
            Err(ContractError::Unavailable(_))
        ));
    }
    let mock = Arc::new(Mock::default());
    let running = start(server::router(mock.clone())).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    let mut invalid = compact_status(TaskState::Queued);
    invalid.task_id = "another".into();
    mock.set("status", Ok(invalid));
    mock.set(
        "result",
        Ok(TaskResult {
            task: compact_status(TaskState::Succeeded),
            outcome: None,
        }),
    );
    assert!(matches!(
        client.status(&scope(), "..").await,
        Err(ContractError::Unavailable(_))
    ));
    assert!(matches!(
        client.result(&scope(), "..").await,
        Err(ContractError::Unavailable(_))
    ));
}

#[test]
fn shared_json_fixtures_preserve_cross_language_values() {
    let fixtures: Vec<Value> =
        serde_json::from_str(include_str!("../../../tests/fixtures/json-values.json")).unwrap();
    for fixture in fixtures {
        let bytes = fixture["json"].as_str().unwrap().as_bytes();
        let decoded: ledgence_worker_api::Result<Value> =
            decode_unique_json(bytes, SUBMISSION_DATA_MAX_BYTES).and_then(|value| {
                ledgence_worker_api::validate_wire_value(&value)?;
                Ok(value)
            });
        if fixture["valid"] == false {
            assert!(decoded.is_err(), "{}", fixture["name"]);
            continue;
        }
        let value = decoded.unwrap_or_else(|error| panic!("{}: {error}", fixture["name"]));
        let kind = match &value {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
            Value::Number(n) if n.is_f64() => "float",
            Value::Number(_) => "integer",
        };
        assert_eq!(
            kind,
            fixture["kind"].as_str().unwrap(),
            "{}",
            fixture["name"]
        );
        if fixture["name"].as_str().unwrap().starts_with("negative-") {
            assert_eq!(value.as_f64().unwrap().to_bits(), (-0.0_f64).to_bits());
        }
        let roundtrip: Value =
            decode_unique_json(&exact(&value), SUBMISSION_DATA_MAX_BYTES).unwrap();
        assert_eq!(value, roundtrip, "{}", fixture["name"]);
    }
}

#[tokio::test]
async fn discovery_round_trips_filters_and_scope_bound_cursor() {
    let mock = Arc::new(Mock::default());
    let running = start(server::router(mock.clone())).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    let query = TaskListQuery {
        filters: TaskFilters {
            state: Some(TaskState::Queued),
            queue: Some("a +%/é".into()),
            correlation_key: Some(String::new()),
            submitted_from: Some(1),
            submitted_until: Some(3),
        },
        limit: 1,
        cursor: None,
    };
    let mut task = compact_status(TaskState::Queued);
    task.queue = query.filters.queue.clone().unwrap();
    task.correlation_key = Some(String::new());
    let cursor = query
        .next_cursor(&scope(), &TaskPosition::from(&task))
        .unwrap();
    let page = TaskPage {
        items: vec![task],
        next_cursor: Some(cursor.clone()),
    };
    mock.set("list", Ok(&page));
    assert_eq!(client.list_tasks(&scope(), &query).await.unwrap(), page);
    assert_eq!(
        mock.calls.lock().unwrap()[0],
        ("list", json!({"scope":scope(),"query":query}))
    );
    let continuation = TaskListQuery {
        limit: 2,
        cursor: Some(cursor),
        ..query.clone()
    };
    mock.set(
        "list",
        Ok(TaskPage {
            items: vec![],
            next_cursor: None,
        }),
    );
    assert!(
        client
            .list_tasks(&scope(), &continuation)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    let mut changed = continuation;
    changed.filters.correlation_key = None;
    assert!(matches!(
        client.list_tasks(&scope(), &changed).await,
        Err(ContractError::InvalidInput(_))
    ));
    assert_eq!(
        mock.calls.lock().unwrap().len(),
        2,
        "bad cursor must not dispatch"
    );
}

#[tokio::test]
async fn discovery_rejects_bad_queries_before_service_and_preserves_method_contract() {
    let mock = Arc::new(Mock::default());
    let running = start(server::router(mock.clone())).await;
    let client = reqwest::Client::new();
    for query in [
        "tenant_id=a",
        "tenant_id=a&namespace=b&task_id=x",
        "tenant_id=a&namespace=b&state=done",
        "tenant_id=a&namespace=b&queue=",
        "tenant_id=a&namespace=b&limit=0",
        "tenant_id=a&namespace=b&limit=101",
        "tenant_id=a&namespace=b&limit=1&limit=2",
        "tenant_id=a&namespace=b&limit=+1",
        "tenant_id=a&namespace=b&submitted_from=-1",
        "tenant_id=a&namespace=b&submitted_from=1.0",
        "tenant_id=a&namespace=b&submitted_from=2&submitted_until=2",
        "tenant_id=a&namespace=b&submitted_until=253402300800000",
        "tenant_id=a&namespace=b&cursor=",
        "tenant_id=a&namespace=b&cursor=aa",
        "tenant_id=a&namespace=b&correlation_key=%00",
        "tenant_id=a&namespace=b&correlation_key=%FF",
        "tenant_id=a&namespace=b&correlation_key=%",
        "tenant_id=a&namespace=b&unknown=x",
    ] {
        let reply = client
            .get(format!("{}/v1/tasks?{query}", running.url))
            .send()
            .await
            .unwrap();
        assert_eq!(reply.status(), 400, "{query}");
        assert_eq!(reply.headers()["cache-control"], "no-store");
        assert!(reply.headers().contains_key("request-id"));
    }
    let reply = client
        .get(format!("{}/v1/tasks?tenant_id=a&namespace=b", running.url))
        .body("{}")
        .send()
        .await
        .unwrap();
    assert_eq!(reply.status(), 400);
    let reply = client
        .put(format!("{}/v1/tasks", running.url))
        .send()
        .await
        .unwrap();
    assert_eq!(reply.status(), 405);
    assert_eq!(reply.headers()["allow"], "GET, POST");
    assert!(mock.calls.lock().unwrap().is_empty());
    mock.set(
        "list",
        Ok(TaskPage {
            items: vec![],
            next_cursor: None,
        }),
    );
    let reply = client
        .get(format!("{}/v1/tasks?tenant_id=a&namespace=b", running.url))
        .send()
        .await
        .unwrap();
    assert_eq!(reply.status(), 200);
    assert_eq!(reply.headers()["cache-control"], "no-store");
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls[0].1["query"]["limit"], 50);
}

#[tokio::test]
async fn discovery_client_and_server_reject_inconsistent_pages() {
    let query = TaskListQuery::default();
    let task = compact_status(TaskState::Queued);
    let mut wrong_scope = task.clone();
    wrong_scope.scope.namespace = "another".into();
    let mut older = task.clone();
    older.task_id = "z".into();
    let cases = vec![
        json!({"items":[task.clone()]}),
        json!({"items":[],"next_cursor":null,"total":0}),
        json!({"items":[wrong_scope],"next_cursor":null}),
        json!({"items":[task.clone(),task.clone()],"next_cursor":null}),
        json!({"items":[task.clone(),older],"next_cursor":null}),
        json!({"items":[],"next_cursor":"aa"}),
        json!({"items":vec![task.clone();101],"next_cursor":null}),
    ];
    for case in cases {
        let (running, _) =
            raw_response(response(200, "application/json", &exact(&case)), None).await;
        assert!(
            matches!(
                HttpTaskService::new(&running.url)
                    .unwrap()
                    .list_tasks(&scope(), &query)
                    .await,
                Err(ContractError::Unavailable(_))
            ),
            "accepted {case}"
        );
    }
    let mock = Arc::new(Mock::default());
    mock.set(
        "list",
        Ok(TaskPage {
            items: vec![task.clone(), task],
            next_cursor: None,
        }),
    );
    let running = start(server::router(mock)).await;
    assert!(matches!(
        HttpTaskService::new(&running.url)
            .unwrap()
            .list_tasks(&scope(), &query)
            .await,
        Err(ContractError::Unavailable(_))
    ));
}

#[tokio::test]
async fn discovery_enforces_streamed_and_declared_page_body_limit() {
    let mut page = exact(json!({"items":[],"next_cursor":null}));
    page.resize(TASK_PAGE_MAX_BYTES + 1, b' ');
    for bytes in [
        response(200, "application/json", &page),
        [
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
                .as_slice(),
            page.as_slice(),
        ]
        .concat(),
    ] {
        let (running, _) = raw_response(bytes, None).await;
        assert!(matches!(
            HttpTaskService::new(&running.url)
                .unwrap()
                .list_tasks(&scope(), &TaskListQuery::default())
                .await,
            Err(ContractError::Unavailable(_))
        ));
    }
}

#[tokio::test]
async fn discovery_round_trips_maximum_escaped_metadata_and_large_cursor() {
    let mock = Arc::new(Mock::default());
    let raw_queries = Arc::new(Mutex::new(Vec::<String>::new()));
    let captured = raw_queries.clone();
    let router = server::router(mock.clone()).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let captured = captured.clone();
            async move {
                captured
                    .lock()
                    .unwrap()
                    .push(request.uri().query().unwrap_or_default().into());
                next.run(request).await
            }
        },
    ));
    let running = start(router).await;
    let client = HttpTaskService::new(&running.url).unwrap();
    // Every source byte needs escaping in JSON and percent-encoding in a URL.
    let identifier = "\"\\".repeat(64);
    let correlation = "\"\\".repeat(256);
    assert_eq!(identifier.len(), 128);
    assert_eq!(correlation.len(), 512);
    let scope = Scope {
        tenant_id: identifier.clone(),
        namespace: identifier.clone(),
    };
    let query = TaskListQuery {
        filters: TaskFilters {
            state: Some(TaskState::Queued),
            queue: Some(identifier.clone()),
            correlation_key: Some(correlation.clone()),
            submitted_from: Some(0),
            submitted_until: Some(253_402_300_799_999),
        },
        limit: 1,
        cursor: None,
    };
    let mut task = compact_status(TaskState::Queued);
    task.scope = scope.clone();
    task.task_id = identifier;
    task.queue = query.filters.queue.clone().unwrap();
    task.correlation_key = Some(correlation);
    task.validate().unwrap();
    let cursor = query
        .next_cursor(&scope, &TaskPosition::from(&task))
        .unwrap();
    assert!(
        cursor.len() > 4096,
        "the regression must exercise a cursor larger than 4 KiB"
    );
    assert!(cursor.len() <= TASK_CURSOR_MAX_BYTES);
    let first = TaskPage {
        items: vec![task],
        next_cursor: Some(cursor.clone()),
    };
    mock.set("list", Ok(&first));
    assert_eq!(client.list_tasks(&scope, &query).await.unwrap(), first);

    let continuation = TaskListQuery {
        cursor: Some(cursor.clone()),
        limit: 100,
        ..query.clone()
    };
    let empty = TaskPage {
        items: Vec::new(),
        next_cursor: None,
    };
    mock.set("list", Ok(&empty));
    assert_eq!(
        client.list_tasks(&scope, &continuation).await.unwrap(),
        empty
    );
    let calls = mock.calls.lock().unwrap();
    assert_eq!(
        calls.as_slice(),
        &[
            ("list", json!({"scope":scope,"query":query})),
            ("list", json!({"scope":scope,"query":continuation})),
        ]
    );
    let raw_queries = raw_queries.lock().unwrap();
    assert_eq!(raw_queries.len(), 2);
    assert!(raw_queries[1].contains(&format!("cursor={cursor}")));
    assert!(raw_queries[1].contains("%22%5C"));
    assert!(raw_queries[1].len() > cursor.len());
    assert!(raw_queries.iter().all(|query| query.len() < 16 * 1024));
}
