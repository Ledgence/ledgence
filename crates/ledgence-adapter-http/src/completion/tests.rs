use super::*;
use ledgence_orchestration_api::*;
use ledgence_worker_api::TraceContext;
use serde_json::json;
use std::sync::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

fn fixture() -> CompletionLease {
    let scope = Scope {
        tenant_id: "acme".into(),
        namespace: "billing".into(),
    };
    let target = CompletionTarget::Task {
        id: "task_example".into(),
    };
    let event = CompletionEvent::new(json!({
        "specversion":"1.0", "id":completion_event_id(&target),
        "source":"urn:ledgence:orchestrator", "type":"com.ledgence.task.completed.v1",
        "subject":"tasks/task_example", "time":"2026-09-22T00:00:00Z", "ldgtenantid":"acme",
        "ldgnamespace":"billing", "ldgstate":"succeeded", "ldgtaskid":"task_example",
        "ldgrunid":"run_example", "ldgresultref":completion_result_ref(&scope, &target),
        "traceparent":"00-11111111111111111111111111111111-2222222222222222-01"
    }))
    .unwrap();
    let lease = CompletionLease {
        event_bytes: serde_json::to_vec_pretty(&event).unwrap(),
        lease_token: "lease_example".into(),
        subscription: CompletionSubscription {
            subscription_id: "sub_example".into(),
            command: CompletionSubscribeCommand {
                scope,
                target,
                destination: "billing-receiver".into(),
                idempotency_key: "subscription-key".into(),
            },
            state: CompletionState::Delivering,
            generation: 1,
            attempts: 1,
            total_attempts: 1,
            created_at: 1,
            activated_at: Some(2),
            next_attempt_at: None,
            lease_expires_at: Some(30_002),
            delivered_at: None,
            exhausted_at: None,
            last_failure: None,
            event: Some(event),
        },
    };
    lease.validate().unwrap();
    lease
}
fn configuration(url: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"destinations":[{"scope":{"tenant_id":"acme","namespace":"billing"},"destination":"billing-receiver","url":url}]})).unwrap()
}
fn sender(url: &str) -> HttpCompletionSender {
    HttpCompletionSender::new(
        CompletionConfig::decode(&configuration(url))
            .unwrap()
            .destinations
            .remove(0),
    )
    .unwrap()
}
async fn request(socket: &mut TcpStream) -> (String, Vec<u8>) {
    let mut bytes = Vec::new();
    loop {
        let mut chunk = [0; 4096];
        let received = socket.read(&mut chunk).await.unwrap();
        assert!(received > 0, "request closed before complete body");
        bytes.extend_from_slice(&chunk[..received]);
        assert!(bytes.len() < 64 * 1024);
        if let Some(split) = bytes.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            let headers = String::from_utf8(bytes[..split].to_vec()).unwrap();
            let length = headers
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            if bytes.len() >= split + 4 + length {
                return (headers, bytes[split + 4..split + 4 + length].to_vec());
            }
        }
    }
}

#[test]
fn config_is_strict_bounded_scoped_and_canonical() {
    let valid = configuration("http://localhost:9000/results?key=opaque");
    let configured = CompletionConfig::decode(&valid)
        .unwrap()
        .destinations
        .remove(0);
    assert_eq!(
        configured.destination.binding,
        r#"{"kind":"http","url":"http://localhost:9000/results?key=opaque"}"#
    );
    let mut scoped: serde_json::Value = serde_json::from_slice(&valid).unwrap();
    let another = scoped["destinations"][0].clone();
    scoped["destinations"].as_array_mut().unwrap().push(another);
    assert!(CompletionConfig::decode(&serde_json::to_vec(&scoped).unwrap()).is_err());
    scoped["destinations"][1]["scope"]["tenant_id"] = json!("another");
    CompletionConfig::decode(&serde_json::to_vec(&scoped).unwrap()).unwrap();
    for url in [
        "file:///tmp/result",
        "https://name:secret@example.com/a",
        "https://example.com/a#secret",
        "https://example.com/ ",
    ] {
        assert!(CompletionConfig::decode(&configuration(url)).is_err());
    }
    for text in [
        "{}",
        r#"{"destinations":[]}"#,
        r#"{"destinations":[],"destinations":[]}"#,
    ] {
        assert!(CompletionConfig::decode(text.as_bytes()).is_err());
    }
    let mut unknown: serde_json::Value = serde_json::from_slice(&valid).unwrap();
    unknown["destinations"][0]["secret"] = json!("not-supported");
    assert!(CompletionConfig::decode(&serde_json::to_vec(&unknown).unwrap()).is_err());
    let mut huge = valid;
    huge.resize(CONFIG_MAX_BYTES, b' ');
    CompletionConfig::decode(&huge).unwrap();
    huge.push(b' ');
    assert!(CompletionConfig::decode(&huge).is_err());
    assert!(CompletionConfig::load(&std::env::temp_dir()).is_err());
    let mut many: serde_json::Value =
        serde_json::from_slice(&configuration("http://localhost/")).unwrap();
    many["destinations"]=json!((0..17).map(|index|json!({"scope":{"tenant_id":"acme","namespace":"billing"},"destination":format!("target-{index}"),"url":"http://localhost/"})).collect::<Vec<_>>());
    assert!(CompletionConfig::decode(&serde_json::to_vec(&many).unwrap()).is_err());
}

#[derive(Default)]
struct Bridge {
    links: Mutex<Vec<TraceContext>>,
}
impl TraceBridge for Bridge {
    fn set_parent(&self, _: &tracing::Span, parent: Option<&TraceContext>) {
        assert!(parent.is_none());
    }
    fn add_link(&self, _: &tracing::Span, context: &TraceContext) {
        self.links.lock().unwrap().push(context.clone());
    }
    fn context(&self, _: &tracing::Span) -> Option<TraceContext> {
        Some(TraceContext {
            traceparent: "00-33333333333333333333333333333333-4444444444444444-01".into(),
            tracestate: Some("vendor=value".into()),
        })
    }
}

#[tokio::test]
async fn sends_exact_event_and_separate_delivery_trace_without_waiting_for_response_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/callback", listener.local_addr().unwrap());
    let (release, wait) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let captured = request(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 202 Accepted\r\nContent-Length: 999999999\r\n\r\n")
            .await
            .unwrap();
        let _ = wait.await;
        captured
    });
    let bridge = Arc::new(Bridge::default());
    let sender = sender(&url).with_trace_bridge(bridge.clone());
    let lease = fixture();
    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        sender.deliver(&lease, Instant::now() + Duration::from_secs(2)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(outcome, CompletionDeliveryOutcome::Confirmed);
    release.send(()).unwrap();
    let (headers, body) = server.await.unwrap();
    assert_eq!(body, lease.event_bytes);
    let headers = headers.to_ascii_lowercase();
    assert!(headers.contains("content-type: application/cloudevents+json"));
    assert!(headers.contains("ledgence-subscription-id: sub_example"));
    assert!(headers.contains("ledgence-delivery-generation: 1"));
    assert!(headers.contains("ledgence-delivery-attempt: 1"));
    assert!(
        headers.contains("traceparent: 00-33333333333333333333333333333333-4444444444444444-01")
    );
    assert!(headers.contains("tracestate: vendor=value"));
    assert_eq!(
        bridge.links.lock().unwrap()[0],
        lease
            .subscription
            .event
            .as_ref()
            .unwrap()
            .trace_context()
            .unwrap()
    );
}

#[tokio::test]
async fn ambiguous_reply_and_manual_retry_keep_identical_event_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sender = sender(&format!(
        "http://{}/callback",
        listener.local_addr().unwrap()
    ));
    let server = tokio::spawn(async move {
        let mut bodies = Vec::new();
        for iteration in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            bodies.push(request(&mut socket).await.1);
            if iteration == 1 {
                socket
                    .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
            }
        }
        bodies
    });
    let mut lease = fixture();
    assert!(matches!(
        sender
            .deliver(&lease, Instant::now() + Duration::from_secs(2))
            .await
            .unwrap(),
        CompletionDeliveryOutcome::Retry { .. }
    ));
    lease.subscription.attempts = 2;
    lease.subscription.total_attempts = 2;
    lease.lease_token = "lease_second".into();
    assert_eq!(
        sender
            .deliver(&lease, Instant::now() + Duration::from_secs(2))
            .await
            .unwrap(),
        CompletionDeliveryOutcome::Confirmed
    );
    assert_eq!(
        server.await.unwrap(),
        vec![lease.event_bytes.clone(), lease.event_bytes]
    );
}

#[tokio::test]
async fn redirects_errors_and_large_headers_retry_with_bounded_retry_after() {
    for (response,reason,delay) in [
        ("HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/secret\r\nContent-Length: 0\r\n\r\n".into(),"http_302",None),
        ("HTTP/1.1 429 Too Many Requests\r\nRetry-After: 9\r\nContent-Length: 0\r\n\r\n".into(),"http_429",Some(9000)),
        ("HTTP/1.1 503 Unavailable\r\nRetry-After: 18446744073709551615\r\nContent-Length: 0\r\n\r\n".into(),"http_503",Some(300000)),
        (format!("HTTP/1.1 200 OK\r\nX-Padding: {}\r\nContent-Length: 0\r\n\r\n","x".repeat(RESPONSE_HEADER_MAX_BYTES)),"response_headers_exceeded",None),
    ] {
        let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
        let sender=sender(&format!("http://{}/callback",listener.local_addr().unwrap()));
        let server=tokio::spawn(async move {
            let(mut socket,_)=listener.accept().await.unwrap();
            request(&mut socket).await;
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        assert_eq!(sender.deliver(&fixture(),Instant::now()+Duration::from_secs(2)).await.unwrap(),retry(reason,delay));
        server.await.unwrap();
    }
    let mut headers = header::HeaderMap::new();
    headers.append(header::RETRY_AFTER, "1".parse().unwrap());
    headers.append(header::RETRY_AFTER, "2".parse().unwrap());
    assert_eq!(retry_after(&headers), None);
    headers.insert(
        header::RETRY_AFTER,
        "Wed, 21 Oct 2015 07:28:00 GMT".parse().unwrap(),
    );
    assert_eq!(retry_after(&headers), None);
}

#[tokio::test]
async fn a_stalled_receiver_respects_the_callers_shorter_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sender = sender(&format!(
        "http://{}/callback",
        listener.local_addr().unwrap()
    ));
    let (release, wait) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        request(&mut socket).await;
        let _ = wait.await;
    });
    let outcome = sender
        .deliver(&fixture(), Instant::now() + Duration::from_millis(100))
        .await
        .unwrap();
    assert_eq!(outcome, retry("delivery_deadline_elapsed", None));
    release.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn successful_empty_responses_reuse_the_destination_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sender = sender(&format!(
        "http://{}/callback",
        listener.local_addr().unwrap()
    ));
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let first = request(&mut socket).await.1;
        socket
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .await
            .unwrap();
        let second = request(&mut socket).await.1;
        socket
            .write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
            .await
            .unwrap();
        assert_eq!(first, second);
    });
    for _ in 0..2 {
        assert_eq!(
            sender
                .deliver(&fixture(), Instant::now() + Duration::from_secs(2))
                .await
                .unwrap(),
            CompletionDeliveryOutcome::Confirmed
        );
    }
    server.await.unwrap();
}
