use super::*;
use ledgence_adapter_http::{HttpProgramPublisher, server::publication::PublicationService};
use ledgence_worker_api::{
    ProgramArtifactPublisher, PublicationErrorKind, PublicationFuture, PublicationLimits,
    PublishArtifactResult,
};
use sha2::{Digest as _, Sha256};

#[derive(Default)]
struct Writer {
    calls: AtomicUsize,
    entered: tokio::sync::Notify,
    release: Arc<(Mutex<bool>, std::sync::Condvar)>,
    blocked: AtomicBool,
    finished: AtomicUsize,
}
struct ReleaseOnDrop(Arc<Writer>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}
impl Writer {
    fn release(&self) {
        *self.release.0.lock().unwrap() = true;
        self.release.1.notify_all();
    }
}
impl ProgramArtifactPublisher for Writer {
    fn publish<'a>(
        &'a self,
        program: ProgramRef,
        digest: Digest,
        archive: Vec<u8>,
    ) -> PublicationFuture<'a, PublishArtifactResult> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            if self.blocked.load(Ordering::SeqCst) {
                let gate = self.release.clone();
                tokio::task::spawn_blocking(move || {
                    let mut open = gate.0.lock().unwrap();
                    while !*open {
                        open = gate.1.wait(open).unwrap();
                    }
                })
                .await
                .unwrap();
            }
            self.finished.fetch_add(1, Ordering::SeqCst);
            Ok(PublishArtifactResult {
                descriptor: ProgramDescriptor {
                    program,
                    digest,
                    size: archive.len() as u64,
                },
                already_published: call != 0,
            })
        })
    }
}
async fn setup(
    writer: Arc<Writer>,
    timeout: Duration,
    maximum: u64,
) -> (Running, PublicationService) {
    let publication = PublicationService::new(writer)
        .with_transport_limits(maximum, 1, timeout)
        .unwrap();
    let console = server::console::ConsoleServices::new(
        SelfHostedInstanceConfig {
            instance_id: "publication-test".into(),
            name: "Publication".into(),
            scope: scope(),
            suggested_queues: vec![],
            allowed_origins: vec!["https://allowed.example".into()],
        },
        "0.4.1".into(),
        Arc::new(console::Queries::default()),
        None,
        None,
    )
    .unwrap()
    .with_publication(publication.clone());
    let service = Arc::new(Mock::default());
    let router = server::router_with_console(
        service.clone(),
        service.clone(),
        service,
        console,
        Arc::new(AtomicBool::new(false)),
        Arc::new(ledgence_worker_api::NoopTraceBridge),
    )
    .unwrap();
    (start(router).await, publication)
}
fn prepared() -> (ProgramDescriptor, Vec<u8>) {
    let bytes = b"opaque prepared archive transport fixture".to_vec();
    (
        ProgramDescriptor {
            program: ProgramRef {
                id: "demo".into(),
                version: "1.0.0".into(),
            },
            digest: Digest(format!("sha256:{:x}", Sha256::digest(&bytes))),
            size: bytes.len() as u64,
        },
        bytes,
    )
}
fn put(running: &Running) -> reqwest::RequestBuilder {
    reqwest::Client::new()
        .put(format!("{}/v1/programs/demo/1.0.0/artifact", running.url))
        .header("content-type", "application/zip")
        .header("x-ledgence-archive-sha256", prepared().0.digest.hex())
}
#[tokio::test]
async fn binary_upload_and_identical_repeat_use_created_and_ok_and_preserve_headers() {
    let writer = Arc::new(Writer::default());
    let (running, _) = setup(writer.clone(), Duration::from_secs(5), 1024).await;
    let client = HttpProgramPublisher::new(&running.url).unwrap();
    let capability = client.capabilities().await.unwrap();
    assert!(capability.enabled);
    assert!(!capability.registration_enabled);
    assert_eq!(capability.max_concurrent_uploads, 1);
    let (descriptor, bytes) = prepared();
    assert!(
        !client
            .publish(&descriptor, bytes.clone())
            .await
            .unwrap()
            .already_published
    );
    assert!(
        client
            .publish(&descriptor, bytes)
            .await
            .unwrap()
            .already_published
    );
    let response = put(&running).body(prepared().1).send().await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert!(response.headers().contains_key("request-id"));
    assert_eq!(writer.calls.load(Ordering::SeqCst), 3);
}
#[tokio::test]
async fn disabled_read_only_service_negotiates_without_reading_or_uploading() {
    let running = start(server::router(Arc::new(Mock::default()))).await;
    let client = HttpProgramPublisher::new(&running.url).unwrap();
    assert!(!client.capabilities().await.unwrap().enabled);
    let (descriptor, bytes) = prepared();
    assert_eq!(
        client.publish(&descriptor, bytes).await.unwrap_err().kind,
        PublicationErrorKind::Disabled
    );
    assert_eq!(
        put(&running)
            .body("anything")
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
}
#[tokio::test]
async fn invalid_headers_origin_and_methods_never_call_writer() {
    let writer = Arc::new(Writer::default());
    let (running, _) = setup(writer.clone(), Duration::from_secs(5), 1024).await;
    for request in [
        put(&running).header("x-ledgence-archive-sha256", "0".repeat(64)),
        put(&running).header("content-type", "application/json"),
        put(&running).header("content-encoding", "gzip"),
        put(&running).header("origin", "https://untrusted.example"),
        reqwest::Client::new().put(format!("{}/v1/programs/demo/1.0.0/artifact", running.url)),
    ] {
        assert_eq!(
            request.body(prepared().1).send().await.unwrap().status(),
            400
        );
    }
    let response = reqwest::Client::new()
        .post(format!("{}/v1/programs/demo/1.0.0/artifact", running.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 405);
    assert_eq!(response.headers()["allow"], "PUT");
    assert_eq!(writer.calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn oversize_is_rejected_with_and_without_content_length() {
    let writer = Arc::new(Writer::default());
    let (running, _) = setup(writer.clone(), Duration::from_secs(5), 8).await;
    assert_eq!(
        put(&running)
            .body(prepared().1)
            .send()
            .await
            .unwrap()
            .status(),
        413
    );
    let address = running.url.trim_start_matches("http://");
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let request = format!(
        "PUT /v1/programs/demo/1.0.0/artifact HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/zip\r\nX-Ledgence-Archive-Sha256: {}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n9\r\n123456789\r\n0\r\n\r\n",
        prepared().0.digest.hex()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 413"), "{response}");
    assert_eq!(writer.calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn expired_request_retains_capacity_and_drain_until_blocking_writer_finishes() {
    let writer = Arc::new(Writer::default());
    writer.blocked.store(true, Ordering::SeqCst);
    let _release_on_failure = ReleaseOnDrop(writer.clone());
    let (running, publication) = setup(writer.clone(), Duration::from_millis(150), 1024).await;
    let request = put(&running).body(prepared().1).send();
    let request = tokio::spawn(request);
    tokio::time::timeout(Duration::from_secs(2), writer.entered.notified())
        .await
        .unwrap();
    assert_eq!(request.await.unwrap().unwrap().status(), 504);
    assert_eq!(
        put(&running)
            .body(prepared().1)
            .send()
            .await
            .unwrap()
            .status(),
        429
    );
    // The capability/control route remains usable while every upload is busy.
    assert!(
        HttpProgramPublisher::new(&running.url)
            .unwrap()
            .capabilities()
            .await
            .unwrap()
            .enabled
    );
    let drain = tokio::spawn(async move {
        publication.drain().await;
    });
    tokio::task::yield_now().await;
    assert!(!drain.is_finished());
    assert_eq!(writer.finished.load(Ordering::SeqCst), 0);
    writer.release();
    tokio::time::timeout(Duration::from_secs(2), drain)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(writer.finished.load(Ordering::SeqCst), 1);
    assert_eq!(
        put(&running)
            .body(prepared().1)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
}
#[tokio::test]
async fn incomplete_and_slow_uploads_release_admission_without_starting_writer() {
    let writer = Arc::new(Writer::default());
    let (running, _) = setup(writer.clone(), Duration::from_millis(100), 1024).await;
    let address = running.url.trim_start_matches("http://");
    for disconnect in [false, true] {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let headers = format!(
            "PUT /v1/programs/demo/1.0.0/artifact HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/zip\r\nX-Ledgence-Archive-Sha256: {}\r\nContent-Length: 20\r\nConnection: close\r\n\r\nx",
            prepared().0.digest.hex()
        );
        stream.write_all(headers.as_bytes()).await.unwrap();
        if disconnect {
            drop(stream);
            tokio::time::sleep(Duration::from_millis(150)).await;
        } else {
            let mut response = String::new();
            tokio::time::timeout(Duration::from_secs(2), stream.read_to_string(&mut response))
                .await
                .unwrap()
                .unwrap();
            assert!(response.starts_with("HTTP/1.1 504"), "{response}");
        }
        assert_eq!(writer.calls.load(Ordering::SeqCst), 0);
        assert!(
            HttpProgramPublisher::new(&running.url)
                .unwrap()
                .capabilities()
                .await
                .unwrap()
                .enabled
        );
    }
    assert_eq!(
        put(&running)
            .body(prepared().1)
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
}

async fn response_server(status: u16, body: Vec<u8>, capability: Option<Value>) -> Running {
    let caps = capability.unwrap_or_else(|| {
        serde_json::to_value(ledgence_worker_api::PublicationCapabilities {
            enabled: true,
            registration_enabled: true,
            mode: Some("immutable".into()),
            limits: PublicationLimits::default(),
            max_concurrent_uploads: 2,
            transfer_timeout_ms: 120000,
        })
        .unwrap()
    });
    start(
        axum::Router::new()
            .route(
                "/v1/programs/publication-capabilities",
                axum::routing::get(move || async move {
                    axum::response::Response::builder()
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(serde_json::to_vec(&caps).unwrap()))
                        .unwrap()
                }),
            )
            .route(
                "/v1/programs/{id}/{version}/artifact",
                axum::routing::put(move || async move {
                    axum::response::Response::builder()
                        .status(status)
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(body))
                        .unwrap()
                }),
            ),
    )
    .await
}
#[tokio::test]
async fn publisher_rejects_mismatched_authority_and_malformed_or_oversized_replies_as_unknown() {
    let (descriptor, archive) = prepared();
    let correct = PublishArtifactResult {
        descriptor: descriptor.clone(),
        already_published: false,
    };
    let mut wrong_digest = correct.clone();
    wrong_digest.descriptor.digest = Digest(format!("sha256:{}", "a".repeat(64)));
    let mut wrong_size = correct.clone();
    wrong_size.descriptor.size += 1;
    let mut wrong_program = correct.clone();
    wrong_program.descriptor.program.id = "another".into();
    let mut wrong_status = correct.clone();
    wrong_status.already_published = true;
    for bytes in [
        serde_json::to_vec(&wrong_digest).unwrap(),
        serde_json::to_vec(&wrong_size).unwrap(),
        serde_json::to_vec(&wrong_program).unwrap(),
        serde_json::to_vec(&wrong_status).unwrap(),
        b"not json".to_vec(),
        vec![b' '; 16385],
    ] {
        let running = response_server(201, bytes, None).await;
        let error = HttpProgramPublisher::new(&running.url)
            .unwrap()
            .publish(&descriptor, archive.clone())
            .await
            .unwrap_err();
        assert_eq!(error.kind, PublicationErrorKind::OutcomeUnknown);
    }
    let running = response_server(201, serde_json::to_vec(&correct).unwrap(), None).await;
    assert_eq!(
        HttpProgramPublisher::new(&running.url)
            .unwrap()
            .publish(&descriptor, archive)
            .await
            .unwrap(),
        correct
    );
}
#[tokio::test]
async fn publisher_preserves_typed_rejections_but_not_untrusted_error_details() {
    let (descriptor, archive) = prepared();
    for (status, kind) in [
        (400, PublicationErrorKind::InvalidArtifact),
        (413, PublicationErrorKind::TooLarge),
        (409, PublicationErrorKind::ImmutableConflict),
        (429, PublicationErrorKind::Saturated),
        (503, PublicationErrorKind::Storage),
        (504, PublicationErrorKind::OutcomeUnknown),
        (404, PublicationErrorKind::Disabled),
    ] {
        let body = serde_json::to_vec(&ledgence_worker_api::PublicationError::new(
            kind,
            "secret/path/token",
        ))
        .unwrap();
        let running = response_server(status, body, None).await;
        let error = HttpProgramPublisher::new(&running.url)
            .unwrap()
            .publish(&descriptor, archive.clone())
            .await
            .unwrap_err();
        assert_eq!(error.kind, kind);
        assert!(!error.message.contains("secret"));
    }
    let body = serde_json::to_vec(&ledgence_worker_api::PublicationError::new(
        PublicationErrorKind::ImmutableConflict,
        "wrong status",
    ))
    .unwrap();
    let running = response_server(400, body, None).await;
    assert_eq!(
        HttpProgramPublisher::new(&running.url)
            .unwrap()
            .publish(&descriptor, archive)
            .await
            .unwrap_err()
            .kind,
        PublicationErrorKind::OutcomeUnknown
    );
}
#[tokio::test]
async fn old_server_and_redirects_do_not_upload_or_follow_elsewhere() {
    let old = start(axum::Router::new()).await;
    assert_eq!(
        HttpProgramPublisher::new(&old.url)
            .unwrap()
            .capabilities()
            .await
            .unwrap_err()
            .kind,
        PublicationErrorKind::Disabled
    );
    let running = start(axum::Router::new().route(
        "/v1/programs/publication-capabilities",
        axum::routing::get(|| async {
            axum::response::Redirect::temporary("/v1/programs/publication-capabilities")
        }),
    ))
    .await;
    assert_eq!(
        HttpProgramPublisher::new(&running.url)
            .unwrap()
            .capabilities()
            .await
            .unwrap_err()
            .kind,
        PublicationErrorKind::Storage
    );
}

#[tokio::test]
async fn disconnected_request_does_not_release_running_publication() {
    let writer = Arc::new(Writer::default());
    writer.blocked.store(true, Ordering::SeqCst);
    let _release_on_failure = ReleaseOnDrop(writer.clone());
    let (running, publication) = setup(writer.clone(), Duration::from_secs(2), 1024).await;
    let address = running.url.trim_start_matches("http://");
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let bytes = prepared().1;
    let headers = format!(
        "PUT /v1/programs/demo/1.0.0/artifact HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/zip\r\nX-Ledgence-Archive-Sha256: {}\r\nContent-Length: {}\r\n\r\n",
        prepared().0.digest.hex(),
        bytes.len()
    );
    stream.write_all(headers.as_bytes()).await.unwrap();
    stream.write_all(&bytes).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), writer.entered.notified())
        .await
        .unwrap();
    drop(stream);
    assert_eq!(
        put(&running).body(bytes).send().await.unwrap().status(),
        429
    );
    let drain = tokio::spawn(async move {
        publication.drain().await;
    });
    tokio::task::yield_now().await;
    assert!(!drain.is_finished());
    writer.release();
    tokio::time::timeout(Duration::from_secs(2), drain)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(writer.finished.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn publication_trace_and_observer_cover_capability_and_invalid_upload_receipt() {
    let carriers = Arc::new(Mutex::new(Vec::new()));
    let capability_carriers = carriers.clone();
    let upload_carriers = carriers.clone();
    let capabilities = serde_json::to_vec(&ledgence_worker_api::PublicationCapabilities {
        enabled: true,
        registration_enabled: true,
        mode: Some("immutable".into()),
        limits: PublicationLimits::default(),
        max_concurrent_uploads: 2,
        transfer_timeout_ms: 120000,
    })
    .unwrap();
    let router = axum::Router::new()
        .route(
            "/v1/programs/publication-capabilities",
            axum::routing::get(move |request: axum::extract::Request| async move {
                capability_carriers.lock().unwrap().push(
                    request.headers()["traceparent"]
                        .to_str()
                        .unwrap()
                        .to_owned(),
                );
                assert_eq!(request.headers()["tracestate"], "transport=unsampled");
                axum::response::Response::builder()
                    .header("content-type", "application/json")
                    .header("request-id", "req-capability")
                    .body(axum::body::Body::from(capabilities))
                    .unwrap()
            }),
        )
        .route(
            "/v1/programs/{id}/{version}/artifact",
            axum::routing::put(move |request: axum::extract::Request| async move {
                upload_carriers.lock().unwrap().push(
                    request.headers()["traceparent"]
                        .to_str()
                        .unwrap()
                        .to_owned(),
                );
                axum::response::Response::builder()
                    .status(201)
                    .header("content-type", "application/json")
                    .header("request-id", "req-upload")
                    .body(axum::body::Body::from("invalid receipt"))
                    .unwrap()
            }),
        );
    let running = start(router).await;
    let bridge = Arc::new(ExchangeTraceBridge::default());
    let observations = Arc::new(Mutex::new(Vec::new()));
    let capture = observations.clone();
    let client = HttpProgramPublisher::new(&running.url)
        .unwrap()
        .with_trace_bridge(bridge)
        .with_observer(move |metadata| capture.lock().unwrap().push(metadata.clone()));
    let (descriptor, archive) = prepared();
    assert_eq!(
        client.publish(&descriptor, archive).await.unwrap_err().kind,
        PublicationErrorKind::OutcomeUnknown
    );
    let carriers = carriers.lock().unwrap();
    assert_eq!(carriers.len(), 2);
    assert_ne!(carriers[0], carriers[1]);
    let metadata = observations.lock().unwrap();
    assert_eq!(metadata.len(), 2);
    assert_eq!(metadata[0].request_id.as_deref(), Some("req-capability"));
    assert_eq!(metadata[1].request_id.as_deref(), Some("req-upload"));
    assert_eq!(metadata[1].status, Some(201));
}
