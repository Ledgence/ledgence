use super::*;
use ledgence_adapter_artifact::{
    ArtifactLimits, FileProgramArtifactPublisher, ZipProgramPackageInspector, pack_directory,
};
use ledgence_worker_api::{
    ProgramPackageInspection, ProgramPackageInspector, ProgramPackageProfile,
    PublicationCapabilities, PublicationResult,
};
use std::{fs, path::Path};

const PROFILE_HEADER: &str = "x-ledgence-program-package-profile";
const EXPANDED: &str = "entries-8192-v1";

async fn capabilities(running: &Running, expanded: bool) -> reqwest::Response {
    let request = reqwest::Client::new().get(format!(
        "{}/v1/programs/publication-capabilities",
        running.url
    ));
    let request = if expanded {
        request.header(PROFILE_HEADER, EXPANDED)
    } else {
        request
    };
    request.send().await.unwrap()
}

#[tokio::test]
async fn selected_profile_preserves_legacy_json_and_uses_matching_writer() {
    let legacy = Arc::new(Writer::default());
    let expanded = Arc::new(Writer::default());
    let service = PublicationService::new(legacy.clone())
        .with_extended_profile(expanded.clone())
        .with_transport_limits(1024, 1, Duration::from_secs(5))
        .unwrap();
    let (running, _) = setup_service(service).await;
    let reply = capabilities(&running, false).await;
    assert!(!reply.headers().contains_key(PROFILE_HEADER));
    let body: Value = serde_json::from_slice(&reply.bytes().await.unwrap()).unwrap();
    let mut limits = PublicationLimits {
        max_archive_bytes: 1024,
        ..PublicationLimits::default()
    };
    assert_eq!(
        body,
        json!({
            "enabled": true, "registration_enabled": false, "mode": "immutable",
            "limits": limits, "max_concurrent_uploads": 1, "transfer_timeout_ms": 5000
        })
    );
    let reply = capabilities(&running, true).await;
    assert_eq!(reply.headers()[PROFILE_HEADER], EXPANDED);
    assert_eq!(reply.headers()["cache-control"], "no-store");
    assert!(reply.headers().contains_key("request-id"));
    let selected: PublicationCapabilities =
        serde_json::from_slice(&reply.bytes().await.unwrap()).unwrap();
    limits.max_entries = 8192;
    assert_eq!(selected.limits, limits);
    assert_eq!(
        put(&running)
            .body(prepared().1)
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    assert_eq!(
        put(&running)
            .header(PROFILE_HEADER, EXPANDED)
            .body(prepared().1)
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    assert_eq!(legacy.calls.load(Ordering::SeqCst), 1);
    assert_eq!(expanded.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn legacy_only_service_falls_back_in_one_get_and_rejects_expanded_put_before_body() {
    let writer = Arc::new(Writer::default());
    let (running, _) = setup(writer.clone(), Duration::from_secs(5), 1024).await;
    let reply = capabilities(&running, true).await;
    assert!(!reply.headers().contains_key(PROFILE_HEADER));
    let selected: PublicationCapabilities =
        serde_json::from_slice(&reply.bytes().await.unwrap()).unwrap();
    assert_eq!(selected.limits.max_entries, 4096);
    let client = HttpProgramPublisher::new(&running.url, Arc::new(FixtureInspector)).unwrap();
    let (descriptor, archive) = prepared();
    client.publish(&descriptor, archive).await.unwrap();
    assert_eq!(writer.calls.load(Ordering::SeqCst), 1);
    let address = running.url.trim_start_matches("http://");
    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    socket.write_all(format!(
        "PUT /v1/programs/demo/1.0.0/artifact HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/zip\r\nX-Ledgence-Archive-Sha256: {}\r\nX-Ledgence-Program-Package-Profile: {EXPANDED}\r\nContent-Length: 1000\r\n\r\n",
        descriptor.digest.hex()
    ).as_bytes()).await.unwrap();
    let mut response = [0; 1024];
    let count = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&response[..count]).starts_with("HTTP/1.1 400"));
    assert_eq!(writer.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn malformed_profile_requests_fail_for_both_routes_and_disabled_services() {
    let writer = Arc::new(Writer::default());
    let (enabled, _) = setup(writer.clone(), Duration::from_secs(5), 1024).await;
    let disabled = start(server::router(Arc::new(Mock::default()))).await;
    for running in [&enabled, &disabled] {
        for values in [
            vec![""],
            vec!["unknown"],
            vec!["ENTRIES-8192-V1"],
            vec!["entries-8192-v1, entries-8192-v1"],
            vec![EXPANDED, EXPANDED],
        ] {
            for upload in [false, true] {
                let mut request = if upload {
                    put(running).body(prepared().1)
                } else {
                    reqwest::Client::new().get(format!(
                        "{}/v1/programs/publication-capabilities",
                        running.url
                    ))
                };
                for value in &values {
                    request = request.header(PROFILE_HEADER, *value);
                }
                assert_eq!(
                    request.send().await.unwrap().status(),
                    400,
                    "{values:?}, upload={upload}"
                );
            }
        }
    }
    let reply = capabilities(&disabled, true).await;
    assert!(!reply.headers().contains_key(PROFILE_HEADER));
    let selected: PublicationCapabilities =
        serde_json::from_slice(&reply.bytes().await.unwrap()).unwrap();
    assert!(!selected.enabled);
    assert_eq!(selected.limits, PublicationLimits::default());
    assert_eq!(
        put(&disabled)
            .header(PROFILE_HEADER, EXPANDED)
            .body(prepared().1)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(writer.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn expanded_and_legacy_writes_share_admission_and_drain_after_timeout() {
    for expanded_first in [false, true] {
        let legacy = Arc::new(Writer::default());
        let expanded = Arc::new(Writer::default());
        let blocked = if expanded_first {
            expanded.clone()
        } else {
            legacy.clone()
        };
        blocked.blocked.store(true, Ordering::SeqCst);
        let _release = ReleaseOnDrop(blocked.clone());
        let service = PublicationService::new(legacy.clone())
            .with_extended_profile(expanded.clone())
            .with_transport_limits(1024, 1, Duration::from_millis(150))
            .unwrap();
        let (running, service) = setup_service(service).await;
        let first = put(&running).body(prepared().1);
        let first = if expanded_first {
            first.header(PROFILE_HEADER, EXPANDED)
        } else {
            first
        };
        let first = tokio::spawn(first.send());
        tokio::time::timeout(Duration::from_secs(2), blocked.entered.notified())
            .await
            .unwrap();
        assert_eq!(first.await.unwrap().unwrap().status(), 504);
        let second = put(&running).body(prepared().1);
        let second = if expanded_first {
            second
        } else {
            second.header(PROFILE_HEADER, EXPANDED)
        };
        assert_eq!(second.send().await.unwrap().status(), 429);
        let drain = tokio::spawn(async move {
            service.drain().await;
        });
        tokio::task::yield_now().await;
        assert!(!drain.is_finished());
        blocked.release();
        tokio::time::timeout(Duration::from_secs(2), drain)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            legacy.calls.load(Ordering::SeqCst) + expanded.calls.load(Ordering::SeqCst),
            1
        );
        assert_eq!(blocked.finished.load(Ordering::SeqCst), 1);
    }
}

fn real_package(entries: usize) -> (ProgramDescriptor, Vec<u8>) {
    assert!(entries >= 2);
    let source = tempfile::tempdir().unwrap();
    fs::write(
        source.path().join("handler.py"),
        "def main(event):\n    return event\n",
    )
    .unwrap();
    fs::write(
        source.path().join("ledgence-program.json"),
        serde_json::to_vec(&json!({
            "schema_version": 1,
            "program": {"id": "profile-test", "version": format!("entries-{entries}")},
            "runtime": {"kind": "python", "python": "3.14", "protocol": 3},
            "handler": "handler:main", "platform": {"os": "linux", "arch": "x86_64"}
        }))
        .unwrap(),
    )
    .unwrap();
    for index in 2..entries {
        fs::create_dir(source.path().join(format!("empty-{index:05}"))).unwrap();
    }
    // The one-over-limit case is intentionally constructed under an explicit
    // test-only limit; production writers still use their fixed profiles.
    let mut limits = ArtifactLimits::for_profile(ProgramPackageProfile::Entries8192V1);
    limits.max_entries = entries.max(limits.max_entries);
    let package = pack_directory(source.path(), &limits).unwrap();
    (package.descriptor, package.archive)
}

fn descriptor_path(root: &Path, descriptor: &ProgramDescriptor) -> std::path::PathBuf {
    root.join("programs")
        .join(&descriptor.program.id)
        .join(&descriptor.program.version)
        .join("descriptor.json")
}

#[tokio::test]
async fn production_writers_enforce_exact_profile_entry_boundaries_before_visibility() {
    let root = tempfile::tempdir().unwrap();
    let legacy = Arc::new(
        FileProgramArtifactPublisher::new(root.path(), ArtifactLimits::default()).unwrap(),
    );
    let expanded = Arc::new(
        FileProgramArtifactPublisher::new(
            root.path(),
            ArtifactLimits::for_profile(ProgramPackageProfile::Entries8192V1),
        )
        .unwrap(),
    );
    let (running, _) =
        setup_service(PublicationService::new(legacy).with_extended_profile(expanded)).await;
    for (count, profile, status) in [
        (4096, None, 201),
        (4097, None, 413),
        (8192, Some(EXPANDED), 201),
        (8193, Some(EXPANDED), 413),
    ] {
        let (descriptor, archive) = real_package(count);
        let mut request = reqwest::Client::new()
            .put(format!(
                "{}/v1/programs/{}/{}/artifact",
                running.url, descriptor.program.id, descriptor.program.version
            ))
            .header("content-type", "application/zip")
            .header("x-ledgence-archive-sha256", descriptor.digest.hex());
        if let Some(profile) = profile {
            request = request.header(PROFILE_HEADER, profile);
        }
        assert_eq!(
            request.body(archive).send().await.unwrap().status(),
            status,
            "entry count {count}"
        );
        assert_eq!(
            descriptor_path(root.path(), &descriptor).exists(),
            status == 201
        );
        assert_eq!(
            root.path()
                .join("blobs")
                .join(format!("{}.zip", descriptor.digest.hex()))
                .exists(),
            status == 201
        );
    }
}

async fn negotiation_peer(
    limits: PublicationLimits,
    response_profiles: Vec<Vec<u8>>,
    put_profile: Option<&'static str>,
    descriptor: ProgramDescriptor,
    archive: Vec<u8>,
) -> (Running, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    let puts = count.clone();
    let caps = PublicationCapabilities {
        enabled: true,
        registration_enabled: false,
        mode: Some("immutable".into()),
        limits,
        max_concurrent_uploads: 2,
        transfer_timeout_ms: 120000,
    };
    let router = axum::Router::new()
        .route(
            "/v1/programs/publication-capabilities",
            axum::routing::get(move |request: axum::extract::Request| async move {
                assert_eq!(request.headers()[PROFILE_HEADER], EXPANDED);
                let mut reply =
                    axum::response::Response::builder().header("content-type", "application/json");
                for value in response_profiles {
                    reply = reply.header(
                        PROFILE_HEADER,
                        axum::http::HeaderValue::from_bytes(&value).unwrap(),
                    );
                }
                reply
                    .body(axum::body::Body::from(serde_json::to_vec(&caps).unwrap()))
                    .unwrap()
            }),
        )
        .route(
            "/v1/programs/{id}/{version}/artifact",
            axum::routing::put(move |request: axum::extract::Request| async move {
                puts.fetch_add(1, Ordering::SeqCst);
                assert_eq!(
                    request
                        .headers()
                        .get(PROFILE_HEADER)
                        .map(|value| value.to_str().unwrap()),
                    put_profile
                );
                let bytes = axum::body::to_bytes(request.into_body(), 64 * 1024 * 1024)
                    .await
                    .unwrap();
                assert_eq!(
                    bytes.as_ref(),
                    archive.as_slice(),
                    "preflight must upload the identical archive bytes"
                );
                axum::response::Response::builder()
                    .status(201)
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::to_vec(&PublishArtifactResult {
                            descriptor,
                            already_published: false,
                        })
                        .unwrap(),
                    ))
                    .unwrap()
            }),
        );
    (start(router).await, count)
}

#[tokio::test]
async fn response_profile_identity_controls_put_even_with_custom_lower_limits() {
    let (descriptor, archive) = real_package(3);
    for (headers, expected) in [
        (vec![], None),
        (vec![EXPANDED.as_bytes().to_vec()], Some(EXPANDED)),
    ] {
        let limits = PublicationLimits {
            max_entries: 3,
            ..PublicationLimits::default()
        };
        let (running, puts) = negotiation_peer(
            limits,
            headers,
            expected,
            descriptor.clone(),
            archive.clone(),
        )
        .await;
        let client =
            HttpProgramPublisher::new(&running.url, Arc::new(ZipProgramPackageInspector)).unwrap();
        client.publish(&descriptor, archive.clone()).await.unwrap();
        assert_eq!(puts.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn unsupported_response_profiles_and_unacknowledged_expanded_limits_never_upload() {
    let (descriptor, archive) = prepared();
    let invalid_headers = [
        vec![vec![]],
        vec![b"unknown".to_vec()],
        vec![b"ENTRIES-8192-V1".to_vec()],
        vec![vec![0xff]],
        vec![b"entries-8192-v1, entries-8192-v1".to_vec()],
        vec![EXPANDED.as_bytes().to_vec(), EXPANDED.as_bytes().to_vec()],
    ];
    for headers in invalid_headers {
        let (running, puts) = negotiation_peer(
            PublicationLimits::default(),
            headers,
            None,
            descriptor.clone(),
            archive.clone(),
        )
        .await;
        let client = HttpProgramPublisher::new(&running.url, Arc::new(FixtureInspector)).unwrap();
        assert_eq!(
            client
                .publish(&descriptor, archive.clone())
                .await
                .unwrap_err()
                .kind,
            PublicationErrorKind::Storage
        );
        assert_eq!(puts.load(Ordering::SeqCst), 0);
    }
    for (entries, headers) in [(8192, vec![]), (8193, vec![EXPANDED.as_bytes().to_vec()])] {
        let limits = PublicationLimits {
            max_entries: entries,
            ..PublicationLimits::default()
        };
        let (running, puts) =
            negotiation_peer(limits, headers, None, descriptor.clone(), archive.clone()).await;
        let client = HttpProgramPublisher::new(&running.url, Arc::new(FixtureInspector)).unwrap();
        assert_eq!(
            client
                .publish(&descriptor, archive.clone())
                .await
                .unwrap_err()
                .kind,
            PublicationErrorKind::Storage
        );
        assert_eq!(puts.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn actual_zip_preflight_rejects_legacy_overflow_and_honors_expanded_profile() {
    let (descriptor, archive) = real_package(4097);
    let (legacy, puts) = negotiation_peer(
        PublicationLimits::default(),
        vec![],
        None,
        descriptor.clone(),
        archive.clone(),
    )
    .await;
    let client =
        HttpProgramPublisher::new(&legacy.url, Arc::new(ZipProgramPackageInspector)).unwrap();
    assert_eq!(
        client
            .publish(&descriptor, archive.clone())
            .await
            .unwrap_err()
            .kind,
        PublicationErrorKind::TooLarge
    );
    assert_eq!(puts.load(Ordering::SeqCst), 0);
    let (expanded, puts) = negotiation_peer(
        ProgramPackageProfile::Entries8192V1.limits(),
        vec![EXPANDED.as_bytes().to_vec()],
        Some(EXPANDED),
        descriptor.clone(),
        archive.clone(),
    )
    .await;
    let client =
        HttpProgramPublisher::new(&expanded.url, Arc::new(ZipProgramPackageInspector)).unwrap();
    client.publish(&descriptor, archive).await.unwrap();
    assert_eq!(puts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn actual_zip_preflight_rejects_false_directory_count_and_lower_file_bound() {
    let (descriptor, archive) = real_package(3);
    let mut limits = ProgramPackageProfile::Entries8192V1.limits();
    limits.max_file_bytes = 1;
    let (running, puts) = negotiation_peer(
        limits,
        vec![EXPANDED.as_bytes().to_vec()],
        Some(EXPANDED),
        descriptor.clone(),
        archive.clone(),
    )
    .await;
    let client =
        HttpProgramPublisher::new(&running.url, Arc::new(ZipProgramPackageInspector)).unwrap();
    assert_eq!(
        client
            .publish(&descriptor, archive.clone())
            .await
            .unwrap_err()
            .kind,
        PublicationErrorKind::TooLarge
    );
    assert_eq!(puts.load(Ordering::SeqCst), 0);
    let mut corrupt = archive;
    let eocd = corrupt.len() - 22;
    assert_eq!(&corrupt[eocd..eocd + 4], b"PK\x05\x06");
    corrupt[eocd + 8..eocd + 10].copy_from_slice(&1_u16.to_le_bytes());
    corrupt[eocd + 10..eocd + 12].copy_from_slice(&1_u16.to_le_bytes());
    let mut changed = descriptor;
    changed.digest = Digest(format!("sha256:{:x}", Sha256::digest(&corrupt)));
    let (running, puts) = negotiation_peer(
        ProgramPackageProfile::Entries8192V1.limits(),
        vec![EXPANDED.as_bytes().to_vec()],
        Some(EXPANDED),
        changed.clone(),
        corrupt.clone(),
    )
    .await;
    let client =
        HttpProgramPublisher::new(&running.url, Arc::new(ZipProgramPackageInspector)).unwrap();
    assert_eq!(
        client.publish(&changed, corrupt).await.unwrap_err().kind,
        PublicationErrorKind::InvalidArtifact
    );
    assert_eq!(puts.load(Ordering::SeqCst), 0);
}

struct BlockingInspector {
    started: tokio::sync::mpsc::UnboundedSender<()>,
    gate: (Mutex<usize>, std::sync::Condvar),
}
impl BlockingInspector {
    fn release(&self, count: usize) {
        *self.gate.0.lock().unwrap() += count;
        self.gate.1.notify_all();
    }
}
impl ProgramPackageInspector for BlockingInspector {
    fn inspect(
        &self,
        descriptor: &ProgramDescriptor,
        archive: &[u8],
        limits: &PublicationLimits,
    ) -> PublicationResult<ProgramPackageInspection> {
        self.started.send(()).unwrap();
        let mut permits = self.gate.0.lock().unwrap();
        while *permits == 0 {
            permits = self.gate.1.wait(permits).unwrap();
        }
        *permits -= 1;
        drop(permits);
        FixtureInspector.inspect(descriptor, archive, limits)
    }
}
struct ReleaseInspector(Arc<BlockingInspector>);
impl Drop for ReleaseInspector {
    fn drop(&mut self) {
        self.0.release(3);
    }
}

#[tokio::test]
async fn cancelled_inspection_keeps_clone_shared_capacity_until_blocking_work_finishes() {
    let writer = Arc::new(Writer::default());
    let (running, _) = setup(writer.clone(), Duration::from_secs(5), 1024).await;
    let (started, mut inspections) = tokio::sync::mpsc::unbounded_channel();
    let inspector = Arc::new(BlockingInspector {
        started,
        gate: (Mutex::new(0), std::sync::Condvar::new()),
    });
    let _release = ReleaseInspector(inspector.clone());
    let (observed, mut responses) = tokio::sync::mpsc::unbounded_channel();
    let client = HttpProgramPublisher::new(&running.url, inspector.clone())
        .unwrap()
        .with_observer(move |_| {
            let _ = observed.send(());
        });
    let publish = |client: HttpProgramPublisher| {
        tokio::spawn(async move {
            let (descriptor, archive) = prepared();
            client.publish(&descriptor, archive).await
        })
    };
    let first = publish(client.clone());
    let second = publish(client.clone());
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(2), inspections.recv())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), responses.recv())
            .await
            .unwrap()
            .unwrap();
    }
    first.abort();
    second.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(second.await.unwrap_err().is_cancelled());
    let third = publish(client);
    tokio::time::timeout(Duration::from_secs(2), responses.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), inspections.recv())
            .await
            .is_err(),
        "cancelling callers must not start a third active inspector"
    );
    assert_eq!(writer.calls.load(Ordering::SeqCst), 0);
    inspector.release(1);
    tokio::time::timeout(Duration::from_secs(2), inspections.recv())
        .await
        .unwrap()
        .unwrap();
    inspector.release(2);
    tokio::time::timeout(Duration::from_secs(2), third)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(writer.calls.load(Ordering::SeqCst), 1);
}
