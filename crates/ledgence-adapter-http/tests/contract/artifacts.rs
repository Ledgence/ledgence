//! Network evidence for the artifact boundary (persistence has separate PG tests).
use super::*;
use ledgence_adapter_http::{HttpArtifactClient, HttpCredentials};
use sha2::{Digest as _, Sha256};
use std::time::Instant;

struct BytesReader(Vec<u8>);
impl ArtifactReader for BytesReader {
    fn read_chunk(&mut self, max: usize) -> ContractFuture<'_, Vec<u8>> {
        Box::pin(async move {
            let size = max.min(self.0.len());
            Ok(self.0.drain(..size).collect())
        })
    }
}
struct Artifacts {
    data: Vec<u8>,
    manifest: ArtifactManifest,
    calls: AtomicUsize,
    releases: AtomicUsize,
    fail_release: AtomicBool,
    corrupt: AtomicBool,
    foreign_scope: AtomicBool,
    changed_release: AtomicBool,
}
impl Artifacts {
    fn new(data: Vec<u8>) -> Self {
        let reference = ArtifactRef {
            schema: "ledgence.artifact.v1".into(),
            id: "art_one".into(),
            digest: Digest(format!("sha256:{:x}", Sha256::digest(&data))),
            size: data.len() as u64,
            media_type: "text/plain".into(),
        };
        Self {
            data,
            manifest: ArtifactManifest {
                reference,
                scope: scope(),
                root: ExecutionTarget::Task {
                    id: "task_one".into(),
                },
                producer: ArtifactProducer::TrustedInstance {
                    instance_id: "audit".into(),
                },
                key: "report".into(),
                filename: Some("résumé.txt".into()),
                created_at: 1,
                accepted_at: 2,
                first_attempt_id: None,
            },
            calls: AtomicUsize::new(0),
            releases: AtomicUsize::new(0),
            fail_release: AtomicBool::new(false),
            corrupt: AtomicBool::new(false),
            foreign_scope: AtomicBool::new(false),
            changed_release: AtomicBool::new(false),
        }
    }
    fn visible_manifest(&self) -> ArtifactManifest {
        let mut manifest = self.manifest.clone();
        if self.foreign_scope.load(Ordering::SeqCst) {
            manifest.scope.namespace = "foreign".into();
        }
        manifest
    }
    fn status(&self) -> ArtifactUploadStatus {
        ArtifactUploadStatus {
            selection: ArtifactSelection {
                artifact_id: "art_one".into(),
                upload_id: "upl_one".into(),
                revision: 1,
            },
            selected: true,
            stage: ArtifactUploadStage::Ready,
            prepare_expires_at: 120_000,
            ingress_expires_at: None,
            dispatch_expires_at: Some(120_000),
            successor: None,
            reference: Some(self.manifest.reference.clone()),
            retry: ArtifactRetryGuidance::Complete,
        }
    }
    fn caller(&self, authority: &ArtifactAuthority) {
        assert_eq!(authority.scope(), &scope());
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}
impl ExecutionArtifactService for Artifacts {
    fn prepare<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        c: &'a ArtifactPrepareCommand,
        _: Instant,
    ) -> ContractFuture<'a, ArtifactPrepareReply> {
        Box::pin(async move {
            self.caller(a);
            c.validate()?;
            Ok(ArtifactPrepareReply {
                upload: self.status(),
            })
        })
    }
    fn upload_status<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        _: &'a str,
        _: Instant,
    ) -> ContractFuture<'a, ArtifactUploadStatus> {
        Box::pin(async move {
            self.caller(a);
            Ok(self.status())
        })
    }
    fn replace<'a>(
        &'a self,
        _: &'a ArtifactAuthority,
        _: &'a ArtifactReplaceCommand,
        _: Instant,
    ) -> ContractFuture<'a, ArtifactReplacementReceipt> {
        Box::pin(async { Err(ContractError::Conflict) })
    }
    fn put(
        &self,
        a: ArtifactAuthority,
        _: ArtifactSelection,
        mut body: Box<dyn ArtifactReader>,
        _: Instant,
    ) -> ContractFuture<'_, ArtifactUploadReply> {
        Box::pin(async move {
            self.caller(&a);
            let mut data = Vec::new();
            loop {
                let chunk = body.read_chunk(ARTIFACT_CHUNK_MAX_BYTES).await?;
                if chunk.is_empty() {
                    break;
                }
                data.extend(chunk);
            }
            assert_eq!(data, self.data);
            Ok(ArtifactUploadReply {
                upload: self.status(),
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        _: &'a ArtifactSelection,
        _: Instant,
    ) -> ContractFuture<'a, ArtifactUploadReply> {
        Box::pin(async move {
            self.caller(a);
            Ok(ArtifactUploadReply {
                upload: self.status(),
            })
        })
    }
    fn inspect<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        _: &'a ArtifactInspectQuery,
        _: Instant,
    ) -> ContractFuture<'a, ArtifactManifest> {
        Box::pin(async move {
            self.caller(a);
            Ok(self.visible_manifest())
        })
    }
    fn list<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        _: &'a ArtifactListQuery,
        _: Instant,
    ) -> ContractFuture<'a, ArtifactPage> {
        Box::pin(async move {
            self.caller(a);
            Ok(ArtifactPage {
                items: vec![self.visible_manifest()],
                next_cursor: None,
            })
        })
    }
    fn prepare_download<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        _: &'a ArtifactRef,
        d: Instant,
    ) -> ContractFuture<'a, PreparedArtifactDownload> {
        Box::pin(async move {
            self.caller(a);
            let mut data = self.data.clone();
            if self.corrupt.load(Ordering::SeqCst) && !data.is_empty() {
                data[0] ^= 1;
            }
            Ok(PreparedArtifactDownload::new(
                self.visible_manifest(),
                Box::new(BytesReader(data)),
                d,
            ))
        })
    }
    fn release_download<'a>(
        &'a self,
        _: &'a ArtifactAuthority,
        p: PreparedArtifactDownload,
        _: Instant,
    ) -> ContractFuture<'a, ArtifactDownload> {
        Box::pin(async move {
            self.releases.fetch_add(1, Ordering::SeqCst);
            if self.fail_release.load(Ordering::SeqCst) {
                return Err(ContractError::OwnershipLost);
            }
            let mut result = p.into_download();
            if self.changed_release.load(Ordering::SeqCst) {
                result.manifest.filename = Some("changed.txt".into());
            }
            Ok(result)
        })
    }
    fn maintain(&self, _: u32, _: Instant) -> ContractFuture<'_, ArtifactMaintenanceProgress> {
        Box::pin(async { Ok(ArtifactMaintenanceProgress::default()) })
    }
    fn drain(&self, _: Instant) -> ContractFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
fn requester() -> ArtifactRequester {
    ArtifactRequester::External { scope: scope() }
}
fn client(url: &str, token: &str) -> HttpArtifactClient {
    HttpArtifactClient::new(url)
        .unwrap()
        .with_credentials(HttpCredentials::new(url, token).unwrap())
        .unwrap()
}
async fn enabled(data: Vec<u8>) -> (Fixture, Arc<Artifacts>) {
    let f = setup().await;
    let artifacts = Arc::new(Artifacts::new(data));
    assert!(f.mock.artifacts.set(artifacts.clone()).is_ok());
    (f, artifacts)
}
#[tokio::test]
async fn binary_round_trip_streams_multiple_chunks_and_zero_bytes_with_unicode_manifest() {
    for data in [Vec::new(), vec![b'x'; ARTIFACT_CHUNK_MAX_BYTES * 3 + 7]] {
        let (f, a) = enabled(data.clone()).await;
        let writer = client(&f.running.url, "artifact-writer");
        let command = ArtifactPrepareCommand {
            scope: scope(),
            target: a.manifest.root.clone(),
            key: "report".into(),
            metadata: ArtifactMetadata {
                digest: a.manifest.reference.digest.clone(),
                size: data.len() as u64,
                media_type: "text/plain".into(),
                filename: a.manifest.filename.clone(),
            },
        };
        let prepared = writer
            .prepare(&requester(), &command, deadline())
            .await
            .unwrap();
        writer
            .put(
                requester(),
                prepared.upload.selection,
                Box::new(BytesReader(data.clone())),
                deadline(),
            )
            .await
            .unwrap();
        let mut download = client(&f.running.url, "source-investigator")
            .download(requester(), a.manifest.reference.clone(), deadline())
            .await
            .unwrap();
        let mut output = Vec::new();
        loop {
            let chunk = download.reader.read_chunk(7 * 1024).await.unwrap();
            if chunk.is_empty() {
                break;
            }
            output.extend(chunk);
        }
        assert_eq!(output, data);
        assert_eq!(download.manifest.filename, Some("résumé.txt".into()));
        assert_eq!(f.auditor.observations.lock().unwrap().len(), 1);
        assert_eq!(a.releases.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn audit_failure_and_stale_authority_release_no_binary_payload() {
    for audit_mode in [1, 2, 3] {
        let (f, a) = enabled(CANARY.as_bytes().to_vec()).await;
        f.auditor.mode.store(audit_mode, Ordering::SeqCst);
        let result = client(&f.running.url, "source-investigator")
            .download(requester(), a.manifest.reference.clone(), deadline())
            .await;
        assert!(result.is_err());
        assert_eq!(a.releases.load(Ordering::SeqCst), 0);
    }
    let (f, a) = enabled(CANARY.as_bytes().to_vec()).await;
    a.fail_release.store(true, Ordering::SeqCst);
    assert!(
        client(&f.running.url, "source-investigator")
            .download(requester(), a.manifest.reference.clone(), deadline())
            .await
            .is_err()
    );
    assert_eq!(f.auditor.observations.lock().unwrap().len(), 1);
    assert_eq!(a.releases.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn payload_permission_and_scope_are_checked_before_artifact_service() {
    let (f, a) = enabled(b"payload".to_vec()).await;
    assert!(matches!(
        client(&f.running.url, "reader")
            .download(requester(), a.manifest.reference.clone(), deadline())
            .await,
        Err(ArtifactClientError::Rejected(ContractError::Forbidden))
    ));
    let mut foreign = scope();
    foreign.namespace = "foreign".into();
    assert!(matches!(
        client(&f.running.url, "source-investigator")
            .download(
                ArtifactRequester::External { scope: foreign },
                a.manifest.reference.clone(),
                deadline()
            )
            .await,
        Err(ArtifactClientError::Rejected(ContractError::NotFound))
    ));
    assert_eq!(a.calls.load(Ordering::SeqCst), 0);
    let manifest = client(&f.running.url, "reader")
        .inspect(
            &ArtifactInspectQuery {
                scope: scope(),
                artifact_id: "art_one".into(),
            },
            deadline(),
        )
        .await
        .unwrap();
    assert_eq!(manifest, a.manifest);
}
#[tokio::test]
async fn client_checks_received_bytes_even_when_manifest_and_framing_match() {
    let (f, a) = enabled(b"payload".to_vec()).await;
    a.corrupt.store(true, Ordering::SeqCst);
    let mut download = client(&f.running.url, "source-investigator")
        .download(requester(), a.manifest.reference.clone(), deadline())
        .await
        .unwrap();
    assert!(
        !download
            .reader
            .read_chunk(ARTIFACT_CHUNK_MAX_BYTES)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        download
            .reader
            .read_chunk(ARTIFACT_CHUNK_MAX_BYTES)
            .await
            .is_err()
    );
    // An ignored integrity error must never turn into a successful EOF.
    assert!(
        download
            .reader
            .read_chunk(ARTIFACT_CHUNK_MAX_BYTES)
            .await
            .is_err()
    );
}
#[tokio::test]
async fn trusted_negotiation_preserves_legacy_reply_and_never_enables_admission() {
    let mock = Arc::new(Mock::default());
    let a = Arc::new(Artifacts::new(Vec::new()));
    assert!(mock.artifacts.set(a).is_ok());
    let context = SelfHostedInstanceContext {
        instance_id: "trusted".into(),
        name: "Trusted".into(),
        scope: scope(),
    };
    let running = start(
        server::router_with_instance(
            mock.clone(),
            mock.clone(),
            mock.clone(),
            context,
            Arc::new(AtomicBool::new(false)),
            Arc::new(ledgence_worker_api::NoopTraceBridge),
        )
        .unwrap(),
    )
    .await;
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/v1/access", running.url))
        .header("ledgence-worker-features", "1")
        .send()
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(value, json!({"mode":"operator_trusted"}));
    let worker = HttpTaskService::new(&running.url).unwrap();
    assert!(worker.artifact_client().is_err());
    let discovered = worker.discover_worker_runtime(&scope()).await.unwrap();
    assert!(discovered.runtime_features.execution_artifacts());
    assert!(!discovered.runtime_features.resource_admission());
    assert!(discovered.authenticated_binding.is_none());
    assert!(discovered.effect_evidence.is_none());
    assert!(worker.artifact_client().is_ok());
    let response = client
        .get(format!("{}/v1/access", running.url))
        .header("ledgence-artifact-features", "2")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
}
#[tokio::test]
async fn console_download_requires_current_actor_origin_and_emits_binary_headers() {
    let (f, a) = enabled(b"payload".to_vec()).await;
    let response = reqwest::Client::new()
        .post(format!("{}/v1/console/artifacts/download", f.running.url))
        .bearer_auth("operator")
        .header("content-type", "application/json")
        .body(json!({"reference":a.manifest.reference}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers()["content-type"],
        "application/octet-stream"
    );
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["ledgence-instance-id"], "audit");
    assert_eq!(response.bytes().await.unwrap(), "payload");
    // Cookie-specific origin and actor checks are shared with every Console route;
    // bearer mode is intentionally allowed without browser origin headers.
    let response = reqwest::Client::new()
        .post(format!("{}/v1/console/artifacts/list", f.running.url))
        .bearer_auth("reader")
        .header("content-type", "application/json")
        .body(
            json!({"scope":scope(),"target":a.manifest.root,"limit":10,"cursor":null}).to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
}

#[tokio::test]
async fn client_distinguishes_rejections_uncertainty_and_invalid_responses() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for (status, body, truncated, expected) in [
        (
            503,
            r#"{"code":"unavailable","message":"store unavailable"}"#,
            false,
            "uncertain",
        ),
        (403, r#"{"code":"forbidden"}"#, false, "rejected"),
        (200, "not JSON", false, "invalid"),
        (200, r#"{"selection":{}}"#, false, "invalid"),
        (200, "{", true, "uncertain"),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 16384];
            let count = stream.read(&mut request).await.unwrap();
            assert!(
                String::from_utf8_lossy(&request[..count])
                    .starts_with("POST /v1/artifacts/uploads/upl_one/status")
            );
            let length = body.len() + usize::from(truncated) * 10;
            let response = format!(
                "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n{body}"
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        let result = HttpArtifactClient::new(&format!("http://{address}"))
            .unwrap()
            .upload_status(&requester(), "upl_one", deadline())
            .await
            .unwrap_err();
        match expected {
            "uncertain" => assert!(
                matches!(result, ArtifactClientError::OutcomeUnknown { .. }),
                "{result:?}"
            ),
            "rejected" => assert!(
                matches!(
                    result,
                    ArtifactClientError::Rejected(ContractError::Forbidden)
                ),
                "{result:?}"
            ),
            _ => assert!(
                matches!(result, ArtifactClientError::InvalidResponse { .. }),
                "{result:?}"
            ),
        }
        server.await.unwrap();
    }
}

#[tokio::test]
async fn server_rejects_foreign_metadata_and_changed_audited_download_before_bytes() {
    let (f, a) = enabled(b"payload".to_vec()).await;
    a.foreign_scope.store(true, Ordering::SeqCst);
    for (path, body) in [
        ("inspect", json!({"artifact_id":"art_one"})),
        (
            "list",
            json!({"target":{"kind":"task","id":"task_one"},"limit":1}),
        ),
        ("download", json!({"reference":a.manifest.reference})),
    ] {
        let response = reqwest::Client::new()
            .post(format!("{}/v1/console/artifacts/{path}", f.running.url))
            .bearer_auth("operator")
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 503);
        assert!(
            !response
                .headers()
                .contains_key("ledgence-artifact-manifest")
        );
    }
    assert_eq!(a.releases.load(Ordering::SeqCst), 0);
    assert!(f.auditor.observations.lock().unwrap().is_empty());
    a.foreign_scope.store(false, Ordering::SeqCst);
    a.changed_release.store(true, Ordering::SeqCst);
    assert!(
        client(&f.running.url, "source-investigator")
            .download(requester(), a.manifest.reference.clone(), deadline())
            .await
            .is_err()
    );
    assert_eq!(a.releases.load(Ordering::SeqCst), 1);
    assert_eq!(f.auditor.observations.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn replacement_current_must_describe_original_successor_even_after_further_replacement() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for mismatched in [false, true] {
        let artifacts = Artifacts::new(Vec::new());
        let replaced = artifacts.status().selection;
        let replacement = ArtifactSelection {
            upload_id: "upl_two".into(),
            revision: 2,
            ..replaced.clone()
        };
        let successor = ArtifactSelection {
            upload_id: "upl_three".into(),
            revision: 3,
            ..replaced.clone()
        };
        let current = ArtifactUploadStatus {
            selection: if mismatched {
                successor.clone()
            } else {
                replacement.clone()
            },
            selected: mismatched,
            stage: ArtifactUploadStage::Prepared,
            prepare_expires_at: 120_000,
            ingress_expires_at: None,
            dispatch_expires_at: None,
            successor: (!mismatched).then_some(successor),
            reference: None,
            retry: if mismatched {
                ArtifactRetryGuidance::Upload
            } else {
                ArtifactRetryGuidance::Unavailable
            },
        };
        current.validate().unwrap();
        let receipt = ArtifactReplacementReceipt {
            replaced,
            replacement,
            current,
        };
        let body = serde_json::to_string(&receipt).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let end = loop {
                if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    break end + 4;
                }
                let mut chunk = [0; 4096];
                let count = stream.read(&mut chunk).await.unwrap();
                assert_ne!(count, 0);
                request.extend_from_slice(&chunk[..count]);
                assert!(request.len() <= 16384);
            };
            let headers = String::from_utf8_lossy(&request[..end]);
            assert!(headers.starts_with("POST /v1/artifacts/uploads/upl_one/replace"));
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            while request.len() < end + length {
                let mut chunk = [0; 4096];
                let count = stream.read(&mut chunk).await.unwrap();
                assert_ne!(count, 0);
                request.extend_from_slice(&chunk[..count]);
                assert!(request.len() <= 16384);
            }
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        let result = HttpArtifactClient::new(&format!("http://{address}"))
            .unwrap()
            .replace(
                &requester(),
                &ArtifactReplaceCommand {
                    upload_id: "upl_one".into(),
                    expected_selection_revision: 1,
                },
                deadline(),
            )
            .await;
        if mismatched {
            assert!(matches!(
                result,
                Err(ArtifactClientError::InvalidResponse { .. })
            ));
        } else {
            assert_eq!(result.unwrap(), receipt);
        }
        server.await.unwrap();
    }
}
