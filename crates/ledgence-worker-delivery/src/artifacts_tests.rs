//! File transfer and retained-IO boundary regressions.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
struct Client {
    data: Vec<u8>,
    prepared: Mutex<Option<ArtifactPrepareCommand>>,
    prepares: AtomicUsize,
    puts: AtomicUsize,
    race_path: Option<PathBuf>,
    replay: bool,
    corrupt: bool,
    upload_error: Option<ArtifactClientError>,
    allow_recovery: bool,
    expected_attempt: String,
    reconciliations: AtomicUsize,
}
impl Client {
    fn new(data: &[u8]) -> Self {
        Self {
            data: data.to_vec(),
            prepared: Mutex::new(None),
            prepares: AtomicUsize::new(0),
            puts: AtomicUsize::new(0),
            race_path: None,
            replay: false,
            corrupt: false,
            upload_error: None,
            allow_recovery: false,
            expected_attempt: "actual-attempt".into(),
            reconciliations: AtomicUsize::new(0),
        }
    }
    fn reference(&self) -> ArtifactRef {
        ArtifactRef {
            schema: "ledgence.artifact.v1".into(),
            id: "art_test".into(),
            digest: ledgence_worker_api::Digest(format!("sha256:{:x}", Sha256::digest(&self.data))),
            size: self.data.len() as u64,
            media_type: "application/octet-stream".into(),
        }
    }
    fn status(&self, ready: bool) -> ArtifactUploadStatus {
        ArtifactUploadStatus {
            selection: ArtifactSelection {
                artifact_id: "art_test".into(),
                upload_id: "upload_test".into(),
                revision: 1,
            },
            selected: true,
            stage: if ready {
                ArtifactUploadStage::Ready
            } else {
                ArtifactUploadStage::Prepared
            },
            prepare_expires_at: 120_000,
            ingress_expires_at: None,
            dispatch_expires_at: ready.then_some(120_000),
            successor: None,
            reference: ready.then(|| self.reference()),
            retry: if ready {
                ArtifactRetryGuidance::Complete
            } else {
                ArtifactRetryGuidance::Upload
            },
        }
    }
    fn check(&self, requester: &ArtifactRequester) {
        let ArtifactRequester::Worker { owner } = requester else {
            panic!("must preserve worker route")
        };
        assert_eq!(owner.attempt_id, self.expected_attempt);
        assert_eq!(owner.task_id, "actual-task");
    }
}
impl ExecutionArtifactClient for Client {
    fn prepare<'a>(
        &'a self,
        requester: &'a ArtifactRequester,
        command: &'a ArtifactPrepareCommand,
        _: Instant,
    ) -> ArtifactClientFuture<'a, ArtifactPrepareReply> {
        Box::pin(async move {
            self.check(requester);
            assert_eq!(
                command.target,
                ExecutionTarget::Task {
                    id: "actual-task".into()
                }
            );
            self.prepares.fetch_add(1, Ordering::SeqCst);
            *self.prepared.lock().unwrap() = Some(command.clone());
            Ok(ArtifactPrepareReply {
                upload: self.status(self.replay),
            })
        })
    }
    fn upload_status<'a>(
        &'a self,
        _: &'a ArtifactRequester,
        _: &'a str,
        _: Instant,
    ) -> ArtifactClientFuture<'a, ArtifactUploadStatus> {
        Box::pin(async { panic!("no hidden retries") })
    }
    fn replace<'a>(
        &'a self,
        _: &'a ArtifactRequester,
        _: &'a ArtifactReplaceCommand,
        _: Instant,
    ) -> ArtifactClientFuture<'a, ArtifactReplacementReceipt> {
        Box::pin(async { panic!("no hidden replacements") })
    }
    fn reconcile<'a>(
        &'a self,
        requester: &'a ArtifactRequester,
        selection: &'a ArtifactSelection,
        _: Instant,
    ) -> ArtifactClientFuture<'a, ArtifactUploadReply> {
        Box::pin(async move {
            assert!(self.allow_recovery, "no hidden reconciliation");
            self.check(requester);
            assert_eq!(selection, &self.status(true).selection);
            self.reconciliations.fetch_add(1, Ordering::SeqCst);
            Ok(ArtifactUploadReply {
                upload: self.status(true),
            })
        })
    }
    fn put(
        &self,
        requester: ArtifactRequester,
        _: ArtifactSelection,
        mut body: Box<dyn ArtifactReader>,
        _: Instant,
    ) -> ArtifactClientFuture<'_, ArtifactUploadReply> {
        Box::pin(async move {
            self.check(&requester);
            self.puts.fetch_add(1, Ordering::SeqCst);
            let mut bytes = Vec::new();
            loop {
                let chunk = body.read_chunk(32_123).await?;
                if chunk.is_empty() {
                    break;
                }
                assert!(chunk.len() <= 32_123);
                bytes.extend(chunk);
            }
            assert_eq!(bytes, self.data);
            if let Some(error) = &self.upload_error {
                return Err(error.clone());
            }
            Ok(ArtifactUploadReply {
                upload: self.status(true),
            })
        })
    }
    fn download(
        &self,
        requester: ArtifactRequester,
        reference: ArtifactRef,
        deadline: Instant,
    ) -> ArtifactClientFuture<'_, ArtifactDownload> {
        Box::pin(async move {
            self.check(&requester);
            assert_eq!(reference, self.reference());
            if let Some(path) = &self.race_path {
                std::fs::write(path, "other writer").unwrap();
            }
            let mut data = self.data.clone();
            if self.corrupt {
                data.push(0);
            }
            Ok(ArtifactDownload {
                manifest: ArtifactManifest {
                    reference,
                    scope: scope(),
                    root: ExecutionTarget::Task {
                        id: "actual-task".into(),
                    },
                    producer: ArtifactProducer::Task {
                        task_id: "actual-task".into(),
                    },
                    key: "file".into(),
                    filename: None,
                    created_at: 1,
                    accepted_at: 2,
                    first_attempt_id: Some("actual-attempt".into()),
                },
                reader: Box::new(Bytes { data, offset: 0 }),
                deadline,
            })
        })
    }
}
struct Bytes {
    data: Vec<u8>,
    offset: usize,
}
impl ArtifactReader for Bytes {
    fn read_chunk(&mut self, max: usize) -> ContractFuture<'_, Vec<u8>> {
        Box::pin(async move {
            let end = (self.offset + max).min(self.data.len());
            let chunk = self.data[self.offset..end].to_vec();
            self.offset = end;
            Ok(chunk)
        })
    }
}
fn scope() -> Scope {
    Scope {
        tenant_id: "tenant".into(),
        namespace: "test".into(),
    }
}
fn requester() -> ArtifactRequester {
    ArtifactRequester::Worker {
        owner: LeaseOwner {
            scope: scope(),
            task_id: "actual-task".into(),
            attempt_id: "actual-attempt".into(),
            worker_session_id: "session".into(),
            consumer_id: 0,
            lease_id: "lease".into(),
            generation: 1,
        },
    }
}
async fn invoke(
    client: &Client,
    operation: &str,
    payload: serde_json::Value,
) -> ledgence_worker_api::Result<serde_json::Value> {
    let io = Arc::new(OwnedIo::default());
    let result = execute(
        client,
        requester(),
        ExecutionTarget::Task {
            id: "actual-task".into(),
        },
        io.clone(),
        RuntimeRequest {
            id: 1,
            operation: operation.into(),
            payload,
        },
        RunControl::new(Duration::from_secs(5)),
    )
    .await;
    io.drain().await.unwrap();
    result
}
#[tokio::test]
async fn put_file_is_bounded_replayable_and_uses_host_identity() {
    for data in [vec![], vec![42; ARTIFACT_CHUNK_MAX_BYTES + 7]] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source");
        std::fs::write(&path, &data).unwrap();
        let mut client = Client::new(&data);
        let payload = serde_json::json!({"path": path, "key": "report", "media_type": "application/octet-stream", "filename": "report.bin"});
        let result = invoke(&client, "artifacts.put_file", payload.clone())
            .await
            .unwrap();
        assert_eq!(result, serde_json::to_value(client.reference()).unwrap());
        let prepared = client.prepared.lock().unwrap().clone().unwrap();
        assert_eq!(prepared.scope, scope());
        assert_eq!(
            prepared.target,
            ExecutionTarget::Task {
                id: "actual-task".into()
            }
        );
        assert_eq!(prepared.metadata.size, data.len() as u64);
        client.replay = true;
        invoke(&client, "artifacts.put_file", payload)
            .await
            .unwrap();
        assert_eq!(client.prepares.load(Ordering::SeqCst), 2);
        assert_eq!(client.puts.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn invalid_sources_and_forged_fields_never_prepare() {
    let client = Client::new(b"");
    let directory = tempfile::tempdir().unwrap();
    for path in [PathBuf::from("relative"), directory.path().to_owned()] {
        assert!(invoke(&client, "artifacts.put_file", serde_json::json!({"path":path,"key":"file","media_type":"application/octet-stream","filename":null})).await.is_err());
    }
    assert!(invoke(&client, "artifacts.put_file", serde_json::json!({"path":directory.path().join("file"),"key":"file","media_type":"application/octet-stream","filename":null,"owner":{"task_id":"forged"}})).await.is_err());
    #[cfg(unix)]
    {
        let source = directory.path().join("source");
        let link = directory.path().join("link");
        std::fs::write(&source, "").unwrap();
        std::os::unix::fs::symlink(&source, &link).unwrap();
        assert!(invoke(&client, "artifacts.put_file", serde_json::json!({"path":link,"key":"file","media_type":"application/octet-stream","filename":null})).await.is_err());
    }
    assert_eq!(client.prepares.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn download_verifies_bytes_and_never_overwrites_even_a_racing_destination() {
    for (data, race, corrupt) in [
        (b"".as_slice(), false, false),
        (b"content".as_slice(), false, false),
        (b"content".as_slice(), true, false),
        (b"content".as_slice(), false, true),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("download");
        let mut client = Client::new(data);
        client.race_path = race.then(|| path.clone());
        client.corrupt = corrupt;
        let result = invoke(
            &client,
            "artifacts.download",
            serde_json::json!({"path":path,"reference":client.reference()}),
        )
        .await;
        if race {
            assert!(result.is_err());
            assert_eq!(std::fs::read(&path).unwrap(), b"other writer");
        } else if corrupt {
            assert!(result.is_err());
            assert!(!path.exists());
        } else {
            assert!(result.is_ok());
            assert_eq!(std::fs::read(&path).unwrap(), data);
        }
        assert_eq!(
            std::fs::read_dir(directory.path()).unwrap().count(),
            usize::from(path.exists()),
            "no partial file remains"
        );
    }
}
#[tokio::test]
async fn cancelled_blocking_io_remains_owned_across_cancelled_drain() {
    let io = Arc::new(OwnedIo::default());
    let (started_tx, started_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = std::sync::mpsc::channel();
    {
        let mut future = Box::pin(io.run(move || {
            let _ = started_tx.send(());
            finish_rx.recv().unwrap();
            Ok(())
        }));
        tokio::select! { result = &mut future => panic!("unexpected completion {result:?}"), _ = started_rx => {} }
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(30), io.drain())
            .await
            .is_err()
    );
    finish_tx.send(()).unwrap();
    io.drain().await.unwrap();
    assert!(io.jobs.lock().await.is_empty());
}

#[tokio::test]
async fn uncertain_upload_can_be_explicitly_reconciled_by_current_retry_without_resending() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    std::fs::write(&path, b"artifact").unwrap();
    let mut client = Client::new(b"artifact");
    client.upload_error = Some(ArtifactClientError::OutcomeUnknown {
        message: "lost reply".into(),
    });
    let uncertain = invoke(&client,"artifacts.put_file",serde_json::json!({"path":path,"key":"file","media_type":"application/octet-stream","filename":null})).await.unwrap();
    let selection: ArtifactSelection =
        serde_json::from_value(uncertain["artifact_error"]["selection"].clone()).unwrap();
    assert_eq!(uncertain["artifact_error"]["kind"], "uncertain");
    client.allow_recovery = true;
    client.expected_attempt = "retry-attempt".into();
    let ArtifactRequester::Worker { mut owner } = requester() else {
        unreachable!()
    };
    owner.attempt_id = "retry-attempt".into();
    owner.generation = 2;
    let result = execute(
        &client,
        ArtifactRequester::Worker { owner },
        ExecutionTarget::Task {
            id: "actual-task".into(),
        },
        Arc::new(OwnedIo::default()),
        RuntimeRequest {
            id: 2,
            operation: "artifacts.reconcile".into(),
            payload: serde_json::json!({"selection":selection}),
        },
        RunControl::new(Duration::from_secs(5)),
    )
    .await
    .unwrap();
    assert_eq!(
        result["reference"],
        serde_json::to_value(client.reference()).unwrap()
    );
    assert_eq!(client.puts.load(Ordering::SeqCst), 1);
    assert_eq!(client.reconciliations.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn malformed_or_rejected_upload_never_becomes_recoverable_runtime_success() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source");
    std::fs::write(&path, b"artifact").unwrap();
    for error in [
        ArtifactClientError::InvalidResponse {
            message: "changed identity".into(),
        },
        ArtifactClientError::Rejected(ContractError::OwnershipLost),
    ] {
        let mut client = Client::new(b"artifact");
        client.upload_error = Some(error);
        let result = invoke(&client,"artifacts.put_file",serde_json::json!({"path":path,"key":"file","media_type":"application/octet-stream","filename":null})).await;
        assert!(result.is_err());
        assert_eq!(client.puts.load(Ordering::SeqCst), 1);
        assert_eq!(client.reconciliations.load(Ordering::SeqCst), 0);
    }
}
