use super::*;
use sha2::{Digest as _, Sha256};
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicUsize, Ordering},
};

fn scope() -> Scope {
    Scope {
        tenant_id: "acme".into(),
        namespace: "test".into(),
    }
}
fn selection() -> ArtifactSelection {
    ArtifactSelection {
        artifact_id: "art_report".into(),
        upload_id: "upload_1".into(),
        revision: 1,
    }
}
fn reference(bytes: &[u8]) -> ArtifactRef {
    ArtifactRef {
        schema: "ledgence.artifact.v1".into(),
        id: selection().artifact_id,
        digest: ledgence_worker_api::Digest(format!("sha256:{:x}", Sha256::digest(bytes))),
        size: bytes.len() as u64,
        media_type: "application/octet-stream".into(),
    }
}
fn status(bytes: &[u8], ready: bool) -> ArtifactUploadStatus {
    ArtifactUploadStatus {
        selection: selection(),
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
        reference: ready.then(|| reference(bytes)),
        retry: if ready {
            ArtifactRetryGuidance::Complete
        } else {
            ArtifactRetryGuidance::Upload
        },
    }
}
fn manifest(bytes: &[u8]) -> ArtifactManifest {
    ArtifactManifest {
        reference: reference(bytes),
        scope: scope(),
        root: ExecutionTarget::Task { id: "task".into() },
        producer: ArtifactProducer::TrustedInstance {
            instance_id: "instance".into(),
        },
        key: "report".into(),
        filename: None,
        created_at: 1,
        accepted_at: 2,
        first_attempt_id: None,
    }
}
fn args(command: &str, extra: &[&str]) -> Vec<String> {
    [
        vec![
            command,
            "--server",
            "http://localhost",
            "--tenant",
            "acme",
            "--namespace",
            "test",
        ],
        extra.to_vec(),
    ]
    .concat()
    .into_iter()
    .map(str::to_owned)
    .collect()
}
fn options(operation: Operation) -> Options {
    Options {
        server: "http://localhost".into(),
        scope: scope(),
        operation,
        timeout: Duration::from_secs(10),
    }
}
struct Client {
    data: Vec<u8>,
    replay: bool,
    prepare_status: Option<ArtifactUploadStatus>,
    put_status: Option<ArtifactUploadStatus>,
    error: Option<ArtifactClientError>,
    puts: AtomicUsize,
    recoveries: AtomicUsize,
}
impl Client {
    fn new(data: &[u8]) -> Self {
        Self {
            data: data.into(),
            replay: false,
            prepare_status: None,
            put_status: None,
            error: None,
            puts: AtomicUsize::new(0),
            recoveries: AtomicUsize::new(0),
        }
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
            assert!(
                matches!(requester, ArtifactRequester::External { scope: actual } if actual == &scope())
            );
            assert_eq!(command.metadata.digest, reference(&self.data).digest);
            assert_eq!(command.metadata.size, self.data.len() as u64);
            Ok(ArtifactPrepareReply {
                upload: self
                    .prepare_status
                    .clone()
                    .unwrap_or_else(|| status(&self.data, self.replay)),
            })
        })
    }
    fn put(
        &self,
        _: ArtifactRequester,
        actual: ArtifactSelection,
        mut reader: Box<dyn ArtifactReader>,
        _: Instant,
    ) -> ArtifactClientFuture<'_, ArtifactUploadReply> {
        Box::pin(async move {
            assert_eq!(actual, selection());
            self.puts.fetch_add(1, Ordering::SeqCst);
            let mut data = Vec::new();
            loop {
                let part = reader.read_chunk(32768).await?;
                if part.is_empty() {
                    break;
                }
                assert!(part.len() <= 32768);
                data.extend(part);
            }
            assert_eq!(data, self.data);
            if let Some(e) = &self.error {
                return Err(e.clone());
            }
            Ok(ArtifactUploadReply {
                upload: self
                    .put_status
                    .clone()
                    .unwrap_or_else(|| status(&self.data, true)),
            })
        })
    }
    fn upload_status<'a>(
        &'a self,
        _: &'a ArtifactRequester,
        _: &'a str,
        _: Instant,
    ) -> ArtifactClientFuture<'a, ArtifactUploadStatus> {
        Box::pin(async { panic!("no implicit status requests") })
    }
    fn reconcile<'a>(
        &'a self,
        _: &'a ArtifactRequester,
        actual: &'a ArtifactSelection,
        _: Instant,
    ) -> ArtifactClientFuture<'a, ArtifactUploadReply> {
        Box::pin(async move {
            assert_eq!(actual, &selection());
            self.recoveries.fetch_add(1, Ordering::SeqCst);
            Ok(ArtifactUploadReply {
                upload: status(&self.data, true),
            })
        })
    }
    fn replace<'a>(
        &'a self,
        _: &'a ArtifactRequester,
        _: &'a ArtifactReplaceCommand,
        _: Instant,
    ) -> ArtifactClientFuture<'a, ArtifactReplacementReceipt> {
        Box::pin(async { panic!("no hidden replacement") })
    }
    fn download(
        &self,
        _: ArtifactRequester,
        _: ArtifactRef,
        _: Instant,
    ) -> ArtifactClientFuture<'_, ArtifactDownload> {
        Box::pin(async { panic!("not used") })
    }
}
impl ArtifactMetadataReader for Client {
    fn inspect<'a>(
        &'a self,
        _: &'a ArtifactInspectQuery,
        _: Instant,
    ) -> ContractFuture<'a, ArtifactManifest> {
        Box::pin(async { panic!("not used") })
    }
    fn list<'a>(
        &'a self,
        _: &'a ArtifactListQuery,
        _: Instant,
    ) -> ContractFuture<'a, ArtifactPage> {
        Box::pin(async { panic!("not used") })
    }
}
#[test]
fn parser_rejects_ambiguous_targets_invalid_revisions_and_unbounded_transfers() {
    for extra in [
        vec![
            "--task",
            "task",
            "--workflow",
            "workflow",
            "--key",
            "k",
            "--file",
            "a",
        ],
        vec![
            "--task",
            "task",
            "--key",
            "k",
            "--file",
            "a",
            "--timeout-ms",
            "120001",
        ],
        vec!["--task", "task", "--key", "k", "--file", "a", "--file", "b"],
    ] {
        assert!(parse(args("put", &extra)).is_err());
    }
    assert!(
        parse(args(
            "replace",
            &["--upload", "up", "--expected-revision", "0"]
        ))
        .is_err()
    );
    assert!(parse(args("inspect", &["--artifact", "art", "--key", "k"])).is_err());
    assert!(parse(args("list", &["--workflow", "wf", "--limit", "101"])).is_err());
    assert!(
        parse(args(
            "put",
            &["--task", "task", "--key", "k", "--file", "a"]
        ))
        .is_ok()
    );
}
#[tokio::test]
async fn put_streams_empty_and_multichunk_files_and_replay_never_resends() {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("source");
    for data in [vec![], vec![42; ARTIFACT_CHUNK_MAX_BYTES + 7]] {
        std::fs::write(&file, &data).unwrap();
        let mut client = Client::new(&data);
        for replay in [false, true] {
            client.replay = replay;
            let result = execute(
                &client,
                options(Operation::Put {
                    target: ExecutionTarget::Task { id: "task".into() },
                    key: "report".into(),
                    file: file.clone(),
                    media_type: "application/octet-stream".into(),
                }),
                Arc::new(Mutex::new(None)),
            )
            .await
            .unwrap();
            assert_eq!(result, json!(reference(&data)));
        }
        assert_eq!(client.puts.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn uncertain_put_preserves_selection_and_requires_explicit_reconcile() {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("source");
    std::fs::write(&file, b"value").unwrap();
    let mut client = Client::new(b"value");
    client.error = Some(ArtifactClientError::OutcomeUnknown {
        message: "lost ACK".into(),
    });
    let saved = Arc::new(Mutex::new(None));
    assert!(matches!(
        execute(
            &client,
            options(Operation::Put {
                target: ExecutionTarget::Task { id: "task".into() },
                key: "report".into(),
                file,
                media_type: "application/octet-stream".into()
            }),
            saved.clone()
        )
        .await,
        Err(ArtifactClientError::OutcomeUnknown { .. })
    ));
    assert_eq!(*saved.lock().unwrap(), Some(selection()));
    assert_eq!(client.puts.load(Ordering::SeqCst), 1);
    assert_eq!(client.recoveries.load(Ordering::SeqCst), 0);
    execute(
        &client,
        options(Operation::Reconcile {
            selection: selection(),
        }),
        saved,
    )
    .await
    .unwrap();
    assert_eq!(client.recoveries.load(Ordering::SeqCst), 1);
    assert_eq!(client.puts.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn pending_status_preserves_uncertainty_without_resending_or_recovering() {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("source");
    std::fs::write(&file, b"value").unwrap();
    for stage in [
        ArtifactUploadStage::Receiving,
        ArtifactUploadStage::Writing,
        ArtifactUploadStage::Uncertain,
    ] {
        for during_prepare in [false, true] {
            let mut pending = status(b"value", false);
            pending.stage = stage.clone();
            pending.ingress_expires_at =
                (stage == ArtifactUploadStage::Receiving).then_some(120_000);
            pending.dispatch_expires_at =
                (stage != ArtifactUploadStage::Receiving).then_some(120_000);
            pending.retry = if stage == ArtifactUploadStage::Receiving {
                ArtifactRetryGuidance::Wait
            } else {
                ArtifactRetryGuidance::Reconcile
            };
            let mut client = Client::new(b"value");
            if during_prepare {
                client.prepare_status = Some(pending);
            } else {
                client.put_status = Some(pending);
            }
            let saved = Arc::new(Mutex::new(None));
            let error = execute(
                &client,
                options(Operation::Put {
                    target: ExecutionTarget::Task { id: "task".into() },
                    key: "report".into(),
                    file: file.clone(),
                    media_type: "application/octet-stream".into(),
                }),
                saved.clone(),
            )
            .await
            .unwrap_err();
            assert_eq!(
                matches!(error, ArtifactClientError::OutcomeUnknown { .. }),
                stage != ArtifactUploadStage::Receiving
            );
            assert_eq!(*saved.lock().unwrap(), Some(selection()));
            assert_eq!(
                client.puts.load(Ordering::SeqCst),
                usize::from(!during_prepare)
            );
            assert_eq!(client.recoveries.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn malformed_pending_upload_is_invalid_response_not_recoverable() {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("source");
    std::fs::write(&file, b"value").unwrap();
    let mut client = Client::new(b"value");
    let mut invalid = status(b"value", false);
    invalid.stage = ArtifactUploadStage::Uncertain;
    invalid.retry = ArtifactRetryGuidance::Reconcile;
    // Missing dispatch provenance makes this a malformed response.
    client.put_status = Some(invalid);
    assert!(matches!(
        execute(
            &client,
            options(Operation::Put {
                target: ExecutionTarget::Task { id: "task".into() },
                key: "report".into(),
                file,
                media_type: "application/octet-stream".into(),
            }),
            Arc::new(Mutex::new(None))
        )
        .await,
        Err(ArtifactClientError::InvalidResponse { .. })
    ));
}

struct Reader {
    chunks: VecDeque<Vec<u8>>,
    race: Option<PathBuf>,
}
impl ArtifactReader for Reader {
    fn read_chunk(&mut self, _: usize) -> ContractFuture<'_, Vec<u8>> {
        Box::pin(async move {
            if let Some(path) = self.race.take() {
                std::fs::write(path, b"other writer").unwrap();
            }
            Ok(self.chunks.pop_front().unwrap_or_default())
        })
    }
}
#[tokio::test]
async fn downloads_verify_bytes_cleanup_partial_and_never_overwrite_a_race() {
    for (bytes, race, success) in [
        (b"value".to_vec(), false, true),
        (b"wrong".to_vec(), false, false),
        (b"value".to_vec(), true, false),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("result");
        let deadline = Instant::now() + Duration::from_secs(3);
        let body = ArtifactDownload {
            manifest: manifest(b"value"),
            reader: Box::new(Reader {
                chunks: vec![bytes].into(),
                race: race.then(|| path.clone()),
            }),
            deadline,
        };
        let result = files::download(body, &reference(b"value"), path.clone(), deadline).await;
        assert_eq!(result.is_ok(), success);
        if success {
            assert_eq!(std::fs::read(&path).unwrap(), b"value");
        } else if race {
            assert_eq!(std::fs::read(&path).unwrap(), b"other writer");
        } else {
            assert!(!path.exists());
        }
        assert_eq!(
            std::fs::read_dir(temp.path()).unwrap().count(),
            usize::from(success || race)
        );
    }
}
#[tokio::test]
async fn sources_reject_links_oversize_changes_and_expired_deadline() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("source");
    std::fs::write(&path, b"value").unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    assert!(
        files::prepare(
            path.clone(),
            "application/octet-stream".into(),
            Instant::now()
        )
        .await
        .is_err()
    );
    let (mut reader, _) = files::prepare(path.clone(), "application/octet-stream".into(), deadline)
        .await
        .unwrap();
    std::fs::write(&path, b"too large").unwrap();
    assert!(reader.read_chunk(32).await.is_err());
    std::fs::File::create(&path)
        .unwrap()
        .set_len(ARTIFACT_MAX_BYTES + 1)
        .unwrap();
    assert!(
        files::prepare(path.clone(), "application/octet-stream".into(), deadline)
            .await
            .is_err()
    );
    #[cfg(unix)]
    {
        let link = temp.path().join("link");
        std::os::unix::fs::symlink(path, &link).unwrap();
        assert!(
            files::prepare(link, "application/octet-stream".into(), deadline)
                .await
                .is_err()
        );
    }
}
