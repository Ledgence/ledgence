//! Artifact orchestration owns transfer lifetime; persistence transactions never
//! span byte IO. Concrete temporary-file behavior belongs to the spool adapter.
use ledgence_orchestration_api::*;
use sha2::{Digest as _, Sha256};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex as AsyncMutex, Notify, oneshot};

#[derive(Clone)]
pub struct ArtifactService {
    inner: Arc<Inner>,
}
struct Inner {
    catalog: Arc<dyn ExecutionArtifactCatalog>,
    blobs: Arc<dyn ExecutionArtifactBlobStore>,
    spools: Arc<dyn ArtifactSpoolFactory>,
    transfers: Mutex<Transfers>,
    finished: Notify,
}
#[derive(Default)]
struct Transfers {
    closed: bool,
    active: usize,
}
struct TransferGuard(Arc<Inner>);
impl Drop for TransferGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.transfers.lock() {
            state.active -= 1;
        }
        self.0.finished.notify_waiters();
    }
}
impl ArtifactService {
    pub async fn new(
        catalog: Arc<dyn ExecutionArtifactCatalog>,
        blobs: Arc<dyn ExecutionArtifactBlobStore>,
        spools: Arc<dyn ArtifactSpoolFactory>,
        deadline: Instant,
    ) -> Result<Self> {
        catalog.bind_store(blobs.identity(), deadline).await?;
        Ok(Self {
            inner: Arc::new(Inner {
                catalog,
                blobs,
                spools,
                transfers: Mutex::new(Transfers::default()),
                finished: Notify::new(),
            }),
        })
    }
    fn admit(&self) -> Result<TransferGuard> {
        let mut state = self
            .inner
            .transfers
            .lock()
            .map_err(|_| unavailable("artifact transfer state unavailable"))?;
        if state.closed {
            return Err(unavailable("artifact service is draining"));
        }
        // IO admission itself is acquired by the spool factory. Bound retained
        // coordination tasks as well, before any async work can be queued.
        if state.active >= 6 {
            return Err(ContractError::Busy);
        }
        state.active += 1;
        Ok(TransferGuard(self.inner.clone()))
    }
}
impl ExecutionArtifactService for ArtifactService {
    fn prepare<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        c: &'a ArtifactPrepareCommand,
        d: Instant,
    ) -> ContractFuture<'a, ArtifactPrepareReply> {
        self.inner.catalog.prepare(a, c, d)
    }
    fn upload_status<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        id: &'a str,
        d: Instant,
    ) -> ContractFuture<'a, ArtifactUploadStatus> {
        self.inner.catalog.upload_status(a, id, d)
    }
    fn replace<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        c: &'a ArtifactReplaceCommand,
        d: Instant,
    ) -> ContractFuture<'a, ArtifactReplacementReceipt> {
        self.inner.catalog.replace(a, c, d)
    }
    fn put(
        &self,
        authority: ArtifactAuthority,
        selection: ArtifactSelection,
        body: Box<dyn ArtifactReader>,
        deadline: Instant,
    ) -> ContractFuture<'_, ArtifactUploadReply> {
        Box::pin(async move {
            let guard = self.admit()?;
            let inner = self.inner.clone();
            let deadline = transfer_deadline(deadline)?;
            let (sender, receiver) = oneshot::channel();
            // Dropping a request does not cancel an authorized persistence
            // operation. Shutdown drains these owned jobs before the runtime.
            tokio::spawn(async move {
                let _guard = guard;
                let result = put(inner, authority, selection, body, deadline).await;
                let _ = sender.send(result);
            });
            receiver
                .await
                .map_err(|_| unavailable("artifact transfer ended without a receipt"))?
        })
    }
    fn reconcile<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        s: &'a ArtifactSelection,
        d: Instant,
    ) -> ContractFuture<'a, ArtifactUploadReply> {
        Box::pin(async move {
            let _guard = self.admit()?;
            let d = transfer_deadline(d)?;
            let prior = self.inner.catalog.upload_status(a, &s.upload_id, d).await?;
            if prior.selection != *s {
                return Err(ContractError::Conflict);
            }
            if !matches!(
                prior.stage,
                ArtifactUploadStage::Writing | ArtifactUploadStage::Uncertain
            ) {
                return Ok(ArtifactUploadReply { upload: prior });
            }
            let object = self.inner.catalog.upload_object(a, s, d).await?;
            let observed = observe(&self.inner, &object, d).await?;
            let status = self.inner.catalog.upload_status(a, &s.upload_id, d).await?;
            if status.selected && observed == ArtifactStorageObservation::VerifiedComplete {
                match self.inner.catalog.accept(a, s, d).await {
                    Ok(_) => (),
                    Err(ContractError::Busy) => (),
                    Err(e) => return Err(e),
                }
            }
            Ok(ArtifactUploadReply {
                upload: self.inner.catalog.upload_status(a, &s.upload_id, d).await?,
            })
        })
    }
    fn inspect<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        q: &'a ArtifactInspectQuery,
        d: Instant,
    ) -> ContractFuture<'a, ArtifactManifest> {
        Box::pin(async move {
            q.validate()?;
            if a.scope() != &q.scope {
                return Err(ContractError::NotFound);
            }
            Ok(self
                .inner
                .catalog
                .resolve(a, &q.artifact_id, ArtifactReadKind::Metadata, d)
                .await?
                .manifest)
        })
    }
    fn list<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        q: &'a ArtifactListQuery,
        d: Instant,
    ) -> ContractFuture<'a, ArtifactPage> {
        self.inner.catalog.list(a, q, d)
    }
    fn prepare_download<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        r: &'a ArtifactRef,
        d: Instant,
    ) -> ContractFuture<'a, PreparedArtifactDownload> {
        Box::pin(async move {
            let guard = self.admit()?;
            r.validate()?;
            let d = transfer_deadline(d)?;
            let record = self
                .inner
                .catalog
                .resolve(a, &r.id, ArtifactReadKind::Content, d)
                .await?;
            if &record.manifest.reference != r {
                return Err(ContractError::Conflict);
            }
            let writer = self
                .inner
                .spools
                .create(ArtifactSpoolPurpose::Download, r.size, d)
                .await?;
            let blob = within(d, self.inner.blobs.open(&record.object, d))
                .await?
                .ok_or_else(|| unavailable("artifact bytes are unavailable"))?;
            let reader = verify(blob.reader, &record.object.metadata, Some(writer), d)
                .await?
                .ok_or_else(|| unavailable("artifact verification lost spool"))?;
            let reader = Box::new(DownloadReader::new(reader, guard, d));
            Ok(PreparedArtifactDownload::new(record.manifest, reader, d))
        })
    }
    fn release_download<'a>(
        &'a self,
        a: &'a ArtifactAuthority,
        p: PreparedArtifactDownload,
        d: Instant,
    ) -> ContractFuture<'a, ArtifactDownload> {
        Box::pin(async move {
            let d = d.min(p.deadline());
            if Instant::now() >= d {
                return Err(unavailable("artifact download deadline elapsed"));
            }
            let current = self
                .inner
                .catalog
                .resolve(a, &p.manifest().reference.id, ArtifactReadKind::Content, d)
                .await?;
            if current.manifest != *p.manifest() {
                return Err(ContractError::Conflict);
            }
            // This is the last authorization after HTTP has committed audit.
            // The body still carries its original, non-extendable deadline.
            Ok(p.into_download())
        })
    }
    fn maintain(&self, limit: u32, d: Instant) -> ContractFuture<'_, ArtifactMaintenanceProgress> {
        Box::pin(async move {
            let _guard = self.admit()?;
            let work = self
                .inner
                .catalog
                .claim_maintenance(self.inner.blobs.identity(), limit, d)
                .await?;
            let mut inspected = 0;
            let mut deleted = 0;
            for item in work {
                match item {
                    ArtifactMaintenanceWork::Inspect(lease) => {
                        observe(&self.inner, &lease.object, d).await?;
                        inspected += 1;
                    }
                    ArtifactMaintenanceWork::Delete(lease) => {
                        let result = within(d, self.inner.blobs.delete(&lease.object, d)).await;
                        self.inner
                            .catalog
                            .finish_delete(
                                &lease,
                                if result.is_ok() {
                                    ArtifactDeleteObservation::Deleted
                                } else {
                                    ArtifactDeleteObservation::Failed
                                },
                                d,
                            )
                            .await?;
                        result?;
                        deleted += 1;
                    }
                }
            }
            let mut progress = self.inner.catalog.maintenance_progress(d).await?;
            progress.inspected = inspected;
            progress.deleted = deleted;
            Ok(progress)
        })
    }
    fn drain(&self, d: Instant) -> ContractFuture<'_, ()> {
        Box::pin(async move {
            self.inner
                .transfers
                .lock()
                .map_err(|_| unavailable("artifact transfer state unavailable"))?
                .closed = true;
            loop {
                let notified = self.inner.finished.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self
                    .inner
                    .transfers
                    .lock()
                    .map_err(|_| unavailable("artifact transfer state unavailable"))?
                    .active
                    == 0
                {
                    break;
                }
                within(d, async {
                    notified.await;
                    Ok(())
                })
                .await?;
            }
            // Attempt both drains even when one reports incomplete cleanup.
            // Each closes admission and retains pending resources on timeout.
            let blobs = self.inner.blobs.drain(d).await;
            let spools = self.inner.spools.drain(d).await;
            blobs?;
            spools
        })
    }
}
struct DownloadReader {
    reader: Box<dyn ArtifactReader>,
    guard: Arc<Mutex<Option<TransferGuard>>>,
    deadline: Instant,
}
impl DownloadReader {
    fn new(reader: Box<dyn ArtifactReader>, guard: TransferGuard, deadline: Instant) -> Self {
        let guard = Arc::new(Mutex::new(Some(guard)));
        let weak = Arc::downgrade(&guard);
        // Expire this service's accounting even when HTTP stops polling the body.
        // Connection drain follows the orchestrator's separate shutdown policy;
        // concrete spool expiration still retains outstanding physical IO.
        tokio::spawn(async move {
            tokio::time::sleep_until(deadline.into()).await;
            if let Some(guard) = weak.upgrade()
                && let Ok(mut value) = guard.lock()
            {
                value.take();
            }
        });
        Self {
            reader,
            guard,
            deadline,
        }
    }
}
impl ArtifactReader for DownloadReader {
    fn read_chunk(&mut self, max: usize) -> ContractFuture<'_, Vec<u8>> {
        Box::pin(async move {
            let result = within(self.deadline, self.reader.read_chunk(max)).await;
            if result.as_ref().is_ok_and(Vec::is_empty)
                && let Ok(mut guard) = self.guard.lock()
            {
                guard.take();
            }
            result
        })
    }
}
struct SharedReader(Arc<AsyncMutex<Box<dyn ArtifactReader>>>);
impl ArtifactReader for SharedReader {
    fn read_chunk(&mut self, max: usize) -> ContractFuture<'_, Vec<u8>> {
        Box::pin(async move { self.0.lock().await.read_chunk(max).await })
    }
}
async fn put(
    inner: Arc<Inner>,
    authority: ArtifactAuthority,
    selection: ArtifactSelection,
    body: Box<dyn ArtifactReader>,
    d: Instant,
) -> Result<ArtifactUploadReply> {
    selection.validate()?;
    let prior = inner
        .catalog
        .upload_status(&authority, &selection.upload_id, d)
        .await?;
    if prior.selection != selection || !prior.selected {
        return Err(ContractError::Conflict);
    }
    if prior.stage == ArtifactUploadStage::Ready {
        return Ok(ArtifactUploadReply { upload: prior });
    }
    let claim = inner
        .catalog
        .claim_ingress(&authority, &selection, d)
        .await?;
    if claim.object.store != *inner.blobs.identity() {
        // A still-running service may retain its old adapter after an operator
        // rebinds an otherwise empty catalog. Fail before irreversible dispatch,
        // leaving the generation usable by the service with the new adapter.
        inner
            .catalog
            .release_ingress(&claim, Instant::now() + Duration::from_secs(5))
            .await?;
        return Err(unavailable(
            "artifact adapter no longer matches the configured store",
        ));
    }
    let spool = async {
        let writer = inner
            .spools
            .create(ArtifactSpoolPurpose::Upload, claim.object.metadata.size, d)
            .await?;
        verify(body, &claim.object.metadata, Some(writer), d)
            .await?
            .ok_or_else(|| unavailable("artifact upload lost spool"))
    }
    .await;
    let reader = match spool {
        Ok(reader) => reader,
        Err(error) => {
            // Recovery has its own short DB deadline; it does not extend permission
            // to dispatch a write after the original transfer deadline.
            let _ = inner
                .catalog
                .release_ingress(&claim, Instant::now() + Duration::from_secs(5))
                .await;
            return Err(error);
        }
    };
    match inner.catalog.begin_write(&authority, &claim, d).await {
        Ok(ArtifactBeginWrite::Dispatch(permit)) => {
            if permit.selection != selection || permit.object != claim.object {
                return Err(unavailable(
                    "artifact dispatch permission does not match ingress",
                ));
            }
        }
        Ok(ArtifactBeginWrite::ReconcileRequired(upload)) => {
            return Ok(ArtifactUploadReply { upload });
        }
        Err(error) => {
            let _ = inner
                .catalog
                .release_ingress(&claim, Instant::now() + Duration::from_secs(5))
                .await;
            return Err(error);
        }
    }
    let retained = Arc::new(AsyncMutex::new(reader));
    let outcome = within(
        d,
        inner
            .blobs
            .put_once(&claim.object, Box::new(SharedReader(retained.clone())), d),
    )
    .await;
    let observation = match outcome {
        Ok(BlobPutOutcome::Created) => ArtifactStorageObservation::VerifiedComplete,
        Ok(BlobPutOutcome::ConfirmedNoWrite) => ArtifactStorageObservation::ConfirmedNoWrite,
        _ => ArtifactStorageObservation::Uncertain,
    };
    // Persist known physical completion independently of stale execution
    // authority. This never accepts bytes or reselects a generation.
    inner
        .catalog
        .record_storage_observation(
            &selection.upload_id,
            observation,
            Instant::now() + Duration::from_secs(5),
        )
        .await?;
    if observation == ArtifactStorageObservation::VerifiedComplete {
        inner.catalog.accept(&authority, &selection, d).await?;
    }
    let upload = inner
        .catalog
        .upload_status(&authority, &selection.upload_id, d)
        .await?;
    drop(retained);
    Ok(ArtifactUploadReply { upload })
}
async fn observe(
    inner: &Inner,
    object: &ArtifactObjectDescriptor,
    d: Instant,
) -> Result<ArtifactStorageObservation> {
    let observed = match within(d, inner.blobs.open(object, d)).await? {
        None => ArtifactStorageObservation::Absent,
        Some(blob) => match verify(
            blob.reader,
            &object.metadata,
            Some(
                inner
                    .spools
                    .create(ArtifactSpoolPurpose::Download, object.metadata.size, d)
                    .await?,
            ),
            d,
        )
        .await
        {
            Ok(_) => ArtifactStorageObservation::VerifiedComplete,
            Err(ContractError::InvalidInput(_)) => ArtifactStorageObservation::Corrupt,
            Err(e) => return Err(e),
        },
    };
    inner
        .catalog
        .record_storage_observation(&object.upload_id, observed, d)
        .await?;
    Ok(observed)
}
async fn verify(
    mut reader: Box<dyn ArtifactReader>,
    metadata: &ArtifactMetadata,
    mut writer: Option<Box<dyn ArtifactSpoolWriter>>,
    d: Instant,
) -> Result<Option<Box<dyn ArtifactReader>>> {
    let mut size = 0u64;
    let mut hash = Sha256::new();
    loop {
        let bytes = within(d, reader.read_chunk(ARTIFACT_CHUNK_MAX_BYTES)).await?;
        if bytes.len() > ARTIFACT_CHUNK_MAX_BYTES {
            return Err(ContractError::InvalidInput(
                "artifact reader exceeded chunk bound".into(),
            ));
        }
        if bytes.is_empty() {
            break;
        }
        size = size
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| ContractError::InvalidInput("artifact size overflow".into()))?;
        if size > metadata.size {
            return Err(ContractError::InvalidInput(
                "artifact content exceeds declared size".into(),
            ));
        }
        hash.update(&bytes);
        if let Some(writer) = writer.as_mut() {
            within(d, writer.append(bytes)).await?;
        }
    }
    if size != metadata.size || format!("sha256:{:x}", hash.finalize()) != metadata.digest.0 {
        return Err(ContractError::InvalidInput(
            "artifact content does not match declared size and SHA-256".into(),
        ));
    }
    match writer {
        Some(writer) => Ok(Some(within(d, writer.finish()).await?)),
        None => Ok(None),
    }
}
fn transfer_deadline(deadline: Instant) -> Result<Instant> {
    if deadline <= Instant::now() {
        Err(unavailable("artifact transfer deadline elapsed"))
    } else {
        Ok(deadline.min(Instant::now() + Duration::from_millis(ARTIFACT_TRANSFER_TIMEOUT_MS)))
    }
}
async fn within<T>(d: Instant, f: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    if Instant::now() >= d {
        return Err(unavailable("artifact transfer deadline elapsed"));
    }
    let result = tokio::time::timeout_at(d.into(), f)
        .await
        .map_err(|_| unavailable("artifact transfer deadline elapsed"))?;
    if Instant::now() >= d {
        return Err(unavailable("artifact transfer deadline elapsed"));
    }
    result
}
fn unavailable(message: &str) -> ContractError {
    ContractError::Unavailable(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    struct Reader(VecDeque<Vec<u8>>);
    impl ArtifactReader for Reader {
        fn read_chunk(&mut self, _: usize) -> ContractFuture<'_, Vec<u8>> {
            Box::pin(async move { Ok(self.0.pop_front().unwrap_or_default()) })
        }
    }
    fn body(chunks: Vec<Vec<u8>>) -> Box<dyn ArtifactReader> {
        Box::new(Reader(chunks.into()))
    }
    fn metadata(bytes: &[u8]) -> ArtifactMetadata {
        ArtifactMetadata {
            digest: ledgence_worker_api::Digest(format!("sha256:{:x}", Sha256::digest(bytes))),
            size: bytes.len() as u64,
            media_type: "application/octet-stream".into(),
            filename: None,
        }
    }
    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(2)
    }
    #[tokio::test]
    async fn verifies_exact_hash_size_empty_and_chunked_content() {
        assert!(
            verify(body(vec![]), &metadata(b""), None, deadline())
                .await
                .is_ok()
        );
        assert!(
            verify(
                body(vec![b"hel".to_vec(), b"lo".to_vec()]),
                &metadata(b"hello"),
                None,
                deadline()
            )
            .await
            .is_ok()
        );
        for chunks in [
            vec![b"hell".to_vec()],
            vec![b"helloo".to_vec()],
            vec![b"jello".to_vec()],
        ] {
            assert!(matches!(
                verify(body(chunks), &metadata(b"hello"), None, deadline()).await,
                Err(ContractError::InvalidInput(_))
            ));
        }
    }
    #[tokio::test]
    async fn rejects_reader_contract_violations_before_spooling() {
        let bytes = vec![b'x'; ARTIFACT_CHUNK_MAX_BYTES + 1];
        assert!(matches!(
            verify(
                body(vec![bytes.clone()]),
                &metadata(&bytes),
                None,
                deadline()
            )
            .await,
            Err(ContractError::InvalidInput(_))
        ));
    }
    struct SlowReader;
    impl ArtifactReader for SlowReader {
        fn read_chunk(&mut self, _: usize) -> ContractFuture<'_, Vec<u8>> {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(vec![b'x'])
            })
        }
    }
    #[tokio::test]
    async fn continuous_small_chunks_cannot_extend_absolute_deadline() {
        let result = verify(
            Box::new(SlowReader),
            &metadata(&[b'x'; 1000]),
            None,
            Instant::now() + Duration::from_millis(30),
        )
        .await;
        assert!(matches!(result, Err(ContractError::Unavailable(_))));
    }
}
