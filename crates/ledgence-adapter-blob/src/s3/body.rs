use super::*;
use http_body::{Body, Frame, SizeHint};
use ledgence_orchestration_api::{ArtifactMetadata, ContractError};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
type PendingRead = Pin<Box<dyn Future<Output = (Box<dyn ArtifactReader>, Result<Vec<u8>>)> + Send>>;
pub(super) struct StreamingBody {
    state: Mutex<State>,
}
struct State {
    reader: Option<Box<dyn ArtifactReader>>,
    pending: Option<PendingRead>,
    deadline: Instant,
    remaining: u64,
    digest: Digest,
    hash: Sha256,
    done: bool,
    verified: Arc<AtomicBool>,
}
impl StreamingBody {
    pub(super) fn new(
        reader: Box<dyn ArtifactReader>,
        metadata: ArtifactMetadata,
        deadline: Instant,
    ) -> (Self, Arc<AtomicBool>) {
        let verified = Arc::new(AtomicBool::new(
            metadata.size == 0 && metadata.digest.0 == format!("sha256:{:x}", Sha256::digest([])),
        ));
        (
            Self {
                state: Mutex::new(State {
                    reader: Some(reader),
                    pending: None,
                    deadline,
                    remaining: metadata.size,
                    digest: metadata.digest,
                    hash: Sha256::new(),
                    done: false,
                    verified: verified.clone(),
                }),
            },
            verified,
        )
    }
}
impl Body for StreamingBody {
    type Data = Bytes;
    type Error = ContractError;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>>>> {
        let Ok(mut state) = self.state.lock() else {
            return Poll::Ready(Some(Err(unavailable(
                "artifact S3 request body unavailable",
            ))));
        };
        if state.done {
            return Poll::Ready(None);
        }
        if check_deadline(state.deadline).is_err() {
            state.done = true;
            return Poll::Ready(Some(Err(unavailable(
                "artifact S3 upload deadline exceeded",
            ))));
        }
        if state.pending.is_none() {
            let Some(mut reader) = state.reader.take() else {
                state.done = true;
                return Poll::Ready(Some(Err(unavailable(
                    "artifact S3 upload reader unavailable",
                ))));
            };
            let deadline = state.deadline;
            state.pending = Some(Box::pin(async move {
                let result =
                    tokio::time::timeout_at(deadline.into(), reader.read_chunk(CHUNK_BYTES))
                        .await
                        .map_err(|_| unavailable("artifact S3 upload body deadline exceeded"))
                        .and_then(|result| result);
                (reader, result)
            }));
        }
        let Some(pending) = state.pending.as_mut() else {
            return Poll::Ready(Some(Err(unavailable("artifact S3 body state unavailable"))));
        };
        let (reader, result) = match pending.as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        state.pending = None;
        state.reader = Some(reader);
        let chunk = match result {
            Ok(chunk) => chunk,
            Err(error) => {
                state.done = true;
                return Poll::Ready(Some(Err(error)));
            }
        };
        if chunk.len() > CHUNK_BYTES || chunk.len() as u64 > state.remaining {
            state.done = true;
            return Poll::Ready(Some(Err(invalid(
                "artifact S3 upload body exceeds declared length",
            ))));
        }
        if chunk.is_empty() {
            state.done = true;
            if state.remaining != 0
                || format!("sha256:{:x}", state.hash.clone().finalize()) != state.digest.0
            {
                return Poll::Ready(Some(Err(invalid("artifact S3 upload integrity mismatch"))));
            }
            state.verified.store(true, Ordering::Release);
            return Poll::Ready(None);
        }
        state.remaining -= chunk.len() as u64;
        state.hash.update(&chunk);
        if state.remaining == 0 {
            if format!("sha256:{:x}", state.hash.clone().finalize()) != state.digest.0 {
                state.done = true;
                return Poll::Ready(Some(Err(invalid("artifact S3 upload integrity mismatch"))));
            }
            state.verified.store(true, Ordering::Release);
        }
        Poll::Ready(Some(Ok(Frame::data(Bytes::from(chunk)))))
    }
    fn is_end_stream(&self) -> bool {
        self.state.lock().is_ok_and(|state| state.done)
    }
    fn size_hint(&self) -> SizeHint {
        self.state
            .lock()
            .map(|state| SizeHint::with_exact(state.remaining))
            .unwrap_or_default()
    }
}
