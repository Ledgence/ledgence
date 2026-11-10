//! Streaming bridge: portable readers stay outside HTTP types and never become
//! an unbounded channel or a detached upload task.
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use ledgence_orchestration_api::{
    ARTIFACT_CHUNK_MAX_BYTES, ARTIFACT_MAX_BYTES, ArtifactReader, ContractError, Result,
};
use std::{
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
    time::Instant,
};

type ReadFuture = Pin<Box<dyn Future<Output = (Box<dyn ArtifactReader>, Result<Vec<u8>>)> + Send>>;
struct State {
    reader: Option<Box<dyn ArtifactReader>>,
    pending: Option<ReadFuture>,
    sent: u64,
    done: bool,
}
pub(crate) struct ReaderBody {
    state: Mutex<State>,
    size: Option<u64>,
    deadline: Instant,
}
impl ReaderBody {
    pub(crate) fn new(
        reader: Box<dyn ArtifactReader>,
        size: Option<u64>,
        deadline: Instant,
    ) -> Self {
        Self {
            state: Mutex::new(State {
                reader: Some(reader),
                pending: None,
                sent: 0,
                done: false,
            }),
            size,
            deadline,
        }
    }
}
fn failed(message: &'static str) -> ContractError {
    ContractError::Unavailable(message.into())
}
impl Body for ReaderBody {
    type Data = Bytes;
    type Error = ContractError;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>>>> {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Some(Err(failed("artifact body ownership failed")))),
        };
        if state.done {
            return Poll::Ready(None);
        }
        if Instant::now() >= self.deadline {
            state.done = true;
            state.pending = None;
            state.reader = None;
            return Poll::Ready(Some(Err(failed("artifact transfer deadline exceeded"))));
        }
        if state.pending.is_none() {
            let mut reader = state.reader.take().expect("live artifact reader");
            let deadline = self.deadline;
            state.pending = Some(Box::pin(async move {
                let result = tokio::time::timeout_at(
                    deadline.into(),
                    reader.read_chunk(ARTIFACT_CHUNK_MAX_BYTES),
                )
                .await
                .unwrap_or_else(|_| Err(failed("artifact transfer deadline exceeded")));
                (reader, result)
            }));
        }
        let Poll::Ready((reader, result)) = state
            .pending
            .as_mut()
            .expect("pending read")
            .as_mut()
            .poll(cx)
        else {
            return Poll::Pending;
        };
        state.pending = None;
        state.reader = Some(reader);
        match result {
            Ok(chunk) if chunk.is_empty() => {
                state.done = true;
                state.reader = None;
                if self.size.is_some_and(|size| size != state.sent) {
                    Poll::Ready(Some(Err(failed("artifact body length mismatch"))))
                } else {
                    Poll::Ready(None)
                }
            }
            Ok(chunk) => {
                let next = state.sent.saturating_add(chunk.len() as u64);
                if chunk.len() > ARTIFACT_CHUNK_MAX_BYTES
                    || next > ARTIFACT_MAX_BYTES
                    || self.size.is_some_and(|size| next > size)
                {
                    state.done = true;
                    state.reader = None;
                    return Poll::Ready(Some(Err(failed(
                        "artifact reader exceeded declared bounds",
                    ))));
                }
                state.sent = next;
                Poll::Ready(Some(Ok(Frame::data(Bytes::from(chunk)))))
            }
            Err(error) => {
                state.done = true;
                state.reader = None;
                Poll::Ready(Some(Err(error)))
            }
        }
    }
    fn size_hint(&self) -> SizeHint {
        let mut hint = SizeHint::default();
        if let Ok(state) = self.state.lock() {
            if state.done {
                hint.set_exact(0);
            } else if let Some(size) = self.size {
                hint.set_exact(size.saturating_sub(state.sent));
            }
        }
        hint
    }
    fn is_end_stream(&self) -> bool {
        self.state.lock().is_ok_and(|state| state.done)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledgence_orchestration_api::ContractFuture;
    use std::{collections::VecDeque, time::Duration};
    struct Reader(VecDeque<Vec<u8>>);
    impl ArtifactReader for Reader {
        fn read_chunk(&mut self, _: usize) -> ContractFuture<'_, Vec<u8>> {
            Box::pin(async move { Ok(self.0.pop_front().unwrap_or_default()) })
        }
    }
    fn body(chunks: Vec<Vec<u8>>, size: Option<u64>) -> ReaderBody {
        ReaderBody::new(
            Box::new(Reader(chunks.into())),
            size,
            Instant::now() + Duration::from_secs(5),
        )
    }
    #[tokio::test]
    async fn streaming_hint_tracks_remaining_bytes_and_truncation_is_not_successful_eof() {
        let mut value = body(vec![vec![1, 2], vec![3]], Some(3));
        assert_eq!(value.size_hint().exact(), Some(3));
        assert_eq!(
            std::future::poll_fn(|cx| Pin::new(&mut value).poll_frame(cx))
                .await
                .unwrap()
                .unwrap()
                .into_data()
                .unwrap(),
            &[1, 2][..]
        );
        assert_eq!(value.size_hint().exact(), Some(1));
        assert!(
            std::future::poll_fn(|cx| Pin::new(&mut value).poll_frame(cx))
                .await
                .unwrap()
                .is_ok()
        );
        assert_eq!(value.size_hint().exact(), Some(0));
        assert!(
            std::future::poll_fn(|cx| Pin::new(&mut value).poll_frame(cx))
                .await
                .is_none()
        );
        assert!(value.is_end_stream());
        let mut truncated = body(vec![vec![1]], Some(2));
        assert!(
            std::future::poll_fn(|cx| Pin::new(&mut truncated).poll_frame(cx))
                .await
                .unwrap()
                .is_ok()
        );
        assert!(
            std::future::poll_fn(|cx| Pin::new(&mut truncated).poll_frame(cx))
                .await
                .unwrap()
                .is_err()
        );
        assert!(
            std::future::poll_fn(|cx| Pin::new(&mut truncated).poll_frame(cx))
                .await
                .is_none()
        );
    }
    #[tokio::test]
    async fn an_overlong_port_chunk_never_escapes_into_http() {
        let mut value = body(vec![vec![0; ARTIFACT_CHUNK_MAX_BYTES + 1]], None);
        assert!(
            std::future::poll_fn(|cx| Pin::new(&mut value).poll_frame(cx))
                .await
                .unwrap()
                .is_err()
        );
        assert!(
            std::future::poll_fn(|cx| Pin::new(&mut value).poll_frame(cx))
                .await
                .is_none()
        );
    }
}
