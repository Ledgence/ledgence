//! Ownership that must outlive the request's result future.

use super::*;
use std::{
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind},
    task::Poll,
};
use tokio::sync::oneshot;

/// The result can be delivered before an operation finishes. Its supervisor,
/// consumer permit and registration remain alive independently of this channel.
pub(super) struct Completion(Option<oneshot::Sender<ExecutionResult>>);
impl Completion {
    pub fn new(sender: oneshot::Sender<ExecutionResult>) -> Self {
        Self(Some(sender))
    }
    pub fn send(&mut self, result: ExecutionResult) {
        if let Some(sender) = self.0.take() {
            if let Err(failure) = &result {
                trace_failure(failure);
            }
            let _ = sender.send(result);
        }
    }
}

/// Unexpected supervisor destruction always fails admission closed, even when
/// its caller disappeared. Normal completion must be acknowledged explicitly.
pub(super) struct Registration {
    pub inner: Arc<Inner>,
    pub id: u64,
    pub key: AttemptKey,
    pub completed: bool,
}
impl Drop for Registration {
    fn drop(&mut self) {
        let mut registry = self
            .inner
            .registry
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if !self.completed {
            registry.accepting = false;
            for control in registry.active.values() {
                control.cancel();
            }
            registry.unresolved.insert(self.key.clone());
            registry.unresolved_operations += 1;
        }
        registry.active.remove(&self.id);
        registry.keys.remove(&self.key);
    }
}

/// Catch each poll at the adapter boundary. Callers keep sessions/reservations
/// outside this future, so unwinding cannot discard their cleanup ownership.
/// Wrapping the port call in `async { ... }` also catches construction panics.
pub(super) async fn catch_panic<F: Future>(future: F) -> std::result::Result<F::Output, Error> {
    let mut future = Box::pin(future);
    poll_fn(
        |context| match catch_call(|| future.as_mut().poll(context)) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        },
    )
    .await
}
pub(super) fn catch_call<T>(call: impl FnOnce() -> T) -> std::result::Result<T, Error> {
    catch_unwind(AssertUnwindSafe(call)).map_err(|payload| {
        let message = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                payload
                    .downcast_ref::<&str>()
                    .map(|message| (*message).to_owned())
            })
            .unwrap_or_else(|| "non-string panic payload".into());
        Error::new(
            ErrorKind::Runtime,
            format!("worker adapter panicked: {message}"),
        )
    })
}
