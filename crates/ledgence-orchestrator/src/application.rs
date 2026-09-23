//! Own application execution and runtime destruction outside the signal runtime.

use std::{future::Future, io, thread, time::Duration};
use tokio::sync::oneshot;

pub struct Application {
    thread: thread::JoinHandle<Result<(), String>>,
    finished: oneshot::Receiver<()>,
}

impl Application {
    pub fn start<F>(work: impl FnOnce() -> F + Send + 'static) -> io::Result<Self>
    where
        F: Future<Output = Result<(), String>>,
    {
        let (finished, observed) = oneshot::channel();
        let subscriber = tracing::dispatcher::get_default(Clone::clone);
        let span = tracing::Span::current();
        let thread = thread::Builder::new()
            .name("ledgence-orchestrator-application".into())
            .spawn(move || {
                let _subscriber = tracing::dispatcher::set_default(&subscriber);
                let _span = span.enter();
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| format!("could not start application runtime: {error}"))?;
                let result = runtime.block_on(async move { work().await });
                tracing::debug!("application work finished; draining runtime operations");
                // Started blocking operations survive cancellation of their
                // async waiters. Retain them through normal runtime destruction.
                drop(runtime);
                let _ = finished.send(());
                result
            })?;
        Ok(Self {
            thread,
            finished: observed,
        })
    }

    /// Dropping this future detaches the thread, so the caller retains it until
    /// completion unless the user explicitly requests forced process exit.
    pub async fn finish(self) -> Result<(), String> {
        // A panic or startup failure also closes this channel. Never join a
        // thread that is still unwinding or completing its runtime destruction.
        let _ = self.finished.await;
        while !self.thread.is_finished() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        self.thread
            .join()
            .map_err(|_| "application thread panicked".to_owned())?
    }
}
