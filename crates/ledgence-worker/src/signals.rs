//! Persistent signal subscriptions: the first signal drains, a further one forces exit.

use ledgence_worker_api::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicBool, Ordering};

pub static FORCE_EXIT: AtomicBool = AtomicBool::new(false);

pub struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    pub fn new() -> Result<Self> {
        Ok(Self {
            #[cfg(unix)]
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
            #[cfg(unix)]
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
        })
    }

    pub async fn recv(&mut self) -> Result<()> {
        #[cfg(unix)]
        {
            let received = tokio::select! {
                received = self.interrupt.recv() => received,
                received = self.terminate.recv() => received,
            };
            received.ok_or_else(|| Error::new(ErrorKind::Io, "shutdown signal stream closed"))
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await.map_err(Error::from)
    }
}

pub fn forced_exit() -> Error {
    FORCE_EXIT.store(true, Ordering::Release);
    Error::new(
        ErrorKind::Runtime,
        "forced exit requested with process cleanup unresolved",
    )
}
