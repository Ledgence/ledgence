//! One owned writer, one bounded page, and an acknowledgement before polling again.
use ledgence_orchestration_api::{ContractError, EXECUTION_PROGRESS_PAGE_MAX_BYTES, Result};
use std::{
    fs::File,
    io::{self, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

struct Record {
    bytes: Vec<u8>,
    deadline: Instant,
    receipt: oneshot::Sender<io::Result<()>>,
}
pub(super) struct Output {
    sender: mpsc::SyncSender<Record>,
    abort: Arc<AtomicBool>,
    writer: Option<thread::JoinHandle<()>>,
}
impl Output {
    pub(super) fn new() -> io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::fd::AsFd;
            let (mut file, restore) = crate::logging::destination(io::stdout().as_fd())?;
            let (sender, receiver) = mpsc::sync_channel::<Record>(1);
            let abort = Arc::new(AtomicBool::new(false));
            let stopped = abort.clone();
            let writer = thread::Builder::new()
                .name("ledgence-progress-stdout".into())
                .spawn(move || {
                    // Restore shared descriptor flags only after the writer stops.
                    let _restore = restore;
                    while !stopped.load(Ordering::Acquire) {
                        match receiver.recv_timeout(Duration::from_millis(10)) {
                            Ok(record) => {
                                let result = deliver(&mut file, &record, &stopped);
                                let failed = result.is_err();
                                let _ = record.receipt.send(result);
                                if failed {
                                    break;
                                }
                            }
                            Err(mpsc::RecvTimeoutError::Timeout) => (),
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        }
                    }
                })?;
            Ok(Self {
                sender,
                abort,
                writer: Some(writer),
            })
        }
        #[cfg(not(unix))]
        Err(io::Error::other("progress output currently requires Unix"))
    }
    pub(super) async fn write(&self, bytes: Vec<u8>) -> Result<()> {
        if bytes.len() > EXECUTION_PROGRESS_PAGE_MAX_BYTES + 1 {
            return Err(super::unavailable("progress output exceeds page limit"));
        }
        let (receipt, received) = oneshot::channel();
        self.sender
            .try_send(Record {
                bytes,
                deadline: Instant::now() + Duration::from_secs(5),
                receipt,
            })
            .map_err(|_| super::unavailable("progress output writer is unavailable"))?;
        tokio::time::timeout(Duration::from_secs(5), received)
            .await
            .map_err(|_| super::unavailable("progress output delivery timed out"))?
            .map_err(|_| super::unavailable("progress output delivery was interrupted"))?
            .map_err(|error| {
                ContractError::Unavailable(format!("progress output delivery failed: {error}"))
            })
    }
    pub(super) fn abort(&self) {
        self.abort.store(true, Ordering::Release);
    }
    pub(super) async fn finish(&mut self) {
        self.abort();
        while self
            .writer
            .as_ref()
            .is_some_and(|writer| !writer.is_finished())
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
    }
}
impl Drop for Output {
    fn drop(&mut self) {
        self.abort();
    }
}
fn deliver(file: &mut File, record: &Record, abort: &AtomicBool) -> io::Result<()> {
    let mut remaining = record.bytes.as_slice();
    while !remaining.is_empty() {
        if abort.load(Ordering::Acquire) {
            return Err(io::Error::other("observation stopped"));
        }
        if Instant::now() >= record.deadline {
            return Err(io::ErrorKind::TimedOut.into());
        }
        match file.write(&remaining[..remaining.len().min(16 * 1024)]) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(count) => remaining = &remaining[count..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => (),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10))
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
