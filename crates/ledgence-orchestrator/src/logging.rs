//! Bounded stderr logging that never waits on a pipe from a Tokio task.
//!
//! One owned thread writes whole queued records. Its operation is retained
//! through normal shutdown; an explicit forced exit may abandon blocked I/O.

use std::{
    fs::File,
    io::{self, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const QUEUE_RECORDS: usize = 64;
const RECORD_BYTES: usize = 256 * 1024;
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(5);
const TICK: Duration = Duration::from_millis(10);

struct Record {
    bytes: Vec<u8>,
    deadline: Instant,
}

#[derive(Default)]
struct State {
    closing: AtomicBool,
    abort: AtomicBool,
    lost: AtomicUsize,
    failed: AtomicBool,
}

#[derive(Clone)]
pub struct Sink {
    sender: mpsc::SyncSender<Record>,
    state: Arc<State>,
}

impl Sink {
    fn enqueue(&self, bytes: Vec<u8>) {
        if bytes.len() > RECORD_BYTES
            || self.state.closing.load(Ordering::Acquire)
            || self
                .sender
                .try_send(Record {
                    bytes,
                    deadline: Instant::now() + DELIVERY_TIMEOUT,
                })
                .is_err()
        {
            self.state.lost.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn diagnostic(&self, message: &str) {
        let message: String = message.chars().take(4096).collect();
        let mut bytes = serde_json::json!({"level":"ERROR","message":message})
            .to_string()
            .into_bytes();
        bytes.push(b'\n');
        self.enqueue(bytes);
    }
}

pub struct LogRecord {
    sink: Sink,
    bytes: Vec<u8>,
    oversized: bool,
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Sink {
    type Writer = LogRecord;
    fn make_writer(&'a self) -> Self::Writer {
        LogRecord {
            sink: self.clone(),
            bytes: Vec::new(),
            oversized: false,
        }
    }
}

impl Write for LogRecord {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.bytes.len().saturating_add(bytes.len()) > RECORD_BYTES {
            self.oversized = true;
        } else if !self.oversized {
            self.bytes.extend_from_slice(bytes);
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for LogRecord {
    fn drop(&mut self) {
        if self.oversized {
            self.sink.state.lost.fetch_add(1, Ordering::Relaxed);
        } else if !self.bytes.is_empty() {
            self.sink.enqueue(std::mem::take(&mut self.bytes));
        }
    }
}

pub struct Logs {
    pub sink: Sink,
    writer: Option<JoinHandle<()>>,
    _restore: RestoreFlags,
}

impl Logs {
    pub fn stderr() -> io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::fd::AsFd;
            let (file, restore) = destination(io::stderr().as_fd())?;
            let state = Arc::new(State::default());
            let (sender, receiver) = mpsc::sync_channel(QUEUE_RECORDS);
            let owned = state.clone();
            let writer = thread::Builder::new()
                .name("ledgence-orchestrator-stderr".into())
                .spawn(move || write_records(file, receiver, owned))?;
            Ok(Self {
                sink: Sink { sender, state },
                writer: Some(writer),
                _restore: restore,
            })
        }
        #[cfg(not(unix))]
        Err(io::Error::other(
            "orchestrator log output currently requires Unix",
        ))
    }

    /// Cancellation only stops observation; ownership remains in this object.
    pub async fn finish(&mut self) -> io::Result<()> {
        let lost = self.sink.state.lost.load(Ordering::Acquire);
        if lost != 0 && !self.sink.state.closing.load(Ordering::Acquire) {
            self.sink
                .diagnostic(&format!("{lost} log record(s) were not delivered"));
        }
        self.sink.state.closing.store(true, Ordering::Release);
        while self
            .writer
            .as_ref()
            .is_some_and(|writer| !writer.is_finished())
        {
            tokio::time::sleep(TICK).await;
        }
        if let Some(writer) = self.writer.take()
            && writer.join().is_err()
        {
            self.sink.state.failed.store(true, Ordering::Release);
        }
        let lost = self.sink.state.lost.load(Ordering::Acquire);
        if lost != 0 || self.sink.state.failed.load(Ordering::Acquire) {
            Err(io::Error::other(format!(
                "stderr delivery failed; {lost} log record(s) were not delivered"
            )))
        } else {
            Ok(())
        }
    }

    pub fn abort(&self) {
        self.sink.state.abort.store(true, Ordering::Release);
    }
}

impl Drop for Logs {
    fn drop(&mut self) {
        self.abort();
    }
}

fn write_records(mut file: File, receiver: mpsc::Receiver<Record>, state: Arc<State>) {
    loop {
        if state.abort.load(Ordering::Acquire) {
            break;
        }
        match receiver.recv_timeout(TICK) {
            Ok(record) => {
                if deliver(&mut file, &record, &state).is_err() {
                    state.failed.store(true, Ordering::Release);
                    state.lost.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) if state.closing.load(Ordering::Acquire) => break,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
    }
    state.closing.store(true, Ordering::Release);
    state
        .lost
        .fetch_add(receiver.try_iter().count(), Ordering::Relaxed);
}

fn deliver(file: &mut File, record: &Record, state: &State) -> io::Result<()> {
    let mut remaining = record.bytes.as_slice();
    while !remaining.is_empty() {
        if state.abort.load(Ordering::Acquire) {
            return Err(io::Error::other("forced shutdown interrupted stderr"));
        }
        if Instant::now() >= record.deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "stderr delivery deadline exceeded",
            ));
        }
        match file.write(&remaining[..remaining.len().min(16 * 1024)]) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(count) => remaining = &remaining[count..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => thread::sleep(TICK),
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

struct RestoreFlags {
    file: File,
    #[cfg(unix)]
    flags: nix::fcntl::OFlag,
}

#[cfg(unix)]
fn destination(fd: std::os::fd::BorrowedFd<'_>) -> io::Result<(File, RestoreFlags)> {
    use nix::fcntl::{FcntlArg, OFlag, fcntl};
    let file = File::from(fd.try_clone_to_owned()?);
    let flags = OFlag::from_bits_retain(fcntl(&file, FcntlArg::F_GETFL)?);
    let restore = RestoreFlags {
        file: file.try_clone()?,
        flags,
    };
    fcntl(&file, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))?;
    Ok((file, restore))
}

impl Drop for RestoreFlags {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use nix::fcntl::{FcntlArg, fcntl};
            let _ = fcntl(&self.file, FcntlArg::F_SETFL(self.flags));
        }
    }
}

#[cfg(all(test, unix))]
mod tests;
