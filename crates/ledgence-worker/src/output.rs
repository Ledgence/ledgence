//! Bounded CLI output with owned writers and explicit delivery failures.
//!
//! File writes stay off Tokio threads. Pipes/terminals are nonblocking, and a
//! record's deadline includes queueing. A genuinely blocked filesystem syscall
//! remains owned through shutdown; only an explicit second signal abandons it.

use ledgence_worker_api::{Error, ErrorKind, Result};
use std::{
    fs::File,
    io::{self, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};

const REPORT_QUEUE_RECORDS: usize = 8;
const LOG_QUEUE_RECORDS: usize = 64;
const REPORT_BYTES: usize = 8 * 1024 * 1024;
const LOG_BYTES: usize = 256 * 1024;
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_INTERVAL: Duration = Duration::from_millis(10);

struct Record {
    bytes: Vec<u8>,
    deadline: Instant,
    delivered: Option<oneshot::Sender<()>>,
}

struct State {
    name: &'static str,
    closing: AtomicBool,
    abort: AtomicBool,
    failure: Mutex<Option<String>>,
    lost_logs: AtomicUsize,
}

impl State {
    fn fail(&self, message: impl std::fmt::Display) {
        self.failure
            .lock()
            .unwrap()
            .get_or_insert_with(|| format!("{} output failed: {message}", self.name));
        self.closing.store(true, Ordering::Release);
    }

    fn error(&self) -> Error {
        Error::new(
            ErrorKind::Io,
            self.failure.lock().unwrap().clone().unwrap_or_else(|| {
                format!("{} output closed before delivery was confirmed", self.name)
            }),
        )
    }
}

#[derive(Clone)]
pub struct Sink {
    sender: mpsc::Sender<Record>,
    state: Arc<State>,
    wake: thread::Thread,
}

impl Sink {
    pub async fn write(&self, bytes: Vec<u8>) -> Result<()> {
        if bytes.len() > REPORT_BYTES {
            self.state.fail("record exceeds eight MiB");
            return Err(self.state.error());
        }
        if self.state.closing.load(Ordering::Acquire) {
            return Err(self.state.error());
        }
        let deadline = Instant::now() + DELIVERY_TIMEOUT;
        let (delivered, receipt) = oneshot::channel();
        let delivery = async {
            self.sender
                .send(Record {
                    bytes,
                    deadline,
                    delivered: Some(delivered),
                })
                .await
                .map_err(|_| self.state.error())?;
            self.wake.unpark();
            receipt.await.map_err(|_| self.state.error())
        };
        match tokio::time::timeout_at(deadline.into(), delivery).await {
            Ok(result) => result,
            Err(_) => {
                self.state.fail("five-second delivery deadline exceeded");
                Err(self.state.error())
            }
        }
    }

    pub async fn line(&self, text: impl Into<String>) -> Result<()> {
        let mut text = text.into();
        text.push('\n');
        self.write(text.into_bytes()).await
    }

    fn log(&self, bytes: Vec<u8>) {
        if self.state.closing.load(Ordering::Acquire)
            || self
                .sender
                .try_send(Record {
                    bytes,
                    deadline: Instant::now() + DELIVERY_TIMEOUT,
                    delivered: None,
                })
                .is_err()
        {
            self.state.lost_logs.fetch_add(1, Ordering::Relaxed);
        } else {
            self.wake.unpark();
        }
    }
}

/// Each formatter owns a whole record, so concurrent JSON logs never interleave.
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
        if self.bytes.len().saturating_add(bytes.len()) > LOG_BYTES {
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
            self.sink.state.lost_logs.fetch_add(1, Ordering::Relaxed);
        } else if !self.bytes.is_empty() {
            self.sink.log(std::mem::take(&mut self.bytes));
        }
    }
}

struct Writer {
    state: Arc<State>,
    thread: Option<JoinHandle<()>>,
}

pub struct Outputs {
    pub stdout: Sink,
    pub stderr: Sink,
    writers: Vec<Writer>,
    // Reverse acquisition order matters when stderr is redirected to stdout:
    // both descriptors then share one set of file-status flags.
    _restore: Vec<RestoreFlags>,
}

impl Outputs {
    pub fn new() -> io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::fd::AsFd;
            // Acquire both before starting threads. Duplicates are close-on-exec;
            // status flags belong to the shared open-file description. The
            // owner restores them after both writers have stopped normally.
            let (stdout, restore_stdout) = Destination::new(io::stdout().as_fd())?;
            let (stderr, restore_stderr) = Destination::new(io::stderr().as_fd())?;
            let restore = vec![restore_stderr, restore_stdout];
            let (stdout, first) = start("stdout", stdout)?;
            let (stderr, second) = match start("stderr", stderr) {
                Ok(started) => started,
                Err(error) => {
                    first.state.abort.store(true, Ordering::Release);
                    // This thread has no records and can only be polling its queue.
                    let _ = first.thread.unwrap().join();
                    return Err(error);
                }
            };
            Ok(Self {
                stdout,
                stderr,
                writers: vec![first, second],
                _restore: restore,
            })
        }
        #[cfg(not(unix))]
        Err(io::Error::other(
            "worker CLI output currently requires Unix",
        ))
    }

    pub async fn finish(&mut self) -> Result<()> {
        let lost = self.stderr.state.lost_logs.load(Ordering::Acquire);
        if lost != 0 && !self.stderr.state.closing.load(Ordering::Acquire) {
            let _ = self
                .stderr
                .line(serde_json::json!({"level":"WARN", "message":"optional log records were not delivered", "dropped_log_records":lost}).to_string())
                .await;
        }
        for writer in &self.writers {
            writer.state.closing.store(true, Ordering::Release);
            if let Some(thread) = &writer.thread {
                thread.thread().unpark();
            }
        }
        // Do not join a writer until it has actually finished. Filesystem I/O
        // cannot always be interrupted; signal handling remains alive here.
        while self.writers.iter().any(|writer| {
            writer
                .thread
                .as_ref()
                .is_some_and(|thread| !thread.is_finished())
        }) {
            tokio::time::sleep(RETRY_INTERVAL).await;
        }
        let mut failures = Vec::new();
        for writer in &mut self.writers {
            if let Some(thread) = writer.thread.take()
                && thread.join().is_err()
            {
                writer.state.fail("writer panicked");
            }
            if let Some(failure) = writer.state.failure.lock().unwrap().as_ref() {
                failures.push(failure.clone());
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(Error::new(ErrorKind::Io, failures.join("; ")))
        }
    }

    pub fn abort(&self) {
        for writer in &self.writers {
            writer.state.abort.store(true, Ordering::Release);
            if let Some(thread) = &writer.thread {
                thread.thread().unpark();
            }
        }
    }
}

impl Drop for Outputs {
    fn drop(&mut self) {
        // Normal paths call finish. Forced exit/panic asks any still-owned
        // writers to stop without joining a potentially blocked file syscall.
        self.abort();
    }
}

fn start(name: &'static str, mut destination: Destination) -> io::Result<(Sink, Writer)> {
    let state = Arc::new(State {
        name,
        closing: AtomicBool::new(false),
        abort: AtomicBool::new(false),
        failure: Mutex::new(None),
        lost_logs: AtomicUsize::new(0),
    });
    let capacity = if name == "stdout" {
        REPORT_QUEUE_RECORDS
    } else {
        LOG_QUEUE_RECORDS
    };
    let (sender, mut receiver) = mpsc::channel::<Record>(capacity);
    let owner = state.clone();
    let thread = thread::Builder::new()
        .name(format!("ledgence-{name}"))
        .spawn(move || {
            loop {
                if owner.abort.load(Ordering::Acquire) {
                    owner.fail("delivery interrupted by forced exit");
                    break;
                }
                if owner.failure.lock().unwrap().is_some() {
                    break;
                }
                if owner.closing.load(Ordering::Acquire) {
                    receiver.close();
                }
                match receiver.try_recv() {
                    Ok(record) => {
                        if let Err(error) = destination.deliver(&record, &owner) {
                            owner.fail(error);
                            if record.delivered.is_none() {
                                owner.lost_logs.fetch_add(1, Ordering::Relaxed);
                            }
                            break;
                        }
                        if let Some(receipt) = record.delivered {
                            let _ = receipt.send(());
                        }
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                    Err(mpsc::error::TryRecvError::Empty) => thread::park_timeout(RETRY_INTERVAL),
                }
            }
            receiver.close();
            while let Ok(record) = receiver.try_recv() {
                if record.delivered.is_none() {
                    owner.lost_logs.fetch_add(1, Ordering::Relaxed);
                }
            }
        })?;
    Ok((
        Sink {
            sender,
            state: state.clone(),
            wake: thread.thread().clone(),
        },
        Writer {
            state,
            thread: Some(thread),
        },
    ))
}

struct Destination {
    file: File,
}

struct RestoreFlags {
    file: File,
    #[cfg(unix)]
    flags: nix::fcntl::OFlag,
}

impl Destination {
    #[cfg(unix)]
    fn new(fd: std::os::fd::BorrowedFd<'_>) -> io::Result<(Self, RestoreFlags)> {
        use nix::fcntl::{FcntlArg, OFlag, fcntl};
        let file = File::from(fd.try_clone_to_owned()?);
        let flags = OFlag::from_bits_retain(fcntl(&file, FcntlArg::F_GETFL)?);
        let restore = RestoreFlags {
            file: file.try_clone()?,
            flags,
        };
        fcntl(&file, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))?;
        Ok((Self { file }, restore))
    }

    fn deliver(&mut self, record: &Record, state: &State) -> io::Result<()> {
        let mut remaining = record.bytes.as_slice();
        while !remaining.is_empty() {
            if state.abort.load(Ordering::Acquire) {
                return Err(io::Error::other("delivery interrupted by forced exit"));
            }
            if Instant::now() >= record.deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "five-second delivery deadline exceeded",
                ));
            }
            match self
                .file
                .write(&remaining[..remaining.len().min(16 * 1024)])
            {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(count) => remaining = &remaining[count..],
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(RETRY_INTERVAL)
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
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
