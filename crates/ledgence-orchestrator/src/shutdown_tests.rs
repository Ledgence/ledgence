use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use std::{
    path::{Path, PathBuf},
    process::{ExitCode, Stdio},
    time::Duration,
};
use tokio::{process::Child, sync::oneshot};

const FIXTURE_DIRECTORY: &str = "LEDGENCE_SHUTDOWN_TEST_DIRECTORY";
const FIXTURE_MODE: &str = "LEDGENCE_SHUTDOWN_TEST_MODE";
const RUNTIME_DRAIN_STARTED: &str = "application work finished; draining runtime operations";
const SHUTDOWN_ACKNOWLEDGED: &str =
    "shutdown requested; retaining in-flight operations until drained";
const LATE_RECORD: &str = "controlled blocking operation finished during runtime drain";
const READY_TIMEOUT: Duration = Duration::from_secs(10);
const EXIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Each scenario runs the production entrypoint in its own process because
/// signal subscriptions and the tracing subscriber are process-wide resources.
#[test]
fn shutdown_child() {
    let Some(directory) = std::env::var_os(FIXTURE_DIRECTORY) else {
        return;
    };
    let directory = PathBuf::from(directory);
    let mode = std::env::var(FIXTURE_MODE).unwrap();
    let status = crate::run(move |mut stopped| async move {
        #[cfg(feature = "otel")]
        if mode == "telemetry-wait-for-stop" {
            std::fs::write(directory.join("work-ready"), b"ready for stop").unwrap();
            while !*stopped.borrow() {
                stopped.changed().await.unwrap();
            }
            // Finish one real SDK span only after the first signal. With no
            // application work left, its stalled export tests the final drain.
            tracing::info_span!("ledgence.shutdown.fixture").in_scope(|| {
                tracing::info!("completed traced shutdown fixture work");
            });
            return Ok(());
        }
        if mode == "application-error" {
            return Err("controlled application failure".into());
        }
        assert_ne!(mode, "application-panic", "controlled application panic");
        let (started, started_receiver) = oneshot::channel();
        let gate = directory.join("release-blocking-operation");
        let blocking = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            while !gate.exists() {
                std::thread::sleep(Duration::from_millis(5));
            }
            tracing::info!("controlled blocking operation finished during runtime drain");
        });
        started_receiver.await.unwrap();
        // This is the FileProgramStore/HTTP deadline pattern: expiration drops
        // the join waiter while the real blocking operation remains outstanding.
        assert!(
            tokio::time::timeout(Duration::from_millis(20), blocking)
                .await
                .is_err()
        );
        std::fs::write(directory.join("work-ready"), b"waiter expired").unwrap();
        if mode == "wait-for-stop" {
            while !*stopped.borrow() {
                stopped.changed().await.unwrap();
            }
        }
        Ok(())
    });
    // A signal-terminated process or assertion panic cannot mimic this code.
    std::process::exit(if status == ExitCode::SUCCESS { 0 } else { 42 });
}

struct Fixture {
    child: Child,
    directory: tempfile::TempDir,
}

impl Fixture {
    fn start(mode: &str) -> Self {
        Self::start_with_endpoint(mode, None)
    }

    fn start_with_endpoint(mode: &str, endpoint: Option<&str>) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let stderr = std::fs::File::create(directory.path().join("stderr.log")).unwrap();
        let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "shutdown_tests::shutdown_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(FIXTURE_DIRECTORY, directory.path())
            .env(FIXTURE_MODE, mode)
            .env("RUST_LOG", "debug")
            .env_remove("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
            .stdout(Stdio::null())
            .stderr(stderr)
            .kill_on_drop(true);
        if let Some(endpoint) = endpoint {
            command
                .env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", endpoint)
                .env("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
                .env("OTEL_TRACES_SAMPLER_ARG", "1");
        }
        let child = command.spawn().unwrap();
        Self { child, directory }
    }

    fn stderr(&self) -> String {
        std::fs::read_to_string(self.directory.path().join("stderr.log")).unwrap()
    }

    fn assert_running(&mut self) {
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "process exited before its retained blocking operation completed: {}",
            self.stderr()
        );
    }

    async fn wait_for(&mut self, description: &str, ready: impl Fn(&Path, &str) -> bool) {
        tokio::time::timeout(READY_TIMEOUT, async {
            loop {
                self.assert_running();
                if ready(self.directory.path(), &self.stderr()) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {description}: {}", self.stderr()));
    }

    async fn wait_for_work(&mut self) {
        self.wait_for("expired blocking waiter", |directory, _| {
            directory.join("work-ready").exists()
        })
        .await;
    }

    async fn wait_for_log(&mut self, message: &str) {
        self.wait_for(message, |_, stderr| stderr.contains(message))
            .await;
    }

    fn signal(&self, signal: Signal) {
        let pid = i32::try_from(self.child.id().expect("child is still running")).unwrap();
        kill(Pid::from_raw(pid), signal).unwrap();
    }

    fn release(&self) {
        std::fs::write(
            self.directory.path().join("release-blocking-operation"),
            b"complete retained operation",
        )
        .unwrap();
    }

    async fn expect_exit(&mut self, code: i32) {
        let status = tokio::time::timeout(EXIT_TIMEOUT, self.child.wait())
            .await
            .unwrap_or_else(|_| panic!("process failed to exit: {}", self.stderr()))
            .unwrap();
        assert_eq!(status.code(), Some(code), "{}", self.stderr());
    }
}

#[tokio::test]
async fn second_signal_forces_exit_after_application_work_finishes() {
    let mut fixture = Fixture::start("wait-for-stop");
    fixture.wait_for_work().await;
    fixture.signal(Signal::SIGTERM);
    fixture.wait_for_log(SHUTDOWN_ACKNOWLEDGED).await;
    fixture.wait_for_log(RUNTIME_DRAIN_STARTED).await;
    fixture.assert_running();
    fixture.signal(Signal::SIGINT);
    fixture.expect_exit(42).await;
    assert!(fixture.stderr().contains("forced exit requested"));
}

#[tokio::test]
async fn first_signal_during_runtime_drain_still_requires_second_signal_to_force() {
    let mut fixture = Fixture::start("complete-work");
    fixture.wait_for_log(RUNTIME_DRAIN_STARTED).await;
    fixture.signal(Signal::SIGTERM);
    fixture.wait_for_log(SHUTDOWN_ACKNOWLEDGED).await;
    fixture.assert_running();
    fixture.signal(Signal::SIGINT);
    fixture.expect_exit(42).await;
    assert!(fixture.stderr().contains("forced exit requested"));
}

#[tokio::test]
async fn graceful_shutdown_waits_for_blocking_operations_and_delivers_their_final_logs() {
    let mut fixture = Fixture::start("wait-for-stop");
    fixture.wait_for_work().await;
    fixture.signal(Signal::SIGTERM);
    fixture.wait_for_log(SHUTDOWN_ACKNOWLEDGED).await;
    fixture.wait_for_log(RUNTIME_DRAIN_STARTED).await;
    fixture.assert_running();
    fixture.release();
    fixture.expect_exit(0).await;
    let stderr = fixture.stderr();
    assert!(stderr.contains(LATE_RECORD), "{stderr}");
    assert!(!stderr.contains("forced exit requested"), "{stderr}");
}

#[tokio::test]
async fn application_errors_survive_the_runtime_thread_boundary() {
    let mut fixture = Fixture::start("application-error");
    fixture.expect_exit(42).await;
    let stderr = fixture.stderr();
    assert!(
        stderr.contains("controlled application failure"),
        "{stderr}"
    );
    assert!(!stderr.contains("forced exit requested"), "{stderr}");
}

#[tokio::test]
async fn application_panics_return_failure_after_the_runtime_thread_exits() {
    let mut fixture = Fixture::start("application-panic");
    fixture.expect_exit(42).await;
    let stderr = fixture.stderr();
    assert!(stderr.contains("controlled application panic"), "{stderr}");
    assert!(stderr.contains("application thread panicked"), "{stderr}");
    assert!(!stderr.contains("forced exit requested"), "{stderr}");
}

#[cfg(feature = "otel")]
#[path = "shutdown_tests/telemetry.rs"]
mod telemetry;
