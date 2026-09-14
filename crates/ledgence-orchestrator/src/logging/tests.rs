use super::*;
use crate::{command::Command, with_signals};
use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use std::{path::PathBuf, process::Stdio};
use tokio::{io::AsyncReadExt, net::TcpListener};

/// Run only in the subprocess below; its stderr is deliberately never consumed
/// until termination. The real orchestrator signal supervisor owns an actual
/// database connection whose handshake remains outstanding during both signals.
#[test]
fn signal_child() {
    let Some(marker) = std::env::var_os("LEDGENCE_LOG_TEST_MARKER") else {
        return;
    };
    let marker = PathBuf::from(marker);
    let mut logs = Logs::stderr().unwrap();
    let telemetry =
        crate::telemetry::Telemetry::start("ledgence-orchestrator", logs.sink.clone()).unwrap();
    let trace = telemetry.bridge();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (result, forced) = runtime.block_on(async {
        tokio::spawn(async move {
            // Allow with_signals to subscribe before announcing the flood.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let detail = "x".repeat(8192);
            for sequence in 0..2048 {
                tracing::info!(sequence, detail, "controlled pipe backpressure");
            }
            std::fs::write(marker, b"logging remains responsive").unwrap();
        });
        with_signals(
            move |stopped| {
                crate::dispatch(
                    Command::Migrate {
                        options: ledgence_adapter_postgres::MigrationOptions::default(),
                    },
                    stopped,
                    trace,
                )
            },
            &mut logs,
            telemetry,
        )
        .await
    });
    assert!(
        forced,
        "second signal must explicitly force shutdown: {result:?}"
    );
    assert!(result.unwrap_err().contains("forced exit"));
    assert!(
        logs.sink.state.lost.load(Ordering::Acquire) > 0,
        "unread sink must have exercised bounded loss"
    );
    logs.abort();
    runtime.shutdown_timeout(Duration::ZERO);
    // Distinguish the asserted force path from signal termination or test panic.
    std::process::exit(42);
}

#[tokio::test]
async fn unread_stderr_does_not_block_service_signals_or_force_shutdown() {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("responsive");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let database = format!(
        "postgresql://postgres@{}/ledgence?sslmode=disable",
        listener.local_addr().unwrap()
    );
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    // This fixture must exercise actual log backpressure regardless of the
    // developer's log filter or collector configuration.
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("OTEL_") {
            command.env_remove(name);
        }
    }
    let mut child = command
        .args([
            "--exact",
            "logging::tests::signal_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("RUST_LOG", "info")
        .env("LEDGENCE_LOG_TEST_MARKER", &marker)
        .env("DATABASE_URL", database)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let (_database_socket, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept())
        .await
        .expect("orchestrator must start its real database connection")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !marker.exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "child failed before logging completed"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a full stderr pipe must never block log producers");
    let pid = Pid::from_raw(child.id().unwrap() as i32);
    kill(pid, Signal::SIGTERM).unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        child.try_wait().unwrap().is_none(),
        "first signal must retain outstanding database work"
    );
    kill(pid, Signal::SIGINT).unwrap();
    let status = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .expect("second signal must stay responsive with unread logs")
        .unwrap();
    let mut stderr = Vec::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_end(&mut stderr)
        .await
        .unwrap();
    assert_eq!(
        status.code(),
        Some(42),
        "child diagnostics: {}",
        String::from_utf8_lossy(&stderr)
    );
    assert!(!stderr.is_empty());
    assert!(
        stderr.len() < 2 * 1024 * 1024,
        "the pipe was kept unread throughout the test"
    );
}
