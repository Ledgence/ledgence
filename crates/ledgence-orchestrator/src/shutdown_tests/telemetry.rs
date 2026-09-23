use super::*;

#[tokio::test]
async fn second_signal_forces_orchestrator_exit_during_stalled_exporter_drain() {
    let mut collector = StalledCollector::start("ledgence.shutdown.fixture").await;
    let mut fixture =
        Fixture::start_with_endpoint("telemetry-wait-for-stop", Some(&collector.endpoint));
    fixture.wait_for_work().await;
    fixture.signal(Signal::SIGTERM);
    fixture.wait_for_log(SHUTDOWN_ACKNOWLEDGED).await;
    fixture.wait_for_log(RUNTIME_DRAIN_STARTED).await;
    collector.wait_until_stalled().await;
    fixture.assert_running();
    let forced_at = std::time::Instant::now();
    fixture.signal(Signal::SIGINT);
    tokio::time::timeout(Duration::from_secs(1), fixture.expect_exit(42))
        .await
        .expect("second signal must not wait for the pending OTLP request");
    assert!(forced_at.elapsed() < Duration::from_secs(1));
    assert!(
        fixture.stderr().contains("forced exit requested"),
        "{}",
        fixture.stderr()
    );
}

#[tokio::test]
async fn single_signal_drains_orchestrator_within_exporter_budget_without_collector_recovery() {
    let mut collector = StalledCollector::start("ledgence.shutdown.fixture").await;
    let mut fixture =
        Fixture::start_with_endpoint("telemetry-wait-for-stop", Some(&collector.endpoint));
    fixture.wait_for_work().await;
    let stopped_at = std::time::Instant::now();
    fixture.signal(Signal::SIGTERM);
    fixture.wait_for_log(SHUTDOWN_ACKNOWLEDGED).await;
    collector.wait_until_stalled().await;
    fixture.assert_running();
    tokio::time::timeout(Duration::from_secs(4), fixture.expect_exit(0))
        .await
        .expect("normal exporter drain must remain bounded");
    assert!(stopped_at.elapsed() < Duration::from_secs(4));
    assert!(
        !fixture.stderr().contains("forced exit requested"),
        "{}",
        fixture.stderr()
    );
    assert!(
        fixture.stderr().contains("telemetry"),
        "{}",
        fixture.stderr()
    );
}
/// Receives real bounded OTLP HTTP bytes. Only the batch containing the named
/// completed span is held without a response; earlier setup batches succeed.
struct StalledCollector {
    endpoint: String,
    observed: Option<tokio::sync::oneshot::Receiver<()>>,
    server: tokio::task::JoinHandle<()>,
}
impl StalledCollector {
    async fn start(span_name: &'static str) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/traces", listener.local_addr().unwrap());
        let (send, observed) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let header_end = loop {
                    if let Some(offset) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        break offset + 4;
                    }
                    let mut chunk = [0; 4096];
                    let count = socket.read(&mut chunk).await.unwrap();
                    assert_ne!(count, 0);
                    bytes.extend_from_slice(&chunk[..count]);
                    assert!(bytes.len() <= 1024 * 1024);
                };
                let headers = String::from_utf8(bytes[..header_end].to_vec())
                    .unwrap()
                    .to_ascii_lowercase();
                assert!(headers.starts_with("post /v1/traces http/1.1"));
                assert!(headers.contains("application/x-protobuf"));
                let size = headers
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("content-length:")
                            .map(|size| size.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                assert!(size <= 1024 * 1024);
                while bytes.len() < header_end + size {
                    let mut chunk = [0; 4096];
                    let count = socket.read(&mut chunk).await.unwrap();
                    assert_ne!(count, 0);
                    bytes.extend_from_slice(&chunk[..count]);
                }
                let body = &bytes[header_end..header_end + size];
                if body
                    .windows(span_name.len())
                    .any(|part| part == span_name.as_bytes())
                {
                    let _ = send.send(());
                    // Keep the actual peer open until test teardown; no network
                    // response, release signal, or collector recovery can aid exit.
                    std::future::pending::<()>().await;
                    drop(socket);
                    return;
                }
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
            }
        });
        Self {
            endpoint,
            observed: Some(observed),
            server,
        }
    }
    async fn wait_until_stalled(&mut self) {
        tokio::time::timeout(Duration::from_secs(10), self.observed.take().unwrap())
            .await
            .expect("expected completed span must reach the collector")
            .unwrap();
    }
}
impl Drop for StalledCollector {
    fn drop(&mut self) {
        self.server.abort();
    }
}
