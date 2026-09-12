use ledgence_worker_api::*;
use ledgence_worker_core::{ExecutionRequest, Worker, WorkerConfig};
use serde_json::json;
use std::{sync::Arc, time::Duration};

struct FailingPreparation;
impl ProgramStore for FailingPreparation {
    fn resolve<'a>(&'a self, _: &'a ProgramRef) -> PortFuture<'a, ProgramDescriptor> {
        unreachable!("assignment is already bound")
    }
    fn fetch<'a>(&'a self, _: &'a ProgramDescriptor) -> PortFuture<'a, Vec<u8>> {
        Box::pin(async { Err(Error::new(ErrorKind::Io, "injected download failure")) })
    }
}
impl ArtifactCache for FailingPreparation {
    fn lookup<'a>(&'a self, _: &'a ProgramDescriptor) -> PortFuture<'a, Option<PreparedArtifact>> {
        Box::pin(async { Ok(None) })
    }
    fn publish<'a>(
        &'a self,
        _: &'a ProgramDescriptor,
        _: Vec<u8>,
    ) -> PortFuture<'a, PreparedArtifact> {
        unreachable!("download failed")
    }
}
impl ExecutionRuntime for FailingPreparation {
    fn start(&self, _: PreparedArtifact, _: RunControl) -> PortFuture<'_, StartOutcome> {
        unreachable!("download failed")
    }
}
#[derive(Clone, Default)]
struct LogCapture(Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for LogCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogCapture {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}
#[tokio::test]
async fn warning_logs_keep_identity_when_info_spans_are_filtered_out() {
    let logs = LogCapture::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .with_writer(logs.clone())
        .finish();
    let _subscriber = tracing::subscriber::set_default(subscriber);
    let worker = Worker::new(
        WorkerConfig::default(),
        Arc::new(FailingPreparation),
        Arc::new(FailingPreparation),
        Arc::new(FailingPreparation),
    )
    .unwrap();
    let invocation = ExecutionRequest {
        descriptor: ProgramDescriptor {
            program: ProgramRef { id: "demo".into(), version: "v1".into() },
            digest: Digest(format!("sha256:{}", "a".repeat(64))), size: 1,
        },
        event: CloudEvent::new(json!({
            "specversion": "1.0", "id": "evt_900", "source": "urn:test:observability",
            "type": "com.ledgence.task.invocation.requested.v1", "datacontenttype": "application/json",
            "ldgtenantid": "tenant_logs", "ldgnamespace": "production", "ldgrunid": "run_logs",
            "ldgtaskid": "task_logs", "ldgattemptid": "attempt_logs", "ldgattemptno": 2,
            "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "tracestate": "vendor=value", "data": { "user": "unchanged" }
        })).unwrap(),
    };
    worker
        .execute(invocation.clone(), RunControl::new(Duration::from_secs(1)))
        .await
        .unwrap_err();
    worker
        .shutdown(Duration::ZERO, Duration::from_secs(1))
        .await
        .unwrap();
    let lines = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    let logged = lines
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .find(|line| line["fields"]["event_id"] == invocation.event.id())
        .expect("failure log");
    let identity = serde_json::to_value(InvocationIdentity::from(&invocation.event)).unwrap();
    for (key, expected) in identity.as_object().unwrap() {
        assert_eq!(
            &logged["fields"][key], expected,
            "missing log identity {key}"
        );
    }
    assert_eq!(logged["fields"]["digest"], invocation.descriptor.digest.0);
    assert_eq!(
        logged["fields"]["program_id"],
        invocation.descriptor.program.id
    );
    assert_eq!(
        logged["fields"]["program_version"],
        invocation.descriptor.program.version
    );
    assert_eq!(logged["fields"]["phase"], "Preparation");
}
