use super::*;
use ledgence_worker_api::{InvocationPermit, RuntimeInvocation};
use std::time::Instant;

#[tokio::test]
async fn admitted_plain_task_checks_send_window_after_ready_without_python_workflow_helpers() {
    let source = "from pathlib import Path\nPath(__file__).with_name('imported').write_text('ready')\ndef handle(event):\n    Path(event['data']['marker']).write_text('invoked')\n    return event['data']['value']\n";
    let (directory, artifact, runtime) = fixture(source);
    let mut session = ready(runtime.start(artifact, control()).await);
    assert!(session.supports_admitted_invocation());
    // Preparing a trusted Python program imports its module. Admission governs
    // the entrypoint invocation, not arbitrary import-time side effects.
    assert_eq!(
        std::fs::read_to_string(directory.path().join("imported")).unwrap(),
        "ready"
    );
    let marker = directory.path().join("expired-entrypoint");
    let mut value = event("expired").value().clone();
    value["data"] = json!({"marker":marker,"value":42});
    let error = session
        .execute_admitted(
            RuntimeInvocation::from(CloudEvent::new(value).unwrap()),
            control(),
            InvocationPermit {
                start_before: Instant::now(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::TimedOut);
    assert!(
        !marker.exists(),
        "expired permission must never send the invocation"
    );
    session.close().await.unwrap();

    let (directory, artifact, runtime) = fixture(source);
    let mut session = ready(runtime.start(artifact, control()).await);
    let marker = directory.path().join("admitted-entrypoint");
    let mut value = event("permitted").value().clone();
    value["ldgattemptno"] = json!(1001);
    value["data"] = json!({"marker":marker,"value":42});
    let result = session
        .execute_admitted(
            RuntimeInvocation::from(CloudEvent::new(value).unwrap()),
            control(),
            InvocationPermit {
                start_before: Instant::now() + Duration::from_secs(2),
            },
        )
        .await
        .unwrap();
    assert_eq!(output(result), json!(42));
    assert_eq!(std::fs::read_to_string(marker).unwrap(), "invoked");
    session.close().await.unwrap();
}

struct ArtifactHandler(std::sync::atomic::AtomicUsize);
impl ledgence_worker_api::RuntimeRequestHandler for ArtifactHandler {
    fn features(&self) -> ledgence_worker_api::RuntimeFeatures {
        ledgence_worker_api::RuntimeFeatures {
            v: 1,
            supported: vec![ledgence_worker_api::EXECUTION_ARTIFACTS_FEATURE.into()],
        }
    }
    fn handle<'a>(
        &'a self,
        request: ledgence_worker_api::RuntimeRequest,
        _: RunControl,
    ) -> ledgence_worker_api::PortFuture<'a, ledgence_worker_api::RuntimeReply> {
        Box::pin(async move {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert_eq!(request.operation, "artifacts.put_file");
            assert!(PathBuf::from(request.payload["path"].as_str().unwrap()).is_absolute());
            assert_eq!(
                std::fs::read(request.payload["path"].as_str().unwrap()).unwrap(),
                b"artifact"
            );
            assert!(request.payload.get("owner").is_none());
            Ok(ledgence_worker_api::RuntimeReply {
                id: request.id,
                result: json!({"schema":"ledgence.artifact.v1","id":"art_test","digest":format!("sha256:{}","a".repeat(64)),"size":8,"media_type":"application/octet-stream"}),
            })
        })
    }
}
#[tokio::test]
async fn admitted_artifact_task_keeps_fence_leaf_semantics_and_warm_capability_reset() {
    let source = "from pathlib import Path\nfrom ledgence.worker import artifacts\nfrom ledgence.worker.workflow import workflow_context, WorkflowError\nasync def handle(event):\n    try:\n        workflow_context()\n        raise AssertionError('plain task gained workflow context')\n    except WorkflowError:\n        pass\n    Path('file.bin').write_bytes(b'artifact')\n    return await artifacts.put_file('file.bin', key='result')\n";
    for expired in [true, false] {
        let (_dir, artifact, runtime) = fixture(source);
        let mut manifest = artifact.manifest().clone();
        manifest.runtime.protocol = 3;
        let artifact = PreparedArtifact::new(
            artifact.root().to_owned(),
            manifest,
            artifact.digest().clone(),
            Arc::new(()),
        );
        let mut session = ready(runtime.start(artifact, control()).await);
        let handler = Arc::new(ArtifactHandler(std::sync::atomic::AtomicUsize::new(0)));
        let result = session
            .execute_admitted_with_requests(
                event("first").into(),
                control(),
                InvocationPermit {
                    start_before: Instant::now()
                        + if expired {
                            Duration::ZERO
                        } else {
                            Duration::from_secs(3)
                        },
                },
                handler.clone(),
            )
            .await;
        if expired {
            assert_eq!(result.unwrap_err().kind, ErrorKind::TimedOut);
            assert_eq!(handler.0.load(std::sync::atomic::Ordering::SeqCst), 0);
        } else {
            assert_eq!(output(result.unwrap())["id"], "art_test");
            assert_eq!(handler.0.load(std::sync::atomic::Ordering::SeqCst), 1);
            let next = session
                .execute(event("next").into(), control())
                .await
                .unwrap();
            assert!(
                matches!(next, ProgramOutcome::Failure { message, .. } if message.contains("unavailable"))
            );
            assert_eq!(handler.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        }
        session.close().await.unwrap();
    }
}
