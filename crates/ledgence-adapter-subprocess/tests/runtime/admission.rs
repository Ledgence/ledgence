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
