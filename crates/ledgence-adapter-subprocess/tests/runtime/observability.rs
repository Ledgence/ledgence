use super::*;
use ledgence_worker_api::{RuntimeInvocation, TraceContext};

fn v2_fixture(source: &str) -> (TempDir, PreparedArtifact, SubprocessRuntime) {
    let (dir, artifact, runtime) = fixture(source);
    let mut manifest = artifact.manifest().clone();
    manifest.runtime.protocol = 2;
    let artifact = PreparedArtifact::new(
        artifact.root().to_owned(),
        manifest,
        artifact.digest().clone(),
        Arc::new(()),
    );
    (dir, artifact, runtime)
}

#[tokio::test]
async fn v2_preserves_origin_and_carries_execution_context_across_reuse_and_failures() {
    let (_dir, artifact, runtime) = v2_fixture(
        r#"import os
from dataclasses import asdict
from ledgence_worker import current_invocation
def handle(event):
    if event['id'] == 'failure': raise ValueError('expected')
    if event['id'] == 'invalid': return object()
    return {'event': event, 'context': asdict(current_invocation()), 'pid': os.getpid()}
"#,
    );
    let mut session = ready(runtime.start(artifact, control()).await);
    let pid = session.pid();
    let carrier = TraceContext {
        traceparent: "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01".into(),
        tracestate: Some("vendor=value".into()),
    };
    for id in ["failure", "invalid", "success", "off"] {
        let event = event(id);
        let result = session
            .execute(
                RuntimeInvocation {
                    extension: None,
                    event: event.clone(),
                    processing_context: (id != "off").then(|| carrier.clone()),
                },
                control(),
            )
            .await
            .unwrap();
        match id {
            "failure" | "invalid" => assert!(matches!(result, ProgramOutcome::Failure { .. })),
            _ => {
                let value = output(result);
                assert_eq!(value["pid"], pid);
                assert_eq!(value["event"], *event.value());
                assert_eq!(value["context"]["task_id"], "task-a");
                if id == "off" {
                    assert!(value["context"]["processing_context"].is_null());
                } else {
                    assert_eq!(
                        value["context"]["processing_context"]["traceparent"],
                        carrier.traceparent
                    );
                }
            }
        }
    }
    session.close().await.unwrap();
}

#[tokio::test]
async fn v2_idle_log_flood_keeps_session_reusable_and_close_fair() {
    let (_dir, artifact, runtime) = v2_fixture(
        r#"import threading, time
from ledgence_worker import get_logger
log = get_logger('background')
stop = threading.Event()
def flood():
    while not stop.is_set():
        for _ in range(30): log.info('unicode 😀' * 500)
        time.sleep(.001)
threading.Thread(target=flood, daemon=True).start()
def handle(event):
    log.info('handler')
    return event['id']
"#,
    );
    let mut session = ready(runtime.start(artifact, control()).await);
    let pid = session.pid();
    tokio::time::sleep(Duration::from_millis(100)).await;
    for id in ["first", "second"] {
        let value = tokio::time::timeout(
            Duration::from_secs(2),
            session.execute(event(id).into(), control()),
        )
        .await
        .expect("telemetry flood must not starve execution")
        .unwrap();
        assert_eq!(output(value), id);
        assert_eq!(session.pid(), pid);
    }
    tokio::time::timeout(Duration::from_secs(3), session.close())
        .await
        .expect("telemetry flood must not starve close")
        .unwrap();
    #[cfg(unix)]
    assert!(!running(pid));
}

#[tokio::test]
async fn v2_malformed_optional_logs_are_dropped_but_wrong_result_identity_retires() {
    for wrong_result in [false, true] {
        let (dir, artifact, _) = v2_fixture("def handle(event): return 42");
        let runner = dir.path().join("telemetry_runner.py");
        let source = format!(
            r#"import json, os, sys
print(json.dumps({{'v':2,'type':'ready','pid':os.getpid(),'python_version':f'{{sys.version_info.major}}.{{sys.version_info.minor}}'}}), flush=True)
while True:
    message = json.loads(sys.stdin.readline())
    if message['type'] == 'shutdown':
        print('{{"v":2,"type":"closing"}}', flush=True)
        sys.stdin.read()
        break
    print('{{"v":2,"type":"log","message":[]}}', flush=True)
    print('{{"v":9,"type":"log"}}', flush=True)
    print(json.dumps({{'v':2,'type':'result','event_id':message['event_id'],
        'attempt_id': 'WRONG' if {wrong_result} else message['attempt_id'], 'status':'success','output':42}}),flush=True)
"#,
            wrong_result = if wrong_result { "True" } else { "False" }
        );
        std::fs::write(&runner, source).unwrap();
        let runtime = SubprocessRuntime::new(interpreter().0, runner);
        let mut session = ready(runtime.start(artifact, control()).await);
        let result = session.execute(event("first").into(), control()).await;
        if wrong_result {
            assert_eq!(result.unwrap_err().kind, ErrorKind::Protocol);
        } else {
            assert_eq!(output(result.unwrap()), 42);
            assert_eq!(
                output(
                    session
                        .execute(event("second").into(), control())
                        .await
                        .unwrap()
                ),
                42
            );
        }
        session.close().await.unwrap();
    }
}

#[tokio::test]
async fn v2_helper_mismatch_fails_readiness_before_handler_and_callbacks_are_bounded() {
    let (dir, artifact, _) = v2_fixture("def handle(event): return 42");
    let runner = dir.path().join("v1_runner.py");
    std::fs::write(&runner, "import json,os,sys\nprint(json.dumps({'v':1,'type':'ready','pid':os.getpid(),'python_version':f'{sys.version_info.major}.{sys.version_info.minor}'}),flush=True)\nsys.stdin.read()\n").unwrap();
    let runtime = SubprocessRuntime::new(interpreter().0, runner);
    match runtime.start(artifact, control()).await {
        Err(error) => assert_eq!(error.kind, ErrorKind::Protocol),
        _ => panic!("mismatched protocol must fail startup"),
    }
    let (_dir, artifact, runtime) = v2_fixture(
        r#"import time
from ledgence_worker import register_shutdown
register_shutdown(lambda: time.sleep(60))
def handle(event): return 42
"#,
    );
    let runtime = runtime.with_config(SubprocessConfig {
        shutdown_timeout: Duration::from_millis(100),
        ..SubprocessConfig::default()
    });
    let mut session = ready(runtime.start(artifact, control()).await);
    let pid = session.pid();
    tokio::time::timeout(Duration::from_secs(2), session.close())
        .await
        .expect("application exporter cannot extend parent shutdown deadline")
        .unwrap();
    #[cfg(unix)]
    assert!(!running(pid));
}

#[derive(Clone)]
struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for CapturedLogs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test]
async fn v2_late_log_carries_creation_identity_while_another_invocation_runs() {
    let captured = CapturedLogs(Arc::new(std::sync::Mutex::new(Vec::new())));
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_writer(captured.clone())
        .finish();
    // Other concurrent tests intentionally have no subscriber. Keep multiple
    // registered dispatchers so tracing-core's single-dispatcher callsite fast
    // path cannot cache a callsite as disabled from one of those test threads.
    let _other_dispatch = tracing::Dispatch::new(tracing_subscriber::registry());
    let _subscriber = tracing::subscriber::set_default(subscriber);
    let (_dir, artifact, runtime) = v2_fixture(
        r#"import contextvars, threading, time
from ledgence_worker import get_logger, _logging
log = get_logger('late')
release, finished = threading.Event(), threading.Event()
def later():
    release.wait()
    log.info('old invocation')
    finished.set()
def handle(event):
    if event['id'] == 'first':
        old_context = contextvars.copy_context()
        threading.Thread(target=lambda: old_context.run(later), daemon=True).start()
    else:
        release.set()
        assert finished.wait(2)
        for _ in range(2000):
            with _logging._sink._condition:
                if not _logging._sink._logs: break
            time.sleep(.001)
        else: raise ValueError("writer did not take queued log")
    return 42
"#,
    );
    let mut session = ready(runtime.start(artifact, control()).await);
    for id in ["first", "second"] {
        let carrier = TraceContext {
            traceparent: format!(
                "00-{}-{}-01",
                "a".repeat(32),
                if id == "first" {
                    "b".repeat(16)
                } else {
                    "c".repeat(16)
                }
            ),
            tracestate: None,
        };
        session
            .execute(
                RuntimeInvocation {
                    extension: None,
                    event: event(id),
                    processing_context: Some(carrier),
                },
                control(),
            )
            .await
            .unwrap();
    }
    session.close().await.unwrap();
    let bytes = captured.0.lock().unwrap();
    let records: Vec<Value> = bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    let late = records
        .iter()
        .find(|record| record["fields"]["text"] == "old invocation")
        .unwrap_or_else(|| {
            panic!(
                "late program log should reach worker JSON output: {}",
                String::from_utf8_lossy(&bytes)
            )
        });
    assert_eq!(late["fields"]["event_id"], "first");
    assert_eq!(late["fields"]["attempt_id"], "attempt-first");
    assert_eq!(late["fields"]["span_id"], "b".repeat(16));
}

#[tokio::test]
async fn v3_program_logs_preserve_workflow_scope_and_clear_it_on_reuse() {
    let captured = CapturedLogs(Arc::new(std::sync::Mutex::new(Vec::new())));
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_writer(captured.clone())
        .finish();
    let _other_dispatch = tracing::Dispatch::new(tracing_subscriber::registry());
    let _subscriber = tracing::subscriber::set_default(subscriber);
    let (_dir, artifact, runtime) = fixture(
        "from ledgence_worker import get_logger\ndef handle(event):\n    get_logger('workflow').info('workflow scope')\n    return 42\n",
    );
    let mut manifest = artifact.manifest().clone();
    manifest.runtime.protocol = 3;
    let artifact = PreparedArtifact::new(
        artifact.root().to_owned(),
        manifest,
        artifact.digest().clone(),
        Arc::new(()),
    );
    let mut session = ready(runtime.start(artifact, control()).await);
    let mut scoped = event("workflow").into_value();
    scoped["ldgworkflowid"] = json!("workflow-1");
    scoped["ldgactivationid"] = scoped["ldgtaskid"].clone();
    scoped["ldgparentworkflowid"] = json!("parent-1");
    scoped["ldgrootworkflowid"] = json!("root-1");
    session
        .execute(CloudEvent::new(scoped).unwrap().into(), control())
        .await
        .unwrap();
    session
        .execute(event("plain").into(), control())
        .await
        .unwrap();
    session.close().await.unwrap();
    let bytes = captured.0.lock().unwrap().clone();
    let text = String::from_utf8(bytes).unwrap();
    let logs: Vec<Value> = text
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|record| {
            record["target"] == "ledgence::program_log"
                && record["fields"]["text"] == "workflow scope"
        })
        .collect();
    assert_eq!(logs.len(), 2, "{text}");
    assert_eq!(logs[0]["fields"]["workflow_id"], "workflow-1");
    assert_eq!(logs[0]["fields"]["parent_workflow_id"], "parent-1");
    assert_eq!(logs[0]["fields"]["root_workflow_id"], "root-1");
    assert_eq!(logs[0]["fields"]["activation_id"], "task-a");
    assert!(logs[1]["fields"].get("workflow_id").is_none());
    assert!(logs[1]["fields"].get("parent_workflow_id").is_none());
    assert!(logs[1]["fields"].get("root_workflow_id").is_none());
    assert!(logs[1]["fields"].get("activation_id").is_none());
}
