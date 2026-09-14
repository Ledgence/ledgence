//! Exercise the Rust transport independently of the bundled Python SDK.
use super::*;
use ledgence_worker_api::{
    Error, PortFuture, RuntimeExtension, RuntimeInvocation, RuntimeReply, RuntimeRequest,
    RuntimeRequestHandler,
};
use std::sync::atomic::{AtomicUsize, Ordering};

fn scripted(source: &str) -> (TempDir, PreparedArtifact, SubprocessRuntime) {
    let (dir, artifact, _) = fixture("");
    let mut manifest = artifact.manifest().clone();
    manifest.runtime.protocol = 3;
    let artifact = PreparedArtifact::new(
        dir.path().to_owned(),
        manifest,
        artifact.digest().clone(),
        Arc::new(()),
    );
    let helper = dir.path().join("transport_fixture.py");
    std::fs::write(&helper, format!(r#"import json, os, sys
def send(value):
    print(json.dumps(value), flush=True)
def receive():
    return json.loads(sys.stdin.readline())
def bound(invocation, kind, **fields):
    return dict(v=3, type=kind, event_id=invocation['event_id'], attempt_id=invocation['attempt_id'], **fields)
send(dict(v=3,type='ready',pid=os.getpid(),python_version=f'{{sys.version_info.major}}.{{sys.version_info.minor}}'))
{source}
"#)).unwrap();
    let (python, _) = interpreter();
    (dir, artifact, SubprocessRuntime::new(python, helper))
}
fn invocation(id: &str) -> RuntimeInvocation {
    RuntimeInvocation {
        event: event(id),
        processing_context: None,
        extension: Some(RuntimeExtension {
            schema: "test.activation.v1".into(),
            payload: json!({"checkpoint": id}),
        }),
    }
}
#[derive(Clone, Copy)]
enum ReplyMode {
    Echo,
    WrongId,
    Unavailable,
    Pending,
}
struct Handler {
    mode: ReplyMode,
    calls: AtomicUsize,
    dropped: Arc<AtomicUsize>,
}
impl Handler {
    fn new(mode: ReplyMode) -> Arc<Self> {
        Arc::new(Self {
            mode,
            calls: AtomicUsize::new(0),
            dropped: Arc::new(AtomicUsize::new(0)),
        })
    }
}
struct DropNotice(Arc<AtomicUsize>);
impl Drop for DropNotice {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl RuntimeRequestHandler for Handler {
    fn handle<'a>(
        &'a self,
        request: RuntimeRequest,
        control: RunControl,
    ) -> PortFuture<'a, RuntimeReply> {
        Box::pin(async move {
            control.check()?;
            self.calls.fetch_add(1, Ordering::SeqCst);
            let _drop = DropNotice(self.dropped.clone());
            assert_eq!(request.operation, "test.commit");
            match self.mode {
                ReplyMode::Echo => Ok(RuntimeReply {
                    id: request.id,
                    result: request.payload,
                }),
                ReplyMode::WrongId => Ok(RuntimeReply {
                    id: request.id + 1,
                    result: Value::Null,
                }),
                ReplyMode::Unavailable => Err(Error::new(
                    ErrorKind::Unavailable,
                    "commit acknowledgement unavailable",
                )),
                ReplyMode::Pending => std::future::pending().await,
            }
        })
    }
}
const REQUEST_AND_WAIT: &str = "i=receive()\nsend(bound(i,'runtime_request',id=1,operation='test.commit',payload={'step':'one'}))\nreceive()\n";

#[tokio::test]
async fn v3_acknowledges_sequential_requests_and_reuses_with_fresh_context() {
    let (_dir, artifact, runtime) = scripted(
        r#"
while True:
    i=receive()
    if i['type']=='shutdown':
        send(dict(v=3,type='bye'))
        break
    for n in (1,2):
        payload={'step':n, 'checkpoint':i['extension']['payload']['checkpoint']}
        send(bound(i,'runtime_request',id=n,operation='test.commit',payload=payload))
        reply=receive()
        assert reply==bound(i,'runtime_reply',id=n,result=payload), reply
    send(bound(i,'result',status='success',output={'event':i['event'],'extension':i['extension']}))
"#,
    );
    let handler = Handler::new(ReplyMode::Echo);
    let mut session = ready(runtime.start(artifact, control()).await);
    let pid = session.pid();
    for id in ["first", "second"] {
        let invocation = invocation(id);
        let value = output(
            session
                .execute_with_requests(invocation.clone(), control(), handler.clone())
                .await
                .unwrap(),
        );
        assert_eq!(value["event"], *invocation.event.value());
        assert_eq!(
            value["extension"],
            serde_json::to_value(invocation.extension.unwrap()).unwrap()
        );
        assert_eq!(session.pid(), pid);
    }
    assert_eq!(handler.calls.load(Ordering::SeqCst), 4);
    session.close().await.unwrap();
}

#[tokio::test]
async fn v3_rejects_invalid_request_identity_sequence_and_unknown_fields() {
    for mutation in [
        "r['id']=0",
        "r['id']=2",
        "r['id']=True",
        "r['event_id']='stale'",
        "r['attempt_id']='stale'",
        "r['v']=2",
        "r['extra']='unknown'",
    ] {
        let source = format!(
            "i=receive()\nr=bound(i,'runtime_request',id=1,operation='test.commit',payload={{}})\n{mutation}\nsend(r)\nreceive()\n"
        );
        let (_dir, artifact, runtime) = scripted(&source);
        let handler = Handler::new(ReplyMode::Echo);
        let mut session = ready(runtime.start(artifact, control()).await);
        let pid = session.pid();
        let error = session
            .execute_with_requests(invocation("bad"), control(), handler.clone())
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol, "{mutation}");
        assert_eq!(handler.calls.load(Ordering::SeqCst), 0);
        session.close().await.unwrap();
        #[cfg(unix)]
        assert!(!running(pid));
    }
}

#[tokio::test]
async fn v3_rejects_replayed_request_ids_without_repeating_callback() {
    let (_dir, artifact, runtime) = scripted(&format!(
        "{REQUEST_AND_WAIT}send(bound(i,'runtime_request',id=1,operation='test.commit',payload={{}}))\nreceive()\n"
    ));
    let handler = Handler::new(ReplyMode::Echo);
    let mut session = ready(runtime.start(artifact, control()).await);
    assert_eq!(
        session
            .execute_with_requests(invocation("replay"), control(), handler.clone())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Protocol
    );
    assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    session.close().await.unwrap();
}

#[tokio::test]
async fn v3_callback_errors_and_mismatched_acknowledgements_retire_process() {
    for (mode, expected) in [
        (ReplyMode::WrongId, ErrorKind::Protocol),
        (ReplyMode::Unavailable, ErrorKind::Unavailable),
    ] {
        let (_dir, artifact, runtime) = scripted(REQUEST_AND_WAIT);
        let mut session = ready(runtime.start(artifact, control()).await);
        let pid = session.pid();
        let error = session
            .execute_with_requests(invocation("failed"), control(), Handler::new(mode))
            .await
            .unwrap_err();
        assert_eq!(error.kind, expected);
        #[cfg(unix)]
        assert!(!running(pid));
        assert!(
            session
                .execute(event("later").into(), control())
                .await
                .is_err()
        );
        session.close().await.unwrap();
    }
}

#[tokio::test]
async fn v3_pending_callback_obeys_deadline_and_dropped_caller() {
    for drop_caller in [false, true] {
        let (_dir, artifact, runtime) = scripted(REQUEST_AND_WAIT);
        let mut session = ready(runtime.start(artifact, control()).await);
        let pid = session.pid();
        let handler = Handler::new(ReplyMode::Pending);
        if drop_caller {
            let future =
                session.execute_with_requests(invocation("drop"), control(), handler.clone());
            let result = tokio::time::timeout(Duration::from_millis(100), future).await;
            assert!(result.is_err());
            session.close().await.unwrap();
        } else {
            let result = session
                .execute_with_requests(
                    invocation("timeout"),
                    RunControl::new(Duration::from_millis(100)),
                    handler.clone(),
                )
                .await;
            assert_eq!(result.unwrap_err().kind, ErrorKind::TimedOut);
            session.close().await.unwrap();
        }
        assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
        assert_eq!(handler.dropped.load(Ordering::SeqCst), 1);
        #[cfg(unix)]
        assert!(!running(pid));
    }
}

#[tokio::test]
async fn v3_runtime_error_is_retryable_execution_failure_and_plain_requests_are_rejected() {
    let (_dir, artifact, runtime) = scripted(
        "i=receive()\nsend(bound(i,'result',status='runtime_error',error={'kind':'controller_error','message':'retry activation'}))\nreceive()\n",
    );
    let mut session = ready(runtime.start(artifact, control()).await);
    let pid = session.pid();
    let error = session
        .execute_with_requests(
            invocation("failed"),
            control(),
            Handler::new(ReplyMode::Echo),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Runtime);
    #[cfg(unix)]
    assert!(!running(pid));
    session.close().await.unwrap();

    let (_dir, artifact, runtime) = scripted(REQUEST_AND_WAIT);
    let mut session = ready(runtime.start(artifact, control()).await);
    assert_eq!(
        session
            .execute(event("plain").into(), control())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Protocol
    );
    session.close().await.unwrap();
}
