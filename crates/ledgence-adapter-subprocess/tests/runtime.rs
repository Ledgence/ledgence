use ledgence_adapter_subprocess::{SubprocessConfig, SubprocessRuntime};
use ledgence_worker_api::{
    CloudEvent, Digest, ErrorKind, ExecutionRuntime, ExecutionSession, MAX_WIRE_VALUE_DEPTH,
    Platform, PreparedArtifact, ProgramManifest, ProgramOutcome, ProgramRef, PythonRuntime,
    RunControl, StartOutcome,
};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tempfile::TempDir;

#[path = "runtime/delivery.rs"]
mod delivery;
#[path = "runtime/observability.rs"]
mod observability;

fn interpreter() -> (PathBuf, String) {
    let python = std::env::var_os("LEDGENCE_PYTHON")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("python3"));
    let output = std::process::Command::new(&python).args(["-I", "-S", "-c", "import sys; assert sys.implementation.name == 'cpython' and sys.version_info >= (3,11); print(f'{sys.version_info.major}.{sys.version_info.minor}')"]).output().expect("set LEDGENCE_PYTHON to CPython >= 3.11");
    assert!(
        output.status.success(),
        "set LEDGENCE_PYTHON to CPython >= 3.11: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    (
        python,
        String::from_utf8(output.stdout).unwrap().trim().to_owned(),
    )
}

fn runner() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../sdk/python/ledgence_worker/bootstrap.py")
}

fn fixture(source: &str) -> (TempDir, PreparedArtifact, SubprocessRuntime) {
    let (python, version) = interpreter();
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("program.py"), source).unwrap();
    let manifest = ProgramManifest {
        schema_version: 1,
        program: ProgramRef {
            id: "test".into(),
            version: "v1".into(),
        },
        runtime: PythonRuntime {
            kind: "python".into(),
            python: version,
            protocol: 1,
        },
        handler: "program:handle".into(),
        platform: Platform {
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
        },
    };
    let artifact = PreparedArtifact::new(
        dir.path().to_owned(),
        manifest,
        Digest(format!("sha256:{}", "0".repeat(64))),
        Arc::new(()),
    );
    (dir, artifact, SubprocessRuntime::new(python, runner()))
}

fn event(id: &str) -> CloudEvent {
    CloudEvent::new(json!({
        "specversion": "1.0", "id": id, "source": "urn:ledgence:test", "type": "example.execute.v1",
        "datacontenttype": "application/json", "subject": "invoice/42", "time": "2026-09-12T12:00:00Z",
        "ldgtenantid": "tenant-a", "ldgnamespace": "default", "ldgrunid": "run-a", "ldgtaskid": "task-a",
        "ldgattemptid": format!("attempt-{id}"), "ldgattemptno": 1,
        "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "customextension": "preserved", "data": {"invoice_id": 42, "nested": [null, true, {"value": "✓"}]}
    })).unwrap()
}

fn control() -> RunControl {
    RunControl::new(Duration::from_secs(5))
}
fn ready(result: ledgence_worker_api::Result<StartOutcome>) -> Box<dyn ExecutionSession> {
    match result.expect("runtime should start") {
        StartOutcome::Ready(session) => session,
        StartOutcome::CleanupRequired { error, .. } => {
            panic!("unexpected startup cleanup uncertainty: {error}")
        }
    }
}
fn output(value: ProgramOutcome) -> Value {
    match value {
        ProgramOutcome::Success { output } => output,
        other => panic!("unexpected result: {other:?}"),
    }
}

#[cfg(unix)]
fn running(pid: u32) -> bool {
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid.try_into().unwrap()), None).is_ok()
}

#[cfg(unix)]
async fn eventually_gone(pid: u32) {
    for _ in 0..100 {
        if !running(pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("process {pid} was not reaped");
}

#[tokio::test]
async fn reuses_process_preserves_full_event_and_isolates_invocation_context() {
    let (_dir, artifact, runtime) = fixture(
        "import os, contextvars\nfrom ledgence_worker import current_invocation\nstate = contextvars.ContextVar('state', default='clean')\ndef handle(event):\n    previous = state.get()\n    state.set('dirty')\n    return {'pid': os.getpid(), 'event': event, 'attempt': current_invocation().attempt_id, 'previous': previous}\n",
    );
    let mut session = ready(runtime.start(artifact, control()).await);
    let pid = session.pid();
    for id in ["first", "second"] {
        let event = event(id);
        let result = output(
            session
                .execute(event.clone().into(), control())
                .await
                .unwrap(),
        );
        assert_eq!(result["pid"], pid);
        assert_eq!(result["event"], *event.value());
        assert_eq!(result["attempt"], event.attempt_id());
        assert_eq!(result["previous"], "clean");
    }
    session.close().await.unwrap();
    session.close().await.unwrap();
    #[cfg(unix)]
    assert!(!running(pid));
}

#[tokio::test]
async fn business_failures_and_invalid_outputs_allow_reuse() {
    let (_dir, artifact, runtime) = fixture(
        "def handle(event):\n    if event['id'] == 'business': raise ValueError('bad invoice')\n    if event['id'] == 'nan': return float('nan')\n    return 42\n",
    );
    let mut session = ready(runtime.start(artifact, control()).await);
    for (id, expected) in [("business", "business_error"), ("nan", "invalid_output")] {
        let result = session.execute(event(id).into(), control()).await.unwrap();
        assert!(matches!(result, ProgramOutcome::Failure { kind, .. } if kind == expected));
    }
    assert_eq!(
        output(
            session
                .execute(event("success").into(), control())
                .await
                .unwrap()
        ),
        42
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn shared_wire_profile_rejects_lossy_outputs_and_preserves_session_reuse() {
    let (_dir, artifact, runtime) = fixture(
        r#"def handle(event):
    kind = event['id']
    if kind == 'keys': return {1: 'first', '1': 'second'}
    if kind == 'floatkeys': return {1.5: 'first', '1.5': 'second'}
    if kind == 'surrogate': return '\ud800'
    if kind == 'surrogatekey': return {'\udfff': 1}
    if kind == 'largeint': return 1 << 64
    if kind == 'smallint': return -(1 << 63) - 1
    if kind == 'hugeint': return 10**400
    if kind == 'depth':
        value = 0
        for _ in range(event['data']['depth']): value = [value]
        return value
    return [-(1 << 63), (1 << 64) - 1, 1.7976931348623157e308, '😀\x00\ufffe']
"#,
    );
    let mut session = ready(runtime.start(artifact, control()).await);
    let pid = session.pid();
    for id in [
        "keys",
        "floatkeys",
        "surrogate",
        "surrogatekey",
        "largeint",
        "smallint",
        "hugeint",
    ] {
        let result = session.execute(event(id).into(), control()).await.unwrap();
        assert!(matches!(result, ProgramOutcome::Failure { kind, .. } if kind == "invalid_output"));
        assert_eq!(session.pid(), pid);
    }
    for (depth, valid) in [
        (MAX_WIRE_VALUE_DEPTH, true),
        (MAX_WIRE_VALUE_DEPTH + 1, false),
    ] {
        let mut envelope = event("depth").into_value();
        envelope["data"] = json!({"depth": depth});
        let result = session
            .execute(CloudEvent::new(envelope).unwrap().into(), control())
            .await
            .unwrap();
        if valid {
            let mut value = output(result);
            for _ in 0..depth {
                value = value.as_array_mut().unwrap().remove(0);
            }
            assert_eq!(value, 0);
        } else {
            assert!(
                matches!(result, ProgramOutcome::Failure { kind, .. } if kind == "invalid_output")
            );
        }
    }
    let result = output(
        session
            .execute(event("valid").into(), control())
            .await
            .unwrap(),
    );
    assert_eq!(
        result,
        json!([i64::MIN, u64::MAX, f64::MAX, "😀\u{0}\u{fffe}"])
    );
    assert_eq!(session.pid(), pid);
    session.close().await.unwrap();
}

#[tokio::test]
async fn encoded_failure_budget_includes_unicode_identities_and_preserves_reuse() {
    let (_dir, artifact, runtime) = fixture(
        "def handle(event):\n    if event['data']['fail']: raise ValueError('\"\\\\😀'*500)\n    return 42\n",
    );
    let runtime = runtime.with_config(SubprocessConfig {
        max_frame_bytes: 1024,
        ..SubprocessConfig::default()
    });
    let mut session = ready(runtime.start(artifact, control()).await);
    let pid = session.pid();
    for fail in [true, false] {
        let mut envelope = event(&"😀".repeat(20)).into_value();
        envelope["data"] = json!({"fail": fail});
        let result = session
            .execute(CloudEvent::new(envelope).unwrap().into(), control())
            .await
            .unwrap();
        if fail {
            assert!(
                matches!(result, ProgramOutcome::Failure { kind, .. } if kind == "business_error")
            );
        } else {
            assert_eq!(output(result), 42);
        }
    }
    assert_eq!(session.pid(), pid);
    session.close().await.unwrap();
}

#[tokio::test]
async fn normal_imports_do_not_write_bytecode_into_a_writable_artifact() {
    let (directory, artifact, runtime) =
        fixture("import helper\ndef handle(event): return helper.VALUE\n");
    std::fs::write(directory.path().join("helper.py"), "VALUE = 42\n").unwrap();
    let mut session = ready(runtime.start(artifact, control()).await);
    assert_eq!(
        output(
            session
                .execute(event("first").into(), control())
                .await
                .unwrap()
        ),
        42
    );
    assert!(!directory.path().join("__pycache__").exists());
    session.close().await.unwrap();
}

#[tokio::test]
async fn large_stderr_and_native_stdout_are_drained_without_protocol_corruption() {
    let (_dir, artifact, runtime) = fixture(
        "import os\ndef handle(event):\n    print('normal print')\n    for _ in range(128): os.write(1, b'x' * 8192)\n    os.write(2, b'z' * 8192)\n    return event['id']\n",
    );
    let runtime = runtime.with_config(SubprocessConfig {
        max_log_bytes: 128,
        ..SubprocessConfig::default()
    });
    let mut session = ready(runtime.start(artifact, control()).await);
    assert_eq!(
        output(
            session
                .execute(event("first").into(), control())
                .await
                .unwrap()
        ),
        "first"
    );
    assert_eq!(
        output(
            session
                .execute(event("second").into(), control())
                .await
                .unwrap()
        ),
        "second"
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn timeout_retires_and_reaps_before_returning() {
    let (_dir, artifact, runtime) =
        fixture("import time\ndef handle(event):\n    time.sleep(30)\n");
    let mut session = ready(runtime.start(artifact, control()).await);
    let pid = session.pid();
    let error = session
        .execute(
            event("slow").into(),
            RunControl::new(Duration::from_millis(50)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::TimedOut);
    #[cfg(unix)]
    assert!(!running(pid));
    assert!(
        session
            .execute(event("again").into(), control())
            .await
            .is_err()
    );
    session.close().await.unwrap();
}

#[tokio::test]
async fn cancellation_retires_and_reaps_before_returning() {
    let (_dir, artifact, runtime) =
        fixture("import time\ndef handle(event):\n    time.sleep(30)\n");
    let mut session = ready(runtime.start(artifact, control()).await);
    let pid = session.pid();
    let run = control();
    let cancel = run.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
    });
    let error = session
        .execute(event("cancel").into(), run)
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Cancelled);
    #[cfg(unix)]
    assert!(!running(pid));
    session.close().await.unwrap();
}

#[tokio::test]
async fn dropping_execute_future_cleans_up_even_with_session_still_owned() {
    let (_dir, artifact, runtime) =
        fixture("import time\ndef handle(event):\n    time.sleep(30)\n");
    let mut session = ready(runtime.start(artifact, control()).await);
    let pid = session.pid();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            session.execute(event("drop").into(), control())
        )
        .await
        .is_err()
    );
    #[cfg(unix)]
    eventually_gone(pid).await;
    session.close().await.unwrap();
}

#[tokio::test]
async fn dropping_idle_session_reaps_child_and_releases_artifact_pin() {
    let (_dir, artifact, runtime) = fixture("def handle(event): return None\n");
    let pin = Arc::new(());
    let weak = Arc::downgrade(&pin);
    let artifact = PreparedArtifact::new(
        artifact.root().to_owned(),
        artifact.manifest().clone(),
        artifact.digest().clone(),
        pin,
    );
    let session = ready(runtime.start(artifact, control()).await);
    let pid = session.pid();
    assert!(weak.upgrade().is_some());
    drop(session);
    #[cfg(unix)]
    eventually_gone(pid).await;
    for _ in 0..100 {
        if weak.upgrade().is_none() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("artifact pin leaked after child cleanup");
}

#[tokio::test]
async fn crash_returns_uncertain_runtime_error_and_session_is_retired() {
    let (_dir, artifact, runtime) = fixture(
        "import os\ndef handle(event):\n    if event['id'] == 'crash': os._exit(23)\n    return os.getcwd()\n",
    );
    let pin = Arc::new(());
    let weak = Arc::downgrade(&pin);
    let artifact = PreparedArtifact::new(
        artifact.root().to_owned(),
        artifact.manifest().clone(),
        artifact.digest().clone(),
        pin,
    );
    let mut session = ready(runtime.start(artifact, control()).await);
    let pid = session.pid();
    let working = output(
        session
            .execute(event("warm").into(), control())
            .await
            .unwrap(),
    );
    let workspace = PathBuf::from(working.as_str().unwrap());
    assert!(workspace.exists());
    assert!(weak.upgrade().is_some());
    let error = session
        .execute(event("crash").into(), control())
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Runtime);
    assert!(error.message.contains("result is unavailable"));
    #[cfg(unix)]
    assert!(!running(pid));
    session.close().await.unwrap();
    session.close().await.unwrap();
    assert!(!workspace.exists());
    assert!(weak.upgrade().is_none());
}

#[tokio::test]
async fn normal_close_ack_avoids_the_darwin_zombie_race_and_is_idempotent() {
    let (_dir, artifact, runtime) = fixture("def handle(event): return event['id']\n");
    // Repeated short sessions specifically exercise the former acknowledgement /
    // child-exit race. Every close must confirm cleanup.
    for index in 0..30 {
        let mut session = ready(runtime.start(artifact.clone(), control()).await);
        let pid = session.pid();
        session
            .execute(event(&format!("close-{index}")).into(), control())
            .await
            .unwrap();
        session.close().await.unwrap();
        session.close().await.unwrap();
        #[cfg(unix)]
        assert!(!running(pid));
    }
}

#[tokio::test]
async fn relative_writes_use_a_separate_session_workspace_removed_after_close() {
    let (dir, artifact, runtime) = fixture(
        "import os\nfrom pathlib import Path\ndef handle(event):\n    state=Path('state.txt')\n    previous=state.read_text() if state.exists() else None\n    state.write_text(event['id'])\n    return {'cwd': os.getcwd(), 'previous': previous}\n",
    );
    let mut first = ready(runtime.start(artifact.clone(), control()).await);
    let initial = output(
        first
            .execute(event("first").into(), control())
            .await
            .unwrap(),
    );
    let working = PathBuf::from(initial["cwd"].as_str().unwrap());
    assert_ne!(working, std::fs::canonicalize(dir.path()).unwrap());
    assert!(!working.starts_with(std::fs::canonicalize(dir.path()).unwrap()));
    assert!(!dir.path().join("state.txt").exists());
    assert_eq!(
        std::fs::read_to_string(working.join("state.txt")).unwrap(),
        "first"
    );

    let next = output(
        first
            .execute(event("second").into(), control())
            .await
            .unwrap(),
    );
    assert_eq!(next["previous"], "first");
    assert_eq!(next["cwd"], initial["cwd"]);
    let mut second = ready(runtime.start(artifact, control()).await);
    let independent = output(
        second
            .execute(event("other").into(), control())
            .await
            .unwrap(),
    );
    let other_working = PathBuf::from(independent["cwd"].as_str().unwrap());
    assert_ne!(other_working, working);
    assert!(independent["previous"].is_null());

    first.close().await.unwrap();
    assert!(
        !working.exists(),
        "confirmed close must remove session working state"
    );
    assert!(
        other_working.exists(),
        "another session owns its working state"
    );
    second.close().await.unwrap();
    assert!(!other_working.exists());
    assert!(!dir.path().join("state.txt").exists());
}

#[tokio::test]
async fn protocol_identity_mismatch_and_oversized_frames_are_retired() {
    for oversized in [false, true] {
        let (dir, artifact, _runtime) = fixture("def handle(event): return None\n");
        let runner = dir.path().join("bad_runner.py");
        let source = if oversized {
            "import os,sys,json\nprint(json.dumps({'v':1,'type':'ready','pid':os.getpid(),'python_version':f'{sys.version_info.major}.{sys.version_info.minor}'}),flush=True)\nsys.stdin.readline()\nprint('x'*4096,flush=True)\n"
        } else {
            "import os,sys,json\nprint(json.dumps({'v':1,'type':'ready','pid':os.getpid(),'python_version':f'{sys.version_info.major}.{sys.version_info.minor}'}),flush=True)\nrequest=json.loads(sys.stdin.readline())\nprint(json.dumps({'v':1,'type':'result','event_id':request['event_id'],'attempt_id':'WRONG','status':'success','output':42}),flush=True)\n"
        };
        std::fs::write(&runner, source).unwrap();
        let runtime =
            SubprocessRuntime::new(interpreter().0, runner).with_config(SubprocessConfig {
                max_frame_bytes: 2048,
                ..SubprocessConfig::default()
            });
        let mut session = ready(runtime.start(artifact, control()).await);
        let pid = session.pid();
        let error = session
            .execute(event("bad").into(), control())
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Protocol);
        #[cfg(unix)]
        assert!(!running(pid));
        session.close().await.unwrap();
    }
}

#[tokio::test]
async fn startup_timeout_and_dropped_start_future_release_process_and_pin() {
    for drop_future in [false, true] {
        let (dir, artifact, runtime) = fixture(
            "import os,time\nopen(__file__ + '.started.pid','w').write(str(os.getpid()))\ntime.sleep(30)\ndef handle(event): return None\n",
        );
        let runtime = runtime.with_config(SubprocessConfig {
            startup_timeout: Duration::from_millis(200),
            ..SubprocessConfig::default()
        });
        if drop_future {
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(100),
                    runtime.start(artifact, control())
                )
                .await
                .is_err()
            );
        } else {
            let error = match runtime.start(artifact, control()).await {
                Ok(_) => panic!("startup should time out"),
                Err(error) => error,
            };
            assert_eq!(error.kind, ErrorKind::TimedOut);
        }
        let pid: u32 = std::fs::read_to_string(dir.path().join("program.py.started.pid"))
            .unwrap()
            .parse()
            .unwrap();
        #[cfg(unix)]
        eventually_gone(pid).await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn failed_startup_retains_cleanup_handle_artifact_and_scratch_until_recovered() {
    let (dir, artifact, _runtime) = fixture("def handle(event): return None\n");
    let pin = Arc::new(());
    let weak = Arc::downgrade(&pin);
    let artifact = PreparedArtifact::new(
        artifact.root().to_owned(),
        artifact.manifest().clone(),
        artifact.digest().clone(),
        pin,
    );
    let runner = dir.path().join("obstructed_workspace_runner.py");
    // A regular file at the recorded directory path makes remove_dir_all fail
    // even for root. Keep the real working state in a sibling until recovery.
    std::fs::write(
        &runner,
        r#"import json,os,sys,time
from pathlib import Path
root=Path(sys.argv[sys.argv.index('--package-root')+1])
workspace=Path.cwd()
retained=workspace.with_name(workspace.name+'.retained')
(workspace/'state.txt').write_text('preserved')
workspace.rename(retained)
workspace.write_text('cleanup obstruction')
(root/'scratch.json').write_text(json.dumps({'cwd':str(workspace),'retained':str(retained),'pid':os.getpid()}))
print(json.dumps({'v':1,'type':'invalid-ready'}),flush=True)
while True: time.sleep(1)
"#,
    )
    .unwrap();
    let runtime = SubprocessRuntime::new(interpreter().0, runner);
    let mut session = match runtime.start(artifact, control()).await {
        Ok(StartOutcome::CleanupRequired { error, session }) => {
            assert_eq!(error.kind, ErrorKind::Protocol);
            session
        }
        Err(error) => panic!("startup discarded unresolved cleanup ownership: {error}"),
        Ok(StartOutcome::Ready(_)) => panic!("invalid startup cannot produce a ready session"),
    };
    let recorded: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("scratch.json")).unwrap()).unwrap();
    let workspace = PathBuf::from(recorded["cwd"].as_str().unwrap());
    let retained = PathBuf::from(recorded["retained"].as_str().unwrap());
    assert_eq!(session.pid(), recorded["pid"].as_u64().unwrap() as u32);
    assert!(
        !running(session.pid()),
        "direct child is reaped before uncertainty is reported"
    );
    assert!(
        weak.upgrade().is_some(),
        "uncertain startup must retain the artifact lease"
    );
    assert!(workspace.is_file(), "cleanup obstruction must remain");
    assert!(
        retained.is_dir(),
        "uncertain startup must retain working state"
    );
    assert_eq!(session.close().await.unwrap_err().kind, ErrorKind::Io);
    assert!(weak.upgrade().is_some());
    assert!(workspace.is_file());
    assert_eq!(
        std::fs::read_to_string(retained.join("state.txt")).unwrap(),
        "preserved"
    );

    std::fs::remove_file(&workspace).unwrap();
    std::fs::rename(&retained, &workspace).unwrap();
    session.close().await.unwrap();
    session.close().await.unwrap();
    assert!(!workspace.exists());
    assert!(!retained.exists());
    assert!(
        weak.upgrade().is_none(),
        "confirmed cleanup releases the artifact lease"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn cancellation_terminates_same_group_grandchildren() {
    let (dir, artifact, runtime) = fixture(
        "import subprocess,sys,time\ndef handle(event):\n    child=subprocess.Popen([sys.executable,'-I','-S','-c','import time; time.sleep(30)'])\n    open(__file__ + '.grandchild.pid','w').write(str(child.pid))\n    time.sleep(30)\n",
    );
    let mut session = ready(runtime.start(artifact, control()).await);
    let error = session
        .execute(
            event("tree").into(),
            RunControl::new(Duration::from_millis(200)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::TimedOut);
    let pid: u32 = std::fs::read_to_string(dir.path().join("program.py.grandchild.pid"))
        .unwrap()
        .parse()
        .unwrap();
    // Grandchildren are reaped by the system, not by this worker. An exited zombie
    // is acceptable briefly; it cannot execute or retain the subprocess capacity.
    for _ in 0..100 {
        let status = std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output()
            .unwrap();
        let status = String::from_utf8_lossy(&status.stdout);
        if status.trim().is_empty() || status.trim().starts_with('Z') {
            session.close().await.unwrap();
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("same-group descendant remains running after cancellation");
}

#[tokio::test]
async fn binary64_results_preserve_exact_python_float_bits() {
    let (_directory, artifact, runtime) = fixture(
        "def handle(event):\n    return [2.291712365432881e-09, -1.527077339613215e-236]\n",
    );
    let mut session = ready(runtime.start(artifact, control()).await);
    let result = output(
        session
            .execute(event("binary64").into(), control())
            .await
            .unwrap(),
    );
    for (actual, expected) in result
        .as_array()
        .unwrap()
        .iter()
        .zip([2.291712365432881e-09_f64, -1.527077339613215e-236_f64])
    {
        assert_eq!(actual.as_f64().unwrap().to_bits(), expected.to_bits());
    }
    assert_eq!(result.as_array().unwrap().len(), 2);
    session.close().await.unwrap();
}
