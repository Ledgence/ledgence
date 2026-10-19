//! Test-only prepared packages for worker lifecycle and delivery regressions.
//!
//! These fixtures deliberately exercise protocol 2 on the selected host CPython.
//! Source scaffolding and Docker-backed builds have their own command tests.

use ledgence_worker_api::{CloudEvent, Platform, ProgramManifest, ProgramRef, PythonRuntime};
use serde_json::json;
use std::{path::Path, time::Duration};

pub async fn prepare_worker_fixture(directory: &Path, python: &str) {
    assert!(!directory.exists(), "fixture directory must be fresh");
    let mut command = tokio::process::Command::new(python);
    command.args(["-I", "-S", "-c", "import json,sys;print(json.dumps({'version':'%d.%d'%sys.version_info[:2],'implementation':sys.implementation.name}))"]).kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(5), command.output())
        .await
        .expect("fixture Python version probe must complete")
        .expect("fixture Python must be available");
    assert!(
        output.status.success(),
        "Python probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let info: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        info["implementation"], "cpython",
        "worker tests require CPython"
    );
    let manifest = ProgramManifest {
        schema_version: 1,
        program: ProgramRef {
            id: "hello".into(),
            version: "1.0.0".into(),
        },
        runtime: PythonRuntime {
            kind: "python".into(),
            python: info["version"].as_str().unwrap().into(),
            protocol: 2,
        },
        handler: "program:handle".into(),
        platform: Platform {
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
        },
    };
    manifest.validate_host().unwrap();
    let program = directory.join("program");
    std::fs::create_dir_all(&program).unwrap();
    std::fs::write(
        program.join("ledgence-program.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    std::fs::write(
        program.join("program.py"),
        include_str!("../../../../examples/python/program.py"),
    )
    .unwrap();
    let tasks: Vec<_> = (1..=2)
        .map(|n| {
            let event = CloudEvent::new(json!({
                "specversion": "1.0",
                "id": format!("evt_fixture_{n}"),
                "source": "urn:ledgence:test",
                "type": "com.ledgence.task.invocation.requested.v1",
                "subject": format!("tasks/task_fixture_{n}"),
                "time": "2026-09-12T14:00:00Z",
                "datacontenttype": "application/json",
                "traceparent": format!("00-0af7651916cd43dd8448eb211c80319c-{n:016x}-01"),
                "ldgtenantid": "tenant_fixture",
                "ldgnamespace": "demo",
                "ldgrunid": "run_fixture",
                "ldgtaskid": format!("task_fixture_{n}"),
                "ldgattemptid": format!("att_fixture_{n}_1"),
                "ldgattemptno": 1,
                "data": {"invoice_id": format!("INV-{n}"), "amount": 42}
            }))
            .unwrap();
            json!({"program": manifest.program, "event": event})
        })
        .collect();
    std::fs::write(
        directory.join("tasks.json"),
        serde_json::to_vec_pretty(&tasks).unwrap(),
    )
    .unwrap();
}
