//! Real CLI/subprocess-boundary tests with an explicitly selected recording
//! Docker executable. These never connect to an engine or pull an image.
#![cfg(unix)]
use serde_json::Value;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Command, Output, Stdio},
    time::{Duration, Instant},
};

struct Fixture {
    _temp: tempfile::TempDir,
    root: PathBuf,
    calls: PathBuf,
    container_ready: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp
            .path()
            .canonicalize()
            .unwrap()
            .join("project with spaces");
        fs::create_dir_all(root.join("src/app/empty")).unwrap();
        fs::create_dir(root.join("tools")).unwrap();
        fs::write(
            root.join("src/app/__init__.py"),
            "def run(event): return event['data']\n",
        )
        .unwrap();
        fs::write(root.join("tools/docker"), DOCKER).unwrap();
        fs::set_permissions(root.join("tools/docker"), fs::Permissions::from_mode(0o755)).unwrap();
        fs::write(
            root.join("ledgence.toml"),
            format!(
                r#"schema_version=1
[program]
id="test"
version="1"
handler="app:run"
[source]
root="src"
include=["app"]
[target]
platform="linux/amd64"
python="3.14"
protocol=3
image="ledgence/ledgence@sha256:{}"
"#,
                "a".repeat(64)
            ),
        )
        .unwrap();
        let calls = root.join("calls.jsonl");
        let container_ready = root.join("container-ready");
        Self {
            _temp: temp,
            root,
            calls,
            container_ready,
        }
    }
    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_ledgence"));
        command
            .args(["program", "build", "--config"])
            .arg(self.root.join("ledgence.toml"))
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    self.root.join("tools").display(),
                    std::env::var("PATH").unwrap_or_default()
                ),
            )
            .env("BUILD_CALLS", &self.calls)
            .env("BUILD_READY", &self.container_ready)
            .env_remove("DOCKER_HOST")
            .env("DOCKER_CONTEXT", "chosen-context")
            .env_remove("DOCKER_DEFAULT_PLATFORM");
        command
    }
    fn records(&self) -> Vec<Value> {
        fs::read_to_string(&self.calls)
            .unwrap_or_default()
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect()
    }
    fn no_output(&self) {
        assert!(!self.root.join(".ledgence/prepared").exists());
        assert!(!self.root.join(".ledgence/prepared.build.json").exists());
        assert!(
            fs::read_dir(self.root.join(".ledgence"))
                .unwrap()
                .all(|entry| !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".ledgence-build-"))
        );
    }
}
fn successful(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
#[test]
fn real_boundary_has_json_output_preserves_empty_dirs_and_respects_selected_context() {
    let fixture = Fixture::new();
    let result = successful(fixture.command().output().unwrap());
    assert_eq!(result["manifest"]["platform"]["arch"], "x86_64");
    assert!(fixture.root.join(".ledgence/prepared/app/empty").is_dir());
    assert!(
        fixture
            .records()
            .iter()
            .all(|v| v[0] == "--context" && v[1] == "chosen-context")
    );
    let again = fixture.command().output().unwrap();
    assert!(!again.status.success());
    assert!(again.stdout.is_empty());
}
#[test]
fn missing_docker_and_bad_runtime_leave_no_output() {
    for mode in ["wrong-runtime", "incomplete", "failure"] {
        let fixture = Fixture::new();
        let output = fixture.command().env("BUILD_MODE", mode).output().unwrap();
        assert!(!output.status.success(), "{mode}");
        assert!(output.stdout.is_empty());
        fixture.no_output();
    }
    let fixture = Fixture::new();
    let output = fixture
        .command()
        .env("PATH", "/does/not/exist")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Docker"));
    fixture.no_output();
}
#[test]
fn failed_creation_does_not_remove_a_container_by_name() {
    let fixture = Fixture::new();
    let result = fixture
        .command()
        .env("BUILD_MODE", "name-collision")
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert!(
        !fixture
            .records()
            .iter()
            .any(|v| v.as_array().unwrap().iter().any(|a| a == "rm"))
    );
    fixture.no_output();
}
#[test]
fn timeout_cleans_owned_container_and_staging() {
    let fixture = Fixture::new();
    let started = Instant::now();
    let output = fixture
        .command()
        .env("BUILD_MODE", "slow")
        .args(["--timeout-seconds", "1"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(started.elapsed() < Duration::from_secs(10));
    fixture.no_output();
    assert_owned_cleanup(&fixture);
}
#[test]
fn interrupt_and_termination_clean_owned_container_and_staging() {
    use nix::{
        sys::signal::{Signal, kill},
        unistd::Pid,
    };
    for signal in [Signal::SIGINT, Signal::SIGTERM] {
        let fixture = Fixture::new();
        let mut child = fixture
            .command()
            .env("BUILD_MODE", "slow")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        // A recorded `run` only proves Docker was invoked. Interrupt after the
        // fixture has created the container identity that cleanup must remove.
        while !fixture.container_ready.is_file() {
            if child.try_wait().unwrap().is_some() || Instant::now() >= deadline {
                let _ = child.kill();
                let output = child.wait_with_output().unwrap();
                panic!(
                    "builder did not create a container: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        kill(Pid::from_raw(child.id() as i32), signal).unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("cancelled"));
        fixture.no_output();
        assert_owned_cleanup(&fixture);
    }
}
fn assert_owned_cleanup(fixture: &Fixture) {
    let calls = fixture.records();
    let run = calls
        .iter()
        .find(|v| v.as_array().unwrap().iter().any(|a| a == "run"))
        .unwrap()
        .as_array()
        .unwrap();
    let index = run.iter().position(|v| v == "--name").unwrap();
    let name = &run[index + 1];
    let remove = calls
        .iter()
        .find(|v| v.as_array().unwrap().iter().any(|a| a == "rm"))
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(
        remove.last().unwrap().as_str(),
        Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    );
    assert!(name.as_str().unwrap().starts_with("ledgence-build-"));
}
const DOCKER: &str = r#"#!/usr/bin/env python3
import json, os, pathlib, shutil, sys, time
args = sys.argv[1:]
with open(os.environ['BUILD_CALLS'], 'a') as stream:
    stream.write(json.dumps(args) + '\n')
if 'inspect' in args:
    print(json.dumps('unix:///recording-local.sock'))
elif 'rm' in args:
    pass
elif 'container' in args and 'ls' in args:
    pass
elif 'run' in args:
    if os.environ.get('BUILD_MODE') == 'name-collision':
        sys.exit(1)
    pathlib.Path(args[args.index('--cidfile') + 1]).write_text('a' * 64)
    pathlib.Path(os.environ['BUILD_READY']).touch()
    mode = os.environ.get('BUILD_MODE', '')
    if mode == 'slow':
        time.sleep(30)
    if mode == 'failure':
        print(json.dumps({'error': 'builder runtime differs from requested OS, architecture or CPython version'}))
        sys.exit(1)
    mounts = {}
    for index, arg in enumerate(args):
        if arg == '--mount':
            fields = dict(part.split('=', 1) for part in args[index + 1].split(',') if '=' in part)
            mounts[fields['target']] = pathlib.Path(fields['source'])
    control = json.loads((mounts['/control'] / 'build.json').read_text())
    manifest = control['manifest']
    runtime = {'os':'linux', 'arch':manifest['platform']['arch'], 'python':manifest['runtime']['python'], 'implementation':'cpython'}
    if mode == 'wrong-runtime':
        runtime['python'] = '3.11'
    (mounts['/output'] / 'runtime.json').write_text(json.dumps(runtime))
    prepared = mounts['/output'] / 'prepared'
    prepared.mkdir()
    if mode != 'incomplete':
        shutil.copytree(mounts['/input'] / 'application', prepared, dirs_exist_ok=True)
    (prepared / 'ledgence-program.json').write_text(json.dumps(manifest))
    print(json.dumps({'prepared':True}))
else:
    sys.exit(2)
"#;
