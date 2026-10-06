//! Exercise the real CLI boundary with a recording Docker executable. No daemon,
//! image pulls, user Docker context changes, or database access occur here.
#![cfg(unix)]

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    net::TcpListener,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    time::{Duration, Instant},
};

struct Fixture {
    _temporary: tempfile::TempDir,
    root: PathBuf,
    executable: PathBuf,
    distribution: PathBuf,
    directory: PathBuf,
    calls: PathBuf,
    port: u16,
}

impl Fixture {
    fn new() -> Self {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let bundle = root.join("relocated bundle");
        fs::create_dir_all(bundle.join("bin")).unwrap();
        fs::hard_link(env!("CARGO_BIN_EXE_ledgence"), bundle.join("bin/ledgence"))
            .or_else(|_| {
                fs::copy(env!("CARGO_BIN_EXE_ledgence"), bundle.join("bin/ledgence")).map(|_| ())
            })
            .unwrap();
        let executable = root.join("launcher");
        symlink(bundle.join("bin/ledgence"), &executable).unwrap();
        let distribution = bundle.join("local");
        write_kit(
            &distribution,
            env!("CARGO_PKG_VERSION"),
            "a",
            &["linux/amd64", "linux/arm64"],
        );
        fs::create_dir(root.join("tools")).unwrap();
        let docker = root.join("tools/docker");
        fs::write(&docker, DOCKER).unwrap();
        fs::set_permissions(&docker, fs::Permissions::from_mode(0o755)).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        Self {
            directory: root.join("state with spaces"),
            calls: root.join("docker.jsonl"),
            _temporary: temporary,
            root,
            executable,
            distribution,
            port,
        }
    }

    fn command(&self, arguments: &[&str]) -> Command {
        let mut command = Command::new(&self.executable);
        command
            .arg("local")
            .args(arguments)
            .arg("--directory")
            .arg(&self.directory)
            .env(
                "PATH",
                format!(
                    "{}:{}",
                    self.root.join("tools").display(),
                    std::env::var("PATH").unwrap()
                ),
            )
            .env("DOCKER_RECORD", &self.calls)
            .env_remove("DOCKER_HOST")
            .env_remove("DOCKER_CONTEXT")
            .env("FAKE_ARCH", "aarch64")
            .env("LEDGENCE_PORT", "1")
            .env("LEDGENCE_CONCURRENCY", "99")
            .env("COMPOSE_FILE", "/do-not-load-this.yml")
            .env("COMPOSE_PROFILES", "tools")
            .env("DOCKER_DEFAULT_PLATFORM", "linux/unsupported");
        command
    }

    fn up(&self) -> Output {
        self.command(&["up", "--port", &self.port.to_string(), "--concurrency", "2"])
            .output()
            .unwrap()
    }

    fn records(&self) -> Vec<Value> {
        fs::read_to_string(&self.calls)
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn state(&self) -> Value {
        serde_json::from_slice(&fs::read(self.directory.join(".ledgence-state.json")).unwrap())
            .unwrap()
    }
}

fn write_kit(path: &Path, version: &str, digest: &str, platforms: &[&str]) {
    fs::create_dir_all(path).unwrap();
    let image = format!("ghcr.io/ledgence/ledgence@sha256:{}", digest.repeat(64));
    fs::write(path.join("distribution.json"), serde_json::to_vec(&json!({
        "format": 1, "version": version, "image": image, "python": "3.14", "platforms": platforms,
    })).unwrap()).unwrap();
    fs::write(
        path.join("compose.yaml"),
        format!("services:\n  worker:\n    image: {image}\n"),
    )
    .unwrap();
    fs::write(path.join("README.md"), "Distribution guide\n").unwrap();
    let mut sums = String::new();
    for name in ["distribution.json", "compose.yaml", "README.md"] {
        sums.push_str(&format!(
            "{:x}  {name}\n",
            Sha256::digest(fs::read(path.join(name)).unwrap())
        ));
    }
    fs::write(path.join("SHA256SUMS"), sums).unwrap();
}

fn success(output: &Output) {
    assert!(
        output.status.success(),
        "{output:?}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn failure(output: &Output, code: i32, message: &str) {
    assert_eq!(output.status.code(), Some(code), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(message),
        "{output:?}"
    );
}

fn compose_record(record: &Value, action: &str) -> bool {
    let args = record["args"].as_array().unwrap();
    args.iter().any(|arg| arg == "compose") && args.iter().any(|arg| arg == action)
}

#[test]
fn relocated_bundle_initializes_once_and_preserves_configuration_and_volumes() {
    let fixture = Fixture::new();
    let output = fixture.up();
    success(&output);
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains(&format!("http://127.0.0.1:{}/console/", fixture.port)));
    assert!(text.contains("tenant=acme namespace=demo queue=demo"));
    let state = fixture.state();
    let bytes = fs::read(fixture.directory.join(".ledgence-state.json")).unwrap();
    assert_eq!(state["port"], fixture.port);
    assert_eq!(state["concurrency"], 2);
    assert_eq!(state["context"], "desktop-linux");
    fs::write(fixture.directory.join("operator-notes.txt"), "keep me").unwrap();
    let original_compose = fs::read(fixture.directory.join("compose.yaml")).unwrap();
    // A replacement CLI bundle must not update the saved installation.
    write_kit(&fixture.distribution, "999.0.0", "b", &["linux/arm64"]);
    success(&fixture.command(&["up"]).output().unwrap());
    success(&fixture.command(&["status"]).output().unwrap());
    success(
        &fixture
            .command(&["logs", "--tail", "37", "--service", "worker"])
            .output()
            .unwrap(),
    );
    success(&fixture.command(&["down"]).output().unwrap());
    assert_eq!(
        fs::read(fixture.directory.join(".ledgence-state.json")).unwrap(),
        bytes
    );
    assert_eq!(
        fs::read(fixture.directory.join("compose.yaml")).unwrap(),
        original_compose
    );
    assert_eq!(
        fs::read_to_string(fixture.directory.join("operator-notes.txt")).unwrap(),
        "keep me"
    );
    for record in fixture.records().iter().filter(|record| {
        ["up", "ps", "logs", "down"]
            .iter()
            .any(|action| compose_record(record, action))
    }) {
        let arguments = record["args"].as_array().unwrap();
        assert!(arguments.contains(&state["project"]));
        assert!(arguments.contains(&json!(fixture.directory.join("compose.yaml"))));
        assert!(
            !arguments
                .iter()
                .any(|arg| arg == "--volumes" || arg == "--remove-orphans")
        );
        assert_eq!(record["port"], fixture.port.to_string());
        assert_eq!(record["concurrency"], "2");
        assert_eq!(record["compose_file"], Value::Null);
        assert_eq!(record["profiles"], Value::Null);
        assert_eq!(record["default_platform"], Value::Null);
        if compose_record(record, "up") {
            assert!(arguments.contains(&json!("--wait")));
            assert!(arguments.contains(&json!("--no-build")));
        }
    }
}

#[test]
fn failed_start_retains_identity_and_the_same_command_reconciles_it() {
    let fixture = Fixture::new();
    let failed = fixture
        .command(&["up", "--port", &fixture.port.to_string()])
        .env("FAKE_FAIL_UP", "1")
        .output()
        .unwrap();
    failure(&failed, 1, "configuration and volumes were preserved");
    let bytes = fs::read(fixture.directory.join(".ledgence-state.json")).unwrap();
    success(&fixture.command(&["up"]).output().unwrap());
    assert_eq!(
        fs::read(fixture.directory.join(".ledgence-state.json")).unwrap(),
        bytes
    );
    let projects: Vec<_> = fixture
        .records()
        .into_iter()
        .filter(|record| compose_record(record, "up"))
        .map(|record| {
            let args = record["args"].as_array().unwrap();
            args[args.iter().position(|arg| arg == "--project-name").unwrap() + 1].clone()
        })
        .collect();
    assert_eq!(projects.len(), 2);
    assert_eq!(projects[0], projects[1]);
}

#[test]
fn changed_installation_options_and_distribution_never_mutate_saved_state() {
    let fixture = Fixture::new();
    success(&fixture.up());
    let bytes = fs::read(fixture.directory.join(".ledgence-state.json")).unwrap();
    for args in [
        vec!["up", "--concurrency", "3"],
        vec!["up", "--context", "other"],
    ] {
        failure(
            &fixture.command(&args).output().unwrap(),
            2,
            "options differ from saved state",
        );
    }
    write_kit(
        &fixture.distribution,
        env!("CARGO_PKG_VERSION"),
        "b",
        &["linux/arm64"],
    );
    failure(
        &fixture
            .command(&[
                "up",
                "--distribution",
                fixture.distribution.to_str().unwrap(),
            ])
            .output()
            .unwrap(),
        2,
        "automatic upgrades are not supported",
    );
    assert_eq!(
        fs::read(fixture.directory.join(".ledgence-state.json")).unwrap(),
        bytes
    );
    assert_eq!(
        fixture
            .records()
            .iter()
            .filter(|record| compose_record(record, "up"))
            .count(),
        1
    );
}

#[test]
fn invalid_or_partial_state_is_preserved_and_never_reinitialized() {
    let fixture = Fixture::new();
    fs::create_dir(&fixture.directory).unwrap();
    fs::write(fixture.directory.join("important.txt"), "existing data").unwrap();
    failure(&fixture.up(), 2, "not empty");
    assert!(fixture.records().is_empty());
    fs::write(fixture.directory.join(".ledgence-state.json"), "{").unwrap();
    failure(&fixture.up(), 2, "invalid local state");
    assert_eq!(
        fs::read_to_string(fixture.directory.join("important.txt")).unwrap(),
        "existing data"
    );
    assert_eq!(
        fs::read_to_string(fixture.directory.join(".ledgence-state.json")).unwrap(),
        "{"
    );
}

#[test]
fn kit_corruption_rejects_start_before_docker_or_state_creation() {
    for damage in ["hash", "path", "symlink", "unpinned", "version"] {
        let fixture = Fixture::new();
        match damage {
            "hash" => fs::write(fixture.distribution.join("compose.yaml"), "modified").unwrap(),
            "path" => fs::write(
                fixture.distribution.join("SHA256SUMS"),
                format!("{}  ../outside\n", "a".repeat(64)),
            )
            .unwrap(),
            "symlink" => {
                fs::remove_file(fixture.distribution.join("README.md")).unwrap();
                symlink(
                    fixture.distribution.join("compose.yaml"),
                    fixture.distribution.join("README.md"),
                )
                .unwrap();
            }
            "unpinned" => write_kit(
                &fixture.distribution,
                env!("CARGO_PKG_VERSION"),
                "z",
                &["linux/arm64"],
            ),
            "version" => write_kit(&fixture.distribution, "999.0.0", "a", &["linux/arm64"]),
            _ => unreachable!(),
        }
        let output = fixture.up();
        assert_eq!(output.status.code(), Some(2), "{damage}: {output:?}");
        assert!(fixture.records().is_empty());
        assert!(!fixture.directory.exists());
    }
}

#[test]
fn docker_version_daemon_platform_and_remote_endpoint_fail_before_state_creation() {
    for (key, value, message, code) in [
        ("FAKE_COMPOSE", "2.23.0", "2.23.1 or newer", 2),
        ("FAKE_ENDPOINT", "ssh://remote", "local Docker endpoint", 2),
        ("FAKE_ARCH", "riscv64", "amd64 or arm64", 2),
        ("FAKE_OS", "windows", "Linux containers", 2),
        ("FAKE_INFO_FAIL", "1", "start Docker Engine", 1),
        ("DOCKER_HOST", "tcp://remote:2375", "DOCKER_HOST is set", 2),
    ] {
        let fixture = Fixture::new();
        failure(
            &fixture.command(&["up"]).env(key, value).output().unwrap(),
            code,
            message,
        );
        assert!(!fixture.directory.exists());
        assert!(
            !fixture
                .records()
                .iter()
                .any(|record| compose_record(record, "up"))
        );
    }
    let fixture = Fixture::new();
    write_kit(
        &fixture.distribution,
        env!("CARGO_PKG_VERSION"),
        "a",
        &["linux/amd64"],
    );
    failure(&fixture.up(), 2, "does not support Docker engine platform");
    assert!(!fixture.directory.exists());
}

#[test]
fn port_conflict_is_reported_but_a_saved_project_can_reconcile_its_own_running_port() {
    let fixture = Fixture::new();
    success(&fixture.up());
    let _listener = TcpListener::bind(("127.0.0.1", fixture.port)).unwrap();
    let port = fixture.port;
    failure(
        &fixture
            .command(&["up", "--port", &port.to_string()])
            .output()
            .unwrap(),
        1,
        "is unavailable",
    );
    assert_eq!(
        fixture
            .records()
            .iter()
            .filter(|record| compose_record(record, "up"))
            .count(),
        1
    );
    let rows = json!([{"Service": "orchestrator", "State": "running", "Publishers": [
        {"URL": "127.0.0.1", "TargetPort": 8080, "PublishedPort": port}
    ]}]);
    success(
        &fixture
            .command(&["up"])
            .env("FAKE_STATUS", rows.to_string())
            .output()
            .unwrap(),
    );
}

#[test]
fn initial_port_conflict_allows_another_port_in_the_same_directory() {
    let fixture = Fixture::new();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let occupied = listener.local_addr().unwrap().port();
    failure(
        &fixture
            .command(&["up", "--port", &occupied.to_string()])
            .output()
            .unwrap(),
        1,
        "No installation was created",
    );
    assert!(!fixture.directory.exists());
    assert!(
        !fixture
            .records()
            .iter()
            .any(|record| compose_record(record, "up"))
    );
    success(&fixture.up());
    assert_eq!(fixture.state()["port"], fixture.port);
}

#[test]
fn existing_installation_is_independent_of_removed_bundle_and_checks_saved_context() {
    let fixture = Fixture::new();
    success(&fixture.up());
    fs::remove_dir_all(&fixture.distribution).unwrap();
    success(&fixture.command(&["status"]).output().unwrap());
    failure(
        &fixture
            .command(&["down"])
            .env("FAKE_ENDPOINT", "unix:///different.sock")
            .output()
            .unwrap(),
        2,
        "different endpoint",
    );
    assert!(
        !fixture
            .records()
            .iter()
            .any(|record| compose_record(record, "down"))
    );
}

#[test]
fn concurrent_mutation_is_rejected_and_existing_lock_file_is_reusable() {
    let fixture = Fixture::new();
    success(&fixture.up());
    let name = format!(
        ".ledgence-local-{:x}.lock",
        Sha256::digest(fixture.directory.to_string_lossy().as_bytes())
    );
    let name = format!("{}.lock", &name[..".ledgence-local-".len() + 24]);
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(fixture.root.join(name))
        .unwrap();
    lock.lock().unwrap();
    let before = fixture.records().len();
    failure(
        &fixture.command(&["down"]).output().unwrap(),
        1,
        "another local command",
    );
    assert_eq!(fixture.records().len(), before);
    drop(lock);
    success(&fixture.command(&["down"]).output().unwrap());
}

#[test]
fn following_logs_does_not_hold_the_mutation_lock() {
    let fixture = Fixture::new();
    success(&fixture.up());
    let started = fixture.root.join("logs-started");
    let release = fixture.root.join("release-logs");
    let mut logs = fixture
        .command(&["logs", "--follow"])
        .env("FAKE_LOG_STARTED", &started)
        .env("FAKE_LOG_RELEASE", &release)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !started.exists() {
        if Instant::now() > deadline {
            let _ = logs.kill();
            let _ = logs.wait();
            panic!("logs command did not start");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let down = fixture.command(&["down"]).output().unwrap();
    fs::write(release, "done").unwrap();
    let status = logs.wait().unwrap();
    success(&down);
    assert!(status.success());
}

#[test]
fn explicit_distribution_works_without_bundle_and_status_does_not_initialize() {
    let mut fixture = Fixture::new();
    fixture.executable = PathBuf::from(env!("CARGO_BIN_EXE_ledgence"));
    failure(
        &fixture.command(&["status"]).output().unwrap(),
        2,
        "no local installation found",
    );
    assert!(!fixture.directory.exists());
    let output = fixture
        .command(&[
            "up",
            "--distribution",
            fixture.distribution.to_str().unwrap(),
            "--port",
            &fixture.port.to_string(),
        ])
        .env("FAKE_CONTEXT", "chosen-context")
        .output()
        .unwrap();
    success(&output);
    assert_eq!(fixture.state()["context"], "chosen-context");
}

const DOCKER: &str = r#"#!/usr/bin/env python3
import json, os, sys, time
args = sys.argv[1:]
with open(os.environ['DOCKER_RECORD'], 'a') as stream:
    stream.write(json.dumps({'args': args, 'port': os.getenv('LEDGENCE_PORT'),
        'concurrency': os.getenv('LEDGENCE_CONCURRENCY'), 'compose_file': os.getenv('COMPOSE_FILE'),
        'profiles': os.getenv('COMPOSE_PROFILES'), 'default_platform': os.getenv('DOCKER_DEFAULT_PLATFORM')}) + '\n')
if args[:1] == ['--context']:
    args = args[2:]
if args == ['context', 'show']:
    print(os.getenv('FAKE_CONTEXT', 'desktop-linux'))
elif args[:2] == ['context', 'inspect']:
    print(json.dumps(os.getenv('FAKE_ENDPOINT', 'unix:///local/docker.sock')))
elif args[:3] == ['compose', 'version', '--short']:
    print(os.getenv('FAKE_COMPOSE', '2.23.1'))
elif args[:1] == ['info']:
    if os.getenv('FAKE_INFO_FAIL'):
        print('daemon unavailable', file=sys.stderr)
        sys.exit(1)
    print(json.dumps({'OSType': os.getenv('FAKE_OS', 'linux'), 'Architecture': os.getenv('FAKE_ARCH', 'arm64')}))
elif args[:1] == ['compose'] and 'ps' in args:
    print(os.getenv('FAKE_STATUS', '[]') if 'json' in args else 'NAME SERVICE STATE')
elif args[:1] == ['compose'] and 'up' in args:
    if os.getenv('FAKE_FAIL_UP'):
        print('orchestrator startup failed after PostgreSQL became healthy', file=sys.stderr)
        sys.exit(17)
elif args[:1] == ['compose'] and ('down' in args or 'logs' in args):
    if 'logs' in args and os.getenv('FAKE_LOG_STARTED'):
        open(os.environ['FAKE_LOG_STARTED'], 'w').close()
        while not os.path.exists(os.environ['FAKE_LOG_RELEASE']):
            time.sleep(0.01)
    print('completed')
else:
    print('unexpected invocation: ' + repr(args), file=sys.stderr)
    sys.exit(91)
"#;
