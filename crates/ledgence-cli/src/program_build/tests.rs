use super::*;
use serde_json::json;
use std::{ffi::OsString, sync::Mutex, time::Duration};

struct FakeDocker {
    calls: Mutex<Vec<Vec<OsString>>>,
    endpoint: &'static str,
    outcome: &'static str,
}
impl FakeDocker {
    fn success() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            endpoint: "unix:///test/docker.sock",
            outcome: "success",
        }
    }
}
impl Process for FakeDocker {
    async fn execute(&self, args: &[OsString], _: Duration) -> Result<Vec<u8>> {
        self.calls.lock().unwrap().push(args.to_vec());
        let strings: Vec<_> = args.iter().map(|s| s.to_string_lossy()).collect();
        if strings.iter().any(|s| s == "show") {
            return Ok(b"test-local\n".to_vec());
        }
        if strings.iter().any(|s| s == "inspect") {
            return Ok(serde_json::to_vec(self.endpoint).unwrap());
        }
        if strings.iter().any(|s| s == "rm") {
            return Ok(Vec::new());
        }
        if let Some(index) = strings.iter().position(|v| v == "--cidfile") {
            fs::write(strings[index + 1].as_ref(), "a".repeat(64)).unwrap();
        }
        if self.outcome == "failed" {
            return Err(operational("fake Docker failure"));
        }
        let mut mounts = BTreeMap::new();
        for pair in strings.windows(2).filter(|s| s[0] == "--mount") {
            let mut parts = pair[1].split(',');
            let source = parts.find_map(|p| p.strip_prefix("source=")).unwrap();
            let target = pair[1]
                .split(',')
                .find_map(|p| p.strip_prefix("target="))
                .unwrap();
            mounts.insert(target.to_owned(), PathBuf::from(source));
        }
        let plan: serde_json::Value =
            serde_json::from_slice(&fs::read(mounts["/control"].join("build.json")).unwrap())
                .unwrap();
        let manifest = &plan["manifest"];
        let mut runtime = json!({"os": "linux", "arch": manifest["platform"]["arch"], "python": manifest["runtime"]["python"], "implementation":"cpython"});
        if self.outcome == "wrong-runtime" {
            runtime["arch"] = json!("wrong");
        }
        fs::write(
            mounts["/output"].join("runtime.json"),
            serde_json::to_vec(&runtime).unwrap(),
        )
        .unwrap();
        let prepared = mounts["/output"].join("prepared");
        fs::create_dir(&prepared).unwrap();
        fs::write(
            prepared.join("ledgence-program.json"),
            serde_json::to_vec(manifest).unwrap(),
        )
        .unwrap();
        if self.outcome != "incomplete" {
            copy_tree(&mounts["/input"].join("application"), &prepared);
        }
        Ok(b"{\"prepared\":true}\n".to_vec())
    }
}
fn copy_tree(source: &Path, destination: &Path) {
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let to = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            fs::create_dir_all(&to).unwrap();
            copy_tree(&entry.path(), &to);
        } else {
            fs::copy(entry.path(), to).unwrap();
        }
    }
}
pub(super) struct Fixture {
    _temp: tempfile::TempDir,
    pub(super) root: PathBuf,
}
impl Fixture {
    pub(super) fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("project with spaces");
        fs::create_dir_all(root.join("src/application")).unwrap();
        fs::write(
            root.join("src/application/__init__.py"),
            "def run(event): return event['data']\n",
        )
        .unwrap();
        fs::write(root.join("LICENSE"), "MIT\n").unwrap();
        fs::write(
            root.join("ledgence.toml"),
            format!(
                r#"schema_version = 1
[program]
id = "test-program"
version = "1.0.0"
handler = "application:run"
kind = "workflow"
[source]
root = "src"
include = ["application"]
[[files]]
source = "LICENSE"
destination = "LICENSE"
[target]
platform = "linux/arm64"
python = "3.14"
protocol = 3
image = "ledgence/ledgence@sha256:{}"
"#,
                "a".repeat(64)
            ),
        )
        .unwrap();
        Self { _temp: temp, root }
    }
    fn options(&self, output: &str) -> Options {
        Options::parse(vec![
            "--config".into(),
            self.root.join("ledgence.toml").to_str().unwrap().into(),
            "--output".into(),
            output.into(),
            "--context".into(),
            "test-local".into(),
        ])
        .unwrap()
    }
    fn change(&self, from: &str, to: &str) {
        let path = self.root.join("ledgence.toml");
        fs::write(&path, fs::read_to_string(&path).unwrap().replace(from, to)).unwrap();
    }
}
#[test]
fn parser_is_strict_and_configuration_is_versioned() {
    for args in [
        vec!["--unknown", "x"],
        vec!["--output"],
        vec!["--config", "a", "--config", "b"],
        vec!["--timeout-seconds", "0"],
    ] {
        assert!(Options::parse(args.into_iter().map(str::to_owned).collect()).is_err());
    }
    for (from, to) in [
        ("schema_version = 1", "schema_version = 2"),
        ("linux/arm64", "macos/arm64"),
        ("python = \"3.14\"", "python = \"3\""),
        ("@sha256:", ":latest#"),
        ("include = [\"application\"]", "include = []"),
        ("kind = \"workflow\"", "kind = \"flow\""),
    ] {
        let fixture = Fixture::new();
        fixture.change(from, to);
        assert!(
            Config::read(&fixture.root.join("ledgence.toml")).is_err(),
            "{to}"
        );
    }
}
#[tokio::test]
async fn explicit_linux_target_outputs_verified_package_and_external_receipt() {
    let fixture = Fixture::new();
    let docker = FakeDocker::success();
    let first = execute(fixture.options(".ledgence/first"), &docker)
        .await
        .unwrap();
    let second = execute(fixture.options(".ledgence/second"), &docker)
        .await
        .unwrap();
    assert_eq!(first.descriptor, second.descriptor);
    assert_eq!(first.manifest.platform.arch, "aarch64");
    assert!(!first.prepared_directory.join("first.build.json").exists());
    let receipt = load_receipt(&first.prepared_directory, &first.descriptor).unwrap();
    assert_eq!(
        receipt.metadata.kind,
        ledgence_orchestration_api::console::ConsoleProgramKind::Workflow
    );
    assert!(
        receipt
            .inputs
            .contains_key("source/application/__init__.py")
    );
    let calls = docker.calls.lock().unwrap();
    let run = calls
        .iter()
        .find(|args| args.iter().any(|a| a == "run"))
        .unwrap();
    assert!(
        run.windows(2)
            .any(|s| s[0] == "--platform" && s[1] == "linux/arm64")
    );
    assert!(
        run.iter()
            .any(|s| s.to_string_lossy().contains("project with spaces"))
    );
    assert!(!run.iter().any(|s| s == "--privileged"));
    assert!(
        run.iter()
            .filter(|s| s.to_string_lossy().contains("readonly"))
            .count()
            >= 2
    );
}
#[tokio::test]
async fn output_and_receipt_are_never_overwritten() {
    let fixture = Fixture::new();
    let docker = FakeDocker::success();
    let first = execute(fixture.options(".ledgence/prepared"), &docker)
        .await
        .unwrap();
    let before = fs::read(&first.receipt).unwrap();
    assert!(
        execute(fixture.options(".ledgence/prepared"), &docker)
            .await
            .is_err()
    );
    assert_eq!(fs::read(first.receipt).unwrap(), before);
    let empty = fixture.root.join("existing");
    fs::create_dir(&empty).unwrap();
    assert!(execute(fixture.options("existing"), &docker).await.is_err());
    assert_eq!(fs::read_dir(empty).unwrap().count(), 0);
}
#[tokio::test]
async fn failed_mismatched_or_incomplete_builds_never_publish_output() {
    for outcome in ["failed", "wrong-runtime", "incomplete"] {
        let fixture = Fixture::new();
        let docker = FakeDocker {
            outcome,
            ..FakeDocker::success()
        };
        assert!(
            execute(fixture.options(".ledgence/prepared"), &docker)
                .await
                .is_err(),
            "{outcome}"
        );
        assert!(!fixture.root.join(".ledgence/prepared").exists());
        assert!(!fixture.root.join(".ledgence/prepared.build.json").exists());
        assert!(
            fs::read_dir(fixture.root.join(".ledgence"))
                .unwrap()
                .all(|e| !e
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".ledgence-build-"))
        );
        if outcome == "failed" {
            assert!(
                docker
                    .calls
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|v| v.iter().any(|a| a == "rm"))
            );
        }
    }
}
#[tokio::test]
async fn remote_engine_and_unselected_host_are_rejected_before_run() {
    let docker = FakeDocker {
        endpoint: "ssh://remote",
        ..FakeDocker::success()
    };
    assert!(
        Docker::connect(&docker, Some("remote"), false)
            .await
            .is_err()
    );
    assert!(Docker::connect(&docker, None, true).await.is_err());
    assert!(
        !docker
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|v| v.iter().any(|a| a == "run"))
    );
}
#[tokio::test]
async fn secrets_caches_and_unselected_files_are_not_staged() {
    let fixture = Fixture::new();
    fs::write(fixture.root.join("src/application/.env"), "DO_NOT_READ").unwrap();
    fs::create_dir(fixture.root.join("src/application/.git")).unwrap();
    fs::write(
        fixture.root.join("src/application/.git/credentials"),
        "DO_NOT_READ",
    )
    .unwrap();
    fs::write(fixture.root.join("src/unselected.py"), "DO_NOT_READ").unwrap();
    let result = execute(
        fixture.options(".ledgence/prepared"),
        &FakeDocker::success(),
    )
    .await
    .unwrap();
    let paths = files::prepared_hashes(&result.prepared_directory).unwrap();
    assert_eq!(paths.len(), 3);
    assert!(
        !paths
            .keys()
            .any(|p| p.contains("env") || p.contains("unselected"))
    );
}
#[tokio::test]
async fn collisions_manifest_disagreement_and_input_links_fail_before_docker() {
    let fixture = Fixture::new();
    fixture.change(
        "destination = \"LICENSE\"",
        "destination = \"application/__init__.py\"",
    );
    let docker = FakeDocker::success();
    assert!(
        execute(fixture.options(".ledgence/prepared"), &docker)
            .await
            .is_err()
    );
    assert!(docker.calls.lock().unwrap().is_empty());
    let fixture = Fixture::new();
    fs::write(fixture.root.join("ledgence-program.json"), b"{}").unwrap();
    assert!(
        execute(fixture.options(".ledgence/prepared"), &docker)
            .await
            .is_err()
    );
    #[cfg(unix)]
    {
        let fixture = Fixture::new();
        std::os::unix::fs::symlink("/etc/passwd", fixture.root.join("src/application/link"))
            .unwrap();
        assert!(
            execute(fixture.options(".ledgence/prepared"), &docker)
                .await
                .is_err()
        );
    }
}
#[tokio::test]
async fn modified_output_invalidates_receipt() {
    let fixture = Fixture::new();
    let result = execute(
        fixture.options(".ledgence/prepared"),
        &FakeDocker::success(),
    )
    .await
    .unwrap();
    fs::write(result.prepared_directory.join("LICENSE"), "changed").unwrap();
    assert!(load_receipt(&result.prepared_directory, &result.descriptor).is_err());
}
#[test]
fn no_replace_rename_preserves_even_empty_racing_target() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let target = root.path().join("target");
    fs::create_dir(&source).unwrap();
    fs::create_dir(&target).unwrap();
    fs::write(source.join("keep"), "value").unwrap();
    assert!(rename_new(&source, &target).is_err());
    assert!(source.join("keep").exists());
    assert!(!target.join("keep").exists());
}

#[test]
fn receipt_bound_round_trips_supported_many_long_paths_and_rejects_oversize() {
    let fixture = Fixture::new();
    let (config, _) = Config::read(&fixture.root.join("ledgence.toml")).unwrap();
    let manifest = config.manifest().unwrap();
    let prefix = format!("{0}/{0}/{0}/{0}", "a".repeat(200));
    let prepared_files: BTreeMap<_, _> = (0..4091)
        .map(|i| {
            (
                format!("{prefix}/{i:04}-{}", "b".repeat(195)),
                format!("sha256:{}", "a".repeat(64)),
            )
        })
        .collect();
    assert!(prepared_files.keys().all(|path| path.len() <= 1024));
    let inputs = prepared_files
        .iter()
        .map(|(path, digest)| (format!("source/{path}"), digest.clone()))
        .collect();
    let mut receipt = BuildReceipt {
        schema_version: 1,
        prepared_directory: fixture.root.join("prepared"),
        metadata: config.metadata(),
        descriptor: ProgramDescriptor {
            program: manifest.program.clone(),
            digest: ledgence_worker_api::Digest(format!("sha256:{}", "a".repeat(64))),
            size: 1024,
        },
        manifest,
        target_image: config.target.image,
        target_platform: config.target.platform,
        inputs,
        prepared_files,
    };
    let bytes = encode_receipt(&receipt).unwrap();
    assert!(
        bytes.len() > 2 * 1024 * 1024,
        "exercise the former unreadable-receipt boundary"
    );
    let path = fixture.root.join("large.build.json");
    fs::write(&path, &bytes).unwrap();
    let decoded: BuildReceipt =
        serde_json::from_slice(&files::read_regular(&path, MAX_BUILD_RECEIPT_BYTES).unwrap())
            .unwrap();
    assert_eq!(decoded.prepared_files, receipt.prepared_files);
    assert_eq!(decoded.inputs, receipt.inputs);
    receipt.target_image = "x".repeat(MAX_BUILD_RECEIPT_BYTES as usize);
    assert!(encode_receipt(&receipt).is_err());
}

#[tokio::test]
async fn cancelled_or_expired_finalization_is_owned_until_finished_without_publication() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    for cancelled in [false, true] {
        let fixture = Fixture::new();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let finished = Arc::new(AtomicBool::new(false));
        let observed = finished.clone();
        let worker = tokio::task::spawn_blocking(move || {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            observed.store(true, Ordering::SeqCst);
            Ok(())
        });
        entered_rx.await.unwrap();
        let deadline = if cancelled {
            tokio::time::Instant::now() + Duration::from_secs(60)
        } else {
            tokio::time::Instant::now() - Duration::from_secs(1)
        };
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(await_finalization(worker, deadline, async move {
            let _ = cancel_rx.await;
        }));
        if cancelled {
            cancel_tx.send(()).unwrap();
        }
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "do not abandon the accepted blocking finalizer"
        );
        assert!(!finished.load(Ordering::SeqCst));
        release_tx.send(()).unwrap();
        let error = waiter.await.unwrap().unwrap_err();
        assert!(
            error
                .to_string()
                .contains(if cancelled { "cancelled" } else { "timed out" })
        );
        assert!(finished.load(Ordering::SeqCst));
        assert!(!fixture.root.join("prepared").exists());
        assert!(!fixture.root.join("prepared.build.json").exists());
    }
}
