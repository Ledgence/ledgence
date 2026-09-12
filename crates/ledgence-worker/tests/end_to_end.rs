use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::Output,
    time::Duration,
};
use tempfile::TempDir;

fn python() -> String {
    std::env::var("LEDGENCE_PYTHON").unwrap_or_else(|_| "python3".into())
}

async fn invoke(args: &[&str]) -> Output {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_ledgence-worker"));
    command
        .args(args)
        .env("RUST_LOG", "warn")
        .kill_on_drop(true);
    tokio::time::timeout(Duration::from_secs(30), command.output())
        .await
        .expect("CLI command must complete within its deadline")
        .unwrap()
}

fn success(output: &Output) {
    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn write_json(path: impl AsRef<Path>, value: &Value) {
    std::fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

struct Fixture {
    _temp: TempDir,
    example: PathBuf,
    store: PathBuf,
    cache: PathBuf,
    runner: PathBuf,
}
impl Fixture {
    async fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let example = temp.path().join("example");
        success(
            &invoke(&[
                "example",
                "--directory",
                example.to_str().unwrap(),
                "--python",
                &python(),
            ])
            .await,
        );
        let store = temp.path().join("store");
        let cache = temp.path().join("cache");
        let runner = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../sdk/python/ledgence_worker/bootstrap.py");
        Self {
            _temp: temp,
            example,
            store,
            cache,
            runner,
        }
    }
    async fn publish(&self) -> Output {
        invoke(&[
            "publish",
            "--source",
            self.example.join("program").to_str().unwrap(),
            "--store",
            self.store.to_str().unwrap(),
        ])
        .await
    }
    async fn run(&self) -> Output {
        invoke(&[
            "run",
            "--tasks",
            self.example.join("tasks.json").to_str().unwrap(),
            "--store",
            self.store.to_str().unwrap(),
            "--cache",
            self.cache.to_str().unwrap(),
            "--python",
            &python(),
            "--runner",
            self.runner.to_str().unwrap(),
            "--concurrency",
            "1",
        ])
        .await
    }
}

#[tokio::test]
async fn published_dependencies_are_downloaded_cached_and_reused_with_immutable_versions() {
    let fixture = Fixture::new().await;
    let program = fixture.example.join("program");
    std::fs::create_dir(program.join("bundled_dependency")).unwrap();
    std::fs::write(
        program.join("bundled_dependency/__init__.py"),
        "VERSION = 'first'\n",
    )
    .unwrap();
    std::fs::write(program.join("program.py"), "import os\nfrom bundled_dependency import VERSION\ndef handle(event):\n    return {'event': event, 'version': VERSION, 'pid': os.getpid()}\n").unwrap();
    let first = fixture.publish().await;
    success(&first);
    let first_descriptor: Value = serde_json::from_slice(&first.stdout).unwrap();
    let repeated = fixture.publish().await;
    success(&repeated);
    assert_eq!(
        first_descriptor,
        serde_json::from_slice::<Value>(&repeated.stdout).unwrap()
    );
    // A version cannot be silently overwritten with new bytes.
    std::fs::write(
        program.join("bundled_dependency/__init__.py"),
        "VERSION = 'second'\n",
    )
    .unwrap();
    assert!(!fixture.publish().await.status.success());
    let manifest_path = program.join("ledgence-program.json");
    let mut manifest: Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    manifest["program"]["version"] = "2.0.0".into();
    write_json(manifest_path, &manifest);
    let second = fixture.publish().await;
    success(&second);
    let second_descriptor: Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_ne!(first_descriptor["digest"], second_descriptor["digest"]);
    let tasks_path = fixture.example.join("tasks.json");
    let mut tasks: Value = serde_json::from_slice(&std::fs::read(&tasks_path).unwrap()).unwrap();
    let mut third = tasks[0].clone();
    third["program"]["version"] = "2.0.0".into();
    third["event"]["id"] = "evt_third".into();
    third["event"]["ldgtaskid"] = "task_third".into();
    third["event"]["ldgattemptid"] = "att_third".into();
    third["event"]["data"] = json!({"business": [null, true, {"anything": "user-owned"}]});
    tasks.as_array_mut().unwrap().push(third);
    write_json(tasks_path, &tasks);
    let result = fixture.run().await;
    success(&result);
    let reports: Vec<Value> = String::from_utf8(result.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap()["report"].clone())
        .collect();
    assert_eq!(reports.len(), 3);
    for (index, report) in reports.iter().enumerate() {
        assert_eq!(report["outcome"]["status"], "success");
        assert_eq!(report["outcome"]["output"]["event"], tasks[index]["event"]);
        assert_eq!(report["outcome"]["output"]["pid"], report["process_id"]);
        assert_eq!(
            report["outcome"]["output"]["version"],
            if index < 2 { "first" } else { "second" }
        );
        assert_eq!(
            &report["digest"],
            if index < 2 {
                &first_descriptor["digest"]
            } else {
                &second_descriptor["digest"]
            }
        );
    }
    assert_eq!(reports[0]["process_id"], reports[1]["process_id"]);
    assert_eq!(reports[1]["reused_process"], true);
    assert_ne!(reports[1]["process_id"], reports[2]["process_id"]);
    // Reopen the persistent cache in a new worker; task-to-descriptor binding and
    // package verification still apply, while the old process is never reused.
    std::fs::remove_dir_all(fixture.store.join("blobs")).unwrap();
    success(&fixture.run().await);
}

#[tokio::test]
async fn duplicate_attempts_and_changed_task_bindings_are_rejected_before_dispatch() {
    let fixture = Fixture::new().await;
    success(&fixture.publish().await);
    let tasks_path = fixture.example.join("tasks.json");
    let mut tasks: Value = serde_json::from_slice(&std::fs::read(&tasks_path).unwrap()).unwrap();
    tasks[1]["event"]["ldgattemptid"] = tasks[0]["event"]["ldgattemptid"].clone();
    write_json(&tasks_path, &tasks);
    let duplicate = fixture.run().await;
    assert!(!duplicate.status.success());
    assert!(
        duplicate.stdout.is_empty(),
        "no invocation may start before fixture validation completes"
    );
    assert!(String::from_utf8_lossy(&duplicate.stderr).contains("duplicate attempt"));
    let manifest_path = fixture.example.join("program/ledgence-program.json");
    let mut manifest: Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    manifest["program"]["version"] = "2.0.0".into();
    write_json(manifest_path, &manifest);
    success(&fixture.publish().await);
    tasks[1]["event"]["ldgattemptid"] = "att_different".into();
    tasks[1]["event"]["ldgtaskid"] = tasks[0]["event"]["ldgtaskid"].clone();
    tasks[1]["program"]["version"] = "2.0.0".into();
    write_json(tasks_path, &tasks);
    let changed = fixture.run().await;
    assert!(!changed.status.success());
    assert!(changed.stdout.is_empty());
    assert!(String::from_utf8_lossy(&changed.stderr).contains("bound program"));
}
