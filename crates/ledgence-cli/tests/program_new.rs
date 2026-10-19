//! Public scaffolding contracts and execution of the generated Python programs.
//! These tests do not start Docker, install packages, or access the network.
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    time::Duration,
};
use tokio::io::AsyncWriteExt;

const FILES: [&str; 7] = [
    ".gitignore",
    "README.md",
    "ledgence.toml",
    "program.py",
    "pyproject.toml",
    "requirements.txt",
    "tests/test_program.py",
];

fn cli() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ledgence"));
    command
        .env("PATH", "/does/not/exist")
        .env("DATABASE_URL", "invalid-database-url")
        .env("OTEL_TRACES_SAMPLER", "invalid-sampler");
    command
}

fn create(path: &Path, options: &[&str]) -> Output {
    cli()
        .args(["program", "new"])
        .arg(path)
        .args(options)
        .output()
        .unwrap()
}

fn success(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn files(root: &Path) -> BTreeSet<String> {
    fn visit(root: &Path, current: &Path, found: &mut BTreeSet<String>) {
        for entry in fs::read_dir(current).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let kind = entry.file_type().unwrap();
            assert!(!kind.is_symlink(), "scaffold must not contain links");
            if kind.is_dir() {
                visit(root, &path, found);
            } else {
                assert!(kind.is_file());
                found.insert(
                    path.strip_prefix(root)
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .replace('\\', "/"),
                );
            }
        }
    }
    let mut found = BTreeSet::new();
    visit(root, root, &mut found);
    found
}

fn read_toml(path: &Path) -> toml::Value {
    toml::from_str(&fs::read_to_string(path).unwrap()).unwrap()
}

#[test]
fn default_scaffold_is_offline_and_separates_source_from_development_files() {
    let temporary = tempfile::tempdir().unwrap();
    let project = temporary.path().join("hello-world");
    let result = success(&create(&project, &[]));
    assert_eq!(
        result["directory"],
        project.canonicalize().unwrap().to_str().unwrap()
    );
    assert_eq!(
        result["program"],
        json!({"id": "hello-world", "version": "0.1.0"})
    );
    assert_eq!(result["kind"], "task");
    assert_eq!(result["target_configured"], false);
    assert_eq!(result["files"], json!(FILES));
    assert_eq!(
        files(&project),
        FILES.into_iter().map(String::from).collect()
    );

    let config = read_toml(&project.join("ledgence.toml"));
    assert_eq!(config["program"]["id"].as_str(), Some("hello-world"));
    assert_eq!(config["program"]["version"].as_str(), Some("0.1.0"));
    assert_eq!(
        config["program"]["handler"].as_str(),
        Some("program:handle")
    );
    assert_eq!(config["program"]["kind"].as_str(), Some("task"));
    assert_eq!(config["source"]["root"].as_str(), Some("."));
    assert_eq!(
        config["source"]["include"].as_array().unwrap(),
        &[toml::Value::String("program.py".into())]
    );
    assert_eq!(config["target"]["protocol"].as_integer(), Some(3));
    for setting in ["platform", "python", "image"] {
        assert!(
            config["target"][setting]
                .as_str()
                .unwrap()
                .starts_with("REPLACE_WITH_"),
            "{setting} must require an explicit deployment target"
        );
    }

    let pyproject = read_toml(&project.join("pyproject.toml"));
    assert_eq!(pyproject["tool"]["uv"]["package"].as_bool(), Some(false));
    assert!(
        pyproject["dependency-groups"]["dev"]
            .as_array()
            .unwrap()
            .contains(&toml::Value::String(format!(
                "ledgence-worker=={}",
                env!("CARGO_PKG_VERSION")
            )))
    );
    assert!(
        pyproject["project"]
            .get("dependencies")
            .is_none_or(|value| value.as_array().unwrap().is_empty()),
        "the bundled worker helper is a development dependency"
    );
    assert!(
        fs::read_to_string(project.join("requirements.txt"))
            .unwrap()
            .lines()
            .all(|line| line.trim().is_empty() || line.trim_start().starts_with('#'))
    );
    assert!(!project.join("ledgence-program.json").exists());
    assert!(!project.join(".git").exists());
}

#[test]
fn explicit_target_and_name_are_preserved_for_both_program_kinds() {
    let temporary = tempfile::tempdir().unwrap();
    let image = format!("ledgence/ledgence@sha256:{}", "a".repeat(64));
    for (kind, platform) in [("task", "linux/amd64"), ("workflow", "linux/arm64")] {
        let project = temporary.path().join(format!("{kind} with spaces"));
        let result = success(&create(
            &project,
            &[
                "--name",
                "hello.world",
                "--kind",
                kind,
                "--platform",
                platform,
                "--python",
                "3.14",
                "--image",
                &image,
            ],
        ));
        assert_eq!(result["program"]["id"], "hello.world");
        assert_eq!(result["kind"], kind);
        assert_eq!(result["target_configured"], true);
        let config = read_toml(&project.join("ledgence.toml"));
        assert_eq!(config["program"]["id"].as_str(), Some("hello.world"));
        assert_eq!(config["program"]["kind"].as_str(), Some(kind));
        assert_eq!(config["target"]["platform"].as_str(), Some(platform));
        assert_eq!(config["target"]["python"].as_str(), Some("3.14"));
        assert_eq!(config["target"]["image"].as_str(), Some(image.as_str()));
        assert_eq!(
            read_toml(&project.join("pyproject.toml"))["project"]["name"].as_str(),
            Some("hello.world")
        );
    }
}

#[test]
fn invalid_arguments_do_not_create_any_project_files() {
    let temporary = tempfile::tempdir().unwrap();
    let project = temporary.path().join("example");
    let image = format!("ledgence/ledgence@sha256:{}", "a".repeat(64));
    let cases = [
        vec!["--kind", "unknown"],
        vec!["--name", "Invalid Name"],
        vec!["--name", "_invalid"],
        vec!["--name", "trailing-"],
        vec!["--unknown", "value"],
        vec!["--kind", "task", "--kind", "workflow"],
        vec!["--name"],
        vec!["--platform", "linux/amd64"],
        vec!["--python", "3.14", "--image", image.as_str()],
        vec![
            "--platform",
            "macos/arm64",
            "--python",
            "3.14",
            "--image",
            image.as_str(),
        ],
        vec![
            "--platform",
            "linux/amd64",
            "--python",
            "3.10",
            "--image",
            image.as_str(),
        ],
        vec![
            "--platform",
            "linux/amd64",
            "--python",
            "3.14",
            "--image",
            "ledgence/ledgence:latest",
        ],
    ];
    for options in cases {
        let result = create(&project, &options);
        assert_eq!(result.status.code(), Some(2), "{options:?}: {result:?}");
        assert!(result.stdout.is_empty());
        assert!(!project.exists(), "{options:?}");
        assert_eq!(fs::read_dir(temporary.path()).unwrap().count(), 0);
    }

    let missing_path = cli().args(["program", "new"]).output().unwrap();
    assert_eq!(missing_path.status.code(), Some(2));
    let missing_parent = temporary.path().join("absent/example");
    assert!(!create(&missing_parent, &[]).status.success());
    assert!(!temporary.path().join("absent").exists());
}

#[test]
fn existing_files_and_directories_are_never_overwritten() {
    let temporary = tempfile::tempdir().unwrap();
    let project = temporary.path().join("example");
    fs::write(&project, b"existing file").unwrap();
    assert!(!create(&project, &[]).status.success());
    assert_eq!(fs::read(&project).unwrap(), b"existing file");
    fs::remove_file(&project).unwrap();

    fs::create_dir(&project).unwrap();
    assert!(!create(&project, &[]).status.success());
    assert_eq!(fs::read_dir(&project).unwrap().count(), 0);
    fs::write(project.join("program.py"), b"existing program").unwrap();
    assert!(!create(&project, &[]).status.success());
    assert_eq!(
        fs::read(project.join("program.py")).unwrap(),
        b"existing program"
    );
    assert_eq!(fs::read_dir(&project).unwrap().count(), 1);
}

#[cfg(unix)]
#[test]
fn symlink_destinations_including_dangling_links_are_preserved() {
    let temporary = tempfile::tempdir().unwrap();
    let project = temporary.path().join("example");
    let target = temporary.path().join("target");
    std::os::unix::fs::symlink(&target, &project).unwrap();
    assert!(!create(&project, &[]).status.success());
    assert_eq!(fs::read_link(&project).unwrap(), target);
    assert!(!target.exists());

    fs::create_dir(&target).unwrap();
    fs::write(target.join("program.py"), b"existing program").unwrap();
    assert!(!create(&project, &[]).status.success());
    assert_eq!(fs::read_link(&project).unwrap(), target);
    assert_eq!(
        fs::read(target.join("program.py")).unwrap(),
        b"existing program"
    );
    assert_eq!(fs::read_dir(&target).unwrap().count(), 1);
}

#[test]
fn competing_creators_produce_one_complete_project_without_replacement() {
    let temporary = tempfile::tempdir().unwrap();
    let project = temporary.path().join("example");
    let mut first_command = cli();
    first_command
        .args(["program", "new"])
        .arg(&project)
        .args(["--kind", "task"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let first = first_command.spawn().unwrap();
    let mut second_command = cli();
    second_command
        .args(["program", "new"])
        .arg(&project)
        .args(["--kind", "workflow"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let second = second_command.spawn().unwrap();
    let outputs = [
        first.wait_with_output().unwrap(),
        second.wait_with_output().unwrap(),
    ];
    assert_eq!(
        outputs
            .iter()
            .filter(|output| output.status.success())
            .count(),
        1,
        "{outputs:?}"
    );
    let winner = success(
        outputs
            .iter()
            .find(|output| output.status.success())
            .unwrap(),
    );
    assert_eq!(
        files(&project),
        FILES.into_iter().map(String::from).collect()
    );
    assert_eq!(fs::read_dir(temporary.path()).unwrap().count(), 1);
    assert_eq!(
        read_toml(&project.join("ledgence.toml"))["program"]["kind"].as_str(),
        winner["kind"].as_str()
    );
}

fn python() -> String {
    std::env::var("LEDGENCE_PYTHON").unwrap_or_else(|_| "python3".into())
}

fn repository() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

async fn run_python(mut command: tokio::process::Command, input: Option<Vec<u8>>) -> Output {
    command
        .kill_on_drop(true)
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    if let Some(input) = input {
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(&input).await.unwrap();
        drop(stdin);
    }
    tokio::time::timeout(Duration::from_secs(30), child.wait_with_output())
        .await
        .expect("generated program check must finish within its deadline")
        .unwrap()
}

fn invocation(kind: &str, index: usize, name: &str) -> Value {
    let event_id = format!("event-{index}");
    let attempt_id = format!("attempt-{index}");
    let activation_id = format!("activation-{index}");
    let mut message = json!({
        "v": 3, "type": "invoke", "event_id": event_id, "attempt_id": attempt_id,
        "processing_context": null,
        "event": {
            "specversion": "1.0", "source": "urn:ledgence:scaffold-test",
            "type": "com.ledgence.task.invocation.requested.v1",
            "id": event_id, "ldgattemptid": attempt_id,
            "ldgtaskid": activation_id,
            "data": {"name": name},
        },
    });
    if kind == "workflow" {
        message["event"]["ldgworkflowid"] = json!("workflow-1");
        message["event"]["ldgactivationid"] = json!(activation_id);
        message["extension"] = json!({
            "schema": "ledgence.workflow.activation.v1",
            "payload": {
                "v": 1, "workflow_id": "workflow-1", "activation_id": activation_id,
                "revision": 0, "continuation": "start",
                "state": null, "inputs": {}, "local_steps": [],
            },
        });
    }
    message
}

#[tokio::test]
async fn generated_unit_tests_and_real_bootstrap_execute_task_and_workflow() {
    let temporary = tempfile::tempdir().unwrap();
    let mut version_command = tokio::process::Command::new(python());
    version_command.args([
        "-I",
        "-c",
        "import sys;print(f'{sys.version_info.major}.{sys.version_info.minor}')",
    ]);
    let version = run_python(version_command, None).await;
    assert!(version.status.success(), "{version:?}");
    let version = String::from_utf8(version.stdout).unwrap();

    for kind in ["task", "workflow"] {
        let project = temporary.path().join(kind);
        success(&create(&project, &["--kind", kind]));
        let mut tests = tokio::process::Command::new(python());
        tests
            .args(["-B", "-m", "unittest", "discover", "-s", "tests", "-v"])
            .current_dir(&project)
            // Test-only source helper. Scaffolded users install the dev dependency.
            .env("PYTHONPATH", repository().join("sdk/python"));
        let result = run_python(tests, None).await;
        assert!(
            result.status.success(),
            "{kind}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(String::from_utf8_lossy(&result.stderr).contains("Ran "));

        let messages = [
            invocation(kind, 1, "Ada"),
            invocation(kind, 2, "Grace"),
            json!({"v": 3, "type": "shutdown"}),
        ];
        let mut input = Vec::new();
        for message in messages {
            serde_json::to_writer(&mut input, &message).unwrap();
            input.push(b'\n');
        }
        let mut bootstrap = tokio::process::Command::new(python());
        bootstrap
            .args(["-I", "-S", "-B"])
            .arg(repository().join("sdk/python/ledgence/worker/bootstrap.py"))
            .arg("--package-root")
            .arg(&project)
            .args([
                "--handler",
                "program:handle",
                "--python-version",
                version.trim(),
                "--protocol-version",
                "3",
            ])
            .current_dir(&project);
        let result = run_python(bootstrap, Some(input)).await;
        assert!(
            result.status.success(),
            "{kind}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let frames: Vec<Value> = result
            .stdout
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        assert_eq!(frames.first().unwrap()["type"], "ready");
        assert_eq!(frames.last().unwrap()["type"], "closing");
        let results: Vec<_> = frames
            .iter()
            .filter(|frame| frame["type"] == "result")
            .collect();
        assert_eq!(results.len(), 2, "{frames:?}");
        for (index, (result, name)) in results.iter().zip(["Ada", "Grace"]).enumerate() {
            assert_eq!(result["status"], "success", "{kind}: {result:?}");
            assert_eq!(result["event_id"], format!("event-{}", index + 1));
            assert_eq!(result["attempt_id"], format!("attempt-{}", index + 1));
            let expected = json!({"message": format!("Hello, {name}!")});
            if kind == "task" {
                assert_eq!(result["output"], expected);
            } else {
                assert_eq!(result["output"]["kind"], "complete");
                assert_eq!(
                    result["output"]["activation_id"],
                    format!("activation-{}", index + 1)
                );
                assert_eq!(result["output"]["revision"], 0);
                assert_eq!(result["output"]["output"], expected);
            }
        }
        assert_eq!(
            files(&project),
            FILES.into_iter().map(String::from).collect(),
            "tests and runtime invocation must leave the selected source unchanged"
        );
    }
}
