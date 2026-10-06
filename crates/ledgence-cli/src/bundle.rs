//! Installed resources travel with the executable, including when a launcher
//! reaches it through a symlink or the complete installation is relocated.

use std::path::{Path, PathBuf};

pub fn root() -> Option<PathBuf> {
    root_from_executable(&std::env::current_exe().ok()?)
}

fn root_from_executable(executable: &Path) -> Option<PathBuf> {
    let executable = executable.canonicalize().ok()?;
    let bin = executable.parent()?;
    (bin.file_name()? == "bin").then(|| bin.parent().map(Path::to_path_buf))?
}

pub fn worker_arguments(arguments: Vec<String>) -> Vec<String> {
    worker_defaults(arguments, root().as_deref())
}

fn worker_defaults(mut arguments: Vec<String>, bundle: Option<&Path>) -> Vec<String> {
    if matches!(
        arguments.first().map(String::as_str),
        Some("run" | "connect")
    ) && !arguments.iter().any(|argument| argument == "--runner")
        && let Some(path) = bundle.map(|root| root.join("runtime/ledgence/worker/bootstrap.py"))
        && path.is_file()
        && let Some(value) = path.to_str()
    {
        arguments.extend(["--runner".into(), value.into()]);
    }
    arguments
}

pub fn orchestrator_arguments(arguments: Vec<String>) -> Vec<String> {
    orchestrator_defaults(arguments, root().as_deref())
}

fn orchestrator_defaults(mut arguments: Vec<String>, bundle: Option<&Path>) -> Vec<String> {
    if arguments.first().map(String::as_str) == Some("serve")
        && arguments
            .iter()
            .any(|argument| argument == "--instance-config")
        && !arguments.iter().any(|argument| argument == "--console-dir")
        && let Some(path) = bundle.map(|root| root.join("console"))
        && path.join("index.html").is_file()
        && let Some(value) = path.to_str()
    {
        arguments.extend(["--console-dir".into(), value.into()]);
    }
    arguments
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).into()).collect()
    }

    #[test]
    fn defaults_require_real_resources_and_preserve_explicit_options() {
        let directory = tempfile::tempdir().unwrap();
        let root = Some(directory.path());
        assert_eq!(worker_defaults(args(&["run"]), root), args(&["run"]));
        fs::create_dir_all(directory.path().join("runtime/ledgence/worker")).unwrap();
        fs::write(
            directory
                .path()
                .join("runtime/ledgence/worker/bootstrap.py"),
            "",
        )
        .unwrap();
        for command in ["run", "connect"] {
            let result = worker_defaults(args(&[command]), root);
            assert_eq!(result[1], "--runner");
            assert!(Path::new(&result[2]).is_file());
            let explicit = args(&[command, "--runner", "custom.py"]);
            assert_eq!(worker_defaults(explicit.clone(), root), explicit);
        }
        assert_eq!(
            worker_defaults(args(&["publish"]), root),
            args(&["publish"])
        );
        fs::create_dir(directory.path().join("console")).unwrap();
        fs::write(directory.path().join("console/index.html"), "").unwrap();
        assert_eq!(
            orchestrator_defaults(args(&["serve"]), root),
            args(&["serve"])
        );
        let configured = args(&["serve", "--instance-config", "instance.json"]);
        let result = orchestrator_defaults(configured.clone(), root);
        assert_eq!(result[3], "--console-dir");
        let mut explicit = configured;
        explicit.extend(args(&["--console-dir", "custom"]));
        assert_eq!(orchestrator_defaults(explicit.clone(), root), explicit);
    }

    #[cfg(unix)]
    #[test]
    fn relocation_and_symlink_resolve_to_the_complete_bundle() {
        let directory = tempfile::tempdir().unwrap();
        let original = directory.path().join("original");
        fs::create_dir_all(original.join("bin")).unwrap();
        fs::write(original.join("bin/ledgence"), "").unwrap();
        let relocated = directory.path().join("relocated bundle");
        fs::rename(&original, &relocated).unwrap();
        let launcher = directory.path().join("launcher");
        std::os::unix::fs::symlink(relocated.join("bin/ledgence"), &launcher).unwrap();
        assert_eq!(
            root_from_executable(&launcher),
            Some(relocated.canonicalize().unwrap())
        );
        assert_eq!(
            root_from_executable(Path::new("/nonexistent/ledgence")),
            None
        );
    }
}
