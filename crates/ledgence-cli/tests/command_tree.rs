//! The public command tree must remain discoverable without starting services,
//! reading configuration, or initializing optional exporters.
use std::process::{Command, Output};

fn invoke(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ledgence"))
        .args(args)
        .env("DATABASE_URL", "invalid-database-url")
        .env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "invalid-exporter-url")
        .env("OTEL_TRACES_SAMPLER", "invalid-sampler")
        .output()
        .unwrap()
}

#[test]
fn help_and_version_need_no_running_services_or_valid_environment() {
    for path in [vec![], vec!["--help"], vec!["-h"], vec!["help"]] {
        let result = invoke(&path);
        assert!(result.status.success(), "{path:?}: {result:?}");
        let text = String::from_utf8(result.stdout).unwrap();
        for group in ["program", "task", "approval", "worker", "orchestrator"] {
            assert!(text.contains(group), "{path:?}: {text}");
        }
        assert!(result.stderr.is_empty());
    }
    for flag in ["--version", "-V"] {
        let result = invoke(&[flag]);
        assert!(result.status.success());
        assert_eq!(
            result.stdout,
            concat!("ledgence ", env!("CARGO_PKG_VERSION"), "\n").as_bytes()
        );
        assert!(result.stderr.is_empty());
    }
    for (group, leaves) in [
        ("program", vec!["example", "publish", "register"]),
        ("approval", vec!["list", "inspect", "decide"]),
        ("worker", vec!["run", "connect"]),
        ("orchestrator", vec!["migrate", "serve", "retain"]),
        (
            "task",
            vec![
                "submit", "list", "inspect", "status", "result", "attempt", "history", "cancel",
            ],
        ),
    ] {
        for path in [vec![group], vec![group, "--help"], vec!["help", group]] {
            let result = invoke(&path);
            assert!(result.status.success(), "{path:?}: {result:?}");
            let text = String::from_utf8(result.stdout).unwrap();
            assert!(text.contains(&format!("Usage: ledgence {group}")));
            for leaf in &leaves {
                assert!(text.contains(leaf), "{path:?}: {text}");
            }
            assert!(result.stderr.is_empty());
        }
        for leaf in leaves {
            let result = invoke(&[group, leaf, "--help"]);
            assert!(result.status.success(), "{group} {leaf}: {result:?}");
            let text = String::from_utf8(result.stdout).unwrap();
            assert!(text.starts_with(&format!("Usage: ledgence {group} {leaf} ")));
            assert!(!text.contains("ledgence-worker"));
            assert!(!text.contains("ledgence-orchestrator"));
            assert!(result.stderr.is_empty());
            assert_eq!(invoke(&["help", group, leaf]).stdout, text.as_bytes());
        }
    }
}

#[test]
fn unknown_paths_and_invalid_options_fail_as_usage_before_startup() {
    for path in [
        vec!["approval", "unknown", "--help"],
        vec![
            "approval",
            "list",
            "--server",
            "http://localhost",
            "--tenant",
            "a",
            "--namespace",
            "b",
            "--workflow",
            "c",
            "--limit",
            "11",
        ],
        vec!["approval", "decide", "--file"],
        vec!["publish"],
        vec!["serve"],
        vec!["worker", "publish"],
        vec!["orchestrator", "connect"],
        vec!["program", "unknown", "--help"],
        vec!["unknown", "--help"],
        vec!["help", "program", "publish", "extra"],
        vec!["--version", "extra"],
        vec!["program", "publish", "--source"],
        vec!["program", "example", "--unexpected", "value"],
        vec!["worker", "connect", "--server", "http://127.0.0.1:1"],
        vec!["worker", "run", "--tasks", "unused", "--unknown", "value"],
        vec!["orchestrator", "migrate", "--timeout-ms", "0"],
        vec![
            "orchestrator",
            "serve",
            "--store",
            "unused",
            "--unknown",
            "value",
        ],
        vec!["orchestrator", "retain", "--apply"],
        vec!["task", "submit", "--unknown", "value"],
    ] {
        let result = invoke(&path);
        assert_eq!(result.status.code(), Some(2), "{path:?}: {result:?}");
        assert!(result.stdout.is_empty(), "{path:?}");
        let diagnostics = String::from_utf8(result.stderr).unwrap();
        assert!(!diagnostics.is_empty(), "{path:?}");
        assert!(
            !diagnostics.contains("invalid-sampler"),
            "command initialized telemetry: {path:?}"
        );
    }
}

#[test]
fn worker_connect_rejects_invalid_server_urls_as_usage_before_startup() {
    for server in [
        "invalid-url",
        "file:///tmp/orchestrator",
        "http://user:password@localhost:8080",
        "http://localhost:8080?scope=other",
        "http://localhost:8080#fragment",
    ] {
        let result = invoke(&[
            "worker",
            "connect",
            "--server",
            server,
            "--tenant",
            "tenant",
            "--namespace",
            "namespace",
            "--queue",
            "queue",
            "--store",
            "/unused",
            "--cache",
            "/unused",
            "--python",
            "python3",
            "--runner",
            "/unused",
        ]);
        assert_eq!(result.status.code(), Some(2), "{server}: {result:?}");
        assert!(result.stdout.is_empty(), "{server}: {result:?}");
        let diagnostics = String::from_utf8(result.stderr).unwrap();
        assert!(
            diagnostics.contains("server URL"),
            "{server}: {diagnostics}"
        );
        assert!(
            !diagnostics.contains("invalid-sampler"),
            "{server}: {diagnostics}"
        );
    }
}

#[cfg(unix)]
#[test]
fn usage_errors_keep_their_exit_status_when_stderr_is_closed() {
    use std::{
        process::Stdio,
        time::{Duration, Instant},
    };

    for args in [
        vec!["worker", "run", "--tasks"],
        vec!["orchestrator", "migrate", "--timeout-ms", "0"],
        vec!["task", "submit", "--file"],
    ] {
        let (reader, writer) = nix::unistd::pipe().unwrap();
        drop(reader);
        let mut child = Command::new(env!("CARGO_BIN_EXE_ledgence"))
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::from(writer))
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("usage diagnostic did not terminate: {args:?}");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.code(), Some(2), "{args:?}: {output:?}");
        assert!(output.stdout.is_empty(), "{args:?}: {output:?}");
    }
}

#[test]
fn admin_rejects_invalid_server_urls_before_optional_telemetry() {
    for server in [
        "invalid-url",
        "file:///tmp/orchestrator",
        "http://user:password@localhost:8080",
        "http://localhost:8080?scope=other",
        "http://localhost:8080#fragment",
    ] {
        for args in [
            vec![
                "task",
                "status",
                "--server",
                server,
                "--tenant",
                "tenant",
                "--namespace",
                "namespace",
                "--task",
                "task",
            ],
            vec![
                "program",
                "register",
                "--server",
                server,
                "--program",
                "program",
                "--version",
                "1.0.0",
            ],
        ] {
            let result = invoke(&args);
            assert_eq!(result.status.code(), Some(2), "{args:?}: {result:?}");
            assert!(result.stdout.is_empty(), "{args:?}: {result:?}");
            let diagnostics = String::from_utf8(result.stderr).unwrap();
            assert!(
                diagnostics.contains("server URL"),
                "{args:?}: {diagnostics}"
            );
            assert!(!diagnostics.contains("OTLP"), "{args:?}: {diagnostics}");
        }
    }
}
