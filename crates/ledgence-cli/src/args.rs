//! Strict command parsing without changing operator-provided identities.

use ledgence_orchestration_api::console::{
    ConsoleProgramKind, ProgramDisplayMetadata, RegisterProgram,
};
use ledgence_orchestration_api::{
    ContractError, Result, Scope, TaskFilters, TaskListQuery, validate_text,
};
use ledgence_worker_api::ProgramRef;
use std::{collections::HashMap, path::PathBuf};

#[derive(Debug)]
pub enum Command {
    Approval {
        server: String,
        operation: ApprovalOperation,
    },
    Help,
    Program {
        server: String,
        registration: RegisterProgram,
    },
    Task {
        server: String,
        operation: Operation,
    },
}

#[derive(Debug)]
pub enum Operation {
    Submit(PathBuf),
    List {
        scope: Scope,
        query: TaskListQuery,
    },
    Inspect {
        scope: Scope,
        task_id: String,
    },
    Status {
        scope: Scope,
        task_id: String,
    },
    Result {
        scope: Scope,
        task_id: String,
    },
    Attempt {
        scope: Scope,
        task_id: String,
        attempt_id: String,
    },
    History {
        scope: Scope,
        task_id: String,
        after_sequence: u64,
    },
    Cancel {
        scope: Scope,
        task_id: String,
    },
}

impl Command {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut args = args.into_iter();
        let Some(command) = args.next() else {
            return Ok(Self::Help);
        };
        if matches!(command.as_str(), "--help" | "-h" | "help") {
            if args.next().is_some() {
                return Err(invalid("unexpected arguments after help"));
            }
            return Ok(Self::Help);
        }
        if command == "approval" {
            return parse_approval(args);
        }
        if command == "program" {
            return parse_program(args);
        }
        if command != "task" {
            return Err(invalid("expected task command; use --help"));
        }
        let operation = args
            .next()
            .ok_or_else(|| invalid("missing task operation"))?;
        let mut options = HashMap::new();
        while let Some(key) = args.next() {
            if !key.starts_with("--") {
                return Err(invalid("expected a --name value option"));
            }
            let value = args
                .next()
                .ok_or_else(|| invalid(format!("missing value for {key}")))?;
            if options.insert(key.clone(), value).is_some() {
                return Err(invalid(format!("duplicate option {key}")));
            }
        }
        let server = required(&mut options, "--server")?;
        let operation = match operation.as_str() {
            "submit" => Operation::Submit(required(&mut options, "--file")?.into()),
            "list" => {
                let scope = Scope {
                    tenant_id: required(&mut options, "--tenant")?,
                    namespace: required(&mut options, "--namespace")?,
                };
                let query = TaskListQuery {
                    filters: TaskFilters {
                        state: options
                            .remove("--state")
                            .map(|value| {
                                serde_json::from_value(serde_json::Value::String(value))
                                    .map_err(|_| invalid("invalid task state"))
                            })
                            .transpose()?,
                        queue: options.remove("--queue"),
                        correlation_key: options.remove("--correlation-key"),
                        submitted_from: number(&mut options, "--submitted-from")?,
                        submitted_until: number(&mut options, "--submitted-until")?,
                    },
                    limit: number(&mut options, "--limit")?
                        .map(u32::try_from)
                        .transpose()
                        .map_err(|_| invalid("invalid list limit"))?
                        .unwrap_or(ledgence_orchestration_api::TASK_LIST_DEFAULT_LIMIT),
                    cursor: options.remove("--cursor"),
                };
                query.validate(&scope)?;
                Operation::List { scope, query }
            }
            "inspect" | "status" | "result" | "attempt" | "history" | "cancel" => {
                let scope = Scope {
                    tenant_id: required(&mut options, "--tenant")?,
                    namespace: required(&mut options, "--namespace")?,
                };
                scope.validate()?;
                let task_id = required(&mut options, "--task")?;
                validate_text(&task_id, 128)?;
                match operation.as_str() {
                    "inspect" => Operation::Inspect { scope, task_id },
                    "status" => Operation::Status { scope, task_id },
                    "result" => Operation::Result { scope, task_id },
                    "attempt" => {
                        let attempt_id = required(&mut options, "--attempt")?;
                        validate_text(&attempt_id, 128)?;
                        Operation::Attempt {
                            scope,
                            task_id,
                            attempt_id,
                        }
                    }
                    "history" => {
                        let value = options.remove("--after").unwrap_or_else(|| "0".into());
                        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                            return Err(invalid("after must be an unsigned 64-bit integer"));
                        }
                        let after_sequence = value
                            .parse()
                            .map_err(|_| invalid("after must be an unsigned 64-bit integer"))?;
                        Operation::History {
                            scope,
                            task_id,
                            after_sequence,
                        }
                    }
                    _ => Operation::Cancel { scope, task_id },
                }
            }
            _ => return Err(invalid("unknown task operation; use --help")),
        };
        if !options.is_empty() {
            let mut keys: Vec<_> = options.keys().map(String::as_str).collect();
            keys.sort_unstable();
            return Err(invalid(format!("unknown options: {}", keys.join(", "))));
        }
        Ok(Self::Task { server, operation })
    }
}

fn required(options: &mut HashMap<String, String>, key: &str) -> Result<String> {
    options
        .remove(key)
        .ok_or_else(|| invalid(format!("missing {key}")))
}

pub fn invalid(message: impl Into<String>) -> ContractError {
    ContractError::InvalidInput(message.into())
}

fn number(options: &mut HashMap<String, String>, key: &str) -> Result<Option<u64>> {
    options
        .remove(key)
        .map(|value| {
            if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid(format!("{key} must be an unsigned integer")));
            }
            value
                .parse()
                .map_err(|_| invalid(format!("{key} exceeds supported range")))
        })
        .transpose()
}

fn parse_program(mut args: impl Iterator<Item = String>) -> Result<Command> {
    if args.next().as_deref() != Some("register") {
        return Err(invalid("expected program register; use --help"));
    }
    let mut options = HashMap::new();
    while let Some(key) = args.next() {
        if !key.starts_with("--") {
            return Err(invalid("expected a --name value option"));
        }
        let value = args
            .next()
            .ok_or_else(|| invalid(format!("missing value for {key}")))?;
        if options.insert(key.clone(), value).is_some() {
            return Err(invalid(format!("duplicate option {key}")));
        }
    }
    let server = required(&mut options, "--server")?;
    let mut registration = RegisterProgram {
        program: ProgramRef {
            id: required(&mut options, "--program")?,
            version: required(&mut options, "--version")?,
        },
        expected_descriptor: None,
        metadata: ProgramDisplayMetadata {
            display_name: options.remove("--display-name"),
            description: options.remove("--description"),
            kind: match options.remove("--kind").as_deref() {
                None | Some("unspecified") => ConsoleProgramKind::Unspecified,
                Some("task") => ConsoleProgramKind::Task,
                Some("workflow") => ConsoleProgramKind::Workflow,
                _ => return Err(invalid("invalid program kind")),
            },
        },
        update_metadata: match options.remove("--update-metadata").as_deref() {
            None | Some("false") => false,
            Some("true") => true,
            _ => return Err(invalid("update-metadata must be true or false")),
        },
    };
    registration.expected_descriptor = match (
        options.remove("--expected-digest"),
        options.remove("--expected-size"),
    ) {
        (None, None) => None,
        (Some(digest), Some(size)) => Some(ledgence_worker_api::ProgramDescriptor {
            program: registration.program.clone(),
            digest: ledgence_worker_api::Digest(digest),
            size: size
                .parse()
                .map_err(|_| invalid("expected-size must be an unsigned integer"))?,
        }),
        _ => {
            return Err(invalid(
                "expected-digest and expected-size must be supplied together",
            ));
        }
    };
    registration.validate()?;
    if !options.is_empty() {
        return Err(invalid("unknown program register option"));
    }
    Ok(Command::Program {
        server,
        registration,
    })
}
#[cfg(test)]
mod program_tests {
    use super::*;
    #[test]
    fn registration_is_explicit_and_has_no_browser_scope() {
        let args = [
            "program",
            "register",
            "--server",
            "http://127.0.0.1:8080",
            "--program",
            "invoice-issuer",
            "--version",
            "release-a",
            "--kind",
            "workflow",
        ]
        .map(str::to_owned);
        let Command::Program { registration, .. } = Command::parse(args).unwrap() else {
            panic!("program command")
        };
        assert_eq!(registration.metadata.kind, ConsoleProgramKind::Workflow);
        assert!(!registration.update_metadata);
        for suffix in [
            ["--tenant", "other"],
            ["--version", "other"],
            ["--kind", "unknown"],
        ] {
            let mut args = vec![
                "program",
                "register",
                "--server",
                "http://localhost:8080",
                "--program",
                "p",
                "--version",
                "v",
            ];
            args.extend(suffix);
            assert!(Command::parse(args.into_iter().map(str::to_owned)).is_err());
        }
    }
    #[test]
    fn registration_descriptor_flags_are_paired_and_strictly_validated() {
        let base = [
            "program",
            "register",
            "--server",
            "http://localhost:8080",
            "--program",
            "invoice",
            "--version",
            "1",
        ];
        let digest = format!("sha256:{}", "a".repeat(64));
        let parse = |extra: Vec<String>| {
            let mut arguments = base.map(str::to_owned).to_vec();
            arguments.extend(extra);
            Command::parse(arguments)
        };
        let Command::Program {
            mut registration, ..
        } = parse(vec![
            "--expected-digest".into(),
            digest.clone(),
            "--expected-size".into(),
            "123".into(),
        ])
        .unwrap()
        else {
            panic!("program registration");
        };
        let expected = registration.expected_descriptor.as_ref().unwrap();
        assert_eq!(expected.program, registration.program);
        assert_eq!(expected.size, 123);
        assert_eq!(expected.digest.0, digest);
        // The public API additionally rejects a precondition for another program.
        registration
            .expected_descriptor
            .as_mut()
            .unwrap()
            .program
            .id = "other".into();
        assert!(registration.validate().is_err());
        for extra in [
            vec!["--expected-digest".into(), digest.clone()],
            vec!["--expected-size".into(), "123".into()],
        ] {
            assert!(parse(extra).is_err());
        }
        for size in ["0", "-1", "18446744073709551616", "1.5", "1e2", "invalid"] {
            assert!(
                parse(vec![
                    "--expected-digest".into(),
                    digest.clone(),
                    "--expected-size".into(),
                    size.into()
                ])
                .is_err(),
                "{size}"
            );
        }
        for hash in [
            "sha256:abc".to_owned(),
            "a".repeat(64),
            format!("sha256:{}", "A".repeat(64)),
            format!("sha512:{}", "a".repeat(64)),
        ] {
            assert!(
                parse(vec![
                    "--expected-digest".into(),
                    hash.clone(),
                    "--expected-size".into(),
                    "123".into()
                ])
                .is_err(),
                "{hash}"
            );
        }
    }
}

#[derive(Debug)]
pub enum ApprovalOperation {
    Inspect {
        scope: Scope,
        workflow_id: String,
        key: String,
    },
    List {
        scope: Scope,
        workflow_id: String,
        after_key: Option<String>,
        limit: u32,
    },
    Decide(PathBuf),
}
fn parse_approval(mut args: impl Iterator<Item = String>) -> Result<Command> {
    let operation = args
        .next()
        .ok_or_else(|| invalid("missing approval operation"))?;
    let mut options = HashMap::new();
    while let Some(key) = args.next() {
        if !key.starts_with("--") {
            return Err(invalid("expected a --name value option"));
        }
        let value = args
            .next()
            .ok_or_else(|| invalid(format!("missing value for {key}")))?;
        if options.insert(key.clone(), value).is_some() {
            return Err(invalid(format!("duplicate option {key}")));
        }
    }
    let server = required(&mut options, "--server")?;
    let operation = match operation.as_str() {
        "decide" => ApprovalOperation::Decide(required(&mut options, "--file")?.into()),
        "inspect" | "list" => {
            let scope = Scope {
                tenant_id: required(&mut options, "--tenant")?,
                namespace: required(&mut options, "--namespace")?,
            };
            scope.validate()?;
            let workflow_id = required(&mut options, "--workflow")?;
            validate_text(&workflow_id, 128)?;
            if operation == "inspect" {
                let key = required(&mut options, "--key")?;
                validate_text(&key, 128)?;
                ApprovalOperation::Inspect {
                    scope,
                    workflow_id,
                    key,
                }
            } else {
                let after_key = options.remove("--after-key");
                let limit = number(&mut options, "--limit")?
                    .map(u32::try_from)
                    .transpose()
                    .map_err(|_| invalid("invalid approval page limit"))?
                    .unwrap_or(10);
                ledgence_orchestration_api::validate_approval_page(after_key.as_deref(), limit)?;
                ApprovalOperation::List {
                    scope,
                    workflow_id,
                    after_key,
                    limit,
                }
            }
        }
        _ => return Err(invalid("unknown approval operation; use --help")),
    };
    if !options.is_empty() {
        return Err(invalid("unknown approval option"));
    }
    Ok(Command::Approval { server, operation })
}
