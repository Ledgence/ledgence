//! Strict command parsing without changing operator-provided identities.

use ledgence_orchestration_api::console::{
    ConsoleProgramKind, ProgramDisplayMetadata, RegisterProgram,
};
use ledgence_orchestration_api::{
    ApprovalInboxQuery, ApprovalStatus, AuditFilter, AuditQuery, AuditResourceKind, ContractError,
    EffectInboxFilter, EffectInboxQuery, EffectInspectQuery, EffectOccurrence, Result, Scope,
    TaskFilters, TaskListQuery, validate_text,
};
use ledgence_worker_api::ProgramRef;
use std::{collections::HashMap, path::PathBuf};

#[derive(Debug)]
pub enum Command {
    Effect {
        server: String,
        operation: EffectOperation,
    },
    Audit {
        server: String,
        query: AuditQuery,
    },
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
        if command == "effect" {
            return parse_effect(args);
        }
        if command == "audit" {
            return parse_audit(args);
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
    Inbox(ApprovalInboxQuery),
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
fn parse_due_before(value: &str) -> Result<u64> {
    use time::{OffsetDateTime, format_description::well_known::Rfc3339};

    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        return value
            .parse()
            .map_err(|_| invalid("--due-before exceeds the supported range"));
    }
    let error = || {
        invalid(
            "--due-before must be Unix milliseconds or an RFC 3339 timestamp with millisecond precision",
        )
    };
    if !matches!(value.as_bytes().get(10), Some(b'T' | b't')) {
        return Err(error());
    }
    // Refuse finer precision, including digits that the nanosecond parser would discard.
    if value
        .split_once('.')
        .is_some_and(|(_, sub)| sub.bytes().take_while(u8::is_ascii_digit).count() > 3)
    {
        return Err(error());
    }
    let nanos = OffsetDateTime::parse(value, &Rfc3339)
        .map_err(|_| error())?
        .unix_timestamp_nanos();
    if nanos % 1_000_000 != 0 {
        return Err(error());
    }
    u64::try_from(nanos / 1_000_000).map_err(|_| error())
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
        "inbox" => {
            let scope = Scope {
                tenant_id: required(&mut options, "--tenant")?,
                namespace: required(&mut options, "--namespace")?,
            };
            let status = match options.remove("--status") {
                None => Some(ApprovalStatus::Pending),
                Some(value) if value == "all" => None,
                Some(value) => Some(
                    serde_json::from_value(serde_json::Value::String(value))
                        .map_err(|_| invalid("invalid approval status"))?,
                ),
            };
            let query = ApprovalInboxQuery {
                scope,
                status,
                program_id: options.remove("--program"),
                due_before: options
                    .remove("--due-before")
                    .as_deref()
                    .map(parse_due_before)
                    .transpose()?,
                cursor: options.remove("--cursor"),
                limit: number(&mut options, "--limit")?
                    .map(u32::try_from)
                    .transpose()
                    .map_err(|_| invalid("invalid approval inbox limit"))?
                    .unwrap_or(50),
            };
            query.validate()?;
            ApprovalOperation::Inbox(query)
        }
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

fn parse_audit(mut args: impl Iterator<Item = String>) -> Result<Command> {
    let operation = args
        .next()
        .ok_or_else(|| invalid("missing audit operation"))?;
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
    let scope = Scope {
        tenant_id: required(&mut options, "--tenant")?,
        namespace: required(&mut options, "--namespace")?,
    };
    let filter = match operation.as_str() {
        "list" => match (options.remove("--task"), options.remove("--workflow")) {
            (None, None) => AuditFilter::All,
            (Some(resource_id), None) => AuditFilter::Resource {
                resource_kind: AuditResourceKind::Task,
                resource_id,
            },
            (None, Some(resource_id)) => AuditFilter::Resource {
                resource_kind: AuditResourceKind::Workflow,
                resource_id,
            },
            (Some(_), Some(_)) => return Err(invalid("choose one task or workflow resource")),
        },
        "approval" => AuditFilter::ApprovalAcceptance {
            workflow_id: required(&mut options, "--workflow")?,
            key: required(&mut options, "--key")?,
            activation_id: required(&mut options, "--activation")?,
            revision: number(&mut options, "--revision")?
                .ok_or_else(|| invalid("missing --revision"))?,
            decision_id: required(&mut options, "--decision")?,
        },
        _ => return Err(invalid("unknown audit operation; use --help")),
    };
    let query = AuditQuery {
        scope,
        filter,
        cursor: options.remove("--cursor"),
        limit: number(&mut options, "--limit")?
            .map(u32::try_from)
            .transpose()
            .map_err(|_| invalid("invalid audit limit"))?
            .unwrap_or(if operation == "approval" { 2 } else { 50 }),
    };
    query.validate()?;
    if !options.is_empty() {
        return Err(invalid("unknown audit option"));
    }
    Ok(Command::Audit { server, query })
}

#[cfg(test)]
mod audit_tests {
    use super::*;

    fn parse(operation: &str, options: &[&str]) -> Result<Command> {
        Command::parse(
            [
                "audit",
                operation,
                "--server",
                "http://localhost",
                "--tenant",
                "acme",
                "--namespace",
                "demo",
            ]
            .into_iter()
            .chain(options.iter().copied())
            .map(str::to_owned),
        )
    }

    #[test]
    fn audit_queries_preserve_scope_resources_and_large_revisions() {
        let Command::Audit { query, .. } = parse("list", &[]).unwrap() else {
            panic!("audit expected")
        };
        assert_eq!(query.scope.tenant_id, "acme");
        assert_eq!(query.scope.namespace, "demo");
        assert_eq!(query.filter, AuditFilter::All);
        assert_eq!(query.limit, 50);
        for (flag, kind) in [
            ("--task", AuditResourceKind::Task),
            ("--workflow", AuditResourceKind::Workflow),
        ] {
            let Command::Audit { query, .. } =
                parse("list", &[flag, "execution", "--limit", "1"]).unwrap()
            else {
                panic!("audit expected")
            };
            assert_eq!(
                query.filter,
                AuditFilter::Resource {
                    resource_kind: kind,
                    resource_id: "execution".into()
                }
            );
            assert_eq!(query.limit, 1);
        }
        let Command::Audit { query, .. } = parse(
            "approval",
            &[
                "--workflow",
                "wf",
                "--key",
                "refund",
                "--activation",
                "task",
                "--revision",
                "18446744073709551615",
                "--decision",
                "decision",
            ],
        )
        .unwrap() else {
            panic!("audit expected")
        };
        assert_eq!(query.limit, 2);
        assert_eq!(query.cursor, None);
        assert!(matches!(
            query.filter,
            AuditFilter::ApprovalAcceptance {
                revision: u64::MAX,
                ..
            }
        ));
    }

    #[test]
    fn invalid_audit_queries_fail_before_transport() {
        for options in [
            vec!["--limit", "0"],
            vec!["--limit", "101"],
            vec!["--limit", "-1"],
            vec!["--limit", "1", "--limit", "2"],
            vec!["--task", "a", "--workflow", "b"],
            vec!["--cursor", "invented"],
            vec!["--task", ""],
            vec!["--actor", "pretend"],
            vec!["--tenant", "other"],
            vec!["--limit"],
        ] {
            assert!(parse("list", &options).is_err(), "{options:?}");
        }
        assert!(parse("approval", &[]).is_err());
        let base = [
            "--workflow",
            "wf",
            "--key",
            "refund",
            "--activation",
            "task",
            "--revision",
            "0",
            "--decision",
            "decision",
        ];
        for extra in [["--limit", "50"], ["--cursor", "invented"]] {
            let options: Vec<_> = base.into_iter().chain(extra).collect();
            assert!(parse("approval", &options).is_err());
        }
    }
}

#[derive(Debug)]
pub enum EffectOperation {
    Inbox(EffectInboxQuery),
    Inspect(EffectInspectQuery),
    Resolve {
        scope: Scope,
        file: std::path::PathBuf,
    },
}

fn parse_effect(mut args: impl Iterator<Item = String>) -> Result<Command> {
    let operation = args
        .next()
        .ok_or_else(|| invalid("missing effect operation"))?;
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
    let scope = Scope {
        tenant_id: required(&mut options, "--tenant")?,
        namespace: required(&mut options, "--namespace")?,
    };
    let cursor = options.remove("--cursor");
    let limit = number(&mut options, "--limit")?
        .map(u32::try_from)
        .transpose()
        .map_err(|_| invalid("invalid effect page limit"))?;
    let operation = match operation.as_str() {
        "inbox" => {
            let filter = options
                .remove("--filter")
                .map(|value| {
                    serde_json::from_value(serde_json::Value::String(value)).map_err(|_| {
                        invalid("effect filter must be all, unconfirmed, confirmed, discrepant, or disputed")
                    })
                })
                .transpose()?
                .unwrap_or(EffectInboxFilter::All);
            let query = EffectInboxQuery {
                scope,
                filter,
                workflow_id: options.remove("--workflow"),
                cursor,
                limit: limit.unwrap_or(50),
            };
            query.validate()?;
            EffectOperation::Inbox(query)
        }
        "inspect" => {
            let query = EffectInspectQuery {
                occurrence: EffectOccurrence {
                    scope,
                    workflow_id: required(&mut options, "--workflow")?,
                    activation_id: required(&mut options, "--activation")?,
                    approval_key: required(&mut options, "--key")?,
                },
                cursor,
                limit: limit.unwrap_or(4),
            };
            query.validate()?;
            EffectOperation::Inspect(query)
        }
        "resolve" => {
            scope.validate()?;
            if cursor.is_some() || limit.is_some() {
                return Err(invalid("resolve does not accept pagination options"));
            }
            EffectOperation::Resolve {
                scope,
                file: required(&mut options, "--file")?.into(),
            }
        }
        _ => {
            return Err(invalid(
                "expected effect inbox, inspect or resolve; use --help",
            ));
        }
    };
    if !options.is_empty() {
        return Err(invalid("unknown effect option"));
    }
    Ok(Command::Effect { server, operation })
}

#[cfg(test)]
mod effect_tests {
    use super::*;

    fn parse(operation: &str, options: &[&str]) -> Result<Command> {
        Command::parse(
            [
                "effect",
                operation,
                "--server",
                "http://127.0.0.1:8080",
                "--tenant",
                "acme",
                "--namespace",
                "billing",
            ]
            .into_iter()
            .chain(options.iter().copied())
            .map(str::to_owned),
        )
    }

    #[test]
    fn effect_queries_bind_scope_occurrence_and_bounded_pages() {
        let Command::Effect {
            operation: EffectOperation::Inbox(query),
            ..
        } = parse("inbox", &[]).unwrap()
        else {
            panic!("effect inbox expected")
        };
        assert_eq!(query.filter, EffectInboxFilter::All);
        assert_eq!(query.limit, 50);
        assert_eq!(query.scope.tenant_id, "acme");
        let Command::Effect {
            operation: EffectOperation::Inbox(query),
            ..
        } = parse(
            "inbox",
            &[
                "--filter",
                "discrepant",
                "--workflow",
                "workflow",
                "--limit",
                "1",
            ],
        )
        .unwrap()
        else {
            panic!("effect inbox expected")
        };
        assert_eq!(query.filter, EffectInboxFilter::Discrepant);
        assert_eq!(query.workflow_id.as_deref(), Some("workflow"));
        assert_eq!(query.cursor, None);
        let Command::Effect {
            operation: EffectOperation::Inspect(query),
            ..
        } = parse(
            "inspect",
            &[
                "--workflow",
                "workflow",
                "--activation",
                "original",
                "--key",
                "refund",
            ],
        )
        .unwrap()
        else {
            panic!("effect inspect expected")
        };
        assert_eq!(query.occurrence.activation_id, "original");
        assert_eq!(query.occurrence.approval_key, "refund");
        assert_eq!(query.limit, 4);
    }

    #[test]
    fn effect_queries_reject_invalid_limits_and_incomplete_identity_before_http() {
        for (operation, options) in [
            ("inbox", vec!["--limit", "0"]),
            ("inbox", vec!["--limit", "101"]),
            ("inbox", vec!["--filter", "pending"]),
            ("inbox", vec!["--cursor", "zz"]),
            ("inbox", vec!["--workflow", ""]),
            ("inbox", vec!["--limit", "1", "--limit", "2"]),
            ("inspect", vec!["--workflow", "w", "--key", "k"]),
            (
                "inspect",
                vec![
                    "--workflow",
                    "w",
                    "--activation",
                    "a",
                    "--key",
                    "k",
                    "--limit",
                    "5",
                ],
            ),
            (
                "inspect",
                vec![
                    "--workflow",
                    "w",
                    "--activation",
                    "a",
                    "--key",
                    "k",
                    "--filter",
                    "all",
                ],
            ),
            ("resolve", vec![]),
        ] {
            assert!(
                parse(operation, &options).is_err(),
                "{operation} {options:?}"
            );
        }
    }
}
