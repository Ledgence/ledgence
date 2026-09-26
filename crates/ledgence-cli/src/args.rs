//! Strict command parsing without changing operator-provided identities.

use ledgence_orchestration_api::console::{
    ConsoleProgramKind, ProgramDisplayMetadata, RegisterProgram,
};
use ledgence_orchestration_api::{
    ContractError, Result, Scope, TaskFilters, TaskListQuery, validate_text,
};
use ledgence_worker_api::ProgramRef;
use std::{collections::HashMap, path::PathBuf};

pub const HELP: &str = "Ledgence task administration\n\nCommands:\n  program register --server URL --program ID --version VERSION [--kind task|workflow|unspecified] [--display-name NAME] [--description TEXT] [--update-metadata true]\n  task submit --server URL --file FILE\n  task list --server URL --tenant ID --namespace ID [--state STATE] [--queue NAME] [--correlation-key KEY] [--submitted-from MS] [--submitted-until MS] [--limit N] [--cursor CURSOR]\n  task inspect --server URL --tenant ID --namespace ID --task ID\n  task status --server URL --tenant ID --namespace ID --task ID\n  task result --server URL --tenant ID --namespace ID --task ID\n  task attempt --server URL --tenant ID --namespace ID --task ID --attempt ID\n  task history --server URL --tenant ID --namespace ID --task ID [--after N]\n  task cancel --server URL --tenant ID --namespace ID --task ID\n\nsubmit reads the complete SubmitCommand JSON, including its idempotency_key.\nEach command makes one bounded HTTP exchange without automatic retries.\nJSON results go to stdout; diagnostics and Request-Id go to stderr.\nExit 0 means accepted operation, 2 means invalid input/usage, 1 means failure.\nA successful submit confirms acceptance, not successful task execution.\n";

#[derive(Debug)]
pub enum Command {
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
    let registration = RegisterProgram {
        program: ProgramRef {
            id: required(&mut options, "--program")?,
            version: required(&mut options, "--version")?,
        },
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
}
