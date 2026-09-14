//! Strict command parsing without changing operator-provided identities.

use ledgence_orchestration_api::{ContractError, Result, Scope, validate_text};
use std::{collections::HashMap, path::PathBuf};

pub const HELP: &str = "Ledgence task administration\n\nCommands:\n  task submit --server URL --file FILE\n  task inspect --server URL --tenant ID --namespace ID --task ID\n  task status --server URL --tenant ID --namespace ID --task ID\n  task result --server URL --tenant ID --namespace ID --task ID\n  task attempt --server URL --tenant ID --namespace ID --task ID --attempt ID\n  task history --server URL --tenant ID --namespace ID --task ID [--after N]\n  task cancel --server URL --tenant ID --namespace ID --task ID\n\nsubmit reads the complete SubmitCommand JSON, including its idempotency_key.\nEach command makes one bounded HTTP exchange without automatic retries.\nJSON results go to stdout; diagnostics and Request-Id go to stderr.\nExit 0 means accepted operation, 2 means invalid input/usage, 1 means failure.\nA successful submit confirms acceptance, not successful task execution.\n";

#[derive(Debug)]
pub enum Command {
    Help,
    Task {
        server: String,
        operation: Operation,
    },
}

#[derive(Debug)]
pub enum Operation {
    Submit(PathBuf),
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
