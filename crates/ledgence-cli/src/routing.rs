//! Public command paths. Dispatch stays in-process so service signals and exit
//! status belong to the single executable rather than a wrapper subprocess.

use crate::args::invalid;
use ledgence_orchestration_api::Result;

pub const HELP: &str = "Ledgence\n\nUsage: ledgence <group> <command> [options]\n\nGroups:\n  program       Package, publish, and register programs\n  task          Submit and inspect task executions\n  approval      Review and decide durable workflow actions\n  worker        Run local fixtures or connect a worker\n  orchestrator  Serve the API and administer its database\n\nUse ledgence <group> --help or ledgence <group> <command> --help.\nUse ledgence --version to print the executable version.\nExit status: 0 success, 2 invalid usage, 1 operational failure.\n";

#[derive(Debug, PartialEq, Eq)]
pub enum Route {
    Help(String),
    Version,
    Admin(Vec<String>),
    Worker(Vec<String>),
    Orchestrator(Vec<String>),
}

struct Leaf {
    group: &'static str,
    name: &'static str,
    options: &'static str,
    description: &'static str,
}

const COMMANDS: &[Leaf] = &[
    Leaf {
        group: "approval",
        name: "list",
        options: "--server URL --tenant ID --namespace NAME --workflow ID [--after-key KEY] [--limit 1..10]",
        description: "List persisted approval requests, ordered by key. Pass next_cursor as --after-key to read the next page.",
    },
    Leaf {
        group: "approval",
        name: "inspect",
        options: "--server URL --tenant ID --namespace NAME --workflow ID --key KEY",
        description: "Read one persisted approval and its exact effective action.",
    },
    Leaf {
        group: "approval",
        name: "decide",
        options: "--server URL --file DECISION.json",
        description: "Send an approval decision bound to the saved request and action. The file includes scope, workflow_id, key, activation_id, revision, action, decision_id, decision, reviewer, and reason. Makes one HTTP exchange. After an uncertain response, retry the identical file; do not create a new decision.",
    },
    Leaf {
        group: "program",
        name: "example",
        options: "--directory DIR --python EXE",
        description: "Create a local Python example package and task fixture.",
    },
    Leaf {
        group: "program",
        name: "publish",
        options: "--source DIR --store DIR",
        description: "Publish a prepared package to a filesystem store. Identical content is idempotent; changed content requires a new version. Prints the immutable descriptor as JSON.",
    },
    Leaf {
        group: "program",
        name: "register",
        options: "--server URL --program ID --version VERSION [--kind task|workflow|unspecified] [--display-name NAME] [--description TEXT] [--update-metadata true]",
        description: "Register an already published package in the installation catalog. Registration verifies the package without executing it. Makes one bounded HTTP exchange without automatic retries.",
    },
    Leaf {
        group: "worker",
        name: "run",
        options: "--tasks FILE --store DIR_OR_URL --cache DIR --python EXE --runner BOOTSTRAP [--concurrency N] [--timeout-ms MS]",
        description: "Execute a local JSON task fixture. Defaults: concurrency 4, timeout 30000 ms. One concurrency limit controls the reusable process pool. JSON results go to stdout; logs go to stderr.",
    },
    Leaf {
        group: "worker",
        name: "connect",
        options: "--server URL --tenant ID --namespace ID --queue NAME --store DIR_OR_URL --cache DIR --python EXE --runner BOOTSTRAP [--concurrency N] [--acquire-wait-ms MS] [--delivery-config FILE] [--display-name NAME]",
        description: "Acquire tasks, renew leases, and reconcile durable results. One concurrency setting controls consumers and the reusable process pool. The first shutdown signal drains; a second forces a nonzero exit with unresolved work. SQS delivery requires the sqs build feature.",
    },
    Leaf {
        group: "orchestrator",
        name: "serve",
        options: "--store DIR_OR_URL [--bind 127.0.0.1:8080] [--delivery-config FILE] [--completion-config FILE] [--instance-config FILE] [--console-dir DIR]",
        description: "Serve HTTP orchestration and supervise recovery. DATABASE_URL is required; the schema is verified, never migrated automatically. Console requires explicit instance configuration. An external proxy can provide HTTPS. First SIGINT/SIGTERM drains; a second forces a nonzero exit. SQS delivery requires the sqs build feature.",
    },
    Leaf {
        group: "orchestrator",
        name: "migrate",
        options: "[--timeout-ms 600000]",
        description: "Explicitly migrate DATABASE_URL. Timeout is 1..2147483647 ms after connection. Interrupted migrations may have committed earlier steps; rerun this command to reconcile.",
    },
    Leaf {
        group: "orchestrator",
        name: "retain",
        options: "--tenant ID --namespace ID [--retain-days 90] [--batch-size 128] [--batches 100] [--apply]",
        description: "Preview bounded retention against DATABASE_URL. --apply irreversibly retires eligible records in the explicit scope. Minimum retention is 90 days.",
    },
    Leaf {
        group: "task",
        name: "submit",
        options: "--server URL --file FILE",
        description: "Read the complete SubmitCommand JSON, including its idempotency_key. Success confirms acceptance, not successful execution. Makes one bounded HTTP exchange without automatic retries; JSON goes to stdout and diagnostics/Request-Id to stderr.",
    },
    Leaf {
        group: "task",
        name: "list",
        options: "--server URL --tenant ID --namespace ID [--state STATE] [--queue NAME] [--correlation-key KEY] [--submitted-from MS] [--submitted-until MS] [--limit N] [--cursor CURSOR]",
        description: "Read one bounded page of tasks as JSON. Preserve the returned cursor when requesting the next page with the same filters.",
    },
    Leaf {
        group: "task",
        name: "inspect",
        options: "--server URL --tenant ID --namespace ID --task ID",
        description: "Inspect a task. JSON goes to stdout; diagnostics and Request-Id go to stderr.",
    },
    Leaf {
        group: "task",
        name: "status",
        options: "--server URL --tenant ID --namespace ID --task ID",
        description: "Read the durable status of a task.",
    },
    Leaf {
        group: "task",
        name: "result",
        options: "--server URL --tenant ID --namespace ID --task ID",
        description: "Read the current task result. This command does not wait for completion or resubmit the task.",
    },
    Leaf {
        group: "task",
        name: "attempt",
        options: "--server URL --tenant ID --namespace ID --task ID --attempt ID",
        description: "Inspect a recorded task attempt.",
    },
    Leaf {
        group: "task",
        name: "history",
        options: "--server URL --tenant ID --namespace ID --task ID [--after N]",
        description: "Read task history after an unsigned sequence number.",
    },
    Leaf {
        group: "task",
        name: "cancel",
        options: "--server URL --tenant ID --namespace ID --task ID",
        description: "Request task cancellation with one HTTP exchange and no automatic retries. JSON goes to stdout; diagnostics and Request-Id go to stderr.",
    },
];

pub fn parse(arguments: Vec<String>) -> Result<Route> {
    let Some(group) = arguments.first().map(String::as_str) else {
        return Ok(Route::Help(HELP.into()));
    };
    if group == "help" {
        return help(&arguments[1..]).map(Route::Help);
    }
    if arguments.len() == 1 {
        if is_help(group) {
            return Ok(Route::Help(HELP.into()));
        }
        if matches!(group, "--version" | "-V") {
            return Ok(Route::Version);
        }
        return help(&arguments).map(Route::Help);
    }
    let command = &arguments[1];
    if arguments.len() == 2 && is_help(command) {
        return help(&arguments[..1]).map(Route::Help);
    }
    if arguments.len() == 3 && is_help(&arguments[2]) {
        return help(&arguments[..2]).map(Route::Help);
    }
    leaf(group, command)?;
    match (group, command.as_str()) {
        ("program", "example" | "publish") | ("worker", _) => {
            Ok(Route::Worker(arguments.into_iter().skip(1).collect()))
        }
        ("orchestrator", _) => Ok(Route::Orchestrator(arguments.into_iter().skip(1).collect())),
        _ => Ok(Route::Admin(arguments)),
    }
}

fn is_help(value: &str) -> bool {
    matches!(value, "--help" | "-h" | "help")
}

fn leaf(group: &str, command: &str) -> Result<&'static Leaf> {
    COMMANDS
        .iter()
        .find(|entry| entry.group == group && entry.name == command)
        .ok_or_else(|| {
            invalid(format!(
                "unknown command {group} {command}; use ledgence --help"
            ))
        })
}

fn help(path: &[String]) -> Result<String> {
    match path {
        [] => Ok(HELP.into()),
        [group] => {
            let commands: Vec<_> = COMMANDS
                .iter()
                .filter(|entry| entry.group == group)
                .collect();
            if commands.is_empty() {
                return Err(invalid(format!(
                    "unknown group {group}; use ledgence --help"
                )));
            }
            let mut text = format!("Usage: ledgence {group} <command> [options]\n\nCommands:\n");
            for command in commands {
                text.push_str(&format!("  {} {}\n", command.name, command.options));
            }
            text.push_str(&format!(
                "\nUse ledgence {group} <command> --help for details.\n"
            ));
            Ok(text)
        }
        [group, command] => {
            let entry = leaf(group, command)?;
            Ok(format!(
                "Usage: ledgence {group} {command} {}\n\n{}\n",
                entry.options, entry.description
            ))
        }
        _ => Err(invalid("help expects a group and an optional command")),
    }
}
