//! Public command paths. Dispatch stays in-process so service signals and exit
//! status belong to the single executable rather than a wrapper subprocess.

use crate::args::invalid;
use ledgence_orchestration_api::Result;

pub const HELP: &str = "Ledgence\n\nUsage: ledgence <group> <command> [options]\n\nGroups:\n  access        Provision installation credentials\n  local         Start and manage a local Docker Compose installation\n  program       Create, package, publish, and register programs\n  task          Submit and inspect task executions\n  approval      Review and decide durable workflow actions\n  audit         Inspect verified decisions and response observations\n  resource      Configure shared resource capacity\n  queue         Manage queue concurrency and retained slots\n  admission     Inspect and release retained resource usage\n  effect        Inspect and assess retained external-effect evidence\n  mcp           Connect MCP clients to a Ledgence API\n  worker        Run local fixtures or connect a worker\n  orchestrator  Serve the API and administer its database\n\nUse ledgence <group> --help or ledgence <group> <command> --help.\nUse ledgence --version to print the executable version.\nExit status: 0 success, 2 invalid usage, 1 operational failure.\n";

#[derive(Debug, PartialEq, Eq)]
pub enum Route {
    Help(String),
    Version,
    Access(Vec<String>),
    Local(Vec<String>),
    Admin(Vec<String>),
    ProgramNew(Vec<String>),
    ProgramBuild(Vec<String>),
    ProgramPublish(Vec<String>),
    Mcp(Vec<String>),
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
        group: "queue",
        name: "list",
        options: "--credentials-file FILE --server URL --tenant ID --namespace NAME [--after QUEUE] [--limit 1..100]",
        description: "List policies and occupied slots in this instance scope.",
    },
    Leaf {
        group: "queue",
        name: "inspect",
        options: "--credentials-file FILE --server URL --tenant ID --namespace NAME --queue NAME",
        description: "Inspect configured capacity, revision and uncertain slots.",
    },
    Leaf {
        group: "queue",
        name: "holds",
        options: "--credentials-file FILE --server URL --tenant ID --namespace NAME --queue NAME [--after CURSOR] [--limit 1..100] [--include-released true|false]",
        description: "Inspect exact held attempts. Every successful read is audited and requires sensitive_read.",
    },
    Leaf {
        group: "queue",
        name: "put",
        options: "--credentials-file FILE --server URL --tenant ID --namespace NAME --file COMMAND.json",
        description: "Create or update a queue policy from a destination-bound command. Zero pauses new attempts and null removes the ceiling. Forced release after grace may admit work while the original operation is unconfirmed. Requires resource_control. Retry the unchanged file after a lost response.",
    },
    Leaf {
        group: "queue",
        name: "release",
        options: "--credentials-file FILE --server URL --tenant ID --namespace NAME --file COMMAND.json",
        description: "Release an uncertain inspected hold. The old operation could still be running. Requires resource_control and an explicit reason. Retry the exact saved command.",
    },
    Leaf {
        group: "resource",
        name: "list",
        options: "--credentials-file FILE --server URL --tenant ID --namespace NAME [--after NAME] [--limit 1..100]",
        description: "List resource policies and occupied capacity. Requires metadata_read. Follow next with the same scope.",
    },
    Leaf {
        group: "resource",
        name: "inspect",
        options: "--credentials-file FILE --server URL --tenant ID --namespace NAME --name NAME",
        description: "Read a resource policy and occupancy counters. Unknown usage remains occupied until a completed report or explicit operator release.",
    },
    Leaf {
        group: "resource",
        name: "put",
        options: "--credentials-file FILE --server URL --tenant ID --namespace NAME --file COMMAND.json",
        description: "Apply a destination-bound saved resource policy command. Requires resource_control. Include a stable request_id and expected_revision (null for creation). Retry only the identical saved file after an uncertain response.",
    },
    Leaf {
        group: "admission",
        name: "inspect",
        options: "--credentials-file FILE --server URL --tenant ID --namespace NAME (--workflow ID --activation ID --key KEY | --task ID --generation N)",
        description: "Inspect the original workflow operation (frozen arguments and retained output), or a standalone task generation (resource, worker and closure evidence). Task program output is available through task result. Requires sensitive_read and durable audit.",
    },
    Leaf {
        group: "admission",
        name: "release",
        options: "--credentials-file FILE --server URL --tenant ID --namespace NAME --file COMMAND.json",
        description: "Explicitly release uncertain capacity using a destination-bound saved command and inspected source_id/revision. Requires resource_control. This does not stop remote work or report business success. Replay an uncertain response using the identical file.",
    },
    Leaf {
        group: "effect",
        name: "inbox",
        options: "[--credentials-file FILE] --server URL --tenant ID --namespace NAME [--filter all|unconfirmed|confirmed|discrepant|disputed] [--provider-filter all|pending|needs_review|succeeded|failed|cancelled|disputed] [--workflow ID] [--cursor CURSOR] [--limit 1..100]",
        description: "Read retained effect metadata. Requires metadata_read in credential mode. Preserve filters with next_cursor. Evidence summaries distinguish journal confirmation, adapter-classified provider knowledge and operator handling. --provider-filter requires --filter all.",
    },
    Leaf {
        group: "effect",
        name: "inspect",
        options: "[--credentials-file FILE] --server URL --tenant ID --namespace NAME --workflow ID --activation ID --key KEY [--cursor CURSOR] [--limit 1..4]",
        description: "Read an effect's approved binding and reported evidence for its original effect activation. Requires credential mode, sensitive_read and a durable audit observation before returning evidence. Follow next_cursor even when a byte-limited page is short.",
    },
    Leaf {
        group: "effect",
        name: "resolve",
        options: "--credentials-file FILE --server URL --tenant ID --namespace NAME --file COMMAND.json",
        description: "Submit one saved, destination-bound operator assessment. Requires effect_resolve. Export the command from Console or client.effects.prepare_resolution(...).to_json(). Keep the same file and stable actor when reconciling a lost response. Does not redispatch or prove provider truth.",
    },
    Leaf {
        group: "audit",
        name: "list",
        options: "[--credentials-file FILE] --server URL --tenant ID --namespace NAME [--task ID | --workflow ID | --resource NAME | --queue NAME] [--cursor CURSOR] [--limit 1..100]",
        description: "Read one bounded page of audit events without task/workflow inputs or outputs. Resource policy and admission release events include retained operator reasons. Requires metadata_read in credential mode. Filtering covers the named execution or resource policy. Preserve filters with next_cursor. Pages are ordered observations, not a snapshot across concurrent commits.",
    },
    Leaf {
        group: "audit",
        name: "approval",
        options: "[--credentials-file FILE] --server URL --tenant ID --namespace NAME --workflow ID --key KEY --activation ID --revision N --decision ID",
        description: "Read verified acceptance for an exact approval occurrence and decision. A successful empty page means no verified accepting actor was recorded. An access or storage error is not evidence of absence. The original reviewer label is caller supplied.",
    },
    Leaf {
        group: "access",
        name: "init",
        options: "--instance-config FILE --server URL --directory NEW_DIR --subject NAME --expires-at RFC3339",
        description: "Create a private credential bundle with access.json, operator.json and worker.json. The operator credential has all operator capabilities; worker access is separate. The parent directory must exist. Secrets are written only to new private files. Start all API replicas with access.json after draining the previous processes. Credential expiry uses the API host clock.",
    },
    Leaf {
        group: "access",
        name: "issue",
        options: "--config FILE --server URL --directory NEW_DIR --subject NAME --class operator|service|worker --capabilities COMMA_SEPARATED --expires-at RFC3339",
        description: "Create a new private credential and a complete replacement access.json containing existing credentials plus the new one. Deploy the replacement configuration to every API replica. Reuse the subject and authority for credential rotation. Supported capabilities: metadata_read,sensitive_read,execution_submit,execution_control,program_publish,approval_decide,effect_resolve,resource_control,worker_execute. The worker class accepts only worker_execute.",
    },
    Leaf {
        group: "local",
        name: "up",
        options: "[--directory DIR] [--distribution DIR] [--port 8080] [--concurrency 1] [--context NAME]",
        description: "Start the local stack and wait for service readiness. Requires a local Docker engine with Linux containers and Compose 2.23.1+. On first use, copy the bundled local distribution (or --distribution) and save a unique project identity. Default directory: $XDG_DATA_HOME/ledgence/local or $HOME/.local/share/ledgence/local. Existing version, image, port, context and concurrency are preserved; upgrades are never automatic. Application packages must match the worker's Linux architecture and CPython version.",
    },
    Leaf {
        group: "local",
        name: "status",
        options: "[--directory DIR]",
        description: "Show the saved installation and all Compose service states. Does not start or change containers, configuration, or images.",
    },
    Leaf {
        group: "local",
        name: "logs",
        options: "[--directory DIR] [--follow] [--tail 100] [--service postgres|migrate|orchestrator|worker]",
        description: "Show bounded recent logs, optionally following new output. --tail accepts 1..10000 lines per service. Following logs does not block another local command.",
    },
    Leaf {
        group: "local",
        name: "down",
        options: "[--directory DIR]",
        description: "Stop the saved Compose project with a 65-second shutdown grace period. Preserve its named volumes, program store, configuration, and version for the next up. No volume deletion or automatic upgrade is performed.",
    },
    Leaf {
        group: "mcp",
        name: "serve",
        options: "[--credentials-file FILE] --server URL --tenant ID --namespace NAME [--read-only | --allow-effect-resolution | --allow-resource-control]",
        description: "Serve MCP over stdio using the configured Ledgence HTTP API. Requires the mcp build feature (enabled by default). Scope is fixed for the session. Submissions require caller-supplied idempotency keys and return immediately; inspect status/results separately. --read-only excludes mutations. Stdout is reserved for MCP; no database or worker starts in this process.",
    },
    Leaf {
        group: "approval",
        name: "inbox",
        options: "[--credentials-file FILE] --server URL --tenant ID --namespace NAME [--status pending|approved|rejected|expired|cancelled|all] [--program ID] [--due-before RFC3339|MS] [--cursor CURSOR] [--limit 1..100]",
        description: "Find approval requests across workflows, earliest deadline first. Defaults to pending requests and 50 items. Pass next_cursor with the same filters. Inspect a returned workflow and key to review its effective arguments.",
    },
    Leaf {
        group: "approval",
        name: "list",
        options: "[--credentials-file FILE] --server URL --tenant ID --namespace NAME --workflow ID [--after-key KEY] [--limit 1..10]",
        description: "List persisted approval requests, ordered by key. Pass next_cursor as --after-key to read the next page.",
    },
    Leaf {
        group: "approval",
        name: "inspect",
        options: "[--credentials-file FILE] --server URL --tenant ID --namespace NAME --workflow ID --key KEY",
        description: "Read one persisted approval and its exact effective action.",
    },
    Leaf {
        group: "approval",
        name: "decide",
        options: "[--credentials-file FILE] --server URL --file DECISION.json",
        description: "Send an approval decision bound to the saved request and action. The file includes scope, workflow_id, key, activation_id, revision, action, decision_id, decision, reviewer, and reason. Makes one HTTP exchange. After an uncertain response, retry the identical file; do not create a new decision.",
    },
    Leaf {
        group: "program",
        name: "new",
        options: "PATH [--name ID] [--kind task|workflow] [--platform linux/amd64|linux/arm64 --python MAJOR.MINOR --image IMAGE@sha256:HEX]",
        description: "Create a Python source project, tests, development metadata and build configuration. Defaults: task, program ID from the directory name, version 0.1.0. The parent directory must exist; existing destinations are never overwritten. Runs offline without starting services or installing dependencies. Supply all three target options together, or edit the generated target placeholders before building. The target is never inferred from this host.",
    },
    Leaf {
        group: "program",
        name: "build",
        options: "[--config ledgence.toml] [--output .ledgence/prepared] [--context NAME] [--timeout-seconds 600]",
        description: "Prepare a Python program in an explicitly pinned Linux Docker image. Includes only configured source/files and hash-pinned wheels. Requires a local Docker engine; no handler is executed. Writes a prepared directory and a separate build receipt. Existing different output is preserved.",
    },
    Leaf {
        group: "program",
        name: "publish",
        options: "[--credentials-file FILE] [--source .ledgence/prepared] (--store DIR | --server URL) [--register] [--kind task|workflow|unspecified] [--display-name NAME] [--description TEXT] [--update-metadata true] | --resume RECEIPT.json",
        description: "Publish an already prepared program; does not build it. Filesystem publication prints the immutable descriptor. HTTP publication requires explicit server enablement and saves the exact ZIP plus a durable receipt for --resume. --register additionally verifies and registers that exact descriptor; publication and registration are separate outcomes. Kind/display metadata can come from the build receipt. A changed immutable version requires a new version.",
    },
    Leaf {
        group: "program",
        name: "register",
        options: "[--credentials-file FILE] --server URL --program ID --version VERSION [--kind task|workflow|unspecified] [--display-name NAME] [--description TEXT] [--update-metadata true] [--expected-digest sha256:HEX --expected-size BYTES]",
        description: "Register an already published package in the installation catalog. Registration verifies the package without executing it. Makes one bounded HTTP exchange without automatic retries.",
    },
    Leaf {
        group: "worker",
        name: "run",
        options: "--tasks FILE --store DIR_OR_URL --cache DIR --python EXE [--runner BOOTSTRAP] [--concurrency N] [--timeout-ms MS]",
        description: "Execute a local JSON task fixture. Defaults: concurrency 4, timeout 30000 ms. The installed runtime helper is used when available; otherwise --runner is required. One concurrency limit controls the reusable process pool. JSON results go to stdout; logs go to stderr.",
    },
    Leaf {
        group: "worker",
        name: "connect",
        options: "[--credentials-file FILE] --server URL --tenant ID --namespace ID (--queue NAME | --poll-plan FILE) --store DIR_OR_URL --cache DIR --python EXE [--runner BOOTSTRAP] [--concurrency N] [--acquire-wait-ms MS] [--delivery-config FILE] [--display-name NAME]",
        description: "Acquire tasks, renew leases, and reconcile durable results. The installed runtime helper is used when available; otherwise --runner is required. One concurrency setting controls consumers and the reusable process pool. The first shutdown signal drains; a second forces a nonzero exit with unresolved work. Use --poll-plan for multiple integrated HTTP queues or multiple SQS Standard queues with a matching --delivery-config and the sqs build feature. Multi-queue SQS defaults to a 1s receive wait. Use --acquire-wait-ms to tune latency and empty receives.",
    },
    Leaf {
        group: "orchestrator",
        name: "serve",
        options: "--store DIR_OR_URL [--bind 127.0.0.1:8080] [--delivery-config FILE] [--completion-config FILE] [--instance-config FILE] [--console-dir DIR] [--allow-program-publication] [--access-config FILE] [--provider-config FILE] [--admission-config FILE] [--allow-insecure-console-access]",
        description: "Serve HTTP orchestration and supervise recovery. DATABASE_URL is required; the schema is verified, never migrated automatically. Console requires explicit instance configuration; with --instance-config, the installed Console is used when available unless --console-dir overrides it. Provider operations require --provider-config plus access and instance configurations; the bound file explicitly enables dispatch or declared-restore quarantine. An external proxy can provide HTTPS. First SIGINT/SIGTERM drains; a second forces a nonzero exit. SQS delivery requires the sqs build feature.",
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
        options: "[--credentials-file FILE] --server URL --file FILE",
        description: "Read the complete SubmitCommand JSON, including its idempotency_key. Success confirms acceptance, not successful execution. Makes one bounded HTTP exchange without automatic retries; JSON goes to stdout and diagnostics/Request-Id to stderr.",
    },
    Leaf {
        group: "task",
        name: "list",
        options: "[--credentials-file FILE] --server URL --tenant ID --namespace ID [--state STATE] [--queue NAME] [--correlation-key KEY] [--submitted-from MS] [--submitted-until MS] [--limit N] [--cursor CURSOR]",
        description: "Read one bounded page of tasks as JSON. Preserve the returned cursor when requesting the next page with the same filters.",
    },
    Leaf {
        group: "task",
        name: "inspect",
        options: "[--credentials-file FILE] --server URL --tenant ID --namespace ID --task ID",
        description: "Inspect a task. JSON goes to stdout; diagnostics and Request-Id go to stderr.",
    },
    Leaf {
        group: "task",
        name: "status",
        options: "[--credentials-file FILE] --server URL --tenant ID --namespace ID --task ID",
        description: "Read the durable status of a task.",
    },
    Leaf {
        group: "task",
        name: "result",
        options: "[--credentials-file FILE] --server URL --tenant ID --namespace ID --task ID",
        description: "Read the current task result. This command does not wait for completion or resubmit the task.",
    },
    Leaf {
        group: "task",
        name: "attempt",
        options: "[--credentials-file FILE] --server URL --tenant ID --namespace ID --task ID --attempt ID",
        description: "Inspect a recorded task attempt.",
    },
    Leaf {
        group: "task",
        name: "history",
        options: "[--credentials-file FILE] --server URL --tenant ID --namespace ID --task ID [--after N]",
        description: "Read task history after an unsigned sequence number.",
    },
    Leaf {
        group: "task",
        name: "cancel",
        options: "[--credentials-file FILE] --server URL --tenant ID --namespace ID --task ID",
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
        ("access", _) => Ok(Route::Access(arguments.into_iter().skip(1).collect())),
        ("local", _) => Ok(Route::Local(arguments.into_iter().skip(1).collect())),
        ("program", "new") => Ok(Route::ProgramNew(arguments.into_iter().skip(2).collect())),
        ("program", "build") => Ok(Route::ProgramBuild(arguments.into_iter().skip(2).collect())),
        ("program", "publish") => Ok(Route::ProgramPublish(
            arguments.into_iter().skip(2).collect(),
        )),
        ("worker", _) => Ok(Route::Worker(arguments.into_iter().skip(1).collect())),
        ("orchestrator", _) => Ok(Route::Orchestrator(arguments.into_iter().skip(1).collect())),
        ("mcp", "serve") => Ok(Route::Mcp(arguments.into_iter().skip(2).collect())),
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
