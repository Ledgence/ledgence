//! Single-exchange task administration over the portable HTTP client adapter.

mod args;
mod logging;
mod submission;
mod telemetry;

use args::{Command, HELP, Operation};
use ledgence_adapter_http::HttpTaskService;
use ledgence_orchestration_api::{ContractError, Result, TaskService};
use serde::Serialize;
use serde_json::json;
use std::{
    io::Write,
    process::ExitCode,
    sync::{Arc, Mutex},
};

fn main() -> ExitCode {
    let arguments = std::env::args_os()
        .skip(1)
        .map(|argument| {
            argument
                .into_string()
                .map_err(|_| args::invalid("arguments must be valid UTF-8"))
        })
        .collect::<Result<Vec<_>>>();
    let command = match arguments.and_then(Command::parse) {
        Ok(command) => command,
        Err(error) => return diagnose(error, None),
    };
    let Command::Task { server, operation } = command else {
        return if std::io::stdout().write_all(HELP.as_bytes()).is_ok() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    };
    let mut logs = match logging::Logs::stderr() {
        Ok(logs) => logs,
        Err(error) => return diagnose(ContractError::Unavailable(error.to_string()), None),
    };
    let telemetry = match telemetry::Telemetry::start("ledgence-cli", logs.sink.clone()) {
        Ok(telemetry) => telemetry,
        Err(error) => {
            let result = diagnose_into(args::invalid(error), None, Some(&logs.sink));
            if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                runtime.block_on(async {
                    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), logs.finish())
                        .await;
                });
            } else {
                logs.abort();
            }
            return result;
        }
    };
    let result = run_task(&server, operation, telemetry.bridge(), &logs.sink);
    // Application runtime is already destroyed. Only optional telemetry and
    // bounded stderr delivery remain; neither changes the operation outcome.
    if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        runtime.block_on(async {
            telemetry.finish().await;
            let _ = logs.finish().await;
        });
    } else {
        logs.abort();
    }
    result
}

fn run_task(
    server: &str,
    operation: Operation,
    trace: Arc<dyn ledgence_worker_api::TraceBridge>,
    sink: &logging::Sink,
) -> ExitCode {
    let diagnose = |error, request_id| diagnose_into(error, request_id, Some(sink));
    let request_id = Arc::new(Mutex::new(None));
    let observed = request_id.clone();
    let client = match HttpTaskService::new(server) {
        Ok(client) => client
            .with_trace_bridge(trace)
            .with_observer(move |metadata| {
                *observed
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = metadata.request_id.clone();
            }),
        Err(error) => return diagnose(error, None),
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            return diagnose(
                ContractError::Unavailable(format!("cannot start command runtime: {error}")),
                None,
            );
        }
    };
    let result = runtime.block_on(execute(&client, operation));
    let request_id = request_id
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    match result {
        Ok(mut output) => {
            output.push(b'\n');
            if let Err(error) = std::io::stdout().write_all(&output) {
                return diagnose(
                    ContractError::Unavailable(format!(
                        "operation accepted but JSON output delivery failed: {error}"
                    )),
                    request_id,
                );
            }
            if sink.json(&json!({"request_id": request_id})).is_err() {
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        Err(error) => diagnose(error, request_id),
    }
}

async fn execute(service: &dyn TaskService, operation: Operation) -> Result<Vec<u8>> {
    match operation {
        Operation::Submit(path) => {
            let command = tokio::task::spawn_blocking(move || submission::read(&path))
                .await
                .map_err(|error| {
                    ContractError::Unavailable(format!("submission preparation failed: {error}"))
                })??;
            encode(service.submit(&command).await?)
        }
        Operation::Inspect { scope, task_id } => encode(service.inspect(&scope, &task_id).await?),
        Operation::Attempt {
            scope,
            task_id,
            attempt_id,
        } => encode(
            service
                .inspect_attempt(&scope, &task_id, &attempt_id)
                .await?,
        ),
        Operation::History {
            scope,
            task_id,
            after_sequence,
        } => encode(service.history(&scope, &task_id, after_sequence).await?),
        Operation::Cancel { scope, task_id } => encode(service.cancel(&scope, &task_id).await?),
    }
}

fn encode(value: impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(&value).map_err(|error| {
        ContractError::Unavailable(format!("cannot encode accepted result: {error}"))
    })
}

fn diagnose(error: ContractError, request_id: Option<String>) -> ExitCode {
    diagnose_into(error, request_id, None)
}

fn diagnose_into(
    error: ContractError,
    request_id: Option<String>,
    sink: Option<&logging::Sink>,
) -> ExitCode {
    let code = if matches!(error, ContractError::InvalidInput(_)) {
        2
    } else {
        1
    };
    let uncertain = matches!(error, ContractError::Unavailable(_));
    let error = match error {
        ContractError::InvalidInput(message) => ContractError::InvalidInput(bounded(message)),
        ContractError::Unavailable(message) => ContractError::Unavailable(bounded(message)),
        error => error,
    };
    let value =
        json!({"error": error, "request_id": request_id, "outcome_may_be_unknown": uncertain});
    let _ = match sink {
        Some(sink) => sink.json(&value),
        None => write_diagnostic(&value),
    };
    ExitCode::from(code)
}

fn bounded(mut message: String) -> String {
    const MAXIMUM: usize = 2048;
    if message.len() > MAXIMUM {
        let mut end = MAXIMUM;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
        message.push('…');
    }
    message
}

fn write_diagnostic(value: &impl Serialize) -> std::io::Result<()> {
    let mut output = serde_json::to_vec(value)?;
    output.push(b'\n');
    std::io::stderr().write_all(&output)
}
