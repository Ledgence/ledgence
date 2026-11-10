//! Read-only, resumable execution observation. A stopped observer never stops work.
mod output;

use crate::{args::invalid, credentials};
use ledgence_orchestration_api::*;
use std::{
    collections::HashMap,
    process::ExitCode,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

#[derive(Debug)]
struct Options {
    server: String,
    query: ExecutionProgressQuery,
    follow: bool,
    keep_following: bool,
    poll_interval: Duration,
    timeout: Option<Duration>,
}

fn parse(arguments: Vec<String>) -> Result<Options> {
    let mut args = arguments.into_iter();
    let group = args
        .next()
        .ok_or_else(|| invalid("missing execution kind"))?;
    if !matches!(group.as_str(), "task" | "workflow") || args.next().as_deref() != Some("progress")
    {
        return Err(invalid("expected task progress or workflow progress"));
    }
    let mut values = HashMap::new();
    let mut follow = false;
    let mut keep_following = false;
    while let Some(key) = args.next() {
        match key.as_str() {
            "--follow" if !follow => follow = true,
            "--keep-following" if !keep_following => keep_following = true,
            "--follow" | "--keep-following" => return Err(invalid(format!("duplicate {key}"))),
            "--server" | "--tenant" | "--namespace" | "--task" | "--workflow" | "--cursor"
            | "--limit" | "--poll-interval-ms" | "--timeout-ms" => {
                let value = args
                    .next()
                    .filter(|value| !value.starts_with("--"))
                    .ok_or_else(|| invalid(format!("missing value for {key}")))?;
                if values.insert(key.clone(), value).is_some() {
                    return Err(invalid(format!("duplicate {key}")));
                }
            }
            _ => return Err(invalid(format!("unknown progress option {key}"))),
        }
    }
    if !follow && (keep_following || values.contains_key("--poll-interval-ms")) {
        return Err(invalid(
            "--keep-following and --poll-interval-ms require --follow",
        ));
    }
    let mut required = |key| {
        values
            .remove(key)
            .ok_or_else(|| invalid(format!("missing {key}")))
    };
    let server = required("--server")?;
    let scope = Scope {
        tenant_id: required("--tenant")?,
        namespace: required("--namespace")?,
    };
    let target = if group == "task" {
        ExecutionProgressTarget::Task {
            id: required("--task")?,
        }
    } else {
        ExecutionProgressTarget::Workflow {
            id: required("--workflow")?,
        }
    };
    let limit = integer(values.remove("--limit"), "--limit")?.unwrap_or(100);
    let poll_ms =
        integer(values.remove("--poll-interval-ms"), "--poll-interval-ms")?.unwrap_or(1000);
    if poll_ms < 250 {
        return Err(invalid("--poll-interval-ms must be at least 250"));
    }
    let timeout = integer(values.remove("--timeout-ms"), "--timeout-ms")?;
    if timeout == Some(0) {
        return Err(invalid("--timeout-ms must be positive"));
    }
    let query = ExecutionProgressQuery {
        scope,
        target,
        cursor: values.remove("--cursor"),
        limit: u32::try_from(limit).map_err(|_| invalid("invalid progress limit"))?,
    };
    query.validate()?;
    if !values.is_empty() {
        return Err(invalid("task and workflow identifiers cannot be combined"));
    }
    Ok(Options {
        server,
        query,
        follow,
        keep_following,
        poll_interval: Duration::from_millis(poll_ms),
        timeout: timeout.map(Duration::from_millis),
    })
}

fn integer(value: Option<String>, name: &str) -> Result<Option<u64>> {
    value
        .map(|value| {
            if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid(format!("{name} must be an unsigned integer")));
            }
            value
                .parse()
                .map_err(|_| invalid(format!("{name} exceeds the supported range")))
        })
        .transpose()
}

pub fn run(mut arguments: Vec<String>) -> ExitCode {
    let prepared = (|| {
        let credentials = credentials::take(&mut arguments)?;
        let options = parse(arguments)?;
        let client = credentials::task_client(&options.server, credentials.as_ref())?;
        Ok::<_, ContractError>((options, client))
    })();
    let (options, client) = match prepared {
        Ok(value) => value,
        Err(error) => return diagnostic(error, None),
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            return diagnostic(
                unavailable(format!("cannot start observation runtime: {error}")),
                None,
            );
        }
    };
    let request_id = Arc::new(Mutex::new(None));
    let observed = request_id.clone();
    let client = client.with_observer(move |metadata| {
        *observed.lock().unwrap_or_else(|error| error.into_inner()) = metadata.request_id.clone();
    });
    let result = runtime.block_on(async {
        let mut signals = Signals::new()?;
        let mut output = output::Output::new().map_err(|error| unavailable(error.to_string()))?;
        let reader = client.progress_reader()?;
        let observation = observe(reader, options, |bytes| output.write(bytes));
        let result = tokio::select! {
            result = observation => result,
            result = signals.next() => {
                result?;
                Err(unavailable("progress observation interrupted; execution continues"))
            }
        };
        // Keep writer ownership until it has actually stopped. A second signal
        // permits a forced observer exit if a filesystem syscall cannot finish.
        output.abort();
        tokio::select! {
            () = output.finish() => result,
            signal = signals.next() => {
                signal?;
                Err(unavailable("forced observer exit before output shutdown completed"))
            }
        }
    });
    let request_id = request_id
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .clone();
    match result {
        Ok(()) => {
            if crate::write_diagnostic(&serde_json::json!({"request_id":request_id})).is_ok() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(error) => diagnostic(error, request_id),
    }
}

fn diagnostic(error: ContractError, request_id: Option<String>) -> ExitCode {
    let code = if matches!(error, ContractError::InvalidInput(_)) {
        2
    } else {
        1
    };
    let _ = crate::write_diagnostic(&serde_json::json!({
        "error":error,"request_id":request_id,"outcome_may_be_unknown":false,
    }));
    ExitCode::from(code)
}
fn unavailable(message: impl Into<String>) -> ContractError {
    ContractError::Unavailable(message.into())
}

async fn observe<F, Fut>(
    reader: &dyn ExecutionProgressReader,
    options: Options,
    mut write: F,
) -> Result<()>
where
    F: FnMut(Vec<u8>) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let deadline = options
        .timeout
        .map(|timeout| {
            Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| invalid("observation timeout exceeds the supported range"))
        })
        .transpose()?;
    let operation = async {
        let mut query = options.query;
        let mut previous: Option<(String, bool, u64)> = None;
        loop {
            let page = reader.execution_progress(&query).await?;
            page.validate_for(&query)
                .map_err(|_| unavailable("invalid progress service response"))?;
            if let Some((state, terminal, high_watermark)) = previous.as_ref()
                && (page.high_watermark.0 < *high_watermark
                    || (*terminal && (!page.terminal || page.state != *state)))
            {
                return Err(unavailable("progress service response regressed"));
            }
            let snapshot = (page.state.clone(), page.terminal, page.high_watermark.0);
            if !page.events.is_empty() || previous.as_ref() != Some(&snapshot) {
                let mut bytes = serde_json::to_vec(&page)
                    .map_err(|_| unavailable("cannot encode progress page"))?;
                if bytes.len() > EXECUTION_PROGRESS_PAGE_MAX_BYTES {
                    return Err(unavailable("progress page exceeds output limit"));
                }
                bytes.push(b'\n');
                write(bytes).await?;
            }
            // Advance only once complete output delivery has been acknowledged.
            query.cursor = Some(page.next_cursor);
            previous = Some(snapshot);
            if !options.follow || (page.terminal && !page.has_more && !options.keep_following) {
                return Ok(());
            }
            if !page.has_more {
                tokio::time::sleep(options.poll_interval).await;
            }
        }
    };
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, operation)
            .await
            .map_err(|_| unavailable("progress observation timed out; execution continues"))?,
        None => operation.await,
    }
}

struct Signals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}
impl Signals {
    fn new() -> Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Self {
                interrupt: signal(SignalKind::interrupt())
                    .map_err(|error| unavailable(error.to_string()))?,
                terminate: signal(SignalKind::terminate())
                    .map_err(|error| unavailable(error.to_string()))?,
            })
        }
        #[cfg(not(unix))]
        Ok(Self {})
    }
    async fn next(&mut self) -> Result<()> {
        #[cfg(unix)]
        {
            tokio::select! {
                _ = self.interrupt.recv() => (),
                _ = self.terminate.recv() => (),
            }
            Ok(())
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c()
            .await
            .map_err(|error| unavailable(error.to_string()))
    }
}

#[cfg(test)]
mod tests;
