//! Connected delivery composition and retained shutdown observation.

use crate::{
    check_empty,
    composition::WorkerParts,
    finish_shutdown, input, number,
    output::Sink,
    required,
    signals::{ShutdownSignals, forced_exit},
};
use ledgence_adapter_http::HttpTaskService;
use ledgence_orchestration_api::{ContractError, Scope, validate_text};
use ledgence_worker_api::{Error, ErrorKind, Result};
use ledgence_worker_delivery::{DeliveryConfig, DeliveryDriver, DeliveryHandle};
use serde_json::json;
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

pub struct ConnectOptions {
    server: String,
    scope: Scope,
    queue: String,
    store: String,
    cache: PathBuf,
    python: PathBuf,
    runner: PathBuf,
    concurrency: usize,
}

impl ConnectOptions {
    pub fn parse(mut options: HashMap<String, String>) -> Result<Self> {
        let config = Self {
            server: required(&mut options, "--server")?,
            scope: Scope {
                tenant_id: required(&mut options, "--tenant")?,
                namespace: required(&mut options, "--namespace")?,
            },
            queue: required(&mut options, "--queue")?,
            store: required(&mut options, "--store")?,
            cache: required(&mut options, "--cache")?.into(),
            python: required(&mut options, "--python")?.into(),
            runner: required(&mut options, "--runner")?.into(),
            concurrency: number(options.remove("--concurrency"), 4, "concurrency")?,
        };
        check_empty(options)?;
        config.scope.validate().map_err(contract_error)?;
        validate_text(&config.queue, 128).map_err(contract_error)?;
        if config.concurrency > 1024 {
            return Err(input("concurrency must be at most 1024"));
        }
        Ok(config)
    }
}

pub async fn run(
    config: ConnectOptions,
    output: &Sink,
    signals: &mut ShutdownSignals,
    interrupted: &mut bool,
) -> Result<()> {
    let client = Arc::new(HttpTaskService::new(&config.server).map_err(contract_error)?);
    let delivery_config = DeliveryConfig::new(config.scope, config.queue);
    // Retain blocking disk preparation while still observing both signals.
    let mut preparation = tokio::task::spawn_blocking(move || {
        WorkerParts::new(&config.store, &config.cache, &config.python, &config.runner)?
            .worker(config.concurrency)
    });
    let worker = loop {
        tokio::select! {
            biased;
            signal = signals.recv() => {
                signal?;
                if *interrupted { return Err(forced_exit()); }
                *interrupted = true;
            }
            result = &mut preparation => {
                break result.map_err(|error| Error::new(ErrorKind::Runtime,
                    format!("worker preparation failed: {error}")))??;
            }
        }
    };
    if *interrupted {
        finish_shutdown(&worker, signals, interrupted).await?;
        return Err(Error::new(
            ErrorKind::Cancelled,
            "worker preparation interrupted",
        ));
    }
    let mut handle = DeliveryDriver::new(worker, client, delivery_config)
        .map_err(contract_error)?
        .start();
    supervise(&mut handle, output, signals, interrupted).await
}

async fn supervise(
    handle: &mut DeliveryHandle,
    output: &Sink,
    signals: &mut ShutdownSignals,
    interrupted: &mut bool,
) -> Result<()> {
    let status = loop {
        tokio::select! {
            biased;
            signal = signals.recv() => {
                signal?;
                if *interrupted { return Err(forced_exit()); }
                *interrupted = true;
                handle.stop();
                tracing::info!("worker shutdown requested; retaining delivery and process cleanup");
            }
            result = observe(handle, *interrupted) => {
                match result {
                    Ok(status) => break status,
                    Err(pending) => tracing::warn!(
                        session_id = pending.status.session_id.as_deref().unwrap_or(""),
                        settled_attempts = pending.status.settled_attempts,
                        lost_attempts = pending.status.lost_attempts,
                        "shutdown pending; retaining unresolved delivery and cleanup; a further signal forces exit"
                    ),
                }
            }
        }
    };
    // Output can itself stall; retain the persistent subscriptions throughout.
    let report = output.line(
        json!({"delivery": {
            "session_id": status.session_id,
            "finished": status.finished,
            "settled_attempts": status.settled_attempts,
            "lost_attempts": status.lost_attempts
        }})
        .to_string(),
    );
    tokio::pin!(report);
    loop {
        tokio::select! {
            biased;
            signal = signals.recv() => {
                signal?;
                if *interrupted { return Err(forced_exit()); }
                *interrupted = true;
            }
            result = &mut report => { result?; break; }
        }
    }
    if *interrupted {
        Err(Error::new(
            ErrorKind::Cancelled,
            "worker delivery interrupted",
        ))
    } else {
        Err(Error::new(
            ErrorKind::Runtime,
            status
                .last_error
                .map(|error| format!("worker delivery stopped: {error}"))
                .unwrap_or_else(|| "worker delivery stopped unexpectedly".into()),
        ))
    }
}

async fn observe(
    handle: &mut DeliveryHandle,
    stopping: bool,
) -> std::result::Result<
    ledgence_worker_delivery::DeliveryStatus,
    ledgence_worker_delivery::ShutdownPending,
> {
    if stopping {
        // Only the observation is bounded. The driver owns one retained cleanup
        // operation and continues remote reconciliation across every timeout.
        handle.shutdown(Duration::from_secs(5)).await
    } else {
        Ok(handle.wait().await)
    }
}

fn contract_error(error: ContractError) -> Error {
    let kind = if matches!(error, ContractError::InvalidInput(_)) {
        ErrorKind::InvalidInput
    } else {
        ErrorKind::Runtime
    };
    Error::new(kind, error.to_string())
}
