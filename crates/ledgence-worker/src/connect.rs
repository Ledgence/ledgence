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
use ledgence_orchestration_api::{ContractError, LONG_POLL_WAIT_MS, Scope, validate_text};
use ledgence_worker_api::{Error, ErrorKind, Result, TraceBridge};
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
    acquire_wait: Duration,
    delivery_config: Option<PathBuf>,
}

impl ConnectOptions {
    pub fn parse(mut options: HashMap<String, String>) -> Result<Self> {
        let acquire_wait_ms = options
            .remove("--acquire-wait-ms")
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|_| input("invalid acquisition wait"))
            })
            .transpose()?
            .unwrap_or(LONG_POLL_WAIT_MS);
        if acquire_wait_ms > LONG_POLL_WAIT_MS {
            return Err(input("acquisition wait must be between 0 and 20000 ms"));
        }
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
            acquire_wait: Duration::from_millis(acquire_wait_ms),
            delivery_config: options.remove("--delivery-config").map(PathBuf::from),
        };
        check_empty(options)?;
        #[cfg(not(feature = "sqs"))]
        if config.delivery_config.is_some() {
            return Err(input(
                "--delivery-config requires a binary built with the sqs feature",
            ));
        }
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
    trace: Arc<dyn TraceBridge>,
) -> Result<()> {
    let client = Arc::new(
        HttpTaskService::new(&config.server)
            .map_err(contract_error)?
            .with_trace_bridge(trace.clone()),
    );
    let broker_source = prepare_broker(&config, client.clone(), signals, interrupted).await?;
    let mut delivery_config = DeliveryConfig::new(config.scope, config.queue);
    delivery_config.acquire_wait = config.acquire_wait;
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
    let worker = worker.with_trace_bridge(trace);
    if *interrupted {
        finish_shutdown(&worker, signals, interrupted).await?;
        return Err(Error::new(
            ErrorKind::Cancelled,
            "worker preparation interrupted",
        ));
    }
    let mut driver =
        DeliveryDriver::new(worker, client, delivery_config).map_err(contract_error)?;
    if let Some(source) = broker_source {
        driver = driver.with_acquisition_source(source);
    }
    let mut handle = driver.start();
    supervise(&mut handle, output, signals, interrupted).await
}

async fn prepare_broker(
    config: &ConnectOptions,
    client: Arc<HttpTaskService>,
    signals: &mut ShutdownSignals,
    interrupted: &mut bool,
) -> Result<Option<Arc<dyn ledgence_worker_delivery::AcquisitionSource>>> {
    let Some(path) = config.delivery_config.clone() else {
        return Ok(None);
    };
    #[cfg(not(feature = "sqs"))]
    {
        let _ = (path, client, signals, interrupted);
        Err(input(
            "--delivery-config requires a binary built with the sqs feature",
        ))
    }
    #[cfg(feature = "sqs")]
    {
        let scope = config.scope.clone();
        let queue = config.queue.clone();
        let mut loading = tokio::task::spawn_blocking(move || {
            let delivery = ledgence_adapter_sqs::deployment::DeliveryConfig::load(&path)?;
            delivery.validate_worker(&scope, &queue)?;
            Ok::<_, ContractError>(delivery)
        });
        let delivery = loop {
            tokio::select! {
                biased;
                signal = signals.recv() => {
                    signal?;
                    if *interrupted { return Err(forced_exit()); }
                    *interrupted = true;
                }
                result = &mut loading => break result.map_err(|_| input("delivery configuration loading failed"))?.map_err(contract_error)?,
            }
        };
        if *interrupted {
            return Err(Error::new(
                ErrorKind::Cancelled,
                "delivery configuration interrupted",
            ));
        }
        let connecting = ledgence_adapter_sqs::SqsQueue::connect(delivery.sqs);
        tokio::pin!(connecting);
        let sqs = tokio::select! {
            biased;
            signal = signals.recv() => {
                signal?;
                if *interrupted { return Err(forced_exit()); }
                *interrupted = true;
                // Configuration verification is read-only; no task/receipt has been acquired.
                return Err(Error::new(ErrorKind::Cancelled, "delivery connection interrupted"));
            }
            result = &mut connecting => result.map_err(contract_error)?,
        };
        let source = ledgence_worker_delivery::BrokerAcquisitionSource::new(Arc::new(sqs), client)
            .map_err(contract_error)?;
        Ok(Some(Arc::new(source)))
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn options(wait: Option<&str>) -> HashMap<String, String> {
        let mut options: HashMap<_, _> = [
            ("--server", "http://127.0.0.1:8080"),
            ("--tenant", "tenant"),
            ("--namespace", "namespace"),
            ("--queue", "queue"),
            ("--store", "/tmp/store"),
            ("--cache", "/tmp/cache"),
            ("--python", "python3"),
            ("--runner", "/tmp/runner.py"),
        ]
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect();
        if let Some(wait) = wait {
            options.insert("--acquire-wait-ms".into(), wait.into());
        }
        options
    }

    #[test]
    fn acquisition_wait_defaults_to_long_poll_and_accepts_immediate_mode() {
        for (input, expected) in [
            (None, 20_000),
            (Some("0"), 0),
            (Some("123"), 123),
            (Some("20000"), 20_000),
        ] {
            let config = ConnectOptions::parse(options(input)).unwrap();
            assert_eq!(config.acquire_wait, Duration::from_millis(expected));
            assert_eq!(config.concurrency, 4);
        }
    }

    #[test]
    fn acquisition_wait_rejects_invalid_and_out_of_contract_values() {
        for invalid in ["-1", "1.5", "twenty", "20001", "18446744073709551616"] {
            assert!(
                ConnectOptions::parse(options(Some(invalid))).is_err(),
                "accepted {invalid}"
            );
        }
    }
    #[test]
    fn delivery_configuration_is_feature_gated_before_startup() {
        let mut supplied = options(None);
        supplied.insert("--delivery-config".into(), "delivery.json".into());
        let result = ConnectOptions::parse(supplied);
        #[cfg(feature = "sqs")]
        assert_eq!(
            result.unwrap().delivery_config,
            Some(PathBuf::from("delivery.json"))
        );
        #[cfg(not(feature = "sqs"))]
        assert!(result.err().unwrap().to_string().contains("sqs feature"));
    }
}
