//! Connected delivery composition and retained shutdown observation.

use crate::{
    check_empty,
    composition::WorkerParts,
    finish_shutdown, input, number,
    output::Sink,
    poll_file::PlanFile,
    required,
    signals::{ShutdownSignals, forced_exit},
};
use ledgence_adapter_http::{HttpCredentials, HttpTaskService};
use ledgence_orchestration_api::{ContractError, LONG_POLL_WAIT_MS, Scope, validate_text};
use ledgence_worker_api::{Error, ErrorKind, Result, TraceBridge};
use ledgence_worker_delivery::{DeliveryConfig, DeliveryDriver, DeliveryHandle};
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

pub struct ConnectOptions {
    server: String,
    client: HttpTaskService,
    credentials: Option<HttpCredentials>,
    scope: Scope,
    queue: String,
    poll_plan: Option<PlanFile>,
    store: String,
    cache: PathBuf,
    python: PathBuf,
    runner: PathBuf,
    concurrency: usize,
    acquire_wait: Duration,
    delivery_config: Option<PathBuf>,
    display_name: Option<String>,
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
            .transpose()?;
        if acquire_wait_ms.is_some_and(|wait| wait > LONG_POLL_WAIT_MS) {
            return Err(input("acquisition wait must be between 0 and 20000 ms"));
        }
        let server = required(&mut options, "--server")?;
        let credentials = options
            .remove("--credentials-file")
            .map(|path| HttpCredentials::from_file(std::path::Path::new(&path)))
            .transpose()
            .map_err(contract_error)?;
        let scope = Scope {
            tenant_id: required(&mut options, "--tenant")?,
            namespace: required(&mut options, "--namespace")?,
        };
        let (queue, poll_plan) = match (options.remove("--queue"), options.remove("--poll-plan")) {
            (Some(queue), None) => (queue, None),
            (None, Some(path)) => {
                let file = PlanFile::load(PathBuf::from(path))?;
                let queue = file
                    .planner()
                    .queues()
                    .first()
                    .ok_or_else(|| input("poll plan requires at least one queue"))?
                    .clone();
                (queue, Some(file))
            }
            _ => return Err(input("specify exactly one of --queue or --poll-plan")),
        };
        let store = required(&mut options, "--store")?;
        let cache = required(&mut options, "--cache")?.into();
        let python = required(&mut options, "--python")?.into();
        let runner = required(&mut options, "--runner")?.into();
        let concurrency = number(options.remove("--concurrency"), 4, "concurrency")?;
        let delivery_config = options.remove("--delivery-config").map(PathBuf::from);
        let acquire_wait_ms = acquire_wait_ms.unwrap_or_else(|| {
            if delivery_config.is_some()
                && poll_plan
                    .as_ref()
                    .is_some_and(|file| file.planner().queues().len() > 1)
            {
                // An empty long receive holds its consumer on the chosen route.
                1000
            } else {
                LONG_POLL_WAIT_MS
            }
        });
        let display_name = options.remove("--display-name");
        check_empty(options)?;
        #[cfg(not(feature = "sqs"))]
        if delivery_config.is_some() {
            return Err(input(
                "--delivery-config requires a binary built with the sqs feature",
            ));
        }
        scope.validate().map_err(contract_error)?;
        validate_text(&queue, 128).map_err(contract_error)?;
        if let Some(name) = &display_name {
            validate_text(name, 128).map_err(contract_error)?;
        }
        if concurrency > 1024 {
            return Err(input("concurrency must be at most 1024"));
        }
        // Reuse the adapter's URL validation before runtime/telemetry startup.
        // The constructor does not connect; retain the client for execution.
        let mut client = HttpTaskService::new(&server).map_err(contract_error)?;
        if let Some(credentials) = &credentials {
            client = client
                .with_credentials(credentials.clone())
                .map_err(contract_error)?;
        }
        let config = Self {
            server,
            client,
            credentials,
            scope,
            queue,
            poll_plan,
            store,
            cache,
            python,
            runner,
            concurrency,
            acquire_wait: Duration::from_millis(acquire_wait_ms),
            delivery_config,
            display_name,
        };
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
    let client = Arc::new(config.client.clone().with_trace_bridge(trace.clone()));
    // Authenticate and negotiate before any broker, session or runtime exists.
    let runtime_access = {
        let discovered = client.discover_worker_runtime(&config.scope);
        tokio::pin!(discovered);
        tokio::select! {
            result = &mut discovered => result.map_err(contract_error)?,
            signal = signals.recv() => {
                signal?;
                *interrupted = true;
                return Err(Error::new(ErrorKind::Cancelled, "worker discovery interrupted"));
            }
        }
    };
    let mut broker_sources = prepare_broker(&config, client.clone(), signals, interrupted).await?;
    // Reporting has its own bounded JSON executor and connection pool, so a slow
    // observation exchange cannot consume execution-control admission.
    let mut observation_client =
        HttpTaskService::with_timeout(&config.server, Duration::from_secs(2))
            .map_err(contract_error)?;
    if let Some(credentials) = &config.credentials {
        observation_client = observation_client
            .with_credentials(credentials.clone())
            .map_err(contract_error)?;
    }
    let observation_client = Arc::new(observation_client.with_trace_bridge(trace.clone()));
    let display_name = config.display_name.clone();
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
    let mut driver = DeliveryDriver::new(worker, client.clone(), delivery_config)
        .map_err(contract_error)?
        .with_workflows(client.clone())
        .with_observation_publisher(observation_client, display_name)
        .map_err(contract_error)?;
    if let Some(binding) = runtime_access.authenticated_binding {
        driver = driver
            .with_runtime_binding(binding.audience, binding.identity)
            .map_err(contract_error)?;
    }
    if let Some(binding) = runtime_access.effect_evidence {
        driver = driver
            .with_effect_evidence(binding.audience, binding.identity)
            .map_err(contract_error)?;
    }
    driver = driver
        .with_runtime_features(runtime_access.runtime_features)
        .map_err(contract_error)?;
    let poll_plan = config.poll_plan;
    driver = match &poll_plan {
        Some(file) if !broker_sources.is_empty() => driver
            .with_planned_sources(file.planner().clone(), broker_sources)
            .map_err(contract_error)?,
        Some(file) => driver
            .with_poll_planner(file.planner().clone())
            .map_err(contract_error)?,
        None => match broker_sources.pop_first() {
            Some((_, source)) => driver.with_acquisition_source(source),
            None => driver,
        },
    };
    let mut handle = driver.start();
    let supervise = supervise(
        &mut handle,
        output,
        signals,
        interrupted,
        poll_plan.is_some(),
    );
    if let Some(file) = poll_plan {
        tokio::select! {
            result = supervise => result,
            () = file.watch() => unreachable!("poll plan watch runs until supervision ends"),
        }
    } else {
        supervise.await
    }
}

async fn prepare_broker(
    config: &ConnectOptions,
    client: Arc<HttpTaskService>,
    signals: &mut ShutdownSignals,
    interrupted: &mut bool,
) -> Result<BTreeMap<String, Arc<dyn ledgence_worker_delivery::AcquisitionSource>>> {
    let Some(path) = config.delivery_config.clone() else {
        return Ok(BTreeMap::new());
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
        let queues = config.poll_plan.as_ref().map_or_else(
            || vec![config.queue.clone()],
            |p| p.planner().queues().to_vec(),
        );
        let mut loading = tokio::task::spawn_blocking(move || {
            let delivery = ledgence_adapter_sqs::deployment::DeliveryRoutes::load(&path)?;
            delivery.validate_worker(&scope, &queues)?;
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
        let connecting = delivery.connect();
        tokio::pin!(connecting);
        let routes = tokio::select! {
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
        let mut sources = BTreeMap::new();
        for (route, queue) in routes {
            let source = ledgence_worker_delivery::BrokerAcquisitionSource::new(
                Arc::new(queue),
                client.clone(),
            )
            .map_err(contract_error)?;
            sources.insert(
                route.queue,
                Arc::new(source) as Arc<dyn ledgence_worker_delivery::AcquisitionSource>,
            );
        }
        Ok(sources)
    }
}

async fn supervise(
    handle: &mut DeliveryHandle,
    output: &Sink,
    signals: &mut ShutdownSignals,
    interrupted: &mut bool,
    planned: bool,
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
    let mut delivery = json!({
        "session_id": status.session_id,
        "finished": status.finished,
        "settled_attempts": status.settled_attempts,
        "lost_attempts": status.lost_attempts
    });
    if planned {
        delivery["route_sessions"] = json!(status.route_sessions);
    }
    let report = output.line(json!({"delivery":delivery}).to_string());
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
    fn plan_selects_one_shared_pool_and_is_exclusive_with_single_route_options() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("plan.json");
        std::fs::write(&file, r#"{"schema_version":1,"revision":1,"strategy":"lottery","queues":[{"queue":"first","weight":0},{"queue":"second","weight":1}]}"#).unwrap();
        let mut supplied = options(None);
        supplied.insert("--poll-plan".into(), file.to_str().unwrap().into());
        assert!(
            ConnectOptions::parse(supplied.clone())
                .err()
                .unwrap()
                .to_string()
                .contains("exactly one")
        );
        supplied.remove("--queue");
        let config = ConnectOptions::parse(supplied.clone()).unwrap();
        assert_eq!(config.queue, "first");
        assert_eq!(
            config.poll_plan.unwrap().planner().queues(),
            ["first", "second"]
        );
        supplied.insert("--delivery-config".into(), "external.json".into());
        #[cfg(feature = "sqs")]
        {
            let parse = |args| ConnectOptions::parse(args).unwrap().acquire_wait;
            assert_eq!(parse(supplied.clone()), Duration::from_secs(1));
            for wait in [0, 500, 20_000] {
                let mut args = supplied.clone();
                args.insert("--acquire-wait-ms".into(), wait.to_string());
                assert_eq!(parse(args), Duration::from_millis(wait));
            }
            std::fs::write(&file,r#"{"schema_version":1,"revision":1,"strategy":"stride","queues":[{"queue":"one","weight":1}]}"#).unwrap();
            assert_eq!(parse(supplied), Duration::from_secs(20));
        }
        #[cfg(not(feature = "sqs"))]
        assert!(
            ConnectOptions::parse(supplied)
                .err()
                .unwrap()
                .to_string()
                .contains("sqs feature")
        );
        let mut missing = options(None);
        missing.remove("--queue");
        assert!(
            ConnectOptions::parse(missing)
                .err()
                .unwrap()
                .to_string()
                .contains("exactly one")
        );
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
    fn display_name_is_optional_explicit_and_bounded() {
        assert!(
            ConnectOptions::parse(options(None))
                .unwrap()
                .display_name
                .is_none()
        );
        let mut named = options(None);
        named.insert("--display-name".into(), "Billing worker".into());
        assert_eq!(
            ConnectOptions::parse(named)
                .unwrap()
                .display_name
                .as_deref(),
            Some("Billing worker")
        );
        for invalid in [String::new(), "x".repeat(129), "bad\nname".into()] {
            let mut supplied = options(None);
            supplied.insert("--display-name".into(), invalid);
            assert!(ConnectOptions::parse(supplied).is_err());
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
