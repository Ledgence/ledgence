//! Network orchestration composition, including supervised expiry recovery.

mod application;
mod command;
mod completion;
mod health;
mod logging;
#[cfg(feature = "sqs")]
mod publication;
mod recovery;
mod telemetry;
mod workflow;

use command::Command;
use health::Health;
use ledgence_adapter_artifact::{ArtifactLimits, FileProgramStore, HttpProgramStore};
use ledgence_adapter_postgres::{PostgresOptions, PostgresStore};
use ledgence_orchestration_service::ApplicationService;
use ledgence_worker_api::{ProgramStore, TraceBridge};
use std::{
    future::{Future, IntoFuture},
    process::ExitCode,
    sync::Arc,
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};
use tracing::instrument::WithSubscriber;

const DRAIN_OBSERVATION: Duration = Duration::from_secs(35);

fn main() -> ExitCode {
    let command = match command::parse(std::env::args().skip(1)) {
        Ok(Command::Help) => {
            print!("{}", command::HELP);
            return ExitCode::SUCCESS;
        }
        Ok(command) => command,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    run_with_bridge(move |stopped, trace| dispatch(command, stopped, trace))
}

/// Only signal handling and log observation run on this runtime. Application
/// work owns a separate runtime so its destruction cannot block force signals.
fn run_with_bridge<F>(
    work: impl FnOnce(watch::Receiver<bool>, Arc<dyn TraceBridge>) -> F + Send + 'static,
) -> ExitCode
where
    F: Future<Output = Result<(), String>>,
{
    let mut logs = match logging::Logs::stderr() {
        Ok(logs) => logs,
        // Do not try a blocking fallback write to the same unavailable output.
        Err(_) => return ExitCode::FAILURE,
    };
    let telemetry = match telemetry::Telemetry::start("ledgence-orchestrator", logs.sink.clone()) {
        Ok(telemetry) => telemetry,
        Err(error) => {
            logs.sink.diagnostic(&error);
            if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                runtime.block_on(async {
                    let _ = tokio::time::timeout(Duration::from_secs(1), logs.finish()).await;
                });
            }
            return ExitCode::FAILURE;
        }
    };
    let trace = telemetry.bridge();
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            logs.sink
                .diagnostic(&format!("could not start runtime: {error}"));
            return ExitCode::FAILURE;
        }
    };
    let (result, forced) = runtime.block_on(with_signals(
        move |stopped| work(stopped, trace),
        &mut logs,
        telemetry,
    ));
    if forced {
        runtime.block_on(async {
            if let Err(error) = &result {
                logs.sink.diagnostic(error);
            }
            let _ = tokio::time::timeout(Duration::from_millis(100), logs.finish()).await;
        });
        logs.abort();
        // The application thread was detached only after an explicit second
        // signal. Process exit abandons its unresolved work; clients reconcile
        // uncertain outcomes using their durable operation identities.
        runtime.shutdown_timeout(Duration::ZERO);
    } else {
        drop(runtime);
    }
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}

async fn with_signals<F>(
    work: impl FnOnce(watch::Receiver<bool>) -> F + Send + 'static,
    logs: &mut logging::Logs,
    telemetry: telemetry::Telemetry,
) -> (Result<(), String>, bool)
where
    F: Future<Output = Result<(), String>>,
{
    let mut signals = match Signals::new() {
        Ok(signals) => signals,
        Err(error) => {
            logs.sink
                .diagnostic(&format!("could not install shutdown signals: {error}"));
            let _ = logs.finish().await;
            return (
                Err(format!("could not install shutdown signals: {error}")),
                false,
            );
        }
    };
    let (stop, stopped) = watch::channel(false);
    // Completion includes runtime destruction, not just dispatch. A timed-out
    // HTTP waiter can leave a started filesystem operation running there.
    let application = application::Application::start(move || work(stopped));
    let work = async move {
        application
            .map_err(|error| format!("could not start application thread: {error}"))?
            .finish()
            .await
    };
    tokio::pin!(work);
    let mut interrupted = false;
    let result = loop {
        tokio::select! {
            biased;
            signal = signals.recv() => {
                if let Err(error) = signal {
                    // A broken signal subscription cannot safely provide the
                    // promised drain/force lifecycle. Request ordinary drain.
                    tracing::error!(%error, "shutdown signal stream failed; draining");
                    let _ = stop.send(true);
                    let result = work.await.and(Err("shutdown signal stream failed".into()));
                    let _ = logs.finish().await;
                    return (result, false);
                }
                if interrupted {
                    return (Err("forced exit requested; in-flight operations may have committed and require reconciliation".into()), true);
                }
                interrupted = true;
                let _ = stop.send(true);
                tracing::info!("shutdown requested; retaining in-flight operations until drained");
            }
            result = &mut work => break result,
        }
    };
    if let Err(error) = &result {
        logs.sink.diagnostic(error);
    }
    // Signal subscriptions remain active after HTTP/recovery stop, including
    // a genuinely blocked filesystem writer that cannot meet its deadline.
    let drain = async {
        telemetry.finish().await;
        logs.finish().await
    };
    tokio::pin!(drain);
    let output = loop {
        tokio::select! {
            biased;
            signal = signals.recv() => {
                if let Err(error) = signal {
                    return (Err(format!("shutdown signal stream failed: {error}")), false);
                }
                if interrupted {
                    return (Err("forced exit requested with stderr delivery unresolved".into()), true);
                }
                interrupted = true;
            }
            finished = &mut drain => break finished.map_err(|error| error.to_string()),
        }
    };
    (result.and(output), false)
}

async fn dispatch(
    command: Command,
    stopped: watch::Receiver<bool>,
    trace: Arc<dyn TraceBridge>,
) -> Result<(), String> {
    let url = std::env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL must contain a PostgreSQL 18 connection URL".to_owned())?;
    let options = PostgresOptions::default();
    let store = tokio::time::timeout(
        options.operation_timeout,
        PostgresStore::connect(&url, options),
    )
    .await
    .map_err(|_| "database connection timed out".to_owned())?
    .map_err(|error| error.to_string())?;
    let store = store.with_trace_bridge(trace.clone());
    let result = if *stopped.borrow() {
        Ok(())
    } else {
        match command {
            Command::Migrate { options } => {
                let result = store
                    .migrate_with_options(options)
                    .await
                    .map_err(|error| error.to_string());
                if result.is_ok() {
                    tracing::info!("database migrations completed");
                }
                result
            }
            Command::Serve {
                bind,
                store: programs,
                delivery_config,
                completion_config,
            } => {
                prepare_and_serve(
                    store.clone(),
                    bind,
                    programs,
                    stopped,
                    trace,
                    &url,
                    (delivery_config, completion_config),
                )
                .await
            }
            Command::Help => unreachable!("help is handled before runtime startup"),
        }
    };
    // The serving composition retains HTTP and recovery tasks through shutdown
    // before returning here. Pool closure waits for checked-out connections.
    store.close().await;
    result
}

async fn prepare_and_serve(
    store: PostgresStore,
    bind: std::net::SocketAddr,
    location: String,
    stopped: watch::Receiver<bool>,
    trace: Arc<dyn TraceBridge>,
    database_url: &str,
    config_paths: (Option<std::path::PathBuf>, Option<std::path::PathBuf>),
) -> Result<(), String> {
    let (delivery_config, completion_config) = config_paths;
    store
        .verify_schema()
        .await
        .map_err(|error| error.to_string())?;
    store
        .check_connection()
        .await
        .map_err(|error| error.to_string())?;
    let completion_destinations = match completion_config {
        Some(path) => {
            let config = tokio::task::spawn_blocking(move || {
                ledgence_adapter_http::completion::CompletionConfig::load(&path)
            })
            .await
            .map_err(|_| "completion configuration loading failed".to_owned())?
            .map_err(|error| error.to_string())?;
            let mut destinations = Vec::with_capacity(config.destinations.len());
            for configured in config.destinations {
                let binding = configured.destination.clone();
                let sender =
                    ledgence_adapter_http::completion::HttpCompletionSender::new(configured)
                        .map_err(|error| error.to_string())?
                        .with_trace_bridge(trace.clone());
                destinations.push(completion::Destination {
                    binding,
                    sender: Arc::new(sender),
                });
            }
            destinations
        }
        None => Vec::new(),
    };
    #[cfg(feature = "sqs")]
    let broker = match delivery_config {
        Some(path) => {
            let config = tokio::task::spawn_blocking(move || {
                ledgence_adapter_sqs::deployment::DeliveryConfig::load(&path)
            })
            .await
            .map_err(|_| "delivery configuration loading failed".to_owned())?
            .map_err(|error| error.to_string())?;
            if *stopped.borrow() {
                return Ok(());
            }
            // Validate external capabilities without changing durable routing.
            let queue = Arc::new(
                ledgence_adapter_sqs::SqsQueue::connect(config.sqs)
                    .await
                    .map_err(|error| error.to_string())?,
            );
            if *stopped.borrow() {
                return Ok(());
            }
            Some((queue, config.route))
        }
        None => None,
    };
    #[cfg(not(feature = "sqs"))]
    if delivery_config.is_some() {
        return Err("--delivery-config requires a binary built with the sqs feature".into());
    }
    let programs = tokio::task::spawn_blocking(move || -> Result<Arc<dyn ProgramStore>, String> {
        let limits = ArtifactLimits::default();
        if location.starts_with("http://") || location.starts_with("https://") {
            HttpProgramStore::new(&location, limits)
                .map(|store| Arc::new(store) as Arc<dyn ProgramStore>)
                .map_err(|error| error.to_string())
        } else {
            FileProgramStore::new(&location, limits)
                .map(|store| Arc::new(store) as Arc<dyn ProgramStore>)
                .map_err(|error| error.to_string())
        }
    })
    .await
    .map_err(|error| format!("program-store setup failed: {error}"))??;
    if *stopped.borrow() {
        return Ok(());
    }
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|error| format!("could not bind HTTP listener: {error}"))?;
    let address = listener.local_addr().map_err(|error| error.to_string())?;
    let config = recovery::RecoveryConfig::default();
    let health = Health::new(config.freshness());
    health.prerequisites_ready();
    let store = Arc::new(store);
    let service = Arc::new(
        ApplicationService::new(store.clone(), programs)
            .with_workflows(store.clone())
            .with_completions(store.clone()),
    );
    store.set_acquisition_wake(service.acquisition_wake());
    let notifications = if notifications_enabled()? {
        let url = std::env::var("LEDGENCE_POSTGRES_NOTIFICATION_URL")
            .unwrap_or_else(|_| database_url.to_owned());
        Some(
            store
                .start_acquisition_notifications(&url)
                .map_err(|error| error.to_string())?,
        )
    } else {
        None
    };
    // Route activation is durable: do not change a live queue until every
    // locally fallible prerequisite, including binding and notification setup,
    // has succeeded. Retain notifications through activation failure or stop.
    let result = async {
        if *stopped.borrow() {
            return Ok(());
        }
        for destination in &completion_destinations {
            use ledgence_orchestration_api::CompletionStore;
            store
                .configure_completion_destination(&destination.binding)
                .await
                .map_err(|error| error.to_string())?;
        }
        #[cfg(feature = "sqs")]
        if let Some((_, route)) = &broker {
            use ledgence_orchestration_api::DispatchIntentStore;
            store
                .configure_route(route)
                .await
                .map_err(|error| error.to_string())?;
        }
        // Complete an issued activation before observing stop. Its successful
        // binding remains durable even if shutdown arrived during the write.
        if *stopped.borrow() {
            return Ok(());
        }
        let router = ledgence_adapter_http::server::router_with_completions(
            service.clone(),
            service.clone(),
            service.clone(),
            health.stopping.clone(),
            trace,
        )
        .merge(health.router());
        let (http_stop, http_stopped) = watch::channel(false);
        let (recovery_stop, recovery_stopped) = watch::channel(false);
        let http = tokio::spawn(
            axum::serve(listener, router)
                .with_graceful_shutdown(stop_requested(http_stopped))
                .into_future()
                .with_current_subscriber(),
        );
        #[cfg(feature = "sqs")]
        let publisher = broker.map(|(queue, route)| {
            health.require_publication();
            let checked_queue = queue.clone();
            let probe: publication::ConfigurationProbe = Arc::new(move || {
                let queue = checked_queue.clone();
                Box::pin(async move { queue.check_configuration().await })
            });
            let (stop, stopped) = watch::channel(false);
            let task = tokio::spawn(
                publication::run(store.clone(), queue, route, probe, health.clone(), stopped)
                    .with_current_subscriber(),
            );
            BackgroundTask { task, stop }
        });
        #[cfg(not(feature = "sqs"))]
        let publisher = None;
        let completion_task = if completion_destinations.is_empty() {
            None
        } else {
            health.require_completions();
            let (stop, stopped) = watch::channel(false);
            Some(BackgroundTask {
                task: tokio::spawn(
                    completion::run(
                        store.clone(),
                        completion_destinations,
                        health.clone(),
                        stopped,
                    )
                    .with_current_subscriber(),
                ),
                stop,
            })
        };
        health.require_workflows();
        let (workflow_stop, workflow_stopped) = watch::channel(false);
        let workflow_task = Some(BackgroundTask {
            task: tokio::spawn(
                workflow::run(service.clone(), health.clone(), workflow_stopped)
                    .with_current_subscriber(),
            ),
            stop: workflow_stop,
        });
        let scanner = tokio::spawn(
            recovery::run(store, health.clone(), recovery_stopped, config)
                .with_current_subscriber(),
        );
        tracing::info!(%address, "orchestration HTTP listener started");
        let acquisition_service = service.clone();
        supervise(
            http,
            BackgroundTasks {
                scanner,
                publisher,
                workflow: workflow_task,
                completion: completion_task,
            },
            health,
            stopped,
            http_stop,
            recovery_stop,
            move || acquisition_service.stop_acquisitions(),
        )
        .await
    }
    .await;
    tracing::info!(statistics = ?service.acquisition_statistics(), "acquisition coordinator drained");
    // Accepted requests and recovery retain their local wake sink until drained.
    // Auxiliary connection cleanup runs before the lifecycle pool is closed.
    if let Some(notifications) = notifications {
        let statistics = notifications.shutdown().await;
        tracing::info!(?statistics, "acquisition notifications stopped");
    }
    result
}

fn notifications_enabled() -> Result<bool, String> {
    match std::env::var("LEDGENCE_POSTGRES_NOTIFICATIONS") {
        Err(std::env::VarError::NotPresent) => Ok(true),
        Ok(value) if value == "on" => Ok(true),
        Ok(value) if value == "off" => Ok(false),
        _ => Err("LEDGENCE_POSTGRES_NOTIFICATIONS must be on or off".into()),
    }
}

async fn stop_requested(mut stopped: watch::Receiver<bool>) {
    while !*stopped.borrow() {
        if stopped.changed().await.is_err() {
            break;
        }
    }
}

struct BackgroundTask {
    task: JoinHandle<ledgence_orchestration_api::Result<()>>,
    stop: watch::Sender<bool>,
}

struct BackgroundTasks {
    scanner: JoinHandle<ledgence_orchestration_api::Result<()>>,
    publisher: Option<BackgroundTask>,
    workflow: Option<BackgroundTask>,
    completion: Option<BackgroundTask>,
}

async fn supervise(
    mut http: JoinHandle<std::io::Result<()>>,
    background: BackgroundTasks,
    health: Health,
    stopped: watch::Receiver<bool>,
    http_stop: watch::Sender<bool>,
    recovery_stop: watch::Sender<bool>,
    stop_acquisitions: impl FnOnce(),
) -> Result<(), String> {
    let BackgroundTasks {
        mut scanner,
        mut publisher,
        mut workflow,
        mut completion,
    } = background;
    let mut http_finished = false;
    let mut scanner_finished = false;
    let mut publisher_finished = false;
    let mut workflow_finished = false;
    let mut completion_finished = false;
    let mut failure = tokio::select! {
        _ = stop_requested(stopped) => None,
        finished = &mut http => {
            http_finished = true;
            Some(format!("HTTP server stopped unexpectedly: {finished:?}"))
        }
        finished = async {
            match &mut completion {
                Some(dispatcher) => (&mut dispatcher.task).await,
                None => std::future::pending().await,
            }
        } => {
            completion_finished = true;
            Some(format!("completion supervisor stopped unexpectedly: {finished:?}"))
        }
        finished = &mut scanner => {
            scanner_finished = true;
            Some(format!("recovery supervisor stopped unexpectedly: {finished:?}"))
        }
        finished = async {
            match &mut workflow {
                Some(coordinator) => (&mut coordinator.task).await,
                None => std::future::pending().await,
            }
        } => {
            workflow_finished = true;
            Some(format!("workflow supervisor stopped unexpectedly: {finished:?}"))
        }
        finished = async {
            match &mut publisher {
                Some(publication) => (&mut publication.task).await,
                None => std::future::pending().await,
            }
        } => {
            publisher_finished = true;
            Some(format!("publication supervisor stopped unexpectedly: {finished:?}"))
        }
    };
    health.stop();
    stop_acquisitions();
    let _ = http_stop.send(true);
    if !http_finished {
        match observe_drain(&mut http, "HTTP requests").await {
            Ok(Ok(())) => {}
            result => {
                failure.get_or_insert_with(|| format!("HTTP drain failed: {result:?}"));
            }
        }
    }
    // Recovery continues while already accepted requests drain. Once no HTTP
    // operation remains, stop scanning between bounded batches before DB close.
    let _ = recovery_stop.send(true);
    if let Some(dispatcher) = &completion {
        let _ = dispatcher.stop.send(true);
    }
    if let Some(coordinator) = &workflow {
        let _ = coordinator.stop.send(true);
    }
    if let Some(publication) = &publisher {
        let _ = publication.stop.send(true);
    }
    if !scanner_finished {
        match observe_drain(&mut scanner, "expiry recovery").await {
            Ok(Ok(())) => {}
            result => {
                failure.get_or_insert_with(|| format!("recovery drain failed: {result:?}"));
            }
        }
    }
    if let Some(publication) = &mut publisher
        && !publisher_finished
    {
        match observe_drain(&mut publication.task, "dispatch publication").await {
            Ok(Ok(())) => {}
            result => {
                failure.get_or_insert_with(|| format!("publication drain failed: {result:?}"));
            }
        }
    }
    if let Some(coordinator) = &mut workflow
        && !workflow_finished
    {
        match observe_drain(&mut coordinator.task, "workflow recovery").await {
            Ok(Ok(())) => {}
            result => {
                failure.get_or_insert_with(|| format!("workflow drain failed: {result:?}"));
            }
        }
    }
    if let Some(dispatcher) = &mut completion
        && !completion_finished
    {
        match observe_drain(&mut dispatcher.task, "completion delivery").await {
            Ok(Ok(())) => {}
            result => {
                failure.get_or_insert_with(|| format!("completion drain failed: {result:?}"));
            }
        }
    }
    failure.map_or(Ok(()), Err)
}

async fn observe_drain<T>(
    task: &mut JoinHandle<T>,
    component: &str,
) -> Result<T, tokio::task::JoinError> {
    loop {
        match tokio::time::timeout(DRAIN_OBSERVATION, &mut *task).await {
            Ok(result) => return result,
            Err(_) => tracing::warn!(
                component,
                "shutdown still pending; retaining operation ownership; a second signal forces exit"
            ),
        }
    }
}

struct Signals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl Signals {
    fn new() -> std::io::Result<Self> {
        Ok(Self {
            #[cfg(unix)]
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
            #[cfg(unix)]
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
        })
    }

    async fn recv(&mut self) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            let received = tokio::select! {
                received = self.interrupt.recv() => received,
                received = self.terminate.recv() => received,
            };
            received.ok_or_else(|| std::io::Error::other("signal stream closed"))
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test(start_paused = true)]
    async fn shutdown_retains_http_and_stops_recovery_only_after_requests_finish() {
        let health = Health::new(Duration::from_secs(40));
        let observed_health = health.clone();
        let (stop, stopped) = watch::channel(false);
        let (http_stop, mut http_stopped) = watch::channel(false);
        let (recovery_stop, mut recovery_stopped) = watch::channel(false);
        let (http_finish, http_finished) = oneshot::channel();
        let (scanner_finish, scanner_finished) = oneshot::channel();
        let http = tokio::spawn(async move {
            http_finished.await.unwrap();
            Ok(())
        });
        let scanner = tokio::spawn(async move {
            scanner_finished.await.unwrap();
            Ok(())
        });
        let supervisor = tokio::spawn(supervise(
            http,
            BackgroundTasks {
                workflow: None,
                completion: None,
                scanner,
                publisher: None,
            },
            health,
            stopped,
            http_stop,
            recovery_stop,
            || {},
        ));
        stop.send(true).unwrap();
        http_stopped.changed().await.unwrap();
        assert!(*http_stopped.borrow());
        assert!(
            observed_health
                .stopping
                .load(std::sync::atomic::Ordering::Acquire)
        );
        assert!(!*recovery_stopped.borrow());
        tokio::time::advance(Duration::from_secs(36)).await;
        assert!(
            !supervisor.is_finished(),
            "observation threshold cannot cancel HTTP handlers"
        );
        assert!(!*recovery_stopped.borrow());
        http_finish.send(()).unwrap();
        recovery_stopped.changed().await.unwrap();
        assert!(*recovery_stopped.borrow());
        assert!(!supervisor.is_finished());
        scanner_finish.send(()).unwrap();
        supervisor.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn scanner_panic_drains_http_and_returns_explicit_failure() {
        let health = Health::new(Duration::from_secs(40));
        let observed_health = health.clone();
        let (_stop, stopped) = watch::channel(false);
        let (http_stop, http_stopped) = watch::channel(false);
        let (recovery_stop, _recovery_stopped) = watch::channel(false);
        let http = tokio::spawn(async move {
            stop_requested(http_stopped).await;
            Ok(())
        });
        let scanner = tokio::spawn(async move {
            panic!("controlled recovery supervisor panic");
            #[allow(unreachable_code)]
            Ok(())
        });
        let result = supervise(
            http,
            BackgroundTasks {
                workflow: None,
                completion: None,
                scanner,
                publisher: None,
            },
            health,
            stopped,
            http_stop,
            recovery_stop,
            || {},
        )
        .await;
        assert!(
            result
                .unwrap_err()
                .contains("recovery supervisor stopped unexpectedly")
        );
        assert!(
            observed_health
                .stopping
                .load(std::sync::atomic::Ordering::Acquire)
        );
    }
}

#[cfg(all(test, unix))]
mod shutdown_tests;

#[cfg(test)]
fn run<F>(work: impl FnOnce(watch::Receiver<bool>) -> F + Send + 'static) -> ExitCode
where
    F: Future<Output = Result<(), String>>,
{
    run_with_bridge(move |stopped, _| work(stopped))
}

#[cfg(test)]
mod publication_supervision_tests {
    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test(start_paused = true)]
    async fn publication_runs_through_http_drain_then_retains_its_inflight_batch() {
        let health = Health::new(Duration::from_secs(40));
        let (stop, stopped) = watch::channel(false);
        let (http_stop, mut http_stopped) = watch::channel(false);
        let (recovery_stop, recovery_stopped) = watch::channel(false);
        let (publication_stop, mut publication_stopped) = watch::channel(false);
        let (http_finish, http_finished) = oneshot::channel();
        let (publication_finish, publication_finished) = oneshot::channel();
        let http = tokio::spawn(async move {
            http_finished.await.unwrap();
            Ok(())
        });
        let scanner = tokio::spawn(async move {
            stop_requested(recovery_stopped).await;
            Ok(())
        });
        let publisher = Some(BackgroundTask {
            stop: publication_stop,
            task: tokio::spawn(async move {
                publication_finished.await.unwrap();
                Ok(())
            }),
        });
        let task = tokio::spawn(supervise(
            http,
            BackgroundTasks {
                scanner,
                publisher,
                workflow: None,
                completion: None,
            },
            health,
            stopped,
            http_stop,
            recovery_stop,
            || {},
        ));
        stop.send(true).unwrap();
        http_stopped.changed().await.unwrap();
        assert!(!*publication_stopped.borrow());
        tokio::time::advance(Duration::from_secs(36)).await;
        assert!(!task.is_finished());
        assert!(!*publication_stopped.borrow());
        http_finish.send(()).unwrap();
        publication_stopped.changed().await.unwrap();
        assert!(*publication_stopped.borrow());
        assert!(!task.is_finished());
        publication_finish.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn publication_failure_and_panic_stop_http_and_recovery_with_explicit_failure() {
        for panic in [false, true] {
            let health = Health::new(Duration::from_secs(40));
            let observed = health.clone();
            let (_stop, stopped) = watch::channel(false);
            let (http_stop, http_stopped) = watch::channel(false);
            let (recovery_stop, recovery_stopped) = watch::channel(false);
            let (publication_stop, _publication_stopped) = watch::channel(false);
            let http = tokio::spawn(async move {
                stop_requested(http_stopped).await;
                Ok(())
            });
            let scanner = tokio::spawn(async move {
                stop_requested(recovery_stopped).await;
                Ok(())
            });
            let publisher = Some(BackgroundTask {
                stop: publication_stop,
                task: tokio::spawn(async move {
                    assert!(!panic, "controlled publication panic");
                    Err(ledgence_orchestration_api::ContractError::InvalidInput(
                        "controlled publication error".into(),
                    ))
                }),
            });
            let result = supervise(
                http,
                BackgroundTasks {
                    scanner,
                    publisher,
                    workflow: None,
                    completion: None,
                },
                health,
                stopped,
                http_stop,
                recovery_stop,
                || {},
            )
            .await;
            assert!(
                result
                    .unwrap_err()
                    .contains("publication supervisor stopped unexpectedly")
            );
            assert!(observed.stopping.load(std::sync::atomic::Ordering::Acquire));
        }
    }
}

#[cfg(test)]
mod completion_supervision_tests {
    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test(start_paused = true)]
    async fn completion_dispatch_continues_through_http_drain_and_retains_current_sends() {
        let health = Health::new(Duration::from_secs(40));
        let (stop, stopped) = watch::channel(false);
        let (http_stop, mut http_stopped) = watch::channel(false);
        let (recovery_stop, recovery_stopped) = watch::channel(false);
        let (completion_stop, mut completion_stopped) = watch::channel(false);
        let (http_finish, http_finished) = oneshot::channel();
        let (completion_finish, completion_finished) = oneshot::channel();
        let http = tokio::spawn(async move {
            http_finished.await.unwrap();
            Ok(())
        });
        let scanner = tokio::spawn(async move {
            stop_requested(recovery_stopped).await;
            Ok(())
        });
        let task = tokio::spawn(supervise(
            http,
            BackgroundTasks {
                scanner,
                publisher: None,
                workflow: None,
                completion: Some(BackgroundTask {
                    stop: completion_stop,
                    task: tokio::spawn(async move {
                        completion_finished.await.unwrap();
                        Ok(())
                    }),
                }),
            },
            health,
            stopped,
            http_stop,
            recovery_stop,
            || {},
        ));
        stop.send(true).unwrap();
        http_stopped.changed().await.unwrap();
        assert!(!*completion_stopped.borrow());
        tokio::time::advance(Duration::from_secs(36)).await;
        assert!(!task.is_finished());
        assert!(!*completion_stopped.borrow());
        http_finish.send(()).unwrap();
        completion_stopped.changed().await.unwrap();
        assert!(*completion_stopped.borrow());
        assert!(!task.is_finished());
        completion_finish.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn completion_dispatch_failure_and_panic_drain_other_components() {
        for panic in [false, true] {
            let health = Health::new(Duration::from_secs(40));
            let observed = health.clone();
            let (_stop, stopped) = watch::channel(false);
            let (http_stop, http_stopped) = watch::channel(false);
            let (recovery_stop, recovery_stopped) = watch::channel(false);
            let (completion_stop, _completion_stopped) = watch::channel(false);
            let http = tokio::spawn(async move {
                stop_requested(http_stopped).await;
                Ok(())
            });
            let scanner = tokio::spawn(async move {
                stop_requested(recovery_stopped).await;
                Ok(())
            });
            let result = supervise(
                http,
                BackgroundTasks {
                    scanner,
                    publisher: None,
                    workflow: None,
                    completion: Some(BackgroundTask {
                        stop: completion_stop,
                        task: tokio::spawn(async move {
                            assert!(!panic, "controlled completion supervisor panic");
                            Err(ledgence_orchestration_api::ContractError::InvalidInput(
                                "controlled failure".into(),
                            ))
                        }),
                    }),
                },
                health,
                stopped,
                http_stop,
                recovery_stop,
                || {},
            )
            .await;
            assert!(
                result
                    .unwrap_err()
                    .contains("completion supervisor stopped unexpectedly")
            );
            assert!(observed.stopping.load(std::sync::atomic::Ordering::Acquire));
        }
    }
}
