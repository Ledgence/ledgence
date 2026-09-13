//! Network orchestration composition, including supervised expiry recovery.

mod command;
mod health;
mod logging;
mod recovery;

use command::Command;
use health::Health;
use ledgence_adapter_artifact::{ArtifactLimits, FileProgramStore, HttpProgramStore};
use ledgence_adapter_postgres::{PostgresOptions, PostgresStore};
use ledgence_orchestration_service::ApplicationService;
use ledgence_worker_api::ProgramStore;
use std::{future::IntoFuture, process::ExitCode, sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle};

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
    let mut logs = match logging::Logs::stderr() {
        Ok(logs) => logs,
        // Do not try a blocking fallback write to the same unavailable output.
        Err(_) => return ExitCode::FAILURE,
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(logs.sink.clone())
        .json()
        .init();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
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
    let (result, forced) = runtime.block_on(with_signals(command, &mut logs));
    if forced {
        runtime.block_on(async {
            if let Err(error) = &result {
                logs.sink.diagnostic(error);
            }
            let _ = tokio::time::timeout(Duration::from_millis(100), logs.finish()).await;
        });
        logs.abort();
        // A second explicit signal permits abandoning unresolved operations.
        // Their outcomes remain uncertain and clients must reconcile identities.
        runtime.shutdown_timeout(Duration::ZERO);
    } else {
        drop(runtime);
    }
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => ExitCode::FAILURE,
    }
}

async fn with_signals(command: Command, logs: &mut logging::Logs) -> (Result<(), String>, bool) {
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
    let work = dispatch(command, stopped);
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
    let drain = logs.finish();
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

async fn dispatch(command: Command, stopped: watch::Receiver<bool>) -> Result<(), String> {
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
    let result = if *stopped.borrow() {
        Ok(())
    } else {
        match command {
            Command::Migrate => {
                let result = store.migrate().await.map_err(|error| error.to_string());
                if result.is_ok() {
                    tracing::info!("database migrations completed");
                }
                result
            }
            Command::Serve {
                bind,
                store: programs,
            } => prepare_and_serve(store.clone(), bind, programs, stopped).await,
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
) -> Result<(), String> {
    store
        .verify_schema()
        .await
        .map_err(|error| error.to_string())?;
    store
        .check_connection()
        .await
        .map_err(|error| error.to_string())?;
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
    let service = Arc::new(ApplicationService::new(store.clone(), programs));
    let router =
        ledgence_adapter_http::server::router_with_admission(service, health.stopping.clone())
            .merge(health.router());
    let (http_stop, http_stopped) = watch::channel(false);
    let (recovery_stop, recovery_stopped) = watch::channel(false);
    let http = tokio::spawn(
        axum::serve(listener, router)
            .with_graceful_shutdown(stop_requested(http_stopped))
            .into_future(),
    );
    let scanner = tokio::spawn(recovery::run(
        store,
        health.clone(),
        recovery_stopped,
        config,
    ));
    tracing::info!(%address, "orchestration HTTP listener started");
    supervise(http, scanner, health, stopped, http_stop, recovery_stop).await
}

async fn stop_requested(mut stopped: watch::Receiver<bool>) {
    while !*stopped.borrow() {
        if stopped.changed().await.is_err() {
            break;
        }
    }
}

async fn supervise(
    mut http: JoinHandle<std::io::Result<()>>,
    mut scanner: JoinHandle<ledgence_orchestration_api::Result<()>>,
    health: Health,
    stopped: watch::Receiver<bool>,
    http_stop: watch::Sender<bool>,
    recovery_stop: watch::Sender<bool>,
) -> Result<(), String> {
    let mut http_finished = false;
    let mut scanner_finished = false;
    let mut failure = tokio::select! {
        _ = stop_requested(stopped) => None,
        finished = &mut http => {
            http_finished = true;
            Some(format!("HTTP server stopped unexpectedly: {finished:?}"))
        }
        finished = &mut scanner => {
            scanner_finished = true;
            Some(format!("recovery supervisor stopped unexpectedly: {finished:?}"))
        }
    };
    health.stop();
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
    if !scanner_finished {
        match observe_drain(&mut scanner, "expiry recovery").await {
            Ok(Ok(())) => {}
            result => {
                failure.get_or_insert_with(|| format!("recovery drain failed: {result:?}"));
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
            scanner,
            health,
            stopped,
            http_stop,
            recovery_stop,
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
        let result = supervise(http, scanner, health, stopped, http_stop, recovery_stop).await;
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
