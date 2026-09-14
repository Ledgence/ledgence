//! Real worker delivery against the application service and isolated PostgreSQL.

use super::*;
use crate::tests::{TestDb, command, scope};
use ledgence_adapter_artifact::{
    ArtifactLimits, FileArtifactCache, FileProgramStore, publish_directory,
};
use ledgence_adapter_subprocess::SubprocessRuntime;
use ledgence_orchestration_service::ApplicationService;
use ledgence_worker_api::{
    ExecutionReport, Platform, PortFuture, ProgramDescriptor, ProgramManifest, ProgramOutcome,
    ProgramRef, ProgramStore, PythonRuntime,
};
use ledgence_worker_core::{Worker, WorkerConfig};
use ledgence_worker_delivery::{DeliveryConfig, DeliveryDriver, DeliveryHandle};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

struct CountingPrograms {
    inner: FileProgramStore,
    fetches: AtomicUsize,
    resolutions: AtomicUsize,
}

impl ProgramStore for CountingPrograms {
    fn resolve<'a>(&'a self, program: &'a ProgramRef) -> PortFuture<'a, ProgramDescriptor> {
        self.resolutions.fetch_add(1, Ordering::SeqCst);
        self.inner.resolve(program)
    }

    fn fetch<'a>(&'a self, descriptor: &'a ProgramDescriptor) -> PortFuture<'a, Vec<u8>> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        self.inner.fetch(descriptor)
    }
}

struct Package {
    root: tempfile::TempDir,
    python: PathBuf,
    descriptor: ProgramDescriptor,
    programs: Arc<CountingPrograms>,
}

impl Package {
    fn publish() -> Self {
        let python = std::env::var_os("LEDGENCE_PYTHON")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("python3"));
        let output = std::process::Command::new(&python)
            .args([
                "-I", "-S", "-c",
                "import sys; assert sys.implementation.name == 'cpython' and sys.version_info >= (3,11); print(f'{sys.version_info.major}.{sys.version_info.minor}')",
            ])
            .output()
            .expect("set LEDGENCE_PYTHON to CPython >= 3.11");
        assert!(
            output.status.success(),
            "set LEDGENCE_PYTHON to CPython >= 3.11: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let version = String::from_utf8(output.stdout).unwrap().trim().to_owned();
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let store = root.path().join("store");
        std::fs::create_dir(&source).unwrap();
        let manifest = ProgramManifest {
            schema_version: 1,
            program: ProgramRef {
                id: "invoice".into(),
                version: "1.0.0".into(),
            },
            runtime: PythonRuntime {
                kind: "python".into(),
                python: version,
                protocol: 1,
            },
            handler: "program:handle".into(),
            platform: Platform {
                os: std::env::consts::OS.into(),
                arch: std::env::consts::ARCH.into(),
            },
        };
        std::fs::write(
            source.join("ledgence-program.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        std::fs::write(
            source.join("prepared_dependency.py"),
            "VALUE = 'packaged'\n",
        )
        .unwrap();
        std::fs::write(
            source.join("program.py"),
            "import os\nfrom prepared_dependency import VALUE\ndef handle(event):\n    with open(event['data']['marker'], 'a', encoding='utf-8') as output:\n        output.write(event['ldgattemptid'] + '\\n')\n    return {'pid': os.getpid(), 'dependency': VALUE, 'event': event}\n",
        )
        .unwrap();
        let limits = ArtifactLimits::default();
        let descriptor = publish_directory(&source, &store, &limits).unwrap();
        let programs = Arc::new(CountingPrograms {
            inner: FileProgramStore::new(store, limits).unwrap(),
            fetches: AtomicUsize::new(0),
            resolutions: AtomicUsize::new(0),
        });
        Self {
            root,
            python,
            descriptor,
            programs,
        }
    }

    fn worker(&self) -> Worker {
        Worker::new(
            WorkerConfig {
                concurrency: 1,
                fetch_timeout: Duration::from_secs(5),
            },
            self.programs.clone(),
            Arc::new(
                FileArtifactCache::new(self.root.path().join("cache"), ArtifactLimits::default())
                    .unwrap(),
            ),
            Arc::new(SubprocessRuntime::new(
                self.python.clone(),
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../sdk/python/ledgence_worker/bootstrap.py"),
            )),
        )
        .unwrap()
    }

    fn submission(&self, key: &str) -> SubmitCommand {
        let mut submission = command();
        submission.idempotency_key = key.into();
        submission.input.program = self.descriptor.program.clone();
        submission.input.data["marker"] = json!(self.root.path().join("invocations.txt"));
        submission
    }

    fn invocations(&self) -> Vec<String> {
        std::fs::read_to_string(self.root.path().join("invocations.txt"))
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

fn start(worker: &Worker, service: Arc<dyn TaskService>) -> DeliveryHandle {
    let mut config = DeliveryConfig::new(scope(), "python");
    config.idle_delay = Duration::from_millis(10);
    config.retry_delay = Duration::from_millis(10);
    config.request_timeout = Duration::from_secs(5);
    config.renew_interval = Duration::from_secs(1);
    config.session_extend_interval = Duration::from_secs(60);
    DeliveryDriver::new(worker.clone(), service, config)
        .unwrap()
        .start()
}

async fn succeeded(
    service: &dyn TaskService,
    handle: &DeliveryHandle,
    task: &TaskSnapshot,
    settled_count: u64,
) -> (AttemptSnapshot, ExecutionReport) {
    let after = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let after = service.inspect(&scope(), &task.task_id).await.unwrap();
            if after.state.is_terminal() && handle.status().settled_attempts >= settled_count {
                break after;
            }
            assert!(
                !handle.status().finished,
                "driver stopped: {:?}",
                handle.status()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("task did not settle: {:?}", handle.status()));
    assert_eq!(after.state, TaskState::Succeeded);
    assert_eq!(after.attempt_count, 1);
    let history = service.history(&scope(), &task.task_id, 0).await.unwrap();
    let attempt_id = history
        .iter()
        .find(|entry| entry.event.reason == TransitionReason::Claimed)
        .unwrap()
        .event
        .attempt_id
        .as_deref()
        .unwrap();
    let attempt = service
        .inspect_attempt(&scope(), &task.task_id, attempt_id)
        .await
        .unwrap();
    assert_eq!(attempt.state, AttemptState::Succeeded);
    assert_eq!(attempt.descriptor, task.descriptor);
    let report = match &attempt.settlement.as_ref().unwrap().command.report {
        AttemptReport::Completed(report) => report.clone(),
        other => panic!("unexpected report: {other:?}"),
    };
    let ProgramOutcome::Success { output } = &report.outcome else {
        panic!("unexpected outcome: {:?}", report.outcome);
    };
    assert_eq!(output["event"], *attempt.event.value());
    assert_eq!(output["event"]["data"], task.input.data);
    assert_eq!(output["dependency"], "packaged");
    assert_eq!(output["pid"], report.process_id);
    assert_eq!(report.context.digest, task.descriptor.digest);
    assert_eq!(
        report.context.identity.attempt_id,
        attempt.event.attempt_id()
    );
    for reason in [
        TransitionReason::Submitted,
        TransitionReason::Claimed,
        TransitionReason::DispatchAuthorized,
        TransitionReason::ReportAccepted,
        TransitionReason::Succeeded,
    ] {
        assert_eq!(
            history
                .iter()
                .filter(|entry| entry.event.reason == reason)
                .count(),
            1,
            "history must contain exactly one {reason:?}: {history:?}"
        );
    }
    assert!(
        history
            .windows(2)
            .all(|pair| pair[0].sequence + 1 == pair[1].sequence)
    );
    (attempt, report)
}

async fn stop(handle: &mut DeliveryHandle, worker: &Worker) {
    let status = handle.shutdown(Duration::from_secs(10)).await.unwrap();
    assert!(status.finished);
    assert_eq!(status.lost_attempts, 0);
    worker
        .shutdown(Duration::from_secs(1), Duration::from_secs(5))
        .await
        .unwrap();
    let stats = worker.stats().await;
    assert_eq!(stats.active_consumers, 0);
    assert_eq!(stats.process_slots, 0);
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 and CPython >= 3.11"]
async fn delivery_publishes_downloads_reuses_python_and_persists_full_event() {
    let db = TestDb::new().await;
    let package = Package::publish();
    let service = Arc::new(ApplicationService::new(
        Arc::new(db.store.clone()),
        package.programs.clone(),
    ));
    let worker = package.worker();
    let first = service.submit(&package.submission("first")).await.unwrap();
    let mut handle = start(&worker, service.clone());
    let (first_attempt, first_report) = succeeded(service.as_ref(), &handle, &first, 1).await;
    assert!(!first_report.reused_process);
    assert_eq!(worker.stats().await.warm_processes, 1);
    assert_eq!(package.programs.fetches.load(Ordering::SeqCst), 1);

    let second = service.submit(&package.submission("second")).await.unwrap();
    let (second_attempt, second_report) = succeeded(service.as_ref(), &handle, &second, 2).await;
    assert!(second_report.reused_process);
    assert_eq!(first_report.process_id, second_report.process_id);
    assert_ne!(first_attempt.event.id(), second_attempt.event.id());
    assert_eq!(package.programs.fetches.load(Ordering::SeqCst), 1);
    assert_eq!(package.programs.resolutions.load(Ordering::SeqCst), 2);
    assert_eq!(
        package.invocations(),
        [
            first_attempt.event.attempt_id(),
            second_attempt.event.attempt_id()
        ]
    );
    stop(&mut handle, &worker).await;
    db.finish().await;
}

#[derive(Default)]
struct LostReplies {
    acquire: Option<(AcquireCommand, Value)>,
    dispatch: Option<RenewCommand>,
    settlement: Option<(Vec<u8>, Timestamp)>,
    acquire_replayed: bool,
    dispatch_replayed: bool,
    settlement_replayed: bool,
}

/// Each injected failure happens after the real service has durably committed.
struct LoseReplies {
    inner: Arc<dyn TaskService>,
    lost: Mutex<LostReplies>,
}

impl LoseReplies {
    fn new(inner: Arc<dyn TaskService>) -> Self {
        Self {
            inner,
            lost: Mutex::new(LostReplies::default()),
        }
    }
}

impl TaskService for LoseReplies {
    fn open_session<'a>(
        &'a self,
        scope: &'a Scope,
        queue: &'a str,
        concurrency: u32,
    ) -> ContractFuture<'a, WorkerSession> {
        self.inner.open_session(scope, queue, concurrency)
    }
    fn extend_session<'a>(&'a self, session: &'a str) -> ContractFuture<'a, WorkerSession> {
        self.inner.extend_session(session)
    }
    fn submit<'a>(&'a self, command: &'a SubmitCommand) -> ContractFuture<'a, TaskSnapshot> {
        self.inner.submit(command)
    }
    fn status<'a>(&'a self, scope: &'a Scope, id: &'a str) -> ContractFuture<'a, TaskStatus> {
        self.inner.status(scope, id)
    }
    fn result<'a>(&'a self, scope: &'a Scope, id: &'a str) -> ContractFuture<'a, TaskResult> {
        self.inner.result(scope, id)
    }
    fn inspect<'a>(
        &'a self,
        scope: &'a Scope,
        task_id: &'a str,
    ) -> ContractFuture<'a, TaskSnapshot> {
        self.inner.inspect(scope, task_id)
    }
    fn inspect_attempt<'a>(
        &'a self,
        scope: &'a Scope,
        task_id: &'a str,
        attempt_id: &'a str,
    ) -> ContractFuture<'a, AttemptSnapshot> {
        self.inner.inspect_attempt(scope, task_id, attempt_id)
    }
    fn history<'a>(
        &'a self,
        scope: &'a Scope,
        task_id: &'a str,
        after: u64,
    ) -> ContractFuture<'a, Vec<RecordedHistoryEvent>> {
        self.inner.history(scope, task_id, after)
    }
    fn acquire<'a>(
        &'a self,
        command: &'a AcquireCommand,
        options: AcquireOptions,
    ) -> ContractFuture<'a, AcquireReply> {
        Box::pin(async move {
            let reply = self.inner.acquire(command, options).await?;
            let mut lost = self.lost.lock().unwrap();
            if let Some((original, assignment)) = &lost.acquire {
                if !lost.acquire_replayed {
                    assert_eq!(
                        command, original,
                        "uncertain acquisition must replay its cursor"
                    );
                    let AcquireReply::Assigned {
                        assignment: replay, ..
                    } = &reply
                    else {
                        panic!("uncertain acquisition changed disposition: {reply:?}");
                    };
                    // TTLs are freshly sampled; the immutable assignment identity is unchanged.
                    assert_eq!(
                        json!({"event": replay.event, "owner": replay.lease.owner, "descriptor": replay.descriptor}),
                        *assignment
                    );
                    lost.acquire_replayed = true;
                }
            } else if let AcquireReply::Assigned { assignment, .. } = &reply {
                lost.acquire = Some((
                    command.clone(),
                    json!({"event": assignment.event, "owner": assignment.lease.owner, "descriptor": assignment.descriptor}),
                ));
                return Err(ContractError::Unavailable(
                    "injected lost acquire reply after commit".into(),
                ));
            }
            Ok(reply)
        })
    }
    fn renew<'a>(&'a self, command: &'a RenewCommand) -> ContractFuture<'a, Authority> {
        Box::pin(async move {
            let reply = self.inner.renew(command).await?;
            let mut lost = self.lost.lock().unwrap();
            if let Some(original) = &lost.dispatch {
                if !lost.dispatch_replayed {
                    assert_eq!(
                        command, original,
                        "uncertain dispatch must replay its sequence"
                    );
                    assert!(reply.dispatch_allowed);
                    lost.dispatch_replayed = true;
                }
            } else if command.intent == RenewIntent::Dispatch {
                assert!(reply.dispatch_allowed);
                lost.dispatch = Some(command.clone());
                return Err(ContractError::Unavailable(
                    "injected lost dispatch reply after commit".into(),
                ));
            }
            Ok(reply)
        })
    }
    fn settle<'a>(&'a self, command: &'a SettleCommand) -> ContractFuture<'a, SettleReply> {
        Box::pin(async move {
            let reply = self.inner.settle(command).await?;
            let mut lost = self.lost.lock().unwrap();
            let encoded = serde_json::to_vec(command).unwrap();
            if let Some((original, accepted_at)) = &lost.settlement {
                if !lost.settlement_replayed {
                    assert_eq!(
                        &encoded, original,
                        "settlement retry must preserve the entire command"
                    );
                    assert_eq!(reply.receipt.accepted_at, *accepted_at);
                    assert!(reply.already_accepted);
                    lost.settlement_replayed = true;
                }
            } else {
                assert!(!reply.already_accepted);
                lost.settlement = Some((encoded, reply.receipt.accepted_at));
                return Err(ContractError::Unavailable(
                    "injected lost settlement reply after commit".into(),
                ));
            }
            Ok(reply)
        })
    }
    fn confirm_quiescence<'a>(&'a self, owner: &'a LeaseOwner) -> ContractFuture<'a, TaskState> {
        self.inner.confirm_quiescence(owner)
    }
    fn cancel<'a>(&'a self, scope: &'a Scope, task_id: &'a str) -> ContractFuture<'a, TaskState> {
        self.inner.cancel(scope, task_id)
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 and CPython >= 3.11"]
async fn delivery_reconciles_lost_replies_and_restarts_with_persisted_binding_and_cache() {
    let mut db = TestDb::new().await;
    let package = Package::publish();
    let service = Arc::new(ApplicationService::new(
        Arc::new(db.store.clone()),
        package.programs.clone(),
    ));
    let lossy = Arc::new(LoseReplies::new(service.clone()));
    let worker = package.worker();
    let submission = package.submission("lost-replies");
    let first = service.submit(&submission).await.unwrap();
    let mut handle = start(&worker, lossy.clone());
    let (attempt, first_report) = succeeded(service.as_ref(), &handle, &first, 1).await;
    {
        let lost = lossy.lost.lock().unwrap();
        assert!(lost.acquire_replayed);
        assert!(lost.dispatch_replayed);
        assert!(lost.settlement_replayed);
    }
    assert_eq!(package.invocations(), [attempt.event.attempt_id()]);
    stop(&mut handle, &worker).await;
    drop(handle);
    drop(worker);
    drop(lossy);

    // The next task is accepted before service/worker restart. Execution must
    // use this durable digest without resolving a mutable release label again.
    let queued_submission = package.submission("queued-before-restart");
    let queued = service.submit(&queued_submission).await.unwrap();
    drop(service);
    db.store.close().await;
    db.store = PostgresStore::connect(&db.url, PostgresOptions::default())
        .await
        .unwrap();
    let service = Arc::new(ApplicationService::new(
        Arc::new(db.store.clone()),
        package.programs.clone(),
    ));
    std::fs::remove_file(
        package
            .root
            .path()
            .join("store/programs/invoice/1.0.0/descriptor.json"),
    )
    .unwrap();
    std::fs::remove_file(package.root.path().join(format!(
        "store/blobs/{}.zip",
        package.descriptor.digest.hex()
    )))
    .unwrap();
    let replay = service.submit(&submission).await.unwrap();
    assert_eq!(replay.task_id, first.task_id);
    assert_eq!(replay.descriptor, first.descriptor);
    assert_eq!(replay.state, TaskState::Succeeded);
    let queued_replay = service.submit(&queued_submission).await.unwrap();
    assert_eq!(queued_replay.task_id, queued.task_id);
    assert_eq!(queued_replay.descriptor, queued.descriptor);
    assert_eq!(package.programs.resolutions.load(Ordering::SeqCst), 2);
    let worker = package.worker();
    let mut handle = start(&worker, service.clone());
    let (restarted_attempt, restarted_report) =
        succeeded(service.as_ref(), &handle, &queued, 1).await;
    assert!(!restarted_report.reused_process);
    assert_ne!(restarted_report.process_id, first_report.process_id);
    assert_eq!(package.programs.fetches.load(Ordering::SeqCst), 1);
    assert_eq!(package.programs.resolutions.load(Ordering::SeqCst), 2);
    assert_eq!(
        package.invocations(),
        [
            attempt.event.attempt_id(),
            restarted_attempt.event.attempt_id()
        ]
    );
    stop(&mut handle, &worker).await;
    db.finish().await;
}
