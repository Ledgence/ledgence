use super::*;
use ledgence_orchestration_core as core;
use ledgence_worker_api::{Digest, PortFuture, ProgramDescriptor, ProgramRef};
use serde_json::json;
use std::{
    future::Future,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

type ResolveReply = oneshot::Sender<ledgence_worker_api::Result<ProgramDescriptor>>;

struct ControlledPrograms {
    requests: mpsc::UnboundedSender<(ProgramRef, ResolveReply)>,
}

impl ProgramStore for ControlledPrograms {
    fn resolve<'a>(&'a self, program: &'a ProgramRef) -> PortFuture<'a, ProgramDescriptor> {
        Box::pin(async move {
            let (reply, response) = oneshot::channel();
            self.requests.send((program.clone(), reply)).unwrap();
            response.await.unwrap()
        })
    }

    fn fetch<'a>(&'a self, _: &'a ProgramDescriptor) -> PortFuture<'a, Vec<u8>> {
        panic!("the application service must not fetch packages")
    }
}

/// Atomic test store: persistence semantics are still tested against PostgreSQL
/// by the adapter suite. This fixture isolates application/resolver ordering.
#[derive(Default)]
struct MemoryStore {
    tasks: Mutex<Vec<TaskSnapshot>>,
    lookups: AtomicUsize,
    accepts: AtomicUsize,
    lookup_error: Mutex<Option<ContractError>>,
}

impl TaskStore for MemoryStore {
    fn lookup_submission<'a>(
        &'a self,
        scope: &'a Scope,
        key: &'a str,
    ) -> ContractFuture<'a, Option<TaskSnapshot>> {
        Box::pin(async move {
            self.lookups.fetch_add(1, Ordering::SeqCst);
            if let Some(error) = self.lookup_error.lock().unwrap().clone() {
                return Err(error);
            }
            Ok(self
                .tasks
                .lock()
                .unwrap()
                .iter()
                .find(|task| task.scope() == *scope && task.idempotency_key == key)
                .cloned())
        })
    }

    fn accept_resolved_submission<'a>(
        &'a self,
        command: &'a SubmitCommand,
        descriptor: &'a ProgramDescriptor,
    ) -> ContractFuture<'a, TaskSnapshot> {
        Box::pin(async move {
            self.accepts.fetch_add(1, Ordering::SeqCst);
            let mut tasks = self.tasks.lock().unwrap();
            if let Some(task) = tasks.iter().find(|task| {
                task.input.tenant_id == command.input.tenant_id
                    && task.input.namespace == command.input.namespace
                    && task.idempotency_key == command.idempotency_key
            }) {
                core::replay_submission(task, command)?;
                return Ok(task.clone());
            }
            let next = tasks.len() + 1;
            let task = core::submit(
                command,
                descriptor,
                &format!("task_{next}"),
                &format!("run_{next}"),
                1_800_000_000_000,
            )?
            .task;
            tasks.push(task.clone());
            Ok(task)
        })
    }

    fn open_session<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a str,
        _: u32,
    ) -> ContractFuture<'a, WorkerSession> {
        unused()
    }

    fn extend_session<'a>(&'a self, _: &'a str) -> ContractFuture<'a, WorkerSession> {
        unused()
    }

    fn inspect<'a>(&'a self, _: &'a Scope, _: &'a str) -> ContractFuture<'a, TaskSnapshot> {
        unused()
    }

    fn inspect_attempt<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a str,
        _: &'a str,
    ) -> ContractFuture<'a, AttemptSnapshot> {
        unused()
    }

    fn history<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a str,
        _: u64,
    ) -> ContractFuture<'a, Vec<RecordedHistoryEvent>> {
        unused()
    }

    fn acquire<'a>(&'a self, _: &'a AcquireCommand) -> ContractFuture<'a, AcquireReply> {
        unused()
    }

    fn renew<'a>(&'a self, _: &'a RenewCommand) -> ContractFuture<'a, Authority> {
        unused()
    }

    fn settle<'a>(&'a self, _: &'a SettleCommand) -> ContractFuture<'a, SettleReply> {
        unused()
    }

    fn confirm_quiescence<'a>(&'a self, _: &'a LeaseOwner) -> ContractFuture<'a, TaskState> {
        unused()
    }

    fn cancel<'a>(&'a self, _: &'a Scope, _: &'a str) -> ContractFuture<'a, TaskState> {
        unused()
    }
}

fn unused<T>() -> ContractFuture<'static, T> {
    Box::pin(async { panic!("unexpected operation on application submission fixture") })
}

struct Fixture {
    store: Arc<MemoryStore>,
    service: ApplicationService,
    requests: mpsc::UnboundedReceiver<(ProgramRef, ResolveReply)>,
}

impl Fixture {
    fn new() -> Self {
        let store = Arc::new(MemoryStore::default());
        let (requests, receiver) = mpsc::unbounded_channel();
        let service =
            ApplicationService::new(store.clone(), Arc::new(ControlledPrograms { requests }));
        Self {
            store,
            service,
            requests: receiver,
        }
    }

    fn start(&self, command: SubmitCommand) -> tokio::task::JoinHandle<Result<TaskSnapshot>> {
        let service = self.service.clone();
        tokio::spawn(async move { service.submit(&command).await })
    }

    async fn resolution(&mut self) -> ResolveReply {
        let (program, reply) = bounded(self.requests.recv()).await.unwrap();
        assert_eq!(program, command().input.program);
        reply
    }
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("application operation stalled")
}

fn command() -> SubmitCommand {
    SubmitCommand {
        idempotency_key: "invoice-42".into(),
        input: SubmitTask::decode(
            br#"{"tenant_id":"acme","namespace":"billing","queue":"invoices",
                "program":{"id":"invoice","version":"1.0.0"},"data":{"amount":42}}"#,
        )
        .unwrap(),
        origin_trace: Some(TraceContext {
            traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
            tracestate: None,
        }),
    }
}

fn descriptor(digit: char) -> ProgramDescriptor {
    ProgramDescriptor {
        program: command().input.program,
        digest: Digest(format!("sha256:{}", digit.to_string().repeat(64))),
        size: 100,
    }
}

#[tokio::test]
async fn accepted_replay_and_conflict_do_not_contact_program_store() {
    let mut fixture = Fixture::new();
    let original = command();
    let accepted = fixture
        .store
        .accept_resolved_submission(&original, &descriptor('a'))
        .await
        .unwrap();
    let mut retry = original.clone();
    retry.origin_trace = None;
    let replay = fixture.service.submit(&retry).await.unwrap();
    assert_eq!(replay.task_id, accepted.task_id);
    assert_eq!(replay.descriptor, accepted.descriptor);
    assert_eq!(replay.origin_trace, original.origin_trace);

    retry.input.data = json!({"amount": 43});
    assert!(matches!(
        fixture.service.submit(&retry).await,
        Err(ContractError::Conflict)
    ));
    assert!(fixture.requests.try_recv().is_err());
    assert_eq!(fixture.store.accepts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalid_submission_and_trace_are_rejected_before_any_io() {
    let mut fixture = Fixture::new();
    let mut invalid = command();
    invalid.origin_trace.as_mut().unwrap().traceparent = "invalid".into();
    assert!(matches!(
        fixture.service.submit(&invalid).await,
        Err(ContractError::InvalidInput(_))
    ));
    invalid = command();
    invalid.idempotency_key.clear();
    assert!(matches!(
        fixture.service.submit(&invalid).await,
        Err(ContractError::InvalidInput(_))
    ));
    invalid = command();
    invalid.input.attempt_timeout_ms = 0;
    assert!(matches!(
        fixture.service.submit(&invalid).await,
        Err(ContractError::InvalidInput(_))
    ));
    assert_eq!(fixture.store.lookups.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.store.accepts.load(Ordering::SeqCst), 0);
    assert!(fixture.requests.try_recv().is_err());
}

#[tokio::test]
async fn stalled_resolution_does_not_block_other_submission_or_rebind_its_winner() {
    let mut fixture = Fixture::new();
    let first = fixture.start(command());
    let first_resolution = fixture.resolution().await;
    assert_eq!(fixture.store.accepts.load(Ordering::SeqCst), 0);

    let mut second_command = command();
    second_command.origin_trace = None;
    let second = fixture.start(second_command);
    let second_resolution = fixture.resolution().await;
    second_resolution.send(Ok(descriptor('b'))).unwrap();
    let winner = bounded(second).await.unwrap().unwrap();
    assert_eq!(winner.descriptor, descriptor('b'));
    assert!(winner.origin_trace.is_none());
    assert!(!first.is_finished());

    first_resolution.send(Ok(descriptor('a'))).unwrap();
    let replay = bounded(first).await.unwrap().unwrap();
    assert_eq!(replay.task_id, winner.task_id);
    assert_eq!(replay.descriptor, winner.descriptor);
    assert_eq!(replay.origin_trace, winner.origin_trace);
    assert_eq!(fixture.store.tasks.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn concurrent_different_input_conflicts_after_resolution() {
    let mut fixture = Fixture::new();
    let first = fixture.start(command());
    let first_resolution = fixture.resolution().await;
    let mut other = command();
    other.input.data = json!("different input");
    let second = fixture.start(other);
    fixture
        .resolution()
        .await
        .send(Ok(descriptor('b')))
        .unwrap();
    bounded(second).await.unwrap().unwrap();
    first_resolution.send(Ok(descriptor('a'))).unwrap();
    assert!(matches!(
        bounded(first).await.unwrap(),
        Err(ContractError::Conflict)
    ));
    assert_eq!(fixture.store.tasks.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn resolver_failure_reconciles_a_concurrently_committed_submission() {
    let mut fixture = Fixture::new();
    let pending = fixture.start(command());
    let resolution = fixture.resolution().await;
    let winner = fixture
        .store
        .accept_resolved_submission(&command(), &descriptor('b'))
        .await
        .unwrap();
    resolution
        .send(Err(Error::new(ErrorKind::Io, "program store disconnected")))
        .unwrap();
    let replay = bounded(pending).await.unwrap().unwrap();
    assert_eq!(replay.task_id, winner.task_id);
    assert_eq!(replay.descriptor, winner.descriptor);
    assert_eq!(fixture.store.lookups.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn resolver_failure_does_not_hide_a_concurrent_input_conflict() {
    let mut fixture = Fixture::new();
    let pending = fixture.start(command());
    let resolution = fixture.resolution().await;
    let mut winner = command();
    winner.input.data = json!(null);
    fixture
        .store
        .accept_resolved_submission(&winner, &descriptor('b'))
        .await
        .unwrap();
    resolution
        .send(Err(Error::new(ErrorKind::NotFound, "program absent")))
        .unwrap();
    assert!(matches!(
        bounded(pending).await.unwrap(),
        Err(ContractError::Conflict)
    ));
}

#[tokio::test]
async fn reconciliation_unavailable_does_not_claim_submission_is_missing() {
    let mut fixture = Fixture::new();
    let pending = fixture.start(command());
    let resolution = fixture.resolution().await;
    *fixture.store.lookup_error.lock().unwrap() =
        Some(ContractError::Unavailable("database unavailable".into()));
    resolution
        .send(Err(Error::new(ErrorKind::NotFound, "program absent")))
        .unwrap();
    assert!(matches!(
        bounded(pending).await.unwrap(),
        Err(ContractError::Unavailable(message)) if message == "database unavailable"
    ));
    assert_eq!(fixture.store.accepts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn invalid_or_mismatched_resolved_descriptor_is_never_accepted() {
    for mismatched in [false, true] {
        let mut fixture = Fixture::new();
        let pending = fixture.start(command());
        let mut resolved = descriptor('a');
        if mismatched {
            resolved.program.version = "2.0.0".into();
        } else {
            resolved.digest.0 = "invalid".into();
        }
        fixture.resolution().await.send(Ok(resolved)).unwrap();
        assert!(matches!(
            bounded(pending).await.unwrap(),
            Err(ContractError::Unavailable(_))
        ));
        assert_eq!(fixture.store.accepts.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn resolver_error_classification_preserves_missing_and_availability() {
    for kind in [
        ErrorKind::NotFound,
        ErrorKind::InvalidInput,
        ErrorKind::Incompatible,
        ErrorKind::Io,
        ErrorKind::Unavailable,
        ErrorKind::TimedOut,
        ErrorKind::Cancelled,
        ErrorKind::Capacity,
        ErrorKind::Runtime,
        ErrorKind::Integrity,
        ErrorKind::Protocol,
    ] {
        let mut fixture = Fixture::new();
        let pending = fixture.start(command());
        fixture
            .resolution()
            .await
            .send(Err(Error::new(kind, "resolver failure")))
            .unwrap();
        let error = bounded(pending).await.unwrap().unwrap_err();
        match kind {
            ErrorKind::NotFound => assert_eq!(error, ContractError::NotFound),
            ErrorKind::InvalidInput | ErrorKind::Incompatible => {
                assert!(matches!(error, ContractError::InvalidInput(_)));
            }
            _ => assert!(matches!(error, ContractError::Unavailable(_))),
        }
        assert_eq!(fixture.store.lookups.load(Ordering::SeqCst), 2);
        assert_eq!(fixture.store.accepts.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn malformed_resolution_can_still_reconcile_a_committed_winner() {
    let mut fixture = Fixture::new();
    let pending = fixture.start(command());
    let resolution = fixture.resolution().await;
    let winner = fixture
        .store
        .accept_resolved_submission(&command(), &descriptor('b'))
        .await
        .unwrap();
    let mut invalid = descriptor('a');
    invalid.size = 0;
    resolution.send(Ok(invalid)).unwrap();
    let replay = bounded(pending).await.unwrap().unwrap();
    assert_eq!(replay.task_id, winner.task_id);
    assert_eq!(replay.descriptor, winner.descriptor);
    assert_eq!(fixture.store.accepts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn submission_scope_keeps_identical_keys_independent() {
    let mut fixture = Fixture::new();
    fixture
        .store
        .accept_resolved_submission(&command(), &descriptor('a'))
        .await
        .unwrap();
    let mut other = command();
    other.input.namespace = "shipping".into();
    let pending = fixture.start(other);
    fixture
        .resolution()
        .await
        .send(Ok(descriptor('b')))
        .unwrap();
    let second = bounded(pending).await.unwrap().unwrap();
    assert_eq!(second.input.namespace, "shipping");
    assert_eq!(second.descriptor, descriptor('b'));
    assert_eq!(fixture.store.tasks.lock().unwrap().len(), 2);
}
