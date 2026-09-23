use super::*;
use std::time::Instant;
struct Completions {
    calls: AtomicUsize,
    reply: Mutex<CompletionSubscription>,
}
impl CompletionStore for Completions {
    fn configure_completion_destination<'a>(
        &'a self,
        _: &'a CompletionDestination,
    ) -> ContractFuture<'a, ()> {
        unused()
    }
    fn subscribe_completion<'a>(
        &'a self,
        _: &'a CompletionSubscribeCommand,
    ) -> ContractFuture<'a, CompletionSubscription> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(self.reply.lock().unwrap().clone()) })
    }
    fn completion_status<'a>(
        &'a self,
        _: &'a Scope,
        _: &'a str,
    ) -> ContractFuture<'a, CompletionSubscription> {
        Box::pin(async { Ok(self.reply.lock().unwrap().clone()) })
    }
    fn retry_completion<'a>(
        &'a self,
        _: &'a CompletionRetryCommand,
    ) -> ContractFuture<'a, CompletionSubscription> {
        Box::pin(async { Ok(self.reply.lock().unwrap().clone()) })
    }
    fn lease_completions<'a>(
        &'a self,
        _: &'a CompletionDestination,
        _: u32,
        _: Instant,
    ) -> ContractFuture<'a, Vec<CompletionLease>> {
        unused()
    }
    fn complete_deliveries<'a>(
        &'a self,
        _: &'a [CompletionDeliveryResult],
        _: Instant,
    ) -> ContractFuture<'a, ()> {
        unused()
    }
}
fn self_command() -> CompletionSubscribeCommand {
    CompletionSubscribeCommand {
        scope: Scope {
            tenant_id: "t".into(),
            namespace: "n".into(),
        },
        target: CompletionTarget::Task { id: "task".into() },
        destination: "receiver".into(),
        idempotency_key: "key".into(),
    }
}
fn store() -> Arc<Completions> {
    Arc::new(Completions {
        calls: AtomicUsize::new(0),
        reply: Mutex::new(CompletionSubscription {
            subscription_id: "sub".into(),
            command: self_command(),
            state: CompletionState::Waiting,
            generation: 1,
            attempts: 0,
            total_attempts: 0,
            created_at: 1,
            activated_at: None,
            next_attempt_at: None,
            lease_expires_at: None,
            delivered_at: None,
            exhausted_at: None,
            last_failure: None,
            event: None,
        }),
    })
}
#[tokio::test]
async fn subscription_validation_precedes_store_and_never_resolves_programs() {
    let mut fixture = Fixture::new();
    let store = store();
    let service = fixture.service.with_completions(store.clone());
    let mut command = self_command();
    command.idempotency_key.clear();
    assert!(matches!(
        service.subscribe_completion(&command).await,
        Err(ContractError::InvalidInput(_))
    ));
    assert_eq!(store.calls.load(Ordering::SeqCst), 0);
    service.subscribe_completion(&self_command()).await.unwrap();
    assert_eq!(store.calls.load(Ordering::SeqCst), 1);
    assert!(fixture.requests.try_recv().is_err());
}
#[tokio::test]
async fn store_replies_cannot_cross_subscription_identity_or_scope() {
    let fixture = Fixture::new();
    let store = store();
    let service = fixture.service.with_completions(store.clone());
    store.reply.lock().unwrap().command.destination = "other".into();
    assert!(matches!(
        service.subscribe_completion(&self_command()).await,
        Err(ContractError::Unavailable(_))
    ));
    assert!(matches!(
        service
            .completion_status(&self_command().scope, "other")
            .await,
        Err(ContractError::Unavailable(_))
    ));
    store.reply.lock().unwrap().command.scope.namespace = "other".into();
    assert!(matches!(
        service
            .completion_status(&self_command().scope, "sub")
            .await,
        Err(ContractError::Unavailable(_))
    ));
}
#[tokio::test]
async fn redelivery_must_advance_the_requested_generation() {
    let fixture = Fixture::new();
    let store = store();
    let service = fixture.service.with_completions(store);
    let command = CompletionRetryCommand {
        scope: self_command().scope,
        subscription_id: "sub".into(),
        expected_generation: 1,
    };
    assert!(matches!(
        service.retry_completion(&command).await,
        Err(ContractError::Unavailable(_))
    ));
    assert!(matches!(
        Fixture::new()
            .service
            .subscribe_completion(&self_command())
            .await,
        Err(ContractError::InvalidInput(_))
    ));
}
