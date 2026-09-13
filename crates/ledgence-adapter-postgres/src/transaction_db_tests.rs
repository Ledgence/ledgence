//! Cancellation and retry acceptance tests against disposable PostgreSQL databases.

use crate::{tests::*, *};
use sqlx::{
    AssertSqlSafe, ConnectOptions,
    postgres::{PgConnectOptions, PgSslMode},
};
use std::{
    io,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, oneshot},
    task::JoinSet,
};

pub(crate) struct BeginReplyGate {
    pub(crate) address: std::net::SocketAddr,
    pub(crate) armed: Arc<AtomicBool>,
    pub(crate) reached: oneshot::Receiver<()>,
    pub(crate) release: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Clone, Copy)]
enum Boundary {
    Begin,
    Commit,
}
impl Boundary {
    fn statement(self) -> &'static [u8] {
        match self {
            Self::Begin => b"BEGIN",
            Self::Commit => b"COMMIT",
        }
    }
    fn status(self) -> u8 {
        match self {
            Self::Begin => b'T',
            Self::Commit => b'I',
        }
    }
}

impl BeginReplyGate {
    async fn new(target_host: String, target_port: u16) -> Self {
        Self::at_boundary(target_host, target_port, Boundary::Begin).await
    }
    pub(crate) async fn at_commit(target_host: String, target_port: u16) -> Self {
        Self::at_boundary(target_host, target_port, Boundary::Commit).await
    }
    async fn at_boundary(target_host: String, target_port: u16, boundary: Boundary) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let armed = Arc::new(AtomicBool::new(false));
        let release = Arc::new(Notify::new());
        let (reached_send, reached) = oneshot::channel();
        let reached_send = Arc::new(Mutex::new(Some(reached_send)));
        let frontend_armed = armed.clone();
        let backend_release = release.clone();
        let task = tokio::spawn(async move {
            // Reconnection is a valid recovery strategy. Accept subsequent
            // connections instead of requiring the cancelled socket to survive.
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (client, _) = accepted.unwrap();
                        connections.spawn(forward_connection(
                            client, target_host.clone(), target_port,
                            frontend_armed.clone(), backend_release.clone(),
                            reached_send.clone(), boundary,
                        ));
                    }
                    Some(result) = connections.join_next() => {
                        if let Err(error) = result.unwrap() {
                            assert!(matches!(error.kind(),
                                io::ErrorKind::UnexpectedEof | io::ErrorKind::BrokenPipe
                                | io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
                            ), "unexpected protocol fixture error: {error}");
                        }
                    }
                }
            }
        });
        Self {
            address,
            armed,
            reached,
            release,
            task,
        }
    }
}

impl Drop for BeginReplyGate {
    fn drop(&mut self) {
        self.release.notify_one();
        // Dropping the JoinSet in this task also aborts its connection forwards.
        self.task.abort();
    }
}

async fn forward_connection(
    mut client: TcpStream,
    target_host: String,
    target_port: u16,
    armed: Arc<AtomicBool>,
    release: Arc<Notify>,
    reached: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    boundary: Boundary,
) -> io::Result<()> {
    let mut server = TcpStream::connect((target_host.as_str(), target_port)).await?;
    client.set_nodelay(true)?;
    server.set_nodelay(true)?;
    // TLS is disabled only for this loopback protocol fixture. The first
    // PostgreSQL startup message is length-prefixed and has no message tag.
    let length = client.read_u32().await?;
    if !(8..=16_384).contains(&length) {
        return Err(io::Error::other("invalid startup message length"));
    }
    let mut startup = vec![0; usize::try_from(length - 4).unwrap()];
    client.read_exact(&mut startup).await?;
    server.write_u32(length).await?;
    server.write_all(&startup).await?;
    let (client_read, client_write) = client.into_split();
    let (server_read, server_write) = server.into_split();
    let pending = Arc::new(AtomicBool::new(false));
    // Either side closing ends both halves, including a withheld BEGIN reply.
    tokio::select! {
        result = forward_frontend(client_read, server_write, armed, pending.clone(), boundary) => result,
        result = forward_backend(server_read, client_write, pending, release, reached, boundary) => result,
    }
}

async fn frame(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<(u8, Vec<u8>)> {
    let kind = reader.read_u8().await?;
    let size = reader.read_u32().await?;
    if !(4..=16 * 1024 * 1024).contains(&size) {
        return Err(io::Error::other("invalid protocol message length"));
    }
    let mut payload = vec![0; usize::try_from(size - 4).unwrap()];
    reader.read_exact(&mut payload).await?;
    Ok((kind, payload))
}

async fn write_frame(
    writer: &mut (impl AsyncWrite + Unpin),
    kind: u8,
    payload: &[u8],
) -> io::Result<()> {
    writer.write_u8(kind).await?;
    writer
        .write_u32(u32::try_from(payload.len() + 4).unwrap())
        .await?;
    writer.write_all(payload).await
}

async fn forward_frontend(
    mut reader: impl AsyncRead + Unpin,
    mut writer: impl AsyncWrite + Unpin,
    armed: Arc<AtomicBool>,
    pending: Arc<AtomicBool>,
    boundary: Boundary,
) -> io::Result<()> {
    loop {
        let (kind, payload) = frame(&mut reader).await?;
        if kind == b'Q'
            && payload.starts_with(boundary.statement())
            && armed.swap(false, Ordering::SeqCst)
        {
            pending.store(true, Ordering::SeqCst);
        }
        write_frame(&mut writer, kind, &payload).await?;
    }
}

async fn forward_backend(
    mut reader: impl AsyncRead + Unpin,
    mut writer: impl AsyncWrite + Unpin,
    pending: Arc<AtomicBool>,
    release: Arc<Notify>,
    reached: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    boundary: Boundary,
) -> io::Result<()> {
    loop {
        let (kind, payload) = frame(&mut reader).await?;
        if kind == b'Z' && pending.swap(false, Ordering::SeqCst) {
            assert_eq!(
                payload,
                [boundary.status()],
                "the backend must have crossed the requested transaction boundary"
            );
            let _ = reached.lock().unwrap().take().unwrap().send(());
            release.notified().await;
        }
        write_frame(&mut writer, kind, &payload).await?;
    }
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("PostgreSQL transaction test stalled")
}

#[derive(Clone, Copy, Debug)]
enum StartupOperation {
    Submission,
    History,
    Attempt,
}

async fn cancelled_startup_recovers(operation: StartupOperation, operation_timeout: bool) {
    let db = TestDb::new().await;
    let (existing, _, assigned) = claimed(&db.store).await;
    let original_history = db
        .store
        .history(&scope(), &existing.task_id, 0)
        .await
        .unwrap();
    let direct = PgConnectOptions::from_str(&db.url).unwrap();
    let mut gate = BeginReplyGate::new(direct.get_host().into(), direct.get_port()).await;
    let proxy_url = direct
        .host("127.0.0.1")
        .port(gate.address.port())
        .ssl_mode(PgSslMode::Disable)
        .to_url_lossy()
        .to_string();
    let mut store = PostgresStore::connect(
        &proxy_url,
        PostgresOptions {
            max_connections: 1,
            acquire_timeout: Duration::from_secs(5),
            statement_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(if operation_timeout { 1 } else { 5 }),
        },
    )
    .await
    .unwrap();
    let mut submission = command();
    submission.idempotency_key = "cancelled-before-submission".into();
    let submitted_store = store.clone();
    let task_id = existing.task_id.clone();
    let attempt_id = assigned.lease.owner.attempt_id.clone();
    let pending_submission = submission.clone();
    gate.armed.store(true, Ordering::SeqCst);
    let pending = tokio::spawn(async move {
        match operation {
            StartupOperation::Submission => submitted_store
                .accept_resolved_submission(&pending_submission, &descriptor())
                .await
                .map(|_| ()),
            StartupOperation::History => submitted_store
                .history(&scope(), &task_id, 0)
                .await
                .map(|_| ()),
            StartupOperation::Attempt => submitted_store
                .inspect_attempt(&scope(), &task_id, &attempt_id)
                .await
                .map(|_| ()),
        }
    });
    bounded(&mut gate.reached).await.unwrap();
    if operation_timeout {
        assert!(matches!(
            bounded(pending).await.unwrap(),
            Err(ContractError::Unavailable(_))
        ));
    } else {
        pending.abort();
        assert!(bounded(pending).await.unwrap_err().is_cancelled());
    }
    gate.release.notify_one();
    store.operation_timeout = Duration::from_secs(5);

    // A read outside an explicit transaction must not silently join the abandoned
    // BEGIN. Reusing that transaction would also poison the next snapshot setup.
    let inspected = bounded(store.inspect(&scope(), &existing.task_id))
        .await
        .unwrap();
    assert_eq!(
        inspected.current_attempt_id.as_deref(),
        Some(assigned.lease.owner.attempt_id.as_str())
    );
    let history = bounded(store.history(&scope(), &existing.task_id, 0))
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&history).unwrap(),
        serde_json::to_value(&original_history).unwrap(),
        "{operation:?}"
    );
    let attempt = bounded(store.inspect_attempt(
        &scope(),
        &existing.task_id,
        &assigned.lease.owner.attempt_id,
    ))
    .await
    .unwrap();
    assert_eq!(attempt.lease.owner, assigned.lease.owner);
    assert!(
        bounded(store.lookup_submission(&scope(), &submission.idempotency_key))
            .await
            .unwrap()
            .is_none()
    );
    let accepted = bounded(store.accept_resolved_submission(&submission, &descriptor()))
        .await
        .unwrap();
    assert_eq!(accepted.state, TaskState::Queued);
    let open_transactions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND state LIKE 'idle in transaction%'",
    )
    .fetch_one(&db.store.pool)
    .await
    .unwrap();
    assert_eq!(open_transactions, 0, "{operation:?}");
    bounded(store.close()).await;
    drop(gate);
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 and a local BEGIN-response gate"]
async fn caller_cancellation_during_mutation_and_snapshot_startup_recovers_pool() {
    for operation in [
        StartupOperation::Submission,
        StartupOperation::History,
        StartupOperation::Attempt,
    ] {
        cancelled_startup_recovers(operation, false).await;
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18 and a local BEGIN-response gate"]
async fn operation_timeout_during_mutation_and_snapshot_startup_recovers_pool() {
    for operation in [
        StartupOperation::Submission,
        StartupOperation::History,
        StartupOperation::Attempt,
    ] {
        cancelled_startup_recovers(operation, true).await;
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn healthy_mutation_and_snapshot_transactions_reuse_the_connection() {
    let db = TestDb::new().await;
    let store = PostgresStore::connect(
        &db.url,
        PostgresOptions {
            max_connections: 1,
            ..PostgresOptions::default()
        },
    )
    .await
    .unwrap();
    let original_backend: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    let (task, _, assigned) = bounded(claimed(&store)).await;
    bounded(store.history(&scope(), &task.task_id, 0))
        .await
        .unwrap();
    bounded(store.inspect_attempt(&scope(), &task.task_id, &assigned.lease.owner.attempt_id))
        .await
        .unwrap();
    bounded(store.open_session(&scope(), "python", 1))
        .await
        .unwrap();
    // Validation occurs after BEGIN here, exercising SQLx's ordinary rollback
    // on an error after transaction startup has been confirmed.
    assert!(matches!(
        bounded(store.open_session(&scope(), "python", 0)).await,
        Err(ContractError::InvalidInput(_))
    ));
    let backend: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(backend, original_backend);
    bounded(store.close()).await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn caller_cancellation_during_commit_reconciles_without_duplicate_history() {
    let db = TestDb::new().await;
    sqlx::raw_sql("CREATE FUNCTION commit_wait() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(8462441); RETURN NEW; END $$; CREATE CONSTRAINT TRIGGER commit_wait AFTER INSERT ON task_history DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION commit_wait();")
        .execute(&db.store.pool).await.unwrap();
    let gate = PgPoolOptions::new()
        .max_connections(1)
        .connect(&db.url)
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_lock(8462441)")
        .execute(&gate)
        .await
        .unwrap();
    let store = PostgresStore::connect(
        &db.url,
        PostgresOptions {
            max_connections: 1,
            ..PostgresOptions::default()
        },
    )
    .await
    .unwrap();
    let worker = store.clone();
    let call = tokio::spawn(async move {
        worker
            .accept_resolved_submission(&command(), &descriptor())
            .await
    });
    bounded(async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query='COMMIT')")
                .fetch_one(&db.store.pool).await.unwrap();
            if waiting { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await;
    call.abort();
    assert!(bounded(call).await.unwrap_err().is_cancelled());
    sqlx::query("SELECT pg_advisory_unlock(8462441)")
        .execute(&gate)
        .await
        .unwrap();
    let loaded = bounded(store.lookup_submission(&scope(), &command().idempotency_key))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        bounded(store.accept_resolved_submission(&command(), &descriptor()))
            .await
            .unwrap()
            .task_id,
        loaded.task_id
    );
    assert_eq!(
        bounded(store.history(&scope(), &loaded.task_id, 0))
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        bounded(store.open_session(&scope(), "python", 1))
            .await
            .unwrap()
            .concurrency,
        1
    );
    bounded(store.close()).await;
    gate.close().await;
    db.finish().await;
}

#[tokio::test]
#[ignore = "requires PostgreSQL 18"]
async fn known_transaction_aborts_retry_atomically_with_a_fixed_limit() {
    for code in ["40001", "40P01"] {
        let db = TestDb::new().await;
        sqlx::raw_sql(AssertSqlSafe(format!("CREATE SEQUENCE fail_count; CREATE FUNCTION fail_twice() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF nextval('fail_count') <= 2 THEN RAISE EXCEPTION 'retry test' USING ERRCODE='{code}'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_twice BEFORE INSERT ON task_history FOR EACH ROW EXECUTE FUNCTION fail_twice();")))
            .execute(&db.store.pool).await.unwrap();
        let accepted = bounded(
            db.store
                .accept_resolved_submission(&command(), &descriptor()),
        )
        .await
        .unwrap();
        let tries: i64 = sqlx::query_scalar("SELECT last_value FROM fail_count")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
        assert_eq!(tries, 3);
        assert_eq!(
            db.store
                .history(&scope(), &accepted.task_id, 0)
                .await
                .unwrap()
                .len(),
            1
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks")
            .fetch_one(&db.store.pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
        db.finish().await;
    }
    let db = TestDb::new().await;
    sqlx::raw_sql("CREATE SEQUENCE fail_count; CREATE FUNCTION always_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('fail_count'); RAISE EXCEPTION 'retry limit test' USING ERRCODE='40001'; END $$; CREATE TRIGGER always_fail BEFORE INSERT ON task_history FOR EACH ROW EXECUTE FUNCTION always_fail();")
        .execute(&db.store.pool).await.unwrap();
    assert!(matches!(
        bounded(
            db.store
                .accept_resolved_submission(&command(), &descriptor())
        )
        .await,
        Err(ContractError::Unavailable(_))
    ));
    let tries: i64 = sqlx::query_scalar("SELECT last_value FROM fail_count")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(tries, 3);
    assert!(
        db.store
            .lookup_submission(&scope(), &command().idempotency_key)
            .await
            .unwrap()
            .is_none()
    );
    let history_count: i64 = sqlx::query_scalar("SELECT count(*) FROM task_history")
        .fetch_one(&db.store.pool)
        .await
        .unwrap();
    assert_eq!(history_count, 0);
    db.finish().await;
}
