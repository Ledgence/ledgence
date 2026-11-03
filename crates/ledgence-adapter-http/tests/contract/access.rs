//! Real HTTP admission: denials precede command decoding, data access and upload.
use super::*;
use ledgence_adapter_http::server::publication::PublicationService;
use ledgence_orchestration_api::console::*;
use ledgence_worker_api::{ProgramArtifactPublisher, PublicationFuture, PublishArtifactResult};
use std::collections::BTreeMap;

struct Verifier {
    audience: AccessAudience,
    actors: Mutex<BTreeMap<String, VerifiedPrincipal>>,
}
impl CredentialVerifier for Verifier {
    fn audience(&self) -> &AccessAudience {
        &self.audience
    }
    fn verify(&self, token: &str, _: u64) -> Result<VerifiedPrincipal> {
        self.actors
            .lock()
            .unwrap()
            .get(token)
            .cloned()
            .ok_or(ContractError::Unauthenticated)
    }
}
#[derive(Default)]
struct Queries {
    calls: AtomicUsize,
}
impl ConsoleQueryService for Queries {
    fn query_console<'a>(&'a self, _: &'a ConsoleQuery) -> ContractFuture<'a, ConsoleQueryReply> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(ContractError::NotFound)
        })
    }
}
impl WorkerObservationPublisher for Queries {
    fn publish_observation<'a>(
        &'a self,
        command: &'a WorkerObservationCommand,
    ) -> ContractFuture<'a, WorkerObservationReceipt> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(WorkerObservationReceipt {
                worker_session_id: command.worker_session_id.clone(),
                sequence: command.sequence,
                received_at: 1,
                already_received: false,
            })
        })
    }
}
impl WorkerObservationService for Queries {
    fn query_workers<'a>(
        &'a self,
        _: &'a WorkerObservationQuery,
    ) -> ContractFuture<'a, WorkerObservationReply> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(ContractError::NotFound)
        })
    }
}
#[derive(Default)]
struct Writer {
    calls: AtomicUsize,
}
impl ProgramArtifactPublisher for Writer {
    fn publish<'a>(
        &'a self,
        program: ProgramRef,
        digest: Digest,
        archive: Vec<u8>,
    ) -> PublicationFuture<'a, PublishArtifactResult> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(PublishArtifactResult {
                descriptor: ProgramDescriptor {
                    program,
                    digest,
                    size: archive.len() as u64,
                },
                already_published: false,
            })
        })
    }
}
struct Fixture {
    running: Running,
    mock: Arc<Mock>,
    queries: Arc<Queries>,
    writer: Arc<Writer>,
    verifier: Arc<Verifier>,
}

#[tokio::test]
async fn worker_observation_scope_is_rejected_before_selecting_the_observation_port() {
    let f = setup().await;
    let client = reqwest::Client::new();
    let mut command = serde_json::json!({
        "schema_version": 1,
        "worker_session_id": "observation-session",
        "scope": scope(),
        "sequence": "1",
        "display_name": null,
        "snapshot": {
            "configured_concurrency": 1,
            "accepting": true,
            "active_consumers": 0,
            "occupied_process_slots": 0,
            "detail_state": "unavailable",
            "slots": []
        }
    });
    command["scope"]["namespace"] = serde_json::json!("foreign");
    for expected in [404, 200] {
        let response = client
            .post(format!("{}/v1/worker-observations", f.running.url))
            .bearer_auth("worker")
            .header("content-type", "application/json")
            .body(command.to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        if expected == 404 {
            assert_no_calls(&f);
        }
        command["scope"] = serde_json::json!(scope());
    }
    assert_eq!(f.queries.calls.load(Ordering::SeqCst), 1);
}
async fn setup() -> Fixture {
    let config = SelfHostedInstanceConfig {
        instance_id: "access-http".into(),
        name: "Access HTTP".into(),
        scope: scope(),
        suggested_queues: vec![],
        allowed_origins: vec!["https://console.example".into()],
    };
    let audience = AccessAudience {
        authority_id: "authority".into(),
        instance_id: config.instance_id.clone(),
        scope: config.scope.clone(),
    };
    let mut actors = BTreeMap::new();
    for (token, class, caps) in [
        (
            "reader",
            PrincipalClass::Operator,
            vec![AccessCapability::MetadataRead],
        ),
        (
            "operator",
            PrincipalClass::Operator,
            vec![
                AccessCapability::MetadataRead,
                AccessCapability::SensitiveRead,
                AccessCapability::ApprovalDecide,
            ],
        ),
        (
            "publisher",
            PrincipalClass::Service,
            vec![
                AccessCapability::MetadataRead,
                AccessCapability::ProgramPublish,
            ],
        ),
        (
            "worker",
            PrincipalClass::Worker,
            vec![AccessCapability::WorkerExecute],
        ),
    ] {
        actors.insert(
            token.into(),
            VerifiedPrincipal::new(
                PrincipalIdentity {
                    issuer: audience.issuer(),
                    subject: token.into(),
                },
                audience.clone(),
                format!("key-{token}"),
                class,
                4_102_444_800_000,
                caps,
            )
            .unwrap(),
        );
    }
    let verifier = Arc::new(Verifier {
        audience,
        actors: Mutex::new(actors),
    });
    let mock = Arc::new(Mock::default());
    let queries = Arc::new(Queries::default());
    let writer = Arc::new(Writer::default());
    let console = server::console::ConsoleServices::new(
        config,
        "test".into(),
        queries.clone(),
        None,
        Some(queries.clone()),
    )
    .unwrap()
    .with_publication(PublicationService::new(writer.clone()))
    .with_access(verifier.clone(), false)
    .unwrap();
    let running = start(
        server::router_with_console(
            mock.clone(),
            mock.clone(),
            mock.clone(),
            console,
            Arc::new(AtomicBool::new(false)),
            Arc::new(ledgence_worker_api::NoopTraceBridge),
        )
        .unwrap(),
    )
    .await;
    Fixture {
        running,
        mock,
        queries,
        writer,
        verifier,
    }
}
fn console_request(f: &Fixture, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
    let request = reqwest::Client::new()
        .request(method.clone(), format!("{}{path}", f.running.url))
        .header("x-ledgence-console-origin", "https://console.example");
    if method == reqwest::Method::GET {
        request
    } else {
        request.header("origin", "https://console.example")
    }
}
async fn login(f: &Fixture, token: &str, old_cookie: Option<&str>) -> (String, Value) {
    let mut request = console_request(f, reqwest::Method::POST, "/v1/console/access/login")
        .header("content-type", "application/json")
        .body(json!({"credential":token}).to_string());
    if let Some(cookie) = old_cookie {
        request = request.header("cookie", cookie);
    }
    let response = request.send().await.unwrap();
    assert_eq!(response.status(), 200);
    let cookie = response.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let body = decode(response).await;
    (cookie, body)
}
async fn decode(response: reqwest::Response) -> Value {
    serde_json::from_slice(&response.bytes().await.unwrap()).unwrap()
}
fn assert_no_calls(f: &Fixture) {
    assert!(f.mock.calls.lock().unwrap().is_empty());
    assert_eq!(f.queries.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.writer.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn every_http_route_denies_anonymous_before_any_service_or_upload() {
    let f = setup().await;
    let client = reqwest::Client::new();
    for (method, path) in ALL_ROUTES {
        let response = client
            .request(
                method.parse::<reqwest::Method>().unwrap(),
                format!("{}{path}", f.running.url),
            )
            .header("content-type", "application/json")
            .body("{}")
            .send()
            .await
            .unwrap();
        let expected = if *path == "/v1/console/access/login" {
            403
        } else {
            401
        };
        assert_eq!(response.status(), expected, "{method} {path}");
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert!(response.headers().contains_key("request-id"));
        let body = response.text().await.unwrap();
        assert!(!body.contains("credential") && !body.contains("LeaseOwner"));
    }
    for (method, path, status) in [
        ("GET", "/v1/unclassified", 404),
        ("PATCH", "/v1/tasks", 405),
        ("POST", "/v1/programs/demo/1.0.0/artifact", 405),
    ] {
        let response = client
            .request(
                method.parse::<reqwest::Method>().unwrap(),
                format!("{}{path}", f.running.url),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), status);
    }
    assert_no_calls(&f);
}
#[tokio::test]
async fn metadata_reader_cannot_reach_sensitive_aliases_or_reuse_raw_worker_authority() {
    let f = setup().await;
    let client = reqwest::Client::new();
    for (method, path) in SENSITIVE_ROUTES {
        let response = client
            .request(
                method.parse::<reqwest::Method>().unwrap(),
                format!("{}{path}", f.running.url),
            )
            .bearer_auth("reader")
            .header("content-type", "application/json")
            .body(json!({"canary":"approval-private-arguments"}).to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 403, "{method} {path}");
        assert!(
            !response
                .text()
                .await
                .unwrap()
                .contains("approval-private-arguments")
        );
    }
    for path in [
        "/v1/attempts/inspect",
        "/v1/worker-sessions",
        "/v1/worker-sessions/extend",
        "/v1/acquisitions",
        "/v1/dispatch/claim",
        "/v1/renewals",
        "/v1/settlements",
        "/v1/quiescence-confirmations",
        "/v1/workflows/activations/context",
        "/v1/workflows/approved-effects/claim",
        "/v1/workflows/approved-effects/confirm",
        "/v1/workflows/local-results",
        "/v1/workflows/forks",
        "/v1/worker-observations",
    ] {
        let method = if path == "/v1/attempts/inspect" {
            reqwest::Method::GET
        } else {
            reqwest::Method::POST
        };
        let response = client
            .request(method, format!("{}{path}", f.running.url))
            .bearer_auth("operator")
            .header("content-type", "application/json")
            .body(json!({}).to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 403, "operator {path}");
    }
    assert_no_calls(&f);
}
#[tokio::test]
async fn denied_upload_returns_before_waiting_for_body_and_allowed_publication_still_works() {
    let f = setup().await;
    let address = f.running.url.strip_prefix("http://").unwrap();
    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    socket.write_all(format!("PUT /v1/programs/demo/1.0.0/artifact HTTP/1.1\r\nHost: {address}\r\nContent-Length: 500000\r\nContent-Type: application/zip\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut response = [0u8; 1024];
    let read = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&response[..read]).starts_with("HTTP/1.1 401"));
    assert_no_calls(&f);
    let bytes = b"transport fixture".to_vec();
    use sha2::{Digest as _, Sha256};
    let descriptor = ProgramDescriptor {
        program: ProgramRef {
            id: "demo".into(),
            version: "1.0.0".into(),
        },
        digest: Digest(format!("sha256:{:x}", Sha256::digest(&bytes))),
        size: bytes.len() as u64,
    };
    let client = ledgence_adapter_http::HttpProgramPublisher::new(&f.running.url)
        .unwrap()
        .with_bearer_token("publisher")
        .unwrap();
    assert!(client.capabilities().await.unwrap().enabled);
    assert_eq!(
        client.publish(&descriptor, bytes).await.unwrap().descriptor,
        descriptor
    );
    assert_eq!(f.writer.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn console_login_bootstrap_actor_switch_revocation_and_logout_use_real_headers() {
    let f = setup().await;
    let (reader_cookie, reader) = login(&f, "reader", None).await;
    assert_eq!(reader["mode"], "credentials");
    assert_eq!(reader["subject"], "reader");
    let response = console_request(&f, reqwest::Method::GET, "/v1/console/access")
        .header("cookie", &reader_cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(decode(response).await, reader);
    let response = console_request(&f, reqwest::Method::GET, "/v1/console/config")
        .header("cookie", &reader_cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
    let response = console_request(&f, reqwest::Method::GET, "/v1/console/config")
        .header("cookie", &reader_cookie)
        .header("x-ledgence-actor", reader["actor_key"].as_str().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    f.verifier.actors.lock().unwrap().remove("reader");
    let response = console_request(&f, reqwest::Method::GET, "/v1/console/config")
        .header("cookie", &reader_cookie)
        .header("x-ledgence-actor", reader["actor_key"].as_str().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
    let (cookie, operator) = login(&f, "operator", Some(&reader_cookie)).await;
    let response = console_request(&f, reqwest::Method::POST, "/v1/console/access/logout")
        .header("cookie", &cookie)
        .header("x-ledgence-actor", reader["actor_key"].as_str().unwrap())
        .header("content-type", "application/json")
        .body(json!({}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
    assert!(!response.headers().contains_key("set-cookie"));
    let response = console_request(&f, reqwest::Method::POST, "/v1/console/access/logout")
        .header("cookie", &cookie)
        .header("x-ledgence-actor", operator["actor_key"].as_str().unwrap())
        .header("content-type", "application/json")
        .body(json!({}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(
        response.headers()["set-cookie"]
            .to_str()
            .unwrap()
            .contains("Max-Age=0")
    );
    assert_eq!(decode(response).await, json!({"signed_out":true}));
    let response = console_request(&f, reqwest::Method::POST, "/v1/console/access/login")
        .header("content-type", "application/json")
        .body(json!({"credential":"worker"}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
    assert!(!response.headers().contains_key("set-cookie"));
    assert_no_calls(&f);
}
#[tokio::test]
async fn unconfigured_mode_is_explicit_and_does_not_invent_a_principal() {
    let mock = Arc::new(Mock::default());
    let running = start(server::router(mock)).await;
    let response = reqwest::get(format!("{}/v1/access", running.url))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(decode(response).await, json!({"mode":"operator_trusted"}));
}

// Explicit network inventory: API additions must classify their denial boundary.
const ALL_ROUTES: &[(&str, &str)] = &[
    ("GET", "/v1/access"),
    ("POST", "/v1/completion-subscriptions"),
    ("GET", "/v1/completion-subscriptions/status"),
    ("POST", "/v1/completion-subscriptions/retry"),
    ("POST", "/v1/workflows"),
    ("GET", "/v1/workflows/status"),
    ("GET", "/v1/workflows/result"),
    ("POST", "/v1/workflows/cancel"),
    ("POST", "/v1/workflows/events"),
    ("POST", "/v1/workflows/approvals/inspect"),
    ("POST", "/v1/workflows/approvals/list"),
    ("POST", "/v1/workflows/approvals/inbox"),
    ("POST", "/v1/workflows/approvals/decide"),
    ("POST", "/v1/workflows/activations/context"),
    ("POST", "/v1/workflows/approved-effects/claim"),
    ("POST", "/v1/workflows/approved-effects/confirm"),
    ("POST", "/v1/workflows/local-results"),
    ("POST", "/v1/workflows/forks"),
    ("GET", "/v1/tasks"),
    ("POST", "/v1/tasks"),
    ("GET", "/v1/tasks/inspect"),
    ("GET", "/v1/tasks/status"),
    ("GET", "/v1/tasks/result"),
    ("GET", "/v1/attempts/inspect"),
    ("GET", "/v1/tasks/history"),
    ("POST", "/v1/tasks/cancel"),
    ("POST", "/v1/worker-sessions"),
    ("POST", "/v1/worker-sessions/extend"),
    ("POST", "/v1/acquisitions"),
    ("POST", "/v1/dispatch/claim"),
    ("POST", "/v1/renewals"),
    ("POST", "/v1/settlements"),
    ("POST", "/v1/quiescence-confirmations"),
    ("GET", "/v1/console/access"),
    ("POST", "/v1/console/access/login"),
    ("POST", "/v1/console/access/logout"),
    ("POST", "/v1/console/approvals/inspect"),
    ("POST", "/v1/console/approvals/list"),
    ("POST", "/v1/console/approvals/inbox"),
    ("POST", "/v1/console/approvals/decide"),
    ("GET", "/v1/console/executions"),
    ("GET", "/v1/console/executions/ancestry"),
    ("GET", "/v1/console/workflows/explorer"),
    ("GET", "/v1/console/workflows/input"),
    ("GET", "/v1/console/attempts/observations"),
    ("GET", "/v1/console/programs/catalog"),
    ("GET", "/v1/console/config"),
    ("GET", "/v1/console/tasks"),
    ("POST", "/v1/console/tasks"),
    ("GET", "/v1/console/tasks/inspect"),
    ("GET", "/v1/console/tasks/status"),
    ("GET", "/v1/console/tasks/result"),
    ("GET", "/v1/console/tasks/history"),
    ("POST", "/v1/console/tasks/cancel"),
    ("GET", "/v1/console/tasks/attempts"),
    ("GET", "/v1/console/attempts/inspect"),
    ("GET", "/v1/console/workflows"),
    ("POST", "/v1/console/workflows"),
    ("GET", "/v1/console/workflows/inspect"),
    ("GET", "/v1/console/workflows/status"),
    ("GET", "/v1/console/workflows/result"),
    ("POST", "/v1/console/workflows/cancel"),
    ("POST", "/v1/console/workflows/events"),
    ("GET", "/v1/console/workflows/activations"),
    ("GET", "/v1/console/workflows/children"),
    ("GET", "/v1/console/workflows/waits"),
    ("GET", "/v1/console/workflows/history"),
    ("GET", "/v1/console/workflows/local-steps"),
    ("GET", "/v1/console/workflows/local-steps/inspect"),
    ("GET", "/v1/console/programs"),
    ("GET", "/v1/console/programs/versions"),
    ("GET", "/v1/console/programs/inspect"),
    ("POST", "/v1/console/programs/register"),
    ("GET", "/v1/console/workers"),
    ("GET", "/v1/console/workers/inspect"),
    ("POST", "/v1/worker-observations"),
    ("GET", "/v1/programs/publication-capabilities"),
    ("PUT", "/v1/programs/demo/1.0.0/artifact"),
];
const SENSITIVE_ROUTES: &[(&str, &str)] = &[
    ("GET", "/v1/tasks/result"),
    ("GET", "/v1/tasks/inspect"),
    ("GET", "/v1/workflows/result"),
    ("POST", "/v1/workflows/approvals/inspect"),
    ("POST", "/v1/workflows/approvals/list"),
    ("POST", "/v1/workflows/approvals/decide"),
    ("POST", "/v1/console/approvals/inspect"),
    ("POST", "/v1/console/approvals/list"),
    ("POST", "/v1/console/approvals/decide"),
    ("GET", "/v1/console/tasks/result"),
    ("GET", "/v1/console/tasks/inspect"),
    ("GET", "/v1/console/workflows/result"),
    ("GET", "/v1/console/workflows/input"),
    ("GET", "/v1/console/workflows/local-steps/inspect"),
];
