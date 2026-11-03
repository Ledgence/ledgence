#![cfg(all(feature = "client", feature = "server"))]
use axum::{
    Router,
    body::Body,
    extract::Request,
    http::{StatusCode, header},
    response::Response,
    routing::any,
};
use ledgence_adapter_http::{HttpCredentials, HttpProgramPublisher, HttpTaskService};
use ledgence_orchestration_api::{ContractError, Scope, TaskService};
use ledgence_worker_api::PublicationErrorKind;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn configured_clients_send_bound_credentials_and_decode_definitive_denials() {
    for (status, error, kind) in [
        (
            StatusCode::UNAUTHORIZED,
            ContractError::Unauthenticated,
            PublicationErrorKind::Unauthenticated,
        ),
        (
            StatusCode::FORBIDDEN,
            ContractError::Forbidden,
            PublicationErrorKind::Forbidden,
        ),
    ] {
        let count = Arc::new(AtomicUsize::new(0));
        let requests = count.clone();
        let reply = error.clone();
        let router = Router::new().fallback(any(move |request: Request| {
            let count = requests.clone();
            let error = reply.clone();
            async move {
                assert_eq!(
                    request.headers().get(header::AUTHORIZATION).unwrap(),
                    "Bearer local-test-credential"
                );
                count.fetch_add(1, Ordering::SeqCst);
                Response::builder()
                    .status(status)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&error).unwrap()))
                    .unwrap()
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let credentials = HttpCredentials::new(&server, "local-test-credential").unwrap();
        let client = HttpTaskService::new(&server)
            .unwrap()
            .with_credentials(credentials.clone())
            .unwrap();
        assert_eq!(
            client
                .status(
                    &Scope {
                        tenant_id: "tenant".into(),
                        namespace: "namespace".into()
                    },
                    "task"
                )
                .await
                .unwrap_err(),
            error
        );
        let publisher = HttpProgramPublisher::new(&server)
            .unwrap()
            .with_credentials(credentials)
            .unwrap();
        assert_eq!(publisher.capabilities().await.unwrap_err().kind, kind);
        assert_eq!(count.load(Ordering::SeqCst), 2);
        task.abort();
    }
}

#[tokio::test]
async fn credential_bearing_clients_never_follow_redirects() {
    let leaked = Arc::new(AtomicUsize::new(0));
    let observed = leaked.clone();
    let destination = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let location = format!("http://{}/sink", destination.local_addr().unwrap());
    let sink = tokio::spawn(async move {
        axum::serve(
            destination,
            Router::new().fallback(any(move || {
                observed.fetch_add(1, Ordering::SeqCst);
                async { StatusCode::OK }
            })),
        )
        .await
        .unwrap();
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = format!("http://{}", listener.local_addr().unwrap());
    let source = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().fallback(any(move || {
                let location = location.clone();
                async move {
                    Response::builder()
                        .status(307)
                        .header(header::LOCATION, location)
                        .body(Body::empty())
                        .unwrap()
                }
            })),
        )
        .await
        .unwrap();
    });
    let client = HttpTaskService::new(&server)
        .unwrap()
        .with_bearer_token("local-test-credential")
        .unwrap();
    assert!(
        client
            .status(
                &Scope {
                    tenant_id: "tenant".into(),
                    namespace: "namespace".into()
                },
                "task"
            )
            .await
            .is_err()
    );
    let publisher = HttpProgramPublisher::new(&server)
        .unwrap()
        .with_bearer_token("local-test-credential")
        .unwrap();
    assert!(publisher.capabilities().await.is_err());
    assert_eq!(leaked.load(Ordering::SeqCst), 0);
    source.abort();
    sink.abort();
}
