//! The production dispatcher uses the same authenticated HTTP reader as other clients.
use crate::operations::{ToolDispatcher, test_dispatcher};
use ledgence_adapter_http::{HttpProgramCatalogService, HttpTaskService};
use ledgence_orchestration_api::*;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

fn scope() -> Scope {
    Scope {
        tenant_id: "acme".into(),
        namespace: "billing".into(),
    }
}
fn occurrence() -> Value {
    json!({"scope":scope(),"workflow_id":"workflow","activation_id":"effect-activation","approval_key":"refund"})
}
fn identity() -> Value {
    json!({"workflow_id":"workflow","activation_id":"effect-activation","approval_key":"refund"})
}
fn summary() -> Value {
    json!({"occurrence":occurrence(),"program":{"id":"refund","version":"1"},
        "callable":"program:refund","action_version":"1","dispatch_provenance":{"kind":"legacy_attempt","attempt_id":"attempt"},
        "journal_state":"confirmed","has_discrepancy":false,"entry_count":2,"effect_revision":3,"created_at":0,"updated_at":15})
}
fn detail() -> Value {
    json!({"binding":{"occurrence":occurrence(),"approval":{"scope":scope(),"workflow_id":"workflow","key":"refund",
            "activation_id":"requested","revision":1,"action":{"name":"program:refund","version":"1","arguments":{"amount":50}},
            "proposed_arguments":null,"created_at":10,"deadline":100,"status":"approved","protection":"external_effect",
            "decision":{"decision_id":"decision","decision":"approve","reviewer":"operator","reason":null,"decided_at":11},
            "resumed_activation_id":"effect-activation"},
        "program":{"program":{"id":"refund","version":"1"},"digest":format!("sha256:{}","a".repeat(64)),"size":1},
        "key":approved_effect_step_key("refund"),"lookup":null,"approval_acceptance_event_id":null},
        "summary":summary(),"entries":[{"kind":"observation","sequence":1,"recorded_at":15,"recorded_revision":2,
            "observation_id":"observation","source":{"kind":"legacy_journal","attempt_id":"attempt","journal_accepted_at":0},
            "output":{"integer":u64::MAX,"float":1.0,"nul":"\u{0000}"}}],"next_cursor":null})
}

async fn dispatcher(
    replies: Vec<(u16, Value)>,
) -> (ToolDispatcher, JoinHandle<Vec<(String, Value)>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = HttpTaskService::new(&format!("http://{}", listener.local_addr().unwrap()))
        .unwrap()
        .with_bearer_token("operator-token")
        .unwrap();
    let catalog = HttpProgramCatalogService::new(http.clone(), scope()).unwrap();
    let dispatcher = ToolDispatcher::new(
        Arc::new(http.clone()),
        Arc::new(http),
        Arc::new(catalog),
        scope(),
        true,
    )
    .unwrap();
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for (status, body) in replies {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let (headers, value) = loop {
                let mut bytes = [0; 4096];
                let count = stream.read(&mut bytes).await.unwrap();
                assert_ne!(count, 0);
                request.extend_from_slice(&bytes[..count]);
                if let Some(end) = request.windows(4).position(|p| p == b"\r\n\r\n") {
                    let headers = std::str::from_utf8(&request[..end]).unwrap();
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|n| n.trim().parse().unwrap())
                        })
                        .unwrap();
                    if request.len() >= end + 4 + length {
                        break (
                            headers.to_owned(),
                            serde_json::from_slice(&request[end + 4..end + 4 + length]).unwrap(),
                        );
                    }
                }
            };
            requests.push((headers, value));
            let body = serde_json::to_string(&body).unwrap();
            let reply = format!(
                "HTTP/1.1 {status} Reply\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(reply.as_bytes()).await.unwrap();
        }
        requests
    });
    (dispatcher, server)
}

#[tokio::test]
async fn effect_tools_preserve_scoped_identity_authenticated_destination_and_exact_u64() {
    let mut item = summary();
    item["entry_count"] = json!(u64::MAX - 1);
    item["effect_revision"] = json!(u64::MAX);
    let mut inspected = detail();
    let query = EffectInspectQuery {
        occurrence: serde_json::from_value(occurrence()).unwrap(),
        cursor: None,
        limit: 4,
    };
    let cursor = query.next_cursor(1).unwrap();
    inspected["next_cursor"] = json!(cursor);
    let (dispatcher, server) = dispatcher(vec![
        (200, json!({"items":[item],"next_cursor":null})),
        (200, inspected.clone()),
    ])
    .await;
    let page = dispatcher
        .call("ledgence_effect_inbox", &json!({}))
        .await
        .unwrap();
    assert_eq!(page["items"][0]["effect_revision"].as_u64(), Some(u64::MAX));
    let result = dispatcher
        .call("ledgence_effect_inspect", &identity())
        .await
        .unwrap();
    assert_eq!(result, inspected);
    assert_eq!(
        result["entries"][0]["output"]["integer"].as_u64(),
        Some(u64::MAX)
    );
    assert!(result["entries"][0]["output"]["float"].is_f64());
    assert_eq!(result["next_cursor"], cursor); // A short page is a valid whole-entry page.
    let requests = server.await.unwrap();
    for (headers, _) in &requests {
        assert!(
            headers
                .to_ascii_lowercase()
                .contains("authorization: bearer operator-token\r\n")
        );
    }
    assert!(
        requests[0]
            .0
            .starts_with("POST /v1/workflows/approved-effects/inbox ")
    );
    assert_eq!(
        requests[0].1,
        json!({"scope":scope(),"filter":"all","workflow_id":null,"cursor":null,"limit":50})
    );
    assert!(
        requests[1]
            .0
            .starts_with("POST /v1/workflows/approved-effects/inspect ")
    );
    assert_eq!(
        requests[1].1,
        json!({"occurrence":occurrence(),"cursor":null,"limit":4})
    );
}

#[tokio::test]
async fn invalid_effect_arguments_never_reach_the_http_reader() {
    let dispatcher = test_dispatcher(true);
    for args in [
        json!({"scope":scope()}),
        json!({"tenant_id":"other"}),
        json!({"token":"pretend"}),
        json!({"filter":"pending"}),
        json!({"filter":null}),
        json!({"limit":0}),
        json!({"limit":101}),
        json!({"cursor":"abcd"}),
        json!({"workflow_id":""}),
    ] {
        let error = dispatcher
            .call("ledgence_effect_inbox", &args)
            .await
            .unwrap_err();
        assert_eq!(error.code, "invalid_arguments", "{args}");
        assert!(!error.outcome_unknown);
    }
    for (name, value) in [
        ("activation_id", json!("")),
        ("limit", json!(5)),
        ("limit", json!(true)),
        ("cursor", json!("abcd")),
        ("scope", json!(scope())),
        ("credential", json!("pretend")),
        ("owner", json!({})),
    ] {
        let mut args = identity();
        args[name] = value;
        let error = dispatcher
            .call("ledgence_effect_inspect", &args)
            .await
            .unwrap_err();
        assert_eq!(error.code, "invalid_arguments", "{args}");
    }
    assert_eq!(
        dispatcher
            .call("ledgence_effect_resolve", &identity())
            .await
            .unwrap_err()
            .code,
        "unknown_tool"
    );
}

#[tokio::test]
async fn effect_inspection_denial_is_not_an_empty_page_or_unknown_mutation() {
    let (dispatcher, server) = dispatcher(vec![(403, json!({"code":"forbidden"}))]).await;
    let error = dispatcher
        .call("ledgence_effect_inspect", &identity())
        .await
        .unwrap_err();
    assert_eq!(error.code, "forbidden");
    assert!(!error.outcome_unknown);
    assert_eq!(server.await.unwrap().len(), 1);
}

#[tokio::test]
async fn malformed_effect_pages_fail_closed_at_the_http_tool_boundary() {
    let mut wrong_scope = detail();
    wrong_scope["binding"]["occurrence"]["scope"]["namespace"] = json!("other");
    let mut private_owner = detail();
    private_owner["binding"]["owner"] = json!("never-public");
    let mut invalid_source = detail();
    invalid_source["entries"][0]["source"]["kind"] = json!("provider_verified");
    let mut repeated = detail();
    repeated["entries"] = json!([repeated["entries"][0], repeated["entries"][0]]);
    let mut wrong_cursor = detail();
    wrong_cursor["next_cursor"] = json!("abcd");
    let (dispatcher, server) = dispatcher(
        [
            wrong_scope,
            private_owner,
            invalid_source,
            repeated,
            wrong_cursor,
        ]
        .into_iter()
        .map(|reply| (200, reply))
        .collect(),
    )
    .await;
    for _ in 0..5 {
        let error = dispatcher
            .call("ledgence_effect_inspect", &identity())
            .await
            .unwrap_err();
        assert_eq!(error.code, "unavailable");
        assert!(!error.outcome_unknown);
    }
    server.await.unwrap();
}
