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
    dispatcher_with_mode(replies, true).await
}

async fn dispatcher_with_mode(
    replies: Vec<(u16, Value)>,
    read_only: bool,
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
        read_only,
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
    assert_eq!(page["items"][0]["effect_revision"], u64::MAX.to_string());
    assert_eq!(page["items"][0]["entry_count"], (u64::MAX - 1).to_string());
    let result = dispatcher
        .call("ledgence_effect_inspect", &identity())
        .await
        .unwrap();
    inspected["summary"]["effect_revision"] = json!("3");
    inspected["summary"]["entry_count"] = json!("2");
    inspected["binding"]["approval"]["revision"] = json!("1");
    inspected["binding"]["program"]["size"] = json!("1");
    inspected["entries"][0]["sequence"] = json!("1");
    inspected["entries"][0]["recorded_revision"] = json!("2");
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

fn resolution_args() -> Value {
    let mut args = identity();
    args.as_object_mut().unwrap().extend(json!({"resolution_id":"resolution-1","expected_effect_revision":(u64::MAX-1).to_string(),
        "supersedes_resolution_id":null,"action":{"name":"refund","version":"1","arguments":{"revision":1.0}},
        "assessment":{"kind":"record_success","result":{"sequence":u64::MAX,"revision":1.0}},
        "reason":"  Reviewed receipt\n","evidence_references":["receipt:1"]}).as_object().unwrap().clone());
    args
}
fn resolution_receipt() -> Value {
    json!({"occurrence":occurrence(),"resolution_id":"resolution-1","accepted_at":0,"accepted_revision":u64::MAX,
        "basis_revision":u64::MAX-1,"supersedes_resolution_id":null,"assessment_kind":"record_success",
        "acceptance_event_id":"audit-1","already_accepted":false})
}
#[tokio::test]
async fn resolution_is_opt_in_scope_bound_lossless_and_never_retried() {
    let (dispatcher, server) = dispatcher_with_mode(
        vec![
            (200, resolution_receipt()),
            (503, json!({"code":"unavailable","message":"lost reply"})),
        ],
        false,
    )
    .await;
    assert!(
        !dispatcher
            .definitions()
            .any(|tool| tool.name == "ledgence_effect_resolve")
    );
    assert_eq!(
        dispatcher
            .call("ledgence_effect_resolve", &resolution_args())
            .await
            .unwrap_err()
            .code,
        "unknown_tool"
    );
    let dispatcher = dispatcher.with_effect_resolution().unwrap();
    assert!(
        dispatcher
            .definitions()
            .any(|tool| tool.name == "ledgence_effect_resolve")
    );
    let receipt = dispatcher
        .call("ledgence_effect_resolve", &resolution_args())
        .await
        .unwrap();
    assert_eq!(receipt["accepted_revision"], u64::MAX.to_string());
    assert_eq!(receipt["basis_revision"], (u64::MAX - 1).to_string());
    assert_eq!(receipt["accepted_at"], 0);
    let error = dispatcher
        .call("ledgence_effect_resolve", &resolution_args())
        .await
        .unwrap_err();
    assert!(error.outcome_unknown);
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].1, requests[1].1);
    assert!(
        requests[0]
            .0
            .starts_with("POST /v1/workflows/approved-effects/resolve ")
    );
    assert!(
        requests[0]
            .0
            .to_ascii_lowercase()
            .contains("authorization: bearer operator-token")
    );
    assert_eq!(requests[0].1["expected_effect_revision"], u64::MAX - 1);
    assert_eq!(requests[0].1["occurrence"], occurrence());
    assert_eq!(requests[0].1["assessment"]["result"]["sequence"], u64::MAX);
    assert!(requests[0].1["assessment"]["result"]["revision"].is_f64());
}
#[tokio::test]
async fn resolution_rejects_invalid_revision_identity_and_scope_before_dispatch() {
    assert!(test_dispatcher(true).with_effect_resolution().is_err());
    let dispatcher = test_dispatcher(false).with_effect_resolution().unwrap();
    for value in [
        json!(1),
        json!(true),
        json!(null),
        json!("01"),
        json!("+1"),
        json!("0"),
        json!(u64::MAX.to_string()),
        json!("18446744073709551616"),
    ] {
        let mut args = resolution_args();
        args["expected_effect_revision"] = value;
        let error = dispatcher
            .call("ledgence_effect_resolve", &args)
            .await
            .unwrap_err();
        assert_eq!(error.code, "invalid_arguments");
        assert!(!error.outcome_unknown);
    }
    for (name, value) in [
        ("scope", json!(scope())),
        ("actor", json!({})),
        ("token", json!("forbidden")),
        ("reason", json!("  ")),
        ("evidence_references", json!([])),
    ] {
        let mut args = resolution_args();
        args[name] = value;
        assert_eq!(
            dispatcher
                .call("ledgence_effect_resolve", &args)
                .await
                .unwrap_err()
                .code,
            "invalid_arguments"
        );
    }
    let mut args = resolution_args();
    args.as_object_mut()
        .unwrap()
        .remove("supersedes_resolution_id");
    assert_eq!(
        dispatcher
            .call("ledgence_effect_resolve", &args)
            .await
            .unwrap_err()
            .code,
        "invalid_arguments"
    );
}
#[tokio::test]
async fn resolution_denial_is_definite_but_bad_accepted_receipt_is_uncertain() {
    let mut malformed = resolution_receipt();
    malformed["resolution_id"] = json!("wrong");
    let (dispatcher, server) = dispatcher_with_mode(
        vec![
            (403, json!({"code":"forbidden"})),
            (409, json!({"code":"conflict"})),
            (200, malformed),
        ],
        false,
    )
    .await;
    let dispatcher = dispatcher.with_effect_resolution().unwrap();
    for code in ["forbidden", "conflict"] {
        let error = dispatcher
            .call("ledgence_effect_resolve", &resolution_args())
            .await
            .unwrap_err();
        assert_eq!(error.code, code);
        assert!(!error.outcome_unknown);
    }
    assert!(
        dispatcher
            .call("ledgence_effect_resolve", &resolution_args())
            .await
            .unwrap_err()
            .outcome_unknown
    );
    assert_eq!(server.await.unwrap().len(), 3);
}

fn provider_detail() -> Value {
    let mut value: Value = serde_json::from_slice(include_bytes!(
        "../../../sdk/python-client/tests/fixtures/provider-effect.json"
    ))
    .unwrap();
    value["binding"]["occurrence"]["scope"] = json!(scope());
    value["binding"]["approval"]["scope"] = json!(scope());
    value["summary"]["occurrence"]["scope"] = json!(scope());
    for entry in value["entries"].as_array_mut().unwrap() {
        entry["worker"]["audience"]["scope"] = json!(scope());
    }
    value["provider"]["current_report"]["worker"]["audience"]["scope"] = json!(scope());
    value
}

#[tokio::test]
async fn provider_inspection_preserves_bound_target_and_stringifies_only_platform_counters() {
    let detail = provider_detail();
    let (dispatcher, server) = dispatcher(vec![(200, detail)]).await;
    let observed = dispatcher
        .call("ledgence_effect_inspect", &identity())
        .await
        .unwrap();
    assert_eq!(
        observed["summary"]["provider"]["selected_report"]["recorded_revision"],
        "3"
    );
    assert_eq!(observed["summary"]["provider"]["candidate_revision"], "3");
    assert_eq!(
        observed["summary"]["provider"]["reconciliation"]["generation"],
        "0"
    );
    assert_eq!(
        observed["summary"]["provider"]["reconciliation"]["max_checks"],
        3
    );
    assert_eq!(
        observed["binding"]["approval"]["provider_binding"]["program"]["size"],
        "1"
    );
    assert_eq!(
        observed["provider"]["current_report"]["recorded_revision"],
        "3"
    );
    assert!(observed["provider"]["current_report"]["report"]["knowledge"]["result"].is_null());
    assert_eq!(observed["provider"]["current_report"]["recorded_at"], 15);
    assert_eq!(
        observed["binding"]["approval"]["provider_binding"]["target"]["account"],
        "demo"
    );
    assert_eq!(server.await.unwrap().len(), 1);
}

#[tokio::test]
async fn provider_inbox_filter_is_explicit_scoped_and_disallows_mixed_or_null_filters() {
    let detail = provider_detail();
    let (dispatcher, server) = dispatcher(vec![(
        200,
        json!({"items":[detail["summary"]],"next_cursor":null}),
    )])
    .await;
    for args in [
        json!({"provider_filter":null}),
        json!({"provider_filter":"invented"}),
        json!({"provider_filter":"all","filter":"unconfirmed"}),
    ] {
        assert!(
            dispatcher
                .call("ledgence_effect_inbox", &args)
                .await
                .is_err()
        );
    }
    let page = dispatcher
        .call(
            "ledgence_effect_inbox",
            &json!({"provider_filter":"succeeded"}),
        )
        .await
        .unwrap();
    assert_eq!(page["items"][0]["provider"]["knowledge"], "succeeded");
    let calls = server.await.unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].1["provider_filter"], "succeeded");
    assert_eq!(calls[0].1["filter"], "all");
    assert_eq!(calls[0].1["scope"], json!(scope()));
}
