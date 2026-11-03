use super::*;

impl EffectResolution for Mock {
    fn resolve_effect<'a>(
        &'a self,
        p: &'a VerifiedPrincipal,
        c: &'a EffectResolutionCommand,
    ) -> ContractFuture<'a, AuditedEffectResolution> {
        Box::pin(async move {
            self.calls
                .lock()
                .unwrap()
                .push(("resolve_effect", serde_json::to_value(c).unwrap()));
            if self.resolution_mode.load(Ordering::SeqCst) == 3 {
                return Err(ContractError::Forbidden);
            }
            let mut receipt = EffectResolutionReceipt {
                occurrence: c.occurrence.clone(),
                resolution_id: c.resolution_id.clone(),
                accepted_at: 30,
                accepted_revision: c.expected_effect_revision + 1,
                basis_revision: c.expected_effect_revision,
                supersedes_resolution_id: c.supersedes_resolution_id.clone(),
                assessment_kind: c.assessment.kind(),
                acceptance_event_id: "accepted-resolution".into(),
                already_accepted: false,
            };
            let mut event = AuditEvent {
                event_id: receipt.acceptance_event_id.clone(),
                recorded_at: receipt.accepted_at,
                audience: p.audience().clone(),
                actor: AuditActor::from_principal(p),
                operation: receipt.audit_operation(),
            };
            if self.resolution_mode.load(Ordering::SeqCst) == 1 {
                event.actor.subject = "foreign".into();
            }
            if self.resolution_mode.load(Ordering::SeqCst) == 2 {
                receipt.resolution_id = "foreign".into();
            }
            Ok(AuditedEffectResolution {
                receipt,
                acceptance_event: event,
            })
        })
    }
}
fn command() -> EffectResolutionCommand {
    EffectResolutionCommand {
        occurrence: effect_evidence::occurrence(),
        resolution_id: "resolution".into(),
        expected_effect_revision: 9007199254740993,
        supersedes_resolution_id: None,
        action: approval().action,
        assessment: EffectResolutionAssessment::RecordSuccess {
            result: json!({"private":CANARY}),
        },
        reason: "Reviewed receipt".into(),
        evidence_references: vec!["provider:receipt:1".into()],
    }
}
fn console_command(c: &EffectResolutionCommand) -> Value {
    let mut value = serde_json::to_value(c).unwrap();
    value["occurrence"].as_object_mut().unwrap().remove("scope");
    value["expected_effect_revision"] = json!(c.expected_effect_revision.to_string());
    value
}
#[tokio::test]
async fn resolution_client_rejects_mismatched_success_without_automatic_resubmission() {
    let c = command();
    let receipt = EffectResolutionReceipt {
        occurrence: c.occurrence.clone(),
        resolution_id: "foreign".into(),
        accepted_at: 30,
        accepted_revision: c.expected_effect_revision + 1,
        basis_revision: c.expected_effect_revision,
        supersedes_resolution_id: None,
        assessment_kind: c.assessment.kind(),
        acceptance_event_id: "event".into(),
        already_accepted: false,
    };
    let (running, count) = raw_response(
        response(
            200,
            "application/json",
            &serde_json::to_vec(&receipt).unwrap(),
        ),
        None,
    )
    .await;
    let client = HttpTaskService::new(&running.url).unwrap();
    assert!(matches!(
        EffectResolutionClient::resolve_effect(&client, &c).await,
        Err(ContractError::Unavailable(_))
    ));
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn frozen_resolution_bytes_are_transmitted_without_reencoding() {
    let c = command();
    let bytes = format!("  {}\n", serde_json::to_string_pretty(&c).unwrap()).into_bytes();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client =
        HttpTaskService::new(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let expected = bytes.clone();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0u8; 1024];
        let header_end = loop {
            let n = stream.read(&mut buffer).await.unwrap();
            assert!(n > 0);
            request.extend_from_slice(&buffer[..n]);
            if let Some(end) = request.windows(4).position(|b| b == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let headers = std::str::from_utf8(&request[..header_end]).unwrap();
        assert!(headers.starts_with("POST /v1/workflows/approved-effects/resolve HTTP/1.1\r\n"));
        let length: usize = headers
            .lines()
            .find_map(|line| {
                line.split_once(':')
                    .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .map(|(_, value)| value.trim().parse().unwrap())
            })
            .unwrap();
        while request.len() < header_end + length {
            let n = stream.read(&mut buffer).await.unwrap();
            assert!(n > 0);
            request.extend_from_slice(&buffer[..n]);
        }
        assert_eq!(&request[header_end..], expected);
        stream
            .write_all(&response(
                503,
                "application/json",
                &serde_json::to_vec(&ContractError::Unavailable("reply lost".into())).unwrap(),
            ))
            .await
            .unwrap();
    });
    assert!(matches!(
        client.resolve_effect_bytes(&bytes).await,
        Err(ContractError::Unavailable(_))
    ));
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn resolution_requires_dedicated_privilege_and_returns_only_bound_metadata() {
    let f = setup().await;
    f.mock.evidence_capabilities.store(7, Ordering::SeqCst);
    let c = command();
    for path in [
        "/v1/workflows/approved-effects/resolve",
        "/v1/console/workflows/approved-effects/resolve",
    ] {
        let body = if path.contains("/console/") {
            console_command(&c)
        } else {
            serde_json::to_value(&c).unwrap()
        };
        for token in ["reader", "operator", "worker", "invalid"] {
            let before = f.mock.calls.lock().unwrap().len();
            let (status, _) = request(&f, path, Some(body.clone()), token).await;
            assert_eq!(status, if token == "invalid" { 401 } else { 403 });
            assert_eq!(f.mock.calls.lock().unwrap().len(), before);
        }
        let (status, reply) = request(&f, path, Some(body), "resolver").await;
        assert_eq!(status, 200, "{reply}");
        assert!(!reply.to_string().contains(CANARY));
        assert_eq!(
            reply["occurrence"],
            serde_json::to_value(&c.occurrence).unwrap()
        );
        if path.contains("/console/") {
            assert_eq!(
                reply["accepted_revision"],
                json!((c.expected_effect_revision + 1).to_string())
            );
        } else {
            assert_eq!(
                reply["accepted_revision"],
                json!(c.expected_effect_revision + 1)
            );
        }
    }
    assert!(
        f.auditor.observations.lock().unwrap().is_empty(),
        "metadata receipt must not create a read audit"
    );
}
#[tokio::test]
async fn bad_acceptance_proof_is_uncertain_and_malformed_commands_do_not_dispatch() {
    let f = setup().await;
    f.mock.evidence_capabilities.store(7, Ordering::SeqCst);
    let c = command();
    let client = HttpTaskService::new(&f.running.url)
        .unwrap()
        .with_bearer_token("resolver")
        .unwrap();
    for mode in [1, 2] {
        f.mock.resolution_mode.store(mode, Ordering::SeqCst);
        assert!(matches!(
            client.resolve_effect(&c).await,
            Err(ContractError::Unavailable(_))
        ));
    }
    f.mock.resolution_mode.store(3, Ordering::SeqCst);
    assert_eq!(
        client.resolve_effect(&c).await.unwrap_err(),
        ContractError::Forbidden
    );
    let before = f.mock.calls.lock().unwrap().len();
    for console in [false, true] {
        let path = if console {
            "/v1/console/workflows/approved-effects/resolve"
        } else {
            "/v1/workflows/approved-effects/resolve"
        };
        let value = if console {
            console_command(&c)
        } else {
            serde_json::to_value(&c).unwrap()
        };
        for field in ["supersedes_resolution_id", "action", "assessment"] {
            let mut bad = value.clone();
            bad.as_object_mut().unwrap().remove(field);
            assert_eq!(request(&f, path, Some(bad), "resolver").await.0, 400);
        }
        let mut bad = value.clone();
        bad["actor"] = json!("forged");
        assert_eq!(request(&f, path, Some(bad), "resolver").await.0, 400);
        if console {
            let mut bad = value;
            bad["expected_effect_revision"] = json!(c.expected_effect_revision);
            assert_eq!(request(&f, path, Some(bad), "resolver").await.0, 400);
        }
    }
    assert_eq!(f.mock.calls.lock().unwrap().len(), before);
}
#[tokio::test]
async fn startup_features_are_explicit_complete_and_preserve_unrequested_a_wire() {
    let f = setup().await;
    let http = reqwest::Client::new();
    for bits in 0..8 {
        f.mock.evidence_capabilities.store(bits, Ordering::SeqCst);
        for opt in [false, true] {
            let mut request = http
                .get(format!("{}/v1/access", f.running.url))
                .bearer_auth("worker");
            if opt {
                request = request.header("Ledgence-Worker-Features", "1");
            }
            let response = request.send().await.unwrap();
            assert_eq!(response.status(), 200);
            let value: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
            assert_eq!(value.get("worker_features").is_some(), opt && bits == 7);
            assert!(value.get("native_effect_resolution").is_none());
            if bits & 3 == 3 {
                assert_eq!(
                    value["runtime_capabilities"],
                    json!({"approved_effect_evidence_v2":true})
                );
            }
        }
        let response = http
            .get(format!("{}/v1/console/access", f.running.url))
            .bearer_auth("operator")
            .send()
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        assert_eq!(value["native_effect_resolution"], bits == 7);
    }
    for values in [vec!["2"], vec!["1", "1"]] {
        let mut request = http
            .get(format!("{}/v1/access", f.running.url))
            .bearer_auth("worker");
        for value in values {
            request = request.header("Ledgence-Worker-Features", value);
        }
        assert_eq!(request.send().await.unwrap().status(), 400);
    }
    assert!(f.mock.calls.lock().unwrap().is_empty());
}
