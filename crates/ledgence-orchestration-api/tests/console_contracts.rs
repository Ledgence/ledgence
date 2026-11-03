use ledgence_orchestration_api::{console::*, *};
use serde_json::{Value, json};
#[allow(dead_code)]
#[path = "support/console_fixtures.rs"]
mod fixture;

fn scope() -> Scope {
    Scope {
        tenant_id: "acme".into(),
        namespace: "billing".into(),
    }
}
#[test]
fn committed_fixtures_are_serialized_from_rust_contracts() {
    let actual = serde_json::to_string_pretty(&fixture::fixtures()).unwrap() + "\n";
    assert_eq!(
        actual,
        include_str!("fixtures/console-v8.json"),
        "regenerate with cargo run -p ledgence-orchestration-api --example console-fixtures -- --write"
    );
    let data = fixture::fixtures();
    let page: ConsolePage<ConsoleTaskSummary> =
        serde_json::from_value(data["tasks"].clone()).unwrap();
    let query = ConsoleQuery::Tasks {
        filters: TaskFilters::default(),
        page: ConsolePagination::default(),
    };
    ConsoleQueryReply::Tasks(page)
        .validate(&scope(), &query)
        .unwrap();
    assert!(data["pending_result"]["outcome"].is_null());
    assert_eq!(data["null_result"]["outcome"]["kind"], "succeeded");
    assert!(data["null_result"]["outcome"]["output"].is_null());
}

#[test]
fn historical_console_five_is_preserved_but_not_current() {
    let old: Value = serde_json::from_str(include_str!("fixtures/console-v5.json")).unwrap();
    assert_eq!(old["contract_version"], 5);
    assert_eq!(CONSOLE_CONTRACT_VERSION, 8);
    assert!(serde_json::from_value::<ConsoleWorkflowExplorer>(old["explorer"].clone()).is_err());
}

#[test]
fn historical_console_six_lacks_provider_provenance() {
    let old: Value = serde_json::from_str(include_str!("fixtures/console-v6.json")).unwrap();
    assert_eq!(old["contract_version"], 6);
    assert_eq!(CONSOLE_CONTRACT_VERSION, 8);
    assert!(serde_json::from_value::<ConsoleWorkflowExplorer>(old["explorer"].clone()).is_err());
    assert!(
        serde_json::from_value::<ConsolePage<ConsoleTaskSummary>>(old["tasks"].clone()).is_err()
    );
}

#[test]
fn provider_probe_provenance_is_explicit_and_bound_to_its_owner() {
    let fixture = fixture::fixtures();
    let status: ConsoleObservedTaskStatus =
        serde_json::from_value(fixture["provider_probe_status"].clone()).unwrap();
    status.task.validate().unwrap();
    let detail: ConsoleTaskDetail =
        serde_json::from_value(fixture["provider_probe_detail"].clone()).unwrap();
    detail.validate().unwrap();
    let mut execution: ConsoleExecutionSummary =
        serde_json::from_value(fixture["provider_probe_execution"].clone()).unwrap();
    execution.validate().unwrap();
    assert_eq!(
        fixture["provider_probe_execution"]["provider_probe"]["generation"],
        "3"
    );
    execution.parent_workflow_id = Some("another-workflow".into());
    assert!(execution.validate().is_err());
}

#[test]
fn historical_console_two_is_explicitly_incompatible() {
    let historical: Value =
        serde_json::from_str(include_str!("fixtures/historical/console-v2.json")).unwrap();
    assert_eq!(historical["contract_version"], 2);
    assert_eq!(historical["config"]["contract_version"], 2);
    assert_eq!(CONSOLE_CONTRACT_VERSION, 8);
    assert_ne!(
        historical["contract_version"],
        json!(CONSOLE_CONTRACT_VERSION)
    );
}

#[test]
fn historical_console_three_lacks_required_durable_relations() {
    let historical: Value =
        serde_json::from_str(include_str!("fixtures/historical/console-v3.json")).unwrap();
    assert_eq!(historical["contract_version"], 3);
    assert_ne!(
        historical["config"]["contract_version"],
        json!(CONSOLE_CONTRACT_VERSION)
    );
    assert!(
        serde_json::from_value::<ConsoleWorkflowExplorer>(historical["explorer"].clone()).is_err()
    );
}

#[test]
fn canonical_explorer_matrix_and_partial_pages_validate_without_inferred_edges() {
    let data = fixture::fixtures();
    let full: ConsoleWorkflowExplorer = serde_json::from_value(data["explorer"].clone()).unwrap();
    let validate = |reply: ConsoleWorkflowExplorer, page: ConsolePagination| {
        let query = ConsoleQuery::Explorer {
            workflow_id: reply.workflow.summary.workflow.workflow_id.clone(),
            page,
        };
        ConsoleQueryReply::Explorer(reply)
            .validate(&scope(), &query)
            .unwrap();
    };
    validate(
        full.clone(),
        ConsolePagination {
            limit: 100,
            cursor: None,
        },
    );
    for value in data["explorer_cases"].as_object().unwrap().values() {
        validate(
            serde_json::from_value(value.clone()).unwrap(),
            ConsolePagination {
                limit: 100,
                cursor: None,
            },
        );
    }
    let mut previous_cursor: Option<String> = None;
    let mut all_ids = Vec::new();
    for page in data["explorer_pages"].as_array().unwrap() {
        let request: ConsolePagination = serde_json::from_value(page["request"].clone()).unwrap();
        assert_eq!(request.cursor, previous_cursor);
        let reply: ConsoleWorkflowExplorer =
            serde_json::from_value(page["response"].clone()).unwrap();
        previous_cursor = reply.page.next_cursor.clone();
        all_ids.extend(reply.page.items.iter().map(|node| node.id.clone()));
        validate(reply, request);
    }
    assert!(previous_cursor.is_none());
    assert_eq!(
        all_ids,
        full.page
            .items
            .iter()
            .map(|node| node.id.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        all_ids.len(),
        all_ids
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
    );
    let forks = full
        .page
        .items
        .iter()
        .filter_map(|node| {
            if let ConsoleExplorerData::Fork {
                key, branch_keys, ..
            } = &node.data
            {
                Some((key, branch_keys))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(forks.len(), 1);
    assert_eq!(forks[0].0, "release-checks:0");
    assert_eq!(
        forks[0].1,
        &["security:0", "tests:0", "dependencies:0", "docs:0"]
    );
    let branch_count = full.page.items.iter().filter(|node| matches!(&node.data, ConsoleExplorerData::Child { fork_key: Some(key), execution, .. } if key == "release-checks:0" && execution.kind == ConsoleExecutionKind::Workflow)).count();
    assert_eq!(branch_count, 4);
    assert!(full.page.items.iter().any(|node| matches!(&node.data, ConsoleExplorerData::Child { key, fork_key: None, .. } if key == "prepare:0")));
    let repeated: ConsoleWorkflowExplorer =
        serde_json::from_value(data["explorer_cases"]["repeated_entrypoint"].clone()).unwrap();
    let reviews = repeated
        .page
        .items
        .iter()
        .filter(|node| node.entrypoint == "review")
        .collect::<Vec<_>>();
    assert_eq!(reviews.len(), 2);
    assert_ne!(reviews[0].activation_id, reviews[1].activation_id);
    assert_ne!(reviews[0].id, reviews[1].id);
    let retry: ConsoleWorkflowExplorer =
        serde_json::from_value(data["explorer_cases"]["retry"].clone()).unwrap();
    assert_eq!(
        retry
            .page
            .items
            .iter()
            .filter(|node| matches!(node.data, ConsoleExplorerData::Entrypoint { .. }))
            .count(),
        1
    );
    let attempts: ConsolePage<ConsoleAttemptSummary> =
        serde_json::from_value(data["entrypoint_attempts"].clone()).unwrap();
    assert_eq!(attempts.items.len(), 2);
    assert!(
        attempts
            .items
            .iter()
            .all(|item| item.task_id == retry.page.items[0].activation_id)
    );
    ConsoleQueryReply::Attempts(attempts)
        .validate(
            &scope(),
            &ConsoleQuery::Attempts {
                task_id: retry.page.items[0].activation_id.clone(),
                page: Default::default(),
            },
        )
        .unwrap();
    let rows: ConsolePage<ConsoleExecutionSummary> =
        serde_json::from_value(data["execution_history"].clone()).unwrap();
    ConsoleQueryReply::Executions(rows)
        .validate(
            &scope(),
            &ConsoleQuery::Executions {
                filters: ConsoleExecutionFilters {
                    include_children: true,
                    ..Default::default()
                },
                page: Default::default(),
            },
        )
        .unwrap();
    let ancestry: ConsoleAncestry = serde_json::from_value(data["ancestry"].clone()).unwrap();
    ancestry.validate(&ancestry.execution).unwrap();
}
#[test]
fn unsigned_wire_values_are_canonical_strings() {
    assert_eq!(
        serde_json::to_string(&ConsoleU64(u64::MAX)).unwrap(),
        "\"18446744073709551615\""
    );
    for invalid in [
        r#"0"#,
        r#""01""#,
        r#""+1""#,
        r#""-1""#,
        r#""1.0""#,
        r#""18446744073709551616""#,
    ] {
        assert!(
            serde_json::from_str::<ConsoleU64>(invalid).is_err(),
            "{invalid}"
        );
    }
}
#[test]
fn cursor_binds_scope_endpoint_parent_filters_and_position_type() {
    let page = ConsolePagination {
        limit: 1,
        cursor: None,
    };
    let query = ConsoleQuery::Attempts {
        task_id: "task_a".into(),
        page: page.clone(),
    };
    let binding = query.binding(&scope()).unwrap();
    let position = vec![
        ConsoleKey::Number(ConsoleU64(7)),
        ConsoleKey::Text("attempt_7".into()),
    ];
    let cursor = page.next_cursor(&binding, &position).unwrap();
    let page = ConsolePagination {
        limit: 100,
        cursor: Some(cursor),
    };
    assert_eq!(page.validate(&binding).unwrap(), Some(position));
    let mut changed = binding.clone();
    changed.scope.namespace = "other".into();
    assert!(page.validate(&changed).is_err());
    changed = binding.clone();
    changed.endpoint = "workflows/activations";
    assert!(page.validate(&changed).is_err());
    changed = binding.clone();
    changed.parent = vec!["task_b".into()];
    assert!(page.validate(&changed).is_err());
    changed = binding.clone();
    changed.filters = json!({"state":"queued"});
    assert!(page.validate(&changed).is_err());
    changed = binding;
    changed.numeric_keys = vec![false, false];
    assert!(page.validate(&changed).is_err());
}
#[test]
fn invalid_adapter_order_filter_parent_and_variant_are_rejected() {
    let data = fixture::fixtures();
    let page: ConsolePage<ConsoleTaskSummary> =
        serde_json::from_value(data["tasks"].clone()).unwrap();
    let query = ConsoleQuery::Tasks {
        filters: TaskFilters {
            queue: Some("wrong".into()),
            ..Default::default()
        },
        page: ConsolePagination::default(),
    };
    assert!(
        ConsoleQueryReply::Tasks(page.clone())
            .validate(&scope(), &query)
            .is_err()
    );
    let query = ConsoleQuery::Tasks {
        filters: TaskFilters::default(),
        page: ConsolePagination::default(),
    };
    let mut duplicate = page.clone();
    duplicate.items.push(page.items[0].clone());
    assert!(
        ConsoleQueryReply::Tasks(duplicate)
            .validate(&scope(), &query)
            .is_err()
    );
    let query = ConsoleQuery::Workflows {
        filters: ConsoleWorkflowFilters::default(),
        page: ConsolePagination::default(),
    };
    assert!(
        ConsoleQueryReply::Tasks(page)
            .validate(&scope(), &query)
            .is_err()
    );
}
#[test]
fn submission_rejects_browser_binding_and_preserves_numeric_categories() {
    let data = fixture::fixtures()["numeric_payload"].clone();
    let mut body = json!({"program":{"id":"invoice-issuer","version":"release-a"},"queue":"billing","data":data});
    let parsed: ConsoleSubmitTask =
        decode_unique_json(&serde_json::to_vec(&body).unwrap(), SUBMISSION_MAX_BYTES).unwrap();
    let input = parsed.into_submission(&scope()).unwrap();
    let encoded = serde_json::to_string(&input.data).unwrap();
    assert!(encoded.contains("9007199254740993"));
    assert!(encoded.contains("18446744073709551615"));
    assert!(encoded.contains("-0.0"));
    body["scope"] = json!({"tenant_id":"another","namespace":"other"});
    assert!(
        decode_unique_json::<ConsoleSubmitTask>(
            &serde_json::to_vec(&body).unwrap(),
            SUBMISSION_MAX_BYTES
        )
        .is_err()
    );
}
#[test]
fn fixture_metadata_contains_no_authority_or_payload_fields() {
    fn walk(value: &Value) {
        match value {
            Value::Object(map) => {
                for (key, value) in map {
                    // C6's explicitly typed effect address carries the bound
                    // scope as metadata, never as lease or dispatch authority.
                    if key == "occurrence" {
                        let occurrence: EffectOccurrence =
                            serde_json::from_value(value.clone()).unwrap();
                        occurrence.validate().unwrap();
                        assert_eq!(occurrence.scope, scope());
                        continue;
                    }
                    assert!(
                        ![
                            "scope",
                            "tenant_id",
                            "namespace",
                            "owner",
                            "lease_id",
                            "input",
                            "data",
                            "output",
                            "checkpoint",
                            "argv",
                            "env"
                        ]
                        .contains(&key.as_str()),
                        "forbidden {key}"
                    );
                    walk(value)
                }
            }
            Value::Array(items) => {
                for item in items {
                    walk(item)
                }
            }
            _ => (),
        }
    }
    let data = fixture::fixtures();
    for key in [
        "config",
        "tasks",
        "workflows",
        "explorer",
        "explorer_cases",
        "execution_history",
        "ancestry",
        "entrypoint_attempts",
    ] {
        walk(&data[key]);
    }
}

#[test]
fn display_metadata_encoded_bound_preserves_all_text_budgets() {
    let maximum = ProgramDisplayMetadata {
        display_name: Some("\\".repeat(128)),
        description: Some("\t\n\\\"".repeat(1024)),
        kind: ConsoleProgramKind::Unspecified,
    };
    maximum.validate().unwrap();
    assert_eq!(PROGRAM_DISPLAY_METADATA_MAX_BYTES, 8505);
    assert_eq!(
        serde_json::to_vec(&maximum).unwrap().len(),
        PROGRAM_DISPLAY_METADATA_MAX_BYTES
    );
    for name in ["\\", "\"", "a", "é", "雪", "🦀"] {
        for description in ["\t", "\n", "\\", "\"", "a", "é", "雪", "🦀"] {
            for kind in [
                ConsoleProgramKind::Task,
                ConsoleProgramKind::Workflow,
                ConsoleProgramKind::Unspecified,
            ] {
                let value = ProgramDisplayMetadata {
                    display_name: Some(name.repeat(128 / name.len())),
                    description: Some(description.repeat(4096 / description.len())),
                    kind,
                };
                value.validate().unwrap();
                assert!(
                    serde_json::to_vec(&value).unwrap().len() <= PROGRAM_DISPLAY_METADATA_MAX_BYTES
                );
            }
        }
    }
    let mut too_long = maximum.clone();
    too_long.display_name.as_mut().unwrap().push('a');
    assert!(too_long.validate().is_err());
    let mut too_long = maximum;
    too_long.description.as_mut().unwrap().push('a');
    assert!(too_long.validate().is_err());
}

#[test]
fn short_pages_continue_from_last_item_but_empty_or_mismatched_cursors_are_rejected() {
    let request = ConsolePagination {
        limit: 100,
        cursor: None,
    };
    let query = ProgramCatalogQuery::Programs(request.clone());
    let binding = query.binding(&scope()).unwrap();
    let mut reply: ConsolePage<ConsoleProgramSummary> =
        serde_json::from_value(fixture::fixtures()["programs"].clone()).unwrap();
    assert_eq!(reply.items.len(), 1);
    let cursor = request
        .next_cursor(&binding, &reply.items[0].position())
        .unwrap();
    reply.next_cursor = Some(cursor.clone());
    reply.validate(&request, &binding).unwrap();
    let mut empty = reply.clone();
    empty.items.clear();
    assert!(empty.validate(&request, &binding).is_err());
    let mut mismatched = reply.clone();
    mismatched.next_cursor = Some(
        request
            .next_cursor(&binding, &vec![ConsoleKey::Text("other".into())])
            .unwrap(),
    );
    assert!(mismatched.validate(&request, &binding).is_err());
    let resumed = ConsolePagination {
        cursor: Some(cursor),
        ..request
    };
    assert!(
        reply.validate(&resumed, &binding).is_err(),
        "continuation must advance past the previous page"
    );
}

#[test]
fn admission_wait_and_acceptance_project_distinct_durable_relationships() {
    let f = fixture::fixtures();
    for name in ["pending", "accepted", "closed"] {
        let e: ConsoleWorkflowExplorer =
            serde_json::from_value(f["admission"][name].clone()).unwrap();
        ConsoleQueryReply::Explorer(e.clone())
            .validate(
                &scope(),
                &ConsoleQuery::Explorer {
                    workflow_id: "wf_admission".into(),
                    page: ConsolePagination {
                        limit: 100,
                        cursor: None,
                    },
                },
            )
            .unwrap();
        let wait = e
            .page
            .items
            .iter()
            .find(|n| matches!(n.data, ConsoleExplorerData::AdmissionWait { .. }))
            .unwrap();
        assert_eq!(
            wait.relations
                .iter()
                .filter(|r| r.kind == ConsoleExplorerRelationKind::Resumes)
                .count(),
            usize::from(name == "accepted")
        );
        assert!(
            !wait
                .relations
                .iter()
                .any(|r| r.kind == ConsoleExplorerRelationKind::Invokes)
        );
    }
    let mut accepted: ConsoleWorkflowExplorer =
        serde_json::from_value(f["admission"]["accepted"].clone()).unwrap();
    let op = accepted
        .page
        .items
        .iter_mut()
        .find(|n| matches!(n.data, ConsoleExplorerData::AdmissionOperation { .. }))
        .unwrap();
    assert_eq!(op.relations.len(), 1);
    assert_eq!(op.relations[0].kind, ConsoleExplorerRelationKind::Invokes);
    assert_eq!(
        op.relations[0].source,
        ConsoleExplorerReference::Entrypoint {
            activation_id: op.activation_id.clone()
        }
    );
    let encoded = serde_json::to_string(&op).unwrap();
    assert!(!encoded.contains("arguments"));
    assert!(!encoded.contains("summary\":\"accepted"));
    if let ConsoleExplorerData::AdmissionOperation { occurrence, .. } = &mut op.data {
        occurrence.scope.namespace = "foreign".into();
    }
    assert!(
        ConsoleQueryReply::Explorer(accepted)
            .validate(
                &scope(),
                &ConsoleQuery::Explorer {
                    workflow_id: "wf_admission".into(),
                    page: ConsolePagination {
                        limit: 100,
                        cursor: None
                    }
                }
            )
            .is_err()
    );
}
#[test]
fn c8_requires_admission_wait_field_and_rejects_conflicting_current_waits() {
    let f = fixture::fixtures();
    let old: Value = serde_json::from_str(include_str!("fixtures/console-v7.json")).unwrap();
    assert_eq!(old["contract_version"], 7);
    assert!(
        serde_json::from_value::<ConsoleWorkflowDetail>(old["workflow_detail"].clone()).is_err()
    );
    let mut pending: ConsoleWorkflowExplorer =
        serde_json::from_value(f["admission"]["pending"].clone()).unwrap();
    pending.workflow.external_wait_key = Some("ordinary-event".into());
    assert!(
        ConsoleQueryReply::Explorer(pending)
            .validate(
                &scope(),
                &ConsoleQuery::Explorer {
                    workflow_id: "wf_admission".into(),
                    page: ConsolePagination {
                        limit: 100,
                        cursor: None
                    }
                }
            )
            .is_err()
    );
}
