use super::*;
use serde_json::json;

fn scope() -> Scope {
    Scope {
        tenant_id: "acme".into(),
        namespace: "billing".into(),
    }
}
fn status(id: &str, at: Timestamp) -> TaskStatus {
    TaskStatus {
        scope: scope(),
        task_id: id.into(),
        run_id: format!("run_{id}"),
        queue: "invoices".into(),
        correlation_key: Some("INV-1".into()),
        state: TaskState::Queued,
        attempt_count: 0,
        current_attempt_id: None,
        latest_attempt_id: None,
        submitted_at: at,
        available_at: at,
        terminal_at: None,
        cancel_requested_at: None,
    }
}
fn full_query() -> TaskListQuery {
    TaskListQuery {
        filters: TaskFilters {
            state: Some(TaskState::Queued),
            queue: Some("invoices".into()),
            submitted_from: Some(10),
            submitted_until: Some(20),
            correlation_key: Some("INV-1".into()),
        },
        limit: 2,
        cursor: None,
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn defaults_and_bounded_filter_validation_preserve_exact_business_metadata() {
    let query: TaskListQuery = serde_json::from_str("{}").unwrap();
    assert_eq!(query, TaskListQuery::default());
    assert_eq!(query.limit, 50);
    query.validate(&scope()).unwrap();
    for limit in [0, 101, u32::MAX] {
        assert!(
            TaskListQuery {
                limit,
                ..query.clone()
            }
            .validate(&scope())
            .is_err()
        );
    }
    for correlation in ["", "INV-1", "é", "\u{ffff}"] {
        let filters = TaskFilters {
            correlation_key: Some(correlation.into()),
            ..TaskFilters::default()
        };
        filters.validate().unwrap();
        let mut task = status("t", 10);
        task.correlation_key = Some(correlation.into());
        assert!(filters.matches(&task));
        task.correlation_key = None;
        assert!(!filters.matches(&task));
    }
    for correlation in ["x\n".into(), "x".repeat(513), "é".repeat(257)] {
        assert!(
            TaskFilters {
                correlation_key: Some(correlation),
                ..TaskFilters::default()
            }
            .validate()
            .is_err()
        );
    }
    for queue in ["", "bad\nqueue", "\u{ffff}"] {
        assert!(
            TaskFilters {
                queue: Some(queue.into()),
                ..TaskFilters::default()
            }
            .validate()
            .is_err()
        );
    }
    let mut bad_scope = scope();
    bad_scope.tenant_id.clear();
    assert!(query.validate(&bad_scope).is_err());
}

#[test]
fn submission_range_is_inclusive_from_and_exclusive_until() {
    let filters = full_query().filters;
    assert!(!filters.matches(&status("t", 9)));
    assert!(filters.matches(&status("t", 10)));
    assert!(filters.matches(&status("t", 19)));
    assert!(!filters.matches(&status("t", 20)));
    for (from, until) in [
        (Some(10), Some(10)),
        (Some(11), Some(10)),
        (None, Some(MAX_TIMESTAMP + 1)),
        (Some(MAX_TIMESTAMP + 1), None),
    ] {
        assert!(
            TaskFilters {
                submitted_from: from,
                submitted_until: until,
                ..TaskFilters::default()
            }
            .validate()
            .is_err()
        );
    }
    TaskFilters {
        submitted_until: Some(0),
        ..TaskFilters::default()
    }
    .validate()
    .unwrap();
    TaskFilters {
        submitted_from: Some(MAX_TIMESTAMP),
        ..TaskFilters::default()
    }
    .validate()
    .unwrap();
}

#[test]
fn cursor_roundtrip_binds_scope_and_every_filter_but_allows_page_size_changes() {
    let query = full_query();
    let position = TaskPosition::from(&status("task_é", 15));
    let cursor = query.next_cursor(&scope(), &position).unwrap();
    assert!(
        cursor
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    let continued = TaskListQuery {
        cursor: Some(cursor),
        limit: 1,
        ..query.clone()
    };
    assert_eq!(continued.validate(&scope()).unwrap(), Some(position));
    for changed_scope in [
        Scope {
            tenant_id: "other".into(),
            ..scope()
        },
        Scope {
            namespace: "other".into(),
            ..scope()
        },
    ] {
        assert!(continued.validate(&changed_scope).is_err());
    }
    let mut variants = Vec::new();
    let mut changed = continued.clone();
    changed.filters.state = Some(TaskState::Failed);
    variants.push(changed);
    let mut changed = continued.clone();
    changed.filters.queue = None;
    variants.push(changed);
    let mut changed = continued.clone();
    changed.filters.correlation_key = Some(String::new());
    variants.push(changed);
    let mut changed = continued.clone();
    changed.filters.submitted_from = Some(11);
    variants.push(changed);
    let mut changed = continued.clone();
    changed.filters.submitted_until = Some(21);
    variants.push(changed);
    for changed in variants {
        assert!(changed.validate(&scope()).is_err());
    }
    assert!(
        query
            .next_cursor(&scope(), &TaskPosition::from(&status("t", 20)))
            .is_err()
    );
}

#[test]
fn cursor_rejects_bad_encoding_duplicate_keys_versions_and_malformed_positions() {
    let query = full_query();
    let cursor_value = json!({"version":1,"scope":scope(),"filters":query.filters,"position":{"submitted_at":15,"task_id":"task"}});
    let mut tokens = vec![
        String::new(),
        "0".into(),
        "gg".into(),
        "FF".into(),
        "00".into(),
        "é".into(),
        "0".repeat(TASK_CURSOR_MAX_BYTES + 2),
    ];
    for replacement in [json!(0), json!(2), json!("1"), json!(true)] {
        let mut value = cursor_value.clone();
        value["version"] = replacement;
        tokens.push(hex(&serde_json::to_vec(&value).unwrap()));
    }
    for position in [
        json!({"submitted_at":20,"task_id":"task"}),
        json!({"submitted_at":15,"task_id":""}),
        json!({"submitted_at":-1,"task_id":"task"}),
        json!({"submitted_at":15,"task_id":"task","extra":true}),
    ] {
        let mut value = cursor_value.clone();
        value["position"] = position;
        tokens.push(hex(&serde_json::to_vec(&value).unwrap()));
    }
    let original = serde_json::to_string(&cursor_value).unwrap();
    tokens.push(hex(original.replacen("{", "{\"version\":1,", 1).as_bytes()));
    tokens.push(hex(original
        .replace(
            "\"task_id\":\"task\"",
            "\"task_id\":\"task\",\"task_id\":\"task\"",
        )
        .as_bytes()));
    for token in tokens {
        let changed = TaskListQuery {
            cursor: Some(token),
            ..query.clone()
        };
        assert!(matches!(
            changed.validate(&scope()),
            Err(ContractError::InvalidInput(_))
        ));
    }
}

#[test]
fn pages_follow_descending_tuple_order_and_cursor_boundaries() {
    let query = full_query();
    let items = vec![status("z", 15), status("a", 15)];
    let cursor = query
        .next_cursor(&scope(), &TaskPosition::from(&items[1]))
        .unwrap();
    let page = TaskPage {
        items: items.clone(),
        next_cursor: Some(cursor.clone()),
    };
    page.validate(&scope(), &query).unwrap();
    TaskPage {
        items: vec![status("ä", 15), status("z", 15)],
        next_cursor: None,
    }
    .validate(&scope(), &query)
    .unwrap();
    let continued = TaskListQuery {
        cursor: Some(cursor),
        ..query.clone()
    };
    TaskPage {
        items: vec![status("z", 14)],
        next_cursor: None,
    }
    .validate(&scope(), &continued)
    .unwrap();
    assert!(
        TaskPage {
            items,
            next_cursor: None
        }
        .validate(&scope(), &continued)
        .is_err()
    );
    TaskPage {
        items: vec![],
        next_cursor: None,
    }
    .validate(&scope(), &continued)
    .unwrap();
    for items in [
        vec![status("a", 15), status("z", 15)],
        vec![status("z", 15), status("z", 15)],
        vec![status("z", 15), status("z", 14)],
        vec![status("z", 15), status("b", 14), status("a", 13)],
    ] {
        assert!(matches!(
            TaskPage {
                items,
                next_cursor: None
            }
            .validate(&scope(), &query),
            Err(ContractError::Unavailable(_))
        ));
    }
}

#[test]
fn pages_reject_wrong_scope_filters_status_and_continuation() {
    let query = full_query();
    for field in ["scope", "queue", "correlation", "state", "time", "status"] {
        let mut task = status("t", 15);
        match field {
            "scope" => task.scope.namespace = "other".into(),
            "queue" => task.queue = "other".into(),
            "correlation" => task.correlation_key = None,
            "state" => {
                task.state = TaskState::Cancelled;
                task.terminal_at = Some(16);
                task.cancel_requested_at = Some(16);
            }
            "time" => task.submitted_at = 20,
            "status" => task.current_attempt_id = Some("incorrect".into()),
            _ => unreachable!(),
        }
        assert!(
            TaskPage {
                items: vec![task],
                next_cursor: None
            }
            .validate(&scope(), &query)
            .is_err(),
            "{field}"
        );
    }
    let correct = query
        .next_cursor(&scope(), &TaskPosition::from(&status("a", 15)))
        .unwrap();
    let wrong = query
        .next_cursor(&scope(), &TaskPosition::from(&status("a", 14)))
        .unwrap();
    for page in [
        TaskPage {
            items: vec![],
            next_cursor: Some(correct.clone()),
        },
        TaskPage {
            items: vec![status("a", 15)],
            next_cursor: Some(correct),
        },
        TaskPage {
            items: vec![status("z", 15), status("a", 15)],
            next_cursor: Some(wrong),
        },
        TaskPage {
            items: vec![status("z", 15), status("a", 15)],
            next_cursor: Some("00".into()),
        },
    ] {
        assert!(matches!(
            page.validate(&scope(), &query),
            Err(ContractError::Unavailable(_))
        ));
    }
}

#[test]
fn page_wire_requires_nullable_cursor_and_rejects_unknown_fields() {
    let empty = TaskPage {
        items: vec![],
        next_cursor: None,
    };
    assert_eq!(
        serde_json::to_value(&empty).unwrap(),
        json!({"items":[],"next_cursor":null})
    );
    for value in [
        json!({"items":[]}),
        json!({"items":[],"next_cursor":null,"extra":true}),
        json!({"items":[],"next_cursor":3}),
    ] {
        assert!(serde_json::from_value::<TaskPage>(value).is_err());
    }
    for value in [
        json!({"limit":true}),
        json!({"limit":1.5}),
        json!({"filters":{"unknown":true}}),
        json!({"filters":{"state":"lost"}}),
    ] {
        assert!(serde_json::from_value::<TaskListQuery>(value).is_err());
    }
}
