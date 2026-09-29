use ledgence_orchestration_api::{console::*, *};
use serde_json::{Value, json};

fn scope() -> Scope {
    Scope {
        tenant_id: "acme".into(),
        namespace: "billing".into(),
    }
}
fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/execution-discovery-v1.json")).unwrap()
}

#[test]
fn execution_discovery_shared_fixtures_roundtrip_and_validate() {
    let value = fixture();
    let executions: ConsolePage<ConsoleExecutionSummary> =
        serde_json::from_value(value["executions"].clone()).unwrap();
    let query = ConsoleQuery::Executions {
        filters: ConsoleExecutionFilters {
            include_children: true,
            ..Default::default()
        },
        page: Default::default(),
    };
    ConsoleQueryReply::Executions(executions.clone())
        .validate(&scope(), &query)
        .unwrap();
    assert_eq!(
        serde_json::to_value(executions).unwrap(),
        value["executions"]
    );
    let catalog: ConsolePage<ConsoleProgramCatalogEntry> =
        serde_json::from_value(value["catalog"].clone()).unwrap();
    let query = ProgramCatalogQuery::Catalog {
        kind: Some(ConsoleProgramKind::Workflow),
        page: Default::default(),
    };
    ProgramCatalogReply::Catalog(catalog.clone())
        .validate(&scope(), &query)
        .unwrap();
    assert_eq!(serde_json::to_value(catalog).unwrap(), value["catalog"]);
}
#[test]
fn execution_filters_expand_children_only_for_explicit_history_or_identity_and_bind_cursor() {
    let mut filters = ConsoleExecutionFilters::default();
    assert!(!filters.includes_children());
    filters.program_id = Some("program".into());
    assert!(filters.includes_children());
    filters.validate().unwrap();
    filters.program_id = None;
    filters.version = Some("1".into());
    assert!(filters.validate().is_err());
    filters.version = None;
    filters.execution_id = Some("child".into());
    assert!(filters.includes_children());
    filters.state = Some(ConsoleExecutionState::Failing);
    filters.kind = Some(ConsoleExecutionKind::Task);
    assert!(filters.validate().is_err());
    filters.kind = Some(ConsoleExecutionKind::Workflow);
    filters.validate().unwrap();
    let query = ConsoleQuery::Executions {
        filters: filters.clone(),
        page: Default::default(),
    };
    let binding = query.binding(&scope()).unwrap();
    let page = ConsolePagination {
        limit: 2,
        cursor: Some(
            ConsolePagination::default()
                .next_cursor(
                    &binding,
                    &vec![
                        ConsoleKey::Number(ConsoleU64(10)),
                        ConsoleKey::Text("workflow".into()),
                        ConsoleKey::Text("child".into()),
                    ],
                )
                .unwrap(),
        ),
    };
    assert!(page.validate(&binding).is_ok());
    filters.include_children = true;
    assert!(
        ConsoleQuery::Executions { filters, page }
            .validate(&scope())
            .is_err()
    );
}
#[test]
fn execution_observations_reject_wrong_states_lineage_order_and_filter_results() {
    let value = fixture();
    let query = ConsoleQuery::Executions {
        filters: ConsoleExecutionFilters {
            include_children: true,
            ..Default::default()
        },
        page: Default::default(),
    };
    for field in [
        json!({"state":"active"}),
        json!({"terminal_at":100}),
        json!({"root_workflow_id":"orphan"}),
        json!({"parent_workflow_id":"shared-id","root_workflow_id":"shared-id"}),
        json!({"state":"succeeded","terminal_at":99}),
    ] {
        let mut row = value["executions"]["items"][0].clone();
        row.as_object_mut()
            .unwrap()
            .extend(field.as_object().unwrap().clone());
        let row: ConsoleExecutionSummary = serde_json::from_value(row).unwrap();
        assert!(row.validate().is_err());
    }
    let mut page: ConsolePage<ConsoleExecutionSummary> =
        serde_json::from_value(value["executions"].clone()).unwrap();
    page.items.reverse();
    assert!(
        ConsoleQueryReply::Executions(page)
            .validate(&scope(), &query)
            .is_err()
    );
    let page = serde_json::from_value(value["executions"].clone()).unwrap();
    assert!(
        ConsoleQueryReply::Executions(page)
            .validate(
                &scope(),
                &ConsoleQuery::Executions {
                    filters: Default::default(),
                    page: Default::default()
                }
            )
            .is_err()
    );
}

#[test]
fn equal_raw_ids_in_different_execution_kinds_do_not_imply_self_ancestry() {
    let mut row = fixture()["executions"]["items"][1].clone();
    row["parent_workflow_id"] = json!("shared-id");
    row["root_workflow_id"] = json!("shared-id");
    serde_json::from_value::<ConsoleExecutionSummary>(row)
        .unwrap()
        .validate()
        .unwrap();
}
