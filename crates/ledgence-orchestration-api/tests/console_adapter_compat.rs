//! Compile this as a downstream adapter using the published 0.6.1 query surface.
use ledgence_orchestration_api::{ContractError, ContractFuture, Scope, console::*};
use std::task::{Context, Poll, Waker};

struct ExistingAdapter;

fn route(query: &ConsoleQuery) -> &'static str {
    match query {
        ConsoleQuery::Explorer {
            workflow_id: _,
            page: _,
        } => "explorer",
        ConsoleQuery::WorkflowInput { .. } => "input",
        ConsoleQuery::Ancestry { .. } => "ancestry",
        ConsoleQuery::AttemptObservations { .. } => "observations",
        ConsoleQuery::Executions { .. } => "executions",
        ConsoleQuery::Tasks { .. } => "tasks",
        ConsoleQuery::Attempts { .. } => "attempts",
        ConsoleQuery::Attempt { .. } => "attempt",
        ConsoleQuery::Workflows { .. } => "workflows",
        ConsoleQuery::Workflow { .. } => "workflow",
        ConsoleQuery::Activations { .. } => "activations",
        ConsoleQuery::Children { .. } => "children",
        ConsoleQuery::Waits { .. } => "waits",
        ConsoleQuery::History { .. } => "history",
        ConsoleQuery::LocalSteps { .. } => "local steps",
    }
}

impl ConsoleQueryStore for ExistingAdapter {
    fn query_console<'a>(
        &'a self,
        _: &'a Scope,
        query: &'a ConsoleQuery,
    ) -> ContractFuture<'a, ConsoleQueryReply> {
        Box::pin(async move { Err(ContractError::Unavailable(route(query).into())) })
    }
}

impl ConsoleQueryService for ExistingAdapter {
    fn query_console<'a>(
        &'a self,
        query: &'a ConsoleQuery,
    ) -> ContractFuture<'a, ConsoleQueryReply> {
        Box::pin(async move { Err(ContractError::Unavailable(route(query).into())) })
    }
}

#[test]
fn existing_ports_report_missing_focused_explorer_without_dispatching_an_old_query() {
    let scope = Scope {
        tenant_id: "acme".into(),
        namespace: "test".into(),
    };
    let focus = ConsoleApprovalFocus {
        workflow_id: "wf_01".into(),
        approval_key: "review".into(),
        page: ConsolePagination::default(),
    };
    let store: &dyn ConsoleQueryStore = &ExistingAdapter;
    let service: &dyn ConsoleQueryService = &ExistingAdapter;
    let mut cx = Context::from_waker(Waker::noop());
    for mut response in [
        store.explore_approval(&scope, &focus),
        service.explore_approval(&focus),
    ] {
        assert!(
            matches!(response.as_mut().poll(&mut cx),Poll::Ready(Err(ContractError::Unavailable(reason))) if reason.contains("focused approval"))
        );
    }
}

#[test]
fn old_adapters_can_decline_local_result_inspection() {
    let scope = Scope {
        tenant_id: "acme".into(),
        namespace: "test".into(),
    };
    let lookup = ConsoleLocalStepLookup {
        workflow_id: "workflow".into(),
        activation_id: "act".into(),
        step_key: "key".into(),
    };
    let store: &dyn ConsoleQueryStore = &ExistingAdapter;
    let service: &dyn ConsoleQueryService = &ExistingAdapter;
    let mut cx = Context::from_waker(Waker::noop());
    for mut response in [
        store.inspect_local_step(&scope, &lookup),
        service.inspect_local_step(&lookup),
    ] {
        assert!(matches!(
            response.as_mut().poll(&mut cx),
            Poll::Ready(Err(ContractError::Unavailable(_)))
        ));
    }
}
