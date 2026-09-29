//! Canonical C4 observations. All API objects are serialized from Rust DTOs.
use ledgence_orchestration_api::{console::*, *};
use ledgence_worker_api::{Digest, ProgramRef};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;

const AT: u64 = 1_790_409_600_000;
const OBSERVED: u64 = AT + 20_000;
const ROOT: &str = "wf_release_fork4";

pub fn scope() -> Scope {
    Scope {
        tenant_id: "acme".into(),
        namespace: "billing".into(),
    }
}
fn program(workflow: bool) -> ProgramRef {
    ProgramRef {
        id: if workflow {
            "explorer-release-workflow"
        } else {
            "explorer-release-task"
        }
        .into(),
        version: "1.0.0".into(),
    }
}
fn descriptor(workflow: bool) -> ConsoleProgramDescriptor {
    ConsoleProgramDescriptor {
        program: program(workflow),
        digest: Digest(format!(
            "sha256:{}",
            if workflow { "b" } else { "c" }.repeat(64)
        )),
        size: ConsoleU64(4096),
    }
}
fn activation(workflow: &str, revision: u64) -> String {
    format!("act_{workflow}_{revision}")
}
fn node(
    workflow: &str,
    revision: u64,
    entrypoint: &str,
    data: ConsoleExplorerData,
) -> ConsoleExplorerNode {
    let activation_id = activation(workflow, revision);
    ConsoleExplorerNode {
        id: serde_json::to_string(&(data.kind(), workflow, &activation_id, data.key())).unwrap(),
        activation_id,
        revision: ConsoleU64(revision),
        entrypoint: entrypoint.into(),
        relations: Vec::new(),
        data,
    }
}
fn entrypoint(
    workflow: &str,
    revision: u64,
    handler: &str,
    decision: ConsoleDecisionKind,
    resume: Option<u64>,
) -> ConsoleExplorerNode {
    node(
        workflow,
        revision,
        handler,
        ConsoleExplorerData::Entrypoint {
            state: Some(TaskState::Succeeded),
            availability: ConsoleEvidenceAvailability::Available,
            submitted_at: AT + revision * 1000,
            terminal_at: Some(AT + revision * 1000 + 80),
            applied_at: Some(if decision == ConsoleDecisionKind::Continue {
                AT + resume.unwrap_or(revision) * 1000 + u64::from(resume.is_none()) * 100
            } else {
                AT + revision * 1000 + 100
            }),
            decision_kind: Some(decision),
            error: None,
            resumed_activation_id: resume.map(|next| activation(workflow, next)),
        },
    )
}
fn child(
    workflow: &str,
    revision: u64,
    entrypoint: &str,
    key: &str,
    id: &str,
    kind: ConsoleExecutionKind,
    fork: Option<&str>,
) -> ConsoleExplorerNode {
    node(
        workflow,
        revision,
        entrypoint,
        ConsoleExplorerData::Child {
            key: key.into(),
            execution: ConsoleExecutionIdentity {
                kind,
                id: id.into(),
            },
            program: program(kind == ConsoleExecutionKind::Workflow),
            fork_key: fork.map(str::to_owned),
            availability: ConsoleEvidenceAvailability::Available,
            state: Some(ConsoleExecutionState::Succeeded),
            submitted_at: AT + revision * 1000 + 100,
            terminal_at: Some(AT + revision * 1000 + 800),
        },
    )
}
fn join(
    workflow: &str,
    revision: u64,
    entrypoint: &str,
    members: &[&str],
    resume: &str,
    next: Option<u64>,
) -> ConsoleExplorerNode {
    node(
        workflow,
        revision,
        entrypoint,
        ConsoleExplorerData::ChildWait {
            member_keys: members.iter().map(|key| (*key).into()).collect(),
            resume: resume.into(),
            applied_at: AT + revision * 1000 + 100,
            resumed_activation_id: next.map(|revision| activation(workflow, revision)),
        },
    )
}
fn explorer(
    workflow: &str,
    state: WorkflowState,
    continuation: &str,
    mut items: Vec<ConsoleExplorerNode>,
) -> ConsoleWorkflowExplorer {
    for node in &mut items {
        node.relations = node.derive_relations(workflow).unwrap();
    }
    items.sort_by_key(ConsoleRecord::position);
    let revision = items
        .iter()
        .map(|node| node.revision)
        .max()
        .unwrap_or(ConsoleU64(0));
    let reply = ConsoleWorkflowExplorer {
        workflow: ConsoleWorkflowDetail {
            summary: ConsoleWorkflowSummary {
                workflow: ConsoleWorkflowStatus {
                    workflow_id: workflow.into(),
                    state,
                    revision,
                    activation_id: (state == WorkflowState::Running)
                        .then(|| activation(workflow, revision.0)),
                    submitted_at: AT,
                    terminal_at: state.is_terminal().then_some(AT + revision.0 * 1000 + 100),
                    correlation_key: Some("explorer-native-fork4".into()),
                    parent_workflow_id: None,
                    root_workflow_id: None,
                },
                controller: descriptor(true),
                queue: "release".into(),
            },
            continuation: continuation.into(),
            child_wait: None,
            external_wait_key: None,
            observed_at: OBSERVED,
        },
        page: ConsolePage {
            items,
            next_cursor: None,
            observed_at: OBSERVED,
        },
        evidence: "retained_records_only".into(),
    };
    let query = ConsoleQuery::Explorer {
        workflow_id: workflow.into(),
        page: ConsolePagination {
            limit: 100,
            cursor: None,
        },
    };
    ConsoleQueryReply::Explorer(reply.clone())
        .validate(&scope(), &query)
        .unwrap();
    reply
}

fn release() -> ConsoleWorkflowExplorer {
    let handlers = [
        "start",
        "after_prepare",
        "publish_draft",
        "review",
        "publish_report",
        "finish",
    ];
    let mut nodes = handlers
        .iter()
        .enumerate()
        .map(|(revision, name)| {
            entrypoint(
                ROOT,
                revision as u64,
                name,
                if revision == 5 {
                    ConsoleDecisionKind::Complete
                } else {
                    ConsoleDecisionKind::Suspend
                },
                (revision < 5).then_some(revision as u64 + 1),
            )
        })
        .collect::<Vec<_>>();
    let branches = ["security:0", "tests:0", "dependencies:0", "docs:0"];
    nodes.push(node(
        ROOT,
        0,
        "start",
        ConsoleExplorerData::Fork {
            key: "release-checks:0".into(),
            branch_keys: branches.iter().map(|key| (*key).into()).collect(),
            accepted_at: AT + 30,
            accepting_attempt_id: "att_release_start_1".into(),
        },
    ));
    for (index, key) in branches.iter().enumerate() {
        let mut record = child(
            ROOT,
            0,
            "start",
            key,
            &format!("wf_release_branch_{index}"),
            ConsoleExecutionKind::Workflow,
            Some("release-checks:0"),
        );
        let ConsoleExplorerData::Child {
            submitted_at,
            terminal_at,
            ..
        } = &mut record.data
        else {
            unreachable!()
        };
        *submitted_at = AT + 30;
        *terminal_at = Some(AT + 900 + index as u64 * 200);
        nodes.push(record);
    }
    for (revision, key, id, resume) in [
        (0, "prepare:0", "task_prepare", "after_prepare"),
        (2, "publish-draft:0", "task_publish_draft", "review"),
        (3, "review-draft:0", "task_review_draft", "publish_report"),
        (4, "publish-final:0", "task_publish_final", "finish"),
    ] {
        nodes.push(child(
            ROOT,
            revision,
            handlers[revision as usize],
            key,
            id,
            ConsoleExecutionKind::Task,
            None,
        ));
        nodes.push(join(
            ROOT,
            revision,
            handlers[revision as usize],
            &[key],
            resume,
            Some(revision + 1),
        ));
    }
    nodes.push(join(
        ROOT,
        1,
        "after_prepare",
        &branches,
        "publish_draft",
        Some(2),
    ));
    explorer(ROOT, WorkflowState::Succeeded, "finish", nodes)
}

fn repeated_entrypoint() -> ConsoleWorkflowExplorer {
    let id = "wf_repeated_review";
    explorer(
        id,
        WorkflowState::Succeeded,
        "review",
        vec![
            entrypoint(id, 0, "start", ConsoleDecisionKind::Continue, Some(1)),
            entrypoint(id, 1, "review", ConsoleDecisionKind::Continue, Some(2)),
            entrypoint(id, 2, "review", ConsoleDecisionKind::Complete, None),
        ],
    )
}

fn retry() -> ConsoleWorkflowExplorer {
    let id = "wf_retry";
    explorer(
        id,
        WorkflowState::Succeeded,
        "start",
        vec![
            entrypoint(id, 0, "start", ConsoleDecisionKind::Complete, None),
            node(
                id,
                0,
                "start",
                ConsoleExplorerData::Local {
                    key: "calculate:0".into(),
                    callable: "release:calculate".into(),
                    accepted_at: Some(AT + 20),
                    accepting_attempt_id: Some("att_retry_1".into()),
                    observation: Some(ConsoleLocalObservation {
                        attempt_id: "att_retry_1".into(),
                        started_at: AT + 10,
                        elapsed_us: ConsoleU64(7500),
                        state: "returned".into(),
                    }),
                },
            ),
        ],
    )
}

fn mixed_local() -> ConsoleWorkflowExplorer {
    let id = "wf_mixed_local";
    explorer(
        id,
        WorkflowState::Succeeded,
        "collect",
        vec![
            entrypoint(id, 0, "start", ConsoleDecisionKind::Suspend, Some(1)),
            node(
                id,
                0,
                "start",
                ConsoleExplorerData::Fork {
                    key: "calculations:0".into(),
                    branch_keys: vec!["double:0".into(), "triple:0".into()],
                    accepted_at: AT + 10,
                    accepting_attempt_id: "att_mixed_0".into(),
                },
            ),
            child(
                id,
                0,
                "start",
                "double:0",
                "wf_double",
                ConsoleExecutionKind::Workflow,
                Some("calculations:0"),
            ),
            child(
                id,
                0,
                "start",
                "triple:0",
                "wf_triple",
                ConsoleExecutionKind::Workflow,
                Some("calculations:0"),
            ),
            node(
                id,
                0,
                "start",
                ConsoleExplorerData::Local {
                    key: "summary".into(),
                    callable: "program:summarize".into(),
                    accepted_at: Some(AT + 50),
                    accepting_attempt_id: Some("att_mixed_0".into()),
                    observation: Some(ConsoleLocalObservation {
                        attempt_id: "att_mixed_0".into(),
                        started_at: AT + 20,
                        elapsed_us: ConsoleU64(20_000),
                        state: "returned".into(),
                    }),
                },
            ),
            join(
                id,
                0,
                "start",
                &["double:0", "triple:0"],
                "collect",
                Some(1),
            ),
            entrypoint(id, 1, "collect", ConsoleDecisionKind::Complete, None),
        ],
    )
}

fn observed_local_failure() -> ConsoleWorkflowExplorer {
    let id = "wf_observed_local_failure";
    explorer(
        id,
        WorkflowState::Failed,
        "start",
        vec![
            node(
                id,
                0,
                "start",
                ConsoleExplorerData::Entrypoint {
                    state: Some(TaskState::Failed),
                    availability: ConsoleEvidenceAvailability::Available,
                    submitted_at: AT,
                    terminal_at: Some(AT + 100),
                    applied_at: None,
                    decision_kind: None,
                    error: None,
                    resumed_activation_id: None,
                },
            ),
            node(
                id,
                0,
                "start",
                ConsoleExplorerData::Local {
                    key: "summary".into(),
                    callable: "program:summarize".into(),
                    accepted_at: None,
                    accepting_attempt_id: None,
                    observation: Some(ConsoleLocalObservation {
                        attempt_id: "att_observed_failure".into(),
                        started_at: AT + 20,
                        elapsed_us: ConsoleU64(20_000),
                        state: "failed".into(),
                    }),
                },
            ),
        ],
    )
}

fn rejected() -> ConsoleWorkflowExplorer {
    let id = "wf_rejected";
    let mut invocation = entrypoint(id, 0, "start", ConsoleDecisionKind::Continue, None);
    let ConsoleExplorerData::Entrypoint {
        error,
        decision_kind,
        ..
    } = &mut invocation.data
    else {
        unreachable!()
    };
    *error = Some(ApplicationError {
        kind: "decision_rejected".into(),
        message: "Conflicting binding\nUnicode: 東京; NUL: \0; literal: \\u0000".into(),
    });
    *decision_kind = None;
    explorer(id, WorkflowState::Failed, "start", vec![invocation])
}

fn unavailable() -> ConsoleWorkflowExplorer {
    let id = "wf_unavailable_child";
    let mut target = child(
        id,
        0,
        "start",
        "prepare:0",
        "task_collected",
        ConsoleExecutionKind::Task,
        None,
    );
    let ConsoleExplorerData::Child {
        availability,
        state,
        ..
    } = &mut target.data
    else {
        unreachable!()
    };
    *availability = ConsoleEvidenceAvailability::Unavailable;
    *state = None;
    explorer(
        id,
        WorkflowState::Succeeded,
        "finish",
        vec![
            entrypoint(id, 0, "start", ConsoleDecisionKind::Suspend, Some(1)),
            target,
            join(id, 0, "start", &["prepare:0"], "finish", Some(1)),
            entrypoint(id, 1, "finish", ConsoleDecisionKind::Complete, None),
        ],
    )
}

fn external_waits() -> ConsoleWorkflowExplorer {
    let id = "wf_external_waits";
    let handlers = [
        "start",
        "wait_event",
        "after_event",
        "after_timer",
        "after_timeout",
    ];
    let mut nodes = handlers
        .iter()
        .enumerate()
        .map(|(index, name)| {
            entrypoint(
                id,
                index as u64,
                name,
                if index == 0 {
                    ConsoleDecisionKind::Continue
                } else if index == 4 {
                    ConsoleDecisionKind::Complete
                } else {
                    ConsoleDecisionKind::Wait
                },
                (index < 4).then_some(index as u64 + 1),
            )
        })
        .collect::<Vec<_>>();
    for (revision, key, kind, wake) in [
        (
            1,
            "approval:0",
            ConsoleWaitKind::Event,
            ConsoleWakeReason::Event,
        ),
        (
            2,
            "pause:0",
            ConsoleWaitKind::Timer,
            ConsoleWakeReason::Timer,
        ),
        (
            3,
            "approval:1",
            ConsoleWaitKind::Event,
            ConsoleWakeReason::Timeout,
        ),
    ] {
        nodes.push(node(
            id,
            revision,
            handlers[revision as usize],
            ConsoleExplorerData::ExternalWait {
                key: key.into(),
                wait_kind: kind,
                deadline: Some(AT + (revision + 1) * 1000),
                registered_at: AT + revision * 1000 + 100,
                closed_at: Some(AT + (revision + 1) * 1000),
                wake_reason: Some(wake),
                resumed_activation_id: Some(activation(id, revision + 1)),
            },
        ));
    }
    explorer(id, WorkflowState::Succeeded, "after_timeout", nodes)
}

fn closed_without_wake() -> ConsoleWorkflowExplorer {
    let id = "wf_cancelled_wait";
    let mut result = explorer(
        id,
        WorkflowState::Cancelled,
        "after_approval",
        vec![
            entrypoint(id, 0, "start", ConsoleDecisionKind::Wait, None),
            node(
                id,
                0,
                "start",
                ConsoleExplorerData::ExternalWait {
                    key: "approval:0".into(),
                    wait_kind: ConsoleWaitKind::Event,
                    deadline: None,
                    registered_at: AT + 100,
                    closed_at: Some(AT + 500),
                    wake_reason: None,
                    resumed_activation_id: None,
                },
            ),
        ],
    );
    result.workflow.summary.workflow.terminal_at = Some(AT + 500);
    result
}

fn branch_security() -> ConsoleWorkflowExplorer {
    let id = "wf_release_branch_0";
    let mut result = explorer(
        id,
        WorkflowState::Succeeded,
        "security",
        vec![
            entrypoint(id, 0, "security", ConsoleDecisionKind::Complete, None),
            node(
                id,
                0,
                "security",
                ConsoleExplorerData::Local {
                    key: "check:0".into(),
                    callable: "release:security".into(),
                    accepted_at: Some(AT + 50),
                    accepting_attempt_id: Some("att_security_1".into()),
                    observation: Some(ConsoleLocalObservation {
                        attempt_id: "att_security_1".into(),
                        started_at: AT + 40,
                        elapsed_us: ConsoleU64(9000),
                        state: "returned".into(),
                    }),
                },
            ),
        ],
    );
    result.workflow.summary.workflow.parent_workflow_id = Some(ROOT.into());
    result.workflow.summary.workflow.root_workflow_id = Some(ROOT.into());
    result.workflow.summary.workflow.submitted_at = AT + 30;
    result.workflow.summary.workflow.terminal_at = Some(AT + 900);
    for node in &mut result.page.items {
        if let ConsoleExplorerData::Entrypoint {
            submitted_at,
            terminal_at,
            applied_at,
            ..
        } = &mut node.data
        {
            *submitted_at = AT + 30;
            *terminal_at = Some(AT + 880);
            *applied_at = Some(AT + 900);
        }
    }
    result
}

fn terminal_branch(state: ConsoleExecutionState) -> ConsoleWorkflowExplorer {
    let mut result = release();
    result.page.items.retain(|node| {
        node.revision.0 < 2
            || matches!(&node.data, ConsoleExplorerData::Entrypoint { .. }) && node.revision.0 == 2
    });
    for node in &mut result.page.items {
        if let ConsoleExplorerData::Child {
            key,
            state: outcome,
            ..
        } = &mut node.data
            && key == "security:0"
        {
            *outcome = Some(state);
        }
        if let ConsoleExplorerData::Entrypoint {
            decision_kind,
            resumed_activation_id,
            ..
        } = &mut node.data
            && node.revision.0 == 2
        {
            *decision_kind = Some(ConsoleDecisionKind::Fail);
            *resumed_activation_id = None;
        }
    }
    result.workflow.summary.workflow.state = WorkflowState::Failed;
    result.workflow.summary.workflow.revision = ConsoleU64(2);
    result.workflow.summary.workflow.terminal_at = Some(AT + 2100);
    result.workflow.continuation = "publish_draft".into();
    result
}

fn review_declined() -> ConsoleWorkflowExplorer {
    let mut result = release();
    result.page.items.retain(|node| {
        node.revision.0 < 4
            || matches!(&node.data, ConsoleExplorerData::Entrypoint { .. }) && node.revision.0 == 4
    });
    for node in &mut result.page.items {
        if let ConsoleExplorerData::Entrypoint {
            decision_kind,
            resumed_activation_id,
            ..
        } = &mut node.data
            && node.revision.0 == 4
        {
            *decision_kind = Some(ConsoleDecisionKind::Complete);
            *resumed_activation_id = None;
        }
    }
    // A business review can decline publication while orchestration succeeds.
    // The business decision lives in the result endpoint, never node metadata.
    result.workflow.summary.workflow.revision = ConsoleU64(4);
    result.workflow.summary.workflow.terminal_at = Some(AT + 4100);
    result.workflow.continuation = "publish_report".into();
    result
}

#[derive(Serialize)]
struct ExplorerFixturePage {
    request: ConsolePagination,
    response: ConsoleWorkflowExplorer,
}
fn partial_pages(full: &ConsoleWorkflowExplorer) -> Vec<ExplorerFixturePage> {
    let mut request = ConsolePagination {
        limit: 2,
        cursor: None,
    };
    let mut pages = Vec::new();
    for (index, chunk) in full.page.items.chunks(2).enumerate() {
        let query = ConsoleQuery::Explorer {
            workflow_id: ROOT.into(),
            page: request.clone(),
        };
        let next_cursor = ((index + 1) * 2 < full.page.items.len()).then(|| {
            request
                .next_cursor(
                    &query.binding(&scope()).unwrap(),
                    &chunk.last().unwrap().position(),
                )
                .unwrap()
        });
        let mut response = full.clone();
        response.page.items = chunk.to_vec();
        response.page.next_cursor = next_cursor.clone();
        ConsoleQueryReply::Explorer(response.clone())
            .validate(&scope(), &query)
            .unwrap();
        pages.push(ExplorerFixturePage {
            request: request.clone(),
            response,
        });
        request.cursor = next_cursor;
    }
    pages
}

pub fn fixtures() -> Value {
    let main = release();
    let cases = BTreeMap::from([
        ("repeated_entrypoint", repeated_entrypoint()),
        ("retry", retry()),
        ("mixed_local", mixed_local()),
        ("observed_local_failure", observed_local_failure()),
        ("rejected", rejected()),
        ("unavailable_child", unavailable()),
        ("external_waits", external_waits()),
        ("closed_without_wake", closed_without_wake()),
        ("branch_security", branch_security()),
        (
            "branch_failed",
            terminal_branch(ConsoleExecutionState::Failed),
        ),
        (
            "branch_cancelled",
            terminal_branch(ConsoleExecutionState::Cancelled),
        ),
        ("review_declined", review_declined()),
    ]);
    let mut execution_rows = vec![ConsoleExecutionSummary {
        kind: ConsoleExecutionKind::Workflow,
        id: ROOT.into(),
        descriptor: descriptor(true),
        queue: "release".into(),
        state: ConsoleExecutionState::Succeeded,
        submitted_at: AT,
        terminal_at: Some(AT + 5100),
        correlation_key: Some("explorer-native-fork4".into()),
        parent_workflow_id: None,
        root_workflow_id: None,
    }];
    for node in &main.page.items {
        if let ConsoleExplorerData::Child {
            execution,
            submitted_at,
            terminal_at,
            ..
        } = &node.data
        {
            execution_rows.push(ConsoleExecutionSummary {
                kind: execution.kind,
                id: execution.id.clone(),
                descriptor: descriptor(execution.kind == ConsoleExecutionKind::Workflow),
                queue: "release".into(),
                state: ConsoleExecutionState::Succeeded,
                submitted_at: *submitted_at,
                terminal_at: *terminal_at,
                correlation_key: Some("explorer-native-fork4".into()),
                parent_workflow_id: Some(ROOT.into()),
                root_workflow_id: Some(ROOT.into()),
            });
        }
    }
    execution_rows.sort_by_key(|value| std::cmp::Reverse(value.position()));
    let executions = ConsolePage {
        items: execution_rows,
        next_cursor: None,
        observed_at: OBSERVED,
    };
    ConsoleQueryReply::Executions(executions.clone())
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
    let child_identity = ConsoleExecutionIdentity {
        kind: ConsoleExecutionKind::Workflow,
        id: "wf_release_branch_0".into(),
    };
    let ancestry = ConsoleAncestry {
        execution: child_identity.clone(),
        path: vec![
            ConsoleAncestor {
                execution: ConsoleExecutionIdentity {
                    kind: ConsoleExecutionKind::Workflow,
                    id: ROOT.into(),
                },
                program: Some(program(true)),
                availability: ConsoleEvidenceAvailability::Available,
            },
            ConsoleAncestor {
                execution: child_identity.clone(),
                program: Some(program(true)),
                availability: ConsoleEvidenceAvailability::Available,
            },
        ],
        observed_at: OBSERVED,
    };
    ancestry.validate(&child_identity).unwrap();
    let attempts = ConsolePage {
        items: [2, 1]
            .into_iter()
            .map(|generation| ConsoleAttemptSummary {
                task_id: activation("wf_retry", 0),
                attempt_id: format!("att_retry_{generation}"),
                generation,
                worker_session_id: "worker_release".into(),
                consumer_id: 0,
                state: if generation == 1 {
                    AttemptState::Failed
                } else {
                    AttemptState::Succeeded
                },
                execution_may_have_started: true,
                quiescence: Quiescence::Confirmed,
                claimed_at: Some(AT + u64::from(generation - 1) * 30),
                dispatch_authorized_at: Some(AT + u64::from(generation - 1) * 30 + 1),
                finished_at: Some(AT + if generation == 1 { 29 } else { 80 }),
            })
            .collect(),
        next_cursor: None,
        observed_at: OBSERVED,
    };
    ConsoleQueryReply::Attempts(attempts.clone())
        .validate(
            &scope(),
            &ConsoleQuery::Attempts {
                task_id: activation("wf_retry", 0),
                page: Default::default(),
            },
        )
        .unwrap();
    json!({"explorer": main, "explorer_cases": cases, "explorer_pages": partial_pages(&main),
        "execution_history": executions, "ancestry": ancestry, "entrypoint_attempts": attempts})
}
