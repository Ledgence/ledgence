//! Generate the versioned examples consumed by both Rust and the Console client.
use ledgence_orchestration_api::{console::*, *};
use ledgence_worker_api::{
    Digest, Platform, ProcessSlotState, ProgramManifest, ProgramRef, PythonRuntime,
    WorkerObservationDetailState,
};
use serde_json::{Value, json};
#[path = "console_explorer_fixtures.rs"]
mod explorer;

pub fn fixtures() -> Value {
    let at = 1_790_409_600_000;
    let descriptor = ConsoleProgramDescriptor {
        program: ProgramRef {
            id: "invoice-issuer".into(),
            version: "release-a".into(),
        },
        digest: Digest(format!("sha256:{}", "a".repeat(64))),
        size: ConsoleU64(4096),
    };
    let status = ConsoleTaskStatus {
        admission: None,
        admission_deferrals: 0,
        task_id: "task_invoice_1042".into(),
        run_id: "run_invoice_1042".into(),
        workflow_id: None,
        workflow_activation_id: None,
        provider_probe: None,
        queue: "billing".into(),
        correlation_key: Some("INV-1042".into()),
        state: TaskState::Queued,
        attempt_count: 0,
        current_attempt_id: None,
        latest_attempt_id: None,
        submitted_at: at,
        available_at: at,
        terminal_at: None,
        cancel_requested_at: None,
    };
    let tasks = ConsolePage {
        items: vec![ConsoleTaskSummary {
            task: status.clone(),
            descriptor: descriptor.clone(),
        }],
        next_cursor: None,
        observed_at: at,
    };
    let workflow = ConsoleWorkflowStatus {
        workflow_id: "wf_invoice_1042".into(),
        state: WorkflowState::Waiting,
        revision: ConsoleU64(9_007_199_254_740_993),
        activation_id: None,
        submitted_at: at,
        terminal_at: None,
        correlation_key: Some("INV-1042".into()),
        parent_workflow_id: None,
        root_workflow_id: None,
    };
    let workflows = ConsolePage {
        items: vec![ConsoleWorkflowSummary {
            workflow: workflow.clone(),
            controller: descriptor.clone(),
            queue: "billing".into(),
        }],
        next_cursor: None,
        observed_at: at,
    };
    let pending = ConsoleTaskResult {
        task: status.clone(),
        outcome: None,
        observed_at: at,
    };
    let mut done = status.clone();
    done.state = TaskState::Succeeded;
    done.attempt_count = 1;
    done.latest_attempt_id = Some("att_invoice_1".into());
    done.terminal_at = Some(at + 100);
    let success = ConsoleTaskResult {
        task: done,
        outcome: Some(TaskOutcome::Succeeded {
            attempt_id: "att_invoice_1".into(),
            quiescence: Quiescence::Confirmed,
            execution_may_have_started: true,
            output: Value::Null,
        }),
        observed_at: at + 100,
    };
    let config = ConsoleConfig {
        contract_version: CONSOLE_CONTRACT_VERSION,
        server_version: "0.1.1".into(),
        instance_id: "instance_demo".into(),
        instance_name: "Ledgence".into(),
        capabilities: ConsoleCapabilities {
            execution_progress: true,
            execution_artifacts: true,
            queue_policies: true,
            resource_policies: true,
            resource_sources: true,
            resource_fair_sharing: true,
            run_budgets: true,
            task_run_budgets: false,
            task_admission: false,
            executions: true,
            workflows: true,
            programs: true,
            workers: true,
        },
        suggested_queues: vec!["billing".into()],
        limits: ConsoleLimits::default(),
        polling: ConsolePolling::default(),
    };
    let metadata = ProgramDisplayMetadata {
        display_name: Some("Invoice issuer".into()),
        description: Some("Issue an invoice from the supplied event.".into()),
        kind: ConsoleProgramKind::Task,
    };
    let version = ConsoleProgramVersion {
        descriptor: descriptor.clone(),
        manifest: ProgramManifest {
            schema_version: 1,
            program: descriptor.program.clone(),
            runtime: PythonRuntime {
                kind: "python".into(),
                python: "3.12".into(),
                protocol: 1,
            },
            handler: "app:handle".into(),
            platform: Platform {
                os: "linux".into(),
                arch: "x86_64".into(),
            },
        },
        metadata: metadata.clone(),
        registered_at: at,
        provenance: ProgramRegistrationProvenance::ConfiguredProgramStore,
    };
    let programs = ConsolePage {
        items: vec![ConsoleProgramSummary {
            program_id: descriptor.program.id.clone(),
            metadata: metadata.clone(),
            registered_versions: ConsoleU64(1),
            last_registered_at: at,
        }],
        next_cursor: None,
        observed_at: at,
    };
    let versions = ConsolePage {
        items: vec![version.clone()],
        next_cursor: None,
        observed_at: at,
    };
    let program_detail = ConsoleProgramDetail {
        version: version.clone(),
        observed_at: at,
    };
    let program_receipt = RegisterProgramReply {
        version,
        already_registered: false,
        metadata_updated: false,
    };
    let worker = ConsoleWorkerSummary {
        worker_session_id: "ws_demo".into(),
        display_name: Some("Local Python worker".into()),
        queue: "billing".into(),
        capacity: 2,
        session_expires_at: at + 86_400_000,
        session_expired: false,
        snapshot_sequence: Some(ConsoleU64(9_007_199_254_740_993)),
        received_at: Some(at),
        accepting: Some(true),
        active_consumers: Some(0),
        occupied_process_slots: Some(1),
        detail_state: Some(WorkerObservationDetailState::Available),
        freshness: WorkerObservationFreshness::Fresh,
    };
    let workers = ConsolePage {
        items: vec![worker.clone()],
        next_cursor: None,
        observed_at: at,
    };
    let slots = vec![
        ConsoleWorkerSlot {
            slot_id: 0,
            state: ProcessSlotState::Warm,
            process_instance_id: Some("process_1".into()),
            process_id: Some(1042),
            program: Some(descriptor.program.clone()),
            digest: Some(descriptor.digest.clone()),
            task_id: None,
            attempt_id: None,
            consumer_id: None,
            link_diagnostic: None,
        },
        ConsoleWorkerSlot {
            slot_id: 1,
            state: ProcessSlotState::Empty,
            process_instance_id: None,
            process_id: None,
            program: None,
            digest: None,
            task_id: None,
            attempt_id: None,
            consumer_id: None,
            link_diagnostic: None,
        },
    ];
    let worker_detail = ConsoleWorkerDetail {
        worker,
        slots: ConsolePage {
            items: slots,
            next_cursor: None,
            observed_at: at,
        },
    };
    let attempt = ConsoleAttemptSummary {
        task_id: status.task_id.clone(),
        attempt_id: "att_invoice_1".into(),
        generation: 1,
        worker_session_id: "ws_demo".into(),
        consumer_id: 0,
        state: AttemptState::Succeeded,
        execution_may_have_started: true,
        quiescence: Quiescence::Confirmed,
        claimed_at: Some(at),
        dispatch_authorized_at: Some(at + 1),
        finished_at: Some(at + 100),
    };
    let attempts = ConsolePage {
        items: vec![attempt.clone()],
        next_cursor: None,
        observed_at: at + 100,
    };
    let attempt_detail = ConsoleAttemptDetail {
        attempt,
        descriptor: descriptor.clone(),
        phase: None,
        error: None,
        application_error: None,
        cleanup_error: None,
        process_id: Some(1042),
        process_instance_id: None,
        reused_process: Some(false),
        worker_elapsed_ms: Some(ConsoleU64(100)),
        observed_at: at + 100,
    };
    let numeric_payload=serde_json::from_str::<Value>(r#"{"large":9007199254740993,"maximum":18446744073709551615,"integer":1,"float":1.0,"negative_zero":-0.0,"nested":[-9223372036854775808]}"#).unwrap();
    let task_detail = ConsoleTaskDetail {
        admission_deferrals: 0,
        task_id: status.task_id.clone(),
        run_id: status.run_id.clone(),
        workflow_id: None,
        workflow_activation_id: None,
        provider_probe: None,
        parent_workflow_id: None,
        root_workflow_id: None,
        input: ConsoleTaskInput {
            budget: None,
            admission: None,
            program: descriptor.program.clone(),
            queue: "billing".into(),
            correlation_key: status.correlation_key.clone(),
            data: numeric_payload.clone(),
            retry_policy: RetryPolicy::default(),
            attempt_timeout_ms: 300_000,
        },
        descriptor,
        idempotency_key: "issue:INV-1042".into(),
        origin_trace: None,
        state: TaskState::Queued,
        attempt_count: 0,
        current_attempt_id: None,
        submitted_at: at,
        available_at: at,
        terminal_at: None,
        cancel_requested_at: None,
        observed_at: at,
    };
    let workflow_detail = ConsoleWorkflowDetail {
        summary: workflows.items[0].clone(),
        continuation: "after_invoice".into(),
        child_wait: Some(ConsoleChildWait {
            activation_id: "task_controller_1".into(),
            command_keys: vec!["issue".into()],
        }),
        external_wait_key: None,
        native_effect_wait: None,
        provider_wait: None,
        admission_wait: None,
        observed_at: at,
    };
    let activations = ConsolePage {
        items: vec![ConsoleActivation {
            workflow_id: workflow.workflow_id.clone(),
            activation_id: "task_controller_1".into(),
            task_id: "task_controller_1".into(),
            revision: workflow.revision,
            state: TaskState::Succeeded,
            applied_at: Some(at),
            error: None,
        }],
        next_cursor: None,
        observed_at: at,
    };
    let children = ConsolePage {
        items: vec![ConsoleWorkflowChild {
            workflow_id: workflow.workflow_id.clone(),
            creating_activation_id: "task_controller_1".into(),
            creating_revision: workflow.revision,
            kind: ConsoleChildKind::Task,
            command_key: "issue".into(),
            target_id: "task_child_1".into(),
            task_state: Some(TaskState::Queued),
            workflow_state: None,
            consumed: false,
        }],
        next_cursor: None,
        observed_at: at,
    };
    let waits = ConsoleWorkflowWaits {
        page: ConsolePage::<ConsoleWorkflowWait> {
            items: vec![],
            next_cursor: None,
            observed_at: at,
        },
        child_wait: workflow_detail.child_wait.clone(),
        revision: workflow.revision,
    };
    let local_steps = ConsolePage {
        items: vec![ConsoleLocalStep {
            workflow_id: workflow.workflow_id.clone(),
            activation_id: "task_controller_1".into(),
            step_key: "lookup".into(),
            callable: "billing.lookup".into(),
            attempt_id: "att_controller_1".into(),
            accepted_at: at,
        }],
        next_cursor: None,
        observed_at: at,
    };
    let workflow_history = ConsolePage {
        items: vec![ConsoleWorkflowHistory {
            workflow_id: workflow.workflow_id.clone(),
            sequence: ConsoleU64(9_007_199_254_740_993),
            activation_id: Some("task_controller_1".into()),
            at,
            reason: "waiting".into(),
        }],
        next_cursor: None,
        observed_at: at,
    };
    let step = ConsoleLocalStep {
        workflow_id: workflow.workflow_id.clone(),
        activation_id: "task_controller_1".into(),
        step_key: "tool:lookup:0".into(),
        callable: "tool:billing:lookup".into(),
        attempt_id: "att_controller_1".into(),
        accepted_at: at,
    };
    let local_step_detail = ConsoleLocalStepDetail::from_record(step.clone(), LocalStepRecord {
        key: step.step_key, callable: step.callable,
        input: json!({"v":1,"kind":"tool","version":"v1","arguments":{"id":u64::MAX,"factor":1.0}}),
        output: json!({"balance":-0.0}),
    }).unwrap();
    let probe_marker = ConsoleProviderProbeAssignment {
        occurrence: EffectOccurrence {
            scope: Scope {
                tenant_id: "acme".into(),
                namespace: "billing".into(),
            },
            workflow_id: "wf_invoice_1042".into(),
            activation_id: "task_controller_1".into(),
            approval_key: "issue".into(),
        },
        probe_id: "probe_invoice_1".into(),
        generation: ConsoleU64(3),
    };
    let mut probe_status = status.clone();
    probe_status.task_id = "task_probe_invoice_1".into();
    probe_status.workflow_id = Some(probe_marker.occurrence.workflow_id.clone());
    probe_status.provider_probe = Some(probe_marker.clone());
    probe_status.queue = "provider-checks".into();
    let mut probe_detail = task_detail.clone();
    probe_detail.task_id = probe_status.task_id.clone();
    probe_detail.workflow_id = probe_status.workflow_id.clone();
    // Detail carries the owning workflow's ancestry; the root owner itself is
    // workflow_id. Execution history separately uses its direct owner.
    probe_detail.parent_workflow_id = None;
    probe_detail.root_workflow_id = None;
    probe_detail.provider_probe = Some(probe_marker.clone());
    probe_detail.input.queue = probe_status.queue.clone();
    let probe_execution = ConsoleExecutionSummary {
        kind: ConsoleExecutionKind::Task,
        id: probe_status.task_id.clone(),
        descriptor: probe_detail.descriptor.clone(),
        queue: probe_status.queue.clone(),
        state: ConsoleExecutionState::Queued,
        submitted_at: at,
        terminal_at: None,
        correlation_key: probe_status.correlation_key.clone(),
        parent_workflow_id: probe_status.workflow_id.clone(),
        root_workflow_id: probe_status.workflow_id.clone(),
        provider_probe: Some(probe_marker),
    };
    let artifact_manifest = ConsoleArtifactManifest::from(ArtifactManifest {
        reference: ArtifactRef {
            schema: "ledgence.artifact.v1".into(),
            id: "art_demo_report".into(),
            digest: Digest(
                "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".into(),
            ),
            size: 5,
            media_type: "text/plain".into(),
        },
        scope: Scope {
            tenant_id: "acme".into(),
            namespace: "billing".into(),
        },
        root: ExecutionTarget::Workflow {
            id: "wf_invoice_1042".into(),
        },
        producer: ArtifactProducer::Task {
            task_id: "task_invoice_1042".into(),
        },
        key: "invoice-report".into(),
        filename: Some("invoice-report.txt".into()),
        created_at: at,
        accepted_at: at + 10,
        first_attempt_id: Some("attempt_1".into()),
    });
    let artifact_page = ConsoleArtifactPage {
        items: vec![artifact_manifest.clone()],
        next_cursor: None,
    };
    let mut result = json!({"artifact_manifest":artifact_manifest,"artifact_page":artifact_page,"contract_version":CONSOLE_CONTRACT_VERSION,"config":config,"tasks":tasks,"task_status":ConsoleObservedTaskStatus{task:status,observed_at:at},"task_detail":task_detail,"attempts":attempts,"attempt_detail":attempt_detail,
        "workflows":workflows,"workflow_status":ConsoleObservedWorkflowStatus{workflow,observed_at:at},"workflow_detail":workflow_detail,"activations":activations,"children":children,"waits":waits,"local_steps":local_steps,"local_step_detail":local_step_detail,"workflow_history":workflow_history,
        "pending_result":pending,"null_result":success,"numeric_payload":numeric_payload,
        "programs":programs,"program_versions":versions,"program_detail":program_detail,"program_receipt":program_receipt,"workers":workers,"worker_detail":worker_detail});
    result
        .as_object_mut()
        .unwrap()
        .extend(explorer::fixtures().as_object().unwrap().clone());
    let queue_policy = QueuePolicySnapshot {
        record: QueuePolicyRecord {
            scope: Scope {
                tenant_id: "acme".into(),
                namespace: "billing".into(),
            },
            queue: "billing".into(),
            revision: 9_007_199_254_740_993,
            updated_at: 100,
            policy: QueueConcurrencyPolicy {
                max_concurrency: Some(5),
                timeout: Some(QueueHoldTimeout {
                    after_ms: 30_000,
                    release: QueueTimeoutRelease::AfterGrace { grace_ms: 5000 },
                }),
            },
        },
        occupied: 1,
        uncertain: 1,
    };
    queue_policy.validate().unwrap();
    let queue_hold = QueueHoldSnapshot {
        scope: queue_policy.record.scope.clone(),
        queue: "billing".into(),
        task_id: "task_invoice".into(),
        attempt_id: "attempt_invoice".into(),
        worker_session_id: "ws_invoice".into(),
        consumer_id: 0,
        revision: 4,
        state: QueueHoldState::Uncertain,
        acquired_at: 100,
        policy_revision: queue_policy.record.revision,
        deadline: Some(30_100),
        stop_requested_at: Some(30_500),
        grace_ms: Some(5000),
        force_at: Some(35_500),
        released_at: None,
        release_kind: None,
    };
    queue_hold.validate().unwrap();
    let fair_resource = ResourceSnapshot {
        resource: ResourceRecord {
            scope: queue_policy.record.scope.clone(),
            name: "model.requests".into(),
            revision: 3,
            updated_at: at,
            policy: ResourcePolicy {
                enabled: true,
                max_occupancy: 5,
                rate: None,
                offer_timeout_ms: 30_000,
                start_timeout_ms: 1_000,
                fair_sharing: Some(ResourceFairSharing {
                    groups: vec![
                        ResourceFairGroup {
                            name: "support".into(),
                            weight: 3,
                            queues: vec!["support".into()],
                        },
                        ResourceFairGroup {
                            name: "batch".into(),
                            weight: 1,
                            queues: vec![],
                        },
                    ],
                    default_group: "batch".into(),
                }),
            },
        },
        offered: 1,
        occupied: 2,
        unknown: 0,
        quarantined: false,
    };
    fair_resource.validate().unwrap();
    let decisions = ResourceDecisionPage {
        items: vec![ResourceOfferDecision {
            scope: fair_resource.resource.scope.clone(),
            resource: fair_resource.resource.name.clone(),
            sequence: 9_007_199_254_740_993,
            policy_revision: 3,
            group: "support".into(),
            weight: 3,
            eligible_groups: 2,
            cycle_round: 2,
            reason: ResourceDecisionReason::Weighted,
            queue: "support".into(),
            subject: ResourceDecisionSubject::Task {
                task_id: "task_support".into(),
                generation: 1,
            },
            offer_generation: 1,
            offered_at: at,
        }],
        next: Some(9_007_199_254_740_993),
    };
    decisions.validate().unwrap();
    let root = RunBudgetSubject::Workflow {
        workflow_id: "wf_root".into(),
    };
    let subject = RunBudgetSubject::Workflow {
        workflow_id: "wf_review".into(),
    };
    let budget = RunBudgetSnapshot {
        scope: fair_resource.resource.scope.clone(),
        subject: subject.clone(),
        root: root.clone(),
        policy: RunBudgetPolicy {
            max_calls: Some(5),
            max_delegations: Some(3),
            deadline_at_ms: None,
        },
        calls: 1,
        delegations: 1,
        observed_at: fair_resource.resource.updated_at,
    };
    budget.validate().unwrap();
    let entries = RunBudgetPage {
        scope: budget.scope.clone(),
        subject: subject.clone(),
        root: root.clone(),
        items: vec![RunBudgetEntry {
            scope: budget.scope.clone(),
            root,
            sequence: 9_007_199_254_740_993,
            subject,
            action: RunBudgetAction::Call,
            outcome: RunBudgetOutcome::Authorized,
            reason: None,
            source_id: "source_review".into(),
            approval: Some(EffectOccurrence {
                scope: budget.scope.clone(),
                workflow_id: "wf_review".into(),
                activation_id: "act_review".into(),
                approval_key: "approve".into(),
            }),
            usage: RunBudgetUsage::Reported {
                input_tokens: Some(120),
                output_tokens: Some(16),
            },
            recorded_at: budget.observed_at,
        }],
        next: Some(9_007_199_254_740_993),
    };
    entries.validate().unwrap();
    let large_budget = RunBudgetSnapshot {
        policy: RunBudgetPolicy {
            max_calls: None,
            max_delegations: None,
            deadline_at_ms: Some(budget.observed_at + 1),
        },
        calls: 9_007_199_254_740_993,
        delegations: u64::MAX,
        ..budget.clone()
    };
    large_budget.validate().unwrap();
    result["run_budget_large_usage"] = serde_json::to_value(large_budget).unwrap();
    result["run_budget"] = serde_json::to_value(budget).unwrap();
    result["run_budget_entries"] = serde_json::to_value(entries).unwrap();
    result["fair_resource_snapshot"] = serde_json::to_value(fair_resource).unwrap();
    result["resource_decision_page"] = serde_json::to_value(decisions).unwrap();
    result["queue_policy_snapshot"] = serde_json::to_value(queue_policy).unwrap();
    result["queue_hold_snapshot"] = serde_json::to_value(queue_hold).unwrap();
    result["admission"] = explorer::admission_cases();
    let retained: AdmissionSnapshot =
        serde_json::from_value(result["admission"]["provider"]["unknown_pending"].clone()).unwrap();
    let binding = retained.binding.provider().unwrap();
    let source_query = ResourceSourceQuery {
        scope: retained.occurrence.scope.clone(),
        name: retained.binding.resource().to_owned(),
        state: Some(ResourceSourceState::UnknownOccupied),
        cursor: None,
        limit: 1,
    };
    let source_sequence = 9_007_199_254_740_993;
    let sources = ResourceSourcePage {
        items: vec![ResourceSource {
            scope: source_query.scope.clone(),
            resource: source_query.name.clone(),
            sequence: source_sequence,
            revision: retained.revision,
            updated_at: retained.updated_at,
            state: ResourceSourceState::UnknownOccupied,
            subject: ResourceSourceSubject::Provider {
                workflow_id: retained.occurrence.workflow_id,
                origin_activation_id: retained.occurrence.origin_activation_id,
                key: retained.occurrence.key,
                approval_activation_id: binding.ticket.occurrence.activation_id.clone(),
                approval_key: binding.ticket.occurrence.approval_key.clone(),
            },
        }],
        next_cursor: Some(source_query.next_cursor(source_sequence).unwrap()),
    };
    sources.validate_for(&source_query).unwrap();
    result["resource_sources"] = serde_json::to_value(sources).unwrap();
    result["provider_probe_status"] = serde_json::to_value(ConsoleObservedTaskStatus {
        task: probe_status,
        observed_at: at,
    })
    .unwrap();
    result["provider_probe_detail"] = serde_json::to_value(probe_detail).unwrap();
    result["provider_probe_execution"] = serde_json::to_value(probe_execution).unwrap();
    let mut admitted: ConsoleObservedTaskStatus =
        serde_json::from_value(result["task_status"].clone()).unwrap();
    admitted.task.admission = Some(TaskAdmissionRequirement {
        resource: "model.requests".into(),
    });
    admitted.task.attempt_count = 1001;
    admitted.task.admission_deferrals = 1000;
    admitted.task.latest_attempt_id = Some("attempt_preparation_1001".into());
    admitted.task.validate().unwrap();
    result["admitted_task_status"] = serde_json::to_value(admitted).unwrap();
    let mut admitted: ConsoleTaskDetail =
        serde_json::from_value(result["task_detail"].clone()).unwrap();
    admitted.input.admission = Some(TaskAdmissionRequirement {
        resource: "model.requests".into(),
    });
    admitted.attempt_count = 1001;
    admitted.admission_deferrals = 1000;
    admitted.validate().unwrap();
    result["admitted_task_detail"] = serde_json::to_value(admitted).unwrap();
    for (name, target, state, reason, after, activation_id) in [
        (
            "task",
            ExecutionProgressTarget::Task {
                id: "task_invoice_1042".into(),
            },
            "queued",
            "submitted",
            0u64,
            None,
        ),
        (
            "workflow",
            ExecutionProgressTarget::Workflow {
                id: "wf_invoice_1042".into(),
            },
            "waiting",
            "wait_registered",
            9_007_199_254_740_992u64,
            Some("activation_1".to_owned()),
        ),
    ] {
        let mut query = ExecutionProgressQuery {
            scope: Scope {
                tenant_id: "acme".into(),
                namespace: "billing".into(),
            },
            target: target.clone(),
            cursor: None,
            limit: 100,
            wait_ms: 5000,
        };
        if after != 0 {
            query.cursor = Some(query.cursor_for(after).unwrap());
        }
        let page = ExecutionProgressPage {
            scope: query.scope.clone(),
            target,
            state: state.into(),
            terminal: false,
            high_watermark: ProgressSequence(after + 1),
            events: vec![ExecutionProgressEvent {
                sequence: ProgressSequence(after + 1),
                at,
                reason: reason.into(),
                activation_id,
                attempt_id: None,
            }],
            next_cursor: query.cursor_for(after + 1).unwrap(),
            has_more: false,
        };
        page.validate_for(&query).unwrap();
        result[format!("progress_{name}_query")] = serde_json::to_value(query).unwrap();
        result[format!("progress_{name}_page")] = serde_json::to_value(page).unwrap();
    }
    result
}
