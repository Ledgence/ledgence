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
        task_id: "task_invoice_1042".into(),
        run_id: "run_invoice_1042".into(),
        workflow_id: None,
        workflow_activation_id: None,
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
        task_id: status.task_id.clone(),
        run_id: status.run_id.clone(),
        workflow_id: None,
        workflow_activation_id: None,
        parent_workflow_id: None,
        root_workflow_id: None,
        input: ConsoleSubmitTask {
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
    let mut result = json!({"contract_version":CONSOLE_CONTRACT_VERSION,"config":config,"tasks":tasks,"task_status":ConsoleObservedTaskStatus{task:status,observed_at:at},"task_detail":task_detail,"attempts":attempts,"attempt_detail":attempt_detail,
        "workflows":workflows,"workflow_status":ConsoleObservedWorkflowStatus{workflow,observed_at:at},"workflow_detail":workflow_detail,"activations":activations,"children":children,"waits":waits,"local_steps":local_steps,"workflow_history":workflow_history,
        "pending_result":pending,"null_result":success,"numeric_payload":numeric_payload,
        "programs":programs,"program_versions":versions,"program_detail":program_detail,"program_receipt":program_receipt,"workers":workers,"worker_detail":worker_detail});
    result
        .as_object_mut()
        .unwrap()
        .extend(explorer::fixtures().as_object().unwrap().clone());
    result
}
