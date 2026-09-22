use super::*;

#[test]
fn terminal_event_is_stable_reference_only_and_preserves_origin() {
    let current = claimed();
    let change = settle(
        &current.task,
        &current.attempt,
        &success(&current.attempt, Quiescence::Confirmed),
        NOW + 1,
    )
    .unwrap();
    let event = task_completion_event(&change.task).unwrap();
    assert_eq!(event, task_completion_event(&change.task).unwrap());
    assert_eq!(event.value()["ldgstate"], "succeeded");
    assert_eq!(event.trace_context(), command().origin_trace);
    assert_eq!(event.value()["time"], "1970-01-01T00:16:40.001Z");
    assert_eq!(event.id(), "evt_task_completed_task_1042");
    assert!(event.value().get("data").is_none());
    assert!(event.value().get("datacontenttype").is_none());
    assert!(event.value().get("output").is_none());
    assert!(event.value().get("ldgattemptid").is_none());
    assert!(ledgence_worker_api::validate_cloudevent_context(event.value()).is_ok());
    assert!(ledgence_worker_api::validate_json_cloudevent(event.value()).is_err());
}

#[test]
fn execution_reports_are_not_completion_until_quiescence_is_confirmed() {
    let current = claimed();
    for task in [&queued(), &current.task] {
        assert!(task_completion_event(task).is_err());
    }
    let change = settle(
        &current.task,
        &current.attempt,
        &success(&current.attempt, Quiescence::Unconfirmed),
        NOW + 1,
    )
    .unwrap();
    assert!(task_completion_event(&change.task).is_err());
    let cancelled = cancel(&queued(), None, NOW + 1).unwrap();
    let event = task_completion_event(&cancelled.task).unwrap();
    assert_eq!(event.value()["ldgstate"], "cancelled");
    assert!(event.value().get("ldgattemptid").is_none());
}

#[test]
fn every_accepted_business_key_can_be_projected_without_changing_it() {
    let mut task = cancel(&queued(), None, NOW + 1).unwrap().task;
    for key in [
        "",
        "literal%20not-space",
        "invoice/é",
        "reference\u{10ffff}",
        "\u{fdd0}",
    ] {
        task.input.correlation_key = Some(key.into());
        task.input.validate().unwrap();
        let event = task_completion_event(&task).unwrap();
        let actual = event.value()["ldgcorrelationkey"].as_str().unwrap();
        if event.value().get("ldgcorrelationkeyencoding").is_some() {
            assert_eq!(decode_completion_correlation(actual).unwrap(), key);
        } else {
            assert_eq!(actual, key);
        }
    }
}

fn workflow(state: WorkflowState) -> WorkflowSnapshot {
    WorkflowSnapshot {
        workflow_id: "wf_child".into(),
        scope: queued().scope(),
        state,
        revision: 2,
        activation_id: if state == WorkflowState::Running {
            Some("controller".into())
        } else {
            None
        },
        parent_workflow_id: Some("wf_parent".into()),
        root_workflow_id: Some("wf_root".into()),
        submitted_at: NOW,
        terminal_at: state.is_terminal().then_some(NOW + 1),
        correlation_key: Some("invoice".into()),
    }
}

#[test]
fn workflow_envelope_describes_only_true_terminal_run_after_owned_work_drain() {
    for state in [
        WorkflowState::Running,
        WorkflowState::Waiting,
        WorkflowState::Failing,
        WorkflowState::Cancelling,
    ] {
        assert!(workflow_completion_event(&workflow(state), None).is_err());
    }
    for (state, text) in [
        (WorkflowState::Succeeded, "succeeded"),
        (WorkflowState::Failed, "failed"),
        (WorkflowState::Cancelled, "cancelled"),
    ] {
        let event =
            workflow_completion_event(&workflow(state), command().origin_trace.as_ref()).unwrap();
        assert_eq!(event.value()["ldgstate"], text);
        assert_eq!(event.value()["ldgparentworkflowid"], "wf_parent");
        assert_eq!(event.value()["ldgrootworkflowid"], "wf_root");
        for key in [
            "ldgtaskid",
            "ldgrunid",
            "ldgattemptid",
            "ldgactivationid",
            "data",
        ] {
            assert!(event.value().get(key).is_none());
        }
    }
}

#[test]
fn retry_backoff_is_bounded_monotone_and_spreads_subscription_load() {
    let mut previous = 0;
    for attempt in 1..=100 {
        let delay = completion_retry_delay_ms("subscription", attempt);
        assert!((5000..=COMPLETION_MAX_RETRY_DELAY_MS).contains(&delay));
        assert!(delay >= previous);
        previous = delay;
        assert_eq!(delay, completion_retry_delay_ms("subscription", attempt));
    }
    let delays: std::collections::HashSet<_> = (0..100)
        .map(|i| completion_retry_delay_ms(&format!("sub_{i}"), 1))
        .collect();
    assert!(delays.len() > 80);
    assert_eq!(
        completion_retry_delay_ms("subscription", u32::MAX),
        COMPLETION_MAX_RETRY_DELAY_MS
    );
}
