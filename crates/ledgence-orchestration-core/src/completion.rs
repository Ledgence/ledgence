//! Deterministic completion envelopes and transport retry timing.
use crate::*;

pub fn task_completion_event(task: &TaskSnapshot) -> Result<CompletionEvent> {
    if !task.state.is_terminal() {
        return Err(invalid_completion("task is not terminal"));
    }
    let target = CompletionTarget::Task {
        id: task.task_id.clone(),
    };
    let mut value = base(
        &task.scope(),
        &target,
        match task.state {
            TaskState::Succeeded => "succeeded",
            TaskState::Failed => "failed",
            TaskState::Cancelled => "cancelled",
            _ => return Err(invalid_completion("task is not terminal")),
        },
        task.terminal_at,
        task.input.correlation_key.as_deref(),
        task.origin_trace.as_ref(),
    )?;
    value["ldgtaskid"] = json!(task.task_id);
    value["ldgrunid"] = json!(task.run_id);
    if task.state != TaskState::Cancelled {
        optional(
            &mut value,
            "ldgattemptid",
            task.current_attempt_id.as_deref(),
        );
    }
    optional(&mut value, "ldgworkflowid", task.workflow_id.as_deref());
    optional(
        &mut value,
        "ldgactivationid",
        task.workflow_activation_id.as_deref(),
    );
    optional(
        &mut value,
        "ldgparentworkflowid",
        task.parent_workflow_id.as_deref(),
    );
    optional(
        &mut value,
        "ldgrootworkflowid",
        task.root_workflow_id.as_deref(),
    );
    CompletionEvent::new(value)
}

pub fn workflow_completion_event(
    workflow: &WorkflowSnapshot,
    origin: Option<&TraceContext>,
) -> Result<CompletionEvent> {
    workflow.validate()?;
    if !workflow.state.is_terminal() {
        return Err(invalid_completion("workflow is not terminal"));
    }
    let target = CompletionTarget::Workflow {
        id: workflow.workflow_id.clone(),
    };
    let mut value = base(
        &workflow.scope,
        &target,
        match workflow.state {
            WorkflowState::Succeeded => "succeeded",
            WorkflowState::Failed => "failed",
            WorkflowState::Cancelled => "cancelled",
            _ => return Err(invalid_completion("workflow is not terminal")),
        },
        workflow.terminal_at,
        workflow.correlation_key.as_deref(),
        origin,
    )?;
    value["ldgworkflowid"] = json!(workflow.workflow_id);
    optional(
        &mut value,
        "ldgparentworkflowid",
        workflow.parent_workflow_id.as_deref(),
    );
    optional(
        &mut value,
        "ldgrootworkflowid",
        workflow.root_workflow_id.as_deref(),
    );
    CompletionEvent::new(value)
}

fn base(
    scope: &Scope,
    target: &CompletionTarget,
    state: &str,
    at: Option<Timestamp>,
    correlation: Option<&str>,
    origin: Option<&TraceContext>,
) -> Result<Value> {
    scope.validate()?;
    target.validate()?;
    let at = at.ok_or_else(|| invalid_completion("missing completion timestamp"))?;
    let mut value = json!({
        "specversion":"1.0", "id":completion_event_id(target),
        "source":"urn:ledgence:orchestrator",
        "type":format!("com.ledgence.{}.completed.v1",target.kind()),
        "subject":format!("{}s/{}",target.kind(),target.id()), "time":timestamp(at)?,
        "ldgtenantid":scope.tenant_id,"ldgnamespace":scope.namespace,"ldgstate":state,
        "ldgresultref":completion_result_ref(scope,target),
    });
    if let Some(key) = correlation {
        if key.len() > 512 || key.chars().any(char::is_control) {
            return Err(invalid_completion("invalid accepted business correlation"));
        }
        if key.chars().any(|character| {
            let code = u32::from(character);
            (0xfdd0..=0xfdef).contains(&code) || code & 0xfffe == 0xfffe
        }) {
            value["ldgcorrelationkey"] = json!(encode_completion_correlation(key));
            value["ldgcorrelationkeyencoding"] = json!("percent");
        } else {
            value["ldgcorrelationkey"] = json!(key);
        }
    }
    if let Some(trace) = origin {
        trace.validate()?;
        value["traceparent"] = json!(trace.traceparent);
        optional(&mut value, "tracestate", trace.tracestate.as_deref());
    }
    Ok(value)
}
fn optional(value: &mut Value, name: &str, text: Option<&str>) {
    if let Some(text) = text {
        value[name] = json!(text);
    }
}
fn invalid_completion(message: &str) -> ContractError {
    ContractError::InvalidInput(message.into())
}

/// Five-second exponential base, up to five minutes, with deterministic 0..25%
/// jitter. No per-subscription timer or randomness state is held in memory.
pub fn completion_retry_delay_ms(subscription_id: &str, attempts: u32) -> u64 {
    let base = 5_000u64
        .saturating_mul(1u64 << attempts.saturating_sub(1).min(16))
        .min(COMPLETION_MAX_RETRY_DELAY_MS);
    let hash = subscription_id
        .bytes()
        .chain(attempts.to_le_bytes())
        .fold(0xcbf29ce484222325u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
        });
    base.saturating_add(hash % (base / 4 + 1))
        .min(COMPLETION_MAX_RETRY_DELAY_MS)
}
