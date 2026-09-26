// Explicit safe projections mirror the Rust Console contract; no transport casts.
import * as s from "./schema";
export const program = s.object({ id: s.id, version: s.id });
export const descriptor = s.object({ program, digest: s.id, size: s.decimal });
export const taskState = s.enumeration(
  "queued",
  "active",
  "succeeded",
  "failed",
  "cancelled",
);
export const workflowState = s.enumeration(
  "running",
  "waiting",
  "failing",
  "cancelling",
  "succeeded",
  "failed",
  "cancelled",
);
export const attemptState = s.enumeration(
  "active",
  "succeeded",
  "failed",
  "cancelled",
  "lost",
);
export const quiescence = s.enumeration("confirmed", "unconfirmed");
export const taskStatus = s.object({
  task_id: s.id,
  run_id: s.id,
  workflow_id: s.nullable(s.id),
  workflow_activation_id: s.nullable(s.id),
  queue: s.id,
  correlation_key: s.nullable(s.string),
  state: taskState,
  attempt_count: s.integer(),
  current_attempt_id: s.nullable(s.id),
  latest_attempt_id: s.nullable(s.id),
  submitted_at: s.timestamp,
  available_at: s.timestamp,
  terminal_at: s.nullable(s.timestamp),
  cancel_requested_at: s.nullable(s.timestamp),
});
export const taskSummary = s.object({ task: taskStatus, descriptor });
export const taskPage = s.page(taskSummary);
export const retryPolicy = s.object({
  max_attempts: s.integer(1, 1000),
  retry_delay_ms: s.integer(0, 86400000),
});
export const submission = s.object({
  program,
  queue: s.id,
  correlation_key: s.nullable(s.string),
  data: s.payload,
  retry_policy: retryPolicy,
  attempt_timeout_ms: s.integer(1, 86400000),
});
export const trace: s.Decoder<{
  traceparent: string;
  tracestate: string | null;
}> = (value) => {
  if (value && typeof value === "object" && !("tracestate" in value))
    return { ...s.object({ traceparent: s.string })(value), tracestate: null };
  return s.object({ traceparent: s.string, tracestate: s.nullable(s.string) })(
    value,
  );
};
export const taskDetail = s.object({
  task_id: s.id,
  run_id: s.id,
  workflow_id: s.nullable(s.id),
  workflow_activation_id: s.nullable(s.id),
  parent_workflow_id: s.nullable(s.id),
  root_workflow_id: s.nullable(s.id),
  input: submission,
  descriptor,
  idempotency_key: s.id,
  origin_trace: s.nullable(trace),
  state: taskState,
  attempt_count: s.integer(),
  current_attempt_id: s.nullable(s.id),
  submitted_at: s.timestamp,
  available_at: s.timestamp,
  terminal_at: s.nullable(s.timestamp),
  cancel_requested_at: s.nullable(s.timestamp),
  observed_at: s.timestamp,
});
export const applicationError = s.object({ kind: s.string, message: s.string });
export const executionError = s.object({ kind: s.string, message: s.string });
export const failure = s.variant({
  application: s.object({
    kind: s.enumeration("application"),
    error: applicationError,
  }),
  execution: s.object({
    kind: s.enumeration("execution"),
    error: executionError,
    phase: s.string,
    cleanup_error: s.nullable(executionError),
  }),
  attempt_lost: s.object({ kind: s.enumeration("attempt_lost") }),
});
export const taskOutcome = s.variant({
  succeeded: s.object({
    kind: s.enumeration("succeeded"),
    attempt_id: s.id,
    quiescence,
    execution_may_have_started: s.boolean,
    output: s.payload,
  }),
  failed: s.object({
    kind: s.enumeration("failed"),
    attempt_id: s.id,
    quiescence,
    execution_may_have_started: s.boolean,
    failure,
  }),
  cancelled: s.object({ kind: s.enumeration("cancelled") }),
});
export const taskResult = s.refine(
  s.object({
    task: taskStatus,
    outcome: s.nullable(taskOutcome),
    observed_at: s.timestamp,
  }),
  (v) =>
    v.outcome === null
      ? ["queued", "active"].includes(v.task.state)
      : v.outcome.kind === v.task.state &&
        (!("attempt_id" in v.outcome) ||
          v.outcome.attempt_id === v.task.latest_attempt_id),
  "Inconsistent task outcome.",
);
export const observedTask = s.object({
  task: taskStatus,
  observed_at: s.timestamp,
});
export const attempt = s.object({
  task_id: s.id,
  attempt_id: s.id,
  generation: s.integer(1, 1000),
  worker_session_id: s.id,
  consumer_id: s.integer(),
  state: attemptState,
  execution_may_have_started: s.boolean,
  quiescence,
  claimed_at: s.nullable(s.timestamp),
  dispatch_authorized_at: s.nullable(s.timestamp),
  finished_at: s.nullable(s.timestamp),
});
export const attemptPage = s.page(attempt);
export const attemptDetail = s.object({
  attempt,
  descriptor,
  phase: s.nullable(s.string),
  error: s.nullable(executionError),
  application_error: s.nullable(applicationError),
  cleanup_error: s.nullable(executionError),
  process_id: s.nullable(s.integer()),
  process_instance_id: s.nullable(s.id),
  reused_process: s.nullable(s.boolean),
  worker_elapsed_ms: s.nullable(s.decimal),
  observed_at: s.timestamp,
});
export const taskHistory = s.object({
  sequence: s.decimal,
  task_id: s.id,
  attempt_id: s.nullable(s.id),
  at: s.timestamp,
  reason: s.string,
});
export const taskHistoryPage = s.page(taskHistory);
export const workflowStatus = s.object({
  workflow_id: s.id,
  state: workflowState,
  revision: s.decimal,
  activation_id: s.nullable(s.id),
  submitted_at: s.timestamp,
  terminal_at: s.nullable(s.timestamp),
  correlation_key: s.nullable(s.string),
  parent_workflow_id: s.nullable(s.id),
  root_workflow_id: s.nullable(s.id),
});
export const workflowSummary = s.object({
  workflow: workflowStatus,
  controller: descriptor,
  queue: s.id,
});
export const workflowPage = s.page(workflowSummary);
export const childWait = s.object({
  activation_id: s.id,
  command_keys: s.array(s.id, 1000),
});
export const workflowDetail = s.object({
  summary: workflowSummary,
  continuation: s.string,
  child_wait: s.nullable(childWait),
  external_wait_key: s.nullable(s.string),
  observed_at: s.timestamp,
});
export const workflowOutcome = s.variant({
  succeeded: s.object({ kind: s.enumeration("succeeded"), output: s.payload }),
  failed: s.object({ kind: s.enumeration("failed"), error: applicationError }),
  cancelled: s.object({ kind: s.enumeration("cancelled") }),
});
export const workflowResult = s.refine(
  s.object({
    workflow: workflowStatus,
    outcome: s.nullable(workflowOutcome),
    observed_at: s.timestamp,
  }),
  (v) =>
    v.outcome === null
      ? !["succeeded", "failed", "cancelled"].includes(v.workflow.state)
      : v.outcome.kind === v.workflow.state,
  "Inconsistent workflow outcome.",
);
export const observedWorkflow = s.object({
  workflow: workflowStatus,
  observed_at: s.timestamp,
});
export const activation = s.object({
  workflow_id: s.id,
  activation_id: s.id,
  task_id: s.id,
  revision: s.decimal,
  state: taskState,
  applied_at: s.nullable(s.timestamp),
  error: s.nullable(applicationError),
});
export const activationPage = s.page(activation);
export const child = s.object({
  workflow_id: s.id,
  creating_activation_id: s.id,
  creating_revision: s.decimal,
  kind: s.enumeration("task", "workflow"),
  command_key: s.id,
  target_id: s.id,
  task_state: s.nullable(taskState),
  workflow_state: s.nullable(workflowState),
  consumed: s.boolean,
});
export const childPage = s.page(child);
export const wait = s.object({
  workflow_id: s.id,
  wait_key: s.id,
  activation_id: s.id,
  kind: s.enumeration("event", "timer"),
  deadline: s.nullable(s.timestamp),
  registered_at: s.timestamp,
  closed_at: s.nullable(s.timestamp),
});
export const workflowWaits = s.object({
  page: s.page(wait),
  child_wait: s.nullable(childWait),
  revision: s.decimal,
});
export const workflowHistory = s.object({
  workflow_id: s.id,
  sequence: s.decimal,
  activation_id: s.nullable(s.id),
  at: s.timestamp,
  reason: s.string,
});
export const workflowHistoryPage = s.page(workflowHistory);
export const localStep = s.object({
  workflow_id: s.id,
  activation_id: s.id,
  step_key: s.id,
  callable: s.string,
  attempt_id: s.id,
  accepted_at: s.timestamp,
});
export const localStepPage = s.page(localStep);
export const eventReceipt = s.object({
  workflow_id: s.id,
  key: s.id,
  event_id: s.id,
  event_source: s.string,
  accepted_at: s.timestamp,
  already_accepted: s.boolean,
});
export const metadata = s.object({
  display_name: s.nullable(s.string),
  description: s.nullable(s.string),
  kind: s.enumeration("task", "workflow", "unspecified"),
});
export const programSummary = s.object({
  program_id: s.id,
  metadata,
  registered_versions: s.decimal,
  last_registered_at: s.timestamp,
});
export const programPage = s.page(programSummary);
export const manifest = s.object({
  schema_version: s.integer(1, 1),
  program,
  runtime: s.object({
    kind: s.enumeration("python"),
    python: s.string,
    protocol: s.integer(1, 3),
  }),
  handler: s.string,
  platform: s.object({
    os: s.enumeration("linux", "macos"),
    arch: s.enumeration("x86_64", "aarch64"),
  }),
});
export const programVersion = s.object({
  descriptor,
  manifest,
  metadata,
  registered_at: s.timestamp,
  provenance: s.enumeration("configured_program_store"),
});
export const programVersions = s.page(programVersion);
export const programDetail = s.object({
  version: programVersion,
  observed_at: s.timestamp,
});
export const programReceipt = s.object({
  version: programVersion,
  already_registered: s.boolean,
  metadata_updated: s.boolean,
});
export const worker = s.object({
  worker_session_id: s.id,
  display_name: s.nullable(s.string),
  queue: s.id,
  capacity: s.integer(1),
  session_expires_at: s.timestamp,
  session_expired: s.boolean,
  snapshot_sequence: s.nullable(s.decimal),
  received_at: s.nullable(s.timestamp),
  accepting: s.nullable(s.boolean),
  active_consumers: s.nullable(s.integer()),
  occupied_process_slots: s.nullable(s.integer()),
  detail_state: s.nullable(
    s.enumeration("available", "unsupported_capacity", "unavailable"),
  ),
  freshness: s.enumeration("fresh", "stale", "no_recent_report", "unavailable"),
});
export const workerPage = s.page(worker);
export const workerSlot = s.object({
  slot_id: s.integer(0, 1023),
  state: s.enumeration(
    "empty",
    "starting",
    "warm",
    "executing",
    "retiring",
    "cleanup_pending",
    "unknown",
  ),
  process_instance_id: s.nullable(s.id),
  process_id: s.nullable(s.integer()),
  program: s.nullable(program),
  digest: s.nullable(s.id),
  task_id: s.nullable(s.id),
  attempt_id: s.nullable(s.id),
  consumer_id: s.nullable(s.integer()),
  link_diagnostic: s.nullable(
    s.enumeration("authority_mismatch", "unavailable"),
  ),
});
export const workerDetail = s.object({ worker, slots: s.page(workerSlot) });
export type TaskSummary = s.Decoded<typeof taskSummary>;
export type TaskDetail = s.Decoded<typeof taskDetail>;
export type WorkflowSummary = s.Decoded<typeof workflowSummary>;
export type ProgramVersion = s.Decoded<typeof programVersion>;
export type Worker = s.Decoded<typeof worker>;
export const terminal = (state: string) =>
  ["succeeded", "failed", "cancelled"].includes(state);

export const cancelTaskReceipt = s.object({
  task_id: s.id,
  state: taskState,
  observed_at: s.timestamp,
});
