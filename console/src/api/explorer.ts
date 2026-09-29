// SPDX-License-Identifier: MIT
import * as s from "./schema";
import { descriptor, programSummary } from "./resources";
export const executionKind = s.enumeration("task", "workflow");
export const executionState = s.enumeration(
  "queued",
  "active",
  "running",
  "waiting",
  "failing",
  "cancelling",
  "succeeded",
  "failed",
  "cancelled",
);
export const execution = s.object({
  kind: executionKind,
  id: s.id,
  descriptor,
  queue: s.id,
  state: executionState,
  submitted_at: s.timestamp,
  terminal_at: s.nullable(s.timestamp),
  correlation_key: s.nullable(s.string),
  parent_workflow_id: s.nullable(s.id),
  root_workflow_id: s.nullable(s.id),
});
export const executionPage = s.page(execution);
export type Execution = s.Decoded<typeof execution>;
export const catalogPage = s.page(
  s.object({
    program: programSummary,
    kinds: s.array(s.enumeration("task", "workflow", "unspecified"), 3),
  }),
);
export function executionPath(kind: "task" | "workflow", id: string) {
  return `/${kind === "task" ? "executions" : "workflows"}/${encodeURIComponent(id)}`;
}
export function programHistory(programId: string, version?: string) {
  const params = new URLSearchParams({
    program_id: programId,
    include_children: "true",
  });
  if (version) params.set("version", version);
  return `/executions?${params}`;
}

import {
  applicationError,
  program,
  taskState,
  workflowDetail,
} from "./resources";
const identity = s.object({ kind: executionKind, id: s.id });
const availability = s.enumeration("available", "unavailable");
const base = {
  id: s.string,
  activation_id: s.id,
  revision: s.decimal,
  entrypoint: s.string,
};
const localState = s.enumeration("returned", "failed", "cancelled", "replayed");
const rawExplorerNode = s.variant({
  phase: s.object({
    ...base,
    kind: s.enumeration("phase"),
    state: s.nullable(taskState),
    availability,
    submitted_at: s.timestamp,
    terminal_at: s.nullable(s.timestamp),
    applied_at: s.nullable(s.timestamp),
    decision_kind: s.nullable(
      s.enumeration("continue", "suspend", "wait", "complete", "fail"),
    ),
    error: s.nullable(applicationError),
    resumed_activation_id: s.nullable(s.id),
  }),
  child: s.object({
    ...base,
    kind: s.enumeration("child"),
    key: s.id,
    execution: identity,
    program,
    fork_key: s.nullable(s.id),
    availability,
    state: s.nullable(executionState),
    submitted_at: s.timestamp,
    terminal_at: s.nullable(s.timestamp),
  }),
  fork: s.object({
    ...base,
    kind: s.enumeration("fork"),
    key: s.id,
    branch_keys: s.array(s.id, 128),
    accepted_at: s.timestamp,
    accepting_attempt_id: s.id,
  }),
  local: s.object({
    ...base,
    kind: s.enumeration("local"),
    key: s.id,
    callable: s.string,
    accepted_at: s.nullable(s.timestamp),
    accepting_attempt_id: s.nullable(s.id),
    observation: s.nullable(
      s.object({
        attempt_id: s.id,
        started_at: s.timestamp,
        elapsed_us: s.decimal,
        state: localState,
      }),
    ),
  }),
  child_wait: s.object({
    ...base,
    kind: s.enumeration("child_wait"),
    member_keys: s.array(s.id, 128),
    resume: s.string,
    applied_at: s.timestamp,
    resumed_activation_id: s.nullable(s.id),
  }),
  external_wait: s.object({
    ...base,
    kind: s.enumeration("external_wait"),
    key: s.id,
    wait_kind: s.enumeration("event", "timer"),
    deadline: s.nullable(s.timestamp),
    registered_at: s.timestamp,
    closed_at: s.nullable(s.timestamp),
    wake_reason: s.nullable(s.enumeration("event", "timer", "timeout")),
    resumed_activation_id: s.nullable(s.id),
  }),
});
export const explorerNode = s.refine(
  rawExplorerNode,
  (node) => {
    if (node.kind === "local")
      return (
        (node.accepted_at !== null || node.observation !== null) &&
        (node.accepted_at === null) === (node.accepting_attempt_id === null)
      );
    if (node.kind === "phase")
      return (
        !(node.error && node.decision_kind) &&
        (!node.decision_kind || node.applied_at !== null) &&
        (!node.resumed_activation_id ||
          ["continue", "suspend", "wait"].includes(node.decision_kind ?? ""))
      );
    if (node.kind === "fork")
      return new Set(node.branch_keys).size === node.branch_keys.length;
    if (node.kind === "child_wait")
      return new Set(node.member_keys).size === node.member_keys.length;
    if (node.kind === "external_wait")
      return (
        (node.wake_reason === null) === (node.resumed_activation_id === null) &&
        (!node.wake_reason || node.closed_at !== null)
      );
    return true;
  },
  "Inconsistent workflow explorer evidence.",
);
export type ExplorerNode = s.Decoded<typeof explorerNode>;
export const workflowExplorer = s.object({
  workflow: workflowDetail,
  page: s.page(explorerNode),
  evidence: s.enumeration("retained_records_only"),
});
export const workflowInput = s.object({
  workflow_id: s.id,
  data: s.payload,
  observed_at: s.timestamp,
});
export const ancestry = s.object({
  execution: identity,
  path: s.array(
    s.object({
      execution: identity,
      program: s.nullable(program),
      availability,
    }),
    18,
  ),
  observed_at: s.timestamp,
});
export const attemptObservations = s.object({
  attempt_id: s.id,
  task_id: s.id,
  observations: s.nullable(
    s.object({
      runtime_started_at_ms: s.timestamp,
      runtime_elapsed_us: s.decimal,
      process_cpu_user_us: s.nullable(s.decimal),
      process_cpu_system_us: s.nullable(s.decimal),
      process_lifetime_peak_rss_bytes: s.nullable(s.decimal),
      local_steps: s.array(
        s.object({
          key: s.id,
          callable: s.string,
          started_at_ms: s.timestamp,
          elapsed_us: s.decimal,
          state: localState,
        }),
        256,
      ),
      local_steps_truncated: s.boolean,
    }),
  ),
  observed_at: s.timestamp,
});
