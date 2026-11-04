// SPDX-License-Identifier: MIT
import * as s from "./schema";
import { stringifyUserJson } from "./json";
import { ContractError } from "./codecs";
import type { Approval } from "./approvals";
import { effectOccurrence } from "./effects";
import { admissionOccurrence } from "./resources";
import { taskAdmissionOccurrence } from "./task-admissions";

const id = s.refine(
  s.id,
  (value) => new TextEncoder().encode(value).length <= 128,
  "Audit identifier exceeds its limit.",
);
const issuer = s.refine(
  s.id,
  (value) => new TextEncoder().encode(value).length <= 256,
  "Audit issuer exceeds its limit.",
);
const scope = s.object({ tenant_id: id, namespace: id });
const audience = s.object({ authority_id: id, instance_id: id, scope });
const actor = s.object({
  issuer,
  subject: id,
  class: s.enumeration("operator", "service"),
  credential_id: id,
});
const resource = s.variant({
  queue: s.object({ kind: s.enumeration("queue"), queue: id }),
  resource: s.object({ kind: s.enumeration("resource"), name: id }),
  task_admission: s.object({
    kind: s.enumeration("task_admission"),
    occurrence: taskAdmissionOccurrence,
  }),
  admission: s.object({
    kind: s.enumeration("admission"),
    occurrence: admissionOccurrence,
  }),
  effect: s.object({
    kind: s.enumeration("effect"),
    workflow_id: id,
    activation_id: id,
    approval_key: id,
  }),
  task: s.object({ kind: s.enumeration("task"), task_id: id }),
  workflow: s.object({ kind: s.enumeration("workflow"), workflow_id: id }),
  local_step: s.object({
    kind: s.enumeration("local_step"),
    workflow_id: id,
    activation_id: id,
    step_key: id,
  }),
  approval: s.object({
    kind: s.enumeration("approval"),
    workflow_id: id,
    key: id,
  }),
  approval_page: s.refine(
    s.object({
      kind: s.enumeration("approval_page"),
      workflow_id: id,
      keys: s.array(id, 10),
    }),
    (value) => new Set(value.keys).size === value.keys.length,
    "Duplicate audit approval keys.",
  ),
});
const acceptance = s.object({
  kind: s.enumeration("approval_accepted"),
  workflow_id: id,
  key: id,
  activation_id: id,
  revision: s.decimal,
  decision_id: id,
  decision: s.enumeration("approve", "reject"),
});
const responseRead = s.refine(
  s.object({
    kind: s.enumeration("sensitive_response"),
    read: s.enumeration(
      "task_inspect",
      "task_result",
      "workflow_result",
      "workflow_input",
      "local_step_inspect",
      "approval_inspect",
      "effect_inspect",
      "admission_inspect",
      "task_admission_inspect",
      "resource_sources",
      "queue_holds",
      "approval_list",
      "approval_decision",
    ),
    resource,
  }),
  (value) =>
    ({
      task_inspect: "task",
      task_result: "task",
      workflow_result: "workflow",
      workflow_input: "workflow",
      local_step_inspect: "local_step",
      approval_inspect: "approval",
      effect_inspect: "effect",
      admission_inspect: "admission",
      task_admission_inspect: "task_admission",
      resource_sources: "resource",
      queue_holds: "queue",
      approval_list: "approval_page",
      approval_decision: "approval",
    })[value.read] === value.resource.kind,
  "Audit read and resource disagree.",
);
export const auditEvent = s.refine(
  s.object({
    event_id: id,
    recorded_at: s.timestamp,
    audience,
    actor,
    operation: s.variant({
      queue_policy_accepted: s.object({
        kind: s.enumeration("queue_policy_accepted"),
        queue: id,
        request_id: id,
        revision: s.decimal,
        policy_digest: s.string,
        reason: s.string,
      }),
      queue_hold_released: s.object({
        kind: s.enumeration("queue_hold_released"),
        queue: id,
        task_id: id,
        attempt_id: id,
        request_id: id,
        revision: s.decimal,
        reason: s.string,
      }),
      resource_policy_accepted: s.object({
        kind: s.enumeration("resource_policy_accepted"),
        name: id,
        request_id: id,
        revision: s.decimal,
        policy_digest: s.string,
        reason: s.string,
      }),
      task_admission_released: s.object({
        kind: s.enumeration("task_admission_released"),
        occurrence: taskAdmissionOccurrence,
        source_id: id,
        request_id: id,
        revision: s.decimal,
        reason: s.string,
      }),
      admission_released: s.object({
        kind: s.enumeration("admission_released"),
        occurrence: admissionOccurrence,
        source_id: id,
        request_id: id,
        revision: s.decimal,
        reason: s.string,
      }),
      effect_resolved: s.refine(
        s.object({
          kind: s.enumeration("effect_resolved"),
          occurrence: effectOccurrence,
          resolution_id: id,
          accepted_revision: s.decimal,
          basis_revision: s.decimal,
          supersedes_resolution_id: s.nullable(id),
          assessment_kind: s.enumeration(
            "record_success",
            "record_failure",
            "close_unknown",
          ),
        }),
        (value) =>
          BigInt(value.basis_revision) > 0n &&
          BigInt(value.basis_revision) + 1n ===
            BigInt(value.accepted_revision) &&
          value.supersedes_resolution_id !== value.resolution_id,
        "Invalid effect acceptance audit binding.",
      ),
      approval_accepted: acceptance,
      sensitive_response: responseRead,
    }),
  }),
  (value) => new TextEncoder().encode(stringifyUserJson(value)).length <= 8192,
  "Audit event exceeds its limit.",
);
export type AuditEvent = s.Decoded<typeof auditEvent>;
const page = s.object({
  items: s.array(auditEvent, 100),
  next_cursor: s.nullable(
    s.refine(
      s.string,
      (value) =>
        value.length > 0 && new TextEncoder().encode(value).length <= 8192,
      "Invalid audit cursor.",
    ),
  ),
});

export function acceptanceQuery(approval: Approval) {
  if (!approval.decision)
    throw new ContractError(
      "An accepted decision is required for attribution.",
    );
  return {
    filter: {
      kind: "approval_acceptance" as const,
      workflow_id: approval.workflow_id,
      key: approval.key,
      activation_id: approval.activation_id,
      revision: approval.revision,
      decision_id: approval.decision.decision_id,
    },
    cursor: null,
    limit: 2,
  };
}
export function acceptanceFor(
  approval: Approval,
  instanceId: string,
  expectedScope?: { tenant_id: string; namespace: string },
) {
  const query = acceptanceQuery(approval);
  return (raw: unknown): AuditEvent | null => {
    const result = page(raw);
    if (result.items.length > 1 || result.next_cursor !== null)
      throw new ContractError("Ambiguous approval attribution.");
    const event = result.items[0];
    if (!event) return null;
    const operation = event.operation;
    if (
      event.audience.instance_id !== instanceId ||
      (expectedScope &&
        (event.audience.scope.tenant_id !== expectedScope.tenant_id ||
          event.audience.scope.namespace !== expectedScope.namespace)) ||
      operation.kind !== "approval_accepted" ||
      operation.workflow_id !== query.filter.workflow_id ||
      operation.key !== query.filter.key ||
      operation.activation_id !== query.filter.activation_id ||
      operation.revision !== query.filter.revision ||
      operation.decision_id !== query.filter.decision_id ||
      operation.decision !== approval.decision!.decision ||
      event.recorded_at !== approval.decision!.decided_at
    )
      throw new ContractError(
        "The audit event does not match this accepted decision.",
      );
    return event;
  };
}

export function effectAcceptanceQuery(eventId: string) {
  return {
    filter: { kind: "event" as const, event_id: id(eventId) },
    cursor: null,
    limit: 2,
  };
}
export function effectAcceptanceFor(
  receipt: import("./effects").EffectResolutionReceipt,
  instanceId: string,
) {
  return (raw: unknown): AuditEvent | null => {
    const result = page(raw);
    if (result.items.length > 1 || result.next_cursor !== null)
      throw new ContractError("Ambiguous effect assessment attribution.");
    const event = result.items[0];
    if (!event) return null;
    const operation = event.operation;
    const occurrence = receipt.occurrence;
    if (
      event.event_id !== receipt.acceptance_event_id ||
      event.audience.instance_id !== instanceId ||
      event.audience.scope.tenant_id !== occurrence.scope.tenant_id ||
      event.audience.scope.namespace !== occurrence.scope.namespace ||
      event.recorded_at !== receipt.accepted_at ||
      operation.kind !== "effect_resolved" ||
      operation.occurrence.scope.tenant_id !== occurrence.scope.tenant_id ||
      operation.occurrence.scope.namespace !== occurrence.scope.namespace ||
      operation.occurrence.workflow_id !== occurrence.workflow_id ||
      operation.occurrence.activation_id !== occurrence.activation_id ||
      operation.occurrence.approval_key !== occurrence.approval_key ||
      operation.resolution_id !== receipt.resolution_id ||
      operation.accepted_revision !== receipt.accepted_revision ||
      operation.basis_revision !== receipt.basis_revision ||
      operation.supersedes_resolution_id !== receipt.supersedes_resolution_id ||
      operation.assessment_kind !== receipt.assessment_kind
    )
      throw new ContractError(
        "The audit event does not match this effect assessment.",
      );
    return event;
  };
}
