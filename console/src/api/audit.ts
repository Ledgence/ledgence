// SPDX-License-Identifier: MIT
import * as s from "./schema";
import { stringifyUserJson } from "./json";
import { ContractError } from "./codecs";
import type { Approval } from "./approvals";

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
