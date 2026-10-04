// SPDX-License-Identifier: MIT
import * as s from "./schema";
import { isLosslessNumber } from "lossless-json";
import { ContractError } from "./codecs";
import { stringifyUserJson } from "./json";

function bytes(value: unknown): number {
  return new TextEncoder().encode(stringifyUserJson(value)).length;
}
const identifier = s.refine(
  s.id,
  (value) => new TextEncoder().encode(value).length <= 128,
  "Approval identifier exceeds 128 bytes.",
);
const reason = s.refine(
  s.string,
  (value) => new TextEncoder().encode(value).length <= 4096,
  "Approval reason exceeds 4096 bytes.",
);

const argumentsObject = s.refine(
  s.payload,
  (value) =>
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    !isLosslessNumber(value) &&
    bytes(value) <= 32 * 1024,
  "Approval arguments must be a JSON object.",
);
export const approvalAction = s.refine(
  s.object({
    name: s.id,
    version: identifier,
    arguments: argumentsObject,
  }),
  (value) => bytes(value) <= 32 * 1024,
  "Approval action exceeds the response limit.",
);
const decision = s.object({
  decision_id: identifier,
  decision: s.enumeration("approve", "reject"),
  reviewer: identifier,
  reason: s.nullable(reason),
  decided_at: s.timestamp,
});
export const approval = s.refine(
  s.object({
    workflow_id: identifier,
    key: identifier,
    activation_id: identifier,
    revision: s.decimal,
    action: approvalAction,
    proposed_arguments: s.nullable(argumentsObject),
    created_at: s.timestamp,
    deadline: s.timestamp,
    status: s.enumeration(
      "pending",
      "approved",
      "rejected",
      "expired",
      "cancelled",
    ),
    decision: s.nullable(decision),
    resumed_activation_id: s.nullable(identifier),
  }),
  (value) =>
    bytes(value) <= 96 * 1024 &&
    (value.status === "approved"
      ? value.decision?.decision === "approve"
      : value.status === "rejected"
        ? value.decision?.decision === "reject"
        : value.decision === null) &&
    (value.resumed_activation_id === null ||
      (!["pending", "cancelled"].includes(value.status) &&
        value.resumed_activation_id !== value.activation_id)) &&
    value.deadline >= value.created_at &&
    value.deadline - value.created_at <= 31_536_000_000 &&
    (value.decision === null ||
      (value.decision.decided_at >= value.created_at &&
        value.decision.decided_at < value.deadline)),
  "Inconsistent approval decision.",
);
export type Approval = s.Decoded<typeof approval>;
export const approvalPage = s.object({
  items: s.array(approval, 10),
  next_cursor: s.nullable(identifier),
});
function compareKeys(left: string, right: string): number {
  const a = new TextEncoder().encode(left);
  const b = new TextEncoder().encode(right);
  for (let index = 0; index < Math.min(a.length, b.length); index++) {
    const difference = a[index]! - b[index]!;
    if (difference) return difference;
  }
  return a.length - b.length;
}
export function approvalPageForWorkflow(
  workflowId: string,
  afterKey: string | null,
) {
  return (value: unknown) => {
    const page = approvalPage(value);
    if (
      page.items.some(
        (item, index) =>
          item.workflow_id !== workflowId ||
          (afterKey !== null && compareKeys(afterKey, item.key) >= 0) ||
          (index > 0 && compareKeys(page.items[index - 1]!.key, item.key) >= 0),
      ) ||
      (page.next_cursor !== null && page.next_cursor !== page.items.at(-1)?.key)
    )
      throw new ContractError(
        "The approval page does not match this workflow or traversal.",
      );
    return page;
  };
}
export const approvalReceipt = s.object({
  approval,
  already_accepted: s.boolean,
});
export type ApprovalDecision = {
  workflow_id: string;
  key: string;
  activation_id: string;
  revision: string;
  action: Approval["action"];
  decision_id: string;
  decision: "approve" | "reject";
  reviewer: string;
  reason: string | null;
};

// JSON property order is not part of an action's identity. Number tokens are.
function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (
    value !== null &&
    typeof value === "object" &&
    Object.getPrototypeOf(value) === Object.prototype
  )
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, canonical(Reflect.get(value, key))]),
    );
  return value;
}
export function sameAction(left: unknown, right: unknown): boolean {
  return (
    stringifyUserJson(canonical(left)) === stringifyUserJson(canonical(right))
  );
}
export function decisionReceipt(expected: ApprovalDecision) {
  return (value: unknown) => {
    const result = approvalReceipt(value);
    const current = result.approval;
    if (
      current.workflow_id !== expected.workflow_id ||
      current.key !== expected.key ||
      current.activation_id !== expected.activation_id ||
      current.revision !== expected.revision ||
      !sameAction(current.action, expected.action) ||
      current.decision?.decision_id !== expected.decision_id ||
      current.decision.decision !== expected.decision ||
      current.decision.reviewer !== expected.reviewer ||
      current.decision.reason !== expected.reason
    )
      throw new ContractError(
        "The approval receipt does not match this exact decision.",
      );
    return result;
  };
}
