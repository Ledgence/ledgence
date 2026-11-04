// SPDX-License-Identifier: MIT
import * as s from "./schema";
import { effectOccurrence } from "./effect-occurrence";
import { queueName, type QueueScope } from "./queue-policies";
import { sequence } from "./resource-sharing";

const maximum = s.integer(0, 1000000);
export const budgetPolicy = s.refine(
  s.withOptional(
    {},
    {
      max_calls: maximum,
      max_delegations: maximum,
      deadline_at_ms: s.timestamp,
    },
  ),
  (v) => Object.keys(v).length > 0,
  "A run budget requires at least one limit.",
);
export type BudgetPolicy = s.Decoded<typeof budgetPolicy>;
export const budgetDenial = s.enumeration(
  "calls_exhausted",
  "delegations_exhausted",
  "deadline_exceeded",
  "cancelled",
);
export const budgetUsage = s.variant({
  unknown: s.object({ kind: s.enumeration("unknown") }),
  reported: usage("reported"),
  estimated: usage("estimated"),
});
function usage<K extends "reported" | "estimated">(kind: K) {
  const count = s.integer(0, Number.MAX_SAFE_INTEGER);
  return s.refine(
    s.withOptional(
      { kind: s.enumeration(kind) },
      { input_tokens: count, output_tokens: count },
    ),
    (v) => v.input_tokens !== undefined || v.output_tokens !== undefined,
    "Usage requires at least one token count.",
  );
}
export const budgetSubject = s.variant({
  task: s.object({ kind: s.enumeration("task"), task_id: queueName }),
  workflow: s.object({
    kind: s.enumeration("workflow"),
    workflow_id: queueName,
  }),
});
export type BudgetSubject = s.Decoded<typeof budgetSubject>;
export function subjectId(v: BudgetSubject) {
  return v.kind === "task" ? v.task_id : v.workflow_id;
}
const scope = s.object({ tenant_id: queueName, namespace: queueName });
const sameScope = (a: QueueScope, b: QueueScope) =>
  a.tenant_id === b.tenant_id && a.namespace === b.namespace;
const sameSubject = (a: BudgetSubject, b: BudgetSubject) =>
  a.kind === b.kind && subjectId(a) === subjectId(b);
const validRoot = (v: BudgetSubject, root: BudgetSubject) =>
  root.kind === "workflow" || sameSubject(v, root);
export function snapshotFor(expected: QueueScope, subject: BudgetSubject) {
  return s.refine(
    s.object({
      scope,
      subject: budgetSubject,
      root: budgetSubject,
      policy: budgetPolicy,
      calls: s.decimal,
      delegations: s.decimal,
      observed_at: s.timestamp,
    }),
    (v) =>
      sameScope(v.scope, expected) &&
      sameSubject(v.subject, subject) &&
      validRoot(v.subject, v.root) &&
      (v.policy.max_calls === undefined ||
        BigInt(v.calls) <= BigInt(v.policy.max_calls)) &&
      (v.policy.max_delegations === undefined ||
        BigInt(v.delegations) <= BigInt(v.policy.max_delegations)),
    "Budget snapshot disagrees with its execution or limits.",
  );
}
const entry = s.refine(
  s.withOptional(
    {
      scope,
      root: budgetSubject,
      sequence,
      subject: budgetSubject,
      action: s.enumeration("call", "delegation"),
      outcome: s.enumeration("authorized", "denied"),
      reason: s.nullable(budgetDenial),
      source_id: queueName,
      usage: budgetUsage,
      recorded_at: s.timestamp,
    },
    { approval: effectOccurrence },
  ),
  (v) =>
    validRoot(v.subject, v.root) &&
    (v.approval === undefined ||
      (v.action === "call" &&
        v.subject.kind === "workflow" &&
        v.subject.workflow_id === v.approval.workflow_id &&
        sameScope(v.scope, v.approval.scope))) &&
    (v.outcome === "denied") === (v.reason !== null) &&
    (v.usage.kind === "unknown" ||
      (v.action === "call" && v.outcome === "authorized")) &&
    (v.reason !== "calls_exhausted" || v.action === "call") &&
    (v.reason !== "delegations_exhausted" || v.action === "delegation"),
  "Inconsistent run budget entry.",
);
export function entriesFor(
  expected: QueueScope,
  subject: BudgetSubject,
  before: string | null,
  limit: number,
) {
  return s.refine(
    s.object({
      scope,
      subject: budgetSubject,
      root: budgetSubject,
      items: s.array(entry, limit),
      next: s.nullable(sequence),
    }),
    (p) =>
      sameScope(p.scope, expected) &&
      sameSubject(p.subject, subject) &&
      validRoot(p.subject, p.root) &&
      p.items.every((row, i) => {
        const previous = p.items[i - 1]?.sequence ?? before;
        return (
          sameSubject(row.root, p.root) &&
          sameScope(row.scope, expected) &&
          (previous === null || BigInt(row.sequence) < BigInt(previous))
        );
      }) &&
      (p.next === null || p.next === p.items.at(-1)?.sequence),
    "Invalid scope, root or sequence in budget ledger.",
  );
}
