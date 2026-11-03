// SPDX-License-Identifier: MIT
import * as s from "./schema";
import { ContractError } from "./codecs";
import { descriptor } from "./resources";
import { stringifyUserJson } from "./json";

const bytes = (value: unknown) =>
  new TextEncoder().encode(stringifyUserJson(value)).length;
const text = (limit: number) =>
  s.refine(
    s.string,
    (value) =>
      value.length > 0 &&
      new TextEncoder().encode(value).length <= limit &&
      [...value].every((character) => {
        const code = character.codePointAt(0)!;
        return (
          code >= 32 &&
          !(code >= 127 && code <= 159) &&
          !(code >= 0xfdd0 && code <= 0xfdef) &&
          (code & 0xfffe) !== 0xfffe
        );
      }),
    "Invalid provider identifier.",
  );
const id = text(128);
export const providerPositive = s.refine(
  s.decimal,
  (v) => BigInt(v) > 0n,
  "Provider revision must be positive.",
);
const digest = s.refine(
  s.string,
  (v) => /^sha256:[0-9a-f]{64}$/.test(v),
  "Invalid provider digest.",
);
function dimensions(value: unknown): Record<string, string> {
  if (!s.isJsonObject(value) || Object.keys(value).length > 8)
    throw new ContractError("Invalid provider dimensions.");
  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [id(key), text(256)(item)]),
  );
}
export const providerTarget = s.object({
  provider: id,
  environment: text(64),
  account: text(256),
  api_variant: id,
  operation: id,
  endpoint_identity: text(512),
  dimensions,
});
const horizon = s.variant({
  unknown: s.object({ kind: s.enumeration("unknown") }),
  resource_lifetime: s.object({ kind: s.enumeration("resource_lifetime") }),
  minimum_duration: s.object({
    kind: s.enumeration("minimum_duration"),
    duration_ms: s.integer(1, 31_536_000_000),
  }),
});
const deduplication = s.object({
  domain: s.object({ provider: id, namespace: id, dimensions }),
  key: text(256),
  horizon,
});
const contract = s.refine(
  s.object({
    adapter_id: id,
    version: id,
    digest,
    completion_criterion: s.refine(
      text(1024),
      (v) => v.trim().length > 0,
      "Invalid completion criterion.",
    ),
    dispatch_callable: text(512),
    reconcile_callable: s.nullable(text(512)),
    lookup: s.enumeration("by_binding", "by_reference", "unavailable"),
  }),
  (v) =>
    (v.lookup === "unavailable") === (v.reconcile_callable === null) &&
    v.dispatch_callable !== v.reconcile_callable,
  "Inconsistent provider callback registration.",
);
export const providerPolicy = s.refine(
  s.object({
    queue: id,
    initial_delay_ms: s.integer(1000, 86_400_000),
    interval_ms: s.integer(1000, 86_400_000),
    maximum_interval_ms: s.integer(1000, 86_400_000),
    max_checks: s.integer(1, 256),
    max_elapsed_ms: s.integer(60_000, 604_800_000),
    check_timeout_ms: s.integer(1000, 30_000),
  }),
  (v) =>
    v.maximum_interval_ms >= v.interval_ms &&
    v.initial_delay_ms < v.max_elapsed_ms,
  "Invalid provider recovery policy.",
);
export const providerBinding = s.refine(
  s.object({
    v: s.integer(1, 1),
    business: s.object({ namespace: id, key: text(256) }),
    contract,
    target: providerTarget,
    deduplication: s.nullable(deduplication),
    credential_reference: text(256),
    reconciliation: providerPolicy,
    program: s.refine(
      descriptor,
      (v) => /^sha256:[0-9a-f]{64}$/.test(v.digest) && BigInt(v.size) > 0n,
      "Invalid provider program descriptor.",
    ),
  }),
  (v) =>
    bytes(v) <= 16 * 1024 &&
    (!v.deduplication || v.deduplication.domain.provider === v.target.provider),
  "Provider binding exceeds its bounds or changes its target.",
);
export type ProviderBinding = s.Decoded<typeof providerBinding>;
export const providerKnowledgeKinds = [
  "unknown",
  "pending",
  "requires_action",
  "succeeded",
  "failed",
  "cancelled",
] as const;
const boundedReason = s.refine(
  s.string,
  (v) => v.length > 0 && new TextEncoder().encode(v).length <= 4096,
  "Invalid provider reason.",
);
const failure = s.object({
  kind: id,
  message: s.refine(
    s.string,
    (v) => new TextEncoder().encode(v).length <= 4096,
    "Invalid provider failure message.",
  ),
});
const result = s.refine(
  s.payload,
  (v) => bytes(v) <= 128 * 1024,
  "Provider result exceeds 128 KiB.",
);
export const providerKnowledge = s.variant({
  unknown: s.object({ kind: s.enumeration("unknown"), reason: boundedReason }),
  pending: s.object({ kind: s.enumeration("pending") }),
  requires_action: s.object({
    kind: s.enumeration("requires_action"),
    action_kind: id,
  }),
  succeeded: s.object({ kind: s.enumeration("succeeded"), result }),
  failed: s.object({ kind: s.enumeration("failed"), failure }),
  cancelled: s.object({
    kind: s.enumeration("cancelled"),
    reason: boundedReason,
  }),
});
export const providerReport = s.refine(
  s.object({
    reported_target: providerTarget,
    provider_reference: s.nullable(text(1024)),
    provider_request_id: s.nullable(text(1024)),
    knowledge: providerKnowledge,
  }),
  (v) => bytes(v) <= 144 * 1024,
  "Provider report exceeds 144 KiB.",
);
export const providerReportReference = s.refine(
  s.object({
    report_id: id,
    source_id: id,
    sequence: providerPositive,
    recorded_revision: providerPositive,
    recorded_at: s.timestamp,
    knowledge: s.enumeration(...providerKnowledgeKinds),
  }),
  (v) => BigInt(v.sequence) < BigInt(v.recorded_revision),
  "Invalid provider report revision.",
);
export const providerStopReason = s.enumeration(
  "provider_terminal",
  "operator_assessment",
  "workflow_cancelled",
  "workflow_failed",
  "action_required",
  "lookup_unavailable",
  "disputed",
  "budget_exhausted",
  "deadline",
  "recovery_failed",
);
export const providerProbeFailure = s.enumeration(
  "incompatible_worker",
  "contract_mismatch",
  "context_invalid",
  "result_invalid",
  "invocation_timeout",
  "invocation_failed",
  "preparation_failed",
  "worker_lost",
  "cancelled",
  "missing_report",
);
export const providerReconciliation = s.refine(
  s.object({
    queue: id,
    checks_used: s.integer(0, 256),
    max_checks: s.integer(1, 256),
    generation: s.decimal,
    active_probe_id: s.nullable(id),
    active_task_id: s.nullable(id),
    first_due_at: s.timestamp,
    next_due_at: s.nullable(s.timestamp),
    deadline_at: s.timestamp,
    stopped_reason: s.nullable(providerStopReason),
  }),
  (v) =>
    v.checks_used <= v.max_checks &&
    BigInt(v.generation) === BigInt(v.checks_used) &&
    (v.active_probe_id === null) === (v.active_task_id === null) &&
    (v.active_task_id === null || BigInt(v.generation) > 0n) &&
    v.first_due_at < v.deadline_at &&
    (v.next_due_at === null || v.next_due_at <= v.deadline_at) &&
    (v.stopped_reason === null || v.next_due_at === null),
  "Inconsistent provider recovery schedule.",
);
export const providerSummary = s.refine(
  s.object({
    binding_digest: digest,
    knowledge: s.enumeration(...providerKnowledgeKinds, "disputed"),
    selected_report: s.nullable(providerReportReference),
    conflicting_reports: s.array(providerReportReference, 2),
    reconciliation: providerReconciliation,
    handling: s.enumeration(
      "unresolved",
      "provider_terminal",
      "operator_closed",
    ),
    candidate_revision: s.nullable(providerPositive),
  }),
  (v) =>
    (v.knowledge === "disputed"
      ? v.selected_report === null &&
        v.conflicting_reports.length === 2 &&
        BigInt(v.conflicting_reports[0]!.sequence) <
          BigInt(v.conflicting_reports[1]!.sequence)
      : v.conflicting_reports.length === 0 &&
        (v.selected_report
          ? v.selected_report.knowledge === v.knowledge
          : v.knowledge === "unknown")) &&
    (v.handling !== "provider_terminal" ||
      ["succeeded", "failed", "cancelled", "disputed"].includes(v.knowledge)) &&
    (v.handling !== "unresolved" ||
      !["succeeded", "failed", "cancelled"].includes(v.knowledge)),
  "Inconsistent provider knowledge summary.",
);
export type ProviderSummary = s.Decoded<typeof providerSummary>;
export const providerFilters = [
  "all",
  "pending",
  "needs_review",
  "succeeded",
  "failed",
  "cancelled",
  "disputed",
] as const;
export type ProviderFilter = (typeof providerFilters)[number];
export function providerNeedsReview(
  provider: ProviderSummary,
  operatorConflict: boolean,
) {
  return (
    operatorConflict ||
    (provider.handling !== "operator_closed" &&
      provider.knowledge === "disputed") ||
    (provider.handling === "unresolved" &&
      [
        "action_required",
        "lookup_unavailable",
        "budget_exhausted",
        "deadline",
        "recovery_failed",
      ].includes(provider.reconciliation.stopped_reason ?? ""))
  );
}
export function matchesProviderFilter(
  provider: ProviderSummary | undefined,
  filter: ProviderFilter,
  conflict: boolean,
) {
  if (!provider) return false;
  if (filter === "all") return true;
  if (filter === "pending")
    return (
      ["unknown", "pending"].includes(provider.knowledge) &&
      provider.handling === "unresolved" &&
      provider.reconciliation.stopped_reason === null &&
      !providerNeedsReview(provider, conflict)
    );
  if (filter === "needs_review") return providerNeedsReview(provider, conflict);
  if (filter === "disputed")
    return provider.knowledge === "disputed" || conflict;
  return provider.knowledge === filter;
}
