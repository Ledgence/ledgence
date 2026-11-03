// SPDX-License-Identifier: MIT
import { expect, it } from "vitest";
import {
  approval,
  decodeApprovalDecision,
  decisionReceipt,
} from "../../src/api/approvals";
import {
  effectDetail,
  effectInboxPage,
  type EffectInboxQuery,
} from "../../src/api/effects";
import {
  providerBinding,
  providerReport,
  providerNeedsReview,
  providerSummary,
  matchesProviderFilter,
} from "../../src/api/providers";
import { effectAddress, effectScope, effectWire } from "../effect-fixture";
import { providerBindingFixture, providerFixture } from "../provider-fixture";

const inspect = { ...effectAddress, cursor: null, limit: 4 };
it("retains the reviewed provider identity and rejects a stripped or changed approval receipt", () => {
  const value = providerFixture().binding.approval;
  const prepared = {
    workflow_id: value.workflow_id,
    key: value.key,
    activation_id: value.activation_id,
    revision: value.revision,
    action: value.action,
    protection: value.protection,
    provider_binding: value.provider_binding,
    ...value.decision!,
  };
  delete (prepared as Partial<typeof prepared>).decided_at;
  const command = decodeApprovalDecision(prepared);
  expect(command.provider_binding).toEqual(value.provider_binding);
  expect(
    decisionReceipt(command)({ approval: value, already_accepted: false })
      .approval.provider_binding,
  ).toEqual(value.provider_binding);
  for (const mutate of [
    (v: typeof value) => {
      delete v.provider_binding;
    },
    (v: typeof value) => {
      v.provider_binding!.target.account = "other";
    },
    (v: typeof value) => {
      v.provider_binding!.deduplication!.key = "different";
    },
  ]) {
    const changed = structuredClone(value);
    mutate(changed);
    expect(() =>
      decisionReceipt(command)({ approval: changed, already_accepted: false }),
    ).toThrow();
  }
  expect(() => approval({ ...value, provider_binding: null })).toThrow();
  expect(() => approval({ ...value, protection: "external_effect" })).toThrow();
  expect(() =>
    decodeApprovalDecision({ ...prepared, provider_binding: null }),
  ).toThrow();
});

it("preserves provider null success and rejects malformed bindings or conflicting targets", () => {
  const binding = providerBindingFixture();
  expect(providerBinding(binding)).toEqual(binding);
  for (const mutate of [
    (v: typeof binding) => {
      v.reconciliation.max_checks = 257;
    },
    (v: typeof binding) => {
      v.reconciliation.check_timeout_ms = 30_001;
    },
    (v: typeof binding) => {
      v.reconciliation.maximum_interval_ms = 1000;
    },
    (v: typeof binding) => {
      v.contract.reconcile_callable = null;
    },
    (v: typeof binding) => {
      v.deduplication!.domain.provider = "other";
    },
    (v: typeof binding) => {
      v.program.size = "0";
    },
  ]) {
    const changed = structuredClone(binding);
    mutate(changed);
    expect(() => providerBinding(changed)).toThrow();
  }
  const report = {
    reported_target: binding.target,
    provider_reference: null,
    provider_request_id: null,
    knowledge: { kind: "succeeded", result: null },
  };
  expect(providerReport(report).knowledge).toEqual({
    kind: "succeeded",
    result: null,
  });
  expect(() =>
    providerReport({ ...report, knowledge: { kind: "succeeded" } }),
  ).toThrow();
});

it("validates provider header, report provenance and immutable timeline agreement", () => {
  const detail = providerFixture();
  expect(
    effectDetail(inspect, effectScope)(effectWire(detail)).provider
      ?.current_report,
  ).toEqual(detail.provider?.current_report);
  for (const mutate of [
    (v: typeof detail) => {
      v.provider!.current_report!.report.provider_request_id = "different";
    },
    (v: typeof detail) => {
      v.summary.provider!.selected_report!.recorded_revision = "4";
    },
    (v: typeof detail) => {
      delete v.summary.provider;
    },
    (v: typeof detail) => {
      delete v.provider;
    },
    (v: typeof detail) => {
      v.provider!.current_report = null;
    },
    (v: typeof detail) => {
      v.binding.approval.provider_binding!.target.account = "another";
    },
    (v: typeof detail) => {
      v.summary.provider!.reconciliation.active_task_id = "unpaired";
    },
    (v: typeof detail) => {
      v.summary.journal_state = "confirmed";
    },
  ]) {
    // JSON copies intentionally break shared fixture object references.
    const changed = JSON.parse(JSON.stringify(detail)) as typeof detail;
    mutate(changed);
    expect(() =>
      effectDetail(inspect, effectScope)(effectWire(changed)),
    ).toThrow();
  }
  const entry = detail.entries[1]!;
  if (entry.kind !== "provider_report")
    throw new Error("fixture report missing");
  entry.ingress = "live_probe";
  expect(() =>
    effectDetail(inspect, effectScope)(effectWire(detail)),
  ).toThrow();
});

function cursor(providerFilter: string) {
  const value = JSON.stringify({
    version: 2,
    scope: effectScope,
    filter: "all",
    provider_filter: providerFilter,
    workflow_id: null,
    position: { updated_at: 30, ...effectAddress },
  });
  return Array.from(new TextEncoder().encode(value), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
}
it("binds provider filters and cursor version without changing legacy cursors", () => {
  const detail = providerFixture();
  const query: EffectInboxQuery = {
    filter: "all",
    provider_filter: "pending",
    workflow_id: null,
    cursor: null,
    limit: 4,
  };
  const page = { items: [detail.summary], next_cursor: cursor("pending") };
  expect(
    effectInboxPage(query, effectScope)(effectWire(page)).items,
  ).toHaveLength(1);
  expect(() =>
    effectInboxPage(
      query,
      effectScope,
    )({ ...page, next_cursor: cursor("failed") }),
  ).toThrow();
  expect(() =>
    effectInboxPage({ ...query, filter: "confirmed" }, effectScope),
  ).toThrow();
  expect(() =>
    effectInboxPage({ ...query, provider_filter: "failed" }, effectScope)(page),
  ).toThrow();
  const legacy = {
    filter: "all" as const,
    workflow_id: null,
    cursor: null,
    limit: 4,
  };
  expect(
    effectInboxPage(
      legacy,
      effectScope,
    )(effectWire({ items: [detail.summary], next_cursor: null })).items,
  ).toHaveLength(1);
  expect(() => effectInboxPage(legacy, effectScope)(page)).toThrow();
});

it("distinguishes pending knowledge, needs-review handling and terminal provider evidence", () => {
  const provider = providerFixture().summary.provider!;
  expect(providerNeedsReview(provider, false)).toBe(false);
  provider.reconciliation.stopped_reason = "budget_exhausted";
  provider.reconciliation.next_due_at = null;
  expect(providerNeedsReview(provider, false)).toBe(true);
  provider.handling = "operator_closed";
  expect(providerNeedsReview(provider, false)).toBe(false);
  expect(providerNeedsReview(provider, true)).toBe(true);
});

it("retains closed handling when late terminal facts conflict", () => {
  const provider = providerFixture().summary.provider!;
  const first = {
    ...provider.selected_report!,
    knowledge: "succeeded" as const,
  };
  const second = {
    ...first,
    sequence: "3",
    recorded_revision: "4",
    report_id: "report-2",
    knowledge: "failed" as const,
  };
  provider.knowledge = "disputed";
  provider.selected_report = null;
  provider.conflicting_reports = [first, second];
  provider.handling = "provider_terminal";
  expect(providerSummary(provider).handling).toBe("provider_terminal");
  expect(providerNeedsReview(provider, false)).toBe(true);
  expect(
    providerSummary({ ...provider, handling: "unresolved" }).handling,
  ).toBe("unresolved");
});

it("excludes stopped recovery from pending and binds operator closure to its assessment", () => {
  const detail = providerFixture();
  const provider = detail.summary.provider!;
  provider.reconciliation.stopped_reason = "workflow_cancelled";
  provider.reconciliation.next_due_at = null;
  expect(matchesProviderFilter(provider, "pending", false)).toBe(false);
  expect(matchesProviderFilter(provider, "all", false)).toBe(true);
  provider.handling = "operator_closed";
  expect(() =>
    effectDetail(inspect, effectScope)(effectWire(detail)),
  ).toThrow();
});

it("separates historical dispute filtering from current operator review needs", () => {
  const provider = providerFixture().summary.provider!;
  provider.knowledge = "disputed";
  provider.handling = "operator_closed";
  expect(providerNeedsReview(provider, false)).toBe(false);
  expect(matchesProviderFilter(provider, "needs_review", false)).toBe(false);
  expect(matchesProviderFilter(provider, "disputed", false)).toBe(true);
  expect(providerNeedsReview(provider, true)).toBe(true);
});
