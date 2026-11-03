import savedConsoleCommand from "../../../sdk/python-client/tests/fixtures/console-effect-resolution.json";
import {
  effectAcceptanceFor,
  effectAcceptanceQuery,
} from "../../src/api/audit";
// SPDX-License-Identifier: MIT
import { expect, it } from "vitest";
import {
  decodeEffectResolutionCommand,
  effectDetail,
  effectInboxPage,
  effectResolutionReceipt,
  effectDetailPath,
  saveEffectResolutionCommand,
} from "../../src/api/effects";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import {
  assessedEffectFixture,
  effectAddress,
  effectScope,
  effectFixture,
  effectWire,
} from "../effect-fixture";
import { requiredCapabilities } from "../../src/api/access-policy";
import { decodeAccess } from "../../src/api/access";
import { AccessController, type Access } from "../../src/app/access-state";

const inspect = { ...effectAddress, cursor: null, limit: 4 };
it("validates empty retained evidence and rejects foreign occurrence/action bindings", () => {
  expect(
    effectDetail(inspect, effectScope)(effectWire(effectFixture())).summary
      .effect_revision,
  ).toBe("1");
  for (const mutate of [
    (v: ReturnType<typeof effectFixture>) => {
      v.binding.occurrence.activation_id = "foreign";
    },
    (v: ReturnType<typeof effectFixture>) => {
      v.summary.callable = "different";
    },
    (v: ReturnType<typeof effectFixture>) => {
      v.binding.approval.resumed_activation_id = "foreign";
    },
    (v: ReturnType<typeof effectFixture>) => {
      v.summary.occurrence.scope.tenant_id = "foreign";
    },
  ]) {
    const value = effectFixture();
    mutate(value);
    expect(() =>
      effectDetail(inspect, effectScope)(effectWire(value)),
    ).toThrow();
  }
});
it("preserves exact u64 revisions and lossless application JSON in frozen commands", () => {
  const value = command();
  value.expected_effect_revision = "18446744073709551614";
  value.assessment = {
    kind: "record_success",
    result: parseUserJson(
      '{"integer":18446744073709551615,"float":1.0,"nul":"\\u0000"}',
      1000,
    ),
  };
  const decoded = decodeEffectResolutionCommand(value);
  expect(stringifyUserJson(decoded)).toContain(
    '"integer":18446744073709551615',
  );
  expect(stringifyUserJson(decoded)).toContain('"float":1.0');
  for (const revision of [
    9,
    true,
    "01",
    "1e1",
    "18446744073709551616",
    "18446744073709551615",
    "0",
  ]) {
    expect(() =>
      decodeEffectResolutionCommand({
        ...value,
        expected_effect_revision: revision,
      }),
    ).toThrow();
  }
});
it("requires explicit predecessor, reason and supporting references for success/failure", () => {
  const value = command();
  for (const bad of [
    { ...value, supersedes_resolution_id: undefined },
    { ...value, reason: " " },
    { ...value, evidence_references: [] },
    { ...value, evidence_references: Array(9).fill("receipt") },
    { ...value, assessment: { kind: "record_success" } },
    {
      ...value,
      assessment: { kind: "record_success", result: "x".repeat(128 * 1024) },
    },
  ]) {
    expect(() => decodeEffectResolutionCommand(bad)).toThrow();
  }
  expect(
    decodeEffectResolutionCommand({
      ...value,
      assessment: { kind: "close_unknown" },
      evidence_references: [],
    }).assessment.kind,
  ).toBe("close_unknown");
});
it("checks the exact accepted receipt without expecting a resumed workflow", () => {
  const value = command();
  const accepted = {
    occurrence: { scope: effectScope, ...effectAddress },
    resolution_id: value.resolution_id,
    accepted_at: 25,
    accepted_revision: "2",
    basis_revision: "1",
    supersedes_resolution_id: null,
    assessment_kind: "record_success",
    acceptance_event_id: "audit-resolution",
    already_accepted: true,
  };
  expect(effectResolutionReceipt(value, effectScope)(accepted)).toEqual(
    accepted,
  );
  for (const change of [
    { basis_revision: "2" },
    { accepted_revision: "3" },
    { resolution_id: "other" },
    { assessment_kind: "record_failure" },
    { supersedes_resolution_id: "previous" },
  ]) {
    expect(() =>
      effectResolutionReceipt(value, effectScope)({ ...accepted, ...change }),
    ).toThrow();
  }
});
it("validates whole assessment evidence and current header reference agreement", () => {
  const value = effectFixture();
  const assessment = {
    kind: "assessment" as const,
    sequence: "1",
    recorded_at: 20,
    recorded_revision: "2",
    resolution_id: "resolution-1",
    basis_revision: "1",
    supersedes_resolution_id: null,
    assessment: { kind: "close_unknown" as const },
    reason: "No independent receipt is available.",
    evidence_references: [],
    actor: {
      issuer: "local",
      subject: "operator",
      class: "operator" as const,
      credential_id: "credential-1",
    },
    acceptance_event_id: "audit-resolution",
  };
  value.entries = [assessment];
  value.current_assessment = assessment;
  value.summary.entry_count = "1";
  value.summary.effect_revision = "2";
  value.summary.operator_resolution = {
    reference: {
      resolution_id: "resolution-1",
      sequence: "1",
      accepted_at: 20,
      accepted_revision: "2",
      basis_revision: "1",
      supersedes_resolution_id: null,
      assessment_kind: "close_unknown",
      acceptance_event_id: "audit-resolution",
    },
    first_conflicting_entry_sequence: null,
  };
  expect(
    effectDetail(inspect, effectScope)(effectWire(value)).current_assessment
      ?.assessment.kind,
  ).toBe("close_unknown");
  value.summary.operator_resolution.reference.acceptance_event_id = "other";
  expect(() => effectDetail(inspect, effectScope)(effectWire(value))).toThrow();
});
it("rejects omitted evidence and accepts bounded inbox pages without assuming a full count", () => {
  const value = effectFixture();
  value.summary.entry_count = "1";
  value.summary.effect_revision = "2";
  expect(() => effectDetail(inspect, effectScope)(effectWire(value))).toThrow();
  const query = {
    filter: "unconfirmed" as const,
    workflow_id: null,
    cursor: null,
    limit: 50,
  };
  expect(
    effectInboxPage(
      query,
      effectScope,
    )(effectWire({ items: [value.summary], next_cursor: null })).items,
  ).toHaveLength(1);
  expect(() =>
    effectInboxPage(
      { ...query, filter: "confirmed" },
      effectScope,
    )(effectWire({ items: [value.summary], next_cursor: null })),
  ).toThrow();
});
it("keeps POST read permissions distinct from assessment privilege", () => {
  for (const [operation, capability] of [
    ["inbox", "metadata_read"],
    ["inspect", "sensitive_read"],
    ["resolve", "effect_resolve"],
  ]) {
    expect(
      requiredCapabilities(
        `/v1/console/workflows/approved-effects/${operation}`,
        true,
      ),
    ).toEqual([capability]);
  }
  expect(
    effectDetailPath({ ...effectAddress, approval_key: "refund/with spaces" }),
  ).toContain("approval_key=refund%2Fwith+spaces");
});
it("clears actor-owned pending commands when native capability changes", () => {
  const access: Access = {
    mode: "credentials",
    actor_key: "actor",
    issuer: "local",
    subject: "operator",
    class: "operator",
    instance_id: "instance",
    scope: effectScope,
    capabilities: ["effect_resolve", "sensitive_read"],
    expires_at_ms: Date.now() + 60_000,
    operator_audit: true,
    native_effect_resolution: true,
  };
  expect(decodeAccess(access)).toEqual(access);
  const controller = new AccessController();
  controller.accept(access, controller.begin(), "instance");
  controller.remember("resolution", {
    body: "saved",
    identity: "id",
    actor: "actor",
  });
  expect(controller.recover("resolution")).toBeDefined();
  controller.accept(
    { ...access, native_effect_resolution: false },
    controller.begin(),
    "instance",
  );
  expect(controller.recover("resolution")).toBeUndefined();
});
function command() {
  return decodeEffectResolutionCommand({
    occurrence: effectAddress,
    resolution_id: "resolution-1",
    expected_effect_revision: "1",
    supersedes_resolution_id: null,
    action: effectFixture().binding.approval.action,
    assessment: { kind: "record_success", result: null },
    reason: "Matched the provider receipt.",
    evidence_references: ["receipt:1042"],
  });
}

it("rejects contradictory current headers, stale headers and impossible sequence bounds", () => {
  for (const mutate of [
    (v: ReturnType<typeof assessedEffectFixture>) => {
      v.current_assessment!.reason = "Different reason";
    },
    (v: ReturnType<typeof assessedEffectFixture>) => {
      v.current_assessment = undefined;
      v.summary.operator_resolution = undefined;
    },
    (v: ReturnType<typeof assessedEffectFixture>) => {
      v.summary.operator_resolution!.first_conflicting_entry_sequence = "1";
    },
    (v: ReturnType<typeof assessedEffectFixture>) => {
      v.summary.created_at = 21;
    },
    (v: ReturnType<typeof assessedEffectFixture>) => {
      v.current_assessment!.sequence = "2";
      v.entries[0]!.sequence = "2";
      v.summary.operator_resolution!.reference.sequence = "2";
    },
    (v: ReturnType<typeof assessedEffectFixture>) => {
      v.entries.push({
        ...v.current_assessment!,
        sequence: "2",
        recorded_revision: "3",
        basis_revision: "2",
        resolution_id: "later",
        supersedes_resolution_id: "resolution-1",
      });
      v.summary.entry_count = "2";
      v.summary.effect_revision = "3";
    },
  ]) {
    const value = assessedEffectFixture();
    mutate(value);
    expect(() =>
      effectDetail(inspect, effectScope)(effectWire(value)),
    ).toThrow();
  }
});
it("rejects invalid platform text and overflowing acceptance revisions", () => {
  for (const text of [
    "receipt\u0085bad",
    "receipt\u0000bad",
    "receipt\ufdd0",
    "receipt\u{1ffff}",
  ]) {
    expect(() =>
      decodeEffectResolutionCommand({
        ...command(),
        evidence_references: [text],
      }),
    ).toThrow();
  }
  expect(() =>
    decodeEffectResolutionCommand({
      ...command(),
      expected_effect_revision: "18446744073709551615",
    }),
  ).toThrow();
});
it("binds inbox cursors to filters, scope and the exact last returned position", () => {
  const value = effectFixture();
  const query = {
    filter: "all" as const,
    workflow_id: null,
    cursor: null,
    limit: 50,
  };
  const shape = {
    version: 1,
    scope: effectScope,
    filter: "all",
    workflow_id: null,
    position: { updated_at: value.summary.updated_at, ...effectAddress },
  };
  const hex = (input: unknown) =>
    [...new TextEncoder().encode(JSON.stringify(input))]
      .map((x) => x.toString(16).padStart(2, "0"))
      .join("");
  const next = hex(shape);
  const page = { items: [effectWire(value.summary)], next_cursor: next };
  expect(effectInboxPage(query, effectScope)(page).next_cursor).toBe(next);
  for (const changed of [
    { ...shape, filter: "confirmed" },
    { ...shape, scope: { ...effectScope, namespace: "other" } },
    { ...shape, position: { ...shape.position, updated_at: 0 } },
  ]) {
    expect(() =>
      effectInboxPage(
        query,
        effectScope,
      )({ ...page, next_cursor: hex(changed) }),
    ).toThrow();
  }
  expect(() =>
    effectInboxPage(
      { ...query, cursor: next },
      effectScope,
    )({ ...page, next_cursor: null }),
  ).toThrow();
});
it("looks up the exact acceptance audit and rejects other actors' unrelated events", () => {
  const value = command();
  const receipt = effectResolutionReceipt(
    value,
    effectScope,
  )({
    occurrence: { scope: effectScope, ...effectAddress },
    resolution_id: value.resolution_id,
    accepted_at: 25,
    accepted_revision: "2",
    basis_revision: "1",
    supersedes_resolution_id: null,
    assessment_kind: "record_success",
    acceptance_event_id: "audit-1",
    already_accepted: true,
  });
  expect(effectAcceptanceQuery(receipt.acceptance_event_id)).toEqual({
    filter: { kind: "event", event_id: "audit-1" },
    cursor: null,
    limit: 2,
  });
  const event = {
    event_id: "audit-1",
    recorded_at: 25,
    audience: {
      authority_id: "authority",
      instance_id: "instance",
      scope: effectScope,
    },
    actor: {
      issuer: "local",
      subject: "original",
      class: "operator",
      credential_id: "credential-1",
    },
    operation: {
      kind: "effect_resolved",
      occurrence: receipt.occurrence,
      resolution_id: receipt.resolution_id,
      accepted_revision: "2",
      basis_revision: "1",
      supersedes_resolution_id: null,
      assessment_kind: "record_success",
    },
  };
  const decode = effectAcceptanceFor(receipt, "instance");
  expect(decode({ items: [event], next_cursor: null })?.actor.subject).toBe(
    "original",
  );
  expect(decode({ items: [], next_cursor: null })).toBeNull();
  for (const changed of [
    { ...event, event_id: "other" },
    { ...event, recorded_at: 26 },
    { ...event, operation: { ...event.operation, resolution_id: "other" } },
    { ...event, audience: { ...event.audience, instance_id: "other" } },
  ]) {
    expect(() => decode({ items: [changed], next_cursor: null })).toThrow();
  }
  expect(() => decode({ items: [event, event], next_cursor: null })).toThrow();
});

it("exports the exact shared CLI/Python envelope without changing application numeric values", () => {
  const wire = parseUserJson(savedConsoleCommand.body, 256 * 1024) as Record<
    string,
    unknown
  >;
  const occurrence = wire.occurrence as Record<string, unknown>;
  const { scope, ...address } = occurrence;
  const body = stringifyUserJson({
    ...wire,
    occurrence: address,
    expected_effect_revision: stringifyUserJson(wire.expected_effect_revision),
  });
  const saved = saveEffectResolutionCommand(
    body,
    scope as typeof effectScope,
    savedConsoleCommand.base_url,
  );
  expect(parseUserJson(saved, 1024 * 1024)).toEqual(savedConsoleCommand);
  expect(savedConsoleCommand.body).toContain('"float":1.0');
  for (const url of [
    savedConsoleCommand.base_url + "/",
    "https://user:pass@example.com",
    "ftp://example.com",
    "https://example.com/path",
  ]) {
    expect(() => saveEffectResolutionCommand(body, effectScope, url)).toThrow();
  }
});
it("rejects another entry kind at the current assessment sequence", () => {
  const value = assessedEffectFixture();
  value.entries[0] = {
    kind: "observation",
    sequence: "1",
    recorded_revision: "2",
    recorded_at: 20,
    observation_id: "other-entry",
    source: {
      kind: "legacy_journal",
      attempt_id: "old-attempt",
      journal_accepted_at: 20,
    },
    output: null,
  };
  expect(() => effectDetail(inspect, effectScope)(effectWire(value))).toThrow();
});
it("rejects mixed scopes in a legacy operator-trusted inbox", () => {
  const first = effectFixture(),
    second = effectFixture();
  first.summary.updated_at = 20;
  second.summary.occurrence.scope.namespace = "other";
  expect(() =>
    effectInboxPage({
      filter: "all",
      workflow_id: null,
      cursor: null,
      limit: 50,
    })(
      effectWire({ items: [first.summary, second.summary], next_cursor: null }),
    ),
  ).toThrow();
});
