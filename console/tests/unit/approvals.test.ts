// SPDX-License-Identifier: MIT
import { describe, expect, it } from "vitest";
import {
  approval,
  approvalPage,
  approvalPageForWorkflow,
  decisionReceipt,
  sameAction,
  type ApprovalDecision,
} from "../../src/api/approvals";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import { approvalFixture } from "../approval-fixture";

describe("approval contracts", () => {
  it("checks workflow and traversal using UTF-8 order for Unicode keys", () => {
    const value = approvalFixture();
    const bmp = { ...value, key: "\ue000" };
    const astral = { ...value, key: "\u{10000}" };
    const decode = approvalPageForWorkflow(value.workflow_id, null);
    expect(
      decode({ items: [bmp, astral], next_cursor: astral.key }).items,
    ).toHaveLength(2);
    expect(
      approvalPageForWorkflow(
        value.workflow_id,
        bmp.key,
      )({ items: [astral], next_cursor: null }).items,
    ).toHaveLength(1);
    for (const items of [
      [astral, bmp],
      [bmp, bmp],
      [{ ...bmp, workflow_id: "other" }],
    ])
      expect(() => decode({ items, next_cursor: null })).toThrow();
    expect(() => decode({ items: [bmp], next_cursor: astral.key })).toThrow();
    expect(() =>
      approvalPageForWorkflow(
        value.workflow_id,
        astral.key,
      )({ items: [bmp], next_cursor: null }),
    ).toThrow();
  });
  it.each(["pending", "approved", "rejected", "expired", "cancelled"] as const)(
    "keeps the server's %s state without using the browser clock",
    (status) => {
      expect(approval(approvalFixture(status)).status).toBe(status);
    },
  );
  it("retains exact action numbers and metadata revisions", () => {
    const value = approvalFixture();
    expect(value.revision).toBe("9007199254740993");
    expect(
      approval({ ...value, revision: "18446744073709551615" }).revision,
    ).toBe("18446744073709551615");
    expect(() =>
      approval({ ...value, revision: "18446744073709551616" }),
    ).toThrow();
    const encoded = stringifyUserJson(value.action.arguments);
    for (const token of ["18446744073709551615", "1.0", "-0.0"])
      expect(encoded).toContain(token);
  });
  it("rejects leaked scope, unknown fields, unsafe revisions and inconsistent audit records", () => {
    for (const patch of [
      { scope: { tenant_id: "other", namespace: "other" } },
      { revision: 1 },
      { status: "unknown" },
      { status: "approved" },
      { resumed_activation_id: "early" },
      { action: { name: "publish", version: "1", arguments: [] } },
      { proposed_arguments: [] },
      { proposed_arguments: parseUserJson("1.0", 32768) },
      {
        action: {
          name: "publish",
          version: "1",
          arguments: parseUserJson("1.0", 32768),
        },
      },
      { deadline: 0 },
    ])
      expect(() => approval({ ...approvalFixture(), ...patch })).toThrow();
    const accepted = approvalFixture("approved");
    expect(() =>
      approval({
        ...accepted,
        decision: { ...accepted.decision, decided_at: accepted.deadline },
      }),
    ).toThrow();
    expect(() =>
      approvalPage({
        items: Array.from({ length: 11 }, () => accepted),
        next_cursor: null,
      }),
    ).toThrow();
  });
  it("matches backend identifier, deadline and continuation limits", () => {
    const pending = approvalFixture();
    const approved = approvalFixture("approved");
    for (const field of ["workflow_id", "key", "activation_id"])
      expect(() => approval({ ...pending, [field]: "é".repeat(65) })).toThrow();
    for (const patch of [
      { action: { ...pending.action, name: "é".repeat(257) } },
      { action: { ...pending.action, version: "é".repeat(65) } },
      { deadline: pending.created_at + 31_536_000_001 },
    ])
      expect(() => approval({ ...pending, ...patch })).toThrow();
    expect(
      approval({ ...pending, deadline: pending.created_at + 31_536_000_000 })
        .deadline,
    ).toBe(pending.created_at + 31_536_000_000);
    for (const patch of [
      { resumed_activation_id: approved.activation_id },
      { resumed_activation_id: "é".repeat(65) },
      { decision: { ...approved.decision, decision_id: "é".repeat(65) } },
      { decision: { ...approved.decision, reviewer: "é".repeat(65) } },
      { decision: { ...approved.decision, reason: "é".repeat(2049) } },
    ])
      expect(() => approval({ ...approved, ...patch })).toThrow();
    expect(() =>
      approval({
        ...approvalFixture("cancelled"),
        resumed_activation_id: "unexpected",
      }),
    ).toThrow();
    expect(() =>
      approvalPage({ items: [], next_cursor: "é".repeat(65) }),
    ).toThrow();
  });
  it("compares action objects without losing numeric tokens or depending on property order", () => {
    expect(
      sameAction(
        parseUserJson('{"b":1.0,"a":-0.0}', 32768),
        parseUserJson('{"a":-0.0,"b":1.0}', 32768),
      ),
    ).toBe(true);
    expect(
      sameAction(
        parseUserJson('{"a":1.0}', 32768),
        parseUserJson('{"a":1}', 32768),
      ),
    ).toBe(false);
  });
  it("requires a receipt for the exact immutable request and submitted decision", () => {
    const value = approvalFixture("approved");
    const expected: ApprovalDecision = {
      workflow_id: value.workflow_id,
      key: value.key,
      activation_id: value.activation_id,
      revision: value.revision,
      action: value.action,
      decision_id: value.decision!.decision_id,
      decision: "approve",
      reviewer: "Casey",
      reason: "Reviewed destination",
    };
    const decode = decisionReceipt(expected);
    expect(
      decode({ approval: value, already_accepted: true }).already_accepted,
    ).toBe(true);
    for (const patch of [
      { workflow_id: "other" },
      { key: "other" },
      { activation_id: "other" },
      { revision: "2" },
      { action: { ...value.action, version: "changed" } },
      { action: { ...value.action, arguments: { destination: "changed" } } },
      { decision: { ...value.decision, reviewer: "someone else" } },
      { decision: { ...value.decision, reason: "changed" } },
      { decision: { ...value.decision, decision_id: "other" } },
    ])
      expect(() =>
        decode({ approval: { ...value, ...patch }, already_accepted: true }),
      ).toThrow();
    expect(() =>
      decode({
        approval: approvalFixture("rejected"),
        already_accepted: false,
      }),
    ).toThrow();
  });
});
