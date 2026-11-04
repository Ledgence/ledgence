// SPDX-License-Identifier: MIT
import { describe, it, expect } from "vitest";
import raw from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v13.json?raw";
import {
  snapshotFor,
  entriesFor,
  budgetPolicy,
} from "../../src/api/run-budgets";
import { submissionCommand } from "../../src/api/resources";
import { resolveDetailNavigation } from "../../src/features/detail-navigation";
const fixture = JSON.parse(raw),
  snap = fixture.run_budget,
  page = fixture.run_budget_entries;
describe("run budgets", () => {
  it("decodes the shared root and exclusive decimal cursor from Rust", () => {
    expect(snapshotFor(snap.scope, snap.subject)(snap).root).toEqual(snap.root);
    const next = entriesFor(snap.scope, snap.subject, null, 5)(page).next;
    expect(next).toBe("9007199254740993");
    expect(() => entriesFor(snap.scope, snap.subject, next, 5)(page)).toThrow();
  });
  it("reads decimal counts beyond the safe integer range without rounding", () => {
    const read = snapshotFor(snap.scope, snap.subject);
    const large = {
      ...snap,
      policy: { deadline_at_ms: 1 },
      calls: "9007199254740993",
      delegations: "4294967297",
    };
    expect(read(large).calls).toBe("9007199254740993");
    expect(read(large).delegations).toBe("4294967297");
    for (const invalid of [0, "00", "-1", "+1", "18446744073709551616", null])
      expect(() => read({ ...large, calls: invalid })).toThrow();
    expect(() => read({ ...large, policy: { max_calls: 1 } })).toThrow();
  });
  it("binds approval references to a call in the same workflow and scope", () => {
    const read = entriesFor(snap.scope, snap.subject, null, 5);
    const approval = {
      scope: snap.scope,
      workflow_id: page.items[0].subject.workflow_id,
      activation_id: "origin",
      approval_key: "authorize",
    };
    const row = { ...page.items[0], approval };
    const response = (changes: object) => ({
      ...page,
      items: [{ ...row, ...changes }],
    });
    expect(read(response({})).items[0]?.approval).toEqual(approval);
    for (const invalid of [
      { approval: null },
      { approval: { ...approval, workflow_id: "outside" } },
      {
        approval: { ...approval, scope: { ...snap.scope, namespace: "other" } },
      },
      { subject: { kind: "task", task_id: "task" } },
      { action: "delegation", usage: { kind: "unknown" } },
    ])
      expect(() => read(response(invalid))).toThrow();
  });
  it("rejects a substituted root and forged usage on denied entries", () => {
    const parse = entriesFor(snap.scope, snap.subject, null, 5);
    expect(() => parse({ ...page, root: snap.subject })).toThrow();
    expect(() => parse({ ...page, subject: snap.root })).toThrow();
    expect(() =>
      parse({
        ...page,
        items: [
          { ...page.items[0], outcome: "denied", reason: "calls_exhausted" },
        ],
      }),
    ).toThrow();
    expect(() =>
      parse({
        ...page,
        items: [{ ...page.items[0], usage: { kind: "reported" } }],
      }),
    ).toThrow();
  });
  it("including zero preserves its meaning and absence preserves the previous submission shape", () => {
    const input = {
      program: { id: "p", version: "1" },
      queue: "q",
      correlation_key: null,
      data: {},
      retry_policy: { max_attempts: 1, retry_delay_ms: 0 },
      attempt_timeout_ms: 60000,
    };
    expect(submissionCommand(input)).toEqual(input);
    expect(
      submissionCommand({ ...input, budget: { max_calls: 0 } }).budget
        ?.max_calls,
    ).toBe(0);
    for (const data of [
      {},
      null,
      { max_calls: null },
      { max_delegations: 1000001 },
      { unknown: 1 },
    ])
      expect(() => budgetPolicy(data)).toThrow();
  });
  it("opens the budget section for a task and a workflow", () => {
    for (const kind of ["task", "workflow"] as const)
      expect(
        resolveDetailNavigation(
          kind,
          new URLSearchParams("tab=General&section=budget"),
        ).section,
      ).toBe("budget");
  });
});
