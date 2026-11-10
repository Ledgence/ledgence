// SPDX-License-Identifier: MIT
import { describe, expect, it } from "vitest";
import { decodeNativeExecutionSeeds } from "../live/seeded-executions";

const consoleUrl = "http://127.0.0.1:43210/console/";
const stack = { console: consoleUrl, database: "owned-test-db" };
const seed = {
  scenario: "native-restart-retains-authoritative-state",
  result: "passed",
  detail: { task_id: "task_original", workflow_id: "wf_owned_root" },
};

describe("native browser seed handoff", () => {
  it("preserves exact seed IDs regardless of newer unrelated acceptance results", () => {
    const newer = Array.from({ length: 100 }, (_, i) => ({
      scenario: `another-${i}`,
      result: "passed",
      detail: { task_id: `task_newer_${i}`, workflow_id: `wf_newer_${i}` },
    }));
    expect(
      decodeNativeExecutionSeeds(stack, [seed, ...newer], consoleUrl),
    ).toEqual({
      taskId: "task_original",
      workflowId: "wf_owned_root",
    });
  });

  it.each([
    { results: [] },
    { results: [seed, seed] },
    { results: [{ ...seed, result: "failed" }] },
  ])(
    "rejects absent, ambiguous or unsuccessful native evidence",
    ({ results }) => {
      expect(() =>
        decodeNativeExecutionSeeds(stack, results, consoleUrl),
      ).toThrow("Exactly one passed native restart seed");
    },
  );

  it("rejects evidence from another server before using its identifiers", () => {
    expect(() =>
      decodeNativeExecutionSeeds(
        { ...stack, console: "http://127.0.0.1:43211/console/" },
        [seed],
        consoleUrl,
      ),
    ).toThrow("belongs to another Console");
  });

  it.each([
    { task_id: "wf_wrong_kind" },
    { workflow_id: "task_wrong_kind" },
    { task_id: "task_contains/control" },
    { workflow_id: null },
  ])("rejects malformed execution identities", (detail) => {
    expect(() =>
      decodeNativeExecutionSeeds(
        stack,
        [{ ...seed, detail: { ...seed.detail, ...detail } }],
        consoleUrl,
      ),
    ).toThrow("Invalid native execution seed identity");
  });
});
