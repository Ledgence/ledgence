// SPDX-License-Identifier: MIT
import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import { admissionFor, admissionSnapshot } from "../../src/api/admissions";
import { workflowExplorer } from "../../src/api/explorer";
import {
  evidenceEdges,
  nodeTiming,
  referenceIdentity,
} from "../../src/features/explorer-model";
const raw = readFileSync(
  new URL(
    "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v8.json",
    import.meta.url,
  ),
  "utf8",
);
const fixture = (parseUserJson(raw, 2 * 1024 * 1024) as Record<string, unknown>)
  .admission as Record<string, unknown>;
it("decodes durable admission metadata without inventing invocation from an offer", () => {
  const pending = workflowExplorer(fixture.pending);
  const accepted = workflowExplorer(fixture.accepted);
  const closed = workflowExplorer(fixture.closed);
  expect(pending.workflow.admission_wait?.resource).toBe("models.shared");
  expect(evidenceEdges(pending.page.items).map((v) => v.kind)).toEqual([
    "registers",
  ]);
  expect(evidenceEdges(closed.page.items).map((v) => v.kind)).toEqual([
    "registers",
  ]);
  const operation = accepted.page.items.find(
    (v) => v.kind === "admission_operation",
  )!;
  expect(nodeTiming(operation).milestone).toBe(true);
  expect(operation.relations[0]?.kind).toBe("invokes");
  expect(referenceIdentity(operation)).toBe(
    referenceIdentity(operation.relations[0]!.target),
  );
  expect(operation.relations[0]!.source).toEqual({
    kind: "entrypoint",
    activation_id: operation.activation_id,
  });
});
it("binds inspected accepted results to original source, program and callback metadata", () => {
  const e = workflowExplorer(fixture.accepted);
  const operation = e.page.items.find((v) => v.kind === "admission_operation")!;
  if (operation.kind !== "admission_operation") throw new Error("fixture kind");
  const decode = admissionFor(operation, "instance_demo");
  const v = decode(fixture.snapshot);
  type Snapshot = typeof v;
  const numbers = stringifyUserJson([
    v.binding.operation.arguments,
    v.finish!.output,
  ]);
  expect(numbers).toContain("18446744073709551615");
  expect(numbers).toContain("1.0");
  expect(numbers).toContain("-0.0");
  for (const modify of [
    (v: Snapshot) => {
      v.source!.source_id = "foreign";
    },
    (v: Snapshot) => {
      v.finish!.source.owner.attempt_id = "replay-attempt";
    },
    (v: Snapshot) => {
      v.binding.operation.version = "2";
    },
    (v: Snapshot) => {
      v.finish!.accepted_at++;
    },
  ]) {
    const changed = admissionSnapshot(fixture.snapshot);
    modify(changed);
    expect(() => decode(changed)).toThrow();
  }
  expect(() =>
    admissionFor(operation, "another-instance")(fixture.snapshot),
  ).toThrow();
});
it("requires native admission metadata and rejects foreign scopes and extra payloads", () => {
  const e = workflowExplorer(fixture.pending);
  delete (e.workflow as Partial<typeof e.workflow>).admission_wait;
  expect(() => workflowExplorer(e)).toThrow();
  const snapshot = admissionSnapshot(fixture.snapshot);
  expect(() => admissionSnapshot({ ...snapshot, hidden: true })).toThrow();
  expect(() =>
    admissionSnapshot({ ...snapshot, state: "unknown_occupied" }),
  ).toThrow();
  const altered = admissionSnapshot(fixture.snapshot);
  altered.finish!.source.owner.scope.namespace = "elsewhere";
  expect(() => admissionSnapshot(altered)).toThrow();
});
