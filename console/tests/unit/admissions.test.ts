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
    "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v12.json",
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
    const changed = decode(fixture.snapshot);
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

it("keeps provider admission capacity evidence separate from business completion", () => {
  const provider = fixture.provider as Record<string, unknown>;
  const graph = workflowExplorer(provider.graph);
  expect(
    graph.page.items.some((node) => node.kind === "admission_operation"),
  ).toBe(false);
  const wait = graph.page.items.find((node) => node.kind === "admission_wait")!;
  if (wait.kind !== "admission_wait") throw new Error("fixture kind");
  expect(wait.subject.kind).toBe("provider");
  expect(wait.selection_kind).toBe("provider");
  const complete = admissionSnapshot(provider.completed_pending);
  expect(complete.state).toBe("finished");
  expect(complete.finish).toBeNull();
  expect(complete.provider?.report?.call_completion).toBe("completed");
  const unknown = admissionSnapshot(provider.unknown_pending);
  expect(unknown.state).toBe("unknown_occupied");
  expect(unknown.provider?.report?.call_completion).toBe("unknown");
  const malformed = parseUserJson(
    stringifyUserJson(provider.unknown_pending),
    256 * 1024,
  ) as Record<string, unknown>;
  malformed.state = "finished";
  expect(() => admissionSnapshot(malformed)).toThrow();
});
it("rejects ambiguous provider subjects and unlinked composed sources", () => {
  const provider = fixture.provider as Record<string, unknown>;
  for (const alter of [
    (v: Record<string, unknown>) => {
      (v.binding as Record<string, unknown>).operation = {};
    },
    (v: Record<string, unknown>) => {
      v.provider = null;
    },
    (v: Record<string, unknown>) => {
      const subject = (v.binding as Record<string, unknown>).provider as Record<
        string,
        unknown
      >;
      (subject.action as Record<string, unknown>).arguments = {
        text: "x".repeat(32 * 1024),
      };
    },
    (v: Record<string, unknown>) => {
      const record = v.provider as Record<string, unknown>;
      const source = record.source as Record<string, unknown>;
      (source.provider as Record<string, unknown>).claim_id = "foreign";
    },
  ]) {
    const value = parseUserJson(
      stringifyUserJson(provider.completed_pending),
      256 * 1024,
    ) as Record<string, unknown>;
    alter(value);
    expect(() => admissionSnapshot(value)).toThrow();
  }
});
