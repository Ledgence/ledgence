import { readFileSync } from "node:fs";
import { describe, it, expect } from "vitest";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import { decodeConfig, record } from "../../src/api/codecs";
import * as dto from "../../src/api/resources";
import { taskPage, workflowPage, taskResult } from "../../src/api/resources";
import { decimal } from "../../src/api/schema";
const source = readFileSync(
  new URL(
    "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v1.json",
    import.meta.url,
  ),
  "utf8",
);
const fixture = parseUserJson(source, 2 * 1024 * 1024);
function field(name: string): unknown {
  if (!fixture || typeof fixture !== "object" || !(name in fixture))
    throw new Error(`Missing Rust fixture ${name}`);
  return Reflect.get(fixture, name);
}
describe("Rust-produced Console contract", () => {
  it("decodes configuration and scope-free task pages", () => {
    expect(decodeConfig(field("config")).contract_version).toBe(1);
    expect(taskPage(field("tasks")).items[0]?.descriptor.program.id).toBe(
      "invoice-issuer",
    );
  });
  it("preserves metadata revisions above Number.MAX_SAFE_INTEGER", () => {
    expect(workflowPage(field("workflows")).items[0]?.workflow.revision).toBe(
      "9007199254740993",
    );
  });
  it("distinguishes pending from successful JSON null", () => {
    expect(taskResult(field("pending_result")).outcome).toBeNull();
    const out = taskResult(field("null_result")).outcome;
    expect(out?.kind).toBe("succeeded");
    if (out?.kind === "succeeded") expect(out.output).toBeNull();
  });
  it("preserves payload integers, floats and negative zero", () => {
    const encoded = stringifyUserJson(field("numeric_payload"));
    for (const token of [
      "9007199254740993",
      "18446744073709551615",
      "-9223372036854775808",
      "1.0",
      "-0.0",
    ])
      expect(encoded).toContain(token);
  });
  it("rejects unknown fields and numeric u64s", () => {
    expect(() => record({ id: "x", scope: {} }, ["id"])).toThrow();
    for (const value of [1, "01", "18446744073709551616", "-1"])
      expect(() => decimal(value)).toThrow();
  });
});

const decoders = {
  task_status: dto.observedTask,
  task_detail: dto.taskDetail,
  attempts: dto.attemptPage,
  attempt_detail: dto.attemptDetail,
  workflow_status: dto.observedWorkflow,
  workflow_detail: dto.workflowDetail,
  activations: dto.activationPage,
  children: dto.childPage,
  waits: dto.workflowWaits,
  local_steps: dto.localStepPage,
  workflow_history: dto.workflowHistoryPage,
  programs: dto.programPage,
  program_versions: dto.programVersions,
  program_detail: dto.programDetail,
  program_receipt: dto.programReceipt,
  workers: dto.workerPage,
  worker_detail: dto.workerDetail,
};
for (const [name, decode] of Object.entries(decoders))
  it(`decodes Rust fixture ${name}`, () => {
    expect(() => decode(field(name))).not.toThrow();
  });
