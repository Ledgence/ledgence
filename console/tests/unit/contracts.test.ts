import { readFileSync } from "node:fs";
import { describe, it, expect } from "vitest";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import { decodeConfig, record } from "../../src/api/codecs";
import * as dto from "../../src/api/resources";
import { taskPage, workflowPage, taskResult } from "../../src/api/resources";
import * as schema from "../../src/api/schema";
import { decimal } from "../../src/api/schema";
import * as explorer from "../../src/api/explorer";
import {
  evidenceEdges,
  recordedRelations,
  nodeStatus,
  nodeTiming,
} from "../../src/features/explorer-model";
const source = readFileSync(
  new URL(
    "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v4.json",
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
    expect(decodeConfig(field("config")).contract_version).toBe(4);
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
  execution_history: explorer.executionPage,
  explorer: explorer.workflowExplorer,
  ancestry: explorer.ancestry,
  entrypoint_attempts: dto.attemptPage,
  workers: dto.workerPage,
  worker_detail: dto.workerDetail,
};
for (const [name, decode] of Object.entries(decoders))
  it(`decodes Rust fixture ${name}`, () => {
    expect(() => decode(field(name))).not.toThrow();
  });

const cases = record(field("explorer_cases"), [
  "branch_cancelled",
  "branch_failed",
  "branch_security",
  "review_declined",
  "closed_without_wake",
  "external_waits",
  "rejected",
  "repeated_entrypoint",
  "retry",
  "unavailable_child",
  "mixed_local",
  "observed_local_failure",
]);
for (const [name, value] of Object.entries(cases))
  it(`decodes Rust Explorer case ${name}`, () => {
    expect(() => explorer.workflowExplorer(value)).not.toThrow();
  });

describe("Rust-produced Explorer C4", () => {
  it("retains local invocation evidence without claiming completion or ordering sibling operations", () => {
    for (const name of ["mixed_local", "observed_local_failure"] as const) {
      const nodes = explorer.workflowExplorer(cases[name]).page.items;
      const local = nodes.find((node) => node.kind === "local")!;
      const invoked = evidenceEdges(nodes).filter(
        (edge) => edge.to === local.id,
      );
      expect(invoked).toEqual([
        expect.objectContaining({ kind: "invokes", evidenceIds: [local.id] }),
      ]);
      expect(evidenceEdges(nodes).some((edge) => edge.from === local.id)).toBe(
        false,
      );
      if (name === "observed_local_failure") {
        expect(local.accepted_at).toBeNull();
        expect(local.observation?.state).toBe("failed");
        expect(nodeStatus(local)).toBe("failed");
      }
    }
  });
  it("preserves branch failures and a declined review without assuming a successful business outcome", () => {
    for (const [name, state] of [
      ["branch_failed", "failed"],
      ["branch_cancelled", "cancelled"],
    ] as const) {
      const response = explorer.workflowExplorer(cases[name]);
      const branch = response.page.items.find(
        (node) => node.kind === "child" && node.state === state,
      )!;
      expect(branch).toBeDefined();
      expect(nodeStatus(branch)).toBe(state);
      expect(evidenceEdges(response.page.items)).toContainEqual(
        expect.objectContaining({
          from: branch.id,
          relation: "terminal outcome awaited by",
        }),
      );
      expect(response.workflow.summary.workflow.state).toBe("failed");
      expect(
        response.page.items.some(
          (node) => node.kind === "child" && node.key === "publish-draft:0",
        ),
      ).toBe(false);
    }
    const declined = explorer.workflowExplorer(cases.review_declined);
    const final = declined.page.items
      .filter((node) => node.kind === "entrypoint")
      .at(-1)!;
    expect(final.entrypoint).toBe("publish_report");
    expect(final.decision_kind).toBe("complete");
    expect(
      declined.page.items.some(
        (node) => node.kind === "child" && node.key === "publish-final:0",
      ),
    ).toBe(false);
  });

  it("keeps a subworkflow's internal evidence in its own response and graph", () => {
    const parent = explorer.workflowExplorer(field("explorer"));
    const child = explorer.workflowExplorer(cases.branch_security);
    const childId = child.workflow.summary.workflow.workflow_id;
    expect(
      parent.page.items.some(
        (node) => node.kind === "child" && node.execution.id === childId,
      ),
    ).toBe(true);
    expect(child.workflow.summary.workflow.parent_workflow_id).toBe(
      parent.workflow.summary.workflow.workflow_id,
    );
    expect(child.page.items.map((node) => node.kind)).toEqual([
      "entrypoint",
      "local",
    ]);
    const parentIds = new Set(parent.page.items.map((node) => node.id));
    expect(child.page.items.every((node) => !parentIds.has(node.id))).toBe(
      true,
    );
    expect(evidenceEdges(child.page.items)).toEqual([
      expect.objectContaining({ kind: "invokes" }),
    ]);
  });

  it("decodes the full fork4 story without a fabricated branch-to-parent relationship", () => {
    const response = explorer.workflowExplorer(field("explorer"));
    const nodes = response.page.items;
    const entrypoints = nodes.filter((node) => node.kind === "entrypoint");
    expect(entrypoints.map((node) => node.entrypoint)).toEqual([
      "start",
      "after_prepare",
      "publish_draft",
      "review",
      "publish_report",
      "finish",
    ]);
    const fork = nodes.find((node) => node.kind === "fork")!;
    const prepare = nodes.find(
      (node) => node.kind === "child" && node.key === "prepare:0",
    )!;
    const edges = evidenceEdges(nodes);
    expect(
      edges.filter((edge) => edge.from === fork.id && edge.style === "fork"),
    ).toHaveLength(4);
    expect(edges).toContainEqual(
      expect.objectContaining({
        from: entrypoints[0]!.id,
        to: prepare.id,
        style: "parent",
      }),
    );
    expect(
      edges.some((edge) => edge.from === fork.id && edge.to === prepare.id),
    ).toBe(false);
    expect(nodes.filter((node) => node.kind === "child_wait")).toHaveLength(5);
    expect(response.evidence).toBe("retained_records_only");
    expect(response.page.next_cursor).toBeNull();
    expect(edges.filter((edge) => edge.relation === "resumes")).toHaveLength(5);
    expect(
      edges.some(
        (edge) =>
          entrypoints.some((node) => node.id === edge.from) &&
          entrypoints.some((node) => node.id === edge.to),
      ),
    ).toBe(false);
  });

  it("decodes every bounded page and preserves opaque cursors and retained partial evidence", () => {
    const pages = schema.array(
      schema.object({
        request: schema.object({
          cursor: schema.nullable(schema.string),
          limit: schema.integer(1, 100),
        }),
        response: explorer.workflowExplorer,
      }),
    )(field("explorer_pages"));
    expect(pages.length).toBeGreaterThan(1);
    let expectedCursor: string | null = null;
    for (const { request, response } of pages) {
      expect(request.cursor).toBe(expectedCursor);
      expect(response.page.items.length).toBeLessThanOrEqual(request.limit);
      const loaded = new Set(response.page.items.map((node) => node.id));
      expect(
        evidenceEdges(response.page.items).every(
          (edge) => loaded.has(edge.from) && loaded.has(edge.to),
        ),
      ).toBe(true);
      const records = recordedRelations(response.page.items);
      expect(records.map((relation) => relation.record.id).sort()).toEqual(
        [
          ...new Set(
            response.page.items.flatMap((node) =>
              node.relations.map((relation) => relation.id),
            ),
          ),
        ].sort(),
      );
      expectedCursor = response.page.next_cursor;
    }
    expect(expectedCursor).toBeNull();
    const allNodes = pages.flatMap(({ response }) => response.page.items);
    const full = explorer.workflowExplorer(field("explorer"));
    expect(allNodes).toEqual(full.page.items);
    expect(new Set(allNodes.map((node) => node.id)).size).toBe(allNodes.length);
    expect(evidenceEdges(allNodes)).toEqual(evidenceEdges(full.page.items));
  });

  it("keeps repeated handler invocations distinct and retries within one invocation", () => {
    const repeated = explorer.workflowExplorer(cases.repeated_entrypoint).page
      .items;
    const reviews = repeated.filter(
      (node) => node.kind === "entrypoint" && node.entrypoint === "review",
    );
    expect(reviews).toHaveLength(2);
    expect(reviews[0]!.id).not.toBe(reviews[1]!.id);
    expect(reviews[0]!.activation_id).not.toBe(reviews[1]!.activation_id);
    expect(evidenceEdges(repeated)).toContainEqual(
      expect.objectContaining({
        from: reviews[0]!.id,
        to: reviews[1]!.id,
        relation: "resumes",
      }),
    );
    const retry = explorer.workflowExplorer(cases.retry).page.items;
    const controller = retry.filter((node) => node.kind === "entrypoint");
    const attempts = dto.attemptPage(field("entrypoint_attempts"));
    expect(controller).toHaveLength(1);
    expect(attempts.items).toHaveLength(2);
    expect(
      attempts.items.every(
        (attempt) => attempt.task_id === controller[0]!.activation_id,
      ),
    ).toBe(true);
    expect(attempts.items.map((attempt) => attempt.state)).toEqual([
      "succeeded",
      "failed",
    ]);
    expect(retry.filter((node) => node.kind === "local")).toHaveLength(1);
    expect(evidenceEdges(retry)).toEqual([
      expect.objectContaining({ kind: "invokes" }),
    ]);
  });

  it("distinguishes continue, event, timer and timeout resumes from a closed wait", () => {
    const nodes = explorer.workflowExplorer(cases.external_waits).page.items;
    const waits = nodes.filter((node) => node.kind === "external_wait");
    expect(waits.map((node) => node.wake_reason)).toEqual([
      "event",
      "timer",
      "timeout",
    ]);
    const resumes = evidenceEdges(nodes).filter(
      (edge) => edge.relation === "resumes",
    );
    expect(resumes).toHaveLength(4);
    for (const wait of waits)
      expect(resumes.some((edge) => edge.from === wait.id)).toBe(true);
    const closed = explorer.workflowExplorer(cases.closed_without_wake).page
      .items;
    const wait = closed.find((node) => node.kind === "external_wait")!;
    expect(nodeStatus(wait)).toBe("closed · reason unavailable");
    expect(
      evidenceEdges(closed).some((edge) => edge.relation === "resumes"),
    ).toBe(false);
  });

  it("preserves unavailable child identity and rejected-decision diagnostics exactly", () => {
    const nodes = explorer.workflowExplorer(cases.unavailable_child).page.items;
    const child = nodes.find((node) => node.kind === "child")!;
    expect(nodeStatus(child)).toBe("unavailable");
    expect(nodeTiming(child).open).toBe(false);
    expect(evidenceEdges(nodes)).toContainEqual(
      expect.objectContaining({
        from: child.id,
        relation: "terminal outcome awaited by",
      }),
    );
    const rejected = explorer.workflowExplorer(cases.rejected).page.items;
    const entrypoint = rejected.find((node) => node.kind === "entrypoint")!;
    expect(entrypoint.error?.message).toBe(
      "Conflicting binding\nUnicode: 東京; NUL: \0; literal: \\u0000",
    );
    expect(nodeStatus(entrypoint)).toBe("decision rejected");
    expect(evidenceEdges(rejected)).toEqual([]);
  });

  it("keeps unified business history and ancestry separate from controller activations", () => {
    const history = explorer.executionPage(field("execution_history"));
    expect(new Set(history.items.map((execution) => execution.kind))).toEqual(
      new Set(["task", "workflow"]),
    );
    const root = history.items.filter(
      (execution) => execution.parent_workflow_id === null,
    );
    expect(root).toHaveLength(1);
    expect(root[0]!.id).toBe("wf_release_fork4");
    expect(
      history.items.some((execution) => execution.id.startsWith("act_")),
    ).toBe(false);
    const ancestry = explorer.ancestry(field("ancestry"));
    expect(ancestry.path.map((item) => item.execution.id)).toEqual([
      "wf_release_fork4",
      "wf_release_branch_0",
    ]);
    expect(ancestry.execution).toEqual(ancestry.path.at(-1)!.execution);
  });

  it("rejects C3 configuration and nodes without relations inside the C4 envelope", () => {
    const historical = parseUserJson(
      readFileSync(
        new URL(
          "../../../crates/ledgence-orchestration-api/tests/fixtures/historical/console-v3.json",
          import.meta.url,
        ),
        "utf8",
      ),
      2 * 1024 * 1024,
    );
    if (!historical || typeof historical !== "object")
      throw new Error("Missing C3 history");
    expect(() => decodeConfig(Reflect.get(historical, "config"))).toThrow(
      "incompatible",
    );
    const current = explorer.workflowExplorer(field("explorer"));
    expect(() =>
      explorer.workflowExplorer({
        ...current,
        page: {
          ...current.page,
          items: current.page.items.map((node) =>
            Object.fromEntries(
              Object.entries(node).filter(([key]) => key !== "relations"),
            ),
          ),
        },
      }),
    ).toThrow("contract");
  });

  it("strictly rejects unknown, untyped, oversized and conflicting relation records", () => {
    const current = explorer.workflowExplorer(field("explorer"));
    const node = current.page.items.find((item) => item.relations.length > 0)!;
    const relation = node.relations[0]!;
    for (const invalid of [
      { ...relation, kind: "guessed_dependency" },
      { ...relation, source: "opaque-node-id" },
      {
        ...relation,
        target: { kind: "child", key: "prepare:0", activation_id: "extra" },
      },
      { ...relation, trace_parent: "not-structural-evidence" },
    ])
      expect(() =>
        explorer.explorerNode({ ...node, relations: [invalid] }),
      ).toThrow();
    expect(() =>
      explorer.explorerNode({
        ...node,
        relations: Array.from({ length: 67 }, () => relation),
      }),
    ).toThrow();
    expect(() =>
      explorer.workflowExplorer({
        ...current,
        page: {
          ...current.page,
          items: [
            node,
            {
              ...node,
              relations: [
                {
                  ...relation,
                  kind: relation.kind === "invokes" ? "registers" : "invokes",
                },
              ],
            },
          ],
        },
      }),
    ).toThrow("Conflicting");
  });
});
