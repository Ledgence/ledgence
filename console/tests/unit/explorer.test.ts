// SPDX-License-Identifier: MIT
import { expect, it } from "vitest";
import {
  explorerNode,
  programHistory,
  type ExplorerNode,
} from "../../src/api/explorer";
import {
  evidenceEdges,
  nodeLabel,
  nodeStatus,
  nodeTiming,
  nodeType,
  micros,
  timelineRows,
  timelineBounds,
} from "../../src/features/explorer-model";
import { fixtureNodes, entrypoint } from "../explorer-fixture";

function select<K extends ExplorerNode["kind"]>(
  nodes: ExplorerNode[],
  kind: K,
) {
  const node = nodes.find(
    (item): item is Extract<ExplorerNode, { kind: K }> => item.kind === kind,
  );
  if (!node) throw new Error(`Missing ${kind} fixture`);
  return node;
}
function relations(nodes: ExplorerNode[]) {
  return evidenceEdges(nodes).map(({ from, to, relation, style }) => ({
    from,
    to,
    relation,
    style,
  }));
}

it("keeps a flat graph with verified parent, fork and resume relations and retained provenance", () => {
  const nodes = fixtureNodes();
  const edges = evidenceEdges(nodes);
  expect(relations(nodes)).toEqual(
    expect.arrayContaining([
      {
        from: "entrypoint:activation_validate",
        to: "fork:validate",
        relation: "registers",
        style: "parent",
      },
      {
        from: "fork:validate",
        to: "child:review",
        relation: "registers",
        style: "fork",
      },
      {
        from: "entrypoint:activation_validate",
        to: "join:validate",
        relation: "registers",
        style: "parent",
      },
      {
        from: "child:review",
        to: "join:validate",
        relation: "awaits terminal outcome",
        style: "fork",
      },
      {
        from: "join:validate",
        to: "entrypoint:activation_collect",
        relation: "resumes",
        style: "parent",
      },
    ]),
  );
  expect(edges).toHaveLength(5);
  expect(
    edges.every(
      (edge) =>
        edge.evidenceIds.includes(edge.from) &&
        edge.evidenceIds.includes(edge.to),
    ),
  ).toBe(true);
  expect(
    edges.some(
      (edge) => edge.from === "local:tests" || edge.to === "local:tests",
    ),
  ).toBe(false);
  expect(
    timelineRows(nodes).some(({ node }) => node.id === "local:tests"),
  ).toBe(true);
  expect(nodeType(select(nodes, "entrypoint"))).toBe("Entrypoint");
  expect(evidenceEdges([...nodes].reverse())).toEqual(edges);
  expect(evidenceEdges([...nodes, ...nodes])).toEqual(edges);
});

it("models fork4 plus parent work and joins without inventing a fork-to-parent-task arrow", () => {
  const nodes = fixtureNodes();
  const fork = select(nodes, "fork");
  const child = select(nodes, "child");
  const wait = select(nodes, "child_wait");
  fork.branch_keys = ["security:0", "tests:0", "dependencies:0", "docs:0"];
  child.key = fork.branch_keys[0]!;
  const branches = fork.branch_keys.slice(1).map((key, index) =>
    explorerNode({
      ...child,
      id: `child:${key}`,
      key,
      execution: { kind: "workflow", id: `wf_branch_${index}` },
      state: index === 0 ? "failed" : index === 1 ? "cancelled" : "succeeded",
    }),
  );
  const parentTask = explorerNode({
    ...child,
    id: "child:prepare",
    key: "prepare:0",
    fork_key: null,
    execution: { kind: "task", id: "task_prepare" },
  });
  wait.member_keys = [...fork.branch_keys];
  nodes.push(...branches, parentTask);
  const edges = evidenceEdges(nodes);
  expect(
    edges.filter((edge) => edge.from === fork.id && edge.style === "fork"),
  ).toHaveLength(4);
  expect(
    edges.filter(
      (edge) =>
        edge.to === wait.id && edge.relation === "awaits terminal outcome",
    ),
  ).toHaveLength(4);
  expect(edges).toContainEqual(
    expect.objectContaining({
      from: "entrypoint:activation_validate",
      to: parentTask.id,
      style: "parent",
    }),
  );
  expect(
    edges.some((edge) => edge.from === fork.id && edge.to === parentTask.id),
  ).toBe(false);
  expect(branches.map(nodeStatus)).toEqual([
    "failed",
    "cancelled",
    "succeeded",
  ]);
  expect(
    edges.some((edge) => edge.from === parentTask.id && edge.to === wait.id),
  ).toBe(false);
});

it("uses activation identity for repeated entrypoint names and never interprets opaque IDs", () => {
  const first = select(
    [entrypoint("activation_review_one", "review")],
    "entrypoint",
  );
  const second = select(
    [entrypoint("activation_review_two", "review")],
    "entrypoint",
  );
  first.id = 'entrypoint:["review",1]';
  first.decision_kind = "continue";
  first.resumed_activation_id = second.activation_id;
  second.decision_kind = "complete";
  second.resumed_activation_id = null;
  const nodes = [first, second];
  expect(timelineRows(nodes).map(({ node }) => nodeLabel(node))).toEqual([
    "review",
    "review",
  ]);
  expect(evidenceEdges(nodes)).toHaveLength(1);
  expect(evidenceEdges(nodes)[0]).toMatchObject({
    from: first.id,
    to: second.id,
    relation: "resumes",
  });
  expect(JSON.parse(evidenceEdges(nodes)[0]!.id)).toEqual([
    first.id,
    second.id,
    "resumes",
  ]);
});

it("never invents edges across partial evidence, timestamps or activation ownership", () => {
  const nodes = fixtureNodes().filter(
    (node) => node.kind !== "fork" && node.kind !== "child_wait",
  );
  expect(evidenceEdges(nodes)).toEqual([]);
  const partial = fixtureNodes().filter(
    (node) => node.kind !== "fork" && node.kind !== "entrypoint",
  );
  expect(relations(partial)).toEqual([
    {
      from: "child:review",
      to: "join:validate",
      relation: "awaits terminal outcome",
      style: "fork",
    },
  ]);
  expect(
    evidenceEdges(fixtureNodes().filter((node) => node.kind === "local")),
  ).toEqual([]);
});

it("requires matching fork membership and never substitutes a direct registration edge", () => {
  const nodes = fixtureNodes();
  const fork = select(nodes, "fork");
  fork.branch_keys = ["other:0"];
  const edges = evidenceEdges(nodes);
  expect(edges.some((edge) => edge.to === "child:review")).toBe(false);
});

it("requires an applied, accepted decision for direct child and wait registration", () => {
  const nodes = fixtureNodes();
  const first = select(nodes, "entrypoint");
  select(nodes, "child").fork_key = null;
  expect(
    evidenceEdges(nodes)
      .filter((edge) => edge.from === first.id)
      .map((edge) => edge.to),
  ).toEqual(["child:review", "fork:validate", "join:validate"]);
  first.decision_kind = null;
  first.applied_at = null;
  expect(
    evidenceEdges(nodes)
      .filter((edge) => edge.from === first.id)
      .map((edge) => edge.to),
  ).toEqual(["fork:validate"]);
  first.applied_at = 1200;
  first.error = { kind: "invalid_decision", message: "rejected" };
  expect(
    evidenceEdges(nodes).some(
      (edge) => edge.from === first.id && edge.to === "join:validate",
    ),
  ).toBe(false);
});

it("only directly resumes from a successfully applied continue decision", () => {
  const first = select([entrypoint()], "entrypoint");
  first.decision_kind = "continue";
  const nodes = [first, entrypoint("activation_collect", "collect")];
  expect(evidenceEdges(nodes)).toContainEqual(
    expect.objectContaining({
      from: first.id,
      to: "entrypoint:activation_collect",
      relation: "resumes",
    }),
  );
  first.applied_at = null;
  expect(evidenceEdges(nodes)).toEqual([]);
  first.applied_at = 1200;
  first.error = { kind: "invalid_decision", message: "rejected" };
  expect(evidenceEdges(nodes)).toEqual([]);
  first.error = null;
  for (const decision of ["suspend", "wait"] as const) {
    first.decision_kind = decision;
    expect(evidenceEdges(nodes)).toEqual([]);
  }
});

it("keeps pending joins visible without claiming completion or an absent destination", () => {
  const nodes = fixtureNodes();
  const wait = select(nodes, "child_wait");
  wait.resumed_activation_id = null;
  expect(nodeStatus(wait)).toBe("wait registered");
  expect(nodeTiming(wait)).toMatchObject({
    milestone: true,
    label: "Join decision applied · closing time not recorded",
  });
  expect(evidenceEdges(nodes).some((edge) => edge.from === wait.id)).toBe(
    false,
  );
  wait.resumed_activation_id = "activation_not_loaded";
  expect(nodeStatus(wait)).toBe("resume scheduled");
  expect(evidenceEdges(nodes).some((edge) => edge.from === wait.id)).toBe(
    false,
  );
});

it("bounds completed timelines by actual evidence rather than later fetch time", () => {
  const nodes = fixtureNodes();
  expect(timelineBounds(timelineRows(nodes), 999999999)).toEqual({
    start: 1000,
    end: 1150,
  });
  const child = select(nodes, "child");
  child.terminal_at = null;
  child.state = "running";
  expect(timelineBounds(timelineRows(nodes), 5000)).toEqual({
    start: 1000,
    end: 5000,
  });
});

it("does not turn an unavailable child's unknown end into a running interval", () => {
  const nodes = fixtureNodes();
  const child = select(nodes, "child");
  child.terminal_at = null;
  child.state = null;
  child.availability = "unavailable";
  expect(nodeTiming(child).open).toBe(false);
  expect(timelineBounds(timelineRows(nodes), 999999)).toEqual({
    start: 1000,
    end: 1100,
  });
  child.state = "running";
  child.availability = "available";
  expect(timelineBounds(timelineRows(nodes), 999999, false)).toEqual({
    start: 1000,
    end: 1100,
  });
});

it("orders trace rows by time with deterministic entrypoint-first ties", () => {
  const nodes = fixtureNodes().reverse();
  const rows = timelineRows(nodes);
  expect(rows[0]?.node.kind).toBe("entrypoint");
  expect(rows.map((row) => row.timing.start)).toEqual(
    [...rows.map((row) => row.timing.start)].sort((a, b) => a - b),
  );
  expect(timelineRows([...nodes].reverse()).map((row) => row.node.id)).toEqual(
    rows.map((row) => row.node.id),
  );
});

it("local acceptance is a milestone and an unaccepted observation is not 'not started'", () => {
  const local = select(fixtureNodes(), "local");
  expect(nodeTiming(local)).toMatchObject({
    start: 1050,
    end: 1050,
    milestone: true,
  });
  const failed = explorerNode({
    ...local,
    accepted_at: null,
    accepting_attempt_id: null,
    observation: {
      attempt_id: "att_2",
      started_at: 1040,
      elapsed_us: "15000",
      state: "failed",
    },
  });
  expect(nodeStatus(failed)).toBe("failed");
  expect(nodeTiming(failed)).toMatchObject({
    start: 1040,
    end: 1055,
    milestone: false,
  });
  expect(() =>
    explorerNode({ ...local, accepted_at: null, accepting_attempt_id: null }),
  ).toThrow("evidence");
});

it("keeps the same logical local identity and truthful timing for replay", () => {
  const local = select(fixtureNodes(), "local");
  const replay = explorerNode({
    ...local,
    observation: {
      attempt_id: "att_2",
      started_at: 1200,
      elapsed_us: "50",
      state: "replayed",
    },
  });
  expect(replay.id).toBe(local.id);
  expect(nodeStatus(replay)).toBe("accepted");
  expect(nodeTiming(replay).label).toContain("callable was not run");
  expect(evidenceEdges([entrypoint(), replay])).toEqual([]);
});

function externalWait() {
  return explorerNode({
    kind: "external_wait",
    id: "wait:approval",
    activation_id: "activation_validate",
    revision: "1",
    entrypoint: "validate",
    key: "approval:0",
    wait_kind: "event",
    deadline: 4000,
    registered_at: 2000,
    closed_at: 3000,
    wake_reason: null,
    resumed_activation_id: null,
  });
}
it("requires frozen wake evidence, not closure, for external resumption", () => {
  const node = select([externalWait()], "external_wait");
  const nodes = [
    entrypoint(),
    node,
    entrypoint("activation_collect", "collect"),
  ];
  expect(nodeStatus(node)).toBe("closed · reason unavailable");
  expect(evidenceEdges(nodes).some((edge) => edge.relation === "resumes")).toBe(
    false,
  );
  for (const reason of ["event", "timer", "timeout"] as const) {
    node.wake_reason = reason;
    node.resumed_activation_id = "activation_collect";
    expect(evidenceEdges(nodes)).toContainEqual(
      expect.objectContaining({
        from: node.id,
        to: "entrypoint:activation_collect",
        relation: "resumes",
      }),
    );
  }
});

it("strictly rejects C2 controller nodes and contradictory or unknown C3 evidence", () => {
  expect(() => explorerNode({ ...entrypoint(), kind: "phase" })).toThrow();
  expect(() =>
    explorerNode({
      ...entrypoint(),
      decision_kind: "continue",
      error: { kind: "rejected", message: "invalid" },
    }),
  ).toThrow("evidence");
  expect(() =>
    explorerNode({ ...entrypoint(), extra: "future-contract-field" }),
  ).toThrow();
  expect(() =>
    explorerNode({ ...externalWait(), wake_reason: "event" }),
  ).toThrow("evidence");
  expect(() =>
    explorerNode({
      ...externalWait(),
      wake_reason: "event",
      resumed_activation_id: "activation_collect",
      closed_at: null,
    }),
  ).toThrow("evidence");
});

it("keeps exact large measurements and caps representable trace dates", () => {
  const local = select(fixtureNodes(), "local");
  expect(nodeLabel(local)).toBe("tests:0");
  expect(micros("18446744073709551615")).toBe("18446744073709551.615 ms");
  local.observation = {
    attempt_id: "att_2",
    started_at: 1000,
    elapsed_us: "18446744073709551615",
    state: "returned",
  };
  expect(nodeTiming(local).end).toBe(253402300799999);
});
it("program history includes child-only programs and pins exact versions", () => {
  expect(programHistory("review", "1.0.0")).toBe(
    "/executions?program_id=review&include_children=true&version=1.0.0",
  );
});
