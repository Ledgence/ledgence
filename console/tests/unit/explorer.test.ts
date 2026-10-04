// SPDX-License-Identifier: MIT
import { expect, it } from "vitest";
import {
  explorerNode,
  programHistory,
  type ExplorerNode,
} from "../../src/api/explorer";
import {
  evidenceEdges,
  graphEdges,
  recordedRelations,
  nodeLabel,
  nodeStatus,
  nodeTiming,
  nodeType,
  micros,
  timelineRows,
  timelineBounds,
} from "../../src/features/explorer-model";
import { fixtureNodes, entrypoint, relation } from "../explorer-fixture";

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
it("resolves only server relations, preserving semantic kinds and carrier provenance", () => {
  const nodes = fixtureNodes();
  const edges = evidenceEdges(nodes);
  expect(edges).toHaveLength(7);
  expect(new Set(edges.map((edge) => edge.kind))).toEqual(
    new Set(["invokes", "registers", "branch", "awaits_terminal", "resumes"]),
  );
  expect(edges).toContainEqual(
    expect.objectContaining({
      from: "entrypoint:activation_validate",
      to: "local:tests",
      relation: "invokes",
      evidenceIds: ["local:tests"],
    }),
  );
  expect(edges).toContainEqual(
    expect.objectContaining({
      from: "fork:validate",
      to: "child:review",
      relation: "includes branch",
      style: "fork",
      evidenceIds: ["child:review", "fork:validate"],
    }),
  );
  expect(edges).toContainEqual(
    expect.objectContaining({
      from: "child:review",
      to: "join:validate",
      relation: "terminal outcome awaited by",
      evidenceIds: ["join:validate"],
    }),
  );
  expect(evidenceEdges([...nodes].reverse())).toEqual(edges);
  expect(evidenceEdges([...nodes, ...nodes])).toEqual(edges);
  expect(nodeType(select(nodes, "entrypoint"))).toBe("Entrypoint");
});

it("does not manufacture edges from ownership, fork keys, decisions, waits, or timestamps", () => {
  const nodes = fixtureNodes().map((node) => ({ ...node, relations: [] }));
  expect(evidenceEdges(nodes)).toEqual([]);
  expect(recordedRelations(nodes)).toEqual([]);
  const source = select(nodes, "entrypoint");
  source.decision_kind = "continue";
  source.resumed_activation_id = "activation_collect";
  expect(evidenceEdges(nodes)).toEqual([]);
});

it("retains unresolved references without fabricating nodes or substituting a parent edge", () => {
  const nodes = fixtureNodes().filter((node) => node.kind === "child");
  const records = recordedRelations(nodes);
  expect(records).toHaveLength(2);
  expect(
    records.every(
      (record) =>
        record.source === null && record.target?.id === "child:review",
    ),
  ).toBe(true);
  expect(
    records.every((record) => record.evidenceIds.join() === "child:review"),
  ).toBe(true);
  expect(evidenceEdges(nodes)).toEqual([]);
  const full = fixtureNodes();
  expect(evidenceEdges([...nodes, ...full])).toEqual(evidenceEdges(full));
});

it("simplifies a fully loaded fork path while retaining direct invocation evidence and page fallback", () => {
  const nodes = fixtureNodes();
  const edges = evidenceEdges(nodes);
  expect(
    edges.some(
      (edge) =>
        edge.from === "entrypoint:activation_validate" &&
        edge.to === "child:review",
    ),
  ).toBe(true);
  expect(graphEdges(edges)).toHaveLength(6);
  expect(
    graphEdges(edges).some(
      (edge) =>
        edge.from === "entrypoint:activation_validate" &&
        edge.to === "child:review",
    ),
  ).toBe(false);
  const partial = evidenceEdges(nodes.filter((node) => node.kind !== "fork"));
  expect(
    graphEdges(partial).some(
      (edge) =>
        edge.from === "entrypoint:activation_validate" &&
        edge.to === "child:review",
    ),
  ).toBe(true);
  expect(graphEdges(edges).some((edge) => edge.to === "local:tests")).toBe(
    true,
  );
});

it("uses typed activation identities for repeated entrypoints and leaves opaque relation IDs uninterpreted", () => {
  const first = select(
    [entrypoint("activation_review_one", "review")],
    "entrypoint",
  );
  const second = select(
    [entrypoint("activation_review_two", "review")],
    "entrypoint",
  );
  first.id = 'entrypoint:["review",1]';
  first.relations = [
    {
      ...relation(
        "resumes",
        { kind: "entrypoint", activation_id: first.activation_id },
        { kind: "entrypoint", activation_id: second.activation_id },
      ),
      id: "opaque:server-relation",
    },
  ];
  expect(evidenceEdges([first, second])).toEqual([
    expect.objectContaining({
      id: "opaque:server-relation",
      from: first.id,
      to: second.id,
      relation: "resumes",
      evidenceIds: [first.id],
    }),
  ]);
  expect(
    timelineRows([first, second]).map(({ node }) => nodeLabel(node)),
  ).toEqual(["review", "review"]);
});

it("resolves same-key local operations by activation without drawing local-to-local order", () => {
  const first = select(fixtureNodes(), "local");
  const second = explorerNode({
    ...first,
    id: "local:second",
    activation_id: "activation_collect",
    relations: [
      relation(
        "invokes",
        { kind: "entrypoint", activation_id: "activation_collect" },
        { kind: "local", activation_id: "activation_collect", key: first.key },
      ),
    ],
  });
  const nodes = [
    entrypoint(),
    entrypoint("activation_collect", "collect"),
    first,
    second,
  ];
  const edges = evidenceEdges(nodes);
  expect(edges).toHaveLength(2);
  expect(edges).toContainEqual(
    expect.objectContaining({
      from: "entrypoint:activation_collect",
      to: "local:second",
    }),
  );
  expect(edges.every((edge) => edge.kind === "invokes")).toBe(true);
});

it("keeps pending joins visible without treating an unloaded resume as absent", () => {
  const nodes = fixtureNodes();
  const wait = select(nodes, "child_wait");
  wait.resumed_activation_id = "activation_not_loaded";
  wait.relations = [
    relation(
      "resumes",
      { kind: "child_wait", activation_id: wait.activation_id },
      { kind: "entrypoint", activation_id: wait.resumed_activation_id },
    ),
  ];
  expect(nodeStatus(wait)).toBe("resume scheduled");
  expect(nodeTiming(wait)).toMatchObject({
    milestone: true,
    label: "Join decision applied · closing time not recorded",
  });
  expect(recordedRelations([wait])[0]?.target).toBeNull();
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
  expect(evidenceEdges([entrypoint(), replay])).toEqual([
    expect.objectContaining({ relation: "invokes", to: replay.id }),
  ]);
});

function externalWait() {
  return explorerNode({
    kind: "external_wait",
    relations: [],
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
it("does not infer external resumption from closure or wake metadata without a server relation", () => {
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
    expect(evidenceEdges(nodes)).toEqual([]);
    node.relations = [
      relation(
        "resumes",
        { kind: "external_wait", key: node.key },
        { kind: "entrypoint", activation_id: "activation_collect" },
      ),
    ];
    expect(evidenceEdges(nodes)).toContainEqual(
      expect.objectContaining({
        from: node.id,
        to: "entrypoint:activation_collect",
        relation: "resumes",
      }),
    );
    node.relations = [];
  }
});

it("strictly rejects old controller nodes and contradictory or unknown C5 evidence", () => {
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
