// SPDX-License-Identifier: MIT
import { expect, it } from "vitest";
import { explorerNode, programHistory } from "../../src/api/explorer";
import {
  evidenceEdges,
  graphLayout,
  graphEdgeRoute,
  nodeLabel,
  nodeStatus,
  nodeTiming,
  phaseGroups,
  micros,
  timelineRows,
  timelineBounds,
} from "../../src/features/explorer-model";
import { fixtureNodes, phase } from "../explorer-fixture";

it("routes registration around joins and keeps every edge out of unrelated cards", () => {
  const nodes = fixtureNodes();
  const child = nodes.find((node) => node.kind === "child")!;
  if (child.kind !== "child") throw Error("fixture");
  child.fork_key = null;
  const source = nodes.find((node) => node.kind === "phase")!;
  if (source.kind !== "phase") throw Error("fixture");
  source.decision_kind = "suspend";
  // Exercise a direct task, a fork branch, multiple gate rows and a later
  // resume. Orthogonal segments must never disappear behind another card.
  const clone = explorerNode({
    ...child,
    id: "child:second",
    key: "second:0",
    fork_key: "validate:0",
  });
  nodes.push(clone);
  const fork = nodes.find((node) => node.kind === "fork")!;
  if (fork.kind !== "fork") throw Error("fixture");
  fork.branch_keys.push("second:0");
  const layout = graphLayout(phaseGroups(nodes), []);
  expect(layout.width).toBeLessThanOrEqual(840);
  for (const edge of evidenceEdges(nodes)) {
    const points = graphEdgeRoute(edge, layout.positions)!;
    for (let i = 1; i < points.length; i++) {
      const a = points[i - 1]!;
      const b = points[i]!;
      expect(a.x === b.x || a.y === b.y).toBe(true);
      for (const [id, card] of layout.positions) {
        if (id === edge.from || id === edge.to) continue;
        const crosses =
          a.x === b.x
            ? a.x > card.x &&
              a.x < card.x + card.width &&
              Math.max(a.y, b.y) > card.y &&
              Math.min(a.y, b.y) < card.y + card.height
            : a.y > card.y &&
              a.y < card.y + card.height &&
              Math.max(a.x, b.x) > card.x &&
              Math.min(a.x, b.x) < card.x + card.width;
        expect(
          crosses,
          `${edge.relation} ${edge.from}→${edge.to} crosses ${id}`,
        ).toBe(false);
      }
    }
  }
  const waitEdge = evidenceEdges(nodes).find(
    (edge) => edge.relation === "awaits terminal outcome",
  )!;
  const registerEdge = evidenceEdges(nodes).find(
    (edge) => edge.to === child.id && edge.relation === "registers",
  )!;
  expect(graphEdgeRoute(waitEdge, layout.positions)![0]!.y).not.toBe(
    graphEdgeRoute(registerEdge, layout.positions)!.at(-1)!.y,
  );
});

it("bounds completed timelines by their actual evidence rather than later fetch time", () => {
  const rows = timelineRows(fixtureNodes());
  expect(timelineBounds(rows, 999999999)).toEqual({ start: 1000, end: 1150 });
  const active = fixtureNodes();
  const child = active.find((node) => node.kind === "child")!;
  if (child.kind !== "child") throw Error("fixture");
  child.terminal_at = null;
  child.state = "running";
  expect(timelineBounds(timelineRows(active), 5000)).toEqual({
    start: 1000,
    end: 5000,
  });
});

it("does not turn an unavailable child's unknown end into a running interval", () => {
  const nodes = fixtureNodes();
  const child = nodes.find((node) => node.kind === "child")!;
  if (child.kind !== "child") throw Error("fixture");
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

it("orders timeline rows by time with deterministic phase-first ties", () => {
  const nodes = fixtureNodes().reverse();
  const rows = timelineRows(nodes);
  expect(rows[0]?.node.kind).toBe("phase");
  expect(rows.map((row) => row.timing.start)).toEqual(
    [...rows.map((row) => row.timing.start)].sort((a, b) => a - b),
  );
  expect(timelineRows([...nodes].reverse()).map((row) => row.node.id)).toEqual(
    rows.map((row) => row.node.id),
  );
});

it("keeps local tests within the parent phase and outside distributed fork membership", () => {
  const nodes = fixtureNodes();
  const edges = evidenceEdges(nodes);
  expect(edges).toEqual([
    {
      from: "phase:phase_validate",
      to: "fork:validate",
      relation: "registers",
    },
    { from: "fork:validate", to: "child:review", relation: "registers" },
    {
      from: "child:review",
      to: "join:validate",
      relation: "awaits terminal outcome",
    },
    { from: "join:validate", to: "phase:phase_collect", relation: "resumes" },
  ]);
  expect(phaseGroups(nodes)[0]?.nodes.some((n) => n.id === "local:tests")).toBe(
    true,
  );
  const layout = graphLayout(phaseGroups(nodes), []);
  expect(layout.positions.get("local:tests")?.x).toBe(
    layout.positions.get("phase:phase_validate")?.x,
  );
  expect(layout.positions.get("child:review")?.x).toBeGreaterThan(
    layout.positions.get("local:tests")!.x,
  );
});
it("never invents edges across partial evidence, timestamps, or phase containment", () => {
  const nodes = fixtureNodes().filter(
    (n) => n.kind !== "fork" && n.kind !== "child_wait",
  );
  expect(evidenceEdges(nodes)).toEqual([]);
});
it("only directly resumes from a successfully applied continue decision", () => {
  const first = phase();
  if (first.kind !== "phase") throw Error("fixture");
  first.decision_kind = "continue";
  const nodes = [first, phase("phase_collect", "collect")];
  expect(evidenceEdges(nodes)).toContainEqual({
    from: first.id,
    to: "phase:phase_collect",
    relation: "resumes",
  });
  first.applied_at = null;
  expect(evidenceEdges(nodes)).toEqual([]);
  first.applied_at = 1200;
  first.error = { kind: "invalid_decision", message: "rejected" };
  expect(evidenceEdges(nodes)).toEqual([]);
});
it("joins await failed children without relabeling their outcome as success", () => {
  const nodes = fixtureNodes();
  const child = nodes.find((n) => n.kind === "child")!;
  if (child.kind !== "child") throw Error("fixture");
  child.state = "failed";
  expect(nodeStatus(child)).toBe("failed");
  expect(
    evidenceEdges(nodes).some((e) => e.relation === "awaits terminal outcome"),
  ).toBe(true);
});
it("local acceptance is a milestone and absence of accepted output is not 'not started'", () => {
  const local = fixtureNodes().find((n) => n.kind === "local")!;
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
it("keeps the same logical local identity for replay", () => {
  const local = fixtureNodes().find((n) => n.kind === "local")!;
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
});
it("closed external waits do not imply successful wake", () => {
  const node = explorerNode({
    kind: "external_wait",
    id: "wait:approval",
    activation_id: "phase_validate",
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
  expect(nodeStatus(node)).toBe("closed · reason unavailable");
});
it("rejects contradictory wake or applied-decision evidence", () => {
  expect(() =>
    explorerNode({
      ...phase(),
      decision_kind: "continue",
      error: { kind: "rejected", message: "invalid" },
    }),
  ).toThrow("evidence");
  expect(() =>
    explorerNode({ ...phase(), extra: "future-contract-field" }),
  ).toThrow();
});
it("collapses phases without adding child dependencies", () => {
  const layout = graphLayout(phaseGroups(fixtureNodes()), ["phase_validate"]);
  expect(layout.positions.has("local:tests")).toBe(false);
  expect(layout.positions.has("phase:phase_validate")).toBe(true);
});
it("uses callable names and keeps exact large measurements", () => {
  expect(nodeLabel(fixtureNodes().find((n) => n.kind === "local")!)).toBe(
    "tests:0",
  );
  expect(micros("18446744073709551615")).toBe("18446744073709551.615 ms");
});
it("program history includes child-only programs and pins exact versions", () => {
  expect(programHistory("review", "1.0.0")).toBe(
    "/executions?program_id=review&include_children=true&version=1.0.0",
  );
});
