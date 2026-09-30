// SPDX-License-Identifier: MIT
import { expect, it } from "vitest";
import raw from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v4.json?raw";
import { parseUserJson } from "../../src/api/json";
import { workflowExplorer } from "../../src/api/explorer";
import { evidenceEdges } from "../../src/features/explorer-model";
import {
  arrangeGraph,
  reconcilePositions,
} from "../../src/features/explorer/dagre-layout";
import { crossesBox, routeEdge } from "../../src/features/explorer/routing";
import {
  nodeSize,
  graphNodeSize,
  intersects,
  type NodeDimensions,
  type Point,
} from "../../src/features/explorer/layout";

const fixture = parseUserJson(raw, 2 * 1024 * 1024) as Record<string, unknown>;
const nodes = workflowExplorer(fixture.explorer).page.items;
const edges = evidenceEdges(nodes);
const ids = nodes.map((node) => node.id);
function expectNoOverlaps(
  positions: Record<string, Point>,
  sizes?: NodeDimensions,
) {
  const items = Object.entries(positions).map(([id, position]) => ({
    ...position,
    ...(sizes?.[id] ?? nodeSize),
  }));
  for (let i = 0; i < items.length; i++)
    for (let j = i + 1; j < items.length; j++)
      expect(intersects(items[i]!, items[j]!)).toBe(false);
}
it("lays out the canonical four branches, parent task and entrypoints deterministically", () => {
  const positions = arrangeGraph(ids, edges);
  expect(Object.keys(positions)).toHaveLength(nodes.length);
  expectNoOverlaps(positions);
  expect(arrangeGraph([...ids].reverse(), [...edges].reverse())).toEqual(
    positions,
  );
  for (const edge of edges)
    expect(positions[edge.to]!.y).toBeGreaterThan(positions[edge.from]!.y);
});
it("routes each canonical relation around unrelated cards", () => {
  const positions = arrangeGraph(ids, edges);
  for (const [lane, edge] of edges.entries()) {
    const route = routeEdge(edge.from, edge.to, positions, lane)!;
    expect(route.obstructed, edge.id).toBe(false);
    for (const [id, point] of Object.entries(positions)) {
      if (id === edge.from || id === edge.to) continue;
      for (let i = 1; i < route.points.length; i++)
        expect(
          crossesBox(route.points[i - 1]!, route.points[i]!, {
            ...point,
            ...nodeSize,
          }),
          `${edge.id} crosses ${id}`,
        ).toBe(false);
    }
  }
});
it("preserves all existing positions on status updates and topological growth", () => {
  const oldIds = ids.slice(0, 7);
  const previous = arrangeGraph(oldIds, edges);
  previous[oldIds[0]!] = { x: 1800, y: 240 };
  expect(reconcilePositions(oldIds, edges, previous)).toEqual(previous);
  const grown = reconcilePositions(ids, edges, previous);
  for (const id of oldIds) expect(grown[id]).toEqual(previous[id]);
  expectNoOverlaps(grown);
});
it("supports bounded large synthetic pages without dropping disconnected records", () => {
  const syntheticIds = Array.from({ length: 100 }, (_, i) => `synthetic:${i}`);
  const links = syntheticIds.slice(1, 81).map((id, i) => ({
    id: `edge:${i}`,
    from: syntheticIds[Math.floor(i / 4)]!,
    to: id,
  }));
  const positions = arrangeGraph(syntheticIds, links);
  expect(Object.keys(positions)).toHaveLength(100);
  expectNoOverlaps(positions);
});
it("routes a manually moved obstacle without a diagonal fallback", () => {
  const positions = {
    source: { x: 200, y: 0 },
    obstacle: { x: 220, y: 180 },
    target: { x: 200, y: 400 },
  };
  const route = routeEdge("source", "target", positions)!;
  expect(route.obstructed).toBe(false);
  for (let i = 1; i < route.points.length; i++)
    expect(
      crossesBox(route.points[i - 1]!, route.points[i]!, {
        ...positions.obstacle,
        ...nodeSize,
      }),
    ).toBe(false);
});

it("lays out and routes mixed-size canonical work with real card boundaries", () => {
  const sizes = Object.fromEntries(
    nodes.map((node) => [node.id, graphNodeSize(node)]),
  );
  const positions = arrangeGraph(ids, edges, sizes);
  expectNoOverlaps(positions, sizes);
  expect(arrangeGraph([...ids].reverse(), [...edges].reverse(), sizes)).toEqual(
    positions,
  );
  for (const [lane, edge] of edges.entries()) {
    const route = routeEdge(edge.from, edge.to, positions, lane, sizes)!;
    expect(route.obstructed, edge.id).toBe(false);
    expect(route.points[0]).toEqual({
      x: positions[edge.from]!.x + sizes[edge.from]!.width / 2,
      y: positions[edge.from]!.y + sizes[edge.from]!.height,
    });
    expect(route.points.at(-1)).toEqual({
      x: positions[edge.to]!.x + sizes[edge.to]!.width / 2,
      y: positions[edge.to]!.y,
    });
    for (const [id, position] of Object.entries(positions)) {
      if (id === edge.from || id === edge.to) continue;
      for (let i = 1; i < route.points.length; i++)
        expect(
          crossesBox(route.points[i - 1]!, route.points[i]!, {
            ...position,
            ...sizes[id]!,
          }),
          `${edge.id} crosses ${id}`,
        ).toBe(false);
    }
  }
});
it("uses compact ranks and aligns unequal-width nodes by their centers", () => {
  const ids = [
    "entrypoint",
    "local",
    "fork",
    "child",
    "child_wait",
    "external_wait",
  ] as const;
  const sizes = Object.fromEntries(
    ids.map((kind) => [kind, graphNodeSize({ kind })]),
  );
  const edges = ids
    .slice(1)
    .map((to, i) => ({ id: String(i), from: ids[i]!, to }));
  const positions = arrangeGraph([...ids], edges, sizes);
  for (const edge of edges) {
    const parent = positions[edge.from]!,
      child = positions[edge.to]!;
    expect(child.y - parent.y - sizes[edge.from]!.height).toBe(52);
    expect(child.x + sizes[edge.to]!.width / 2).toBe(
      parent.x + sizes[edge.from]!.width / 2,
    );
    const route = routeEdge(edge.from, edge.to, positions, 3, sizes)!;
    expect(route.obstructed).toBe(false);
    for (let i = 1; i < route.points.length; i++)
      expect(route.points[i]!.y).toBeGreaterThanOrEqual(route.points[i - 1]!.y);
  }
});
it("preserves mixed-size positions while placing new work clear of actual occupied bounds", () => {
  const sizes: NodeDimensions = {
    parent: graphNodeSize({ kind: "entrypoint" }),
    obstacle: { width: 540, height: 180 },
    local: graphNodeSize({ kind: "local" }),
    fork: graphNodeSize({ kind: "fork" }),
  };
  const previous = { parent: { x: 300, y: 100 }, obstacle: { x: 80, y: 230 } };
  const links = [
    { id: "local", from: "parent", to: "local" },
    { id: "fork", from: "parent", to: "fork" },
  ];
  const ids = ["parent", "obstacle", "local", "fork"];
  const positions = reconcilePositions(ids, links, previous, sizes);
  expect(positions.parent).toEqual(previous.parent);
  expect(positions.obstacle).toEqual(previous.obstacle);
  expectNoOverlaps(positions, sizes);
  expect(
    reconcilePositions(
      [...ids].reverse(),
      [...links].reverse(),
      previous,
      sizes,
    ),
  ).toEqual(positions);
  expect(reconcilePositions(ids, links, positions, sizes)).toEqual(positions);
});
it("routes around an obstacle beyond fallback dimensions and falls back for missing sizes", () => {
  const positions = {
    source: { x: 200, y: 0 },
    obstacle: { x: 0, y: 180 },
    target: { x: 200, y: 440 },
  };
  const sizes: NodeDimensions = {
    source: graphNodeSize({ kind: "entrypoint" }),
    obstacle: { width: 700, height: 180 },
  };
  const route = routeEdge("source", "target", positions, 2, sizes)!;
  expect(route.obstructed).toBe(false);
  expect(route.points[0]).toEqual({ x: 316, y: 64 });
  expect(route.points.at(-1)).toEqual({ x: 200 + nodeSize.width / 2, y: 440 });
  for (let i = 1; i < route.points.length; i++)
    expect(
      crossesBox(route.points[i - 1]!, route.points[i]!, {
        ...positions.obstacle,
        ...sizes.obstacle!,
      }),
    ).toBe(false);
  const arranged = arrangeGraph(
    ["source", "target"],
    [{ id: "edge", from: "source", to: "target" }],
    sizes,
  );
  expect(arranged.target!.x + nodeSize.width / 2).toBe(
    arranged.source!.x + sizes.source!.width / 2,
  );
});
