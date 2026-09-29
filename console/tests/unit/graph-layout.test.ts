// SPDX-License-Identifier: MIT
import { expect, it } from "vitest";
import raw from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v3.json?raw";
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
  intersects,
  type Point,
} from "../../src/features/explorer/layout";

const fixture = parseUserJson(raw, 2 * 1024 * 1024) as Record<string, unknown>;
const nodes = workflowExplorer(fixture.explorer).page.items;
const edges = evidenceEdges(nodes);
const ids = nodes.map((node) => node.id);
function expectNoOverlaps(positions: Record<string, Point>) {
  const items = Object.values(positions);
  for (let i = 0; i < items.length; i++)
    for (let j = i + 1; j < items.length; j++)
      expect(
        intersects(
          { ...items[i]!, ...nodeSize },
          { ...items[j]!, ...nodeSize },
        ),
      ).toBe(false);
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
