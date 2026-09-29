// SPDX-License-Identifier: MIT
import dagre from "@dagrejs/dagre";
import { intersects, nodeSize, type Box, type Point } from "./layout";

type Link = { from: string; to: string; id: string };
// Layout sees IDs and geometry only, never execution payloads or timestamps.
export function arrangeGraph(
  ids: string[],
  edges: Link[],
): Record<string, Point> {
  const graph = new dagre.graphlib.Graph({ multigraph: true });
  graph.setGraph({
    rankdir: "TB",
    ranksep: 76,
    nodesep: 38,
    edgesep: 24,
    marginx: 32,
    marginy: 32,
  });
  graph.setDefaultEdgeLabel(() => ({}));
  for (const id of [...ids].sort()) graph.setNode(id, { ...nodeSize });
  for (const edge of [...edges].sort((a, b) => a.id.localeCompare(b.id)))
    if (graph.hasNode(edge.from) && graph.hasNode(edge.to))
      graph.setEdge(edge.from, edge.to, {}, edge.id);
  dagre.layout(graph);
  return Object.fromEntries(
    ids.map((id) => {
      const node = graph.node(id) as Box;
      return [
        id,
        { x: node.x - nodeSize.width / 2, y: node.y - nodeSize.height / 2 },
      ];
    }),
  );
}

// Preserve every loaded position, including user adjustments. New work can be
// placed without moving existing work; only explicit Reorganize does a full pass.
export function reconcilePositions(
  ids: string[],
  edges: Link[],
  previous: Record<string, Point>,
) {
  const loaded = ids.filter((id) => Object.hasOwn(previous, id));
  if (loaded.length === ids.length)
    return Object.fromEntries(ids.map((id) => [id, previous[id]!])) as Record<
      string,
      Point
    >;
  const proposed = arrangeGraph(ids, edges);
  if (!loaded.length) return proposed;
  const result: Record<string, Point> = Object.fromEntries(
    loaded.map((id) => [id, previous[id]!]),
  );
  for (const id of [...ids].sort()) {
    if (Object.hasOwn(result, id)) continue;
    const parent = edges.find(
      (edge) => edge.to === id && Object.hasOwn(result, edge.from),
    );
    let position: Point = parent
      ? {
          x: result[parent.from]!.x,
          y: result[parent.from]!.y + nodeSize.height + 76,
        }
      : proposed[id]!;
    // Bounded loaded pages: each step passes at least one occupied row.
    while (
      Object.values(result).some((p) =>
        intersects({ ...position, ...nodeSize }, { ...p, ...nodeSize }, 28),
      )
    )
      position = { x: position.x + nodeSize.width + 38, y: position.y };
    result[id] = position;
  }
  return result;
}
