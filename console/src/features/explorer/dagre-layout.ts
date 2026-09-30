// SPDX-License-Identifier: MIT
import dagre from "@dagrejs/dagre";
import {
  intersects,
  nodeSize,
  type Box,
  type NodeDimensions,
  type Point,
} from "./layout";

type Link = { from: string; to: string; id: string };
// Layout sees IDs and geometry only, never execution payloads or timestamps.
export function arrangeGraph(
  ids: string[],
  edges: Link[],
  sizes?: NodeDimensions,
): Record<string, Point> {
  const graph = new dagre.graphlib.Graph({ multigraph: true });
  graph.setGraph({
    rankdir: "TB",
    ranksep: 52,
    nodesep: 36,
    edgesep: 24,
    marginx: 32,
    marginy: 32,
  });
  graph.setDefaultEdgeLabel(() => ({}));
  for (const id of [...ids].sort())
    graph.setNode(id, { ...(sizes?.[id] ?? nodeSize) });
  for (const edge of [...edges].sort((a, b) => a.id.localeCompare(b.id)))
    if (graph.hasNode(edge.from) && graph.hasNode(edge.to))
      graph.setEdge(edge.from, edge.to, {}, edge.id);
  dagre.layout(graph);
  return Object.fromEntries(
    ids.map((id) => {
      const node = graph.node(id) as Box;
      return [id, { x: node.x - node.width / 2, y: node.y - node.height / 2 }];
    }),
  );
}

// Preserve every loaded position, including user adjustments. New work can be
// placed without moving existing work; only explicit Reorganize does a full pass.
export function reconcilePositions(
  ids: string[],
  edges: Link[],
  previous: Record<string, Point>,
  sizes?: NodeDimensions,
) {
  const loaded = ids.filter((id) => Object.hasOwn(previous, id));
  if (loaded.length === ids.length)
    return Object.fromEntries(ids.map((id) => [id, previous[id]!])) as Record<
      string,
      Point
    >;
  const proposed = arrangeGraph(ids, edges, sizes);
  if (!loaded.length) return proposed;
  const result: Record<string, Point> = Object.fromEntries(
    loaded.map((id) => [id, previous[id]!]),
  );
  const orderedEdges = [...edges].sort((a, b) => a.id.localeCompare(b.id));
  for (const id of [...ids].sort()) {
    if (Object.hasOwn(result, id)) continue;
    const size = sizes?.[id] ?? nodeSize;
    const parent = orderedEdges.find(
      (edge) => edge.to === id && Object.hasOwn(result, edge.from),
    );
    const parentSize = parent ? (sizes?.[parent.from] ?? nodeSize) : nodeSize;
    let position: Point = parent
      ? {
          x: result[parent.from]!.x + (parentSize.width - size.width) / 2,
          y: result[parent.from]!.y + parentSize.height + 52,
        }
      : proposed[id]!;
    // Bounded loaded pages: each step passes at least one occupied row.
    while (
      Object.entries(result).some(([other, p]) =>
        intersects(
          { ...position, ...size },
          { ...p, ...(sizes?.[other] ?? nodeSize) },
          28,
        ),
      )
    )
      position = { x: position.x + size.width + 36, y: position.y };
    result[id] = position;
  }
  return result;
}
