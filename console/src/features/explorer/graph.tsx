// SPDX-License-Identifier: MIT
import { useEffect, useLayoutEffect, useMemo, useRef } from "react";
import {
  ReactFlow,
  Background,
  BaseEdge,
  Handle,
  Position,
  MarkerType,
  type Node,
  type NodeProps,
  type Edge,
  type EdgeProps,
  type ReactFlowInstance,
} from "@xyflow/react";
import {
  Box,
  GitFork,
  GitMerge,
  Play,
  Timer,
  Workflow,
  ZoomIn,
  ZoomOut,
  Scan,
  LayoutGrid,
} from "lucide-react";
import "@xyflow/react/dist/style.css";
import type { ExplorerNode } from "../../api/explorer";
import { Button } from "../../components/ui/button";
import { Status } from "../../components/resource-ui";
import {
  nodeLabel,
  nodeStatus,
  nodeType,
  type EvidenceEdge,
} from "../explorer-model";
import { arrangeGraph, reconcilePositions } from "./dagre-layout";
import { nodeSize, type GraphPresentation } from "./layout";
import { routeEdge, edgePath } from "./routing";

type WorkNode = Node<{ record: ExplorerNode }, "work">;
type WorkEdge = Edge<
  { path: string; relation: string; obstructed: boolean },
  "evidence"
>;
function WorkCard({ data }: NodeProps<WorkNode>) {
  const node = data.record;
  const Icon =
    node.kind === "entrypoint"
      ? Play
      : node.kind === "fork"
        ? GitFork
        : node.kind === "child_wait"
          ? GitMerge
          : node.kind === "external_wait"
            ? Timer
            : node.kind === "child" && node.execution.kind === "workflow"
              ? Workflow
              : Box;
  return (
    <div className={`work-card work-card-${node.kind}`}>
      <Handle type="target" position={Position.Top} isConnectable={false} />
      <div className="work-card-kind">
        <Icon size={14} aria-hidden="true" />
        <span>{nodeType(node)}</span>
      </div>
      <strong title={nodeLabel(node)}>{nodeLabel(node)}</strong>
      <Status value={nodeStatus(node)} />
      <Handle type="source" position={Position.Bottom} isConnectable={false} />
    </div>
  );
}
function EvidenceLine({
  id,
  data,
  markerEnd,
  style,
  source,
  target,
}: EdgeProps<WorkEdge>) {
  if (!data) return null;
  return (
    <g
      data-edge-id={id}
      data-edge-from={source}
      data-edge-to={target}
      data-obstructed={data.obstructed || undefined}
    >
      <title>{data.relation}</title>
      <BaseEdge
        id={id}
        path={data.path}
        {...(markerEnd ? { markerEnd } : {})}
        style={style}
        interactionWidth={0}
      />
    </g>
  );
}
const nodeTypes = { work: WorkCard };
const edgeTypes = { evidence: EvidenceLine };

export function GraphCanvas({
  nodes,
  edges,
  selectedId,
  select,
  scope,
  presentation,
  save,
  matchingIds,
}: {
  nodes: ExplorerNode[];
  edges: EvidenceEdge[];
  selectedId: string | null;
  select: (id: string) => void;
  scope: string;
  presentation: GraphPresentation | undefined;
  save: (value: GraphPresentation) => void;
  matchingIds?: string[] | undefined;
}) {
  const container = useRef<HTMLDivElement>(null);
  const flow = useRef<ReactFlowInstance<WorkNode, WorkEdge> | null>(null);
  const current = presentation?.scope === scope ? presentation : undefined;
  useLayoutEffect(() => {
    const instance = flow.current;
    const restored = current?.viewport;
    if (!instance || !restored) return;
    const actual = instance.getViewport();
    // defaultViewport applies only on mount. Back/Forward may restore another
    // history entry of the same workflow without remounting this canvas.
    if (
      actual.x !== restored.x ||
      actual.y !== restored.y ||
      actual.zoom !== restored.zoom
    )
      void instance.setViewport(restored);
  }, [current?.viewport]);
  const positions = useMemo(
    () =>
      reconcilePositions(
        nodes.map((n) => n.id),
        edges,
        current?.positions ?? {},
      ),
    [nodes, edges, current?.positions],
  );
  useEffect(() => {
    // Record incremental positions as soon as they are observed, before the
    // next poll can reorder new nodes. Waiting for a user pan loses this state.
    if (
      current &&
      Object.keys(current.positions).length === nodes.length &&
      nodes.every((node) => {
        const before = current.positions[node.id],
          after = positions[node.id];
        return before && after && before.x === after.x && before.y === after.y;
      })
    )
      return;
    const viewport = flow.current?.getViewport() ?? current?.viewport;
    save({ scope, positions, ...(viewport ? { viewport } : {}) });
  }, [current, nodes, positions, scope, save]);
  const renderedNodes: WorkNode[] = nodes.map((record) => ({
    id: record.id,
    type: "work",
    position: positions[record.id]!,
    data: { record },
    width: nodeSize.width,
    height: nodeSize.height,
    // Fixed-size cards must retain their measurement across controlled updates.
    // Otherwise React Flow discards measured handle bounds on a new node object
    // even though its DOM size has not changed enough to notify ResizeObserver.
    measured: nodeSize,
    selected: record.id === selectedId,
    ariaRole: "button",
    ariaLabel: `${nodeLabel(record)} · ${nodeType(record)} · ${nodeStatus(record)}`,
    domAttributes: {
      "aria-pressed": record.id === selectedId,
      "data-focus-key": `node:${record.id}`,
      "data-position-x": positions[record.id]!.x,
      "data-position-y": positions[record.id]!.y,
    } as NonNullable<WorkNode["domAttributes"]>,
    className: `graph-node${matchingIds && !matchingIds.includes(record.id) ? " graph-node-dimmed" : ""}`,
    deletable: false,
    connectable: false,
  }));
  const renderedEdges: WorkEdge[] = useMemo(
    () =>
      edges.flatMap((edge, index) => {
        const route = routeEdge(edge.from, edge.to, positions, index);
        if (!route) return [];
        return [
          {
            id: edge.id,
            source: edge.from,
            target: edge.to,
            type: "evidence" as const,
            data: {
              path: edgePath(route.points),
              relation: edge.relation,
              obstructed: route.obstructed,
            },
            markerEnd: {
              type: MarkerType.ArrowClosed,
              width: 14,
              height: 14,
              color: "var(--muted)",
            },
            style: {
              stroke: "var(--muted)",
              strokeWidth: 1.5,
              ...(edge.style === "fork" ? { strokeDasharray: "6 5" } : {}),
            },
            selectable: false,
            deletable: false,
            focusable: false,
          },
        ];
      }),
    [edges, positions],
  );
  function persist(next = positions) {
    const viewport = flow.current?.getViewport() ?? current?.viewport;
    save({ scope, positions: next, ...(viewport ? { viewport } : {}) });
  }
  return (
    <div className="workflow-flow" ref={container}>
      <ReactFlow<WorkNode, WorkEdge>
        nodes={renderedNodes}
        edges={renderedEdges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        {...(current?.viewport ? { defaultViewport: current.viewport } : {})}
        onInit={(instance) => {
          flow.current = instance;
          if (!current?.viewport) {
            const values = Object.values(positions);
            const left = Math.min(...values.map((p) => p.x)),
              right = Math.max(...values.map((p) => p.x + nodeSize.width));
            const top = Math.min(...values.map((p) => p.y));
            const width = Math.max(container.current?.clientWidth || 900, 80);
            // Start at a readable scale near the top. Fit is an explicit action
            // for the whole graph, never a side effect of polling or resizing.
            const tooNarrow = (right - left) * 0.65 > width - 64;
            const root = [...nodes]
              .filter((node) => node.kind === "entrypoint")
              .sort((a, b) => positions[a.id]!.y - positions[b.id]!.y)[0];
            const anchor = root ? positions[root.id]! : { x: left, y: top };
            const zoom = tooNarrow
              ? Math.min(1, (width - 56) / nodeSize.width)
              : Math.max(0.65, Math.min(1, (width - 64) / (right - left)));
            const viewport = {
              x: tooNarrow
                ? (width - nodeSize.width * zoom) / 2 - anchor.x * zoom
                : (width - (right - left) * zoom) / 2 - left * zoom,
              y: 52 - top * zoom,
              zoom,
            };
            void instance.setViewport(viewport);
            save({ scope, positions, viewport });
          }
        }}
        onNodesChange={(changes) => {
          const moved = changes.filter(
            (change) => change.type === "position" && change.position,
          );
          if (moved.length) {
            const next = { ...positions };
            for (const change of moved)
              if (change.type === "position" && change.position)
                next[change.id] = change.position;
            persist(next);
          }
          const selected = changes.find(
            (change) =>
              change.type === "select" &&
              change.selected &&
              change.id !== selectedId,
          );
          if (selected?.type === "select") select(selected.id);
        }}
        onMoveEnd={(_event, viewport) => save({ scope, positions, viewport })}
        nodesConnectable={false}
        edgesReconnectable={false}
        deleteKeyCode={null}
        selectionOnDrag={false}
        multiSelectionKeyCode={null}
        autoPanOnNodeFocus
        minZoom={0.15}
        maxZoom={1.8}
        zoomOnDoubleClick={false}
        proOptions={{ hideAttribution: true }}
        ariaLabelConfig={{
          "node.a11yDescription.default":
            "Press Enter or Space to inspect. Use arrow keys to adjust this card's position. This does not change the execution.",
          "node.a11yDescription.keyboardDisabled":
            "Select a card to inspect its recorded execution.",
        }}
      >
        <Background color="var(--line)" gap={22} size={1} />
      </ReactFlow>
      <div className="graph-tools" role="toolbar" aria-label="Graph navigation">
        <Button
          variant="ghost"
          aria-label="Zoom out"
          title="Zoom out"
          onClick={() => void flow.current?.zoomOut()}
        >
          <ZoomOut />
        </Button>
        <Button
          variant="ghost"
          aria-label="Zoom in"
          title="Zoom in"
          onClick={() => void flow.current?.zoomIn()}
        >
          <ZoomIn />
        </Button>
        <span className="graph-tools-divider" />
        <Button
          variant="ghost"
          onClick={() =>
            void flow.current?.fitView({
              padding: {
                top: "50px",
                bottom: "86px",
                left: "30px",
                right: "30px",
              },
              minZoom: 0.15,
              maxZoom: 1,
            })
          }
        >
          <Scan />
          Fit
        </Button>
        <Button
          variant="ghost"
          title="Restore automatic positions for this page"
          onClick={() =>
            persist(
              arrangeGraph(
                nodes.map((n) => n.id),
                edges,
              ),
            )
          }
        >
          <LayoutGrid />
          Reorganize
        </Button>
      </div>
      <div className="graph-legend" aria-label="Relationship legend">
        <span>
          <i />
          Invocation / registration / wait / resume
        </span>
        <span>
          <i className="fork-line" />
          Branch membership
        </span>
      </div>
      {renderedEdges.some((edge) => edge.data?.obstructed) && (
        <p className="graph-routing-note">
          Cards overlap a connection. Reorganize to restore the automatic
          layout.
        </p>
      )}
    </div>
  );
}
