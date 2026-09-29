// SPDX-License-Identifier: MIT
import type { ExplorerNode } from "../api/explorer";
export function micros(value: string) {
  const n = BigInt(value);
  const whole = n / 1000n;
  const fraction = (n % 1000n).toString().padStart(3, "0").replace(/0+$/, "");
  return `${whole}${fraction ? `.${fraction}` : ""} ms`;
}

export type EvidenceEdge = {
  from: string;
  to: string;
  relation: "registers" | "awaits terminal outcome" | "resumes";
};
export function nodeLabel(node: ExplorerNode): string {
  switch (node.kind) {
    case "phase":
      return node.entrypoint;
    case "child":
      return node.key;
    case "fork":
      return `Fork · ${node.key}`;
    case "local":
      return node.key;
    case "child_wait":
      return `Join · ${node.resume}`;
    case "external_wait":
      return `${node.wait_kind === "event" ? "Event" : "Timer"} · ${node.key}`;
  }
}
export function nodeStatus(node: ExplorerNode): string {
  switch (node.kind) {
    case "phase":
      return node.error ? "decision rejected" : (node.state ?? "unavailable");
    case "child":
      return node.state ?? "unavailable";
    case "fork":
      return "registered";
    case "local":
      return node.accepted_at !== null
        ? "accepted"
        : (node.observation?.state ?? "unknown");
    case "child_wait":
      return node.resumed_activation_id
        ? "resume scheduled"
        : "wait registered";
    case "external_wait":
      return node.wake_reason
        ? `${node.wake_reason} · resume scheduled`
        : node.closed_at !== null
          ? "closed · reason unavailable"
          : "waiting";
  }
}
export function nodeType(node: ExplorerNode): string {
  return node.kind === "phase"
    ? "Controller phase"
    : node.kind === "child"
      ? node.execution.kind === "task"
        ? "Task"
        : "Subworkflow"
      : node.kind === "local"
        ? "Local step"
        : node.kind === "fork"
          ? "Fork"
          : node.kind === "child_wait"
            ? "Join"
            : "External wait";
}

// These are the only relationships the durable projection establishes. In
// particular, phase containment and timestamp order are never dependency edges.
export function evidenceEdges(nodes: ExplorerNode[]): EvidenceEdge[] {
  const edges: EvidenceEdge[] = [];
  const phases = new Map(
    nodes.filter((n) => n.kind === "phase").map((n) => [n.activation_id, n]),
  );
  const children = new Map(
    nodes.filter((n) => n.kind === "child").map((n) => [n.key, n]),
  );
  const forks = new Map(
    nodes.filter((n) => n.kind === "fork").map((n) => [n.key, n]),
  );
  function add(
    from: string | undefined,
    to: string | undefined,
    relation: EvidenceEdge["relation"],
  ) {
    if (
      from &&
      to &&
      !edges.some(
        (edge) =>
          edge.from === from && edge.to === to && edge.relation === relation,
      )
    )
      edges.push({ from, to, relation });
  }
  for (const node of nodes) {
    const phase = phases.get(node.activation_id);
    if (node.kind === "fork") add(phase?.id, node.id, "registers");
    if (node.kind === "external_wait") add(phase?.id, node.id, "registers");
    if (node.kind === "child") {
      const fork = node.fork_key ? forks.get(node.fork_key) : undefined;
      if (fork?.kind === "fork" && fork.branch_keys.includes(node.key))
        add(fork.id, node.id, "registers");
      else if (!node.fork_key && phase?.decision_kind)
        add(phase.id, node.id, "registers");
    }
    if (node.kind === "child_wait") {
      for (const key of node.member_keys)
        add(children.get(key)?.id, node.id, "awaits terminal outcome");
    }
    if (
      (node.kind === "phase" ||
        node.kind === "child_wait" ||
        node.kind === "external_wait") &&
      node.resumed_activation_id
    ) {
      // Suspend/wait are represented by their explicit coordination record;
      // only continue goes directly from one phase to the next.
      if (
        node.kind !== "phase" ||
        (node.decision_kind === "continue" &&
          node.applied_at !== null &&
          !node.error)
      )
        add(node.id, phases.get(node.resumed_activation_id)?.id, "resumes");
    }
  }
  return edges;
}

export type NodeTiming = {
  start: number;
  end: number | null;
  label: string;
  milestone: boolean;
  open?: boolean;
};
export function nodeTiming(node: ExplorerNode): NodeTiming {
  switch (node.kind) {
    case "phase":
    case "child":
      return {
        start: node.submitted_at,
        end: node.terminal_at,
        label: "Submitted to terminal · includes queue and wait time",
        milestone: false,
        open:
          node.terminal_at === null &&
          node.availability === "available" &&
          node.state !== null &&
          !["succeeded", "failed", "cancelled"].includes(node.state),
      };
    case "fork":
      return {
        start: node.accepted_at,
        end: node.accepted_at,
        label: "Registration accepted",
        milestone: true,
      };
    case "local": {
      const observed = node.observation;
      if (observed)
        return {
          start: observed.started_at,
          end: Math.min(
            253402300799999,
            observed.started_at + Number(BigInt(observed.elapsed_us) / 1000n),
          ),
          label:
            observed.state === "replayed"
              ? "Replay observation · callable was not run"
              : "Observed callable interval · acceptance is separate",
          milestone: false,
        };
      if (node.accepted_at === null)
        throw new Error(
          "Local step lacks acceptance and observation evidence.",
        );
      return {
        start: node.accepted_at,
        end: node.accepted_at,
        label: "Durable result accepted · runtime interval not recorded",
        milestone: true,
      };
    }
    case "child_wait":
      return {
        start: node.applied_at,
        end: node.applied_at,
        label: "Join decision applied · closing time not recorded",
        milestone: true,
      };
    case "external_wait":
      return {
        start: node.registered_at,
        end: node.closed_at,
        label:
          "Registered to closed · closure alone does not prove the wake reason",
        milestone: false,
        open: node.closed_at === null,
      };
  }
}

export type PhaseGroup = {
  activationId: string;
  label: string;
  nodes: ExplorerNode[];
};
export function phaseGroups(nodes: ExplorerNode[]): PhaseGroup[] {
  const groups = new Map<string, PhaseGroup>();
  // Server order is a stable revision/kind/key order, not causal sequencing.
  for (const node of nodes) {
    let group = groups.get(node.activation_id);
    if (!group) {
      group = {
        activationId: node.activation_id,
        label: node.entrypoint,
        nodes: [],
      };
      groups.set(node.activation_id, group);
    }
    group.nodes.push(node);
  }
  return [...groups.values()];
}

export type GraphPosition = {
  x: number;
  y: number;
  width: number;
  height: number;
};
export type GraphPoint = { x: number; y: number };
const graphColumns = [
  { x: 24, width: 220 },
  { x: 294, width: 220 },
  { x: 584, width: 220 },
] as const;
function rightGutter(position: GraphPosition) {
  const next = graphColumns.find((column) => column.x > position.x);
  return next
    ? (position.x + position.width + next.x) / 2
    : position.x + position.width + 24;
}
function leftGutter(position: GraphPosition) {
  const previous = graphColumns
    .filter((column) => column.x < position.x)
    .at(-1);
  return previous
    ? (previous.x + previous.width + position.x) / 2
    : position.x - 24;
}
export function graphEdgeRoute(
  edge: EvidenceEdge,
  positions: Map<string, GraphPosition>,
): GraphPoint[] | null {
  const source = positions.get(edge.from);
  const target = positions.get(edge.to);
  if (!source || !target) return null;
  // Cards occupy three fixed columns. Keep vertical travel in the gutters,
  // and reserve the lane above the first card for edges skipping a column.
  // Registration and wait use separate ports, avoiding a misleading shared
  // bidirectional segment when a child is both registered and joined.
  const sourceRatio = edge.relation === "awaits terminal outcome" ? 0.7 : 0.3;
  const targetRatio = edge.relation === "registers" ? 0.3 : 0.7;
  const y1 = source.y + source.height * sourceRatio;
  const y2 = target.y + target.height * targetRatio;
  if (source.x === target.x) {
    const right = source.x + source.width;
    return [
      { x: right, y: y1 },
      { x: rightGutter(source), y: y1 },
      { x: rightGutter(source), y: y2 },
      { x: target.x + target.width, y: y2 },
    ];
  }
  const forward = target.x > source.x;
  const x1 = source.x + (forward ? source.width : 0);
  const x2 = target.x + (forward ? 0 : target.width);
  if (Math.abs(target.x - source.x) > 400) {
    const firstGutter = forward ? rightGutter(source) : leftGutter(source);
    const lastGutter = forward ? leftGutter(target) : rightGutter(target);
    const lane = Math.min(source.y, target.y) - 14;
    return [
      { x: x1, y: y1 },
      { x: firstGutter, y: y1 },
      { x: firstGutter, y: lane },
      { x: lastGutter, y: lane },
      { x: lastGutter, y: y2 },
      { x: x2, y: y2 },
    ];
  }
  const gutter = (x1 + x2) / 2;
  return [
    { x: x1, y: y1 },
    { x: gutter, y: y1 },
    { x: gutter, y: y2 },
    { x: x2, y: y2 },
  ];
}

export function timelineRows(nodes: ExplorerNode[]) {
  return nodes
    .map((node) => ({ node, timing: nodeTiming(node) }))
    .sort(
      (a, b) =>
        a.timing.start - b.timing.start ||
        Number(b.node.kind === "phase") - Number(a.node.kind === "phase") ||
        (a.node.id < b.node.id ? -1 : a.node.id > b.node.id ? 1 : 0),
    );
}
export function timelineBounds(
  rows: ReturnType<typeof timelineRows>,
  observedAt: number,
  workflowActive = true,
) {
  if (!rows.length) return { start: observedAt, end: observedAt + 1 };
  const start = Math.min(...rows.map((row) => row.timing.start));
  // An observation made after completion must not stretch a completed run.
  const end = Math.max(
    start + 1,
    ...rows.map(
      (row) =>
        row.timing.end ??
        (row.timing.open && workflowActive ? observedAt : row.timing.start),
    ),
  );
  return { start, end };
}

export function graphLayout(groups: PhaseGroup[], collapsed: string[]) {
  const positions = new Map<string, GraphPosition>();
  const areas: { group: PhaseGroup; y: number; height: number }[] = [];
  let top = 24;
  for (const group of groups) {
    const visible = !collapsed.includes(group.activationId);
    const phase = group.nodes.find((node) => node.kind === "phase");
    if (phase)
      positions.set(phase.id, { x: 24, y: top + 45, width: 220, height: 98 });
    const locals = group.nodes.filter((node) => node.kind === "local");
    const gateOrder = { fork: 0, external_wait: 1, child_wait: 2 };
    const gates = group.nodes
      .filter(
        (node) =>
          node.kind === "fork" ||
          node.kind === "external_wait" ||
          node.kind === "child_wait",
      )
      .sort((a, b) => gateOrder[a.kind] - gateOrder[b.kind]);
    const children = group.nodes.filter((node) => node.kind === "child");
    if (visible) {
      locals.forEach((node, i) =>
        positions.set(node.id, {
          x: 24,
          y: top + 178 + i * 118,
          width: 220,
          height: 98,
        }),
      );
      gates.forEach((node, i) =>
        positions.set(node.id, {
          x: 294,
          y: top + 45 + i * 118,
          width: 220,
          height: 98,
        }),
      );
      children.forEach((node, i) =>
        positions.set(node.id, {
          x: 584,
          y: top + 45 + i * 118,
          width: 220,
          height: 98,
        }),
      );
    }
    const height = visible
      ? Math.max(
          184 + locals.length * 118,
          70 + gates.length * 118,
          70 + children.length * 118,
        )
      : 164;
    areas.push({ group, y: top, height });
    top += height + 32;
  }
  return { positions, areas, width: 832, height: top };
}
