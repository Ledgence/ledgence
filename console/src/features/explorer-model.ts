// SPDX-License-Identifier: MIT
import type { ExplorerNode } from "../api/explorer";
export function micros(value: string) {
  const n = BigInt(value);
  const whole = n / 1000n;
  const fraction = (n % 1000n).toString().padStart(3, "0").replace(/0+$/, "");
  return `${whole}${fraction ? `.${fraction}` : ""} ms`;
}

export type EvidenceEdge = {
  id: string;
  from: string;
  to: string;
  relation: "registers" | "awaits terminal outcome" | "resumes";
  style: "parent" | "fork";
  /** Retained record IDs that justify this relationship, never inferred order. */
  evidenceIds: string[];
};
export function nodeLabel(node: ExplorerNode): string {
  switch (node.kind) {
    case "entrypoint":
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
    case "entrypoint":
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
  return node.kind === "entrypoint"
    ? "Entrypoint"
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
// particular, activation ownership and timestamp order are not causal edges.
export function evidenceEdges(nodes: ExplorerNode[]): EvidenceEdge[] {
  const edges = new Map<string, EvidenceEdge>();
  const entrypoints = new Map(
    nodes
      .filter((n) => n.kind === "entrypoint")
      .map((n) => [n.activation_id, n]),
  );
  const children = new Map(
    nodes.filter((n) => n.kind === "child").map((n) => [n.key, n]),
  );
  const forks = new Map(
    nodes.filter((n) => n.kind === "fork").map((n) => [n.key, n]),
  );
  function add(
    from: ExplorerNode | undefined,
    to: ExplorerNode | undefined,
    relation: EvidenceEdge["relation"],
    style: EvidenceEdge["style"] = "parent",
  ) {
    if (!from || !to) return;
    // Tuple encoding avoids ambiguity when opaque record IDs contain separators.
    const id = JSON.stringify([from.id, to.id, relation]);
    edges.set(id, {
      id,
      from: from.id,
      to: to.id,
      relation,
      style,
      evidenceIds: [from.id, to.id],
    });
  }
  for (const node of nodes) {
    const entrypoint = entrypoints.get(node.activation_id);
    const applied =
      entrypoint?.applied_at !== null &&
      Boolean(entrypoint?.decision_kind) &&
      !entrypoint?.error;
    if (node.kind === "fork") add(entrypoint, node, "registers");
    if (node.kind === "external_wait") add(entrypoint, node, "registers");
    if (node.kind === "child") {
      const fork = node.fork_key ? forks.get(node.fork_key) : undefined;
      if (fork?.branch_keys.includes(node.key))
        add(fork, node, "registers", "fork");
      else if (!node.fork_key && applied) add(entrypoint, node, "registers");
    }
    if (node.kind === "child_wait") {
      if (applied) add(entrypoint, node, "registers");
      for (const key of node.member_keys) {
        const child = children.get(key);
        // The child's retained fork_key identifies branch outcomes even when
        // the fork registration lies on an unloaded page.
        add(
          child,
          node,
          "awaits terminal outcome",
          child?.fork_key ? "fork" : "parent",
        );
      }
      if (node.resumed_activation_id)
        add(node, entrypoints.get(node.resumed_activation_id), "resumes");
    }
    if (
      node.kind === "external_wait" &&
      node.wake_reason !== null &&
      node.closed_at !== null &&
      node.resumed_activation_id
    )
      add(node, entrypoints.get(node.resumed_activation_id), "resumes");
    // Suspend/wait resume through explicit coordination evidence, never a
    // shortcut directly from one entrypoint to the next.
    if (
      node.kind === "entrypoint" &&
      node.decision_kind === "continue" &&
      node.applied_at !== null &&
      !node.error &&
      node.resumed_activation_id
    )
      add(node, entrypoints.get(node.resumed_activation_id), "resumes");
  }
  return [...edges.values()].sort((a, b) =>
    a.id < b.id ? -1 : a.id > b.id ? 1 : 0,
  );
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
    case "entrypoint":
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

export function timelineRows(nodes: ExplorerNode[]) {
  return nodes
    .map((node) => ({ node, timing: nodeTiming(node) }))
    .sort(
      (a, b) =>
        a.timing.start - b.timing.start ||
        Number(b.node.kind === "entrypoint") -
          Number(a.node.kind === "entrypoint") ||
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
