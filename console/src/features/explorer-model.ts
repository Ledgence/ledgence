// SPDX-License-Identifier: MIT
import type {
  ExplorerNode,
  ExplorerReference,
  ExplorerRelation,
} from "../api/explorer";
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
  kind: ExplorerRelation["kind"];
  relation: string;
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
      return `${node.wait_kind === "event" ? "Event" : node.wait_kind === "approval" ? "Approval" : "Timer"} · ${node.key}`;
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

// These keys only resolve explicit typed references; they never establish edges.
export function referenceIdentity(reference: ExplorerReference): string {
  switch (reference.kind) {
    case "entrypoint":
    case "child_wait":
      return JSON.stringify([reference.kind, reference.activation_id]);
    case "local":
      return JSON.stringify([
        reference.kind,
        reference.activation_id,
        reference.key,
      ]);
    case "child":
    case "fork":
    case "external_wait":
      return JSON.stringify([reference.kind, reference.key]);
  }
}
export function referenceLabel(reference: ExplorerReference): string {
  switch (reference.kind) {
    case "entrypoint":
      return `Entrypoint · ${reference.activation_id}`;
    case "child_wait":
      return `Join · ${reference.activation_id}`;
    case "local":
      return `Local step · ${reference.key} · ${reference.activation_id}`;
    case "child":
      return `Child · ${reference.key}`;
    case "fork":
      return `Fork · ${reference.key}`;
    case "external_wait":
      return `External wait · ${reference.key}`;
  }
}
const relationLabels: Record<ExplorerRelation["kind"], string> = {
  invokes: "invokes",
  registers: "registers",
  branch: "includes branch",
  awaits_terminal: "terminal outcome awaited by",
  resumes: "resumes",
};
export type RecordedRelation = {
  record: ExplorerRelation;
  relation: string;
  source: ExplorerNode | null;
  target: ExplorerNode | null;
  evidenceIds: string[];
};
export function recordedRelations(nodes: ExplorerNode[]): RecordedRelation[] {
  const loaded = new Map(nodes.map((node) => [referenceIdentity(node), node]));
  const records = new Map<string, RecordedRelation>();
  for (const node of nodes) {
    for (const record of node.relations) {
      const previous = records.get(record.id);
      if (previous) {
        if (!previous.evidenceIds.includes(node.id))
          previous.evidenceIds.push(node.id);
        continue;
      }
      records.set(record.id, {
        record,
        relation: relationLabels[record.kind],
        source: loaded.get(referenceIdentity(record.source)) ?? null,
        target: loaded.get(referenceIdentity(record.target)) ?? null,
        evidenceIds: [node.id],
      });
    }
  }
  return [...records.values()]
    .map((relation) => ({
      ...relation,
      evidenceIds: relation.evidenceIds.sort(),
    }))
    .sort((a, b) =>
      a.record.id < b.record.id ? -1 : a.record.id > b.record.id ? 1 : 0,
    );
}
export function evidenceEdges(nodes: ExplorerNode[]): EvidenceEdge[] {
  return resolvedEdges(recordedRelations(nodes));
}
export function resolvedEdges(relations: RecordedRelation[]): EvidenceEdge[] {
  return relations.flatMap(
    ({ record, relation, source, target, evidenceIds }) =>
      source && target
        ? [
            {
              id: record.id,
              kind: record.kind,
              from: source.id,
              to: target.id,
              relation,
              style:
                record.kind === "branch"
                  ? ("fork" as const)
                  : ("parent" as const),
              evidenceIds,
            },
          ]
        : [],
  );
}
// Omit a redundant invocation only when its explicit, fully loaded fork path
// is drawn. The invocation remains in recordedRelations and the evidence UI.
export function graphEdges(edges: EvidenceEdge[]): EvidenceEdge[] {
  const registered = new Map<string, Set<string>>();
  const memberships = new Map<string, Set<string>>();
  for (const edge of edges) {
    if (edge.kind === "registers") {
      const targets = registered.get(edge.from) ?? new Set<string>();
      targets.add(edge.to);
      registered.set(edge.from, targets);
    } else if (edge.kind === "branch") {
      const forks = memberships.get(edge.to) ?? new Set<string>();
      forks.add(edge.from);
      memberships.set(edge.to, forks);
    }
  }
  return edges.filter((edge) => {
    if (edge.kind !== "invokes") return true;
    const sources = memberships.get(edge.to);
    if (sources)
      for (const fork of sources)
        if (registered.get(edge.from)?.has(fork)) return false;
    return true;
  });
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
