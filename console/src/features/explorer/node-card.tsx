// SPDX-License-Identifier: MIT
import { Handle, Position } from "@xyflow/react";
import {
  ArrowRight,
  Box,
  Check,
  Circle,
  Clock3,
  Code2,
  GitFork,
  GitMerge,
  LogIn,
  Radio,
  ShieldCheck,
  Workflow,
  X,
} from "lucide-react";
import type { ExplorerNode } from "../../api/explorer";
import { micros, nodeLabel, nodeStatus, nodeType } from "../explorer-model";

function statusAppearance(status: string) {
  if (["succeeded", "accepted", "returned", "replayed"].includes(status))
    return { Icon: Check, tone: "success" };
  if (["failed", "failing", "decision rejected"].includes(status))
    return { Icon: X, tone: "failure" };
  if (["running", "active", "waiting", "wait registered"].includes(status))
    return { Icon: Clock3, tone: "waiting" };
  if (status.includes("scheduled"))
    return { Icon: ArrowRight, tone: "neutral" };
  return { Icon: Circle, tone: "neutral" };
}

function recordedDuration(node: ExplorerNode) {
  if (node.kind === "local" && node.observation)
    return {
      text:
        node.observation.state === "replayed"
          ? "Replay"
          : micros(node.observation.elapsed_us),
      description:
        node.observation.state === "replayed"
          ? "Replay observation; the callable did not run"
          : "Observed callable duration; durable acceptance is separate",
    };
  if (node.kind !== "child" || node.terminal_at === null) return null;
  const ms = Math.max(0, node.terminal_at - node.submitted_at);
  const text =
    ms < 1000
      ? `${ms} ms`
      : ms < 60000
        ? `${Number((ms / 1000).toFixed(1))} s`
        : ms < 3600000
          ? `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`
          : `${Math.floor(ms / 3600000)}h ${Math.floor((ms % 3600000) / 60000)}m`;
  return {
    text,
    description:
      "Elapsed from submission to terminal outcome, including queue and wait time",
  };
}

export function WorkCard({ node }: { node: ExplorerNode }) {
  const label = nodeLabel(node);
  const status = nodeStatus(node);
  const { Icon: StatusIcon, tone } = statusAppearance(status);
  const gate = node.kind === "fork" || node.kind === "child_wait";
  const compact =
    gate || node.kind === "entrypoint" || node.kind === "external_wait";
  const Icon =
    node.kind === "entrypoint"
      ? LogIn
      : node.kind === "fork"
        ? GitFork
        : node.kind === "child_wait"
          ? GitMerge
          : node.kind === "external_wait"
            ? node.wait_kind === "event"
              ? Radio
              : node.wait_kind === "approval"
                ? ShieldCheck
                : Clock3
            : node.kind === "local"
              ? Code2
              : node.execution.kind === "workflow"
                ? Workflow
                : Box;
  const duration = recordedDuration(node);
  const count =
    node.kind === "fork"
      ? node.branch_keys.length
      : node.kind === "child_wait"
        ? node.member_keys.length
        : 0;
  const gateName =
    node.kind === "fork"
      ? node.key
      : node.kind === "child_wait"
        ? node.resume
        : label;
  return (
    <div
      className={`work-card work-card-${node.kind}${compact ? " work-card-compact" : ""}${gate ? " work-card-gate" : ""}`}
      title={gate ? `${label} · ${status}` : undefined}
      data-status={tone}
    >
      <Handle type="target" position={Position.Top} isConnectable={false} />
      <div className="work-card-main">
        <span className="work-card-icon">
          <Icon size={17} aria-hidden="true" />
        </span>
        <div className="work-card-content">
          {!gate && (
            <div className="work-card-kind">
              <span>{nodeType(node)}</span>
              {node.kind === "entrypoint" && (
                <span className="work-card-revision">rev {node.revision}</span>
              )}
            </div>
          )}
          <strong title={label}>{gate ? gateName : label}</strong>
          {gate && (
            <div className="work-card-kind work-card-gate-caption">
              {node.kind === "fork" ? "Fork" : "Join"} · {count}{" "}
              {node.kind === "fork"
                ? count === 1
                  ? "branch"
                  : "branches"
                : count === 1
                  ? "result"
                  : "results"}
            </div>
          )}
        </div>
        {compact && (
          <span
            className={`work-card-status-mark tone-${tone}`}
            title={status}
            aria-label={status}
          >
            <StatusIcon size={13} aria-hidden="true" />
          </span>
        )}
      </div>
      {!compact && (
        <div className="work-card-footer">
          <span className={`work-card-status tone-${tone}`}>
            <StatusIcon size={12} aria-hidden="true" />
            {status}
          </span>
          {duration && (
            <span className="work-card-duration" title={duration.description}>
              {duration.text}
            </span>
          )}
        </div>
      )}
      <Handle type="source" position={Position.Bottom} isConnectable={false} />
    </div>
  );
}
