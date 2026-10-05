// SPDX-License-Identifier: MIT
import { useState } from "react";
import { Link, useLocation, useSearchParams } from "react-router";
import { ArrowUpRight } from "lucide-react";
import { executionPath, type ExplorerNode } from "../../api/explorer";
import {
  Field,
  Fields,
  JsonView,
  Status,
  When,
} from "../../components/resource-ui";
import { AttemptResources } from "../execution-resources";
import { detailDestination } from "../detail-navigation";
import {
  micros,
  nodeLabel,
  nodeType,
  nodeStatus,
  nodeTiming,
  type RecordedRelation,
} from "../explorer-model";
import { RelationList } from "./relations";
export function NodeInspector({
  node,
  workflowId,
  relations,
  select,
}: {
  node: ExplorerNode;
  workflowId: string;
  relations: RecordedRelation[];
  select: (id: string) => void;
}) {
  const location = useLocation();
  const [params] = useSearchParams();
  const timing = nodeTiming(node);
  return (
    <>
      <div className="explorer-node-heading">
        <p className="eyebrow">{nodeType(node)}</p>
        <h2>{nodeLabel(node)}</h2>
        <Status value={nodeStatus(node)} />
      </div>
      {node.kind === "child" &&
        (node.availability === "available" ? (
          <Link
            className="button button-primary"
            to={executionPath(node.execution.kind, node.execution.id)}
          >
            {node.execution.kind === "workflow" ? "Open workflow" : "Open task"}{" "}
            <ArrowUpRight aria-hidden="true" />
          </Link>
        ) : (
          <p className="notice">
            This execution is referenced by retained history, but its details
            are unavailable. The removal reason is not recorded.
          </p>
        ))}
      <Fields>
        <Field label="Entrypoint">{node.entrypoint}</Field>
        <Field label="Activation">{node.activation_id}</Field>
        <Field label="Revision">{node.revision}</Field>
        {"key" in node && <Field label="Key">{node.key}</Field>}
        <Field
          label={timing.milestone ? "Recorded milestone" : "Recorded start"}
        >
          <When value={timing.start} />
        </Field>
        {!timing.milestone && (
          <Field label="Recorded end">
            <When value={timing.end} />
          </Field>
        )}
      </Fields>
      <p className="muted">{timing.label}.</p>
      {node.kind === "entrypoint" && (
        <>
          <Fields>
            <Field label="Applied decision">
              {node.decision_kind ??
                "No successfully applied decision recorded"}
            </Field>
          </Fields>
          {node.error && (
            <p className="notice error-notice">
              {node.error.kind}: {node.error.message}
            </p>
          )}
          <Link
            to={`${executionPath("task", node.activation_id)}?tab=Attempts`}
          >
            Inspect controller attempts
          </Link>
          {node.resumed_activation_id && (
            <p>
              Resume scheduled: <code>{node.resumed_activation_id}</code>.
              Scheduling does not prove the handler started.
            </p>
          )}
        </>
      )}
      {node.kind === "child" && (
        <Fields>
          <Field label="Program / version">
            {node.program.id} / {node.program.version}
          </Field>
          <Field label="Execution ID">{node.execution.id}</Field>
          <Field label="Fork membership">
            {node.fork_key ?? "No fork membership recorded"}
          </Field>
        </Fields>
      )}
      {node.kind === "fork" && (
        <>
          <p>Registration accepted. Branch execution may start later.</p>
          <Fields>
            <Field label="Distributed branch keys">
              {node.branch_keys.join(", ") || "None"}
            </Field>
          </Fields>
        </>
      )}
      {node.kind === "child_wait" && (
        <>
          <p>
            Waits for terminal outcomes, including failure or cancellation. It
            does not require every child to succeed.
          </p>
          <Fields>
            <Field label="Member keys">{node.member_keys.join(", ")}</Field>
            <Field label="Resume entrypoint">{node.resume}</Field>
          </Fields>
          {!node.resumed_activation_id && (
            <p className="muted">
              No resume is recorded. This alone does not prove the wait is still
              active.
            </p>
          )}
        </>
      )}
      {node.kind === "external_wait" && (
        <>
          <Fields>
            <Field label="Deadline">
              <When value={node.deadline} />
            </Field>
          </Fields>
          <p>
            {node.wake_reason
              ? `Recorded wake: ${node.wake_reason}. Resume scheduled; execution is observed separately.`
              : node.closed_at !== null
                ? "The wait closed without retained wake evidence; cancellation or failure can also close waits."
                : node.wait_kind === "approval"
                  ? "Waiting for an approval decision."
                  : "Waiting for an external wake."}
          </p>
          <Link
            to={`/workflows/${encodeURIComponent(workflowId)}?${detailDestination(params, "General", node.wait_kind === "approval" ? "approvals" : "waits")}`}
            preventScrollReset
            state={{ ...location.state, restoreNavigationKey: location.key }}
          >
            {node.wait_kind === "approval"
              ? "Inspect approval requests"
              : "Inspect waits and available actions"}
          </Link>
        </>
      )}
      {node.kind === "local" && (
        <>
          <p>
            This operation runs inside the workflow’s process. It is not an
            independently scheduled task. An invocation records which entrypoint
            requested it; it does not prove completion or a dependency on
            another local step.
          </p>
          <Fields>
            <Field label="Callable">{node.callable}</Field>
            <Field label="Durable result accepted">
              <When value={node.accepted_at} />
            </Field>
          </Fields>
          {node.observation ? (
            <>
              <Fields>
                <Field label="Callable observation">
                  {node.observation.state} ·{" "}
                  {micros(node.observation.elapsed_us)}
                </Field>
              </Fields>
              <p className="muted">
                Returned means the callable returned; durable result acceptance
                is separate. Replay reuses an accepted result.
              </p>
              <LocalResources attemptId={node.observation.attempt_id} />
            </>
          ) : (
            <p className="notice">
              The result was accepted; callable start, end and resource usage
              were not recorded.
            </p>
          )}
        </>
      )}
      <details className="explorer-relationships">
        <summary>Recorded relationships ({relations.length})</summary>
        <RelationList relations={relations} select={select} />
      </details>
      <details>
        <summary>Recorded evidence</summary>
        <JsonView value={node} label="Explorer evidence" />
      </details>
    </>
  );
}

function LocalResources({ attemptId }: { attemptId: string }) {
  const [open, setOpen] = useState(false);
  return (
    <details onToggle={(event) => setOpen(event.currentTarget.open)}>
      <summary>Process resources for this attempt</summary>
      {open && <AttemptResources attemptId={attemptId} />}
    </details>
  );
}
