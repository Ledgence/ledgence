// SPDX-License-Identifier: MIT
import { useState } from "react";
import { Link, useLocation, useSearchParams } from "react-router";
import { ArrowUpRight } from "lucide-react";
import { executionPath, type ExplorerNode } from "../../api/explorer";
import { effectDetailPath } from "../../api/effects";
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
import { AdmissionRecord } from "./admission-record";
import { OperationRecord } from "./operation-record";
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
  const [inspectOperation, setInspectOperation] = useState(false);
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
          <p>This operation runs inside the workflow’s process.</p>
          <Fields>
            <Field label="Callable">{node.callable}</Field>
            <Field label="Durable result accepted">
              <When value={node.accepted_at} />
            </Field>
          </Fields>
          {node.accepted_at !== null ? (
            <details
              onToggle={(event) =>
                setInspectOperation(event.currentTarget.open)
              }
            >
              <summary>Inspect durable operation</summary>
              {inspectOperation && (
                <OperationRecord workflowId={workflowId} node={node} />
              )}
            </details>
          ) : (
            <p className="muted">
              No accepted result is recorded for this step.
            </p>
          )}
          {node.observation ? (
            <>
              <Fields>
                <Field label="Observed attempt">
                  {node.observation.attempt_id}
                </Field>
                <Field label="Callable observation">
                  {node.observation.state} ·{" "}
                  {micros(node.observation.elapsed_us)}
                </Field>
              </Fields>
              <p className="muted">
                {node.observation.state === "replayed"
                  ? "This attempt was observed reusing an accepted result."
                  : "This observation describes one attempt. Other attempts may have reused an accepted result."}
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
      {node.kind === "admission_wait" && (
        <>
          <Fields>
            <Field label="Resource">{node.resource}</Field>
            <Field label="Original activation">
              {node.occurrence.origin_activation_id}
            </Field>
            <Field label="Operation key">{node.occurrence.key}</Field>
            <Field label="Deadline">
              <When value={node.deadline} />
            </Field>
          </Fields>
          <p>
            {node.selection_kind
              ? `Selected ${node.selection_kind.replaceAll("_", " ")}. A continuation was scheduled; an offer is not permission to execute the callback.`
              : node.closed_at !== null
                ? "This admission wait closed without a scheduled continuation."
                : "Waiting durably for resource admission. No worker process is held while parked."}
          </p>
        </>
      )}
      {node.kind === "admission_operation" && (
        <>
          <p>
            The original callback result was accepted durably. Replaying this
            result does not create another operation.
          </p>
          <Fields>
            <Field label="Callable">{node.callable}</Field>
            <Field label="Resource">{node.resource}</Field>
            <Field label="Original activation">
              {node.occurrence.origin_activation_id}
            </Field>
            <Field label="Operation key">{node.occurrence.key}</Field>
            <Field label="Accepting attempt">{node.accepting_attempt_id}</Field>
          </Fields>
          <details
            onToggle={(event) => setInspectOperation(event.currentTarget.open)}
          >
            <summary>Inspect accepted operation</summary>
            {inspectOperation && <AdmissionRecord node={node} />}
          </details>
          <p className="muted">
            Acceptance is a recorded milestone. Callback duration and resource
            usage are not recorded for this operation.
          </p>
        </>
      )}
      {node.kind === "provider_wait" && (
        <>
          <p>
            {node.selection_kind
              ? `Selected ${node.selection_kind.replaceAll("_", " ")}. A continuation was scheduled from retained evidence; this does not prove the resumed code has run.`
              : node.closed_at !== null
                ? "This provider wait closed without scheduling a continuation. Closure does not establish an external outcome."
                : "Waiting durably for a provider outcome. No worker process is held by this wait; ordinary workflow events cannot resolve it."}
          </p>
          <Fields>
            <Field label="Effect origin activation">
              {node.occurrence.activation_id}
            </Field>
            <Field label="Approval key">{node.occurrence.approval_key}</Field>
            <Field label="After effect revision">
              {node.after_effect_revision ?? "First outcome"}
            </Field>
            <Field label="Selected effect revision">
              {node.selected_effect_revision ?? "Not selected"}
            </Field>
          </Fields>
          <Link
            to={effectDetailPath(node.occurrence)}
            state={{
              returnTo: `${location.pathname}${location.search}`,
              returnNavigationKey: location.key,
              restoreNavigationKey: location.key,
            }}
          >
            Inspect provider evidence
          </Link>
        </>
      )}
      {node.kind === "native_effect_wait" && (
        <>
          <p>
            {node.outcome_kind
              ? `Recorded operator resolution: ${node.outcome_kind.replaceAll("_", " ")}. A resume was scheduled; this does not confirm provider execution.`
              : node.closed_at !== null
                ? "This recovery wait closed without a resume. Closure does not establish an effect outcome."
                : "Waiting for a recorded operator resolution. Ordinary workflow events cannot resolve this wait."}
          </p>
          <Fields>
            <Field label="Effect origin activation">
              {node.occurrence.activation_id}
            </Field>
            <Field label="Approval key">{node.occurrence.approval_key}</Field>
            <Field label="After resolution">
              {node.after_resolution_id ?? "First resolution"}
            </Field>
            <Field label="Selected resolution">
              {node.resolution_id ?? "Not selected"}
            </Field>
          </Fields>
          <Link
            to={effectDetailPath(node.occurrence)}
            state={{
              returnTo: `${location.pathname}${location.search}`,
              returnNavigationKey: location.key,
              restoreNavigationKey: location.key,
            }}
          >
            Inspect effect and operator resolution
          </Link>
          <p>
            <Link
              to={`${executionPath("task", node.occurrence.activation_id)}?tab=Attempts`}
              state={{
                returnTo: `${location.pathname}${location.search}`,
                restoreNavigationKey: location.key,
              }}
            >
              Inspect original effect activation
            </Link>
          </p>
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
