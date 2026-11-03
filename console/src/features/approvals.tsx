// SPDX-License-Identifier: MIT
import { ApprovalAttribution } from "./approval-attribution";
import { useEffect, useRef, useState } from "react";
import { Link, useLocation, useSearchParams } from "react-router";
import { useQueryClient } from "@tanstack/react-query";
import { accessController } from "../app/access-state";
import { usePermission } from "../app/access-session";
import { parseUserJson } from "../api/json";
import { useInstance } from "../app/instance";
import { useApprovalRead } from "../api/use-approval-read";
import { ContractError } from "../api/codecs";
import { freezeCommand, useCommand } from "../api/commands";
import { ApiError } from "../api/errors";
import { useTerminalRefresh } from "../api/terminal-refresh";
import {
  approvalPageForWorkflow,
  decisionReceipt,
  decodeApprovalDecision,
  type Approval,
  type ApprovalDecision,
} from "../api/approvals";
import { LoadingState } from "../components/async-state";
import {
  CopyText,
  Empty,
  Field,
  Fields,
  JsonView,
  QueryError,
  Status,
  When,
} from "../components/resource-ui";
import { Button } from "../components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from "../components/ui/dialog";

export function WorkflowApprovals({
  workflowId,
  active,
}: {
  workflowId: string;
  active: boolean;
}) {
  const config = useInstance();
  const [params, setParams] = useSearchParams();
  const location = useLocation();
  const cursor = params.get("approval_cursor");
  const query = useApprovalRead(
    "list",
    { workflow_id: workflowId, after_key: cursor, limit: 10 },
    approvalPageForWorkflow(workflowId, cursor),
    () => (active ? config.polling.waiting_workflow_ms : false),
  );
  useTerminalRefresh(active, [workflowId, cursor], query);
  function changePage(next: string | null) {
    const search = new URLSearchParams(params);
    if (next) search.set("approval_cursor", next);
    else search.delete("approval_cursor");
    setParams(search, { preventScrollReset: true, state: location.state });
  }
  return (
    <>
      <p className="muted">
        Review the recorded effective action before deciding. Approval permits
        the workflow to continue; execution is observed separately.
      </p>
      {query.isPending && <LoadingState />}
      {query.error && (
        <QueryError
          error={query.error}
          retry={() => void query.refetch()}
          stale={!!query.data}
        />
      )}
      {query.data && (
        <>
          {!query.data.items.length && (
            <Empty>No approval requests were recorded on this page.</Empty>
          )}
          <div className="approval-list">
            {query.data.items.map((item) => (
              <ApprovalCard
                key={JSON.stringify([
                  item.workflow_id,
                  item.key,
                  item.activation_id,
                  item.revision,
                ])}
                approval={item}
                unavailable={
                  query.error instanceof ApiError && query.error.status === 404
                }
              />
            ))}
          </div>
          {(cursor || query.data.next_cursor) && (
            <div className="page-controls">
              <Button
                variant="outline"
                disabled={!cursor}
                onClick={() => changePage(null)}
              >
                First page
              </Button>
              <Button
                variant="outline"
                disabled={!query.data.next_cursor}
                onClick={() => changePage(query.data?.next_cursor ?? null)}
              >
                Next approvals
              </Button>
            </div>
          )}
        </>
      )}
    </>
  );
}

function ActionDetails({ approval }: { approval: Approval }) {
  return (
    <>
      <Fields>
        <Field label="Tool / action">{approval.action.name}</Field>
        <Field label="Version">{approval.action.version}</Field>
        {approval.protection === "external_effect" && (
          <Field label="Execution">Protected external write</Field>
        )}
      </Fields>
      <h4>Effective arguments</h4>
      <JsonView
        value={approval.action.arguments}
        label="Effective action arguments"
      />
      {approval.proposed_arguments !== null && (
        <details>
          <summary>Original proposed arguments</summary>
          <JsonView
            value={approval.proposed_arguments}
            label="Original proposed arguments"
          />
        </details>
      )}
    </>
  );
}

export function ApprovalCard({
  approval,
  unavailable = false,
  showAttribution = false,
}: {
  approval: Approval;
  unavailable?: boolean;
  showAttribution?: boolean;
}) {
  const client = useQueryClient();
  const config = useInstance();
  const recoveryKey = JSON.stringify([
    "approval",
    config.instance_id,
    approval.workflow_id,
    approval.key,
    approval.activation_id,
    approval.revision,
  ]);
  const saved = accessController.recover(recoveryKey);
  const restoredDecision = saved
    ? decodeApprovalDecision(parseUserJson(saved.body, 1024 * 1024))
    : null;
  const [restored] = useState(!!saved);
  const restoredDenied = saved?.outcome === "denied";
  const [open, setOpen] = useState(false);
  const [choice, setChoice] = useState<"approve" | "reject">(
    restoredDecision?.decision ?? "approve",
  );
  const [reviewer, setReviewer] = useState(restoredDecision?.reviewer ?? "");
  const [reason, setReason] = useState(restoredDecision?.reason ?? "");
  const [validation, setValidation] = useState("");
  const canDecide = usePermission("approval_decide");
  const expected = useRef<ApprovalDecision | null>(restoredDecision);
  const trigger = useRef<HTMLButtonElement | null>(null);
  const submittedTrigger = useRef<HTMLButtonElement | null>(null);
  const command = useCommand(
    "approvals/decide",
    (value) => {
      if (!expected.current)
        throw new ContractError("Approval decision is missing.");
      return decisionReceipt(expected.current)(value);
    },
    () => void client.invalidateQueries(),
    recoveryKey,
  );
  const conflict =
    command.mutation.error instanceof ApiError &&
    command.mutation.error.status === 409;
  useEffect(() => {
    if (conflict) void client.invalidateQueries();
  }, [conflict, client]);
  const receipt = command.mutation.data?.approval;
  // Keep acceptance ahead of stale reads, then allow polling to add the resume.
  const current =
    receipt &&
    (approval.status !== receipt.status ||
      approval.decision?.decision_id !== receipt.decision?.decision_id ||
      (receipt.resumed_activation_id !== null &&
        approval.resumed_activation_id === null))
      ? receipt
      : approval;
  const locked = command.command !== null;
  const editable =
    current.status === "pending" && !locked && !unavailable && canDecide;
  function submit() {
    if (!editable || expected.current) return;
    const attribution = reviewer.trim();
    if (
      !attribution ||
      new TextEncoder().encode(attribution).length > 128 ||
      [...attribution].some(
        (character) =>
          character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127,
      )
    ) {
      setValidation(
        "Enter a reviewer name of at most 128 bytes, without control characters.",
      );
      return;
    }
    if (new TextEncoder().encode(reason).length > 4096) {
      setValidation("The reason must be at most 4096 bytes.");
      return;
    }
    const decision: ApprovalDecision = {
      workflow_id: current.workflow_id,
      key: current.key,
      activation_id: current.activation_id,
      revision: current.revision,
      action: current.action,
      ...(current.protection ? { protection: current.protection } : {}),
      decision_id: crypto.randomUUID(),
      decision: choice,
      reviewer: attribution,
      reason: reason || null,
    };
    expected.current = decision;
    setValidation("");
    command.send(freezeCommand(decision, decision.decision_id));
  }
  return (
    <article
      className="card approval-card"
      aria-label={`Approval ${approval.key}`}
    >
      <div className="summary-line">
        <h3>{approval.key}</h3>
        <Status value={current.status} />
      </div>
      <ActionDetails approval={current} />
      <Fields>
        <Field label="Requested">
          <When value={current.created_at} />
        </Field>
        <Field label="Deadline">
          <When value={current.deadline} />
        </Field>
        <Field label="Request activation">
          <Link to={`/executions/${encodeURIComponent(current.activation_id)}`}>
            {current.activation_id}
          </Link>
        </Field>
        <Field label="Revision">{current.revision}</Field>
        {current.resumed_activation_id && (
          <Field label="Resume scheduled">
            <Link
              to={`/executions/${encodeURIComponent(current.resumed_activation_id)}`}
            >
              {current.resumed_activation_id}
            </Link>
          </Field>
        )}
      </Fields>
      {current.decision && (
        <Fields>
          <Field label="Reviewer label (caller supplied)">
            {current.decision.reviewer}
          </Field>
          <Field label="Decision">{current.decision.decision}</Field>
          <Field label="Decided">
            <When value={current.decision.decided_at} />
          </Field>
          <Field label="Decision ID">
            <CopyText value={current.decision.decision_id} />
          </Field>
          <Field label="Reason">
            {current.decision.reason ?? "Not supplied"}
          </Field>
        </Fields>
      )}
      {current.decision &&
        (showAttribution ? (
          <ApprovalAttribution approval={current} />
        ) : (
          <Link
            to={`/approvals/review?${new URLSearchParams({ workflow: current.workflow_id, key: current.key })}`}
          >
            View recorded attribution
          </Link>
        ))}
      {!canDecide && (
        <p className="notice">
          Your credential allows inspection, but does not permit approval
          decisions.
        </p>
      )}
      <div className="actions">
        {editable && (
          <>
            <Button
              aria-haspopup="dialog"
              onClick={(event) => {
                trigger.current = event.currentTarget;
                setChoice("approve");
                setOpen(true);
              }}
            >
              Approve
            </Button>
            <Button
              variant="outline"
              aria-haspopup="dialog"
              onClick={(event) => {
                trigger.current = event.currentTarget;
                setChoice("reject");
                setOpen(true);
              }}
            >
              Reject
            </Button>
          </>
        )}
        {locked && (
          <Button
            variant="outline"
            ref={submittedTrigger}
            aria-haspopup="dialog"
            onClick={() => setOpen(true)}
          >
            View submitted decision
          </Button>
        )}
      </div>
      <Dialog
        open={open}
        onOpenChange={(next) => {
          if (!command.mutation.isPending) setOpen(next);
        }}
      >
        <DialogContent
          onCloseAutoFocus={(event) => {
            const target = trigger.current?.isConnected
              ? trigger.current
              : submittedTrigger.current;
            if (target) {
              event.preventDefault();
              target.focus();
            }
          }}
        >
          <DialogTitle>
            {locked
              ? "Submitted approval decision"
              : choice === "approve"
                ? "Approve this action?"
                : "Reject this action?"}
          </DialogTitle>
          <DialogDescription>
            Request {approval.key}. Your decision applies to this exact action
            and version. The recorded action cannot be edited here.
          </DialogDescription>
          <ActionDetails approval={current} />
          {current.decision && open && (
            <ApprovalAttribution approval={current} />
          )}
          {command.mutation.data ? (
            <p className="notice" role="status">
              {command.mutation.data.already_accepted
                ? "This exact decision was already recorded."
                : "Decision recorded."}{" "}
              Current status: {current.status}.
            </p>
          ) : unavailable && !locked ? (
            <p className="notice" role="status">
              This request is no longer available for a decision.
            </p>
          ) : conflict || (current.status !== "pending" && !locked) ? (
            <p
              className="notice"
              role="status"
              aria-label="Approval decision status"
            >
              {conflict && "The server declined your decision. "}Recorded
              request status: {current.status}.
              {current.decision && (
                <> Recorded reviewer: {current.decision.reviewer}.</>
              )}
            </p>
          ) : (
            <>
              <label>
                Reviewer
                <input
                  required
                  maxLength={128}
                  value={reviewer}
                  disabled={!editable}
                  onChange={(event) => setReviewer(event.target.value)}
                />
              </label>
              <p className="muted">
                Reviewer is a caller-supplied label, separate from the identity
                verified by Ledgence.
              </p>
              <label>
                Reason (optional)
                <textarea
                  rows={3}
                  maxLength={4096}
                  value={reason}
                  disabled={!editable}
                  onChange={(event) => setReason(event.target.value)}
                />
              </label>
              {validation && <p role="alert">{validation}</p>}
              {(command.mutation.error || restored) && command.command ? (
                <>
                  <QueryError
                    error={
                      command.mutation.error ??
                      new ApiError(
                        restoredDenied
                          ? "Access was denied before this decision was accepted. You can retry it explicitly with your current access."
                          : "A previously submitted decision is awaiting confirmation. Its outcome may be unknown.",
                      )
                    }
                    retry={command.retry}
                  />
                  <p className="notice">
                    {!restoredDenied && "The result may be uncertain. "}
                    Retry sends the same request, action, reviewer, decision and
                    decision ID. It cannot create a different decision.
                  </p>
                  <p>
                    Decision ID <CopyText value={command.command.identity} />
                  </p>
                </>
              ) : (
                <Button
                  disabled={
                    !editable || !reviewer.trim() || command.mutation.isPending
                  }
                  onClick={submit}
                >
                  {command.mutation.isPending
                    ? "Recording decision…"
                    : choice === "approve"
                      ? "Confirm approval"
                      : "Confirm rejection"}
                </Button>
              )}
            </>
          )}
        </DialogContent>
      </Dialog>
    </article>
  );
}
