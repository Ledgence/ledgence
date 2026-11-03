// SPDX-License-Identifier: MIT
import { useMemo, useRef, useState } from "react";
import { useAccess, usePermission } from "../app/access-session";
import { accessController } from "../app/access-state";
import { useInstance } from "../app/instance";
import { freezeCommand, useCommand, type FrozenCommand } from "../api/commands";
import { ContractError } from "../api/codecs";
import { ApiError } from "../api/errors";
import { parseUserJson } from "../api/json";
import {
  decodeEffectResolutionCommand,
  effectResolutionReceipt,
  saveEffectResolutionCommand,
  type EffectDetail,
  type EffectResolutionCommand,
} from "../api/effects";
import { Button } from "../components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from "../components/ui/dialog";
import {
  CopyText,
  Field,
  Fields,
  JsonView,
  When,
} from "../components/resource-ui";
import { EffectAttribution } from "./effect-attribution";

export function EffectResolutionForm({
  detail,
  unavailable,
  refresh,
}: {
  detail: EffectDetail;
  unavailable: boolean;
  refresh: () => void;
}) {
  const config = useInstance();
  const { access } = useAccess();
  const canResolve = usePermission("effect_resolve");
  const supported =
    access?.mode === "credentials" &&
    access.native_effect_resolution &&
    access.operator_audit;
  const permitted = !!supported && canResolve;
  const recoveryKey = JSON.stringify([
    "effect-resolution",
    config.instance_id,
    detail.summary.occurrence,
  ]);
  const saved = accessController.recover(recoveryKey);
  const [restored] = useState(saved !== undefined);
  const [draft, setDraft] = useState<FrozenCommand | null>(saved ?? null);
  const [savedCommand, setSavedCommand] = useState<string | null>(() =>
    saved
      ? saveEffectResolutionCommand(
          saved.body,
          detail.summary.occurrence.scope,
          globalThis.location.origin,
        )
      : null,
  );
  const expected = useRef<EffectResolutionCommand | null>(
    saved
      ? decodeEffectResolutionCommand(parseUserJson(saved.body, 256 * 1024))
      : null,
  );
  const [kind, setKind] = useState("record_success");
  const [result, setResult] = useState("null");
  const [failureKind, setFailureKind] = useState("");
  const [failureMessage, setFailureMessage] = useState("");
  const [reason, setReason] = useState("");
  const [references, setReferences] = useState("");
  const [validation, setValidation] = useState("");
  const [open, setOpen] = useState(false);
  const [separate, setSeparate] = useState(false);
  const command = useCommand(
    "workflows/approved-effects/resolve",
    (value) => {
      if (!expected.current)
        throw new ContractError("The frozen assessment is missing.");
      return effectResolutionReceipt(
        expected.current,
        detail.summary.occurrence.scope,
      )(value);
    },
    refresh,
    recoveryKey,
  );
  const locked = command.command !== null;
  const editable = permitted && !unavailable && !locked && !draft;
  const receipt = command.mutation.data;
  const conflict =
    command.mutation.error instanceof ApiError &&
    command.mutation.error.status === 409;
  const frozen = useMemo(
    () =>
      draft
        ? decodeEffectResolutionCommand(parseUserJson(draft.body, 256 * 1024))
        : null,
    [draft],
  );

  function review() {
    if (!editable) return;
    try {
      const { workflow_id, activation_id, approval_key } =
        detail.summary.occurrence;
      const assessment =
        kind === "record_success"
          ? { kind, result: parseUserJson(result, 128 * 1024) }
          : kind === "record_failure"
            ? { kind, failure: { kind: failureKind, message: failureMessage } }
            : { kind };
      const value = decodeEffectResolutionCommand({
        occurrence: { workflow_id, activation_id, approval_key },
        resolution_id: crypto.randomUUID(),
        expected_effect_revision: detail.summary.effect_revision,
        supersedes_resolution_id:
          detail.summary.operator_resolution?.reference.resolution_id ?? null,
        action: detail.binding.approval.action,
        assessment,
        reason,
        evidence_references: references
          .split("\n")
          .map((value) => value.trim())
          .filter(Boolean),
      });
      const next = freezeCommand(value, value.resolution_id);
      const savedEnvelope = saveEffectResolutionCommand(
        next.body,
        detail.summary.occurrence.scope,
        globalThis.location.origin,
      );
      expected.current = value;
      setSavedCommand(savedEnvelope);
      setDraft(next);
      setValidation("");
      setOpen(true);
    } catch (error) {
      setValidation(
        error instanceof Error
          ? error.message
          : "Review the assessment fields.",
      );
    }
  }
  function closeReview(next: boolean) {
    if (command.mutation.isPending) return;
    setOpen(next);
    if (!next && !locked) {
      setDraft(null);
      setSavedCommand(null);
      expected.current = null;
    }
  }
  function newReview() {
    command.reset();
    setSavedCommand(null);
    expected.current = null;
    setDraft(null);
    setReason("");
    setReferences("");
    setResult("null");
    setFailureKind("");
    setFailureMessage("");
    setValidation("");
    setSeparate(false);
    setOpen(false);
    refresh();
  }
  function download() {
    if (!savedCommand || !expected.current) return;
    const url = URL.createObjectURL(
      new Blob([savedCommand], { type: "application/json" }),
    );
    const link = document.createElement("a");
    link.href = url;
    link.download = `ledgence-effect-${expected.current.resolution_id}.json`;
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
  return (
    <section className="card" aria-label="Operator assessment">
      <h2>Operator assessment</h2>
      <p>
        Record your conclusion from external evidence. A matching durable wait
        can then resume according to the workflow's code.
      </p>
      {!supported ? (
        <p className="notice">
          Native effect resolution and operator auditing must be enabled on this
          server.
        </p>
      ) : !canResolve ? (
        <p className="notice">
          Your credential needs effect_resolve to record an assessment.
        </p>
      ) : null}
      {!locked && (
        <form
          className="execution-form"
          onSubmit={(event) => {
            event.preventDefault();
            review();
          }}
        >
          <fieldset disabled={!editable} className="execution-form">
            <legend>Assessment details</legend>
            <label>
              Outcome
              <select
                value={kind}
                onChange={(event) => setKind(event.target.value)}
              >
                <option value="record_success">Record success</option>
                <option value="record_failure">Record failure</option>
                <option value="close_unknown">
                  Close with unknown outcome
                </option>
              </select>
            </label>
            {kind === "record_success" && (
              <label>
                Result JSON
                <textarea
                  rows={6}
                  value={result}
                  onChange={(event) => setResult(event.target.value)}
                  spellCheck={false}
                />
              </label>
            )}
            {kind === "record_failure" && (
              <>
                <label>
                  Failure kind
                  <input
                    value={failureKind}
                    onChange={(event) => setFailureKind(event.target.value)}
                    maxLength={128}
                  />
                </label>
                <label>
                  Failure message
                  <textarea
                    value={failureMessage}
                    onChange={(event) => setFailureMessage(event.target.value)}
                    rows={3}
                  />
                </label>
              </>
            )}
            <label>
              Reason
              <textarea
                value={reason}
                onChange={(event) => setReason(event.target.value)}
                rows={3}
                required
              />
            </label>
            <label>
              Evidence references
              <textarea
                value={references}
                onChange={(event) => setReferences(event.target.value)}
                rows={3}
                placeholder="One reference per line"
              />
            </label>
            <p className="muted">
              Use up to eight references. Success and failure require at least
              one. References are retained as text.
            </p>
            <Button type="submit" aria-haspopup="dialog">
              Review assessment
            </Button>
          </fieldset>
          {validation && <p role="alert">{validation}</p>}
          {unavailable && (
            <p role="status">
              Refresh the effect evidence before preparing a new assessment.
            </p>
          )}
        </form>
      )}
      {locked && (
        <Button variant="outline" onClick={() => setOpen(true)}>
          View frozen assessment
        </Button>
      )}
      {locked && savedCommand && (
        <Button variant="outline" onClick={download}>
          Download saved command
        </Button>
      )}
      {locked && !receipt && !command.mutation.isPending && (
        <div className="notice">
          <p role="alert">
            {conflict
              ? "The server rejected this assessment because its evidence revision or predecessor changed. Inspect the latest evidence before starting a new review."
              : restored && !command.mutation.error
                ? "An earlier assessment is retained. Confirm its outcome by retrying the same command."
                : "Acceptance is unconfirmed. Retry preserves the exact assessment, identity and evidence revision."}
          </p>
          <p>
            Resolution ID <CopyText value={command.command!.identity} />
          </p>
          <Button
            variant="outline"
            disabled={!permitted}
            onClick={command.retry}
          >
            Retry same assessment
          </Button>
          <Button variant="outline" onClick={refresh}>
            Refresh evidence
          </Button>
          <Button variant="outline" onClick={() => setSeparate(true)}>
            Start a new review
          </Button>
        </div>
      )}
      {command.mutation.isPending && <p role="status">Recording assessment…</p>}
      {receipt && (
        <section aria-label="Assessment receipt">
          <h3>Assessment recorded</h3>
          <Fields>
            <Field label="Resolution ID">
              <CopyText value={receipt.resolution_id} />
            </Field>
            <Field label="Accepted">
              <When value={receipt.accepted_at} />
            </Field>
            <Field label="Evidence revision">{receipt.accepted_revision}</Field>
          </Fields>
          <p>
            Workflow progress is tracked separately. Later worker evidence can
            dispute an assessment.
          </p>
          <EffectAttribution receipt={receipt} />
          <Button
            variant="outline"
            disabled={!permitted || unavailable}
            onClick={() => setSeparate(true)}
          >
            Start a new review
          </Button>
        </section>
      )}
      <Dialog open={open} onOpenChange={closeReview}>
        <DialogContent>
          <DialogTitle>
            {locked ? "Frozen assessment" : "Review assessment"}
          </DialogTitle>
          <DialogDescription>
            These values are frozen together. Sending records the assessment
            against the displayed evidence revision.
          </DialogDescription>
          {frozen && (
            <>
              <Fields>
                <Field label="Resolution ID">
                  <CopyText value={frozen.resolution_id} />
                </Field>
                <Field label="Evidence revision">
                  {frozen.expected_effect_revision}
                </Field>
                <Field label="Previous resolution">
                  {frozen.supersedes_resolution_id ?? "None"}
                </Field>
              </Fields>
              <JsonView
                label="Approved effective action"
                value={frozen.action}
              />
              <JsonView label="Assessment" value={frozen.assessment} />
              <Field label="Reason">{frozen.reason}</Field>
              <JsonView
                label="Evidence references"
                value={frozen.evidence_references}
              />
            </>
          )}
          {savedCommand && (
            <>
              <p className="muted">
                Save the destination-bound command before sending. The file
                includes the approved action and assessment result. Use it with
                the same server, scope and actor for CLI or Python retry.
              </p>
              <Button variant="outline" onClick={download}>
                Download saved command
              </Button>
            </>
          )}
          {!locked && (
            <Button
              disabled={!permitted || unavailable || !draft}
              onClick={() => {
                if (draft && permitted && !unavailable) command.send(draft);
              }}
            >
              Record assessment
            </Button>
          )}
          <Button
            variant="outline"
            disabled={command.mutation.isPending}
            onClick={() => closeReview(false)}
          >
            Close
          </Button>
        </DialogContent>
      </Dialog>
      <Dialog open={separate} onOpenChange={setSeparate}>
        <DialogContent>
          <DialogTitle>Start a new assessment review?</DialogTitle>
          <DialogDescription>
            Keep the frozen resolution ID for reconciliation. This clears the
            local command and refreshes current evidence. Any previously
            accepted assessment remains in the durable history.
          </DialogDescription>
          {command.command && <CopyText value={command.command.identity} />}
          <Button disabled={command.mutation.isPending} onClick={newReview}>
            Refresh and start new review
          </Button>
        </DialogContent>
      </Dialog>
    </section>
  );
}
