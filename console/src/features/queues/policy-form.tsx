// SPDX-License-Identifier: MIT
import { useState } from "react";
import { useInstance } from "../../app/instance";
import { usePermission } from "../../app/access-session";
import {
  put,
  putReceiptFor,
  type QueueScope,
  type QueueSnapshot,
  type QueuePolicy,
} from "../../api/queue-policies";
import { Button } from "../../components/ui/button";
import { PendingRequest } from "./command";
import { useQueueCommand, policyKey } from "./use-queue-command";

function integer(text: string, max: number, label: string, min = 1) {
  if (
    !/^(0|[1-9][0-9]*)$/.test(text) ||
    Number(text) < min ||
    Number(text) > max
  )
    throw new Error(label + " is outside the supported range.");
  return Number(text);
}
export function PolicyForm({
  queue,
  snapshot,
  scope,
  onSaved,
  onClose,
}: {
  queue: string;
  snapshot: QueueSnapshot | null;
  scope: QueueScope;
  onSaved: (revision: string) => void;
  onClose: () => void;
}) {
  const config = useInstance();
  const canControl = usePermission("resource_control");
  // Pin the revision shown when editing; automatic reads may observe concurrent operator changes.
  const [base] = useState(snapshot?.record ?? null);
  const [mode, setMode] = useState(
    base?.policy.max_concurrency === null
      ? "unlimited"
      : base?.policy.max_concurrency === 0
        ? "paused"
        : "limited",
  );
  const [limit, setLimit] = useState(String(base?.policy.max_concurrency || 5));
  const previous = base?.policy.timeout;
  const [hasTimeout, setHasTimeout] = useState(!!previous);
  const [limitMs, setLimitMs] = useState(String(previous?.after_ms ?? 60000));
  const [force, setForce] = useState(previous?.release.mode === "after_grace");
  const [grace, setGrace] = useState(
    String(
      previous?.release.mode === "after_grace"
        ? previous.release.grace_ms
        : 30000,
    ),
  );
  const [reason, setReason] = useState("");
  const [acceptedRisk, setAcceptedRisk] = useState(false);
  const [error, setError] = useState("");
  const req = useQueueCommand(
    "put",
    policyKey(config.instance_id, queue),
    put,
    (raw, v) => putReceiptFor(raw, scope, v),
    (receipt) => onSaved(receipt.record.revision),
  );
  const locked = !!req.command;
  function submit() {
    if (!canControl || locked) return;
    try {
      const timeout: QueuePolicy["timeout"] = hasTimeout
        ? {
            after_ms: integer(limitMs, 2147483647, "Stop timeout"),
            release: force
              ? {
                  mode: "after_grace",
                  grace_ms: integer(grace, 2147483647, "Release grace"),
                }
              : { mode: "confirm" },
          }
        : null;
      if (timeout?.release.mode === "after_grace" && !acceptedRisk)
        throw new Error(
          "Acknowledge the risk of releasing without a confirmed stop.",
        );
      const policy = {
        max_concurrency:
          mode === "limited"
            ? integer(limit, 1000000, "Maximum concurrent attempts")
            : mode === "paused"
              ? 0
              : null,
        timeout,
      };
      const command = put({
        queue,
        request_id: crypto.randomUUID(),
        expected_revision: base?.revision ?? null,
        policy,
        reason: reason.trim(),
      });
      setError("");
      req.send(command);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Review the queue policy.");
    }
  }
  return (
    <section className="resource-list queue-editor">
      <h2>{base ? "Edit policy" : "Create queue policy"}</h2>
      <p className="muted">
        Limits apply across workers in this scope. Existing holds retain the
        timeout and grace policy in effect when they were acquired.
        {!base &&
          !locked &&
          " On first enrollment, any retained attempts that may still be running count toward occupancy, and their timeout starts at enrollment."}
      </p>
      {locked ? (
        <PendingRequest
          identity={req.command!.identity}
          body={req.command!.body}
          pending={req.mutation.isPending}
          error={req.mutation.error}
          retry={req.retry}
          reset={() => {
            req.reset();
            onClose();
          }}
        />
      ) : (
        <form
          className="queue-form"
          onSubmit={(e) => {
            e.preventDefault();
            submit();
          }}
        >
          <fieldset disabled={!canControl}>
            <legend className="sr-only">Queue policy settings</legend>
            <label>
              Concurrency policy
              <select
                autoFocus
                value={mode}
                onChange={(e) => setMode(e.target.value)}
              >
                <option value="limited">Limit concurrent attempts</option>
                <option value="paused">Pause new attempts</option>
                <option value="unlimited">Unlimited</option>
              </select>
            </label>
            {mode === "limited" && (
              <label>
                Maximum concurrent attempts
                <input
                  type="number"
                  min="1"
                  max="1000000"
                  required
                  value={limit}
                  onChange={(e) => setLimit(e.target.value)}
                />
              </label>
            )}
            <label className="check">
              <input
                type="checkbox"
                checked={hasTimeout}
                onChange={(e) => setHasTimeout(e.target.checked)}
              />{" "}
              Request a stop after a timeout
            </label>
            {hasTimeout && (
              <>
                <label>
                  Stop timeout (ms)
                  <input
                    type="number"
                    min="1"
                    max="2147483647"
                    required
                    value={limitMs}
                    onChange={(e) => setLimitMs(e.target.value)}
                  />
                </label>
                <label>
                  After requesting a stop
                  <select
                    value={force ? "force" : "confirm"}
                    onChange={(e) => {
                      setForce(e.target.value === "force");
                      setAcceptedRisk(false);
                    }}
                  >
                    <option value="confirm">Wait for confirmed closure</option>
                    <option value="force">Release after a grace period</option>
                  </select>
                </label>
                {force && (
                  <>
                    <label>
                      Release grace (ms)
                      <input
                        type="number"
                        min="1"
                        max="2147483647"
                        required
                        value={grace}
                        onChange={(e) => {
                          setGrace(e.target.value);
                          setAcceptedRisk(false);
                        }}
                      />
                    </label>
                    <label className="check notice">
                      <input
                        type="checkbox"
                        required
                        checked={acceptedRisk}
                        onChange={(e) => setAcceptedRisk(e.target.checked)}
                      />{" "}
                      I understand that another task may start while the
                      previous task is still running.
                    </label>
                  </>
                )}
              </>
            )}
            <label>
              Reason for this change
              <textarea
                required
                value={reason}
                onChange={(e) => setReason(e.target.value)}
              />
            </label>
          </fieldset>
          {error && (
            <p role="alert" className="notice">
              {error}
            </p>
          )}
          <div className="actions">
            <Button type="submit" disabled={!canControl}>
              Save policy
            </Button>
            <Button variant="outline" onClick={onClose}>
              Cancel
            </Button>
          </div>
        </form>
      )}
    </section>
  );
}
