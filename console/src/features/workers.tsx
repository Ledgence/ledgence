import { useRef, useState } from "react";
import { Link, useParams, useSearchParams } from "react-router";
import { Server, Activity, Scan } from "lucide-react";
import { useInstance } from "../app/instance";
import { usePagination, useResource } from "../api/hooks";
import { ContractError } from "../api/codecs";
import { ApiError } from "../api/errors";
import * as dto from "../api/resources";
import type { Decoded } from "../api/schema";
import { Button } from "../components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from "../components/ui/dialog";
import { LoadingState } from "../components/async-state";
import {
  BackLink,
  CopyText,
  Empty,
  Field,
  Fields,
  PageControls,
  PageHeading,
  QueryError,
  Status,
  When,
} from "../components/resource-ui";

export function WorkersPage() {
  const config = useInstance();
  const [params, setParams] = useSearchParams();
  const pagination = usePagination();
  const query = useResource(
    "workers",
    {
      limit: pagination.limit,
      cursor: pagination.cursor,
      queue: params.get("queue"),
    },
    dto.workerPage,
    {
      enabled: config.capabilities.workers,
      interval: config.polling.workers_ms,
    },
  );
  function filter(form: HTMLFormElement) {
    const queue = new FormData(form).get("queue");
    const next = new URLSearchParams();
    if (typeof queue === "string" && queue) next.set("queue", queue);
    next.set("limit", String(pagination.limit));
    setParams(next);
  }
  return (
    <>
      <PageHeading
        title="Workers"
        description="Worker sessions, reported capacity and process activity."
      />
      {!config.capabilities.workers ? (
        <Empty>Worker observations are unavailable on this server.</Empty>
      ) : (
        <>
          <form
            key={params.get("queue") ?? ""}
            className="filters"
            onSubmit={(e) => {
              e.preventDefault();
              filter(e.currentTarget);
            }}
          >
            <label>
              Exact queue
              <input
                name="queue"
                defaultValue={params.get("queue") ?? ""}
                list="worker-queue-suggestions"
                maxLength={128}
              />
              <datalist id="worker-queue-suggestions">
                {config.suggested_queues.map((q) => (
                  <option key={q}>{q}</option>
                ))}
              </datalist>
            </label>
            <Button type="submit" variant="outline">
              Apply filter
            </Button>
            {params.has("queue") && (
              <Button
                variant="ghost"
                onClick={() => setParams(new URLSearchParams())}
              >
                Clear
              </Button>
            )}
          </form>
          {query.isPending && <LoadingState label="Loading workers" />}
          {query.error && (
            <QueryError
              error={query.error}
              retry={() => void query.refetch()}
              stale={!!query.data}
            />
          )}
          {query.data && (
            <>
              {query.data.items.length ? (
                <div className="card-grid worker-grid">
                  {query.data.items.map((worker) => (
                    <article
                      className="card worker-card"
                      key={worker.worker_session_id}
                    >
                      <div className="worker-card-heading">
                        <Server aria-hidden="true" />
                        <h2>
                          <Link
                            to={`/workers/${encodeURIComponent(worker.worker_session_id)}`}
                          >
                            {worker.display_name ?? worker.worker_session_id}
                          </Link>
                        </h2>
                      </div>
                      {worker.display_name &&
                        worker.display_name !== worker.worker_session_id && (
                          <p className="wrap muted worker-session-id">
                            {worker.worker_session_id}
                          </p>
                        )}
                      <WorkerSummary
                        worker={worker}
                        observedAt={query.data.observed_at}
                      />
                    </article>
                  ))}
                </div>
              ) : (
                <Empty filtered={params.has("queue")}>
                  No registered worker sessions were returned. Start a worker or
                  adjust the exact queue filter.
                </Empty>
              )}
              <PageControls
                pagination={pagination}
                nextCursor={query.data.next_cursor}
                observedAt={query.data.observed_at}
                refresh={() => void query.refetch()}
                fetching={query.isFetching}
              />
            </>
          )}
        </>
      )}
    </>
  );
}

export function WorkerDetailPage() {
  const { workerSessionId = "" } = useParams();
  const config = useInstance();
  const pagination = usePagination();
  const [selection, setSelection] = useState<{
    sessionId: string;
    slotId: number;
  } | null>(null);
  const selectedSlot =
    selection?.sessionId === workerSessionId ? selection.slotId : null;
  const returnFocus = useRef<HTMLButtonElement | null>(null);
  const valid =
    workerSessionId.length > 0 &&
    new TextEncoder().encode(workerSessionId).length <= 128 &&
    ![...workerSessionId].some((character) => {
      const code = character.codePointAt(0) ?? 0;
      return (
        code < 32 ||
        (code >= 127 && code <= 159) ||
        (code >= 0xfdd0 && code <= 0xfdef) ||
        (code & 0xfffe) === 0xfffe
      );
    });
  const query = useResource(
    "workers/inspect",
    {
      worker_session_id: workerSessionId,
      limit: pagination.limit,
      cursor: pagination.cursor,
    },
    (value) => {
      const detail = dto.workerDetail(value);
      if (detail.worker.worker_session_id !== workerSessionId)
        throw new ContractError(
          "The observation belongs to a different worker session.",
        );
      return detail;
    },
    {
      enabled: valid && config.capabilities.workers,
      interval: config.polling.workers_ms,
    },
  );
  const detail = query.data;
  const selected = detail?.slots.items.find(
    (slot) => slot.slot_id === selectedSlot,
  );
  return (
    <>
      <BackLink to="/workers">Workers</BackLink>
      <PageHeading
        title={detail?.worker.display_name ?? "Worker session"}
        description="Process slots and consumers are separate observations of the same worker."
      />
      {!valid ? (
        <Empty>This link does not contain a valid worker session ID.</Empty>
      ) : !config.capabilities.workers ? (
        <Empty>Worker observations are unavailable on this server.</Empty>
      ) : (
        <>
          <CopyText value={workerSessionId} label="Copy worker session ID" />
          {query.isPending && (
            <LoadingState label="Loading worker observation" />
          )}
          {query.error && (
            <>
              {!detail &&
                query.error instanceof ApiError &&
                query.error.status === 404 && (
                  <p className="notice">
                    This worker session was not found or is no longer retained.
                  </p>
                )}
              <QueryError
                error={query.error}
                retry={() => void query.refetch()}
                stale={!!detail}
              />
            </>
          )}
          {detail && (
            <>
              <section
                className="card worker-overview"
                aria-label="Worker summary"
              >
                <WorkerSummary
                  worker={detail.worker}
                  observedAt={detail.slots.observed_at}
                />
                <details className="worker-session-details">
                  <summary>Session details</summary>
                  <Fields>
                    <Field label="Session expiry">
                      <When value={detail.worker.session_expires_at} />
                    </Field>
                    <Field label="Snapshot sequence">
                      {detail.worker.snapshot_sequence ?? "Not reported"}
                    </Field>
                  </Fields>
                </details>
              </section>
              <section
                className="card worker-process-panel"
                aria-labelledby="process-slots-heading"
              >
                <div className="section-heading">
                  <div>
                    <h2 id="process-slots-heading">Process slots</h2>
                    {detail.worker.detail_state === "available" &&
                      detail.slots.items.length > 0 && (
                        <p className="muted">
                          Select a slot to inspect its reported state.
                        </p>
                      )}
                  </div>
                </div>
                {detail.worker.detail_state === "available" ? (
                  <>
                    {detail.slots.items.length ? (
                      <div className="slot-grid">
                        {detail.slots.items.map((slot) => (
                          <Button
                            key={slot.slot_id}
                            className={`slot slot-${slot.state}`}
                            variant="ghost"
                            onClick={(event) => {
                              returnFocus.current = event.currentTarget;
                              setSelection({
                                sessionId: workerSessionId,
                                slotId: slot.slot_id,
                              });
                            }}
                            aria-label={`Inspect process slot ${slot.slot_id + 1}: ${slot.state.replaceAll("_", " ")}`}
                          >
                            <span className="summary-line">
                              Slot {slot.slot_id + 1}
                              {slot.state === "empty" ? (
                                <Scan aria-hidden="true" />
                              ) : (
                                <Activity aria-hidden="true" />
                              )}
                            </span>
                            <strong className="wrap">
                              {slot.state === "empty"
                                ? "Empty process slot"
                                : (slot.program?.id ?? "Program not recorded")}
                            </strong>
                            {slot.program && (
                              <span>{slot.program.version}</span>
                            )}
                            <Status value={slot.state} />
                          </Button>
                        ))}
                      </div>
                    ) : (
                      <Empty>
                        No process slots are present on this observed page.
                      </Empty>
                    )}
                    <p className="muted">
                      Warm processes may be reused for compatible programs.
                      Empty slots describe the reported snapshot; they are not a
                      reservation of current capacity.
                    </p>
                  </>
                ) : (
                  <p className="notice">
                    {detail.worker.detail_state === "unsupported_capacity"
                      ? `Detailed reporting is unsupported for this worker’s configured capacity (${detail.worker.capacity}). The registered capacity is preserved; slots are not fabricated.`
                      : detail.worker.detail_state === "unavailable"
                        ? "The latest report cannot provide process details. The session and summary remain available."
                        : "No process snapshot has been reported for this session. Older workers can still execute work without detailed reporting."}
                  </p>
                )}
                <PageControls
                  pagination={pagination}
                  nextCursor={detail.slots.next_cursor}
                  observedAt={detail.slots.observed_at}
                  refresh={() => void query.refetch()}
                  fetching={query.isFetching}
                />
              </section>
            </>
          )}
        </>
      )}
      <Dialog
        open={selectedSlot !== null}
        onOpenChange={(open) => {
          if (!open) setSelection(null);
        }}
      >
        <DialogContent
          onCloseAutoFocus={(event) => {
            event.preventDefault();
            returnFocus.current?.focus();
          }}
        >
          <DialogTitle>
            Process slot {selectedSlot === null ? "" : selectedSlot + 1}
          </DialogTitle>
          <DialogDescription>
            This is the latest observed state for this slot in the selected
            worker session. It does not grant process control.
          </DialogDescription>
          {detail && (
            <p className="muted">
              Snapshot {detail.worker.snapshot_sequence ?? "not reported"} ·
              received <When value={detail.worker.received_at} />
            </p>
          )}
          {selected ? (
            <SlotDetails slot={selected} />
          ) : (
            <p className="notice">
              This slot is not present in the current observed page.
            </p>
          )}
        </DialogContent>
      </Dialog>
    </>
  );
}

function WorkerSummary({
  worker,
  observedAt,
}: {
  worker: dto.Worker;
  observedAt: number;
}) {
  const age =
    worker.received_at === null
      ? null
      : Math.max(0, Math.floor((observedAt - worker.received_at) / 1000));
  return (
    <div className="worker-summary">
      <div className="summary-line worker-status-line">
        <Status value={worker.freshness} />
        <span className="muted">
          {worker.session_expired ? "Session expired" : "Session registered"}
        </span>
      </div>
      <dl className="worker-context">
        <Field label="Queue">{worker.queue}</Field>
        <Field label="Acquisition">
          {worker.accepting === null
            ? "Not reported"
            : worker.accepting
              ? "Accepting (reported)"
              : "Not accepting (reported)"}
        </Field>
      </dl>
      <dl className="worker-metrics">
        <Field label="Configured capacity">{worker.capacity}</Field>
        <Field label="Active consumers">
          {worker.active_consumers ?? "Not reported"}
        </Field>
        <Field label="Occupied process slots">
          {worker.occupied_process_slots ?? "Not reported"}
        </Field>
      </dl>
      {worker.freshness !== "fresh" && (
        <p className="notice">
          {worker.freshness === "stale"
            ? "The last report is stale. Process state and free capacity may have changed."
            : worker.freshness === "no_recent_report"
              ? "No recent report. Retained observations do not prove the worker or its processes are still running."
              : "Worker reporting is unavailable. Registered capacity is not proof of current free capacity."}
        </p>
      )}
      {worker.session_expired && (
        <p className="notice">
          The session has expired. This alone does not prove its operating
          system processes have stopped.
        </p>
      )}
      <p className="muted worker-report">
        Last report <When value={worker.received_at} />
        {age !== null && <span> · {age} s before this observation</span>}
      </p>
    </div>
  );
}

function SlotDetails({ slot }: { slot: Decoded<typeof dto.workerSlot> }) {
  const validatedLinks = slot.link_diagnostic === null;
  return (
    <>
      <Fields>
        <Field label="Observed state">
          <Status value={slot.state} />
        </Field>
        <Field label="Process instance">
          {slot.process_instance_id ? (
            <CopyText value={slot.process_instance_id} />
          ) : (
            "Not reported"
          )}
        </Field>
        <Field label="PID">{slot.process_id ?? "Not reported"}</Field>
        <Field label="Program">
          {slot.program
            ? `${slot.program.id} / ${slot.program.version}`
            : "Not recorded"}
        </Field>
        <Field label="Artifact digest">
          {slot.digest ? <CopyText value={slot.digest} /> : "Not recorded"}
        </Field>
        <Field label="Consumer ID">{slot.consumer_id ?? "Not assigned"}</Field>
      </Fields>
      {slot.state === "empty" && (
        <p>Empty process slot at the time of this report.</p>
      )}
      {slot.state === "warm" && (
        <p>
          Warm process, reusable only for a compatible program and execution
          context.
        </p>
      )}
      {!validatedLinks && (
        <p className="notice">
          {slot.link_diagnostic === "authority_mismatch"
            ? "The reported execution association could not be validated. No execution links are shown."
            : "Execution association could not be checked. No execution links are shown."}
        </p>
      )}
      {validatedLinks && slot.task_id && (
        <div className="actions">
          <Link to={`/executions/${encodeURIComponent(slot.task_id)}`}>
            Open observed execution
          </Link>
          {slot.attempt_id && (
            <Link
              to={`/executions/${encodeURIComponent(slot.task_id)}?tab=Attempts&attempt=${encodeURIComponent(slot.attempt_id)}`}
            >
              Inspect observed attempt
            </Link>
          )}
        </div>
      )}
    </>
  );
}
