// SPDX-License-Identifier: MIT
import { Link, useLocation, useParams, useSearchParams } from "react-router";
import { Plus } from "lucide-react";
import { useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useInstance } from "../app/instance";
import { useResource, usePagination } from "../api/hooks";
import { useTerminalRefresh } from "../api/terminal-refresh";
import * as dto from "../api/resources";
import { useCommand, freezeCommand } from "../api/commands";
import { Button } from "../components/ui/button";
import { Table } from "../components/ui/table";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from "../components/ui/dialog";
import { LoadingState } from "../components/async-state";
import {
  PageHeading,
  Elapsed,
  Status,
  When,
  CopyText,
  PageControls,
  QueryError,
  Empty,
  Fields,
  Field,
  JsonView,
} from "../components/resource-ui";
import { CommandFeedback } from "../components/command-feedback";
import { Filters, ExecutionStatusTabs } from "./filters";
import { ExecutionContext } from "../components/execution-context";
import { TaskResources, AttemptResources } from "./execution-resources";
import { DetailPanels, DetailSections } from "./detail-panels";
export function ExecutionsPage() {
  const config = useInstance();
  const [params] = useSearchParams();
  const paging = usePagination();
  const query = useResource(
    "tasks",
    {
      limit: paging.limit,
      cursor: paging.cursor,
      state: params.get("state"),
      queue: params.get("queue"),
      correlation_key: params.get("correlation_key"),
      submitted_from: params.get("submitted_from"),
      submitted_until: params.get("submitted_until"),
    },
    dto.taskPage,
    {
      enabled: config.capabilities.executions,
      interval: paging.cursor ? false : config.polling.lists_ms,
    },
  );
  return (
    <>
      <PageHeading
        title="Executions"
        description="Inspect work submitted to this instance."
        actions={
          config.capabilities.executions && (
            <Link className="button button-primary" to="/executions/new">
              <Plus aria-hidden="true" />
              New execution
            </Link>
          )
        }
      />
      {!config.capabilities.executions ? (
        <Empty>Executions are unavailable on this server.</Empty>
      ) : (
        <>
          <ExecutionStatusTabs />
          <Filters kind="tasks" />
          <div className="table-panel">
            {query.isPending && <LoadingState label="Loading executions" />}
            {query.error && (
              <QueryError
                error={query.error}
                retry={() => void query.refetch()}
                stale={!!query.data}
              />
            )}{" "}
            {query.data && (
              <>
                {query.data.items.length ? (
                  <Table className="responsive-table">
                    <thead>
                      <tr>
                        {[
                          "Execution",
                          "Program / version",
                          "Status",
                          "Queue",
                          "Correlation",
                          "Attempts",
                          "Submitted",
                        ].map((h) => (
                          <th key={h}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {query.data.items.map(({ task, descriptor }) => (
                        <tr key={task.task_id}>
                          <td data-label="Execution">
                            <Link
                              state={{
                                returnTo: `/executions${params.size ? `?${params}` : ""}`,
                              }}
                              to={`/executions/${encodeURIComponent(task.task_id)}`}
                            >
                              {task.task_id}
                            </Link>
                          </td>
                          <td data-label="Program / version">
                            <Link
                              to={`/programs/${encodeURIComponent(descriptor.program.id)}/versions/${encodeURIComponent(descriptor.program.version)}`}
                            >
                              {descriptor.program.id}
                            </Link>
                            <span className="cell-secondary">
                              {descriptor.program.version}
                            </span>
                          </td>
                          <td data-label="Status">
                            <Status value={task.state} />
                            {task.cancel_requested_at !== null &&
                              !dto.terminal(task.state) && (
                                <span className="cell-secondary">
                                  Cancellation requested
                                </span>
                              )}
                          </td>
                          <td data-label="Queue">{task.queue}</td>
                          <td data-label="Correlation">
                            {task.correlation_key ?? (
                              <span className="muted">Not set</span>
                            )}
                          </td>
                          <td data-label="Attempts">{task.attempt_count}</td>
                          <td data-label="Submitted">
                            <When value={task.submitted_at} />
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </Table>
                ) : (
                  <Empty filtered={params.size > 0}>
                    Submit an execution or adjust the exact filters.
                  </Empty>
                )}
                <PageControls
                  pagination={paging}
                  nextCursor={query.data.next_cursor}
                  observedAt={query.data.observed_at}
                  refresh={() => void query.refetch()}
                  fetching={query.isFetching}
                />
              </>
            )}
          </div>
        </>
      )}
    </>
  );
}
export function ExecutionDetailPage() {
  const { taskId = "" } = useParams();
  const location = useLocation();
  const config = useInstance();
  const query = useResource(
    "tasks/status",
    { task_id: taskId },
    dto.observedTask,
    {
      interval: (d) =>
        d && dto.terminal(d.task.state) ? false : config.polling.active_task_ms,
    },
  );
  const task = query.data?.task;
  return (
    <>
      <ExecutionContext kind="task" id={taskId} />
      <PageHeading
        title={task?.correlation_key || "Task execution"}
        metadata={
          <>
            {task && (
              <>
                <Status value={task.state} />
                <Elapsed
                  start={task.submitted_at}
                  end={
                    task.terminal_at ??
                    query.data?.observed_at ??
                    task.submitted_at
                  }
                />
                {task.cancel_requested_at !== null &&
                  !dto.terminal(task.state) && (
                    <span>Cancellation requested; awaiting final state</span>
                  )}
              </>
            )}
            <CopyText value={taskId} label="Copy task ID" />
          </>
        }
        actions={
          task && (
            <>
              <Link
                className="button button-outline"
                to={`/executions/new?source_task=${encodeURIComponent(taskId)}`}
              >
                Run again
              </Link>
              {!dto.terminal(task.state) && (
                <CancelTask key={taskId} taskId={taskId} />
              )}
            </>
          )
        }
      />
      {query.isPending && <LoadingState />}
      {query.error && (
        <QueryError
          error={query.error}
          retry={() => void query.refetch()}
          stale={!!task}
        />
      )}{" "}
      {task && (
        <>
          <DetailPanels kind="task">
            {({ tab, section, openSection }) => (
              <DetailSections
                current={section}
                onChange={openSection}
                sections={
                  tab === "Trace"
                    ? [
                        {
                          id: "attempts",
                          title: "Attempts",
                          description:
                            "Worker sessions, outcomes and diagnostics for each attempt.",
                          content: (
                            <Attempts
                              taskId={taskId}
                              active={!dto.terminal(task.state)}
                            />
                          ),
                        },
                        {
                          id: "history",
                          title: "Lifecycle",
                          description: "Recorded changes to this task’s state.",
                          content: (
                            <TaskHistory
                              taskId={taskId}
                              active={!dto.terminal(task.state)}
                            />
                          ),
                        },
                      ]
                    : [
                        {
                          id: "identity",
                          title: "Execution details",
                          description:
                            "Identity, queue, timing and workflow context.",
                          content: (
                            <section className="card">
                              <Fields>
                                <Field label="Run ID">
                                  <CopyText value={task.run_id} />
                                </Field>
                                <Field label="Queue">{task.queue}</Field>
                                <Field label="Correlation">
                                  {task.correlation_key ?? "Not set"}
                                </Field>
                                <Field label="Submitted">
                                  <When value={task.submitted_at} />
                                </Field>
                                <Field label="Available">
                                  <When value={task.available_at} />
                                </Field>
                                <Field label="Terminal">
                                  <When value={task.terminal_at} />
                                </Field>
                                <Field label="Observed">
                                  <When
                                    value={query.data?.observed_at ?? null}
                                  />
                                </Field>
                                <Field label="Attempts">
                                  {task.attempt_count}
                                </Field>
                                <Field label="Current attempt">
                                  {task.current_attempt_id ? (
                                    <Link
                                      to={`?tab=Trace&section=attempts&attempt=${encodeURIComponent(task.current_attempt_id)}`}
                                      state={location.state}
                                    >
                                      {task.current_attempt_id}
                                    </Link>
                                  ) : (
                                    "None"
                                  )}
                                </Field>
                                {task.workflow_id && (
                                  <Field label="Workflow">
                                    <Link
                                      to={`/workflows/${encodeURIComponent(task.workflow_id)}`}
                                    >
                                      {task.workflow_id}
                                    </Link>
                                  </Field>
                                )}
                              </Fields>
                            </section>
                          ),
                        },
                        {
                          id: "input",
                          title: "Input",
                          description:
                            "Submitted data, program version and retry policy.",
                          content: <ExecutionInput taskId={taskId} />,
                        },
                        {
                          id: "output",
                          title: "Output",
                          description:
                            "The recorded result or failure of this task.",
                          content: <ExecutionResult taskId={taskId} />,
                        },
                        {
                          id: "resources",
                          title: "Resources",
                          description:
                            "Observed runtime and process measurements.",
                          content: (
                            <TaskResources
                              key={taskId}
                              taskId={taskId}
                              active={!dto.terminal(task.state)}
                            />
                          ),
                        },
                      ]
                }
              />
            )}
          </DetailPanels>
        </>
      )}
    </>
  );
}
function ExecutionInput({ taskId }: { taskId: string }) {
  const query = useResource(
    "tasks/inspect",
    { task_id: taskId },
    dto.taskDetail,
    { maximumBytes: 3 * 1024 * 1024, staleTime: Infinity },
  );
  return (
    <>
      {query.isPending && <LoadingState label="Loading input" />}
      {query.error && (
        <QueryError error={query.error} retry={() => void query.refetch()} />
      )}{" "}
      {query.data && (
        <>
          <section className="card">
            <Fields>
              <Field label="Program">
                {query.data.descriptor.program.id} /{" "}
                {query.data.descriptor.program.version}
              </Field>
              <Field label="Digest">
                <CopyText value={query.data.descriptor.digest} />
              </Field>
              <Field label="Submission identity">
                <CopyText value={query.data.idempotency_key} />
              </Field>
              {query.data.origin_trace && (
                <Field label="Origin trace">
                  <CopyText value={query.data.origin_trace.traceparent} />
                </Field>
              )}
              <Field label="Retry policy">
                Up to {query.data.input.retry_policy.max_attempts} attempts,{" "}
                {query.data.input.retry_policy.retry_delay_ms} ms delay
              </Field>
              <Field label="Attempt timeout">
                {query.data.input.attempt_timeout_ms} ms
              </Field>
            </Fields>
          </section>
          <JsonView value={query.data.input.data} label="Input" />
        </>
      )}
    </>
  );
}
function ExecutionResult({ taskId }: { taskId: string }) {
  const config = useInstance();
  const query = useResource(
    "tasks/result",
    { task_id: taskId },
    dto.taskResult,
    {
      maximumBytes: 10 * 1024 * 1024,
      interval: (d) =>
        d && dto.terminal(d.task.state) ? false : config.polling.active_task_ms,
    },
  );
  return (
    <>
      {query.isPending && <LoadingState label="Loading result" />}
      {query.error && (
        <QueryError
          error={query.error}
          retry={() => void query.refetch()}
          stale={!!query.data}
        />
      )}{" "}
      {query.data &&
        (query.data.outcome === null ? (
          <div className="notice">
            Result pending. The execution is {query.data.task.state}.
          </div>
        ) : (
          <>
            <h2>
              {query.data.outcome.kind === "succeeded"
                ? "Execution succeeded"
                : query.data.outcome.kind === "cancelled"
                  ? "Execution cancelled"
                  : "Execution failed"}
            </h2>
            {"quiescence" in query.data.outcome && (
              <p>
                Quiescence: {query.data.outcome.quiescence}. Execution may have
                started: {String(query.data.outcome.execution_may_have_started)}
                .
              </p>
            )}
            <JsonView
              value={
                query.data.outcome.kind === "succeeded"
                  ? query.data.outcome.output
                  : query.data.outcome
              }
              label="Result"
            />
          </>
        ))}
    </>
  );
}
function Attempts({ taskId, active }: { taskId: string; active: boolean }) {
  const config = useInstance();
  const location = useLocation();
  const [params, set] = useSearchParams();
  const paging = usePagination();
  const query = useResource(
    "tasks/attempts",
    { task_id: taskId, limit: paging.limit, cursor: paging.cursor },
    dto.attemptPage,
    {
      interval:
        active && !paging.cursor ? config.polling.active_task_ms : false,
    },
  );
  useTerminalRefresh(
    active,
    [taskId, paging.cursor, paging.limit],
    query,
    !paging.cursor,
  );
  const selected = params.get("attempt");
  return (
    <>
      {query.isPending && <LoadingState />}
      {query.error && (
        <QueryError error={query.error} retry={() => void query.refetch()} />
      )}{" "}
      {query.data && (
        <>
          {query.data.items.length ? (
            <Table className="responsive-table">
              <thead>
                <tr>
                  <th>Attempt</th>
                  <th>Generation</th>
                  <th>State</th>
                  <th>Worker session</th>
                  <th>Claimed</th>
                  <th>Finished</th>
                </tr>
              </thead>
              <tbody>
                {query.data.items.map((a) => (
                  <tr key={a.attempt_id}>
                    <td data-label="Attempt">
                      <Button
                        variant="ghost"
                        onClick={() => {
                          const p = new URLSearchParams(params);
                          p.set("attempt", a.attempt_id);
                          set(p, {
                            preventScrollReset: true,
                            state: location.state,
                          });
                        }}
                      >
                        {a.attempt_id}
                      </Button>
                    </td>
                    <td data-label="Generation">{a.generation}</td>
                    <td data-label="State">
                      <Status value={a.state} />
                    </td>
                    <td data-label="Worker session">
                      <Link
                        to={`/workers/${encodeURIComponent(a.worker_session_id)}`}
                      >
                        {a.worker_session_id}
                      </Link>
                    </td>
                    <td data-label="Claimed">
                      <When value={a.claimed_at} />
                    </td>
                    <td data-label="Finished">
                      <When value={a.finished_at} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </Table>
          ) : (
            <Empty>No attempt has been recorded yet.</Empty>
          )}
          <PageControls
            pagination={paging}
            nextCursor={query.data.next_cursor}
            observedAt={query.data.observed_at}
            refresh={() => void query.refetch()}
            fetching={query.isFetching}
          />
        </>
      )}
      {selected && <AttemptDetails attemptId={selected} />}
    </>
  );
}
function AttemptDetails({ attemptId }: { attemptId: string }) {
  const config = useInstance();
  const query = useResource(
    "attempts/inspect",
    { attempt_id: attemptId },
    dto.attemptDetail,
    {
      interval: (d) =>
        d?.attempt.state === "active" ? config.polling.active_task_ms : false,
    },
  );
  return (
    <section className="card">
      <h2>Attempt details</h2>
      {query.isPending && <LoadingState />}
      {query.error && (
        <QueryError error={query.error} retry={() => void query.refetch()} />
      )}{" "}
      {query.data && (
        <>
          <Fields>
            <Field label="Attempt ID">
              <CopyText value={attemptId} />
            </Field>
            <Field label="Quiescence">{query.data.attempt.quiescence}</Field>
            <Field label="Execution may have started">
              {String(query.data.attempt.execution_may_have_started)}
            </Field>
            <Field label="Failure stage">
              {query.data.phase ?? "Not recorded"}
            </Field>
            <Field label="Consumer">{query.data.attempt.consumer_id}</Field>
            <Field label="PID">{query.data.process_id ?? "Not recorded"}</Field>
            <Field label="Process instance">
              {query.data.process_instance_id ?? "Not recorded"}
            </Field>
            <Field label="Reused process">
              {query.data.reused_process === null
                ? "Not recorded"
                : String(query.data.reused_process)}
            </Field>
            <Field label="Worker elapsed, including preparation">
              {query.data.worker_elapsed_ms === null
                ? "Not recorded"
                : `${query.data.worker_elapsed_ms} ms`}
            </Field>
          </Fields>
          <AttemptObservationDisclosure
            attemptId={attemptId}
            active={query.data.attempt.state === "active"}
          />
          {query.data.application_error && (
            <JsonView
              label="Application error"
              value={query.data.application_error}
            />
          )}{" "}
          {query.data.error && (
            <JsonView label="Execution error" value={query.data.error} />
          )}{" "}
          {query.data.cleanup_error && (
            <JsonView label="Cleanup error" value={query.data.cleanup_error} />
          )}
        </>
      )}
    </section>
  );
}
function AttemptObservationDisclosure({
  attemptId,
  active,
}: {
  attemptId: string;
  active: boolean;
}) {
  const [open, setOpen] = useState(false);
  return (
    <details
      className="attempt-observations"
      open={open}
      onToggle={(event) => setOpen(event.currentTarget.open)}
    >
      <summary>Resource observations</summary>
      {open && <AttemptResources attemptId={attemptId} active={active} />}
    </details>
  );
}
function TaskHistory({ taskId, active }: { taskId: string; active: boolean }) {
  const config = useInstance();
  const paging = usePagination();
  const query = useResource(
    "tasks/history",
    { task_id: taskId, limit: paging.limit, cursor: paging.cursor },
    dto.taskHistoryPage,
    {
      interval:
        active && !paging.cursor ? config.polling.active_task_ms : false,
    },
  );
  useTerminalRefresh(
    active,
    [taskId, paging.cursor, paging.limit],
    query,
    !paging.cursor,
  );
  return (
    <>
      {query.isPending && <LoadingState />}
      {query.error && (
        <QueryError error={query.error} retry={() => void query.refetch()} />
      )}{" "}
      {query.data && (
        <>
          <ol className="history">
            {query.data.items.map((event) => (
              <li key={event.sequence}>
                <Status value={event.reason} />
                <When value={event.at} />
                <span>{event.attempt_id ?? "Task lifecycle"}</span>
              </li>
            ))}
          </ol>
          <PageControls
            pagination={paging}
            nextCursor={query.data.next_cursor}
            observedAt={query.data.observed_at}
            refresh={() => void query.refetch()}
            fetching={query.isFetching}
          />
        </>
      )}
    </>
  );
}
function CancelTask({ taskId }: { taskId: string }) {
  const [open, setOpen] = useState(false);
  const client = useQueryClient();
  const command = useCommand("tasks/cancel", dto.cancelTaskReceipt, () => {
    setOpen(false);
    void client.invalidateQueries();
  });
  return (
    <>
      <Button variant="outline" onClick={() => setOpen(true)}>
        Cancel execution
      </Button>
      <Dialog
        open={open}
        onOpenChange={(next) => {
          if (!command.mutation.isPending) setOpen(next);
        }}
      >
        <DialogContent>
          <DialogTitle>Cancel this execution?</DialogTitle>
          <DialogDescription>
            Running work may need cleanup. Cancellation does not undo effects
            already performed. The final state will be observed from the server.
          </DialogDescription>
          {command.mutation.error && command.command ? (
            <CommandFeedback
              error={command.mutation.error}
              identity={command.command.identity}
              retry={command.retry}
              reset={command.reset}
            />
          ) : (
            <Button
              disabled={command.mutation.isPending}
              onClick={() =>
                command.send(freezeCommand({ task_id: taskId }, taskId))
              }
            >
              {command.mutation.isPending
                ? "Requesting cancellation…"
                : "Request cancellation"}
            </Button>
          )}
        </DialogContent>
      </Dialog>
    </>
  );
}
