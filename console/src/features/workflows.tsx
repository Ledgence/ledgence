import { useState } from "react";
import { Link, useLocation, useParams, useSearchParams } from "react-router";
import { useQueryClient } from "@tanstack/react-query";
import { useInstance } from "../app/instance";
import { useResource, usePagination } from "../api/hooks";
import { useTerminalRefresh } from "../api/terminal-refresh";
import * as dto from "../api/resources";
import { useCommand, freezeCommand } from "../api/commands";
import { parseUserJson } from "../api/json";
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
  Status,
  When,
  CopyText,
  PageControls,
  QueryError,
  Empty,
  BackLink,
  Tabs,
  Fields,
  Field,
  JsonView,
} from "../components/resource-ui";
import { CommandFeedback } from "../components/command-feedback";
import { Filters } from "./filters";
export function WorkflowsPage() {
  const config = useInstance();
  const [params] = useSearchParams();
  const paging = usePagination();
  const query = useResource(
    "workflows",
    {
      limit: paging.limit,
      cursor: paging.cursor,
      state: params.get("state"),
      correlation_key: params.get("correlation_key"),
      submitted_from: params.get("submitted_from"),
      submitted_until: params.get("submitted_until"),
      parent_workflow_id: params.get("parent_workflow_id"),
      root_only: params.get("root_only"),
    },
    dto.workflowPage,
    {
      enabled: config.capabilities.workflows,
      interval: paging.cursor ? false : config.polling.lists_ms,
    },
  );
  return (
    <>
      <PageHeading
        title="Workflows"
        description="Durable controllers, recorded work and external waits."
        actions={
          <Link className="button button-outline" to="/agents">
            Choose a controller
          </Link>
        }
      />
      {!config.capabilities.workflows ? (
        <Empty>Workflows are unavailable on this server.</Empty>
      ) : (
        <>
          <Filters kind="workflows" />
          <div className="table-panel">
            {query.isPending && <LoadingState label="Loading workflows" />}
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
                          "Workflow",
                          "Controller",
                          "Status",
                          "Revision",
                          "Correlation",
                          "Lineage",
                          "Submitted",
                        ].map((h) => (
                          <th key={h}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {query.data.items.map(({ workflow, controller }) => (
                        <tr key={workflow.workflow_id}>
                          <td data-label="Workflow">
                            <Link
                              state={{
                                returnTo: `/workflows${params.size ? `?${params}` : ""}`,
                              }}
                              to={`/workflows/${encodeURIComponent(workflow.workflow_id)}`}
                            >
                              {workflow.workflow_id}
                            </Link>
                          </td>
                          <td data-label="Controller">
                            {controller.program.id}
                            <span className="cell-secondary">
                              {controller.program.version}
                            </span>
                          </td>
                          <td data-label="Status">
                            <Status value={workflow.state} />
                          </td>
                          <td data-label="Revision">{workflow.revision}</td>
                          <td data-label="Correlation">
                            {workflow.correlation_key ?? "Not set"}
                          </td>
                          <td data-label="Lineage">
                            {workflow.parent_workflow_id ? (
                              <Link
                                to={`/workflows/${encodeURIComponent(workflow.parent_workflow_id)}`}
                              >
                                Parent workflow
                              </Link>
                            ) : (
                              "Root workflow"
                            )}
                          </td>
                          <td data-label="Submitted">
                            <When value={workflow.submitted_at} />
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </Table>
                ) : (
                  <Empty filtered={params.size > 0}>
                    Start a registered workflow controller or adjust the exact
                    filters.
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
export function WorkflowDetailPage() {
  const { workflowId = "" } = useParams();
  const [params] = useSearchParams();
  const config = useInstance();
  const requestedTab = params.get("tab") ?? "Recorded work";
  const tab = [
    "Recorded work",
    "Waits",
    "Local steps",
    "History",
    "Result",
    "Context",
  ].includes(requestedTab)
    ? requestedTab
    : "Recorded work";
  const query = useResource(
    "workflows/inspect",
    { workflow_id: workflowId },
    dto.workflowDetail,
    {
      interval: (d) =>
        d && dto.terminal(d.summary.workflow.state)
          ? false
          : config.polling.waiting_workflow_ms,
    },
  );
  const detail = query.data;
  return (
    <>
      <BackLink to="/workflows">Workflows</BackLink>
      <PageHeading
        title="Workflow"
        actions={
          detail &&
          !dto.terminal(detail.summary.workflow.state) && (
            <CancelWorkflow workflowId={workflowId} />
          )
        }
      />
      <CopyText value={workflowId} label="Copy workflow ID" />
      {query.isPending && <LoadingState />}
      {query.error && (
        <QueryError
          error={query.error}
          retry={() => void query.refetch()}
          stale={!!detail}
        />
      )}{" "}
      {detail && (
        <>
          <div className="summary-line">
            <Status value={detail.summary.workflow.state} />
            <span>Revision {detail.summary.workflow.revision}</span>
            <span className="muted">
              Observed <When value={detail.observed_at} />
            </span>
          </div>
          {["failing", "cancelling"].includes(
            detail.summary.workflow.state,
          ) && (
            <p className="notice">
              This workflow is draining owned children. The current state is not
              terminal.
            </p>
          )}
          <Tabs
            values={[
              "Recorded work",
              "Waits",
              "Local steps",
              "History",
              "Result",
              "Context",
            ]}
            current={tab}
          />
          {tab === "Recorded work" && (
            <RecordedWork
              workflowId={workflowId}
              active={!dto.terminal(detail.summary.workflow.state)}
            />
          )}{" "}
          {tab === "Waits" && (
            <Waits
              workflowId={workflowId}
              active={!dto.terminal(detail.summary.workflow.state)}
            />
          )}{" "}
          {tab === "Local steps" && (
            <LocalSteps
              workflowId={workflowId}
              currentActivation={detail.summary.workflow.activation_id}
            />
          )}{" "}
          {tab === "History" && <WorkflowHistory workflowId={workflowId} />}{" "}
          {tab === "Result" && <WorkflowResult workflowId={workflowId} />}{" "}
          {tab === "Context" && (
            <section className="card">
              <Fields>
                <Field label="Controller">
                  {detail.summary.controller.program.id} /{" "}
                  {detail.summary.controller.program.version}
                </Field>
                <Field label="Digest">
                  <CopyText value={detail.summary.controller.digest} />
                </Field>
                <Field label="Queue">{detail.summary.queue}</Field>
                <Field label="Continuation">{detail.continuation}</Field>
                <Field label="Current activation">
                  {detail.summary.workflow.activation_id ? (
                    <Link
                      to={`/executions/${encodeURIComponent(detail.summary.workflow.activation_id)}`}
                    >
                      {detail.summary.workflow.activation_id}
                    </Link>
                  ) : (
                    "None"
                  )}
                </Field>
                <Field label="Parent">
                  {detail.summary.workflow.parent_workflow_id ? (
                    <Link
                      to={`/workflows/${encodeURIComponent(detail.summary.workflow.parent_workflow_id)}`}
                    >
                      {detail.summary.workflow.parent_workflow_id}
                    </Link>
                  ) : (
                    "Root workflow"
                  )}
                </Field>
                <Field label="Root">
                  {detail.summary.workflow.root_workflow_id ? (
                    <Link
                      to={`/workflows/${encodeURIComponent(detail.summary.workflow.root_workflow_id)}`}
                    >
                      {detail.summary.workflow.root_workflow_id}
                    </Link>
                  ) : (
                    workflowId
                  )}
                </Field>
                <Field label="Submitted">
                  <When value={detail.summary.workflow.submitted_at} />
                </Field>
                <Field label="Terminal">
                  <When value={detail.summary.workflow.terminal_at} />
                </Field>
              </Fields>
            </section>
          )}
        </>
      )}
    </>
  );
}
function RecordedWork({
  workflowId,
  active,
}: {
  workflowId: string;
  active: boolean;
}) {
  const config = useInstance();
  const activations = usePagination("activation_");
  const children = usePagination("child_");
  const max = Math.max(
    1,
    Math.floor(config.limits.max_visible_workflow_nodes / 2),
  );
  const aq = useResource(
    "workflows/activations",
    {
      workflow_id: workflowId,
      limit: Math.min(activations.limit, max),
      cursor: activations.cursor,
    },
    dto.activationPage,
    {
      interval:
        active && !activations.cursor
          ? config.polling.waiting_workflow_ms
          : false,
    },
  );
  const cq = useResource(
    "workflows/children",
    {
      workflow_id: workflowId,
      limit: Math.min(children.limit, max),
      cursor: children.cursor,
    },
    dto.childPage,
    {
      interval:
        active && !children.cursor ? config.polling.waiting_workflow_ms : false,
    },
  );
  useTerminalRefresh(
    active,
    [workflowId, activations.cursor, Math.min(activations.limit, max)],
    aq,
    !activations.cursor,
  );
  useTerminalRefresh(
    active,
    [workflowId, children.cursor, Math.min(children.limit, max)],
    cq,
    !children.cursor,
  );
  const [showCompleted, setShowCompleted] = useState(false);
  const visibleChildren = (cq.data?.items ?? []).filter(
    (c) =>
      showCompleted || !dto.terminal(c.task_state ?? c.workflow_state ?? ""),
  );
  const unmatched = visibleChildren.filter(
    (c) =>
      !aq.data?.items.some((a) => a.activation_id === c.creating_activation_id),
  );
  return (
    <section className="recorded-work">
      <div className="section-heading">
        <h2>Recorded work</h2>
        <label className="check-label">
          <input
            type="checkbox"
            checked={showCompleted}
            onChange={(e) => setShowCompleted(e.target.checked)}
          />
          Show completed children
        </label>
      </div>
      <p className="muted">
        Relationships below come from recorded creation IDs. These pages are
        live observations, not a complete frozen graph.
      </p>
      {aq.isPending && <LoadingState />}
      {aq.error && (
        <QueryError
          error={aq.error}
          retry={() => void aq.refetch()}
          stale={!!aq.data}
        />
      )}{" "}
      {cq.error && (
        <QueryError
          error={cq.error}
          retry={() => void cq.refetch()}
          stale={!!cq.data}
        />
      )}{" "}
      {aq.data && (
        <>
          <div className="workflow-graph">
            {aq.data.items.map((a) => (
              <section className="activation-group" key={a.activation_id}>
                <div className="card activation-node">
                  <span className="eyebrow">
                    Activation · revision {a.revision}
                  </span>
                  <Link to={`/executions/${encodeURIComponent(a.task_id)}`}>
                    {a.activation_id}
                  </Link>
                  <Status value={a.state} />
                  <span className="muted">
                    Applied <When value={a.applied_at} />
                  </span>
                  {a.error && (
                    <p>
                      {a.error.kind}: {a.error.message}
                    </p>
                  )}
                </div>
                <div className="child-nodes">
                  {visibleChildren
                    .filter((c) => c.creating_activation_id === a.activation_id)
                    .map((c) => (
                      <ChildNode
                        key={`${c.kind}:${c.command_key}:${c.target_id}`}
                        child={c}
                      />
                    ))}
                </div>
              </section>
            ))}
          </div>
          {!aq.data.items.length && (
            <Empty>No activations were recorded on this page.</Empty>
          )}
          <h3>Activation pages</h3>
          <PageControls
            pagination={activations}
            nextCursor={aq.data.next_cursor}
            observedAt={aq.data.observed_at}
            refresh={() => void aq.refetch()}
            fetching={aq.isFetching}
          />
        </>
      )}
      {unmatched.length > 0 && (
        <section>
          <h3>Children created outside this activation page</h3>
          <div className="card-grid">
            {unmatched.map((c) => (
              <ChildNode
                key={`${c.kind}:${c.command_key}:${c.target_id}`}
                child={c}
              />
            ))}
          </div>
        </section>
      )}
      {cq.data && (
        <>
          <h3>Recorded child pages</h3>
          <p className="muted">
            {cq.data.items.length} child records loaded; completed children{" "}
            {showCompleted ? "included" : "collapsed"}.
          </p>
          <PageControls
            pagination={children}
            nextCursor={cq.data.next_cursor}
            observedAt={cq.data.observed_at}
            refresh={() => void cq.refetch()}
            fetching={cq.isFetching}
          />
        </>
      )}
    </section>
  );
}
function ChildNode({ child }: { child: ReturnType<typeof dto.child> }) {
  return (
    <article className="card child-node">
      <span className="eyebrow">
        {child.kind} · {child.command_key}
      </span>
      <Link
        to={`/${child.kind === "task" ? "executions" : "workflows"}/${encodeURIComponent(child.target_id)}`}
      >
        {child.target_id}
      </Link>
      <Status value={child.task_state ?? child.workflow_state ?? "unknown"} />
      <span className="muted">
        Created by{" "}
        <Link
          to={`/executions/${encodeURIComponent(child.creating_activation_id)}`}
        >
          {child.creating_activation_id}
        </Link>
      </span>
      <span>{child.consumed ? "Result consumed" : "Result not consumed"}</span>
    </article>
  );
}
function Waits({
  workflowId,
  active,
}: {
  workflowId: string;
  active: boolean;
}) {
  const config = useInstance();
  const paging = usePagination();
  const query = useResource(
    "workflows/waits",
    { workflow_id: workflowId, limit: paging.limit, cursor: paging.cursor },
    dto.workflowWaits,
    {
      interval:
        active && !paging.cursor ? config.polling.waiting_workflow_ms : false,
    },
  );
  useTerminalRefresh(
    active,
    [workflowId, paging.cursor, paging.limit],
    query,
    !paging.cursor,
  );
  return (
    <>
      {query.isPending && <LoadingState />}
      {query.error && (
        <QueryError
          error={query.error}
          retry={() => void query.refetch()}
          stale={!!query.data}
        />
      )}{" "}
      {query.data && (
        <>
          {query.data.child_wait && (
            <div className="notice">
              <h2>Waiting for registered children</h2>
              <p>Activation {query.data.child_wait.activation_id}</p>
              <ul>
                {query.data.child_wait.command_keys.map((key) => (
                  <li key={key}>
                    <code>{key}</code>
                  </li>
                ))}
              </ul>
            </div>
          )}
          <div className="card-grid">
            {query.data.page.items.map((wait) => (
              <article className="card" key={wait.wait_key}>
                <h3>{wait.kind === "event" ? "External event" : "Timer"}</h3>
                <CopyText value={wait.wait_key} />
                <Status value={wait.closed_at === null ? "open" : "closed"} />
                <p>
                  Registered <When value={wait.registered_at} />
                </p>
                {wait.deadline !== null && (
                  <p>
                    Deadline <When value={wait.deadline} />
                  </p>
                )}
                {wait.closed_at !== null && (
                  <p>
                    Closed <When value={wait.closed_at} />
                  </p>
                )}
                {wait.kind === "event" && (
                  <SendEvent
                    workflowId={workflowId}
                    waitKey={wait.wait_key}
                    enabled={active && wait.closed_at === null}
                  />
                )}
              </article>
            ))}
          </div>
          {!query.data.page.items.length && !query.data.child_wait && (
            <Empty>No external waits were recorded on this page.</Empty>
          )}
          <PageControls
            pagination={paging}
            nextCursor={query.data.page.next_cursor}
            observedAt={query.data.page.observed_at}
            refresh={() => void query.refetch()}
            fetching={query.isFetching}
          />
        </>
      )}
    </>
  );
}
function LocalSteps({
  workflowId,
  currentActivation,
}: {
  workflowId: string;
  currentActivation: string | null;
}) {
  const [params, set] = useSearchParams();
  const location = useLocation();
  const activation = params.get("activation") ?? currentActivation;
  const paging = usePagination();
  const query = useResource(
    "workflows/local-steps",
    {
      workflow_id: workflowId,
      activation_id: activation,
      limit: paging.limit,
      cursor: paging.cursor,
    },
    dto.localStepPage,
    { enabled: !!activation },
  );
  return (
    <>
      <form
        className="filters"
        key={activation}
        onSubmit={(e) => {
          e.preventDefault();
          const id = new FormData(e.currentTarget).get("activation");
          if (typeof id === "string" && id) {
            const next = new URLSearchParams(params);
            next.set("activation", id);
            next.delete("cursor");
            next.delete("previous");
            set(next, { state: location.state });
          }
        }}
      >
        <label>
          Activation ID
          <input required name="activation" defaultValue={activation ?? ""} />
        </label>
        <Button type="submit" variant="outline">
          Load steps
        </Button>
      </form>
      {!activation ? (
        <Empty>Choose an activation to inspect its accepted local steps.</Empty>
      ) : (
        <>
          {query.isPending && <LoadingState />}
          {query.error && (
            <QueryError
              error={query.error}
              retry={() => void query.refetch()}
            />
          )}{" "}
          {query.data && (
            <>
              <p className="muted">
                Accepted local checkpoints; these are not distributed task
                attempts.
              </p>
              <Table className="responsive-table">
                <thead>
                  <tr>
                    <th>Step key</th>
                    <th>Callable</th>
                    <th>Attempt</th>
                    <th>Accepted</th>
                  </tr>
                </thead>
                <tbody>
                  {query.data.items.map((step) => (
                    <tr key={step.step_key}>
                      <td data-label="Step key">{step.step_key}</td>
                      <td data-label="Callable">{step.callable}</td>
                      <td data-label="Attempt">{step.attempt_id}</td>
                      <td data-label="Accepted">
                        <When value={step.accepted_at} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </Table>
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
      )}
    </>
  );
}
function WorkflowHistory({ workflowId }: { workflowId: string }) {
  const paging = usePagination();
  const query = useResource(
    "workflows/history",
    { workflow_id: workflowId, limit: paging.limit, cursor: paging.cursor },
    dto.workflowHistoryPage,
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
                <span>{event.activation_id ?? "Workflow lifecycle"}</span>
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
function WorkflowResult({ workflowId }: { workflowId: string }) {
  const config = useInstance();
  const query = useResource(
    "workflows/result",
    { workflow_id: workflowId },
    dto.workflowResult,
    {
      maximumBytes: 10 * 1024 * 1024,
      interval: (d) =>
        d && dto.terminal(d.workflow.state)
          ? false
          : config.polling.waiting_workflow_ms,
    },
  );
  return (
    <>
      {query.isPending && <LoadingState />}
      {query.error && (
        <QueryError error={query.error} retry={() => void query.refetch()} />
      )}{" "}
      {query.data &&
        (query.data.outcome === null ? (
          <p className="notice">
            Result pending. The workflow is {query.data.workflow.state}.
          </p>
        ) : (
          <>
            <h2>Workflow {query.data.outcome.kind}</h2>
            <JsonView
              value={
                query.data.outcome.kind === "succeeded"
                  ? query.data.outcome.output
                  : query.data.outcome
              }
              label="Workflow result"
            />
          </>
        ))}
    </>
  );
}
function CancelWorkflow({ workflowId }: { workflowId: string }) {
  const [open, setOpen] = useState(false);
  const client = useQueryClient();
  const command = useCommand("workflows/cancel", dto.observedWorkflow, () => {
    setOpen(false);
    void client.invalidateQueries();
  });
  return (
    <>
      <Button variant="outline" onClick={() => setOpen(true)}>
        Cancel workflow
      </Button>
      <Dialog
        open={open}
        onOpenChange={(next) => {
          if (!command.mutation.isPending) setOpen(next);
        }}
      >
        <DialogContent>
          <DialogTitle>Cancel this workflow?</DialogTitle>
          <DialogDescription>
            Cancellation applies to this workflow and its owned children.
            Running work may drain before a terminal state is observed. Effects
            already performed are not undone.
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
                command.send(
                  freezeCommand({ workflow_id: workflowId }, workflowId),
                )
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
function SendEvent({
  workflowId,
  waitKey,
  enabled,
}: {
  workflowId: string;
  waitKey: string;
  enabled: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [input, setInput] = useState("");
  const [validation, setValidation] = useState("");
  const client = useQueryClient();
  const config = useInstance();
  const command = useCommand(
    "workflows/events",
    dto.eventReceipt,
    () => void client.invalidateQueries(),
  );
  function submit() {
    try {
      const event = parseUserJson(input, config.limits.input_max_bytes);
      if (
        !event ||
        typeof event !== "object" ||
        !("specversion" in event) ||
        event.specversion !== "1.0" ||
        !("id" in event) ||
        typeof event.id !== "string" ||
        !event.id ||
        !("source" in event) ||
        typeof event.source !== "string" ||
        !event.source ||
        !("type" in event) ||
        typeof event.type !== "string" ||
        !event.type
      )
        throw new Error(
          "Supply a complete CloudEvent with specversion 1.0, id, source and type.",
        );
      command.send(
        freezeCommand(
          { workflow_id: workflowId, key: waitKey, event },
          `${waitKey} | ${event.source} | ${event.id}`,
        ),
      );
      setValidation("");
    } catch (error) {
      setValidation(error instanceof Error ? error.message : "Invalid event.");
    }
  }
  return (
    <>
      <Button
        variant="outline"
        disabled={!enabled}
        onClick={() => setOpen(true)}
      >
        Send event
      </Button>
      <Dialog
        open={open}
        onOpenChange={(next) => {
          if (!command.mutation.isPending) setOpen(next);
        }}
      >
        <DialogContent>
          <DialogTitle>Send external event</DialogTitle>
          <DialogDescription>
            Wait key: {waitKey}. Acceptance records the event; workflow progress
            is observed separately.
          </DialogDescription>
          {command.mutation.data ? (
            <div className="notice" role="status">
              <h3>Event accepted</h3>
              <p>
                {command.mutation.data.already_accepted
                  ? "This event was already accepted."
                  : "The event was recorded."}
              </p>
              <p>
                Accepted <When value={command.mutation.data.accepted_at} />
              </p>
            </div>
          ) : (
            <>
              <label>
                Complete CloudEvent JSON
                <textarea
                  className="json-editor"
                  rows={10}
                  disabled={command.command !== null}
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  spellCheck={false}
                />
              </label>
              {validation && <p role="alert">{validation}</p>}
              {command.mutation.error && command.command ? (
                <CommandFeedback
                  error={command.mutation.error}
                  identity={command.command.identity}
                  retry={command.retry}
                  reset={command.reset}
                />
              ) : (
                <Button
                  disabled={command.mutation.isPending || !input}
                  onClick={submit}
                >
                  {command.mutation.isPending ? "Sending…" : "Send event"}
                </Button>
              )}
            </>
          )}
        </DialogContent>
      </Dialog>
    </>
  );
}
