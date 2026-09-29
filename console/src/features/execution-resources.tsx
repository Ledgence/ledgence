// SPDX-License-Identifier: MIT
import { useState } from "react";
import { Link } from "react-router";
import { useInstance } from "../app/instance";
import { usePagination, useResource } from "../api/hooks";
import { useTerminalRefresh } from "../api/terminal-refresh";
import { attemptObservations } from "../api/explorer";
import { ContractError } from "../api/codecs";
import * as dto from "../api/resources";
import { LoadingState } from "../components/async-state";
import {
  Empty,
  Field,
  Fields,
  PageControls,
  QueryError,
  Status,
  When,
} from "../components/resource-ui";
import { Button } from "../components/ui/button";
import { micros } from "./explorer-model";
export function AttemptResources({
  attemptId,
  active = false,
}: {
  attemptId: string;
  active?: boolean;
}) {
  const config = useInstance();
  const query = useResource(
    "attempts/observations",
    { attempt_id: attemptId },
    (value) => {
      const result = attemptObservations(value);
      if (result.attempt_id !== attemptId)
        throw new ContractError("The measurements belong to another attempt.");
      return result;
    },
    {
      interval: (data) =>
        active && !data?.observations ? config.polling.active_task_ms : false,
    },
  );
  useTerminalRefresh(active, [attemptId], query);
  const observation = query.data?.observations;
  return (
    <section className="resource-measurements">
      <h3>Attempt resources</h3>
      <Button
        variant="outline"
        disabled={query.isFetching}
        onClick={() => void query.refetch()}
      >
        Refresh measurements
      </Button>
      {query.isPending && <LoadingState label="Loading measurements" />}
      {query.error && (
        <QueryError
          error={query.error}
          retry={() => void query.refetch()}
          stale={!!query.data}
        />
      )}
      {query.data &&
        (observation ? (
          <>
            <Fields>
              <Field label="Runtime started">
                <When value={observation.runtime_started_at_ms} />
              </Field>
              <Field label="Runtime elapsed">
                {micros(observation.runtime_elapsed_us)}
              </Field>
              <Field label="Process CPU · user">
                {observation.process_cpu_user_us === null
                  ? "Unsupported by this runtime"
                  : micros(observation.process_cpu_user_us)}
              </Field>
              <Field label="Process CPU · system">
                {observation.process_cpu_system_us === null
                  ? "Unsupported by this runtime"
                  : micros(observation.process_cpu_system_us)}
              </Field>
              <Field label="Process lifetime peak memory">
                {observation.process_lifetime_peak_rss_bytes === null
                  ? "Unsupported by this runtime"
                  : `${observation.process_lifetime_peak_rss_bytes} bytes`}
              </Field>
              <Field label="Provider usage">Not recorded by the platform</Field>
            </Fields>
            <p className="muted">
              CPU covers the Python process and all its threads during this
              invocation. Child processes are excluded. Peak memory covers the
              process lifetime, including earlier invocations when the process
              is reused; it is not this invocation’s peak.
            </p>
            {observation.local_steps.length > 0 && (
              <details>
                <summary>
                  Local observations ({observation.local_steps.length})
                </summary>
                <ul className="resource-local-list">
                  {observation.local_steps.map((local, index) => (
                    <li key={`${local.key}:${index}`}>
                      <strong>{local.key}</strong> ·{" "}
                      <Status value={local.state} /> ·{" "}
                      {micros(local.elapsed_us)}
                      <span className="cell-secondary">
                        <When value={local.started_at_ms} />
                      </span>
                    </li>
                  ))}
                </ul>
                <p className="muted">
                  Returned means the callable returned. Durable acceptance is
                  recorded separately; replay does not invoke the callable
                  again.
                </p>
              </details>
            )}
            {observation.local_steps_truncated && (
              <p className="notice">
                Partial local observations: this attempt exceeded the report
                limit.
              </p>
            )}
          </>
        ) : (
          <p className="notice">
            Measurements were not recorded yet, or are unavailable for this
            attempt. Older workers and interrupted processes may not report
            measurements. Missing values are not zero.
          </p>
        ))}
    </section>
  );
}
export function TaskResources({
  taskId,
  active,
}: {
  taskId: string;
  active: boolean;
}) {
  const config = useInstance();
  const paging = usePagination("resource_");
  const [selected, setSelected] = useState<string | null>(null);
  const query = useResource(
    "tasks/attempts",
    { task_id: taskId, limit: paging.limit, cursor: paging.cursor },
    dto.attemptPage,
    {
      interval:
        active && !paging.cursor ? config.polling.active_task_ms : false,
    },
  );
  useTerminalRefresh(active, [taskId, paging.cursor, paging.limit], query);
  const attempt = selected ?? query.data?.items[0]?.attempt_id;
  return (
    <>
      <p className="muted">
        Inspect one attempt at a time. Waiting time is not active runtime, and
        memory peaks are not additive.
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
          <div className="attempt-options">
            {query.data.items.map((item) => (
              <Button
                key={item.attempt_id}
                variant="outline"
                aria-pressed={attempt === item.attempt_id}
                onClick={() => setSelected(item.attempt_id)}
              >
                Attempt {item.generation} · {item.state}
              </Button>
            ))}
          </div>
          {!query.data.items.length && (
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
      {selected &&
        query.data &&
        !query.data.items.some((item) => item.attempt_id === selected) && (
          <p className="notice">
            The selected attempt is not on this page.{" "}
            <Button variant="ghost" onClick={() => setSelected(null)}>
              Select an attempt on this page
            </Button>
          </p>
        )}
      {attempt && (
        <AttemptResources
          key={attempt}
          attemptId={attempt}
          active={
            query.data?.items.some(
              (item) => item.attempt_id === attempt && item.state === "active",
            ) ?? false
          }
        />
      )}
    </>
  );
}
export function WorkflowResources({
  workflowId,
  active,
}: {
  workflowId: string;
  active: boolean;
}) {
  const config = useInstance();
  const paging = usePagination("resource_");
  const query = useResource(
    "workflows/activations",
    { workflow_id: workflowId, limit: paging.limit, cursor: paging.cursor },
    dto.activationPage,
    {
      interval:
        active && !paging.cursor ? config.polling.waiting_workflow_ms : false,
    },
  );
  useTerminalRefresh(active, [workflowId, paging.cursor, paging.limit], query);
  return (
    <>
      <p className="muted">
        Workflow elapsed time includes durable waiting. Inspect controller
        attempts or open a child execution for its own measurements. Shared
        process measurements cannot be attributed exclusively to concurrent
        local functions.
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
          <ul className="resource-local-list">
            {query.data.items.map((item) => (
              <li key={item.activation_id}>
                <Link
                  to={`/executions/${encodeURIComponent(item.task_id)}?tab=Resources`}
                >
                  Controller phase {item.revision} resources
                </Link>{" "}
                · <Status value={item.state} />
              </li>
            ))}
          </ul>
          {!query.data.items.length && (
            <Empty>No controller attempts are available on this page.</Empty>
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
    </>
  );
}
