// SPDX-License-Identifier: MIT
import { useState } from "react";
import { Link, Navigate, useLocation, useSearchParams } from "react-router";
import { Plus } from "lucide-react";
import { useInstance } from "../app/instance";
import { usePagination, useResource } from "../api/hooks";
import { executionPage, executionPath } from "../api/explorer";
import { LoadingState } from "../components/async-state";
import {
  Empty,
  PageControls,
  PageHeading,
  QueryError,
  Status,
} from "../components/resource-ui";
import { Button } from "../components/ui/button";
import {
  applyHistoryFilters,
  compatibleHistoryState,
  executionFilterKeys,
  formatHistoryElapsed,
  hasExactHistoryDates,
  historyDateValue,
  historyElapsed,
  historyStates,
} from "./execution-history-filters";
import "../styles/execution-history.css";

export function LegacyWorkflowsRedirect() {
  const location = useLocation();
  const params = new URLSearchParams(location.search);
  params.set("kind", "workflow");
  params.delete("cursor");
  params.delete("previous");
  return (
    <Navigate to={`/executions?${params}`} replace state={location.state} />
  );
}
export function ExecutionsPage() {
  const config = useInstance();
  const [params] = useSearchParams();
  const paging = usePagination();
  const includesChildren =
    params.get("include_children") === "true" ||
    !!params.get("program_id") ||
    !!params.get("execution_id");
  const filters = Object.fromEntries(
    executionFilterKeys.map((key) => [key, params.get(key)]),
  );
  const query = useResource(
    "executions",
    {
      ...filters,
      include_children: includesChildren ? "true" : "false",
      limit: paging.limit,
      cursor: paging.cursor,
    },
    executionPage,
    {
      enabled: config.capabilities.executions || config.capabilities.workflows,
      interval: paging.cursor ? false : config.polling.lists_ms,
    },
  );
  const filtered =
    executionFilterKeys.some((key) => params.has(key)) ||
    params.get("include_children") === "true";
  return (
    <>
      <PageHeading
        title="Executions"
        description="Follow your tasks and workflows, from submission to result."
        actions={
          (config.capabilities.executions || config.capabilities.workflows) && (
            <Link className="button button-primary" to="/executions/new">
              <Plus aria-hidden="true" />
              New execution
            </Link>
          )
        }
      />
      <HistoryFilters key={params.toString()} />
      <p className="muted list-caption">
        {includesChildren
          ? "Root and child executions."
          : "Root workflows and standalone tasks."}{" "}
        <span>Submitted dates are shown in UTC.</span>
      </p>
      {!config.capabilities.executions && !config.capabilities.workflows ? (
        <Empty>Executions are unavailable on this server.</Empty>
      ) : (
        <div className="table-panel history-table-panel">
          {query.isPending && <LoadingState label="Loading executions" />}
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
                <div
                  className="history-table-scroll"
                  role="region"
                  aria-label="Execution history"
                  tabIndex={0}
                >
                  <table className="table execution-table history-table">
                    <caption className="sr-only">
                      Tasks and workflows. Scroll horizontally to view all
                      columns on smaller screens.
                    </caption>
                    <thead>
                      <tr>
                        {[
                          "Execution",
                          "Status",
                          "Type",
                          "Program / version",
                          "Submitted · UTC",
                          "Elapsed",
                          ...(includesChildren ? ["Scope"] : []),
                        ].map((label) => (
                          <th scope="col" key={label}>
                            {label}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {query.data.items.map((item) => {
                        const elapsed = historyElapsed(
                          item,
                          query.data!.observed_at,
                        );
                        return (
                          <tr key={`${item.kind}:${item.id}`}>
                            <td>
                              <Link
                                className="execution-name"
                                title={
                                  item.correlation_key ||
                                  item.descriptor.program.id
                                }
                                to={executionPath(item.kind, item.id)}
                                state={{
                                  returnTo: `/executions${params.size ? `?${params}` : ""}`,
                                }}
                              >
                                {item.correlation_key ||
                                  item.descriptor.program.id}
                              </Link>
                              <span
                                className="cell-secondary execution-id"
                                title={item.id}
                              >
                                {item.id}
                              </span>
                            </td>
                            <td>
                              <Status value={item.state} />
                            </td>
                            <td>
                              <span className="execution-kind">
                                {item.kind === "task" ? "Task" : "Workflow"}
                              </span>
                            </td>
                            <td>
                              <Link
                                className="execution-program"
                                title={item.descriptor.program.id}
                                to={`/programs/${encodeURIComponent(item.descriptor.program.id)}/versions/${encodeURIComponent(item.descriptor.program.version)}`}
                              >
                                {item.descriptor.program.id}
                              </Link>
                              <span
                                className="cell-secondary execution-version"
                                title={item.descriptor.program.version}
                              >
                                {item.descriptor.program.version}
                              </span>
                            </td>
                            <td>
                              <SubmittedAt value={item.submitted_at} />
                            </td>
                            <td>
                              <span
                                className={
                                  elapsed === null ? "muted" : "history-elapsed"
                                }
                                title={
                                  elapsed === null
                                    ? undefined
                                    : item.terminal_at === null
                                      ? "Time since submission at the last observation, including time waiting."
                                      : "Time from submission to completion, including time waiting."
                                }
                              >
                                {elapsed === null
                                  ? "Not recorded"
                                  : formatHistoryElapsed(elapsed)}
                              </span>
                            </td>
                            {includesChildren && (
                              <td>
                                {item.parent_workflow_id ? (
                                  <Link
                                    to={executionPath(
                                      "workflow",
                                      item.parent_workflow_id,
                                    )}
                                  >
                                    Child execution
                                  </Link>
                                ) : (
                                  "Root"
                                )}
                              </td>
                            )}
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              ) : (
                <Empty filtered={filtered}>
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
      )}
    </>
  );
}
function SubmittedAt({ value }: { value: number }) {
  const date = new Date(value);
  const iso = date.toISOString();
  return (
    <time className="history-submitted" dateTime={iso} title={iso}>
      {date.toLocaleDateString("en", {
        timeZone: "UTC",
        month: "short",
        day: "numeric",
        year: "numeric",
      })}
      <span className="cell-secondary">{iso.slice(11, 19)}</span>
    </time>
  );
}
export function HistoryFilters() {
  const [params, setParams] = useSearchParams();
  const [kind, setKind] = useState(params.get("kind") ?? "");
  const [state, setState] = useState(params.get("state") ?? "");
  const [program, setProgram] = useState(params.get("program_id") ?? "");
  const [executionId, setExecutionId] = useState(
    params.get("execution_id") ?? "",
  );
  const [error, setError] = useState("");
  const [includeChildren, setIncludeChildren] = useState(
    params.get("include_children") === "true",
  );
  const implicitChildren = !!program || !!executionId;
  const activeFilters =
    executionFilterKeys.filter((key) => params.has(key)).length +
    (params.get("include_children") === "true" ? 1 : 0);
  const advancedFilters =
    [
      "program_id",
      "version",
      "execution_id",
      "queue",
      "correlation_key",
    ].filter((key) => params.has(key)).length +
    (params.get("include_children") === "true" ? 1 : 0);
  return (
    <form
      className="filters history-filters history-filters--calendar"
      aria-label="Filter executions"
      onSubmit={(event) => {
        event.preventDefault();
        const result = applyHistoryFilters(
          params,
          new FormData(event.currentTarget),
        );
        if (!result.ok) {
          setError(result.message);
          return;
        }
        setError("");
        setParams(result.params);
      }}
    >
      <div className="filter-primary">
        <label>
          Submitted from · UTC
          <input
            type="date"
            name="submitted_from"
            min="1970-01-01"
            max="9999-12-31"
            defaultValue={historyDateValue(params, "submitted_from")}
            aria-invalid={error ? true : undefined}
            aria-describedby={error ? "history-date-error" : undefined}
          />
        </label>
        <label>
          Submitted through · UTC
          <input
            type="date"
            name="submitted_until"
            min="1970-01-01"
            max="9999-12-30"
            defaultValue={historyDateValue(params, "submitted_until")}
            aria-invalid={error ? true : undefined}
            aria-describedby={error ? "history-date-error" : undefined}
          />
        </label>
        <label>
          Type
          <select
            name="kind"
            value={kind}
            onChange={(event) => {
              setKind(event.target.value);
              setState(compatibleHistoryState(event.target.value, state));
            }}
          >
            <option value="">All types</option>
            <option value="task">Task</option>
            <option value="workflow">Workflow</option>
          </select>
        </label>
        <label>
          Status
          <select
            name="state"
            value={state}
            onChange={(event) => setState(event.target.value)}
          >
            <option value="">All statuses</option>
            {historyStates(kind).map((value) => (
              <option key={value} value={value}>
                {value === "active"
                  ? "Active task"
                  : value === "running"
                    ? "Running workflow"
                    : value === "failing"
                      ? "Stopping after failure"
                      : value[0]!.toUpperCase() + value.slice(1)}
              </option>
            ))}
          </select>
        </label>
        <div className="filter-actions">
          <Button type="submit" variant="outline">
            Apply filters
          </Button>
          {activeFilters > 0 && (
            <Button
              type="button"
              variant="ghost"
              onClick={() => {
                const next = new URLSearchParams();
                if (params.has("limit"))
                  next.set("limit", params.get("limit")!);
                setParams(next);
              }}
            >
              Clear
            </Button>
          )}
        </div>
      </div>
      {error && (
        <p
          id="history-date-error"
          className="history-filter-error"
          role="alert"
        >
          {error}
        </p>
      )}
      {hasExactHistoryDates(params) && (
        <p className="history-date-note muted">
          This link uses exact timestamps. Unchanged dates keep that range.
        </p>
      )}
      <details
        className="advanced-filters"
        open={advancedFilters > 0 || undefined}
      >
        <summary>
          More filters
          {advancedFilters > 0 && (
            <span className="filter-count">{advancedFilters} active</span>
          )}
        </summary>
        <div className="filter-extra">
          <label>
            Exact program ID
            <input
              name="program_id"
              value={program}
              onChange={(event) => setProgram(event.target.value)}
            />
          </label>
          <label>
            Exact version
            <input name="version" defaultValue={params.get("version") ?? ""} />
          </label>
          <label>
            Exact execution ID
            <input
              name="execution_id"
              value={executionId}
              onChange={(event) => setExecutionId(event.target.value)}
            />
          </label>
          <label>
            Queue
            <input name="queue" defaultValue={params.get("queue") ?? ""} />
          </label>
          <div>
            <label>
              Exact correlation
              <input
                name="correlation_key"
                defaultValue={params.get("correlation_key") ?? ""}
              />
            </label>
            <label className="check-label">
              <input
                type="checkbox"
                name="correlation_enabled"
                defaultChecked={params.has("correlation_key")}
              />
              Apply correlation, including empty
            </label>
          </div>
          <div className="history-scope-filter">
            {implicitChildren && includeChildren && (
              <input type="hidden" name="include_children" value="true" />
            )}
            <label
              className="check-label"
              title={
                implicitChildren
                  ? "Program and execution ID searches always include child executions."
                  : undefined
              }
            >
              <input
                type="checkbox"
                name="include_children"
                disabled={implicitChildren}
                checked={implicitChildren || includeChildren}
                onChange={(event) => setIncludeChildren(event.target.checked)}
              />
              Include child executions
            </label>
            {implicitChildren && (
              <p className="muted">
                Included for exact program or execution searches.
              </p>
            )}
          </div>
        </div>
      </details>
    </form>
  );
}
