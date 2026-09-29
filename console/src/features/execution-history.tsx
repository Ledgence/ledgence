// SPDX-License-Identifier: MIT
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
  When,
} from "../components/resource-ui";
import { Button } from "../components/ui/button";
import { Table } from "../components/ui/table";

const filterKeys = [
  "kind",
  "state",
  "program_id",
  "version",
  "queue",
  "correlation_key",
  "execution_id",
  "submitted_from",
  "submitted_until",
] as const;
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
  const [params, setParams] = useSearchParams();
  const paging = usePagination();
  const includesChildren =
    params.get("include_children") === "true" ||
    !!params.get("program_id") ||
    !!params.get("execution_id");
  const filters = Object.fromEntries(
    filterKeys.map((key) => [key, params.get(key)]),
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
  function changeKind(kind: string) {
    const next = new URLSearchParams(params);
    if (kind) next.set("kind", kind);
    else next.delete("kind");
    next.delete("cursor");
    next.delete("previous");
    // States specific to the other execution type cannot match the new filter.
    if (
      (kind === "task" &&
        ["running", "waiting", "failing", "cancelling"].includes(
          next.get("state") ?? "",
        )) ||
      (kind === "workflow" &&
        ["queued", "active"].includes(next.get("state") ?? ""))
    )
      next.delete("state");
    setParams(next);
  }
  return (
    <>
      <PageHeading
        title="Executions"
        description="Tasks and workflows submitted to this instance."
        actions={
          (config.capabilities.executions || config.capabilities.workflows) && (
            <Link className="button button-primary" to="/executions/new">
              <Plus aria-hidden="true" />
              New execution
            </Link>
          )
        }
      />
      <nav className="tabs" aria-label="Execution type">
        {[
          ["", "All"],
          ["task", "Tasks"],
          ["workflow", "Workflows"],
        ].map(([kind, label]) => (
          <Button
            key={kind}
            variant="ghost"
            aria-pressed={(params.get("kind") ?? "") === kind}
            onClick={() => changeKind(kind ?? "")}
          >
            {label}
          </Button>
        ))}
      </nav>
      <HistoryFilters />
      <p className="muted list-caption">
        {includesChildren
          ? "Root and child executions."
          : "Root workflows and standalone tasks."}
      </p>
      {!config.capabilities.executions && !config.capabilities.workflows ? (
        <Empty>Executions are unavailable on this server.</Empty>
      ) : (
        <div className="table-panel">
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
                <Table className="responsive-table execution-table">
                  <thead>
                    <tr>
                      {[
                        "Execution",
                        "Type",
                        "Status",
                        "Program / version",
                        "Submitted",
                        "Scope",
                      ].map((label) => (
                        <th key={label}>{label}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {query.data.items.map((item) => (
                      <tr key={`${item.kind}:${item.id}`}>
                        <td data-label="Execution">
                          <Link
                            className="execution-name"
                            title={
                              item.correlation_key || item.descriptor.program.id
                            }
                            to={executionPath(item.kind, item.id)}
                            state={{
                              returnTo: `/executions${params.size ? `?${params}` : ""}`,
                            }}
                          >
                            {item.correlation_key || item.descriptor.program.id}
                          </Link>
                          <span
                            className="cell-secondary execution-id"
                            title={item.id}
                          >
                            {item.id}
                          </span>
                        </td>
                        <td data-label="Type">
                          {item.kind === "task" ? "Task" : "Workflow"}
                        </td>
                        <td data-label="Status">
                          <Status value={item.state} />
                        </td>
                        <td data-label="Program / version">
                          <Link
                            to={`/programs/${encodeURIComponent(item.descriptor.program.id)}/versions/${encodeURIComponent(item.descriptor.program.version)}`}
                          >
                            {item.descriptor.program.id}
                          </Link>
                          <span className="cell-secondary">
                            {item.descriptor.program.version}
                          </span>
                        </td>
                        <td data-label="Submitted">
                          <When value={item.submitted_at} />
                        </td>
                        <td data-label="Scope">
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
      )}
    </>
  );
}
function dateValue(params: URLSearchParams, key: string) {
  const value = params.get(key);
  if (value === null) return "";
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 0 || number > 253402300799999)
    return "";
  const date = new Date(number);
  return new Date(number - date.getTimezoneOffset() * 60000)
    .toISOString()
    .slice(0, 16);
}
function HistoryFilters() {
  const [params, setParams] = useSearchParams();
  const implicitChildren =
    !!params.get("program_id") || !!params.get("execution_id");
  const activeFilters =
    filterKeys.filter((key) => key !== "kind" && params.has(key)).length +
    (params.get("include_children") === "true" ? 1 : 0);
  const advancedFilters = [
    "version",
    "execution_id",
    "queue",
    "correlation_key",
    "submitted_from",
    "submitted_until",
  ].filter((key) => params.has(key)).length;
  const states =
    params.get("kind") === "task"
      ? ["queued", "active", "succeeded", "failed", "cancelled"]
      : params.get("kind") === "workflow"
        ? [
            "running",
            "waiting",
            "failing",
            "cancelling",
            "succeeded",
            "failed",
            "cancelled",
          ]
        : [
            "queued",
            "active",
            "running",
            "waiting",
            "failing",
            "cancelling",
            "succeeded",
            "failed",
            "cancelled",
          ];
  return (
    <form
      className="filters history-filters"
      key={params.toString()}
      onSubmit={(event) => {
        event.preventDefault();
        const form = new FormData(event.currentTarget);
        const next = new URLSearchParams();
        if (params.get("kind")) next.set("kind", params.get("kind") ?? "");
        if (params.get("limit")) next.set("limit", params.get("limit") ?? "");
        for (const key of filterKeys.filter(
          (key) => key !== "kind" && key !== "correlation_key",
        )) {
          const value = String(form.get(key) ?? "");
          if (!value) continue;
          if (key.startsWith("submitted_")) {
            const at = Date.parse(value);
            if (Number.isFinite(at)) next.set(key, String(at));
          } else next.set(key, value);
        }
        if (form.get("correlation_enabled"))
          next.set(
            "correlation_key",
            String(form.get("correlation_key") ?? ""),
          );
        if (form.get("include_children")) next.set("include_children", "true");
        setParams(next);
      }}
    >
      <div className="filter-primary">
        <label>
          Status
          <select name="state" defaultValue={params.get("state") ?? ""}>
            <option value="">All statuses</option>
            {states.map((state) => (
              <option key={state} value={state}>
                {state === "active"
                  ? "Active task"
                  : state === "running"
                    ? "Running workflow"
                    : state === "failing"
                      ? "Stopping after failure"
                      : state}
              </option>
            ))}
          </select>
        </label>
        <label>
          Program
          <input
            name="program_id"
            defaultValue={params.get("program_id") ?? ""}
          />
        </label>
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
            aria-description={
              implicitChildren
                ? "Program and execution ID searches always include child executions."
                : undefined
            }
            defaultChecked={
              implicitChildren || params.get("include_children") === "true"
            }
          />
          Include child executions
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
                if (params.get("kind")) next.set("kind", params.get("kind")!);
                if (params.get("limit"))
                  next.set("limit", params.get("limit")!);
                setParams(next);
              }}
            >
              Clear
            </Button>
          )}
        </div>
      </div>
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
            Exact version
            <input name="version" defaultValue={params.get("version") ?? ""} />
          </label>
          <label>
            Exact execution ID
            <input
              name="execution_id"
              defaultValue={params.get("execution_id") ?? ""}
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
          <label>
            Submitted from
            <input
              type="datetime-local"
              name="submitted_from"
              defaultValue={dateValue(params, "submitted_from")}
            />
          </label>
          <label>
            Submitted until (exclusive)
            <input
              type="datetime-local"
              name="submitted_until"
              defaultValue={dateValue(params, "submitted_until")}
            />
          </label>
        </div>
      </details>
    </form>
  );
}
