// SPDX-License-Identifier: MIT
import { useEffect, useRef } from "react";
import { ListChecks } from "lucide-react";
import "../styles/lists.css";
import { Link, useLocation, useSearchParams } from "react-router";
import { useInstance } from "../app/instance";
import {
  approvalFor,
  approvalInboxPage,
  approvalStatuses,
} from "../api/approvals";
import { useApprovalRead } from "../api/use-approval-read";
import { usePagination, useResource } from "../api/hooks";
import { ApiError, retryableRead } from "../api/errors";
import { observedWorkflow, terminal } from "../api/resources";
import { ContractError } from "../api/codecs";
import { LoadingState, ErrorState } from "../components/async-state";
import {
  BackLink,
  Empty,
  PageHeading,
  QueryError,
  Status,
  When,
} from "../components/resource-ui";
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "../components/ui/table";
import { Button } from "../components/ui/button";
import { filterTimestamp } from "./execution-history-filters";
import { ApprovalCard } from "./approvals";

export function ApprovalInboxPage() {
  const [params, setParams] = useSearchParams();
  const cutoff = params.get("due_before");
  const dueBefore = filterTimestamp(cutoff);
  if (cutoff !== null && dueBefore === null)
    return (
      <>
        <PageHeading title="Approvals" />
        <ErrorState
          title="Invalid deadline filter"
          message="The due-before value in this link is outside the supported UTC date range."
        />
        <Button
          variant="outline"
          onClick={() => {
            const next = new URLSearchParams(params);
            next.delete("due_before");
            next.delete("cursor");
            setParams(next);
          }}
        >
          Clear deadline filter
        </Button>
      </>
    );
  return <FilteredInbox dueBefore={dueBefore} />;
}

function FilteredInbox({ dueBefore }: { dueBefore: number | null }) {
  const { polling, capabilities } = useInstance();
  const location = useLocation();
  const [params, setParams] = useSearchParams();
  const paging = usePagination();
  const statusParam = params.get("status");
  const status =
    statusParam === "all"
      ? null
      : (approvalStatuses.find((s) => s === statusParam) ?? "pending");
  const program = params.get("program_id") || null;
  const input = {
    status,
    program_id: program,
    ...(dueBefore === null ? {} : { due_before: dueBefore }),
    cursor: paging.cursor,
    limit: paging.limit,
  };
  const query = useApprovalRead("inbox", input, approvalInboxPage(input), () =>
    paging.cursor ? false : polling.lists_ms,
  );
  if (!capabilities.workflows)
    return (
      <>
        <PageHeading title="Approvals" />
        <Empty>Workflows are unavailable on this server.</Empty>
      </>
    );
  return (
    <>
      <PageHeading
        title="Approvals"
        description="Review requested actions and their decisions across workflows."
      />
      <section className="resource-list">
        <header className="resource-list-heading">
          <ListChecks aria-hidden="true" />
          <div>
            <h2>Approval inbox</h2>
            <p>
              Earliest deadline first. Deadlines are shown in your local time.
            </p>
          </div>
        </header>
        <form
          aria-label="Filter approval requests"
          className="filters list-filters"
          key={JSON.stringify([status, program, dueBefore])}
          onSubmit={(event) => {
            event.preventDefault();
            const data = new FormData(event.currentTarget);
            const next = new URLSearchParams();
            const state = String(data.get("status"));
            const id = String(data.get("program_id") ?? "").trim();
            if (state !== "pending") next.set("status", state);
            if (id) next.set("program_id", id);
            const cutoff = String(data.get("due_before") ?? "");
            if (cutoff)
              next.set("due_before", String(Date.parse(cutoff + "Z")));
            setParams(next);
          }}
        >
          <label>
            Status
            <select name="status" defaultValue={status ?? "all"}>
              <option value="all">All statuses</option>
              {approvalStatuses.map((s) => (
                <option key={s} value={s}>
                  {s[0]!.toUpperCase() + s.slice(1)}
                </option>
              ))}
            </select>
          </label>
          <label>
            Program ID
            <input
              name="program_id"
              defaultValue={program ?? ""}
              placeholder="All programs"
              maxLength={128}
            />
          </label>
          <label>
            Due before (UTC)
            <input
              name="due_before"
              type="datetime-local"
              step="any"
              min="1970-01-01T00:00"
              max="9999-12-31T23:59:59.999"
              defaultValue={
                dueBefore === null
                  ? ""
                  : new Date(dueBefore).toISOString().slice(0, -1)
              }
            />
          </label>
          <Button variant="outline" type="submit">
            Apply filters
          </Button>
        </form>
        {query.isPending && <LoadingState label="Loading approval requests" />}
        {query.error && (
          <QueryError
            error={query.error}
            stale={!!query.data}
            retry={() => void query.refetch()}
          />
        )}
        {query.data && (
          <>
            {query.data.items.length ? (
              <div
                className="approval-table-scroll"
                role="region"
                aria-label="Approval inbox results"
                tabIndex={0}
              >
                <Table
                  className="approval-requests"
                  aria-label="Approval requests"
                >
                  <TableHeader>
                    <TableRow>
                      <TableHead>Requested action</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead aria-sort="ascending">Deadline</TableHead>
                      <TableHead>Program / workflow</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {query.data.items.map((item) => {
                      const identity = new URLSearchParams({
                        workflow: item.workflow_id,
                        key: item.key,
                      });
                      return (
                        <TableRow key={identity.toString()}>
                          <TableCell>
                            <Link
                              to={`/approvals/review?${identity}`}
                              state={{
                                returnTo: location.pathname + location.search,
                              }}
                            >
                              {item.action_name}
                            </Link>
                            <div className="resource-id">{item.key}</div>
                          </TableCell>
                          <TableCell>
                            <Status value={item.status} />
                          </TableCell>
                          <TableCell>
                            <When value={item.deadline} />
                          </TableCell>
                          <TableCell>
                            {item.program_id} · {item.program_version}
                            <div className="resource-id">
                              {item.workflow_id}
                            </div>
                          </TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              </div>
            ) : (
              <Empty filtered>
                {status === "pending"
                  ? "No pending approval requests match this view."
                  : "No approval requests match this view."}
              </Empty>
            )}
            {(paging.cursor || query.data.next_cursor) && (
              <nav className="page-controls" aria-label="Approval pages">
                <Button
                  variant="outline"
                  disabled={!paging.cursor}
                  onClick={paging.back}
                >
                  {paging.previous.length ? "Previous" : "First page"}
                </Button>
                <Button
                  variant="outline"
                  disabled={!query.data.next_cursor}
                  onClick={() => {
                    if (query.data.next_cursor)
                      paging.next(query.data.next_cursor);
                  }}
                >
                  Next
                </Button>
              </nav>
            )}
          </>
        )}
        {query.isError && !query.data && paging.cursor && (
          <Button variant="outline" onClick={paging.reset}>
            First page
          </Button>
        )}
      </section>
    </>
  );
}

export function ApprovalReviewPage() {
  const { capabilities } = useInstance();
  const [params] = useSearchParams();
  const workflow = params.get("workflow");
  const key = params.get("key");
  return (
    <>
      <BackLink to="/approvals">Back to approvals</BackLink>
      <PageHeading title="Review approval" />
      {!capabilities.workflows ? (
        <Empty>Workflows are unavailable on this server.</Empty>
      ) : workflow && key ? (
        <ApprovalReview
          key={JSON.stringify([workflow, key])}
          workflow={workflow}
          approvalKey={key}
        />
      ) : (
        <ErrorState
          title="Incomplete approval link"
          message="Choose a request from the approvals inbox."
        />
      )}
    </>
  );
}

function ApprovalReview({
  workflow,
  approvalKey,
}: {
  workflow: string;
  approvalKey: string;
}) {
  const { polling } = useInstance();
  const query = useApprovalRead(
    "inspect",
    { workflow_id: workflow, key: approvalKey },
    approvalFor(workflow, approvalKey),
    (item) =>
      !item || item.status === "pending" ? polling.waiting_workflow_ms : false,
  );
  return (
    <>
      {query.isPending && <LoadingState label="Loading approval details" />}
      {query.error && (
        <QueryError
          error={query.error}
          stale={!!query.data}
          retry={() => void query.refetch()}
        />
      )}
      {query.data && (
        <>
          <p className="wrap">
            Workflow{" "}
            <Link
              to={`/workflows/${encodeURIComponent(workflow)}?${new URLSearchParams(
                {
                  tab: "Graph",
                  approval_key: approvalKey,
                  approval_activation: query.data.activation_id,
                },
              )}`}
            >
              {workflow}
            </Link>
          </p>
          <ApprovalCard
            showAttribution
            approval={query.data}
            unavailable={
              query.error instanceof ApiError && query.error.status === 404
            }
          />
          {query.data.status !== "pending" &&
            query.data.status !== "cancelled" &&
            query.data.resumed_activation_id === null &&
            (!query.error || retryableRead(query.error)) && (
              <ResumeObservation
                workflow={workflow}
                fetching={query.isFetching}
                refresh={query.refetch}
              />
            )}
        </>
      )}
    </>
  );
}

function ResumeObservation({
  workflow,
  fetching,
  refresh,
}: {
  workflow: string;
  fetching: boolean;
  refresh: () => Promise<unknown>;
}) {
  const { polling } = useInstance();
  const state = useResource(
    "workflows/status",
    { workflow_id: workflow },
    (value) => {
      const result = observedWorkflow(value);
      if (result.workflow.workflow_id !== workflow)
        throw new ContractError(
          "The observed workflow does not match this approval.",
        );
      return result;
    },
    {
      interval: (value) =>
        value && terminal(value.workflow.state)
          ? false
          : polling.waiting_workflow_ms,
    },
  );
  // A decision and its resumed activation commit separately. Read the approval
  // after each status observation, including the last one after workflow closure.
  // Wait out earlier in-flight reads so the final observation cannot join one.
  const observed = state.isSuccess
    ? state.dataUpdatedAt
    : state.error instanceof ApiError && state.error.status === 404
      ? state.errorUpdatedAt
      : 0;
  const lastRead = useRef(0);
  useEffect(() => {
    if (observed && !fetching && lastRead.current !== observed) {
      lastRead.current = observed;
      void refresh();
    }
  }, [observed, fetching, refresh]);
  return state.error ? (
    <QueryError error={state.error} retry={() => void state.refetch()} />
  ) : null;
}
