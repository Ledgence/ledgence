// SPDX-License-Identifier: MIT
import { useContext } from "react";
import { Link, useLocation, useNavigate } from "react-router";
import { ArrowLeft, ArrowUp } from "lucide-react";
import { useResource } from "../api/hooks";
import { ancestry, executionPath } from "../api/explorer";
import { ContractError } from "../api/codecs";
import {
  contextDestination,
  NavigationHistoryContext,
} from "../app/navigation-state";
import { Button } from "./ui/button";
import { QueryError } from "./resource-ui";
export function ExecutionContext({
  kind,
  id,
  title,
}: {
  kind: "task" | "workflow";
  id: string;
  title?: string;
}) {
  const location = useLocation();
  const navigate = useNavigate();
  const canGoBack = useContext(NavigationHistoryContext);
  const query = useResource("executions/ancestry", { kind, id }, (value) => {
    const result = ancestry(value);
    if (result.execution.kind !== kind || result.execution.id !== id)
      throw new ContractError("The ancestry belongs to a different execution.");
    return result;
  });
  const path = query.data?.path ?? [];
  const parent = path.at(-2);
  const ancestors = path.slice(0, -1);
  const current = path.at(-1);
  const returnTo =
    location.state &&
    typeof location.state.returnTo === "string" &&
    /^\/(executions|workflows)(\?|$)/.test(location.state.returnTo)
      ? (location.state.returnTo as string)
      : null;
  const returnNavigationKey =
    location.state &&
    typeof location.state.returnNavigationKey === "string" &&
    location.state.returnNavigationKey.length > 0
      ? (location.state.returnNavigationKey as string)
      : null;
  const listDestination = returnTo
    ? {
        to: returnTo,
        ...(returnNavigationKey
          ? { state: { restoreNavigationKey: returnNavigationKey } }
          : {}),
      }
    : contextDestination("/executions");
  const itemLink = (item: (typeof path)[number]) => (
    <Link
      {...contextDestination(
        executionPath(item.execution.kind, item.execution.id),
      )}
    >
      {item.program?.id ?? item.execution.id}
      {item.availability === "unavailable" ? " (unavailable)" : ""}
    </Link>
  );
  return (
    <div
      className={`execution-context${title ? " execution-heading-context" : ""}`}
    >
      <div className="context-actions">
        <Button
          variant="ghost"
          title={title ? "Back" : undefined}
          onClick={() => {
            if (canGoBack) void navigate(-1);
            else
              void navigate(listDestination.to, {
                state: listDestination.state,
              });
          }}
        >
          <ArrowLeft aria-hidden="true" />
          <span className={title ? "sr-only" : undefined}>Back</span>
        </Button>
        {parent && (
          <Link
            className="back-link"
            title={title ? "Up to parent" : undefined}
            {...contextDestination(
              executionPath(parent.execution.kind, parent.execution.id),
            )}
          >
            <ArrowUp aria-hidden="true" />
            <span className={title ? "sr-only" : undefined}>Up to parent</span>
          </Link>
        )}
      </div>
      <nav aria-label="Execution ancestry" className="breadcrumbs">
        <ol>
          <li>
            <Link {...listDestination}>Executions</Link>
          </li>
          {ancestors.length > 3 ? (
            <>
              <li>{itemLink(ancestors[0]!)}</li>
              <li>
                <details>
                  <summary>{ancestors.length - 2} ancestors</summary>
                  <ol>
                    {ancestors.slice(1, -1).map((item) => (
                      <li key={`${item.execution.kind}:${item.execution.id}`}>
                        {itemLink(item)}
                      </li>
                    ))}
                  </ol>
                </details>
              </li>
              <li>{itemLink(ancestors.at(-1)!)}</li>
            </>
          ) : (
            ancestors.map((item) => (
              <li key={`${item.execution.kind}:${item.execution.id}`}>
                {itemLink(item)}
              </li>
            ))
          )}
          <li aria-current="page">
            {title ? (
              <h1 title={title}>{title}</h1>
            ) : (
              (current?.program?.id ?? (kind === "task" ? "Task" : "Workflow"))
            )}
          </li>
        </ol>
      </nav>
      {query.error && (
        <details className="context-error">
          <summary>Execution ancestry unavailable</summary>
          <QueryError
            error={query.error}
            retry={() => void query.refetch()}
            stale={!!query.data}
          />
        </details>
      )}
    </div>
  );
}
