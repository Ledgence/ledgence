// SPDX-License-Identifier: MIT
import { useContext } from "react";
import { Link, useLocation, useNavigate, useSearchParams } from "react-router";
import { ArrowLeft, ShieldCheck } from "lucide-react";
import { useAccess, usePermission } from "../app/access-session";
import {
  NavigationHistoryContext,
  contextDestination,
} from "../app/navigation-state";
import { useInstance } from "../app/instance";
import { usePagination } from "../api/hooks";
import { useEffectRead } from "../api/use-effect-read";
import {
  effectDetail,
  effectDetailPath,
  effectFilters,
  effectInboxPage,
  type EffectAddress,
  type EffectEntry,
  type EffectSummary,
} from "../api/effects";
import { ErrorState, LoadingState } from "../components/async-state";
import {
  CopyText,
  Empty,
  Field,
  Fields,
  JsonView,
  PageHeading,
  QueryError,
  When,
} from "../components/resource-ui";
import { Button } from "../components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../components/ui/table";
import { EffectResolutionForm } from "./effect-resolution-form";
import "../styles/lists.css";

function assessmentLabel(item: EffectSummary) {
  const assessment = item.operator_resolution;
  if (!assessment) return "Unassessed";
  if (assessment.first_conflicting_entry_sequence !== null)
    return "Assessment disputed";
  return assessment.reference.assessment_kind === "close_unknown"
    ? "Closed · outcome unknown"
    : "Assessment recorded";
}
const filterLabels = {
  all: "All effects",
  unconfirmed: "Unconfirmed journal",
  confirmed: "Confirmed journal",
  discrepant: "Conflicting worker reports",
  disputed: "Disputed assessment",
};

export function EffectsPage() {
  const { access } = useAccess();
  const canRead = usePermission("metadata_read");
  const config = useInstance();
  const location = useLocation();
  const [params, setParams] = useSearchParams();
  const paging = usePagination();
  const filter =
    effectFilters.find((value) => value === params.get("filter")) ?? "all";
  const workflowId = params.get("workflow_id") || null;
  const input = {
    filter,
    workflow_id: workflowId,
    cursor: paging.cursor,
    limit: paging.limit,
  };
  const query = useEffectRead(
    "inbox",
    input,
    effectInboxPage(
      input,
      access?.mode === "credentials" ? access.scope : undefined,
    ),
    canRead,
  );
  return (
    <>
      <PageHeading
        title="Effects"
        description="Track approved external actions, retained reports and operator assessments."
      />
      {!config.capabilities.workflows ? (
        <Empty>Workflows are unavailable on this server.</Empty>
      ) : !canRead ? (
        <Empty>Your credential needs metadata_read to list effects.</Empty>
      ) : (
        <section className="resource-list">
          <header className="resource-list-heading">
            <ShieldCheck aria-hidden="true" />
            <div>
              <h2>Effect history</h2>
              <p>
                Most recently updated first. Journal acceptance and operational
                handling are shown separately.
              </p>
            </div>
          </header>
          <form
            className="filters list-filters"
            aria-label="Filter effects"
            key={`${filter}:${workflowId}`}
            onSubmit={(event) => {
              event.preventDefault();
              const values = new FormData(event.currentTarget);
              const next = new URLSearchParams();
              const chosen = String(values.get("filter"));
              if (chosen !== "all") next.set("filter", chosen);
              const workflow = String(values.get("workflow_id") ?? "").trim();
              if (workflow) next.set("workflow_id", workflow);
              setParams(next);
            }}
          >
            <label>
              Evidence filter
              <select name="filter" defaultValue={filter}>
                {effectFilters.map((value) => (
                  <option key={value} value={value}>
                    {filterLabels[value]}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Workflow ID
              <input
                name="workflow_id"
                defaultValue={workflowId ?? ""}
                placeholder="All workflows"
                maxLength={128}
              />
            </label>
            <Button type="submit" variant="outline">
              Apply filters
            </Button>
          </form>
          {query.isPending && <LoadingState label="Loading effects" />}
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
                  aria-label="Effect history results"
                  tabIndex={0}
                >
                  <Table aria-label="Effects">
                    <TableHeader>
                      <TableRow>
                        <TableHead>Approved action</TableHead>
                        <TableHead>Handling</TableHead>
                        <TableHead>Journal</TableHead>
                        <TableHead>Evidence</TableHead>
                        <TableHead>Program / workflow</TableHead>
                        <TableHead aria-sort="descending">Updated</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {query.data.items.map((item) => (
                        <TableRow key={effectDetailPath(item.occurrence)}>
                          <TableCell>
                            <Link
                              to={effectDetailPath(item.occurrence)}
                              state={{
                                returnTo: location.pathname + location.search,
                                returnNavigationKey: location.key,
                              }}
                            >
                              {item.callable}
                            </Link>
                            <div className="resource-id">
                              {item.occurrence.approval_key}
                            </div>
                          </TableCell>
                          <TableCell>{assessmentLabel(item)}</TableCell>
                          <TableCell>
                            {item.journal_state === "confirmed"
                              ? "Confirmed"
                              : "Unconfirmed"}
                          </TableCell>
                          <TableCell>
                            {item.has_discrepancy
                              ? "Conflicting reports"
                              : `${item.entry_count} entries`}
                          </TableCell>
                          <TableCell>
                            {item.program.id} · {item.program.version}
                            <div className="resource-id">
                              {item.occurrence.workflow_id}
                            </div>
                          </TableCell>
                          <TableCell>
                            <When value={item.updated_at} />
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              ) : (
                <Empty filtered>No effects match this view.</Empty>
              )}
              <nav className="page-controls" aria-label="Effect pages">
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
            </>
          )}
        </section>
      )}
    </>
  );
}

function EffectBack() {
  const location = useLocation(),
    navigate = useNavigate();
  const canGoBack = useContext(NavigationHistoryContext);
  const state: unknown = location.state;
  const returnTo =
    state &&
    typeof state === "object" &&
    "returnTo" in state &&
    typeof state.returnTo === "string" &&
    /^\/(effects|workflows|executions)(\/|\?|$)/.test(state.returnTo)
      ? state.returnTo
      : null;
  const key =
    state &&
    typeof state === "object" &&
    "returnNavigationKey" in state &&
    typeof state.returnNavigationKey === "string"
      ? state.returnNavigationKey
      : null;
  const fallback = contextDestination("/effects");
  return (
    <Button
      variant="ghost"
      onClick={() => {
        if (returnTo)
          void navigate(returnTo, {
            state: key ? { restoreNavigationKey: key } : undefined,
          });
        else if (canGoBack) void navigate(-1);
        else void navigate(fallback.to, { state: fallback.state });
      }}
    >
      <ArrowLeft aria-hidden="true" />
      Back
    </Button>
  );
}

export function EffectReviewPage() {
  const [params] = useSearchParams();
  const workflowId = params.get("workflow_id"),
    activationId = params.get("activation_id"),
    approvalKey = params.get("approval_key");
  return (
    <>
      <EffectBack />
      <PageHeading
        title="Review effect"
        description="Inspect the frozen action and evidence before recording an assessment."
      />
      {workflowId && activationId && approvalKey ? (
        <EffectReview
          key={JSON.stringify([workflowId, activationId, approvalKey])}
          address={{
            workflow_id: workflowId,
            activation_id: activationId,
            approval_key: approvalKey,
          }}
        />
      ) : (
        <ErrorState
          title="Incomplete effect link"
          message="Choose an effect from its history or workflow graph."
        />
      )}
    </>
  );
}

function EffectReview({ address }: { address: EffectAddress }) {
  const { access } = useAccess();
  const canRead = usePermission("sensitive_read");
  const location = useLocation();
  const paging = usePagination("evidence_");
  const input = { ...address, cursor: paging.cursor, limit: 4 };
  const enabled = access?.mode === "credentials" && canRead;
  const query = useEffectRead(
    "inspect",
    input,
    effectDetail(
      input,
      access?.mode === "credentials" ? access.scope : undefined,
    ),
    enabled,
  );
  if (!enabled)
    return (
      <Empty>
        Sign in with sensitive_read to inspect this effect. Each successful
        inspection is audited.
      </Empty>
    );
  const detail = query.data;
  const returnState = {
    returnTo: location.pathname + location.search,
    returnNavigationKey: location.key,
  };
  return (
    <>
      {query.isPending && (
        <LoadingState label="Loading retained effect evidence" />
      )}
      {query.error && (
        <QueryError
          error={query.error}
          stale={!!detail}
          retry={() => void query.refetch()}
        />
      )}
      {detail && (
        <div className="approval-list">
          <section className="card" aria-label="Approved effect binding">
            <h2>{detail.summary.callable}</h2>
            <Fields>
              <Field label="Approval key">
                <CopyText value={address.approval_key} />
              </Field>
              <Field label="Original effect activation">
                <Link
                  to={`/executions/${encodeURIComponent(address.activation_id)}`}
                  state={returnState}
                >
                  {address.activation_id}
                </Link>
              </Field>
              <Field label="Workflow">
                <Link
                  to={`/workflows/${encodeURIComponent(address.workflow_id)}?tab=Graph`}
                  state={returnState}
                >
                  {address.workflow_id}
                </Link>
              </Field>
              <Field label="Program">
                {detail.summary.program.id} · {detail.summary.program.version}
              </Field>
              <Field label="Journal">{detail.summary.journal_state}</Field>
              <Field label="Handling">{assessmentLabel(detail.summary)}</Field>
              <Field label="Evidence revision">
                {detail.summary.effect_revision}
              </Field>
            </Fields>
            <p className="muted">
              Execution records can be collected while this evidence remains
              available.
            </p>
            <JsonView
              label="Approved effective action"
              value={detail.binding.approval.action}
            />
            {detail.summary.has_discrepancy && (
              <p className="notice">
                Worker reports disagree. This history remains visible after
                subsequent assessments.
              </p>
            )}
            {detail.summary.operator_resolution
              ?.first_conflicting_entry_sequence != null && (
              <p className="notice">
                Evidence entry{" "}
                {
                  detail.summary.operator_resolution
                    .first_conflicting_entry_sequence
                }{" "}
                conflicts with the current assessment. An already selected
                workflow wake remains immutable.
              </p>
            )}
            <Button
              variant="outline"
              disabled={query.isFetching}
              onClick={() => void query.refetch()}
            >
              Refresh evidence
            </Button>
          </section>
          {detail.current_assessment && (
            <section className="card" aria-label="Current assessment">
              <h2>Current assessment</h2>
              <EvidenceEntry entry={detail.current_assessment} />
              <p className="muted">
                The current assessment remains visible while browsing older
                evidence.
              </p>
            </section>
          )}
          <EffectResolutionForm
            detail={detail}
            unavailable={!!query.error || query.isFetching}
            refresh={() => void query.refetch()}
          />
          <section className="card" aria-label="Retained evidence timeline">
            <h2>Retained evidence</h2>
            <p className="muted">
              Entries retain their original source and order. Application JSON
              is shown exactly as recorded.
            </p>
            {detail.entries.length ? (
              detail.entries.map((item) => (
                <EvidenceEntry key={item.sequence} entry={item} />
              ))
            ) : (
              <Empty>No entries are present on this page.</Empty>
            )}
            <nav className="page-controls" aria-label="Evidence pages">
              <Button
                variant="outline"
                disabled={!paging.cursor}
                onClick={paging.back}
              >
                {paging.previous.length ? "Previous" : "First page"}
              </Button>
              <Button
                variant="outline"
                disabled={!detail.next_cursor}
                onClick={() => {
                  if (detail.next_cursor) paging.next(detail.next_cursor);
                }}
              >
                Next evidence
              </Button>
            </nav>
          </section>
        </div>
      )}
    </>
  );
}

function EvidenceEntry({ entry }: { entry: EffectEntry }) {
  const title =
    entry.kind === "claim"
      ? "Worker claim"
      : entry.kind === "assessment"
        ? "Operator assessment"
        : "Worker observation";
  return (
    <article
      className="approval-card"
      aria-label={`${title} ${entry.sequence}`}
    >
      <h3>
        {title} · {entry.sequence}
      </h3>
      <p>
        <When value={entry.recorded_at} /> · revision {entry.recorded_revision}
      </p>
      {entry.kind === "claim" ? (
        <Fields>
          <Field label="Worker">{entry.worker.subject}</Field>
          <Field label="Permission">{entry.initial_permission}</Field>
          <Field label="Attempt">{entry.attempt_id}</Field>
        </Fields>
      ) : entry.kind === "assessment" ? (
        <>
          <Fields>
            <Field label="Recorded by">
              {entry.actor.subject} · {entry.actor.class}
            </Field>
            <Field label="Issuer">{entry.actor.issuer}</Field>
            <Field label="Resolution">{entry.resolution_id}</Field>
          </Fields>
          <p>{entry.reason}</p>
          <JsonView label="Recorded assessment" value={entry.assessment} />
          <ul>
            {entry.evidence_references.map((reference, index) => (
              <li className="wrap" key={index}>
                <code>{reference}</code>
              </li>
            ))}
          </ul>
        </>
      ) : (
        <>
          <p>
            {entry.source.kind === "verified_worker_claim"
              ? `${entry.source.worker.subject} · reported ${entry.source.reported_callback} · ${entry.source.ingress}`
              : "Historical journal · worker claim provenance unavailable"}
          </p>
          <JsonView label="Reported output" value={entry.output} />
        </>
      )}
    </article>
  );
}
