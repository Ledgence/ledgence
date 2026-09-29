// SPDX-License-Identifier: MIT
import {
  useEffect,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { Link, useLocation, useSearchParams } from "react-router";
import {
  ArrowUpRight,
  ChevronDown,
  ChevronRight,
  GitFork,
  ListTree,
} from "lucide-react";
import { useInstance } from "../app/instance";
import {
  readExplorerState,
  saveExplorerState,
  type ExplorerViewState,
} from "../app/navigation-state";
import { usePagination, useResource } from "../api/hooks";
import { useTerminalRefresh } from "../api/terminal-refresh";
import { ContractError } from "../api/codecs";
import {
  executionPath,
  workflowExplorer,
  workflowInput,
  type ExplorerNode,
} from "../api/explorer";
import { terminal } from "../api/resources";
import { LoadingState } from "../components/async-state";
import {
  Empty,
  Field,
  Fields,
  JsonView,
  PageControls,
  QueryError,
  Status,
  When,
} from "../components/resource-ui";
import { Button } from "../components/ui/button";
import { AttemptResources } from "./execution-resources";
import {
  evidenceEdges,
  graphLayout,
  graphEdgeRoute,
  micros,
  nodeLabel,
  nodeStatus,
  nodeTiming,
  nodeType,
  phaseGroups,
  timelineRows,
  timelineBounds,
  type EvidenceEdge,
} from "./explorer-model";

function defaultView(): "graph" | "timeline" {
  try {
    const saved = localStorage.getItem("ledgence-explorer-view");
    if (saved === "graph" || saved === "timeline") return saved;
  } catch {
    /* Storage can be disabled. */
  }
  return window.matchMedia("(max-width: 768px)").matches ? "timeline" : "graph";
}
export function WorkflowExplorer({
  workflowId,
  active,
}: {
  workflowId: string;
  active: boolean;
}) {
  const config = useInstance();
  const location = useLocation();
  const [params, setParams] = useSearchParams();
  const paging = usePagination("explorer_");
  const [preferredView] = useState(defaultView);
  const view =
    params.get("view") === "timeline"
      ? "timeline"
      : params.get("view") === "graph"
        ? "graph"
        : preferredView;
  const [saved, setSaved] = useState(() => ({
    key: location.key,
    value: readExplorerState(location.key, location.state),
  }));
  // A history entry, not a URL, owns its expansion/filter/camera state.
  const state =
    saved.key === location.key
      ? saved.value
      : readExplorerState(location.key, location.state);
  useEffect(() => {
    // Copy restored presentation into this entry before another URL-only
    // selection/view change chains its restoration key through this entry.
    saveExplorerState(location.key, state);
  }, [location.key, state]);
  function updateState(next: ExplorerViewState) {
    saveExplorerState(location.key, next);
    setSaved({ key: location.key, value: next });
  }
  const query = useResource(
    "workflows/explorer",
    {
      workflow_id: workflowId,
      limit: Math.min(paging.limit, config.limits.max_visible_workflow_nodes),
      cursor: paging.cursor,
    },
    (value) => {
      const result = workflowExplorer(value);
      if (result.workflow.summary.workflow.workflow_id !== workflowId)
        throw new ContractError(
          "The explorer belongs to a different workflow.",
        );
      if (
        new Set(result.page.items.map((node) => node.id)).size !==
        result.page.items.length
      )
        throw new ContractError("Duplicate explorer node identity.");
      return result;
    },
    {
      interval: (data) =>
        active &&
        !paging.cursor &&
        (!data || !terminal(data.workflow.summary.workflow.state))
          ? config.polling.waiting_workflow_ms
          : false,
    },
  );
  useTerminalRefresh(active, [workflowId, paging.cursor, paging.limit], query);
  const nodes = query.data?.page.items ?? [];
  const selectedId = params.get("node");
  const selected = nodes.find((node) => node.id === selectedId);
  const search = state.search.toLocaleLowerCase();
  const filtered = nodes.filter(
    (node) =>
      !search ||
      `${nodeLabel(node)} ${"key" in node ? node.key : ""} ${nodeType(node)} ${nodeStatus(node)}`
        .toLocaleLowerCase()
        .includes(search),
  );
  const groups = phaseGroups(filtered);
  const edges = evidenceEdges(nodes);
  function changeParam(key: string, value: string | null) {
    const next = new URLSearchParams(params);
    if (value) next.set(key, value);
    else next.delete(key);
    // Shareable selection/view changes retain the current entry's presentation.
    setParams(next, {
      preventScrollReset: true,
      state: { ...location.state, restoreNavigationKey: location.key },
    });
  }
  function select(id: string) {
    changeParam("node", id);
  }
  function collapse(id: string) {
    updateState({
      ...state,
      collapsed: state.collapsed.includes(id)
        ? state.collapsed.filter((value) => value !== id)
        : [...state.collapsed, id],
    });
  }
  const canvas = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (state.follow && canvas.current) {
      const viewport =
        canvas.current.querySelector<HTMLElement>(".timeline-viewport") ??
        canvas.current;
      viewport.scrollTop = viewport.scrollHeight;
    }
  }, [query.dataUpdatedAt, state.follow]);
  return (
    <section className="execution-explorer" aria-label="Workflow execution">
      <div className="explorer-toolbar">
        <div className="segmented" aria-label="Execution view">
          {(["graph", "timeline"] as const).map((mode) => (
            <Button
              key={mode}
              variant="ghost"
              aria-pressed={view === mode}
              onClick={() => {
                try {
                  localStorage.setItem("ledgence-explorer-view", mode);
                } catch {
                  /* Optional preference. */
                }
                changeParam("view", mode);
              }}
            >
              {mode === "graph" ? (
                <GitFork aria-hidden="true" />
              ) : (
                <ListTree aria-hidden="true" />
              )}
              {mode === "graph" ? "Graph" : "Timeline"}
            </Button>
          ))}
        </div>
        <label className="explorer-search">
          <span className="sr-only">Find recorded work on this page</span>
          <input
            placeholder="Find work on this page"
            value={state.search}
            onChange={(event) =>
              updateState({ ...state, search: event.target.value })
            }
          />
        </label>
        <label className="check-label">
          <input
            type="checkbox"
            checked={state.follow}
            onChange={(event) =>
              updateState({ ...state, follow: event.target.checked })
            }
          />
          Follow activity
        </label>
      </div>
      {query.isPending && <LoadingState label="Loading recorded execution" />}
      {query.error && (
        <QueryError
          error={query.error}
          retry={() => void query.refetch()}
          stale={!!query.data}
        />
      )}
      {query.data && (
        <>
          <p className="muted explorer-caption">
            Recorded execution · {nodes.length} records on this page. Local
            steps run within their controller phase; child executions run
            independently.
          </p>
          {(paging.cursor || query.data.page.next_cursor) && (
            <p className="notice">
              Partial view. More recorded work is available on other pages; an
              edge appears when both endpoints are loaded.
            </p>
          )}
          <div className="explorer-body">
            <div>
              <div
                className={`explorer-canvas explorer-${view}`}
                data-scroll-memory={`explorer:${workflowId}:${view}`}
                ref={canvas}
                tabIndex={0}
                aria-label={`${view === "graph" ? "Graph" : "Timeline"} workspace`}
              >
                {!filtered.length ? (
                  <Empty filtered={!!search}>
                    {search
                      ? "No work on this page matches your search."
                      : "No retained execution structure is available on this page. Older or collected records cannot be reconstructed."}
                  </Empty>
                ) : view === "graph" ? (
                  <SemanticGraph
                    nodes={filtered}
                    edges={edges}
                    collapsed={state.collapsed}
                    selectedId={selectedId}
                    select={select}
                    collapse={collapse}
                  />
                ) : (
                  <Timeline
                    nodes={filtered}
                    observedAt={query.data.page.observed_at}
                    workflowActive={
                      !terminal(query.data.workflow.summary.workflow.state)
                    }
                    selectedId={selectedId}
                    select={select}
                    state={state}
                    updateState={updateState}
                  />
                )}
              </div>
              <details className="explorer-relationships">
                <summary>Recorded relationships ({edges.length})</summary>
                <p className="muted">
                  Phase grouping shows containment. Lines only represent
                  recorded registration, terminal-outcome waits and scheduled
                  resumes.
                </p>
                {edges.length ? (
                  <ul>
                    {edges.map((edge) => (
                      <li key={`${edge.from}:${edge.to}:${edge.relation}`}>
                        <button type="button" onClick={() => select(edge.from)}>
                          {nodeLabel(
                            nodes.find((node) => node.id === edge.from)!,
                          )}
                        </button>{" "}
                        → {edge.relation} →{" "}
                        <button type="button" onClick={() => select(edge.to)}>
                          {nodeLabel(
                            nodes.find((node) => node.id === edge.to)!,
                          )}
                        </button>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p>No complete relationship is recorded on this page.</p>
                )}
              </details>
              <details className="explorer-semantic-list">
                <summary>Accessible work list ({filtered.length})</summary>
                {groups.map((group) => (
                  <section key={group.activationId}>
                    <h3>{group.label}</h3>
                    <ul>
                      {group.nodes.map((node) => (
                        <li key={node.id}>
                          <button
                            type="button"
                            onClick={() => select(node.id)}
                            aria-pressed={selectedId === node.id}
                          >
                            {nodeLabel(node)} · {nodeType(node)} ·{" "}
                            {nodeStatus(node)}
                          </button>
                        </li>
                      ))}
                    </ul>
                  </section>
                ))}
              </details>
            </div>
            <aside
              className="explorer-inspector"
              aria-label="Selected work details"
            >
              {selected ? (
                <NodeInspector
                  key={selected.id}
                  node={selected}
                  workflowId={workflowId}
                />
              ) : selectedId ? (
                <>
                  <h2>Selection unavailable on this page</h2>
                  <p>
                    The selected record may be on another page, unavailable, or
                    no longer retained. Its absence does not prove it expired.
                  </p>
                  <Button
                    variant="outline"
                    onClick={() => changeParam("node", null)}
                  >
                    Clear selection
                  </Button>
                </>
              ) : (
                <>
                  <h2>Inspect recorded work</h2>
                  <p>
                    Select a node or timeline row to see its status and
                    evidence. Open a task or subworkflow to explore its
                    execution.
                  </p>
                </>
              )}
            </aside>
          </div>
          <PageControls
            pagination={paging}
            nextCursor={query.data.page.next_cursor}
            observedAt={query.data.page.observed_at}
            refresh={() => void query.refetch()}
            fetching={query.isFetching}
          />
          <details className="muted">
            <summary>About this execution view</summary>
            <p>
              This view uses retained Ledgence records and works without
              exported traces. Missing local evidence does not mean a step never
              started. Timestamps do not establish dependencies. Measurements
              and history may be incomplete after interrupted attempts or
              retention cleanup.
            </p>
          </details>
        </>
      )}
    </section>
  );
}

function SemanticGraph({
  nodes,
  edges,
  collapsed,
  selectedId,
  select,
  collapse,
}: {
  nodes: ExplorerNode[];
  edges: EvidenceEdge[];
  collapsed: string[];
  selectedId: string | null;
  select: (id: string) => void;
  collapse: (id: string) => void;
}) {
  const marker = useId().replaceAll(":", "");
  const layout = graphLayout(phaseGroups(nodes), collapsed);
  const graph = useRef<HTMLDivElement>(null);
  const anchor = useRef<{ id: string | null; x: number; y: number } | null>(
    null,
  );
  const selectedPosition = selectedId
    ? layout.positions.get(selectedId)
    : undefined;
  useLayoutEffect(() => {
    const before = anchor.current;
    const viewport = graph.current?.parentElement;
    if (selectedPosition && selectedId === before?.id && viewport) {
      viewport.scrollLeft += selectedPosition.x - before.x;
      viewport.scrollTop += selectedPosition.y - before.y;
    }
    anchor.current = selectedPosition
      ? { id: selectedId, x: selectedPosition.x, y: selectedPosition.y }
      : null;
  }, [selectedId, selectedPosition]);
  return (
    <div
      className="semantic-graph"
      ref={graph}
      style={{ width: layout.width, height: layout.height }}
    >
      {layout.areas.map(({ group, y, height }) => (
        <div
          key={group.activationId}
          className="graph-phase-area"
          style={{ top: y, height }}
        >
          <Button
            variant="ghost"
            aria-expanded={!collapsed.includes(group.activationId)}
            aria-label={`${collapsed.includes(group.activationId) ? "Expand" : "Collapse"} phase ${group.label}`}
            onClick={() => collapse(group.activationId)}
          >
            {collapsed.includes(group.activationId) ? (
              <ChevronRight aria-hidden="true" />
            ) : (
              <ChevronDown aria-hidden="true" />
            )}
            Phase · {group.label}
            <small>{group.nodes.length} records</small>
          </Button>
          <span className="graph-lane-label">Within workflow</span>
          <span className="graph-child-label">Child executions</span>
        </div>
      ))}
      <svg
        className="graph-edges"
        width={layout.width}
        height={layout.height}
        aria-hidden="true"
      >
        <defs>
          <marker
            id={marker}
            viewBox="0 0 10 10"
            refX="9"
            refY="5"
            markerWidth="7"
            markerHeight="7"
            orient="auto-start-reverse"
          >
            <path d="M 0 0 L 10 5 L 0 10 z" />
          </marker>
        </defs>
        {edges.map((edge) => {
          const points = graphEdgeRoute(edge, layout.positions);
          if (!points) return null;
          const d = points
            .map((point, index) => `${index ? "L" : "M"}${point.x},${point.y}`)
            .join(" ");
          return (
            <path
              key={`${edge.from}:${edge.to}:${edge.relation}`}
              d={d}
              data-edge-from={edge.from}
              data-edge-to={edge.to}
              markerEnd={`url(#${marker})`}
              className={
                edge.relation === "awaits terminal outcome"
                  ? "edge-wait"
                  : undefined
              }
            >
              <title>{edge.relation}</title>
            </path>
          );
        })}
      </svg>
      {nodes.map((node) => {
        const position = layout.positions.get(node.id);
        return (
          position && (
            <button
              type="button"
              key={node.id}
              className={`graph-node graph-node-${node.kind}`}
              style={{
                left: position.x,
                top: position.y,
                width: position.width,
                height: position.height,
              }}
              aria-pressed={selectedId === node.id}
              aria-label={`${nodeLabel(node)} · ${nodeType(node)} · ${nodeStatus(node)}`}
              data-focus-key={`node:${node.id}`}
              onClick={() => select(node.id)}
            >
              <span className="eyebrow">{nodeType(node)}</span>
              <strong>{nodeLabel(node)}</strong>
              <Status value={nodeStatus(node)} />
            </button>
          )
        );
      })}
    </div>
  );
}

const rowHeight = 86;
function Timeline({
  nodes,
  observedAt,
  workflowActive,
  selectedId,
  select,
  state,
  updateState,
}: {
  nodes: ExplorerNode[];
  observedAt: number;
  workflowActive: boolean;
  selectedId: string | null;
  select: (id: string) => void;
  state: ExplorerViewState;
  updateState: (value: ExplorerViewState) => void;
}) {
  const [scroll, setScroll] = useState(0);
  const rows = useMemo(() => timelineRows(nodes), [nodes]);
  const viewport = useRef<HTMLDivElement>(null);
  const anchor = useRef<{ id: string | null; index: number } | null>(null);
  const selectedIndex = rows.findIndex((row) => row.node.id === selectedId);
  useLayoutEffect(() => {
    const element = viewport.current;
    const previous = anchor.current;
    if (element) {
      if (state.follow) element.scrollTop = element.scrollHeight;
      else if (
        selectedIndex >= 0 &&
        previous?.id === selectedId &&
        previous.index >= 0
      )
        element.scrollTop += (selectedIndex - previous.index) * rowHeight;
    }
    anchor.current = { id: selectedId, index: selectedIndex };
  }, [rows, selectedIndex, selectedId, state.follow]);
  if (!rows.length)
    return <Empty>No retained work is available in this timeline.</Empty>;
  const { start: naturalStart, end: naturalEnd } = timelineBounds(
    rows,
    observedAt,
    workflowActive,
  );
  const rangeStart = state.start ? Date.parse(state.start) : naturalStart;
  const rangeEnd = state.end ? Date.parse(state.end) : naturalEnd;
  const validRange =
    Number.isFinite(rangeStart) &&
    Number.isFinite(rangeEnd) &&
    rangeEnd > rangeStart;
  const start = validRange ? rangeStart : naturalStart;
  const end = validRange ? rangeEnd : naturalEnd + 1;
  const first = Math.max(0, Math.floor(scroll / rowHeight) - 4);
  const last = Math.min(rows.length, first + 16);
  return (
    <div
      className="execution-timeline"
      data-timeline-start={start}
      data-timeline-end={end}
    >
      <div className="timeline-range">
        <label>
          From
          <input
            type="datetime-local"
            value={state.start}
            onChange={(event) =>
              updateState({ ...state, start: event.target.value })
            }
          />
        </label>
        <label>
          Until
          <input
            type="datetime-local"
            value={state.end}
            onChange={(event) =>
              updateState({ ...state, end: event.target.value })
            }
          />
        </label>
        <Button
          variant="ghost"
          onClick={() => updateState({ ...state, start: "", end: "" })}
        >
          Reset range
        </Button>
      </div>
      {!validRange && (state.start || state.end) && (
        <p className="notice">
          Choose an end after the start. Showing the full observed range.
        </p>
      )}
      <p className="muted timeline-caption">
        Bars show recorded intervals; dots show milestones.
        Submitted-to-terminal bars include queue and waiting time.
      </p>
      <div className="timeline-axis">
        <When value={start} />
        <When value={end} />
      </div>
      <div
        className="timeline-viewport"
        ref={viewport}
        data-scroll-memory="timeline-rows"
        tabIndex={0}
        aria-label="Timeline work rows"
        onScroll={(event) => setScroll(event.currentTarget.scrollTop)}
      >
        <div
          role="list"
          aria-label="Recorded work"
          style={{ height: rows.length * rowHeight, position: "relative" }}
        >
          {rows.slice(first, last).map(({ node, timing }, offset) => {
            const open = timing.open && workflowActive;
            const unknownEnd = timing.end === null && !open;
            const renderedEnd =
              timing.end ?? (open ? observedAt : timing.start);
            const left = Math.max(
              0,
              Math.min(100, ((timing.start - start) / (end - start)) * 100),
            );
            const right = Math.max(
              0,
              Math.min(100, ((renderedEnd - start) / (end - start)) * 100),
            );
            const inRange = timing.start <= end && renderedEnd >= start;
            return (
              <div
                role="listitem"
                aria-posinset={first + offset + 1}
                aria-setsize={rows.length}
                key={node.id}
                style={{
                  position: "absolute",
                  top: (first + offset) * rowHeight,
                  width: "100%",
                  height: rowHeight,
                }}
              >
                <button
                  className="timeline-row"
                  type="button"
                  aria-pressed={selectedId === node.id}
                  data-focus-key={`node:${node.id}`}
                  onClick={() => select(node.id)}
                >
                  <span className="timeline-row-label">
                    <strong>{nodeLabel(node)}</strong>
                    <small>
                      {nodeType(node)} · {node.entrypoint}
                    </small>
                    <Status value={nodeStatus(node)} />
                  </span>
                  <span className="timeline-track" aria-label={timing.label}>
                    {unknownEnd && (
                      <span className="timeline-unknown">End not recorded</span>
                    )}
                    {inRange && (
                      <span
                        className={
                          timing.milestone || unknownEnd
                            ? "timeline-milestone"
                            : `timeline-bar${open ? " timeline-open" : ""}`
                        }
                        style={{
                          left: `${left}%`,
                          width:
                            timing.milestone || unknownEnd
                              ? undefined
                              : `${Math.max(0.4, right - left)}%`,
                        }}
                      />
                    )}
                    <span className="sr-only">
                      {timing.label}: {new Date(timing.start).toISOString()}
                      {timing.end !== null
                        ? ` to ${new Date(timing.end).toISOString()}`
                        : "; end not recorded"}
                    </span>
                  </span>
                </button>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function NodeInspector({
  node,
  workflowId,
}: {
  node: ExplorerNode;
  workflowId: string;
}) {
  const timing = nodeTiming(node);
  const [expanded, setExpanded] = useState(false);
  return (
    <>
      <div>
        <p className="eyebrow">{nodeType(node)}</p>
        <h2>{nodeLabel(node)}</h2>
        <Status value={nodeStatus(node)} />
      </div>
      {node.kind === "child" &&
        (node.availability === "available" ? (
          <Link
            className="button button-primary"
            to={executionPath(node.execution.kind, node.execution.id)}
          >
            Open execution <ArrowUpRight aria-hidden="true" />
          </Link>
        ) : (
          <p className="notice">
            This execution is referenced by retained history, but its details
            are unavailable. The removal reason is not recorded.
          </p>
        ))}
      {node.kind === "child" &&
        node.availability === "available" &&
        node.execution.kind === "workflow" && (
          <>
            <Button
              variant="outline"
              aria-expanded={expanded}
              onClick={() => setExpanded(!expanded)}
            >
              {expanded ? "Close child timeline" : "Preview child timeline"}
            </Button>
            {expanded && <ChildTimeline workflowId={node.execution.id} />}
          </>
        )}
      <Fields>
        <Field label="Phase">{node.entrypoint}</Field>
        {"key" in node && <Field label="Key">{node.key}</Field>}
        <Field
          label={timing.milestone ? "Recorded milestone" : "Recorded start"}
        >
          <When value={timing.start} />
        </Field>
        {!timing.milestone && (
          <Field label="Recorded end">
            <When value={timing.end} />
          </Field>
        )}
      </Fields>
      <p className="muted">{timing.label}.</p>
      {node.kind === "phase" && (
        <>
          <Fields>
            <Field label="Applied decision">
              {node.decision_kind ??
                "No successfully applied decision recorded"}
            </Field>
          </Fields>
          {node.error && (
            <p className="notice error-notice">
              {node.error.kind}: {node.error.message}
            </p>
          )}
          <Link
            to={`${executionPath("task", node.activation_id)}?tab=Attempts`}
          >
            Inspect controller attempts
          </Link>
          {node.resumed_activation_id && (
            <p>
              Resume scheduled: <code>{node.resumed_activation_id}</code>.
              Scheduling does not prove the handler started.
            </p>
          )}
        </>
      )}
      {node.kind === "child" && (
        <Fields>
          <Field label="Program / version">
            {node.program.id} / {node.program.version}
          </Field>
          <Field label="Execution ID">{node.execution.id}</Field>
          <Field label="Fork membership">
            {node.fork_key ?? "No fork membership recorded"}
          </Field>
        </Fields>
      )}
      {node.kind === "fork" && (
        <>
          <p>Registration accepted. Branch execution may start later.</p>
          <Fields>
            <Field label="Distributed branch keys">
              {node.branch_keys.join(", ") || "None"}
            </Field>
          </Fields>
        </>
      )}
      {node.kind === "child_wait" && (
        <>
          <p>
            Waits for terminal outcomes, including failure or cancellation. It
            does not require every child to succeed.
          </p>
          <Fields>
            <Field label="Member keys">{node.member_keys.join(", ")}</Field>
            <Field label="Resume entrypoint">{node.resume}</Field>
          </Fields>
          {!node.resumed_activation_id && (
            <p className="muted">
              No resume is recorded. This alone does not prove the wait is still
              active.
            </p>
          )}
        </>
      )}
      {node.kind === "external_wait" && (
        <>
          <Fields>
            <Field label="Deadline">
              <When value={node.deadline} />
            </Field>
          </Fields>
          <p>
            {node.wake_reason
              ? `Recorded wake: ${node.wake_reason}. Resume scheduled; execution is observed separately.`
              : node.closed_at !== null
                ? "The wait closed without retained wake evidence; cancellation or failure can also close waits."
                : "Waiting for an external wake."}
          </p>
          <Link to={`/workflows/${encodeURIComponent(workflowId)}?tab=Waits`}>
            Inspect waits and available actions
          </Link>
        </>
      )}
      {node.kind === "local" && (
        <>
          <p>
            This operation runs inside the workflow’s process. It is not an
            independently scheduled task.
          </p>
          <Fields>
            <Field label="Callable">{node.callable}</Field>
            <Field label="Durable result accepted">
              <When value={node.accepted_at} />
            </Field>
          </Fields>
          {node.observation ? (
            <>
              <Fields>
                <Field label="Callable observation">
                  {node.observation.state} ·{" "}
                  {micros(node.observation.elapsed_us)}
                </Field>
              </Fields>
              <p className="muted">
                Returned means the callable returned; durable result acceptance
                is separate. Replay reuses an accepted result.
              </p>
              <LocalResources attemptId={node.observation.attempt_id} />
            </>
          ) : (
            <p className="notice">
              The result was accepted; callable start, end and resource usage
              were not recorded.
            </p>
          )}
        </>
      )}
      <details>
        <summary>Recorded evidence</summary>
        <JsonView value={node} label="Explorer evidence" />
      </details>
    </>
  );
}

function LocalResources({ attemptId }: { attemptId: string }) {
  const [open, setOpen] = useState(false);
  return (
    <details onToggle={(event) => setOpen(event.currentTarget.open)}>
      <summary>Process resources for this attempt</summary>
      {open && <AttemptResources attemptId={attemptId} />}
    </details>
  );
}

function ChildTimeline({ workflowId }: { workflowId: string }) {
  const config = useInstance();
  const query = useResource(
    "workflows/explorer",
    { workflow_id: workflowId, limit: 25 },
    (value) => {
      const data = workflowExplorer(value);
      if (data.workflow.summary.workflow.workflow_id !== workflowId)
        throw new ContractError("Child timeline belongs to another workflow.");
      return data;
    },
    {
      interval: (data) =>
        data && terminal(data.workflow.summary.workflow.state)
          ? false
          : config.polling.waiting_workflow_ms,
    },
  );
  const [state, updateState] = useState<ExplorerViewState>({
    collapsed: [],
    start: "",
    end: "",
    search: "",
    follow: false,
  });
  const [selection, select] = useState<string | null>(null);
  const selected = query.data?.page.items.find((node) => node.id === selection);
  return (
    <section aria-label="Child timeline preview">
      {query.isPending && <LoadingState label="Loading child timeline" />}
      {query.error && (
        <QueryError
          error={query.error}
          retry={() => void query.refetch()}
          stale={!!query.data}
        />
      )}
      {query.data && (
        <>
          <Timeline
            nodes={query.data.page.items}
            observedAt={query.data.page.observed_at}
            workflowActive={
              !terminal(query.data.workflow.summary.workflow.state)
            }
            selectedId={selection}
            select={select}
            state={state}
            updateState={updateState}
          />
          {query.data.page.next_cursor && (
            <p className="notice">
              Preview limited to 25 retained records. Open this execution to
              page through all recorded work.
            </p>
          )}
          <details>
            <summary>Child work list</summary>
            <ul>
              {query.data.page.items.map((node) => (
                <li key={node.id}>
                  <Button variant="ghost" onClick={() => select(node.id)}>
                    {nodeLabel(node)} · {nodeStatus(node)}
                  </Button>
                </li>
              ))}
            </ul>
          </details>
          {selected && (
            <div className="card">
              <NodeInspector
                key={selected.id}
                node={selected}
                workflowId={workflowId}
              />
            </div>
          )}
        </>
      )}
    </section>
  );
}

export function WorkflowInput({ workflowId }: { workflowId: string }) {
  const query = useResource(
    "workflows/input",
    { workflow_id: workflowId },
    (value) => {
      const result = workflowInput(value);
      if (result.workflow_id !== workflowId)
        throw new ContractError("Input belongs to another workflow.");
      return result;
    },
    { maximumBytes: 10 * 1024 * 1024 },
  );
  return (
    <>
      {query.isPending && <LoadingState />}
      {query.error && (
        <QueryError error={query.error} retry={() => void query.refetch()} />
      )}
      {query.data && (
        <JsonView value={query.data.data} label="Workflow input" />
      )}
    </>
  );
}
