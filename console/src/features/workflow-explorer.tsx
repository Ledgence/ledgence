// SPDX-License-Identifier: MIT
import {
  lazy,
  Suspense,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import { useLocation, useSearchParams } from "react-router";
import {
  GitFork,
  ListTree,
  Maximize2,
  Minimize2,
  Search,
  X,
} from "lucide-react";
import "../styles/explorer.css";
import { useInstance } from "../app/instance";
import {
  readExplorerState,
  saveExplorerState,
  type ExplorerViewState,
} from "../app/navigation-state";
import { usePagination, useResource } from "../api/hooks";
import { useTerminalRefresh } from "../api/terminal-refresh";
import { ContractError } from "../api/codecs";
import { workflowExplorer, workflowInput } from "../api/explorer";
import { terminal } from "../api/resources";
import { LoadingState } from "../components/async-state";
import {
  Empty,
  JsonView,
  PageControls,
  QueryError,
} from "../components/resource-ui";
import { Button } from "../components/ui/button";
import {
  evidenceEdges,
  nodeLabel,
  nodeStatus,
  nodeType,
} from "./explorer-model";
import { Trace } from "./explorer/trace";
import { NodeInspector } from "./explorer/inspector";
const GraphCanvas = lazy(() =>
  import("./explorer/graph").then((module) => ({
    default: module.GraphCanvas,
  })),
);

function defaultView(): "graph" | "trace" {
  try {
    const saved = localStorage.getItem("ledgence-explorer-view-v3");
    if (saved === "graph" || saved === "trace") return saved;
  } catch {
    /* Storage can be disabled. */
  }
  return window.matchMedia("(max-width: 768px)").matches ? "trace" : "graph";
}
export function WorkflowExplorer({
  workflowId,
  active,
  view: controlledView,
  visible = true,
}: {
  workflowId: string;
  active: boolean;
  view?: "graph" | "trace";
  visible?: boolean;
}) {
  const config = useInstance();
  const [expanded, setMaximized] = useState(false);
  const maximized = expanded && visible;
  const workspace = useRef<HTMLElement>(null);
  const maximizeButton = useRef<HTMLButtonElement>(null);
  const inspector = useRef<HTMLElement>(null);
  const requestedInspection = useRef<{ id: string; workflowId: string } | null>(
    null,
  );
  useEffect(() => {
    if (!maximized || !workspace.current) return;
    const element = workspace.current;
    // Safari does not focus buttons on pointer clicks, so capture the actual
    // trigger rather than assuming activeElement is the opener.
    const before = maximizeButton.current ?? document.activeElement;
    const previousOverflow = document.body.style.overflow;
    const inertElements: { element: HTMLElement; previous: boolean }[] = [];
    // Keep the mounted canvas (and its camera/selection) in place while making
    // the rest of the document unavailable to keyboard and assistive input.
    let branch: HTMLElement = element;
    while (branch.parentElement) {
      for (const sibling of branch.parentElement.children) {
        if (sibling !== branch && sibling instanceof HTMLElement) {
          inertElements.push({ element: sibling, previous: sibling.inert });
          sibling.inert = true;
        }
      }
      branch = branch.parentElement;
      if (branch === document.body) break;
    }
    document.body.style.overflow = "hidden";
    maximizeButton.current?.focus({ preventScroll: true });
    function trapFocus(event: KeyboardEvent) {
      if (event.key === "Escape") {
        event.preventDefault();
        event.stopPropagation();
        setMaximized(false);
      }
      if (event.key !== "Tab") return;
      const closedDetails = Array.from(
        element.querySelectorAll("details:not([open])"),
      );
      const candidates = Array.from(
        element.querySelectorAll<HTMLElement>(
          'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), summary, [tabindex="0"]',
        ),
      ).filter(
        (item) =>
          item.getClientRects().length &&
          !item.closest("[inert]") &&
          !closedDetails.some(
            (details) =>
              details.contains(item) &&
              !details.querySelector(":scope > summary")?.contains(item),
          ),
      );
      const first = candidates[0];
      const last = candidates.at(-1);
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last?.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first?.focus();
      }
    }
    document.addEventListener("keydown", trapFocus);
    return () => {
      document.removeEventListener("keydown", trapFocus);
      document.body.style.overflow = previousOverflow;
      for (const item of inertElements) item.element.inert = item.previous;
      if (before instanceof HTMLElement && before.isConnected)
        before.focus({ preventScroll: true });
    };
  }, [maximized]);
  const location = useLocation();
  const [params, setParams] = useSearchParams();
  const paging = usePagination("explorer_");
  const [preferredView] = useState(defaultView);
  const view =
    controlledView ??
    (["trace", "timeline"].includes(params.get("view") ?? "")
      ? "trace"
      : params.get("view") === "graph"
        ? "graph"
        : preferredView);
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
      enabled: visible,
      interval: (data) =>
        visible &&
        active &&
        !paging.cursor &&
        (!data || !terminal(data.workflow.summary.workflow.state))
          ? config.polling.waiting_workflow_ms
          : false,
    },
  );
  useTerminalRefresh(
    active,
    [workflowId, paging.cursor, paging.limit],
    query,
    visible,
  );
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
    if (id === selectedId) return;
    if (window.matchMedia("(max-width: 1080px)").matches)
      requestedInspection.current = { id, workflowId };
    changeParam("node", id);
  }
  useLayoutEffect(() => {
    const request = requestedInspection.current;
    requestedInspection.current = null;
    if (
      !visible ||
      !request ||
      request.workflowId !== workflowId ||
      request.id !== selectedId
    )
      return;
    // Only an explicit selection reveals the stacked panel. Polling and
    // restored URL selections must not take focus or move the document.
    const frame = requestAnimationFrame(() => {
      const panel = inspector.current;
      if (panel) {
        panel.focus({ preventScroll: true });
        panel.scrollIntoView({ block: "nearest" });
      }
    });
    return () => cancelAnimationFrame(frame);
  }, [location.key, selectedId, visible, workflowId]);
  function clearSelection() {
    const selectedButton = canvas.current?.querySelector<HTMLElement>(
      '[aria-pressed="true"]',
    );
    changeParam("node", null);
    if (window.matchMedia("(max-width: 1080px)").matches) {
      requestAnimationFrame(() => {
        canvas.current?.scrollIntoView({ block: "nearest" });
        (selectedButton?.isConnected ? selectedButton : canvas.current)?.focus({
          preventScroll: true,
        });
      });
    } else (selectedButton ?? canvas.current)?.focus({ preventScroll: true });
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
    <section
      ref={workspace}
      className={`execution-explorer${maximized ? " explorer-maximized" : ""}`}
      role={maximized ? "dialog" : undefined}
      aria-modal={maximized ? true : undefined}
      aria-label="Workflow execution"
    >
      <div className="explorer-toolbar">
        {!controlledView && (
          <div className="segmented" aria-label="Execution view">
            {(["graph", "trace"] as const).map((mode) => (
              <Button
                key={mode}
                variant="ghost"
                aria-pressed={view === mode}
                onClick={() => {
                  try {
                    localStorage.setItem("ledgence-explorer-view-v3", mode);
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
                {mode === "graph" ? "Graph" : "Trace"}
              </Button>
            ))}
          </div>
        )}
        <label className="explorer-search">
          <Search aria-hidden="true" />
          <span className="sr-only">Find recorded work on this page</span>
          <input
            placeholder="Find a step…"
            value={state.search}
            onChange={(event) =>
              updateState({ ...state, search: event.target.value })
            }
          />
        </label>
        {active && view === "trace" && (
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
        )}
        <Button
          ref={maximizeButton}
          variant="ghost"
          className="explorer-maximize"
          aria-label={maximized ? "Exit full screen" : "Expand to full screen"}
          title={
            maximized ? "Exit full screen (Escape)" : "Expand to full screen"
          }
          onClick={() => setMaximized(!maximized)}
        >
          {maximized ? (
            <Minimize2 aria-hidden="true" />
          ) : (
            <Maximize2 aria-hidden="true" />
          )}
          <span>{maximized ? "Exit full screen" : "Full screen"}</span>
        </Button>
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
          {(paging.cursor || query.data.page.next_cursor) && (
            <p className="notice">
              Partial view. More recorded work is available on other pages; an
              edge appears when both endpoints are loaded.
            </p>
          )}
          <div className={`explorer-body${selectedId ? " has-selection" : ""}`}>
            <div className="explorer-stage">
              <div className="explorer-stage-heading">
                <div className="explorer-record-count">
                  {maximized && (
                    <strong
                      title={`${query.data.workflow.summary.controller.program.id} · ${workflowId}`}
                    >
                      {query.data.workflow.summary.controller.program.id ||
                        workflowId}
                    </strong>
                  )}
                  <span>
                    {nodes.length} recorded{" "}
                    {nodes.length === 1 ? "item" : "items"}
                    {search ? ` · ${filtered.length} matching` : ""}
                  </span>
                </div>
                <span>
                  {selectedId
                    ? "Select another item to inspect"
                    : "Select an item to inspect"}
                </span>
              </div>
              <div
                className={`explorer-canvas explorer-${view}`}
                data-scroll-memory={`explorer:${workflowId}:${view}`}
                ref={canvas}
                tabIndex={0}
                aria-label={`${view === "graph" ? "Graph" : "Trace"} workspace`}
              >
                {!filtered.length ? (
                  <Empty filtered={!!search}>
                    {search
                      ? "No work on this page matches your search."
                      : "No retained execution structure is available on this page. Older or collected records cannot be reconstructed."}
                  </Empty>
                ) : view === "graph" ? (
                  visible && (
                    <Suspense fallback={<LoadingState label="Loading graph" />}>
                      <GraphCanvas
                        key={`${workflowId}:${paging.cursor ?? "first"}`}
                        scope={`${workflowId}:${paging.cursor ?? "first"}`}
                        nodes={nodes}
                        edges={edges}
                        selectedId={selectedId}
                        select={select}
                        matchingIds={
                          search ? filtered.map((node) => node.id) : undefined
                        }
                        presentation={state.graph}
                        save={(graph) => updateState({ ...state, graph })}
                      />
                    </Suspense>
                  )
                ) : (
                  <Trace
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
            </div>
            {visible && selectedId && (
              <aside
                ref={inspector}
                tabIndex={-1}
                className="explorer-inspector"
                aria-label="Selected work details"
              >
                <div className="explorer-inspector-toolbar">
                  <span>Details</span>
                  <Button
                    variant="ghost"
                    aria-label="Close details"
                    title="Close details"
                    onClick={clearSelection}
                  >
                    <X aria-hidden="true" />
                  </Button>
                </div>
                {selected ? (
                  <NodeInspector
                    key={selected.id}
                    node={selected}
                    workflowId={workflowId}
                  />
                ) : (
                  <>
                    <h2>Selection unavailable on this page</h2>
                    <p>
                      The selected record may be on another page, unavailable,
                      or no longer retained. Its absence does not prove it
                      expired.
                    </p>
                    <Button variant="outline" onClick={clearSelection}>
                      Clear selection
                    </Button>
                  </>
                )}
              </aside>
            )}
          </div>
          <PageControls
            pagination={paging}
            nextCursor={query.data.page.next_cursor}
            observedAt={query.data.page.observed_at}
            refresh={() => void query.refetch()}
            fetching={query.isFetching}
          />
          <details className="explorer-reference">
            <summary>Work list & recorded evidence</summary>
            <div className="explorer-reference-content">
              <details className="explorer-relationships">
                <summary>Recorded relationships ({edges.length})</summary>
                <p className="muted">
                  Lines represent recorded registration, terminal-outcome waits
                  and scheduled resumes. A subworkflow opens its own graph.
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
                <ul>
                  {filtered.map((node) => (
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
              </details>
              <p className="muted explorer-evidence-note">
                This view uses retained Ledgence records and works without
                exported traces. Missing local evidence does not mean a step
                never started. Timestamps do not establish dependencies.
                Measurements and history may be incomplete after interrupted
                attempts or retention cleanup.
              </p>
            </div>
          </details>
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
