// SPDX-License-Identifier: MIT
import { useLayoutEffect, useMemo, useRef, useState } from "react";
import type { ExplorerNode } from "../../api/explorer";
import type { ExplorerViewState } from "../../app/navigation-state";
import { Empty, Status, When } from "../../components/resource-ui";
import { Button } from "../../components/ui/button";
import {
  nodeLabel,
  nodeType,
  nodeStatus,
  timelineRows,
  timelineBounds,
} from "../explorer-model";
const rowHeight = 86;
const overscan = 4;
export function Trace({
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
  const [viewportHeight, setViewportHeight] = useState(0);
  const rows = useMemo(() => timelineRows(nodes), [nodes]);
  const hasRows = rows.length > 0;
  const viewport = useRef<HTMLDivElement>(null);
  const anchor = useRef<{ id: string | null; index: number } | null>(null);
  const selectedIndex = rows.findIndex((row) => row.node.id === selectedId);
  useLayoutEffect(() => {
    const element = viewport.current;
    if (!element) return;
    const measure = () => {
      setViewportHeight(element.clientHeight);
      if (state.follow) element.scrollTop = element.scrollHeight;
      setScroll(element.scrollTop);
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, [hasRows, state.follow]);
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
      setScroll(element.scrollTop);
    }
    anchor.current = { id: selectedId, index: selectedIndex };
  }, [rows, selectedIndex, selectedId, state.follow]);
  if (!rows.length)
    return <Empty>No retained work is available in this trace.</Empty>;
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
  const first = Math.max(0, Math.floor(scroll / rowHeight) - overscan);
  const last = Math.min(
    rows.length,
    Math.ceil((scroll + viewportHeight) / rowHeight) + overscan,
  );
  return (
    <div
      className="execution-timeline"
      data-timeline-start={start}
      data-timeline-end={end}
    >
      <details className="timeline-range-options">
        <summary>
          {state.start || state.end ? "Custom time range" : "Adjust time range"}
        </summary>
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
          {(state.start || state.end) && (
            <Button
              variant="ghost"
              onClick={() => updateState({ ...state, start: "", end: "" })}
            >
              Reset range
            </Button>
          )}
        </div>
      </details>
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
        aria-label="Trace work rows"
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
