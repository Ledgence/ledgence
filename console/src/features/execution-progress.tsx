// SPDX-License-Identifier: MIT
import { useState, type ReactNode } from "react";
import { Activity, Pause, Play, RotateCw } from "lucide-react";
import { Button } from "../components/ui/button";
import { Status, When } from "../components/resource-ui";
import { ApiError } from "../api/errors";
import { useExecutionProgress } from "../api/use-execution-progress";
import { progressWindowLimit, type ProgressTarget } from "../api/progress";

export function ExecutionProgress({
  target,
  children,
}: {
  target: ProgressTarget;
  children: ReactNode;
}) {
  const [browse, setBrowse] = useState(false);
  if (browse)
    return (
      <section aria-label="Paginated lifecycle history">
        <div className="progress-toolbar">
          <p className="muted">
            Browse the complete retained history one page at a time.
          </p>
          <Button variant="outline" onClick={() => setBrowse(false)}>
            Return to live history
          </Button>
        </div>
        {children}
      </section>
    );
  return (
    <LiveHistory
      key={`${target.kind}:${target.id}`}
      target={target}
      browse={() => setBrowse(true)}
    />
  );
}
function LiveHistory({
  target,
  browse,
}: {
  target: ProgressTarget;
  browse: () => void;
}) {
  const feed = useExecutionProgress(target);
  const retired = feed.error instanceof ApiError && feed.error.status === 404;
  const status =
    feed.paused || !feed.available
      ? "Updates paused"
      : {
          loading: "Reading history",
          following: "Following execution",
          paused: "Updates paused",
          complete: "Caught up · execution finished",
          limited: "More history available",
          reconnecting: "Reconnecting",
          error: "Updates stopped",
        }[feed.phase];
  return (
    <section className="execution-progress" aria-label="Live lifecycle history">
      <div className="progress-toolbar">
        <div className="progress-heading">
          <span
            className="progress-indicator"
            data-active={
              feed.phase === "following" && feed.available && !feed.paused
            }
            aria-hidden="true"
          >
            <Activity size={16} />
          </span>
          <div>
            <strong role="status">{status}</strong>
            <p className="muted">
              Committed lifecycle events for this {target.kind}.
            </p>
          </div>
        </div>
        <div className="progress-actions">
          {feed.paused ||
          ["complete", "error", "limited"].includes(feed.phase) ? (
            <Button
              variant="outline"
              onClick={feed.resume}
              disabled={!feed.available || retired}
            >
              {feed.phase === "limited" ? (
                <Play size={14} />
              ) : (
                <RotateCw size={14} />
              )}
              {feed.phase === "limited"
                ? "Load next events"
                : feed.paused
                  ? "Resume updates"
                  : "Check for updates"}
            </Button>
          ) : (
            <Button variant="outline" onClick={feed.pause}>
              <Pause size={14} />
              Pause updates
            </Button>
          )}
          <Button variant="ghost" onClick={browse}>
            Browse all history
          </Button>
        </div>
      </div>
      {feed.phase === "error" && (
        <div className="progress-message" role="alert">
          <strong>
            {retired
              ? "Retained history is unavailable"
              : "Could not continue reading history"}
          </strong>
          <p>
            {retired
              ? "This execution is missing or has been retired. Previously read events are shown below; the cursor has not been reset."
              : "The last accepted events and cursor are preserved. Check the connection or access, then try again."}
          </p>
          {feed.error instanceof Error && (
            <p className="muted">{feed.error.message}</p>
          )}
        </div>
      )}
      {feed.phase === "limited" && (
        <p className="progress-message">
          The next retained events are ready. Load another batch to continue
          toward the live tail.
        </p>
      )}
      {feed.omitted && (
        <p className="progress-message">
          Showing the latest {progressWindowLimit} events read in this view. Use
          “Browse all history” to inspect earlier events.
        </p>
      )}
      {feed.events.length ? (
        <ol className="progress-events" aria-label="Lifecycle events">
          {feed.events.map((event) => (
            <li key={event.sequence}>
              <span
                className="progress-sequence"
                title={`Event ${event.sequence}`}
              >
                #{event.sequence}
              </span>
              <div className="progress-event-content">
                <Status value={event.reason} />
                <span className="progress-identity">
                  {event.attempt_id
                    ? `Attempt ${event.attempt_id}`
                    : event.activation_id
                      ? `Entrypoint activation ${event.activation_id}`
                      : `${target.kind === "task" ? "Task" : "Workflow"} lifecycle`}
                </span>
              </div>
              <When value={event.at} />
            </li>
          ))}
        </ol>
      ) : (
        feed.page && (
          <p className="muted">
            No lifecycle events are available in this snapshot.
          </p>
        )
      )}
      <p className="progress-footnote muted">
        {feed.phase === "complete"
          ? "Late cleanup events can still arrive. Check for updates to read them. "
          : "Updates resume from the last accepted event after a temporary disconnect. "}
        Reloading this page rebuilds the retained history.
      </p>
    </section>
  );
}
