// SPDX-License-Identifier: MIT
import { useRef, useState } from "react";
import { Link } from "react-router";
import { usePermission } from "../../app/access-session";
import { accessController } from "../../app/access-state";
import { useInstance } from "../../app/instance";
import {
  releaseHold,
  releaseReceiptFor,
  holdsFor,
  type QueueScope,
  type QueueHold,
} from "../../api/queue-policies";
import { Button } from "../../components/ui/button";
import {
  Table,
  TableHeader,
  TableRow,
  TableHead,
  TableBody,
  TableCell,
} from "../../components/ui/table";
import {
  Dialog,
  DialogContent,
  DialogTitle,
  DialogDescription,
} from "../../components/ui/dialog";
import { When, QueryError } from "../../components/resource-ui";
import { LoadingState } from "../../components/async-state";
import { useQueueRead } from "./read";
import { PendingRequest } from "./command";
import { useQueueCommand } from "./use-queue-command";

const releaseKey = (instance: string, queue: string) =>
  JSON.stringify(["queue-hold-release", instance, queue]);
function ReleaseForm({
  queue,
  scope,
  selected,
  restoreFocus,
  onClose,
  onSuccess,
}: {
  queue: string;
  scope: QueueScope;
  selected: QueueHold | null;
  restoreFocus: () => void;
  onClose: () => void;
  onSuccess: () => void;
}) {
  const config = useInstance();
  const [reason, setReason] = useState("");
  const [error, setError] = useState("");
  const req = useQueueCommand(
    "release",
    releaseKey(config.instance_id, queue),
    releaseHold,
    (raw, v) => releaseReceiptFor(raw, scope, v),
    onSuccess,
  );
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open && !req.mutation.isPending) onClose();
      }}
    >
      <DialogContent
        onCloseAutoFocus={(event) => {
          event.preventDefault();
          restoreFocus();
        }}
      >
        <DialogTitle>Release uncertain capacity</DialogTitle>
        <DialogDescription>
          Releasing this slot allows another attempt to start. The earlier
          attempt may still be running.
        </DialogDescription>
        {req.command ? (
          <PendingRequest
            identity={req.command.identity}
            body={req.command.body}
            pending={req.mutation.isPending}
            error={req.mutation.error}
            retry={req.retry}
            reset={() => {
              req.reset();
              onClose();
            }}
          />
        ) : (
          selected && (
            <form
              className="queue-form"
              onSubmit={(e) => {
                e.preventDefault();
                try {
                  const value = releaseHold({
                    queue,
                    task_id: selected.task_id,
                    attempt_id: selected.attempt_id,
                    expected_revision: selected.revision,
                    request_id: crypto.randomUUID(),
                    reason: reason.trim(),
                  });
                  setError("");
                  req.send(value);
                } catch (err) {
                  setError(
                    err instanceof Error
                      ? err.message
                      : "Review the release reason.",
                  );
                }
              }}
            >
              <p>
                Attempt <code className="wrap">{selected.attempt_id}</code> ·
                revision {selected.revision}
              </p>
              <label>
                Reason for release
                <textarea
                  required
                  value={reason}
                  onChange={(e) => setReason(e.target.value)}
                />
              </label>
              {error && (
                <p className="notice" role="alert">
                  {error}
                </p>
              )}
              <div className="actions">
                <Button variant="destructive" type="submit">
                  Release this slot
                </Button>
                <Button variant="outline" onClick={onClose}>
                  Cancel
                </Button>
              </div>
            </form>
          )
        )}
      </DialogContent>
    </Dialog>
  );
}
function outcome(hold: QueueHold) {
  if (hold.release_kind === "timeout")
    return "Released after grace; closure unconfirmed";
  if (hold.release_kind === "operator")
    return "Released by operator; closure unconfirmed";
  if (hold.release_kind === "confirmed") return "Closure confirmed";
  if (hold.release_kind === "never_started") return "Released before start";
  return hold.state.replaceAll("_", " ");
}
export function Holds({
  queue,
  scope,
  refresh,
}: {
  queue: string;
  scope: QueueScope;
  refresh: () => void;
}) {
  const config = useInstance();
  const sensitive = usePermission("sensitive_read");
  const control = usePermission("resource_control");
  const heading = useRef<HTMLHeadingElement>(null);
  const opener = useRef<HTMLButtonElement>(null);
  const [opened, setOpened] = useState(false);
  const [released, setReleased] = useState(false);
  const [pages, setPages] = useState<(string | null)[]>([null]);
  const [selected, setSelected] = useState<QueueHold | null>(null);
  const [releaseOpen, setReleaseOpen] = useState(false);
  const [notice, setNotice] = useState("");
  const limit = config.limits.default_page_size;
  const data = useQueueRead(
    "holds",
    { queue, after: pages.at(-1) ?? null, limit, include_released: released },
    holdsFor(scope, queue, limit, released, pages.at(-1) ?? null),
    opened && sensitive,
  );
  const restored = !!accessController.recover(
    releaseKey(config.instance_id, queue),
  );
  return (
    <section className="resource-list queue-holds" aria-label="Capacity holds">
      <header className="resource-list-heading">
        <div>
          <h2 ref={heading} tabIndex={-1}>
            Capacity holds
          </h2>
          <p>
            Inspect which attempts own capacity, when a stop was requested, and
            why a slot was released.
          </p>
        </div>
      </header>
      {!sensitive ? (
        <p className="muted">Inspecting attempts requires sensitive_read.</p>
      ) : !opened ? (
        <Button variant="outline" onClick={() => setOpened(true)}>
          Inspect holds
        </Button>
      ) : (
        <>
          <div className="actions">
            <label className="check">
              <input
                type="checkbox"
                checked={released}
                onChange={(e) => {
                  setReleased(e.target.checked);
                  setPages([null]);
                }}
              />{" "}
              Include released holds
            </label>
            <Button
              variant="outline"
              disabled={data.isFetching}
              onClick={() => void data.refetch()}
            >
              Refresh holds
            </Button>
          </div>
          {data.isPending && <LoadingState label="Loading capacity holds" />}
          {data.isError && (
            <QueryError
              error={data.error}
              stale={!!data.data}
              retry={() => void data.refetch()}
            />
          )}
          {data.data &&
            (data.data.items.length === 0 ? (
              <p>No matching holds.</p>
            ) : (
              <div
                className="approval-table-scroll"
                tabIndex={0}
                role="region"
                aria-label="Capacity holds table"
              >
                <Table aria-label="Capacity holds">
                  <TableHeader>
                    <TableRow>
                      <TableHead>Execution</TableHead>
                      <TableHead>Worker</TableHead>
                      <TableHead>State</TableHead>
                      <TableHead>Acquired</TableHead>
                      <TableHead>Deadline</TableHead>
                      <TableHead>Stop requested</TableHead>
                      <TableHead>Released</TableHead>
                      <TableHead>Action</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {data.data.items.map((h) => (
                      <TableRow key={h.attempt_id}>
                        <TableCell>
                          <Link
                            to={"/executions/" + encodeURIComponent(h.task_id)}
                          >
                            {h.task_id}
                          </Link>
                          <small className="muted wrap">{h.attempt_id}</small>
                        </TableCell>
                        <TableCell>
                          <span className="wrap">{h.worker_session_id}</span>
                          <small className="muted">
                            Consumer {h.consumer_id}
                          </small>
                        </TableCell>
                        <TableCell>
                          {outcome(h)}
                          <small className="muted">
                            Hold revision {h.revision}
                          </small>
                        </TableCell>
                        <TableCell>
                          <When value={h.acquired_at} />
                          <small className="muted">
                            Policy revision {h.policy_revision}
                          </small>
                        </TableCell>
                        <TableCell>
                          <When value={h.deadline} />
                        </TableCell>
                        <TableCell>
                          <When value={h.stop_requested_at} />
                          {h.grace_ms !== null && (
                            <small className="muted">
                              Grace {h.grace_ms.toLocaleString()} ms
                            </small>
                          )}
                          {h.force_at !== null && (
                            <small className="muted">
                              Force eligible <When value={h.force_at} />
                            </small>
                          )}
                        </TableCell>
                        <TableCell>
                          <When value={h.released_at} />
                        </TableCell>
                        <TableCell>
                          {control && h.state === "uncertain" && (
                            <Button
                              variant="outline"
                              aria-label={"Review release of " + h.attempt_id}
                              onClick={(event) => {
                                opener.current = event.currentTarget;
                                setSelected(h);
                                setReleaseOpen(true);
                              }}
                            >
                              Review release
                            </Button>
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            ))}
          <div className="actions">
            <Button
              variant="outline"
              disabled={pages.length === 1}
              onClick={() => setPages((p) => p.slice(0, -1))}
            >
              Previous holds
            </Button>
            <Button
              variant="outline"
              disabled={!data.data?.next}
              onClick={() => {
                if (data.data?.next) setPages((p) => [...p, data.data.next]);
              }}
            >
              Next holds
            </Button>
          </div>
        </>
      )}
      {notice && <p role="status">{notice}</p>}
      {control && sensitive && restored && !releaseOpen && (
        <Button
          variant="outline"
          onClick={(event) => {
            opener.current = event.currentTarget;
            setReleaseOpen(true);
          }}
        >
          Reconcile previous release
        </Button>
      )}
      {control && sensitive && releaseOpen && (
        <ReleaseForm
          queue={queue}
          scope={scope}
          selected={selected}
          restoreFocus={() =>
            (opener.current?.isConnected
              ? opener.current
              : heading.current
            )?.focus()
          }
          onClose={() => setReleaseOpen(false)}
          onSuccess={() => {
            setNotice("Release accepted.");
            setSelected(null);
            setReleaseOpen(false);
            refresh();
            if (opened) void data.refetch();
          }}
        />
      )}
    </section>
  );
}
