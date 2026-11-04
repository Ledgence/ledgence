// SPDX-License-Identifier: MIT
import { useState } from "react";
import { Link, useSearchParams } from "react-router";
import { useInstance } from "../../app/instance";
import { useAccess, usePermission } from "../../app/access-session";
import { accessController } from "../../app/access-state";
import {
  queueName,
  policiesFor,
  inspectFor,
  type QueueScope,
  type QueuePolicy,
} from "../../api/queue-policies";
import {
  PageHeading,
  Empty,
  QueryError,
  Fields,
  Field,
  When,
} from "../../components/resource-ui";
import { LoadingState } from "../../components/async-state";
import { Button } from "../../components/ui/button";
import {
  Table,
  TableHeader,
  TableRow,
  TableHead,
  TableBody,
  TableCell,
} from "../../components/ui/table";
import { useQueueRead } from "./read";
import { Holds } from "./holds";
import { PolicyForm } from "./policy-form";
import { policyKey } from "./use-queue-command";
import "../../styles/queue-policies.css";

function label(value: number | null) {
  return value === null ? "Unlimited" : value === 0 ? "Paused" : String(value);
}
function timeoutLabel(policy: QueuePolicy) {
  const t = policy.timeout;
  if (!t) return "No timeout";
  return (
    "Stop after " +
    t.after_ms.toLocaleString() +
    " ms · " +
    (t.release.mode === "confirm"
      ? "wait for closure"
      : "release after " + t.release.grace_ms.toLocaleString() + " ms grace")
  );
}
function link(queue: string, create = false) {
  const p = new URLSearchParams({ queue });
  if (create) p.set("new", "1");
  return "/queues?" + p.toString();
}
function QueueList({ scope }: { scope: QueueScope }) {
  const config = useInstance();
  const canControl = usePermission("resource_control");
  const [params, setParams] = useSearchParams();
  const after = params.get("after");
  const limit = config.limits.default_page_size;
  const records = useQueueRead(
    "list",
    { after, limit },
    policiesFor(scope, limit, after),
    true,
  );
  const [error, setError] = useState("");
  const [name, setName] = useState("");
  return (
    <>
      <PageHeading
        title="Queues"
        description="Manage shared limits on concurrent attempts across workers in this scope."
      />
      {canControl && (
        <form
          className="filters list-filters"
          aria-label="Create queue policy"
          onSubmit={(e) => {
            e.preventDefault();
            try {
              const queue = queueName(name.trim());
              setParams({ queue, new: "1" });
            } catch {
              setError(
                "Enter a queue name of 1–128 bytes without control characters.",
              );
            }
          }}
        >
          <label>
            Queue name
            <input
              required
              aria-invalid={!!error}
              aria-describedby={error ? "queue-name-error" : undefined}
              value={name}
              list="concurrency-queues"
              onChange={(e) => {
                setName(e.target.value);
                setError("");
              }}
            />
          </label>
          <datalist id="concurrency-queues">
            {config.suggested_queues.map((q) => (
              <option key={q} value={q} />
            ))}
          </datalist>
          <Button type="submit">Create policy</Button>
          {error && (
            <p id="queue-name-error" role="alert">
              {error}
            </p>
          )}
        </form>
      )}
      <section className="resource-list queue-list">
        <header className="resource-list-heading">
          <div>
            <h2>Configured queue policies</h2>
            <p>Queues without a policy have no global queue limit.</p>
          </div>
        </header>
        {records.isPending && <LoadingState label="Loading queue policies" />}
        {records.isError && (
          <QueryError
            error={records.error}
            stale={!!records.data}
            retry={() => void records.refetch()}
          />
        )}
        {records.data &&
          (records.data.items.length ? (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Queue</TableHead>
                  <TableHead>Limit</TableHead>
                  <TableHead>Occupied</TableHead>
                  <TableHead>Uncertain</TableHead>
                  <TableHead>Last changed</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {records.data.items.map((q) => (
                  <TableRow key={q.record.queue}>
                    <TableCell>
                      <Link to={link(q.record.queue)}>{q.record.queue}</Link>
                    </TableCell>
                    <TableCell>
                      {label(q.record.policy.max_concurrency)}
                    </TableCell>
                    <TableCell>{q.occupied}</TableCell>
                    <TableCell>{q.uncertain}</TableCell>
                    <TableCell>
                      <When value={q.record.updated_at} />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <Empty>No configured policies on this page.</Empty>
          ))}
        <div className="actions">
          {after && (
            <Button variant="outline" onClick={() => setParams({})}>
              First page
            </Button>
          )}
          {records.data?.next && (
            <Button
              variant="outline"
              onClick={() => {
                if (records.data?.next) setParams({ after: records.data.next });
              }}
            >
              Next page
            </Button>
          )}
          <Button
            variant="outline"
            disabled={records.isFetching}
            onClick={() => void records.refetch()}
          >
            Refresh policies
          </Button>
        </div>
      </section>
    </>
  );
}
function QueueDetail({
  scope,
  queue,
  create,
}: {
  scope: QueueScope;
  queue: string;
  create: boolean;
}) {
  const config = useInstance();
  const canControl = usePermission("resource_control");
  const [, setParams] = useSearchParams();
  const [editing, setEditing] = useState(false);
  const [acceptedRevision, setAcceptedRevision] = useState<string | null>(null);
  const [notice, setNotice] = useState("");
  const q = useQueueRead(
    "inspect",
    { queue },
    inspectFor(scope, queue),
    !create,
  );
  // A policy receipt does not contain current occupancy. Wait for an authoritative
  // snapshot at least as new as the receipt before opening the next revision-bound form.
  const refreshingAccepted =
    acceptedRevision !== null &&
    (!q.data || BigInt(q.data.record.revision) < BigInt(acceptedRevision));
  const restored = !!accessController.recover(
    policyKey(config.instance_id, queue),
  );
  return (
    <>
      <Link className="back-link" to="/queues">
        ← Queues
      </Link>
      <PageHeading
        title={queue}
        description="Global queue concurrency"
        metadata={
          <span>
            {scope.tenant_id} / {scope.namespace}
          </span>
        }
      />
      {!create && q.isPending && <LoadingState label="Loading queue policy" />}
      {!create && q.isError && (
        <QueryError
          error={q.error}
          stale={!!q.data}
          retry={() => void q.refetch()}
        />
      )}
      {!create && q.data && (
        <section className="resource-list queue-detail">
          <header className="resource-list-heading">
            <div>
              <h2>Current policy</h2>
            </div>
            {canControl && !editing && !restored && (
              <Button
                variant="outline"
                disabled={refreshingAccepted}
                onClick={() => {
                  setNotice("");
                  setEditing(true);
                }}
              >
                Edit policy
              </Button>
            )}
          </header>
          <Fields>
            <Field label="Maximum concurrency">
              {label(q.data.record.policy.max_concurrency)}
            </Field>
            <Field label="Occupied">{q.data.occupied}</Field>
            <Field label="Uncertain holds">{q.data.uncertain}</Field>
            <Field label="Timeout">{timeoutLabel(q.data.record.policy)}</Field>
            <Field label="Revision">{q.data.record.revision}</Field>
            <Field label="Changed">
              <When value={q.data.record.updated_at} />
            </Field>
          </Fields>
          {q.data.record.policy.max_concurrency !== null &&
            BigInt(q.data.occupied) >
              BigInt(q.data.record.policy.max_concurrency) && (
              <p className="notice">
                Occupancy exceeds the new limit. New attempts wait while
                existing holds drain.
              </p>
            )}
        </section>
      )}
      {notice && (
        <p role="status">
          {notice}
          {refreshingAccepted && " Refreshing the current snapshot…"}
        </p>
      )}
      {(create || editing || restored) &&
        canControl &&
        (create || q.data || restored) && (
          <PolicyForm
            queue={queue}
            scope={scope}
            snapshot={create ? null : (q.data ?? null)}
            onSaved={(revision) => {
              setAcceptedRevision(revision);
              setNotice("Queue policy saved.");
              setEditing(false);
              if (create) setParams({ queue });
              else void q.refetch();
            }}
            onClose={() => {
              setEditing(false);
              if (create) setParams({});
              else void q.refetch();
            }}
          />
        )}
      {create && !canControl && (
        <Empty>
          Your credential needs resource_control to create a queue policy.
        </Empty>
      )}
      {!create && q.data && (
        <Holds queue={queue} scope={scope} refresh={() => void q.refetch()} />
      )}
    </>
  );
}
export function QueuesPage() {
  const config = useInstance();
  const { access } = useAccess();
  const read = usePermission("metadata_read");
  const [params] = useSearchParams();
  if (!config.capabilities.queue_policies || access?.mode !== "credentials")
    return (
      <>
        <PageHeading title="Queues" />
        <Empty>
          Queue policy management requires an instance with authenticated queue
          policies enabled.
        </Empty>
      </>
    );
  if (!read)
    return (
      <>
        <PageHeading title="Queues" />
        <Empty>
          Your credential needs metadata_read to inspect queue policies.
        </Empty>
      </>
    );
  const queue = params.get("queue");
  if (queue === null) return <QueueList scope={access.scope} />;
  try {
    queueName(queue);
  } catch {
    return <Empty>The queue name is invalid.</Empty>;
  }
  const create = params.get("new") === "1";
  return (
    <QueueDetail
      key={JSON.stringify([config.instance_id, queue])}
      scope={access.scope}
      queue={queue}
      create={create}
    />
  );
}
