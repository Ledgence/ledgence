// SPDX-License-Identifier: MIT
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useAccess, usePermission } from "../../app/access-session";
import { useInstance } from "../../app/instance";
import { request, apiPath } from "../../api/client";
import type { QueueScope } from "../../api/queue-policies";
import {
  sourcesFor,
  sourceInspection,
  type SourceRow,
  type SourceState,
} from "../../api/resource-sources";
import { Button } from "../../components/ui/button";
import { QueryError, When, CopyText } from "../../components/resource-ui";
import { LoadingState } from "../../components/async-state";

const explicitRead = {
  gcTime: 0,
  retry: false,
  refetchOnWindowFocus: false,
  refetchOnReconnect: false,
} as const;
function Inspect({ row }: { row: SourceRow }) {
  const config = useInstance(),
    { view } = useAccess();
  const inspect = sourceInspection(row, config.instance_id),
    body = JSON.stringify(inspect.body),
    path = apiPath(inspect.path);
  const q = useQuery({
    ...explicitRead,
    queryKey: [
      location.origin,
      config.contract_version,
      config.instance_id,
      view,
      path,
      body,
      row.revision,
    ],
    queryFn: async ({ signal }) =>
      (
        await request(
          path,
          inspect.decode,
          signal,
          config.instance_id,
          { body, maximumBytes: 4096 },
          256 * 1024,
        )
      ).value,
  });
  if (q.error)
    return <QueryError error={q.error} retry={() => void q.refetch()} />;
  if (!q.data) return <LoadingState label="Inspecting retained source" />;
  return (
    <div>
      <p>
        Latest state: {q.data.state.replaceAll("_", " ")} · revision{" "}
        {q.data.revision}
      </p>
      <p>
        Source ID <CopyText value={q.data.source} />
      </p>
    </div>
  );
}
type Position = { cursor: string; before: string };
function SourceList({ name, scope }: { name: string; scope: QueueScope }) {
  const config = useInstance(),
    { view } = useAccess();
  const [filter, setFilter] = useState<SourceState | null>(null);
  const [pages, setPages] = useState<Position[]>([]);
  const [inspected, setInspected] = useState<string | null>(null);
  const pos = pages.at(-1),
    limit = config.limits.default_page_size,
    path = apiPath("resources/sources");
  const body = JSON.stringify({
    name,
    state: filter,
    cursor: pos?.cursor ?? null,
    limit,
  });
  const q = useQuery({
    ...explicitRead,
    queryKey: [
      location.origin,
      config.contract_version,
      config.instance_id,
      view,
      path,
      body,
    ],
    queryFn: async ({ signal }) =>
      (
        await request(
          path,
          sourcesFor(scope, name, filter, pos?.before ?? null, limit),
          signal,
          config.instance_id,
          { body, maximumBytes: 8192 },
          config.limits.metadata_max_bytes,
        )
      ).value,
  });
  return (
    <div className="stack">
      <label>
        Source state
        <select
          value={filter ?? ""}
          onChange={(e) => {
            setInspected(null);
            setPages([]);
            setFilter((e.target.value || null) as SourceState | null);
          }}
        >
          <option value="">All charged sources</option>
          <option value="possibly_dispatched">Possibly dispatched</option>
          <option value="unknown_occupied">Unknown occupied</option>
        </select>
      </label>
      {q.error ? (
        <QueryError error={q.error} retry={() => void q.refetch()} />
      ) : !q.data ? (
        <LoadingState label="Loading retained sources" />
      ) : (
        <>
          {q.data.items.length === 0 ? (
            <p>No matching charged sources.</p>
          ) : (
            <ul>
              {q.data.items.map((row) => {
                const a = row.subject;
                return (
                  <li key={row.sequence}>
                    <p>
                      <strong>
                        {a.kind === "task"
                          ? "Task " +
                            a.task_id +
                            " · generation " +
                            a.generation
                          : (a.kind === "provider" ? "Provider" : "Workflow") +
                            " " +
                            a.workflow_id +
                            " · " +
                            a.origin_activation_id +
                            " / " +
                            a.key}
                      </strong>
                    </p>
                    {a.kind === "provider" && (
                      <p>
                        Approval {a.approval_activation_id} / {a.approval_key}
                      </p>
                    )}
                    <p>
                      {row.state.replaceAll("_", " ")} · revision {row.revision}{" "}
                      · <When value={row.updated_at} />
                    </p>
                    {a.kind === "task" &&
                    !config.capabilities.task_admission ? (
                      <p>
                        This adapter supports source discovery but does not
                        support exact task admission inspection.
                      </p>
                    ) : (
                      <Button
                        variant="outline"
                        onClick={() =>
                          setInspected(
                            inspected === row.sequence ? null : row.sequence,
                          )
                        }
                      >
                        {inspected === row.sequence
                          ? "Close record"
                          : "Inspect exact record"}
                      </Button>
                    )}
                    {inspected === row.sequence &&
                      (a.kind !== "task" ||
                        config.capabilities.task_admission) && (
                        <Inspect row={row} />
                      )}
                  </li>
                );
              })}
            </ul>
          )}
          <div className="actions">
            {pos && (
              <Button
                variant="outline"
                onClick={() => {
                  setInspected(null);
                  setPages((p) => p.slice(0, -1));
                }}
              >
                Previous sources
              </Button>
            )}
            {q.data.next_cursor && (
              <Button
                variant="outline"
                onClick={() => {
                  if (!q.data.next_cursor) return;
                  setInspected(null);
                  setPages((p) => [
                    ...p,
                    {
                      cursor: q.data.next_cursor!,
                      before: q.data.items.at(-1)!.sequence,
                    },
                  ]);
                }}
              >
                More sources
              </Button>
            )}
          </div>
        </>
      )}
    </div>
  );
}
export function ResourceSources({
  name,
  scope,
}: {
  name: string;
  scope: QueueScope;
}) {
  const allowed = usePermission("sensitive_read"),
    [open, setOpen] = useState(false);
  return (
    <section>
      <h2>Retained sources</h2>
      {allowed ? (
        <>
          <p>
            Find charged workflow operations, provider dispatches and task
            attempts. Each lookup records an audit event.
          </p>
          <Button variant="outline" onClick={() => setOpen(!open)}>
            {open ? "Close retained sources" : "Inspect retained sources"}
          </Button>
          {open && <SourceList name={name} scope={scope} />}
        </>
      ) : (
        <p>Inspecting retained sources requires sensitive_read.</p>
      )}
    </section>
  );
}
