// SPDX-License-Identifier: MIT
import { Link, useSearchParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { useAccess, usePermission } from "../../app/access-session";
import { useInstance } from "../../app/instance";
import { request, apiPath } from "../../api/client";
import type { Decoder } from "../../api/schema";
import {
  resourceList,
  inspectResource,
  decisionsFor,
  resourceName,
  sequence,
} from "../../api/resource-sharing";
import type { QueueScope } from "../../api/queue-policies";
import {
  PageHeading,
  Empty,
  QueryError,
  Field,
  Fields,
  When,
} from "../../components/resource-ui";
import { LoadingState } from "../../components/async-state";
import {
  Table,
  TableHead,
  TableRow,
  TableHeader,
  TableBody,
  TableCell,
} from "../../components/ui/table";

function useRead<T>(
  route: "list" | "inspect" | "decisions",
  input: object,
  decode: Decoder<T>,
) {
  const config = useInstance(),
    { view } = useAccess();
  const body = JSON.stringify(input),
    path = apiPath("resources/" + route);
  return useQuery({
    queryKey: [
      location.origin,
      config.instance_id,
      config.contract_version,
      view,
      path,
      body,
    ],
    queryFn: async ({ signal }) =>
      (
        await request(
          path,
          decode,
          signal,
          config.instance_id,
          { body, maximumBytes: 16384 },
          config.limits.metadata_max_bytes,
        )
      ).value,
  });
}
function link(input: Record<string, string>) {
  return "/resources?" + new URLSearchParams(input).toString();
}
function List({ scope, after }: { scope: QueueScope; after: string | null }) {
  const limit = useInstance().limits.default_page_size;
  const q = useRead(
    "list",
    { after, limit },
    resourceList(scope, after, limit),
  );
  return (
    <>
      <PageHeading
        title="Resources"
        description="Inspect shared capacity and see which workloads received each weighted offer."
      />
      {q.isPending ? (
        <LoadingState label="Loading resources" />
      ) : q.isError ? (
        <QueryError error={q.error} retry={() => void q.refetch()} />
      ) : (
        <>
          {q.data.items.length === 0 ? (
            <Empty>No configured resources on this page.</Empty>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Resource</TableHead>
                  <TableHead>Offer order</TableHead>
                  <TableHead>Occupied / limit</TableHead>
                  <TableHead>Policy</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {q.data.items.map((v) => (
                  <TableRow key={v.resource.name}>
                    <TableCell>
                      <Link to={link({ name: v.resource.name })}>
                        {v.resource.name}
                      </Link>
                    </TableCell>
                    <TableCell>
                      {v.resource.policy.fair_sharing ? "Weighted" : "FIFO"}
                    </TableCell>
                    <TableCell>
                      {v.occupied} / {v.resource.policy.max_occupancy}
                    </TableCell>
                    <TableCell>
                      {v.quarantined
                        ? "Quarantined"
                        : v.resource.policy.enabled
                          ? "Enabled"
                          : "Paused"}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
          <nav aria-label="Resource pages">
            {after && <Link to="/resources">First page</Link>}{" "}
            {q.data.next && (
              <Link to={link({ after: q.data.next })}>Next resources</Link>
            )}
          </nav>
        </>
      )}
    </>
  );
}
function Detail({
  scope,
  name,
  before,
}: {
  scope: QueueScope;
  name: string;
  before: string | null;
}) {
  const limit = useInstance().limits.default_page_size;
  const q = useRead("inspect", { name }, inspectResource(scope, name));
  const history = useRead(
    "decisions",
    { name, before, limit },
    decisionsFor(scope, name, before, limit),
  );
  const current = q.data,
    fair = current?.resource.policy.fair_sharing;
  return (
    <>
      <Link to="/resources">All resources</Link>
      <PageHeading
        title={name}
        description="Capacity and committed offers in this resource."
      />
      {q.isError ? (
        <QueryError error={q.error} retry={() => void q.refetch()} />
      ) : !current ? (
        <LoadingState label="Loading resource policy" />
      ) : (
        <>
          <Fields>
            <Field label="Occupied / limit">
              {current.occupied} / {current.resource.policy.max_occupancy}
            </Field>
            <Field label="Unstarted offers">{current.offered}</Field>
            <Field label="Unknown occupied">{current.unknown}</Field>
            <Field label="Current revision">{current.resource.revision}</Field>
            <Field label="Policy">
              {current.quarantined
                ? "Quarantined"
                : current.resource.policy.enabled
                  ? "Enabled"
                  : "Paused"}
            </Field>
          </Fields>
          <h2>Offer order</h2>
          {fair ? (
            <>
              <p>
                When groups compete, their weights determine how often each
                receives the next offer. A lone eligible group can use all
                available capacity.
              </p>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Group</TableHead>
                    <TableHead>Weight</TableHead>
                    <TableHead>Source queues</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {fair.groups.map((g) => (
                    <TableRow key={g.name}>
                      <TableCell>{g.name}</TableCell>
                      <TableCell>{g.weight}</TableCell>
                      <TableCell>
                        {[
                          ...g.queues,
                          ...(g.name === fair.default_group
                            ? ["All unmapped queues"]
                            : []),
                        ].join(", ") || "None"}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </>
          ) : (
            <p>FIFO. The oldest eligible request receives the next offer.</p>
          )}
        </>
      )}
      <h2>Offer history</h2>
      <p>
        Each row records a weighted reservation, before the worker starts.
        Previously recorded offers remain visible after policy changes.
      </p>
      {history.isError ? (
        <QueryError
          error={history.error}
          retry={() => void history.refetch()}
        />
      ) : !history.data ? (
        <LoadingState label="Loading offer decisions" />
      ) : (
        <>
          {history.data.items.length === 0 ? (
            <Empty>No weighted offers on this page.</Empty>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Offer / time</TableHead>
                  <TableHead>Execution</TableHead>
                  <TableHead>Group / queue</TableHead>
                  <TableHead>Why it received the offer</TableHead>
                  <TableHead>Policy revision</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {history.data.items.map((row) => {
                  const subject = row.subject,
                    task = subject.kind === "task",
                    id = task ? subject.task_id : subject.workflow_id;
                  return (
                    <TableRow key={row.sequence}>
                      <TableCell>
                        #{row.sequence}
                        <br />
                        <When value={row.offered_at} />
                      </TableCell>
                      <TableCell>
                        <Link
                          to={
                            (task ? "/executions/" : "/workflows/") +
                            encodeURIComponent(id)
                          }
                        >
                          {id}
                        </Link>
                        <br />
                        {task
                          ? "Task generation " + subject.generation
                          : subject.kind + " · " + subject.key}
                      </TableCell>
                      <TableCell>
                        {row.group} / {row.queue}
                      </TableCell>
                      <TableCell>
                        {row.reason === "sole_eligible"
                          ? "Only eligible group"
                          : "Weighted turn across " +
                            row.eligible_groups +
                            " eligible groups"}{" "}
                        · weight {row.weight}, round {row.cycle_round}
                      </TableCell>
                      <TableCell>{row.policy_revision}</TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          )}
          <nav aria-label="Offer history pages">
            {before && <Link to={link({ name })}>Newest offers</Link>}{" "}
            {history.data.next && (
              <Link to={link({ name, before: history.data.next })}>
                Older offers
              </Link>
            )}
          </nav>
        </>
      )}
    </>
  );
}
export function ResourcesPage() {
  const config = useInstance(),
    { access } = useAccess(),
    read = usePermission("metadata_read"),
    [params] = useSearchParams();
  if (
    !config.capabilities.resource_fair_sharing ||
    access?.mode !== "credentials" ||
    !read
  )
    return (
      <>
        <PageHeading title="Resources" />
        <Empty>
          Resource inspection requires authenticated resource admission and
          metadata_read access.
        </Empty>
      </>
    );
  const name = params.get("name"),
    before = params.get("before"),
    after = params.get("after");
  try {
    if (name !== null) resourceName(name);
    if (before !== null) sequence(before);
    if (after !== null) resourceName(after);
  } catch {
    return (
      <>
        <PageHeading title="Invalid resource link" />
        <Link to="/resources">All resources</Link>
      </>
    );
  }
  return name === null ? (
    <List scope={access.scope} after={after} />
  ) : (
    <Detail scope={access.scope} name={name} before={before} />
  );
}
