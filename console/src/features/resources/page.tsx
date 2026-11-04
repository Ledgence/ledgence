// SPDX-License-Identifier: MIT
import { useState } from "react";
import { accessController } from "../../app/access-state";
import { ResourcePolicyForm } from "./policy-form";
import { ResourceSources } from "./sources";
import { Button } from "../../components/ui/button";
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
  resourceKey,
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
  enabled = true,
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
    enabled,
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
  const canControl = usePermission("resource_control");
  const [, setParams] = useSearchParams();
  const [error, setError] = useState("");
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
      {canControl && (
        <form
          className="filters list-filters"
          onSubmit={(e) => {
            e.preventDefault();
            try {
              const name = resourceName(
                String(new FormData(e.currentTarget).get("name") ?? "").trim(),
              );
              setParams({ name, new: "1" });
            } catch {
              setError(
                "Enter a resource name of up to 128 ASCII letters, digits, underscores, dots, colons or hyphens.",
              );
            }
          }}
        >
          <label>
            Resource name
            <input name="name" required onChange={() => setError("")} />
          </label>
          <Button type="submit">Create policy</Button>
          {error && <p role="alert">{error}</p>}
        </form>
      )}
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
  const config = useInstance(),
    canControl = usePermission("resource_control");
  const [params, setParams] = useSearchParams();
  const create = params.get("new") === "1";
  const [editing, setEditing] = useState(false);
  const [accepted, setAccepted] = useState<number | null>(null);
  const restored = !!accessController.recover(
    resourceKey(config.instance_id, name),
  );
  const q = useRead("inspect", { name }, inspectResource(scope, name), !create);
  const history = useRead(
    "decisions",
    { name, before, limit },
    decisionsFor(scope, name, before, limit),
    !create && config.capabilities.resource_fair_sharing,
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
      {accepted !== null && (
        <p role="status">Resource policy saved at revision {accepted}.</p>
      )}
      {(create || restored || editing) && canControl && (
        <ResourcePolicyForm
          name={name}
          scope={scope}
          snapshot={create ? null : (current ?? null)}
          onSaved={(revision) => {
            setAccepted(revision);
            setEditing(false);
            if (create) setParams({ name });
            else void q.refetch();
          }}
          onClose={(reconcile) => {
            setEditing(false);
            if (create) setParams(reconcile ? { name } : {});
            else void q.refetch();
          }}
        />
      )}
      {create && !canControl && (
        <Empty>Creating a policy requires resource_control.</Empty>
      )}
      {!create &&
        (q.isError ? (
          <QueryError error={q.error} retry={() => void q.refetch()} />
        ) : !current ? (
          <LoadingState label="Loading resource policy" />
        ) : (
          <>
            {canControl && !editing && !restored && (
              <Button
                variant="outline"
                disabled={
                  q.isFetching ||
                  (accepted !== null && current.resource.revision < accepted)
                }
                onClick={() => setEditing(true)}
              >
                Edit policy
              </Button>
            )}
            <Fields>
              <Field label="Offer / Start timeouts">
                {current.resource.policy.offer_timeout_ms} /{" "}
                {current.resource.policy.start_timeout_ms} ms
              </Field>
              <Field label="Start rate">
                {current.resource.policy.rate
                  ? current.resource.policy.rate.numerator +
                    " per " +
                    current.resource.policy.rate.period_ms +
                    " ms, burst " +
                    current.resource.policy.rate.burst
                  : "No rate limit"}
              </Field>
              <Field label="Occupied / limit">
                {current.occupied} / {current.resource.policy.max_occupancy}
              </Field>
              <Field label="Unstarted offers">{current.offered}</Field>
              <Field label="Unknown occupied">{current.unknown}</Field>
              <Field label="Current revision">
                {current.resource.revision}
              </Field>
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
        ))}
      {!create && current && config.capabilities.resource_sources && (
        <ResourceSources name={name} scope={scope} />
      )}
      {!create && config.capabilities.resource_fair_sharing && (
        <>
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
      )}
    </>
  );
}
function SourceLookup({
  name,
  scope,
}: {
  name: string | null;
  scope: QueueScope;
}) {
  const [, setParams] = useSearchParams();
  const [error, setError] = useState("");
  return (
    <>
      <PageHeading
        title={name ?? "Resources"}
        description="Find outstanding dispatch sources by resource name."
      />
      {name ? (
        <>
          <Link to="/resources">Choose another resource</Link>
          <ResourceSources key={name} name={name} scope={scope} />
        </>
      ) : (
        <form
          className="filters list-filters"
          onSubmit={(event) => {
            event.preventDefault();
            try {
              const input = String(
                new FormData(event.currentTarget).get("name") ?? "",
              ).trim();
              setParams({ name: resourceName(input) });
            } catch {
              setError("Enter a valid resource name.");
            }
          }}
        >
          <label>
            Resource name
            <input name="name" required onChange={() => setError("")} />
          </label>
          <Button type="submit">Open resource</Button>
          {error && <p role="alert">{error}</p>}
        </form>
      )}
    </>
  );
}
export function ResourcesPage() {
  const config = useInstance(),
    { access } = useAccess(),
    read = usePermission("metadata_read"),
    sensitive = usePermission("sensitive_read"),
    [params] = useSearchParams();
  if (
    access?.mode !== "credentials" ||
    (!(read && config.capabilities.resource_policies) &&
      !(sensitive && config.capabilities.resource_sources))
  )
    return (
      <>
        <PageHeading title="Resources" />
        <Empty>
          Resource access requires an authenticated resource adapter and
          permission to read metadata or retained sources.
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
  if (!read || !config.capabilities.resource_policies)
    return <SourceLookup name={name} scope={access.scope} />;
  return name === null ? (
    <List scope={access.scope} after={after} />
  ) : (
    <Detail
      key={JSON.stringify([config.instance_id, name])}
      scope={access.scope}
      name={name}
      before={before}
    />
  );
}
