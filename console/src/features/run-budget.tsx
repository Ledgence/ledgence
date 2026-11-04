// SPDX-License-Identifier: MIT
import { useState } from "react";
import { Link } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { useAccess, usePermission } from "../app/access-session";
import { useInstance } from "../app/instance";
import { apiPath, request } from "../api/client";
import { ApiError } from "../api/errors";
import type { Decoder } from "../api/schema";
import {
  snapshotFor,
  entriesFor,
  subjectId,
  type BudgetSubject,
} from "../api/run-budgets";
import {
  Fields,
  Field,
  QueryError,
  When,
  Empty,
} from "../components/resource-ui";
import {
  Table,
  TableHeader,
  TableHead,
  TableRow,
  TableBody,
  TableCell,
} from "../components/ui/table";
import { Button } from "../components/ui/button";
import { LoadingState } from "../components/async-state";

function Execution({ value }: { value: BudgetSubject }) {
  const id = subjectId(value);
  return (
    <Link
      to={
        (value.kind === "task" ? "/executions/" : "/workflows/") +
        encodeURIComponent(id)
      }
    >
      {id}
    </Link>
  );
}
function useRead<T>(
  route: "inspect" | "entries",
  body: object,
  decode: Decoder<T>,
  enabled: boolean,
) {
  const config = useInstance(),
    { view } = useAccess(),
    path = apiPath("budgets/" + route),
    input = JSON.stringify(body);
  return useQuery({
    queryKey: [
      location.origin,
      config.instance_id,
      config.contract_version,
      view,
      path,
      input,
    ],
    enabled,
    retry: false,
    queryFn: async ({ signal }) =>
      (
        await request(
          path,
          decode,
          signal,
          config.instance_id,
          { body: input, maximumBytes: 16384 },
          config.limits.metadata_max_bytes,
        )
      ).value,
  });
}
export function RunBudgetPanel({ subject }: { subject: BudgetSubject }) {
  const config = useInstance(),
    { access } = useAccess(),
    read = usePermission("metadata_read");
  if (
    !config.capabilities.run_budgets ||
    access?.mode !== "credentials" ||
    !read
  )
    return (
      <Empty>Budget inspection requires authenticated metadata access.</Empty>
    );
  return (
    <Detail
      key={subject.kind + ":" + subjectId(subject)}
      subject={subject}
      scope={access.scope}
    />
  );
}
function Detail({
  subject,
  scope,
}: {
  subject: BudgetSubject;
  scope: { tenant_id: string; namespace: string };
}) {
  const [before, setBefore] = useState<string | null>(null),
    limit = useInstance().limits.default_page_size;
  const q = useRead("inspect", { subject }, snapshotFor(scope, subject), true);
  const ledger = useRead(
    "entries",
    { subject, before, limit },
    entriesFor(scope, subject, before, limit),
    q.isSuccess,
  );
  if (q.isPending) return <LoadingState label="Loading run budget" />;
  if (q.isError)
    return q.error instanceof ApiError && q.error.status === 404 ? (
      <Empty>This execution has no shared run budget.</Empty>
    ) : (
      <QueryError error={q.error} retry={() => void q.refetch()} />
    );
  const b = q.data;
  return (
    <section className="card">
      <h3>Shared run budget</h3>
      <Fields>
        <Field label="Budget owner">
          <Execution value={b.root} />
        </Field>
        <Field label="Authorized calls">
          {b.calls} / {b.policy.max_calls ?? "Unlimited"}
        </Field>
        <Field label="Delegations">
          {b.delegations} / {b.policy.max_delegations ?? "Unlimited"}
        </Field>
        <Field label="Deadline">
          {b.policy.deadline_at_ms === undefined ? (
            "None"
          ) : (
            <When value={b.policy.deadline_at_ms} />
          )}
        </Field>
      </Fields>
      <h3>Budget ledger</h3>
      <p>
        Each entry records a call or delegation decision across the entire
        execution tree.
      </p>
      {ledger.isPending ? (
        <LoadingState label="Loading budget entries" />
      ) : ledger.isError ? (
        <QueryError error={ledger.error} retry={() => void ledger.refetch()} />
      ) : (
        <>
          {ledger.data.items.length === 0 ? (
            <Empty>No budget entries on this page.</Empty>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Time</TableHead>
                  <TableHead>Execution / source</TableHead>
                  <TableHead>Decision</TableHead>
                  <TableHead>Token usage</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {ledger.data.items.map((v) => (
                  <TableRow key={v.sequence}>
                    <TableCell>
                      #{v.sequence}
                      <br />
                      <When value={v.recorded_at} />
                    </TableCell>
                    <TableCell>
                      <Execution value={v.subject} />
                      <br />
                      {v.source_id}
                      {v.approval && (
                        <>
                          <br />
                          <Link
                            to={
                              "/approvals/review?" +
                              new URLSearchParams({
                                workflow: v.approval.workflow_id,
                                key: v.approval.approval_key,
                                activation: v.approval.activation_id,
                              })
                            }
                          >
                            Review approval
                          </Link>
                        </>
                      )}
                    </TableCell>
                    <TableCell>
                      {v.action} · {v.outcome}
                      {v.reason && " · " + v.reason.replaceAll("_", " ")}
                    </TableCell>
                    <TableCell>
                      {v.usage.kind === "unknown" ? (
                        v.action === "call" && v.outcome === "authorized" ? (
                          "Unknown"
                        ) : (
                          "—"
                        )
                      ) : (
                        <>
                          {v.usage.kind} · input{" "}
                          {v.usage.input_tokens ?? "unknown"} · output{" "}
                          {v.usage.output_tokens ?? "unknown"}
                        </>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
          {before && (
            <Button variant="ghost" onClick={() => setBefore(null)}>
              Newest entries
            </Button>
          )}
          {ledger.data.next && (
            <Button variant="ghost" onClick={() => setBefore(ledger.data.next)}>
              Older entries
            </Button>
          )}
        </>
      )}
      <Button
        variant="ghost"
        onClick={() => {
          void q.refetch();
          void ledger.refetch();
        }}
      >
        Refresh budget
      </Button>
    </section>
  );
}
