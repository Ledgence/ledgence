// SPDX-License-Identifier: MIT
import { useQuery } from "@tanstack/react-query";
import { useInstance } from "../../app/instance";
import { apiPath, request } from "../../api/client";
import { admissionFor, admissionWaitFor } from "../../api/admissions";
import type { ExplorerNode } from "../../api/explorer";
import { LoadingState } from "../../components/async-state";
import {
  Field,
  Fields,
  JsonView,
  QueryError,
} from "../../components/resource-ui";
export function AdmissionRecord({
  node,
}: {
  node: Extract<
    ExplorerNode,
    { kind: "admission_operation" | "admission_wait" }
  >;
}) {
  const config = useInstance();
  const path = apiPath("admissions/inspect");
  const address = {
    workflow_id: node.occurrence.workflow_id,
    origin_activation_id: node.occurrence.origin_activation_id,
    key: node.occurrence.key,
  };
  const body = JSON.stringify(address);
  const query = useQuery({
    queryKey: [
      location.origin,
      config.contract_version,
      config.instance_id,
      path,
      body,
      node.kind === "admission_operation" ? node.source_id : node.key,
    ],
    gcTime: 0,
    queryFn: async ({ signal }) =>
      (
        await request(
          path,
          node.kind === "admission_operation"
            ? admissionFor(node, config.instance_id)
            : admissionWaitFor(node, config.instance_id),
          signal,
          config.instance_id,
          { body, maximumBytes: 4096 },
          256 * 1024,
        )
      ).value,
  });
  if (query.error)
    return (
      <QueryError error={query.error} retry={() => void query.refetch()} />
    );
  if (!query.data) return <LoadingState label="Loading admission record" />;
  const value = query.data;
  if ("provider" in value.binding) {
    const provider = value.binding.provider;
    const record = value.provider;
    return (
      <>
        <Fields>
          <Field label="Resource">{value.binding.resource}</Field>
          <Field label="Capacity state">
            {value.state.replaceAll("_", " ")}
          </Field>
          <Field label="Provider">
            {provider.binding.target.provider} ·{" "}
            {provider.binding.target.operation}
          </Field>
          <Field label="Call completion">
            {record?.report?.call_completion ?? "Not recorded"}
          </Field>
          <Field label="Provider occurrence">
            {record?.ticket.occurrence.workflow_id ??
              provider.ticket.occurrence.workflow_id}
          </Field>
          <Field label="Dispatch source">
            {record?.source?.provider.source_id ?? "No dispatch source"}
          </Field>
        </Fields>
        <p>
          Call completion describes capacity settlement. Provider business
          status is recorded separately in the protected operation ledger.
        </p>
        <JsonView
          value={provider.action.arguments}
          label="Approved arguments"
        />
        <JsonView
          value={record?.ticket ?? provider.ticket}
          label="Provider ticket"
        />
        {record?.report && (
          <JsonView value={record.report} label="Accepted report receipt" />
        )}
      </>
    );
  }
  return (
    <>
      <Fields>
        <Field label="Resource">{value.binding.resource}</Field>
        <Field label="Operation">
          {value.binding.operation.kind} · {value.binding.operation.version}
        </Field>
        <Field label="Source">{value.source?.source_id ?? "Not started"}</Field>
        <Field label="Program">
          {value.binding.program.program.id} ·{" "}
          {value.binding.program.program.version}
        </Field>
      </Fields>
      <JsonView
        value={value.binding.operation.arguments}
        label="Effective arguments"
      />
      {value.finish && (
        <JsonView value={value.finish.output} label="Accepted result" />
      )}
    </>
  );
}
