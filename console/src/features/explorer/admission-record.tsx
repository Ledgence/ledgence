// SPDX-License-Identifier: MIT
import { useQuery } from "@tanstack/react-query";
import { useInstance } from "../../app/instance";
import { apiPath, request } from "../../api/client";
import { admissionFor } from "../../api/admissions";
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
  node: Extract<ExplorerNode, { kind: "admission_operation" }>;
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
      node.source_id,
    ],
    gcTime: 0,
    queryFn: async ({ signal }) =>
      (
        await request(
          path,
          admissionFor(node, config.instance_id),
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
  if (!query.data) return <LoadingState label="Loading accepted operation" />;
  const value = query.data;
  return (
    <>
      <Fields>
        <Field label="Resource">{value.binding.resource}</Field>
        <Field label="Operation">
          {value.binding.operation.kind} · {value.binding.operation.version}
        </Field>
        <Field label="Source">{value.finish!.source.source_id}</Field>
        <Field label="Program">
          {value.binding.program.program.id} ·{" "}
          {value.binding.program.program.version}
        </Field>
      </Fields>
      <JsonView
        value={value.binding.operation.arguments}
        label="Effective arguments"
      />
      <JsonView value={value.finish!.output} label="Accepted result" />
    </>
  );
}
