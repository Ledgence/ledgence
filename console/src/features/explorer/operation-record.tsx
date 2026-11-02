// SPDX-License-Identifier: MIT
import { useResource } from "../../api/hooks";
import type { ExplorerNode } from "../../api/explorer";
import { operationFor } from "../../api/operation";
import { LoadingState } from "../../components/async-state";
import {
  Field,
  Fields,
  JsonView,
  QueryError,
} from "../../components/resource-ui";

export function OperationRecord({
  workflowId,
  node,
}: {
  workflowId: string;
  node: Extract<ExplorerNode, { kind: "local" }>;
}) {
  const identity = {
    workflow_id: workflowId,
    activation_id: node.activation_id,
    step_key: node.key,
  };
  const query = useResource(
    "workflows/local-steps/inspect",
    identity,
    operationFor(identity, node),
    { gcTime: 0 },
  );
  if (query.error)
    return (
      <QueryError error={query.error} retry={() => void query.refetch()} />
    );
  if (!query.data) return <LoadingState label="Loading durable operation" />;
  const { step, kind, version, arguments: args, result } = query.data;
  return (
    <>
      <Fields>
        <Field label="Operation kind">
          {
            { local: "Local step", model: "Model call", tool: "Tool call" }[
              kind
            ]
          }
        </Field>
        {version !== null && <Field label="Operation version">{version}</Field>}
        <Field label="Attempt that recorded the result">
          {step.attempt_id}
        </Field>
      </Fields>
      <JsonView
        value={args}
        label={kind === "local" ? "Recorded input" : "Effective arguments"}
      />
      <JsonView value={result} label="Accepted result" />
    </>
  );
}
