// SPDX-License-Identifier: MIT
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useInstance } from "../app/instance";
import { apiPath, request } from "../api/client";
import { taskAdmissionFor } from "../api/task-admissions";
import { LoadingState } from "../components/async-state";
import {
  CopyText,
  Field,
  Fields,
  QueryError,
  When,
} from "../components/resource-ui";
import { Button } from "../components/ui/button";

export function TaskAdmissionPanel({
  taskId,
  resource,
  current,
}: {
  taskId: string;
  resource: string;
  current: number;
}) {
  const config = useInstance();
  const [selected, setSelected] = useState<number | null>(null);
  const [input, setInput] = useState("");
  const generation = selected ?? current;
  const path = apiPath("task-admissions/inspect");
  const body = JSON.stringify({ task_id: taskId, generation });
  const query = useQuery({
    queryKey: [
      location.origin,
      config.contract_version,
      config.instance_id,
      path,
      body,
    ],
    gcTime: 0,
    queryFn: async ({ signal }) =>
      (
        await request(
          path,
          taskAdmissionFor(taskId, generation, resource, config.instance_id),
          signal,
          config.instance_id,
          { body, maximumBytes: 4096 },
          256 * 1024,
        )
      ).value,
  });
  return (
    <section className="card">
      <form
        className="form-grid"
        onSubmit={(event) => {
          event.preventDefault();
          setSelected(input ? Number(input) : null);
        }}
      >
        <label>
          Attempt generation
          <input
            type="number"
            min={1}
            max={2147483647}
            step={1}
            placeholder={String(current)}
            value={input}
            onChange={(e) => setInput(e.target.value)}
          />
        </label>
        <Button type="submit">Inspect generation</Button>
      </form>
      {query.error ? (
        <QueryError error={query.error} retry={() => void query.refetch()} />
      ) : !query.data ? (
        <LoadingState label="Loading task admission" />
      ) : (
        <>
          <Fields>
            <Field label="Resource">{query.data.resource}</Field>
            <Field label="Generation">{query.data.occurrence.generation}</Field>
            <Field label="Admission state">
              {query.data.state.replaceAll("_", " ")}
            </Field>
            <Field label="Revision">{query.data.revision}</Field>
            <Field label="Dispatch source">
              {query.data.source ? (
                <CopyText value={query.data.source.source_id} />
              ) : (
                "No invocation authorized"
              )}
            </Field>
            <Field label="Last change">
              <When value={query.data.updated_at} />
            </Field>
          </Fields>
          {query.data.state === "waiting" && (
            <p>
              Waiting for shared capacity. No worker subprocess is reserved.
            </p>
          )}
          {query.data.state === "unknown_occupied" && (
            <p>
              The invocation may have run. Capacity stays occupied until its
              worker confirms local quiescence or an operator releases this
              source.
            </p>
          )}
          <Button type="button" onClick={() => void query.refetch()}>
            Refresh admission
          </Button>
        </>
      )}
    </section>
  );
}
