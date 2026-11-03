// SPDX-License-Identifier: MIT
import { useQuery } from "@tanstack/react-query";
import { useAccess } from "../app/access-session";
import { useInstance } from "../app/instance";
import {
  effectAcceptanceFor,
  effectAcceptanceQuery,
  type AuditEvent,
} from "../api/audit";
import { apiPath, request } from "../api/client";
import type { EffectResolutionReceipt } from "../api/effects";
import { CopyText, Field, Fields } from "../components/resource-ui";
import { Button } from "../components/ui/button";

export function EffectAttribution({
  receipt,
}: {
  receipt: EffectResolutionReceipt;
}) {
  const config = useInstance();
  const { access } = useAccess();
  const allowed =
    access?.mode === "credentials" &&
    access.operator_audit &&
    access.capabilities.includes("metadata_read");
  const path = apiPath("audit/events");
  const body = JSON.stringify(
    effectAcceptanceQuery(receipt.acceptance_event_id),
  );
  const query = useQuery<AuditEvent | null>({
    queryKey: [
      globalThis.location.origin,
      config.contract_version,
      config.instance_id,
      path,
      body,
    ],
    enabled: (query) =>
      !!allowed && query.state.data === undefined && !query.state.error,
    queryFn: async ({ signal }) =>
      (
        await request(
          path,
          effectAcceptanceFor(receipt, config.instance_id),
          signal,
          config.instance_id,
          { body, maximumBytes: 16384 },
          32768,
        )
      ).value,
    retry: false,
    staleTime: Infinity,
    refetchInterval: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
  });
  if (!allowed)
    return (
      <p className="muted">
        Verified attribution requires metadata_read and operator auditing.
      </p>
    );
  if (query.error)
    return (
      <section aria-label="Assessment attribution">
        <p role="alert">
          Verified attribution is unavailable. The assessment receipt remains
          recorded.
        </p>
        <Button variant="outline" onClick={() => void query.refetch()}>
          Retry attribution
        </Button>
      </section>
    );
  if (query.isPending)
    return <p role="status">Checking recorded attribution…</p>;
  if (!query.data) return <p className="muted">No verified actor recorded.</p>;
  return (
    <section aria-label="Assessment attribution">
      <Fields>
        <Field label="Accepted by">{query.data.actor.subject}</Field>
        <Field label="Actor type">
          {query.data.actor.class === "service" ? "Service" : "Operator"}
        </Field>
        <Field label="Identity issuer">{query.data.actor.issuer}</Field>
        <Field label="Acceptance event">
          <CopyText value={query.data.event_id} />
        </Field>
      </Fields>
    </section>
  );
}
