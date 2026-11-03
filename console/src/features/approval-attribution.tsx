// SPDX-License-Identifier: MIT
import { useQuery } from "@tanstack/react-query";
import { useAccess } from "../app/access-session";
import { useInstance } from "../app/instance";
import { acceptanceFor, acceptanceQuery, type AuditEvent } from "../api/audit";
import { apiPath, request } from "../api/client";
import type { Approval } from "../api/approvals";
import { CopyText, Field, Fields } from "../components/resource-ui";
import { Button } from "../components/ui/button";

export function ApprovalAttribution({ approval }: { approval: Approval }) {
  const config = useInstance();
  const { access } = useAccess();
  const supported = access?.mode === "credentials" && access.operator_audit;
  const allowed = supported && access.capabilities.includes("metadata_read");
  const path = apiPath("audit/events");
  const body = JSON.stringify(acceptanceQuery(approval));
  const query = useQuery<AuditEvent | null>({
    queryKey: [
      location.origin,
      config.contract_version,
      config.instance_id,
      path,
      body,
      approval.decision?.decision,
      approval.decision?.decided_at,
    ],
    // Acceptance evidence is immutable. Do not turn list polling or global
    // invalidation into repeated audit queries; failures need an explicit retry.
    enabled: (query) =>
      !!allowed && query.state.data === undefined && !query.state.error,
    queryFn: async ({ signal }) =>
      (
        await request(
          path,
          acceptanceFor(
            approval,
            config.instance_id,
            access?.mode === "credentials" ? access.scope : undefined,
          ),
          signal,
          config.instance_id,
          { body, maximumBytes: config.limits.submission_max_bytes },
          1024 * 1024,
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
        Verified attribution is unavailable with this instance or credential.
      </p>
    );
  if (query.isError)
    return (
      <section aria-label="Approval attribution" className="notice">
        <p role="alert">
          Verified attribution is unavailable. The recorded decision is
          unchanged.
        </p>
        <Button
          variant="outline"
          onClick={() => {
            void query.refetch();
          }}
        >
          Retry attribution
        </Button>
      </section>
    );
  if (query.isPending)
    return <p role="status">Checking recorded attribution…</p>;
  if (query.data === null)
    return <p className="muted">No verified actor recorded.</p>;
  return (
    <section aria-label="Approval attribution">
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
      <p className="muted">
        This identity was verified when Ledgence accepted the decision. The
        reviewer label is caller supplied.
      </p>
    </section>
  );
}
