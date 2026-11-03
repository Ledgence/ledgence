// SPDX-License-Identifier: MIT
import type { ProviderBinding } from "../api/providers";
import { Field, Fields, JsonView } from "../components/resource-ui";

export function ProviderBindingDetails({
  binding,
}: {
  binding: ProviderBinding;
}) {
  const policy = binding.reconciliation;
  return (
    <section aria-label="Approved provider intent">
      <h4>Provider operation</h4>
      <Fields>
        <Field label="Provider / environment">
          {binding.target.provider} · {binding.target.environment}
        </Field>
        <Field label="Account">{binding.target.account}</Field>
        <Field label="Operation / API">
          {binding.target.operation} · {binding.target.api_variant}
        </Field>
        <Field label="Endpoint">{binding.target.endpoint_identity}</Field>
        <Field label="Business identity">
          {binding.business.namespace} · {binding.business.key}
        </Field>
        <Field label="Credential reference">
          {binding.credential_reference}
        </Field>
        <Field label="Completion criterion">
          {binding.contract.completion_criterion}
        </Field>
        <Field label="Read-only recovery">
          {binding.contract.lookup.replaceAll("_", " ")}
        </Field>
        <Field label="Probe queue">{policy.queue}</Field>
        <Field label="Recovery budget">
          {policy.max_checks} checks · {policy.max_elapsed_ms / 1000}s total
        </Field>
        <Field label="First check / interval">
          {policy.initial_delay_ms / 1000}s · {policy.interval_ms / 1000}s, up
          to {policy.maximum_interval_ms / 1000}s
        </Field>
        <Field label="Callback timeout">
          {policy.check_timeout_ms / 1000}s per check
        </Field>
        <Field label="Provider deduplication key">
          {binding.deduplication?.key ?? "Unavailable"}
        </Field>
        <Field label="Adapter">
          {binding.contract.adapter_id} · {binding.contract.version}
        </Field>
        <Field label="Immutable program">
          {binding.program.program.id} · {binding.program.program.version}
        </Field>
      </Fields>
      <details>
        <summary>Complete reviewed binding</summary>
        <JsonView label="Provider binding" value={binding} />
      </details>
    </section>
  );
}
