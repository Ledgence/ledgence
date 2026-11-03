// SPDX-License-Identifier: MIT
import type { ProviderBinding } from "../src/api/providers";
import type { EffectDetail, EffectEntry } from "../src/api/effects";
import { effectFixture, effectScope } from "./effect-fixture";

export function providerBindingFixture(): ProviderBinding {
  return {
    v: 1,
    business: { namespace: "exports", key: "exp-1042" },
    contract: {
      adapter_id: "example.export",
      version: "1",
      digest: "sha256:" + "c".repeat(64),
      completion_criterion: "Export ready to download",
      dispatch_callable: "program:refund",
      reconcile_callable: "program:inspect_export",
      lookup: "by_binding",
    },
    target: {
      provider: "example",
      environment: "test",
      account: "acme",
      api_variant: "v1",
      operation: "export",
      endpoint_identity: "http://localhost:8090",
      dimensions: { region: "local" },
    },
    deduplication: {
      domain: {
        provider: "example",
        namespace: "exports",
        dimensions: { account: "acme" },
      },
      key: "export-1042",
      horizon: { kind: "minimum_duration", duration_ms: 86400000 },
    },
    credential_reference: "exports-local",
    reconciliation: {
      queue: "probes",
      initial_delay_ms: 30_000,
      interval_ms: 5000,
      maximum_interval_ms: 300_000,
      max_checks: 12,
      max_elapsed_ms: 3_600_000,
      check_timeout_ms: 10_000,
    },
    program: effectFixture().binding.program,
  };
}

export function providerFixture(): EffectDetail {
  const detail = effectFixture();
  const bound = providerBindingFixture();
  detail.binding.approval.protection = "provider_operation";
  detail.binding.approval.provider_binding = bound;
  detail.binding.lookup = null;
  const worker = {
    audience: {
      authority_id: "authority",
      instance_id: "instance",
      scope: effectScope,
    },
    issuer: "local",
    subject: "worker",
    credential_id: "credential",
  };
  const report: EffectEntry & { kind: "provider_report" } = {
    kind: "provider_report",
    sequence: "2",
    recorded_at: 30,
    recorded_revision: "3",
    source_id: "source-1",
    source: { kind: "dispatch", claim_id: "claim-1" },
    report_id: "report-1",
    worker,
    ingress: "live_dispatch",
    report: {
      reported_target: bound.target,
      provider_reference: "export-1042",
      provider_request_id: "request-1",
      knowledge: { kind: "pending" },
    },
  };
  detail.entries = [
    {
      kind: "provider_dispatch",
      sequence: "1",
      recorded_at: 20,
      recorded_revision: "2",
      claim_id: "claim-1",
      source_id: "source-1",
      report_id: "report-1",
      worker,
    },
    report,
  ];
  detail.summary.dispatch_provenance = {
    kind: "verified_claim",
    claim_id: "claim-1",
  };
  detail.summary.entry_count = "2";
  detail.summary.effect_revision = "3";
  detail.summary.updated_at = 30;
  detail.summary.provider = {
    binding_digest: "sha256:" + "d".repeat(64),
    knowledge: "pending",
    selected_report: {
      report_id: "report-1",
      source_id: "source-1",
      sequence: "2",
      recorded_revision: "3",
      recorded_at: 30,
      knowledge: "pending",
    },
    conflicting_reports: [],
    handling: "unresolved",
    candidate_revision: null,
    reconciliation: {
      queue: "probes",
      checks_used: 0,
      max_checks: 12,
      generation: "0",
      active_probe_id: null,
      active_task_id: null,
      first_due_at: 50,
      next_due_at: 50,
      deadline_at: 1000,
      stopped_reason: null,
    },
  };
  detail.provider = { current_report: report };
  return detail;
}
