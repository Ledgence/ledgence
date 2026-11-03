// SPDX-License-Identifier: MIT
import type { EffectDetail } from "../src/api/effects";
export const effectScope = { tenant_id: "acme", namespace: "demo" };
export const effectAddress = {
  workflow_id: "wf-effect",
  activation_id: "task-effect",
  approval_key: "refund",
};
export function effectFixture(): EffectDetail {
  const occurrence = { scope: { ...effectScope }, ...effectAddress };
  return {
    binding: {
      occurrence,
      approval: {
        workflow_id: occurrence.workflow_id,
        key: occurrence.approval_key,
        activation_id: "task-request",
        revision: "0",
        protection: "external_effect",
        action: {
          name: "program:refund",
          version: "1",
          arguments: { amount: 100 },
        },
        proposed_arguments: null,
        created_at: 10,
        deadline: 100,
        status: "approved",
        decision: {
          decision_id: "decision-1",
          decision: "approve",
          reviewer: "reviewer",
          reason: null,
          decided_at: 15,
        },
        resumed_activation_id: occurrence.activation_id,
      },
      program: {
        program: { id: "refund", version: "1" },
        digest: "sha256:" + "a".repeat(64),
        size: "4096",
      },
      key: "_approval:fixture",
      lookup: "program:lookup",
      approval_acceptance_event_id: "audit-approval",
    },
    summary: {
      occurrence,
      program: { id: "refund", version: "1" },
      callable: "program:refund",
      action_version: "1",
      dispatch_provenance: { kind: "legacy_fence" },
      journal_state: "unconfirmed",
      has_discrepancy: false,
      entry_count: "0",
      effect_revision: "1",
      created_at: 15,
      updated_at: 15,
      operator_resolution: undefined,
    },
    entries: [],
    next_cursor: null,
    current_assessment: undefined,
  };
}
export function effectWire(value: unknown): unknown {
  return JSON.parse(JSON.stringify(value)) as unknown;
}

export function assessedEffectFixture(): EffectDetail {
  const value = effectFixture();
  const assessment = {
    kind: "assessment" as const,
    sequence: "1",
    recorded_at: 20,
    recorded_revision: "2",
    resolution_id: "resolution-1",
    basis_revision: "1",
    supersedes_resolution_id: null,
    assessment: {
      kind: "record_success" as const,
      result: { receipt: "1042" },
    },
    reason: "Matched receipt",
    evidence_references: ["receipt:1042"],
    actor: {
      issuer: "local",
      subject: "operator",
      class: "operator" as const,
      credential_id: "credential-1",
    },
    acceptance_event_id: "audit-resolution",
  };
  value.entries = [structuredClone(assessment)];
  value.current_assessment = assessment;
  value.summary.entry_count = "1";
  value.summary.effect_revision = "2";
  value.summary.updated_at = 20;
  value.summary.operator_resolution = {
    reference: {
      resolution_id: assessment.resolution_id,
      sequence: assessment.sequence,
      accepted_at: assessment.recorded_at,
      accepted_revision: assessment.recorded_revision,
      basis_revision: assessment.basis_revision,
      supersedes_resolution_id: null,
      assessment_kind: "record_success",
      acceptance_event_id: assessment.acceptance_event_id,
    },
    first_conflicting_entry_sequence: null,
  };
  return value;
}
