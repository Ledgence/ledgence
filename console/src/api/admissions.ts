import { providerBindingRequest } from "./providers";
// SPDX-License-Identifier: MIT
import * as s from "./schema";
import { budgetDenial, budgetUsage } from "./run-budgets";
import { ContractError } from "./codecs";
import { admissionOccurrence, program, providerTicket } from "./resources";
import { stringifyUserJson } from "./json";
import type { ExplorerNode } from "./explorer";
const positive = s.integer(1, Number.MAX_SAFE_INTEGER);
const digest = s.refine(
  s.string,
  (v) => /^sha256:[0-9a-f]{64}$/.test(v),
  "Invalid admission digest.",
);
const scope = s.object({ tenant_id: s.id, namespace: s.id });
const same = (a: unknown, b: unknown) =>
  stringifyUserJson(a) === stringifyUserJson(b);
const bounded = (v: unknown) =>
  new TextEncoder().encode(stringifyUserJson(v)).length <= 65536;
const operation = s.object({
  callable: s.id,
  kind: s.enumeration("model", "tool"),
  version: s.id,
  arguments: s.refine(
    s.payload,
    (v) => s.isJsonObject(v) && bounded(v),
    "Invalid admission arguments.",
  ),
});
const operationBinding = s.object({
  resource: s.id,
  program: s.object({ program, digest, size: positive }),
  operation,
});
const providerBinding = s.refine(
  s.object({
    resource: s.id,
    program: s.object({ program, digest, size: positive }),
    provider: s.object({
      ticket: providerTicket,
      binding: providerBindingRequest,
      action: s.refine(
        s.object({
          name: s.id,
          version: s.id,
          arguments: s.refine(
            s.payload,
            s.isJsonObject,
            "Invalid approved arguments.",
          ),
        }),
        (v) =>
          new TextEncoder().encode(stringifyUserJson(v)).length <= 32 * 1024,
        "Approved action exceeds its limit.",
      ),
    }),
  }),
  (v) =>
    v.provider.binding.admission?.resource === v.resource &&
    v.provider.action.name === v.provider.binding.contract.dispatch_callable &&
    v.provider.action.version === v.provider.binding.contract.version,
  "Provider admission intent mismatch.",
);
function binding(raw: unknown) {
  return s.isJsonObject(raw) && Object.hasOwn(raw, "provider")
    ? providerBinding(raw)
    : operationBinding(raw);
}
const offer = s.object({
  occurrence: admissionOccurrence,
  generation: positive,
  policy_revision: positive,
  activation_id: s.id,
  expires_at: s.timestamp,
  binding_digest: digest,
});
const source = s.refine(
  s.object({
    occurrence: admissionOccurrence,
    source_id: s.id,
    start_id: s.id,
    finish_id: s.id,
    generation: positive,
    binding_digest: digest,
    owner: s.object({
      scope,
      task_id: s.id,
      attempt_id: s.id,
      lease_id: s.id,
      generation: s.integer(1),
      worker_session_id: s.id,
      consumer_id: s.integer(),
    }),
    worker: s.object({
      audience: s.object({ authority_id: s.id, instance_id: s.id, scope }),
      issuer: s.id,
      subject: s.id,
      credential_id: s.id,
    }),
    accepted_at: s.timestamp,
    start_deadline_at: s.timestamp,
  }),
  (v) =>
    same(v.occurrence.scope, v.owner.scope) &&
    same(v.owner.scope, v.worker.audience.scope) &&
    v.start_deadline_at > v.accepted_at &&
    v.start_deadline_at - v.accepted_at <= 30000,
  "Inconsistent admission source.",
);
const finish = s.refine(
  s.withOptional(
    {
      source,
      output: s.refine(
        s.payload,
        bounded,
        "Admission output exceeds its limit.",
      ),
      accepted_at: s.timestamp,
      already_accepted: s.boolean,
    },
    { usage: budgetUsage },
  ),
  (v) => v.accepted_at >= v.source.accepted_at,
  "Finish predates its source.",
);
const providerSource = s.refine(
  s.withOptional(
    {
      source_id: s.id,
      claim_id: s.id,
      report_id: s.id,
      ticket: providerTicket,
      worker: s.object({
        audience: s.object({ authority_id: s.id, instance_id: s.id, scope }),
        issuer: s.id,
        subject: s.id,
        credential_id: s.id,
      }),
      accepted_at: s.timestamp,
      first_possible_dispatch_at: s.timestamp,
      reconciliation_deadline: s.timestamp,
    },
    {
      budget_grant: s.object({
        authorized_at: s.timestamp,
        expires_at: s.timestamp,
      }),
    },
  ),
  (v) =>
    (!v.budget_grant ||
      (v.budget_grant.authorized_at === v.accepted_at &&
        v.budget_grant.expires_at > v.accepted_at &&
        v.budget_grant.expires_at - v.accepted_at <= 5000)) &&
    v.accepted_at === v.first_possible_dispatch_at &&
    v.reconciliation_deadline > v.accepted_at &&
    same(v.worker.audience.scope, v.ticket.occurrence.scope),
  "Invalid provider source.",
);
const combinedSource = s.refine(
  s.object({ admission: source, provider: providerSource }),
  (v) =>
    same(v.admission.worker, v.provider.worker) &&
    v.admission.accepted_at === v.provider.accepted_at &&
    v.admission.start_id === v.provider.claim_id &&
    v.admission.finish_id === v.provider.report_id &&
    same(v.admission.occurrence.scope, v.provider.ticket.occurrence.scope) &&
    v.admission.occurrence.workflow_id ===
      v.provider.ticket.occurrence.workflow_id &&
    v.admission.occurrence.origin_activation_id ===
      v.provider.ticket.occurrence.activation_id,
  "Unlinked provider admission sources.",
);
const nativeRevision = (raw: unknown) => {
  const text = stringifyUserJson(raw);
  if (!/^[1-9][0-9]{0,19}$/.test(text) || BigInt(text) > 18446744073709551615n)
    throw new ContractError("Invalid provider revision.");
  return text;
};
const providerReceipt = s.object({
  ticket: providerTicket,
  source_id: s.id,
  source: s.object({ kind: s.enumeration("dispatch"), claim_id: s.id }),
  report_id: s.id,
  report_digest: digest,
  worker: s.object({
    audience: s.object({ authority_id: s.id, instance_id: s.id, scope }),
    issuer: s.id,
    subject: s.id,
    credential_id: s.id,
  }),
  accepted_at: s.timestamp,
  recorded_revision: nativeRevision,
  accepted_via: s.enumeration("evidence_only"),
  already_accepted: s.boolean,
});
const combinedReceipt = s.refine(
  s.object({
    source: combinedSource,
    call_completion: s.enumeration("completed", "unknown"),
    receipt: providerReceipt,
  }),
  (v) =>
    same(v.receipt.ticket, v.source.provider.ticket) &&
    v.receipt.source_id === v.source.provider.source_id &&
    v.receipt.source.claim_id === v.source.provider.claim_id &&
    v.receipt.report_id === v.source.provider.report_id &&
    same(v.receipt.worker.audience, v.source.provider.worker.audience) &&
    v.receipt.worker.issuer === v.source.provider.worker.issuer &&
    v.receipt.worker.subject === v.source.provider.worker.subject &&
    v.receipt.accepted_at >= v.source.provider.accepted_at,
  "Unlinked provider report receipt.",
);
const providerRecord = s.refine(
  s.object({
    ticket: providerTicket,
    source: s.nullable(combinedSource),
    report: s.nullable(combinedReceipt),
  }),
  (v) =>
    (!v.source || same(v.source.provider.ticket, v.ticket)) &&
    (!v.report || same(v.report.source, v.source)),
  "Unlinked provider admission record.",
);
const snapshotFields = {
  occurrence: admissionOccurrence,
  binding,
  state: s.enumeration(
    "waiting",
    "offered",
    "awaiting_rewait",
    "possibly_dispatched",
    "unknown_occupied",
    "finished",
    "closed_before_start",
    "administratively_released",
  ),
  revision: positive,
  created_at: s.timestamp,
  updated_at: s.timestamp,
  deadline_at: s.timestamp,
  offer: s.nullable(offer),
  source: s.nullable(source),
  finish: s.nullable(finish),
};
const snapshotShape = s.withOptional(snapshotFields, {
  provider: providerRecord,
  budget_denial: budgetDenial,
});
export const admissionSnapshot = s.refine(
  snapshotShape,
  (v) => {
    if (v.budget_denial && v.state !== "closed_before_start") return false;
    if (v.updated_at < v.created_at || v.deadline_at < v.created_at)
      return false;
    if (
      v.offer &&
      (!same(v.offer.occurrence, v.occurrence) ||
        v.offer.expires_at > v.deadline_at)
    )
      return false;
    if (
      v.source &&
      (!same(v.source.occurrence, v.occurrence) ||
        v.source.accepted_at > v.updated_at ||
        v.source.accepted_at < v.created_at ||
        v.source.accepted_at >= v.deadline_at)
    )
      return false;
    if (
      v.finish &&
      (v.finish.accepted_at > v.updated_at ||
        !v.source ||
        !same(v.finish.source, v.source))
    )
      return false;
    if ("provider" in v.binding) {
      const ticket = v.binding.provider.ticket;
      if (
        v.finish ||
        !same(ticket.occurrence.scope, v.occurrence.scope) ||
        ticket.occurrence.workflow_id !== v.occurrence.workflow_id ||
        ticket.occurrence.activation_id !== v.occurrence.origin_activation_id
      )
        return false;
      if (v.provider) {
        const record = v.provider;
        if (
          !same(record.ticket.occurrence.scope, v.occurrence.scope) ||
          record.ticket.binding_digest !== ticket.binding_digest ||
          !same(v.source, record.source?.admission ?? null) ||
          (record.source && !same(record.ticket, ticket)) ||
          (record.report && record.report.receipt.accepted_at > v.updated_at)
        )
          return false;
        const valid =
          v.state === "closed_before_start"
            ? record.source === null
            : v.state === "possibly_dispatched"
              ? record.source !== null && record.report === null
              : v.state === "unknown_occupied"
                ? record.source !== null &&
                  (!record.report ||
                    record.report.call_completion === "unknown")
                : v.state === "finished"
                  ? record.report?.call_completion === "completed"
                  : v.state === "administratively_released" &&
                    record.source !== null;
        if (!valid) return false;
      } else if (v.source) return false;
    } else if (v.provider) return false;
    switch (v.state) {
      case "waiting":
      case "awaiting_rewait":
      case "closed_before_start":
        return v.offer === null && v.source === null && v.finish === null;
      case "offered":
        return v.offer !== null && v.source === null && v.finish === null;
      case "possibly_dispatched":
      case "unknown_occupied":
        return v.offer === null && v.source !== null && v.finish === null;
      case "finished":
        return (
          v.offer === null &&
          v.source !== null &&
          (v.finish !== null ||
            v.provider?.report?.call_completion === "completed")
        );
      case "administratively_released":
        return v.offer === null && v.source !== null;
    }
  },
  "Inconsistent admission snapshot.",
);
export function admissionFor(
  node: Extract<ExplorerNode, { kind: "admission_operation" }>,
  instanceId: string,
) {
  return (raw: unknown) => {
    const v = admissionSnapshot(raw);
    const f = v.finish;
    if (
      !("operation" in v.binding) ||
      !same(v.occurrence, node.occurrence) ||
      v.binding.resource !== node.resource ||
      v.binding.operation.callable !== node.callable ||
      v.binding.operation.kind !== node.operation_kind ||
      v.binding.operation.version !== node.operation_version ||
      !f ||
      f.source.source_id !== node.source_id ||
      f.source.binding_digest !== node.binding_digest ||
      f.source.owner.task_id !== node.activation_id ||
      f.source.owner.attempt_id !== node.accepting_attempt_id ||
      f.accepted_at !== node.accepted_at ||
      f.source.worker.audience.instance_id !== instanceId
    )
      throw new ContractError(
        "Admission result does not match the selected operation.",
      );
    if (!("operation" in v.binding))
      throw new ContractError("Expected operation binding.");
    return { ...v, binding: v.binding };
  };
}

export function admissionWaitFor(
  node: Extract<ExplorerNode, { kind: "admission_wait" }>,
  instanceId: string,
) {
  return (raw: unknown) => {
    const value = admissionSnapshot(raw);
    if (
      !same(value.occurrence, node.occurrence) ||
      value.binding.resource !== node.resource ||
      (value.source &&
        value.source.worker.audience.instance_id !== instanceId) ||
      (node.subject.kind === "provider"
        ? !("provider" in value.binding) ||
          !same(value.binding.provider.ticket, node.subject.ticket)
        : !("operation" in value.binding))
    )
      throw new ContractError(
        "Admission record does not match the selected wait.",
      );
    return value;
  };
}
