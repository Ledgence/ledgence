// SPDX-License-Identifier: MIT
import * as s from "./schema";
import { ContractError } from "./codecs";
import { approval, approvalAction, sameAction } from "./approvals";
import { descriptor, program } from "./resources";
import { parseUserJson, stringifyUserJson } from "./json";

const byteLength = (value: unknown) =>
  new TextEncoder().encode(stringifyUserJson(value)).length;
function validPlatformText(value: string, limit: number) {
  return (
    value.length > 0 &&
    new TextEncoder().encode(value).length <= limit &&
    [...value].every((character) => {
      const code = character.codePointAt(0)!;
      return (
        code >= 32 &&
        !(code >= 127 && code <= 159) &&
        !(code >= 0xfdd0 && code <= 0xfdef) &&
        (code & 0xfffe) !== 0xfffe
      );
    })
  );
}
const text = (limit: number) =>
  s.refine(
    s.string,
    (value) => validPlatformText(value, limit),
    "Effect identifier exceeds its limit.",
  );
const id = text(128);
const positive = s.refine(
  s.decimal,
  (value) => BigInt(value) > 0n,
  "Effect revision must be positive.",
);
const scope = s.object({ tenant_id: id, namespace: id });
export const effectOccurrence = s.object({
  scope,
  workflow_id: id,
  activation_id: id,
  approval_key: id,
});
export type EffectOccurrence = s.Decoded<typeof effectOccurrence>;
export type EffectAddress = Omit<EffectOccurrence, "scope">;
const audience = s.object({ authority_id: id, instance_id: id, scope });
const worker = s.object({
  audience,
  issuer: text(256),
  subject: id,
  credential_id: id,
});
const actor = s.object({
  issuer: text(256),
  subject: id,
  class: s.enumeration("operator", "service"),
  credential_id: id,
});
const reason = s.refine(
  s.string,
  (value) =>
    value.trim().length > 0 && new TextEncoder().encode(value).length <= 4096,
  "A reason of at most 4096 bytes is required.",
);
const reference = text(1024);
const result = s.refine(
  s.payload,
  (value) => byteLength(value) <= 128 * 1024,
  "Assessment result exceeds 128 KiB.",
);
const failure = s.object({
  kind: id,
  message: s.refine(
    s.string,
    (value) => new TextEncoder().encode(value).length <= 4096,
    "Failure message exceeds 4096 bytes.",
  ),
});
export const effectAssessment = s.variant({
  record_success: s.object({ kind: s.enumeration("record_success"), result }),
  record_failure: s.object({ kind: s.enumeration("record_failure"), failure }),
  close_unknown: s.object({ kind: s.enumeration("close_unknown") }),
});
export type EffectAssessment = s.Decoded<typeof effectAssessment>;
const assessmentKind = s.enumeration(
  "record_success",
  "record_failure",
  "close_unknown",
);
const resolutionReference = s.refine(
  s.object({
    resolution_id: id,
    sequence: positive,
    accepted_at: s.timestamp,
    accepted_revision: positive,
    basis_revision: positive,
    supersedes_resolution_id: s.nullable(id),
    assessment_kind: assessmentKind,
    acceptance_event_id: id,
  }),
  (value) =>
    BigInt(value.sequence) < BigInt(value.accepted_revision) &&
    BigInt(value.basis_revision) + 1n === BigInt(value.accepted_revision) &&
    value.supersedes_resolution_id !== value.resolution_id,
  "Inconsistent assessment revision or predecessor.",
);
const resolutionSummary = s.object({
  reference: resolutionReference,
  first_conflicting_entry_sequence: s.nullable(positive),
});
const provenance = s.variant({
  verified_claim: s.object({
    kind: s.enumeration("verified_claim"),
    claim_id: id,
  }),
  legacy_attempt: s.object({
    kind: s.enumeration("legacy_attempt"),
    attempt_id: id,
  }),
  legacy_fence: s.object({ kind: s.enumeration("legacy_fence") }),
});
const summaryShape = {
  occurrence: effectOccurrence,
  program,
  callable: s.id,
  action_version: id,
  dispatch_provenance: provenance,
  journal_state: s.enumeration("unconfirmed", "confirmed"),
  has_discrepancy: s.boolean,
  entry_count: s.decimal,
  effect_revision: positive,
  created_at: s.timestamp,
  updated_at: s.timestamp,
};
function decodeSummary(value: unknown) {
  const decoded =
    s.isJsonObject(value) && Object.hasOwn(value, "operator_resolution")
      ? s.object({ ...summaryShape, operator_resolution: resolutionSummary })(
          value,
        )
      : { ...s.object(summaryShape)(value), operator_resolution: undefined };
  const resolution = decoded.operator_resolution;
  if (
    BigInt(decoded.entry_count) >= BigInt(decoded.effect_revision) ||
    decoded.created_at > decoded.updated_at ||
    (resolution &&
      (BigInt(resolution.reference.accepted_revision) >
        BigInt(decoded.effect_revision) ||
        BigInt(resolution.reference.sequence) > BigInt(decoded.entry_count) ||
        (resolution.first_conflicting_entry_sequence !== null &&
          (resolution.reference.assessment_kind !== "record_success" ||
            BigInt(resolution.first_conflicting_entry_sequence) <=
              BigInt(resolution.reference.sequence) ||
            BigInt(resolution.first_conflicting_entry_sequence) >
              BigInt(decoded.entry_count)))))
  )
    throw new ContractError(
      "Effect summary disagrees with its evidence revision.",
    );
  return decoded;
}
export type EffectSummary = ReturnType<typeof decodeSummary>;
const commonEntry = {
  sequence: positive,
  recorded_at: s.timestamp,
  recorded_revision: positive,
};
const assessmentEntry = s.refine(
  s.object({
    ...commonEntry,
    kind: s.enumeration("assessment"),
    resolution_id: id,
    basis_revision: positive,
    supersedes_resolution_id: s.nullable(id),
    assessment: effectAssessment,
    reason,
    evidence_references: s.array(reference, 8),
    actor,
    acceptance_event_id: id,
  }),
  (value) =>
    BigInt(value.sequence) < BigInt(value.recorded_revision) &&
    BigInt(value.recorded_revision) === BigInt(value.basis_revision) + 1n &&
    value.supersedes_resolution_id !== value.resolution_id &&
    (value.assessment.kind === "close_unknown" ||
      value.evidence_references.length > 0) &&
    byteLength(value) <= 256 * 1024,
  "Invalid retained assessment.",
);
const observationSource = s.variant({
  verified_worker_claim: s.object({
    kind: s.enumeration("verified_worker_claim"),
    claim_id: id,
    worker,
    reported_callback: s.enumeration("dispatch", "lookup"),
    ingress: s.enumeration("live_confirm", "late_observe"),
  }),
  legacy_journal: s.object({
    kind: s.enumeration("legacy_journal"),
    attempt_id: id,
    journal_accepted_at: s.timestamp,
  }),
});
const entry = s.variant({
  assessment: assessmentEntry,
  claim: s.object({
    ...commonEntry,
    kind: s.enumeration("claim"),
    claim_id: id,
    attempt_id: id,
    worker,
    initial_permission: s.enumeration("dispatch", "lookup_only"),
  }),
  observation: s.object({
    ...commonEntry,
    kind: s.enumeration("observation"),
    observation_id: id,
    source: observationSource,
    output: result,
  }),
});
export type EffectEntry = s.Decoded<typeof entry>;
const binding = s.object({
  occurrence: effectOccurrence,
  approval,
  program: descriptor,
  key: id,
  lookup: s.nullable(s.id),
  approval_acceptance_event_id: s.nullable(id),
});
const cursor = s.refine(
  s.string,
  (value) => /^(?:[0-9a-f]{2})+$/.test(value) && value.length <= 8192,
  "Invalid effect cursor.",
);
function cursorValue(value: string): unknown {
  cursor(value);
  const bytes = Uint8Array.from(value.match(/../g) ?? [], (pair) =>
    parseInt(pair, 16),
  );
  return parseUserJson(
    new TextDecoder("utf-8", { fatal: true }).decode(bytes),
    4096,
  );
}
function cursorInteger(value: unknown): string {
  return s.decimal(stringifyUserJson(value));
}
export const effectFilters = [
  "all",
  "unconfirmed",
  "confirmed",
  "discrepant",
  "disputed",
] as const;
export type EffectFilter = (typeof effectFilters)[number];
export type EffectInboxQuery = {
  filter: EffectFilter;
  workflow_id: string | null;
  cursor: string | null;
  limit: number;
};
function addressMatches(value: EffectOccurrence, expected: EffectAddress) {
  return (
    value.workflow_id === expected.workflow_id &&
    value.activation_id === expected.activation_id &&
    value.approval_key === expected.approval_key
  );
}
function matchesScope(
  value: EffectOccurrence,
  expected?: EffectOccurrence["scope"],
) {
  return !expected || sameAction(value.scope, expected);
}
function utf8Compare(left: string, right: string) {
  const a = new TextEncoder().encode(left),
    b = new TextEncoder().encode(right);
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    const difference = a[i]! - b[i]!;
    if (difference) return difference;
  }
  return a.length - b.length;
}
const inboxPosition = s.object({
  updated_at: s.timestamp,
  workflow_id: id,
  activation_id: id,
  approval_key: id,
});
function compareInboxPosition(
  left: s.Decoded<typeof inboxPosition>,
  right: s.Decoded<typeof inboxPosition>,
) {
  return (
    left.updated_at - right.updated_at ||
    utf8Compare(left.workflow_id, right.workflow_id) ||
    utf8Compare(left.activation_id, right.activation_id) ||
    utf8Compare(left.approval_key, right.approval_key)
  );
}
function decodeInboxCursor(
  value: string,
  query: EffectInboxQuery,
  expectedScope?: EffectOccurrence["scope"],
) {
  const decoded = s.object({
    version: s.integer(1, 1),
    scope,
    filter: s.enumeration(...effectFilters),
    workflow_id: s.nullable(id),
    position: inboxPosition,
  })(cursorValue(value));
  if (
    decoded.filter !== query.filter ||
    decoded.workflow_id !== query.workflow_id ||
    (expectedScope && !sameAction(decoded.scope, expectedScope)) ||
    (query.workflow_id !== null &&
      decoded.position.workflow_id !== query.workflow_id)
  )
    throw new ContractError("Effect inbox cursor belongs to another query.");
  return decoded.position;
}
export function effectInboxPage(
  query: EffectInboxQuery,
  expectedScope?: EffectOccurrence["scope"],
) {
  return (raw: unknown) => {
    const page = s.object({
      items: s.array(decodeSummary, query.limit),
      next_cursor: s.nullable(cursor),
    })(raw);
    const pageScope = expectedScope ?? page.items[0]?.occurrence.scope;
    let prior =
      query.cursor === null
        ? null
        : decodeInboxCursor(query.cursor, query, pageScope);
    const ids = new Set<string>();
    for (const item of page.items) {
      const identity = stringifyUserJson(item.occurrence);
      const position = { updated_at: item.updated_at, ...item.occurrence };
      const matchesFilter =
        query.filter === "all" ||
        query.filter === item.journal_state ||
        (query.filter === "discrepant" && item.has_discrepancy) ||
        (query.filter === "disputed" &&
          item.operator_resolution?.first_conflicting_entry_sequence != null);
      if (
        !matchesScope(item.occurrence, pageScope) ||
        ids.has(identity) ||
        !matchesFilter ||
        (query.workflow_id !== null &&
          item.occurrence.workflow_id !== query.workflow_id) ||
        (prior !== null && compareInboxPosition(prior, position) <= 0)
      )
        throw new ContractError(
          "Effect page does not match this scope, filter or order.",
        );
      ids.add(identity);
      prior = position;
    }
    if (
      page.next_cursor !== null &&
      (page.items.length === 0 || page.next_cursor === query.cursor)
    )
      throw new ContractError("Effect pagination did not advance.");
    if (
      page.next_cursor !== null &&
      prior !== null &&
      compareInboxPosition(
        decodeInboxCursor(page.next_cursor, query, pageScope),
        prior,
      ) !== 0
    )
      throw new ContractError("Effect inbox cursor omitted entries.");
    return page;
  };
}
export type EffectInspectQuery = EffectAddress & {
  cursor: string | null;
  limit: number;
};
export function effectDetail(
  query: EffectInspectQuery,
  expectedScope?: EffectOccurrence["scope"],
) {
  return (raw: unknown) => {
    const shape = {
      binding,
      summary: decodeSummary,
      entries: s.array(entry, query.limit),
      next_cursor: s.nullable(cursor),
    };
    const detail =
      s.isJsonObject(raw) && Object.hasOwn(raw, "current_assessment")
        ? s.object({ ...shape, current_assessment: assessmentEntry })(raw)
        : { ...s.object(shape)(raw), current_assessment: undefined };
    const { binding: bound, summary } = detail;
    if (
      !addressMatches(bound.occurrence, query) ||
      !matchesScope(bound.occurrence, expectedScope) ||
      !sameAction(bound.occurrence, summary.occurrence) ||
      !sameAction(bound.program.program, summary.program) ||
      bound.approval.workflow_id !== query.workflow_id ||
      bound.approval.key !== query.approval_key ||
      bound.approval.resumed_activation_id !== query.activation_id ||
      bound.approval.status !== "approved" ||
      bound.approval.action.name !== summary.callable ||
      bound.approval.action.version !== summary.action_version
    )
      throw new ContractError(
        "Effect evidence belongs to another occurrence or action.",
      );
    let sequence = 0n,
      revision = 0n;
    if (query.cursor !== null) {
      const position = s.object({
        version: s.integer(1, 1),
        occurrence: effectOccurrence,
        after_sequence: cursorInteger,
      })(cursorValue(query.cursor));
      if (!sameAction(position.occurrence, bound.occurrence))
        throw new ContractError("Effect cursor belongs to another occurrence.");
      sequence = BigInt(position.after_sequence);
    }
    const identities = new Set<string>();
    for (const evidence of detail.entries) {
      const identity = `${evidence.kind}:${evidence.kind === "claim" ? evidence.claim_id : evidence.kind === "observation" ? evidence.observation_id : evidence.resolution_id}`;
      if (identities.has(identity))
        throw new ContractError("Duplicate retained evidence identity.");
      identities.add(identity);
      if (
        BigInt(evidence.sequence) !== sequence + 1n ||
        BigInt(evidence.sequence) >= BigInt(evidence.recorded_revision) ||
        BigInt(evidence.recorded_revision) <= revision ||
        BigInt(evidence.recorded_revision) > BigInt(summary.effect_revision)
      )
        throw new ContractError(
          "Effect evidence contains a gap or invalid revision.",
        );
      const source =
        evidence.kind === "claim"
          ? evidence.worker
          : evidence.kind === "observation" &&
              evidence.source.kind === "verified_worker_claim"
            ? evidence.source.worker
            : null;
      if (source && !sameAction(source.audience.scope, bound.occurrence.scope))
        throw new ContractError("Effect report belongs to another scope.");
      sequence = BigInt(evidence.sequence);
      revision = BigInt(evidence.recorded_revision);
    }
    if (
      sequence > BigInt(summary.entry_count) ||
      (detail.next_cursor !== null) !== sequence < BigInt(summary.entry_count)
    )
      throw new ContractError("Effect evidence pagination omitted an entry.");
    if (detail.next_cursor !== null) {
      const next = s.object({
        version: s.integer(1, 1),
        occurrence: effectOccurrence,
        after_sequence: cursorInteger,
      })(cursorValue(detail.next_cursor));
      if (
        !detail.entries.length ||
        detail.next_cursor === query.cursor ||
        BigInt(next.after_sequence) !== sequence ||
        !sameAction(next.occurrence, bound.occurrence)
      )
        throw new ContractError(
          "Effect evidence cursor did not advance correctly.",
        );
    }
    const current = detail.current_assessment,
      selected = summary.operator_resolution?.reference;
    if (
      Boolean(current) !== Boolean(selected) ||
      (current &&
        selected &&
        (current.resolution_id !== selected.resolution_id ||
          current.sequence !== selected.sequence ||
          current.recorded_revision !== selected.accepted_revision ||
          current.basis_revision !== selected.basis_revision ||
          current.recorded_at !== selected.accepted_at ||
          current.supersedes_resolution_id !==
            selected.supersedes_resolution_id ||
          current.assessment.kind !== selected.assessment_kind ||
          current.acceptance_event_id !== selected.acceptance_event_id))
    )
      throw new ContractError(
        "Current assessment and retained summary disagree.",
      );
    for (const item of detail.entries) {
      if (
        current &&
        item.sequence === current.sequence &&
        !sameAction(item, current)
      )
        throw new ContractError(
          "The assessment header differs from its immutable evidence entry.",
        );
      if (item.kind !== "assessment") continue;
      if (!current || BigInt(item.sequence) > BigInt(current.sequence))
        throw new ContractError(
          "The assessment timeline disagrees with its current header.",
        );
    }
    return detail;
  };
}
export type EffectDetail = ReturnType<ReturnType<typeof effectDetail>>;
export const decodeEffectResolutionCommand = s.refine(
  s.object({
    occurrence: s.object({
      workflow_id: id,
      activation_id: id,
      approval_key: id,
    }),
    resolution_id: id,
    expected_effect_revision: positive,
    supersedes_resolution_id: s.nullable(id),
    action: approvalAction,
    assessment: effectAssessment,
    reason,
    evidence_references: s.array(reference, 8),
  }),
  (value) =>
    BigInt(value.expected_effect_revision) < 18446744073709551615n &&
    value.resolution_id !== value.supersedes_resolution_id &&
    (value.assessment.kind === "close_unknown" ||
      value.evidence_references.length > 0) &&
    byteLength(value) <= 256 * 1024,
  "Invalid frozen effect assessment.",
);
export type EffectResolutionCommand = s.Decoded<
  typeof decodeEffectResolutionCommand
>;
const receipt = s.object({
  occurrence: effectOccurrence,
  resolution_id: id,
  accepted_at: s.timestamp,
  accepted_revision: positive,
  basis_revision: positive,
  supersedes_resolution_id: s.nullable(id),
  assessment_kind: assessmentKind,
  acceptance_event_id: id,
  already_accepted: s.boolean,
});
export type EffectResolutionReceipt = s.Decoded<typeof receipt>;
export function effectResolutionReceipt(
  command: EffectResolutionCommand,
  expectedScope: EffectOccurrence["scope"],
) {
  return (raw: unknown) => {
    const accepted = receipt(raw);
    if (
      !addressMatches(accepted.occurrence, command.occurrence) ||
      !matchesScope(accepted.occurrence, expectedScope) ||
      accepted.resolution_id !== command.resolution_id ||
      accepted.basis_revision !== command.expected_effect_revision ||
      BigInt(accepted.accepted_revision) !==
        BigInt(accepted.basis_revision) + 1n ||
      accepted.supersedes_resolution_id !== command.supersedes_resolution_id ||
      accepted.assessment_kind !== command.assessment.kind
    )
      throw new ContractError(
        "Receipt does not match this exact frozen assessment.",
      );
    return accepted;
  };
}

export function effectDetailPath(address: EffectAddress): string {
  const { workflow_id, activation_id, approval_key } = address;
  return `/effects/review?${new URLSearchParams({ workflow_id, activation_id, approval_key })}`;
}

// The Console alias projects scope from the credential and u64 metadata as text.
// Export the same frozen operation in the generic API wire format for CLI/SDK retry.
export function saveEffectResolutionCommand(
  body: string,
  expectedScope: EffectOccurrence["scope"],
  baseUrl: string,
): string {
  const destination = new URL(baseUrl);
  if (
    !["http:", "https:"].includes(destination.protocol) ||
    destination.origin !== baseUrl
  )
    throw new ContractError("A canonical Console server origin is required.");
  const command = decodeEffectResolutionCommand(
    parseUserJson(body, 256 * 1024),
  );
  const genericBody = stringifyUserJson({
    ...command,
    occurrence: { scope: scope(expectedScope), ...command.occurrence },
    expected_effect_revision: parseUserJson(
      command.expected_effect_revision,
      20,
    ),
  });
  if (new TextEncoder().encode(genericBody).length > 256 * 1024)
    throw new ContractError("The saved effect command exceeds 256 KiB.");
  const envelope = stringifyUserJson({
    format: "ledgence.effect-resolution.v1",
    base_url: baseUrl,
    body: genericBody,
  });
  if (new TextEncoder().encode(envelope).length > 1024 * 1024)
    throw new ContractError("The saved command envelope exceeds 1 MiB.");
  return envelope;
}
