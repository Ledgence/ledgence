"""Typed retained effect evidence with immutable application values (MIT)."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
import hashlib
import json
import re
from typing import Literal

from . import codec
from .approval_models import ApprovalStatus, WorkflowApproval, parse_approval, _json_identity
from .errors import InputError, ProtocolError
from .effect_resolution_models import (
    EffectAssessmentEntry, EffectResolutionSummary, parse_assessment_entry, parse_resolution_summary,
)
from .models import Scope, _scope

EFFECT_PAGE_LIMIT = 1024 * 1024
EFFECT_OUTPUT_LIMIT = 128 * 1024
EFFECT_CURSOR_LIMIT = 8192
EFFECT_TIMESTAMP_LIMIT = codec.MAX_TIMESTAMP


class EffectFilter(StrEnum):
    ALL = "all"
    UNCONFIRMED = "unconfirmed"
    CONFIRMED = "confirmed"
    DISCREPANT = "discrepant"
    DISPUTED = "disputed"


class EffectJournalState(StrEnum):
    UNCONFIRMED = "unconfirmed"
    CONFIRMED = "confirmed"


class EffectPermission(StrEnum):
    DISPATCH = "dispatch"
    LOOKUP_ONLY = "lookup_only"


class EffectCallback(StrEnum):
    DISPATCH = "dispatch"
    LOOKUP = "lookup"


class EffectObservationIngress(StrEnum):
    LIVE_CONFIRM = "live_confirm"
    LATE_OBSERVE = "late_observe"


@dataclass(frozen=True, slots=True)
class EffectOccurrence:
    scope: Scope
    workflow_id: str
    activation_id: str
    approval_key: str

    def to_dict(self):
        return {"scope": {"tenant_id": self.scope.tenant_id, "namespace": self.scope.namespace},
                "workflow_id": self.workflow_id, "activation_id": self.activation_id,
                "approval_key": self.approval_key}


@dataclass(frozen=True, slots=True)
class EffectAudience:
    authority_id: str
    instance_id: str
    scope: Scope


@dataclass(frozen=True, slots=True)
class EffectWorkerIdentity:
    audience: EffectAudience
    issuer: str
    subject: str
    credential_id: str


@dataclass(frozen=True, slots=True)
class EffectProgram:
    id: str
    version: str


@dataclass(frozen=True, slots=True)
class EffectProgramDescriptor:
    program: EffectProgram
    digest: str
    size: int


@dataclass(frozen=True, slots=True)
class EffectDispatchClaim:
    claim_id: str
    kind: Literal["verified_claim"] = field(default="verified_claim", init=False)


@dataclass(frozen=True, slots=True)
class EffectLegacyAttempt:
    attempt_id: str
    kind: Literal["legacy_attempt"] = field(default="legacy_attempt", init=False)


@dataclass(frozen=True, slots=True)
class EffectLegacyFence:
    kind: Literal["legacy_fence"] = field(default="legacy_fence", init=False)


EffectDispatchProvenance = EffectDispatchClaim | EffectLegacyAttempt | EffectLegacyFence


@dataclass(frozen=True, slots=True)
class EffectEvidenceSummary:
    occurrence: EffectOccurrence
    program: EffectProgram
    callable: str
    action_version: str
    dispatch_provenance: EffectDispatchProvenance
    journal_state: EffectJournalState
    has_discrepancy: bool
    entry_count: int
    effect_revision: int
    created_at: int
    updated_at: int
    operator_resolution: EffectResolutionSummary | None = None


@dataclass(frozen=True, slots=True)
class EffectEvidenceBinding:
    occurrence: EffectOccurrence
    approval: WorkflowApproval
    program: EffectProgramDescriptor
    key: str
    lookup: str | None
    approval_acceptance_event_id: str | None


@dataclass(frozen=True, slots=True)
class EffectVerifiedWorkerClaim:
    claim_id: str
    worker: EffectWorkerIdentity
    reported_callback: EffectCallback
    ingress: EffectObservationIngress
    kind: Literal["verified_worker_claim"] = field(default="verified_worker_claim", init=False)


@dataclass(frozen=True, slots=True)
class EffectLegacyJournal:
    attempt_id: str
    journal_accepted_at: int
    kind: Literal["legacy_journal"] = field(default="legacy_journal", init=False)


EffectObservationSource = EffectVerifiedWorkerClaim | EffectLegacyJournal


@dataclass(frozen=True, slots=True)
class EffectClaimEntry:
    sequence: int
    recorded_at: int
    recorded_revision: int
    claim_id: str
    attempt_id: str
    worker: EffectWorkerIdentity
    initial_permission: EffectPermission
    kind: Literal["claim"] = field(default="claim", init=False)


@dataclass(frozen=True, slots=True)
class EffectObservationEntry:
    sequence: int
    recorded_at: int
    recorded_revision: int
    observation_id: str
    source: EffectObservationSource
    _output: bytes = field(repr=False)
    kind: Literal["observation"] = field(default="observation", init=False)

    @property
    def output(self):
        """An independent copy of the worker-reported value."""
        return json.loads(self._output)


EffectEvidenceEntry = EffectClaimEntry | EffectObservationEntry | EffectAssessmentEntry


@dataclass(frozen=True, slots=True)
class EffectInboxPage:
    items: tuple[EffectEvidenceSummary, ...]
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class EffectEvidenceDetail:
    binding: EffectEvidenceBinding
    summary: EffectEvidenceSummary
    entries: tuple[EffectEvidenceEntry, ...]
    next_cursor: str | None
    current_assessment: EffectAssessmentEntry | None = None
    _base_url: str = field(default="", repr=False, compare=False)


def effect_cursor(value):
    if value is not None:
        codec.text(value, "cursor", EFFECT_CURSOR_LIMIT)
        if not re.fullmatch(r"(?:[0-9a-f]{2})+", value):
            raise InputError("invalid effect cursor")
    return value


def _occurrence(raw, scope):
    codec.fields(raw, {"scope", "workflow_id", "activation_id", "approval_key"})
    actual_scope = _scope(raw["scope"])
    if actual_scope != scope:
        raise InputError("effect belongs to another scope")
    return EffectOccurrence(actual_scope, *(codec.text(raw[name], name) for name in
                                           ("workflow_id", "activation_id", "approval_key")))


def _program(raw):
    codec.fields(raw, {"id", "version"})
    return EffectProgram(*(codec.program_component(raw[name], name) for name in ("id", "version")))


def _worker(raw, scope):
    codec.fields(raw, {"audience", "issuer", "subject", "credential_id"})
    audience = raw["audience"]
    codec.fields(audience, {"authority_id", "instance_id", "scope"})
    if _scope(audience["scope"]) != scope:
        raise InputError("effect worker belongs to another scope")
    audience = EffectAudience(codec.text(audience["authority_id"], "authority_id"),
                              codec.text(audience["instance_id"], "instance_id"), scope)
    return EffectWorkerIdentity(audience, codec.text(raw["issuer"], "issuer", 256),
                                codec.text(raw["subject"], "subject"),
                                codec.text(raw["credential_id"], "credential_id"))


def _dispatch_provenance(raw):
    kind = raw.get("kind") if type(raw) is dict else None
    if kind == "verified_claim":
        codec.fields(raw, {"kind", "claim_id"})
        return EffectDispatchClaim(codec.text(raw["claim_id"], "claim_id"))
    if kind == "legacy_attempt":
        codec.fields(raw, {"kind", "attempt_id"})
        return EffectLegacyAttempt(codec.text(raw["attempt_id"], "attempt_id"))
    if kind == "legacy_fence":
        codec.fields(raw, {"kind"})
        return EffectLegacyFence()
    raise InputError("invalid effect dispatch provenance")


def _summary(raw, scope):
    codec.fields(raw, {"occurrence", "program", "callable", "action_version", "dispatch_provenance",
                       "journal_state", "has_discrepancy", "entry_count", "effect_revision", "created_at", "updated_at"}, {"operator_resolution"})
    if type(raw["has_discrepancy"]) is not bool:
        raise InputError("invalid effect discrepancy flag")
    created = codec.integer(raw["created_at"], "created_at", 0, EFFECT_TIMESTAMP_LIMIT)
    updated = codec.integer(raw["updated_at"], "updated_at", created, EFFECT_TIMESTAMP_LIMIT)
    revision = codec.integer(raw["effect_revision"], "effect_revision", 1)
    count = codec.integer(raw["entry_count"], "entry_count", 0, revision - 1)
    resolution = parse_resolution_summary(raw["operator_resolution"], count, revision) if "operator_resolution" in raw else None
    return EffectEvidenceSummary(_occurrence(raw["occurrence"], scope), _program(raw["program"]),
        codec.text(raw["callable"], "callable", 512), codec.text(raw["action_version"], "action_version"),
        _dispatch_provenance(raw["dispatch_provenance"]), EffectJournalState(raw["journal_state"]),
        raw["has_discrepancy"], codec.integer(raw["entry_count"], "entry_count", 0,
            codec.integer(raw["effect_revision"], "effect_revision", 1) - 1),
        raw["effect_revision"], created, updated, resolution)


def _binding(raw, occurrence, base_url):
    codec.fields(raw, {"occurrence", "approval", "program", "key", "lookup", "approval_acceptance_event_id"})
    if _occurrence(raw["occurrence"], occurrence.scope) != occurrence:
        raise InputError("effect binding does not match the request")
    approval = parse_approval(raw["approval"], occurrence.scope, occurrence.workflow_id,
                              occurrence.approval_key, base_url)
    if approval.status != ApprovalStatus.APPROVED or approval.resumed_activation_id != occurrence.activation_id:
        raise InputError("effect binding does not match its approved activation")
    descriptor = raw["program"]
    codec.fields(descriptor, {"program", "digest", "size"})
    digest = descriptor["digest"]
    if type(digest) is not str or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise InputError("invalid effect program digest")
    program = EffectProgramDescriptor(_program(descriptor["program"]), digest,
                                      codec.integer(descriptor["size"], "size", 1))
    key = "_approval:" + hashlib.sha256(occurrence.approval_key.encode()).hexdigest()
    if raw["key"] != key:
        raise InputError("effect journal key does not match its approval")
    lookup = raw["lookup"]
    if lookup is not None:
        codec.text(lookup, "lookup", 512)
        if lookup == approval.action.name:
            raise InputError("effect lookup must differ from dispatch")
    event_id = raw["approval_acceptance_event_id"]
    if event_id is not None:
        codec.text(event_id, "approval_acceptance_event_id")
    return EffectEvidenceBinding(occurrence, approval, program, key, lookup, event_id)


def _source(raw, scope):
    kind = raw.get("kind") if type(raw) is dict else None
    if kind == "verified_worker_claim":
        codec.fields(raw, {"kind", "claim_id", "worker", "reported_callback", "ingress"})
        return EffectVerifiedWorkerClaim(codec.text(raw["claim_id"], "claim_id"), _worker(raw["worker"], scope),
            EffectCallback(raw["reported_callback"]), EffectObservationIngress(raw["ingress"]))
    if kind == "legacy_journal":
        codec.fields(raw, {"kind", "attempt_id", "journal_accepted_at"})
        return EffectLegacyJournal(codec.text(raw["attempt_id"], "attempt_id"),
            codec.integer(raw["journal_accepted_at"], "journal_accepted_at", 0, EFFECT_TIMESTAMP_LIMIT))
    raise InputError("invalid effect observation source")


def _entry(raw, summary):
    if type(raw) is dict and raw.get("kind") == "assessment":
        return parse_assessment_entry(raw, summary)
    fields = {"sequence", "recorded_at", "recorded_revision", "kind"}
    kind = raw.get("kind") if type(raw) is dict else None
    if kind == "claim":
        codec.fields(raw, fields | {"claim_id", "attempt_id", "worker", "initial_permission"})
    elif kind == "observation":
        codec.fields(raw, fields | {"observation_id", "source", "output"})
    else:
        raise InputError("invalid effect entry kind")
    sequence = codec.integer(raw["sequence"], "sequence", 1, summary.entry_count)
    at = codec.integer(raw["recorded_at"], "recorded_at", 0, EFFECT_TIMESTAMP_LIMIT)
    revision = codec.integer(raw["recorded_revision"], "recorded_revision", 1, summary.effect_revision)
    if sequence >= revision:
        raise InputError("effect sequence exceeds its recorded revision")
    if kind == "claim":
        return EffectClaimEntry(sequence, at, revision, codec.text(raw["claim_id"], "claim_id"),
            codec.text(raw["attempt_id"], "attempt_id"), _worker(raw["worker"], summary.occurrence.scope),
            EffectPermission(raw["initial_permission"]))
    codec.validate_authoritative(raw["output"], EFFECT_OUTPUT_LIMIT)
    encoded = json.dumps(raw["output"], ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
    return EffectObservationEntry(sequence, at, revision, codec.text(raw["observation_id"], "observation_id"),
                                  _source(raw["source"], summary.occurrence.scope), encoded)


def _cursor_value(value):
    effect_cursor(value)
    try:
        return codec.decode(bytes.fromhex(value), EFFECT_CURSOR_LIMIT // 2)
    except ProtocolError as exc:
        raise InputError("invalid effect cursor") from exc


def inbox_position(cursor, scope, selected, workflow_id):
    """Validate cursor binding; callers still treat the token as opaque."""
    if cursor is None:
        return None
    raw = _cursor_value(cursor)
    codec.fields(raw, {"version", "scope", "filter", "workflow_id", "position"})
    if (type(raw["version"]) is not int or raw["version"] != 1 or _scope(raw["scope"]) != scope
            or raw["filter"] != selected or raw["workflow_id"] != workflow_id):
        raise InputError("effect cursor does not match the query")
    pos = raw["position"]
    codec.fields(pos, {"updated_at", "workflow_id", "activation_id", "approval_key"})
    ids = tuple(codec.text(pos[name], name) for name in ("workflow_id", "activation_id", "approval_key"))
    if workflow_id is not None and ids[0] != workflow_id:
        raise InputError("effect cursor does not match its workflow")
    return (codec.integer(pos["updated_at"], "updated_at", 0, EFFECT_TIMESTAMP_LIMIT),
            *(value.encode() for value in ids))


def inspect_sequence(cursor, occurrence):
    if cursor is None:
        return 0
    raw = _cursor_value(cursor)
    codec.fields(raw, {"version", "occurrence", "after_sequence"})
    if (type(raw["version"]) is not int or raw["version"] != 1
            or _occurrence(raw["occurrence"], occurrence.scope) != occurrence):
        raise InputError("effect cursor does not match the occurrence")
    return codec.integer(raw["after_sequence"], "after_sequence", 1)


def parse_effect_inbox(raw, scope, selected, workflow_id, cursor, limit):
    try:
        codec.fields(raw, {"items", "next_cursor"})
        if type(raw["items"]) is not list or len(raw["items"]) > limit:
            raise InputError("invalid effect inbox page size")
        items = tuple(_summary(item, scope) for item in raw["items"])
        seen = set()
        previous = inbox_position(cursor, scope, selected, workflow_id)
        for item in items:
            identity = item.occurrence
            position = (item.updated_at, *(value.encode() for value in
                        (identity.workflow_id, identity.activation_id, identity.approval_key)))
            if identity in seen or (previous is not None and position >= previous):
                raise InputError("effect inbox is not strictly ordered")
            if (workflow_id is not None and identity.workflow_id != workflow_id
                    or selected == EffectFilter.UNCONFIRMED and item.journal_state != EffectJournalState.UNCONFIRMED
                    or selected == EffectFilter.CONFIRMED and item.journal_state != EffectJournalState.CONFIRMED
                    or selected == EffectFilter.DISCREPANT and not item.has_discrepancy
                    or selected == EffectFilter.DISPUTED and (item.operator_resolution is None
                        or item.operator_resolution.first_conflicting_entry_sequence is None)):
                raise InputError("effect inbox item does not match its filters")
            seen.add(identity)
            previous = position
        next_cursor = raw["next_cursor"]
        if next_cursor is not None and (not items or next_cursor == cursor
                or inbox_position(next_cursor, scope, selected, workflow_id) != previous):
            raise InputError("effect cursor does not advance to the final item")
        return EffectInboxPage(items, next_cursor)
    except (ValueError, TypeError, KeyError, UnicodeError) as exc:
        raise ProtocolError("invalid effect inbox response") from exc


def parse_effect_detail(raw, occurrence, cursor, limit, base_url):
    try:
        codec.fields(raw, {"binding", "summary", "entries", "next_cursor"}, {"current_assessment"})
        summary = _summary(raw["summary"], occurrence.scope)
        if summary.occurrence != occurrence:
            raise InputError("effect summary does not match the request")
        binding = _binding(raw["binding"], occurrence, base_url)
        if (binding.program.program != summary.program or binding.approval.action.name != summary.callable
                or binding.approval.action.version != summary.action_version):
            raise InputError("effect summary and binding disagree")
        if type(raw["entries"]) is not list or len(raw["entries"]) > limit:
            raise InputError("invalid effect detail page size")
        entries = tuple(_entry(entry, summary) for entry in raw["entries"])
        after = inspect_sequence(cursor, occurrence)
        if after > summary.entry_count:
            raise InputError("effect cursor exceeds retained evidence")
        command_ids = set()
        sequence, revision = after, 0
        for entry in entries:
            command_id = (entry.kind, entry.claim_id if isinstance(entry, EffectClaimEntry) else
                           entry.resolution_id if isinstance(entry, EffectAssessmentEntry) else entry.observation_id)
            if (entry.sequence != sequence + 1 or entry.recorded_revision <= revision
                    or command_id in command_ids):
                raise InputError("effect entries omit, reorder or duplicate retained evidence")
            command_ids.add(command_id)
            sequence, revision = entry.sequence, entry.recorded_revision
        next_cursor = raw["next_cursor"]
        remaining = sequence < summary.entry_count
        if remaining != (next_cursor is not None) or (remaining and not entries):
            raise InputError("effect detail does not expose its remaining entries")
        if next_cursor is not None and inspect_sequence(next_cursor, occurrence) != sequence:
            raise InputError("effect cursor does not advance to the final entry")
        assessment = parse_assessment_entry(raw["current_assessment"], summary) if "current_assessment" in raw else None
        if ((assessment is None) != (summary.operator_resolution is None)
                or assessment is not None and assessment.reference != summary.operator_resolution.reference):
            raise InputError("current assessment and summary disagree")
        if assessment is None and any(isinstance(entry, EffectAssessmentEntry) for entry in entries):
            raise InputError("assessment timeline has no current assessment header")
        if assessment is not None:
            for original, entry in zip(raw["entries"], entries):
                if entry.sequence == assessment.sequence and _json_identity(original) != _json_identity(raw["current_assessment"]):
                    raise InputError("current assessment and timeline disagree")
                if isinstance(entry, EffectAssessmentEntry) and entry.sequence > assessment.sequence:
                    raise InputError("current assessment is not the latest assessment")
        return EffectEvidenceDetail(binding, summary, entries, next_cursor, assessment, base_url)
    except (ValueError, TypeError, KeyError, UnicodeError) as exc:
        raise ProtocolError("invalid effect detail response") from exc
