"""Immutable, destination-bound operator assessments and acceptance receipts (MIT)."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
import json
from typing import TYPE_CHECKING, Literal

from . import codec
from .approval_models import _action
from .errors import InputError, ProtocolError
from .models import ErrorDetail

if TYPE_CHECKING:
    from .audit import AuditActor
    from .effect_models import EffectOccurrence
    from .models import Scope

EFFECT_RESOLUTION_RESULT_MAX_BYTES = 131072
EFFECT_RESOLUTION_COMMAND_MAX_BYTES = 262144
EFFECT_RESOLUTION_ENTRY_MAX_BYTES = 262144
EFFECT_RESOLUTION_RECEIPT_MAX_BYTES = 16384
EFFECT_RESOLUTION_SAVED_MAX_BYTES = 1048576
_SAVED_FORMAT = "ledgence.effect-resolution.v1"


class EffectAssessmentKind(StrEnum):
    RECORD_SUCCESS = "record_success"
    RECORD_FAILURE = "record_failure"
    CLOSE_UNKNOWN = "close_unknown"


def _utf8_size(value):
    if type(value) is not str:
        raise InputError("expected UTF-8 text")
    try:
        return len(value.encode("utf-8"))
    except UnicodeError as exc:
        raise InputError("expected valid UTF-8 text") from exc


def _failure(raw):
    codec.fields(raw, {"kind", "message"})
    kind = codec.text(raw["kind"], "failure.kind")
    message = raw["message"]
    if type(message) is not str or _utf8_size(message) > 4096:
        raise InputError("failure.message must contain at most 4096 UTF-8 bytes")
    return ErrorDetail(kind, message)


def _reason(value):
    if type(value) is not str or not value.strip() or _utf8_size(value) > 4096:
        raise InputError("resolution reason must be nonblank and at most 4096 UTF-8 bytes")
    return value


def _references(value, kind):
    if type(value) not in (list, tuple) or len(value) > 8:
        raise InputError("resolution requires at most eight ordered evidence references")
    if not value and kind != EffectAssessmentKind.CLOSE_UNKNOWN:
        raise InputError("success and failure assessments require evidence references")
    return tuple(codec.text(item, "evidence reference", 1024) for item in value)


@dataclass(frozen=True, slots=True, init=False)
class EffectResolutionAssessment:
    _encoded: bytes = field(repr=False)

    def __init__(self):
        raise TypeError("use record_success(), record_failure() or close_unknown()")

    @classmethod
    def _create(cls, raw, *, authoritative=False):
        kind = raw.get("kind") if type(raw) is dict else None
        if kind == EffectAssessmentKind.RECORD_SUCCESS:
            codec.fields(raw, {"kind", "result"})
            if authoritative:
                codec.validate_authoritative(raw["result"], EFFECT_RESOLUTION_RESULT_MAX_BYTES)
            else:
                codec.encode(raw["result"], EFFECT_RESOLUTION_RESULT_MAX_BYTES)
        elif kind == EffectAssessmentKind.RECORD_FAILURE:
            codec.fields(raw, {"kind", "failure"})
            _failure(raw["failure"])
        elif kind == EffectAssessmentKind.CLOSE_UNKNOWN:
            codec.fields(raw, {"kind"})
        else:
            raise InputError("invalid effect assessment kind")
        result = object.__new__(cls)
        # Accepted Rust floats may require slightly longer Python JSON tokens.
        # Bound inbound values first; authored commands use strict encode below.
        object.__setattr__(result, "_encoded", json.dumps(raw, ensure_ascii=False, allow_nan=False,
                                                        separators=(",", ":")).encode())
        return result

    @classmethod
    def record_success(cls, result): return cls._create({"kind": "record_success", "result": result})

    @classmethod
    def record_failure(cls, kind, message):
        return cls._create({"kind": "record_failure", "failure": {"kind": kind, "message": message}})

    @classmethod
    def close_unknown(cls): return cls._create({"kind": "close_unknown"})

    def to_dict(self): return json.loads(self._encoded)

    @property
    def kind(self): return EffectAssessmentKind(self.to_dict()["kind"])

    @property
    def result(self):
        if self.kind != EffectAssessmentKind.RECORD_SUCCESS:
            raise InputError("only a success assessment has a result")
        return self.to_dict()["result"]

    @property
    def failure(self):
        if self.kind != EffectAssessmentKind.RECORD_FAILURE:
            raise InputError("only a failure assessment has a failure")
        return _failure(self.to_dict()["failure"])


@dataclass(frozen=True, slots=True)
class EffectResolutionReference:
    resolution_id: str
    sequence: int
    accepted_at: int
    accepted_revision: int
    basis_revision: int
    supersedes_resolution_id: str | None
    assessment_kind: EffectAssessmentKind
    acceptance_event_id: str


@dataclass(frozen=True, slots=True)
class EffectResolutionSummary:
    reference: EffectResolutionReference
    first_conflicting_entry_sequence: int | None


@dataclass(frozen=True, slots=True)
class EffectAssessmentEntry:
    sequence: int
    recorded_at: int
    recorded_revision: int
    resolution_id: str
    basis_revision: int
    supersedes_resolution_id: str | None
    assessment: EffectResolutionAssessment
    reason: str = field(repr=False)
    evidence_references: tuple[str, ...] = field(repr=False)
    actor: AuditActor
    acceptance_event_id: str
    kind: Literal["assessment"] = field(default="assessment", init=False)

    @property
    def reference(self):
        return EffectResolutionReference(self.resolution_id, self.sequence, self.recorded_at,
            self.recorded_revision, self.basis_revision, self.supersedes_resolution_id,
            self.assessment.kind, self.acceptance_event_id)


def _predecessor(raw, resolution_id):
    if raw is not None:
        codec.text(raw, "supersedes_resolution_id")
        if raw == resolution_id:
            raise InputError("a resolution cannot supersede itself")
    return raw


def _revisions(basis, accepted):
    codec.integer(basis, "basis_revision", 1, (1 << 64) - 2)
    codec.integer(accepted, "accepted_revision", 2)
    if accepted != basis + 1:
        raise InputError("resolution revision does not follow its basis")


def parse_resolution_reference(raw):
    codec.fields(raw, set(EffectResolutionReference.__dataclass_fields__))
    identity = codec.text(raw["resolution_id"], "resolution_id")
    _revisions(raw["basis_revision"], raw["accepted_revision"])
    sequence = codec.integer(raw["sequence"], "sequence", 1)
    if sequence >= raw["accepted_revision"]:
        raise InputError("assessment sequence exceeds its accepted revision")
    return EffectResolutionReference(identity, sequence,
        codec.integer(raw["accepted_at"], "accepted_at", 0, codec.MAX_TIMESTAMP),
        raw["accepted_revision"], raw["basis_revision"], _predecessor(raw["supersedes_resolution_id"], identity),
        EffectAssessmentKind(raw["assessment_kind"]), codec.text(raw["acceptance_event_id"], "acceptance_event_id"))


def parse_resolution_summary(raw, entry_count, effect_revision):
    codec.fields(raw, {"reference", "first_conflicting_entry_sequence"})
    reference = parse_resolution_reference(raw["reference"])
    if reference.sequence > entry_count or reference.accepted_revision > effect_revision:
        raise InputError("resolution reference exceeds retained evidence")
    conflict = raw["first_conflicting_entry_sequence"]
    if conflict is not None:
        codec.integer(conflict, "first_conflicting_entry_sequence", reference.sequence + 1, entry_count)
        if reference.assessment_kind != EffectAssessmentKind.RECORD_SUCCESS:
            raise InputError("opaque reports cannot dispute failure or unknown assessments")
    return EffectResolutionSummary(reference, conflict)


def parse_assessment_entry(raw, summary):
    from .audit import AuditActor, AuditActorClass
    codec.fields(raw, {"kind", "sequence", "recorded_at", "recorded_revision", "resolution_id",
        "basis_revision", "supersedes_resolution_id", "assessment", "reason", "evidence_references",
        "actor", "acceptance_event_id"})
    if raw["kind"] != "assessment": raise InputError("expected an assessment entry")
    codec.validate_authoritative(raw, EFFECT_RESOLUTION_ENTRY_MAX_BYTES, max_depth=96)
    identity = codec.text(raw["resolution_id"], "resolution_id")
    _revisions(raw["basis_revision"], raw["recorded_revision"])
    sequence = codec.integer(raw["sequence"], "sequence", 1, summary.entry_count)
    revision = codec.integer(raw["recorded_revision"], "recorded_revision", 1, summary.effect_revision)
    if sequence >= revision:
        raise InputError("assessment sequence exceeds its accepted revision")
    at = codec.integer(raw["recorded_at"], "recorded_at", 0, codec.MAX_TIMESTAMP)
    assessment = EffectResolutionAssessment._create(raw["assessment"], authoritative=True)
    actor = raw["actor"]
    codec.fields(actor, {"issuer", "subject", "class", "credential_id"})
    actor = AuditActor(codec.text(actor["issuer"], "issuer", 256), codec.text(actor["subject"], "subject"),
        AuditActorClass(actor["class"]), codec.text(actor["credential_id"], "credential_id"))
    return EffectAssessmentEntry(sequence, at, revision, identity, raw["basis_revision"],
        _predecessor(raw["supersedes_resolution_id"], identity), assessment, _reason(raw["reason"]),
        _references(raw["evidence_references"], assessment.kind), actor,
        codec.text(raw["acceptance_event_id"], "acceptance_event_id"))


def validate_resolution_command(raw, scope):
    from .effect_models import _occurrence
    codec.fields(raw, {"occurrence", "resolution_id", "expected_effect_revision", "supersedes_resolution_id",
                       "action", "assessment", "reason", "evidence_references"})
    occurrence = _occurrence(raw["occurrence"], scope)
    identity = codec.text(raw["resolution_id"], "resolution_id")
    codec.integer(raw["expected_effect_revision"], "expected_effect_revision", 1, (1 << 64) - 2)
    _predecessor(raw["supersedes_resolution_id"], identity)
    _action(raw["action"])
    assessment = EffectResolutionAssessment._create(raw["assessment"], authoritative=True)
    _reason(raw["reason"])
    _references(raw["evidence_references"], assessment.kind)
    return occurrence


@dataclass(frozen=True, slots=True, init=False)
class EffectResolutionCommand:
    base_url: str
    scope: Scope
    occurrence: EffectOccurrence
    _body: bytes = field(repr=False)

    def __init__(self):
        raise TypeError("create commands with client.effects.prepare_resolution()")

    @classmethod
    def _create(cls, base_url, scope, occurrence, body):
        result = object.__new__(cls)
        for name, value in (("base_url", base_url), ("scope", scope), ("occurrence", occurrence), ("_body", body)):
            object.__setattr__(result, name, value)
        return result

    def to_dict(self): return codec.decode(self._body, EFFECT_RESOLUTION_COMMAND_MAX_BYTES)

    def to_json(self):
        return codec.encode({"format": _SAVED_FORMAT, "base_url": self.base_url,
            "body": self._body.decode("utf-8")}, EFFECT_RESOLUTION_SAVED_MAX_BYTES).decode()


def restore_resolution(saved, base_url, scope):
    from .client import _endpoint
    if type(saved) not in (str, bytes):
        raise InputError("restore requires the saved JSON envelope text or bytes")
    try:
        raw = codec.decode(saved.encode("utf-8") if type(saved) is str else saved, EFFECT_RESOLUTION_SAVED_MAX_BYTES)
        codec.fields(raw, {"format", "base_url", "body"})
        if (raw["format"] != _SAVED_FORMAT or _endpoint(raw["base_url"]) != raw["base_url"]
                or raw["base_url"] != base_url or type(raw["body"]) is not str):
            raise InputError("saved resolution has a different destination or format")
        body = raw["body"].encode("utf-8")
        command = codec.decode(body, EFFECT_RESOLUTION_COMMAND_MAX_BYTES)
        occurrence = validate_resolution_command(command, scope)
        return EffectResolutionCommand._create(base_url, scope, occurrence, body)
    except (ProtocolError, ValueError, TypeError, KeyError, UnicodeError) as exc:
        raise InputError("invalid destination-bound saved effect resolution") from exc


@dataclass(frozen=True, slots=True)
class EffectResolutionReceipt:
    occurrence: EffectOccurrence
    resolution_id: str
    accepted_at: int
    accepted_revision: int
    basis_revision: int
    supersedes_resolution_id: str | None
    assessment_kind: EffectAssessmentKind
    acceptance_event_id: str
    already_accepted: bool


def parse_resolution_receipt(raw, command):
    from .effect_models import _occurrence
    try:
        codec.fields(raw, set(EffectResolutionReceipt.__dataclass_fields__))
        expected = command.to_dict()
        occurrence = _occurrence(raw["occurrence"], command.scope)
        _revisions(raw["basis_revision"], raw["accepted_revision"])
        if (occurrence != command.occurrence or raw["resolution_id"] != expected["resolution_id"]
                or raw["basis_revision"] != expected["expected_effect_revision"]
                or raw["supersedes_resolution_id"] != expected["supersedes_resolution_id"]
                or raw["assessment_kind"] != expected["assessment"]["kind"]
                or type(raw["already_accepted"]) is not bool):
            raise InputError("resolution receipt does not match the frozen command")
        return EffectResolutionReceipt(occurrence, raw["resolution_id"],
            codec.integer(raw["accepted_at"], "accepted_at", 0, codec.MAX_TIMESTAMP),
            raw["accepted_revision"], raw["basis_revision"], raw["supersedes_resolution_id"],
            EffectAssessmentKind(raw["assessment_kind"]), codec.text(raw["acceptance_event_id"], "acceptance_event_id"),
            raw["already_accepted"])
    except (ValueError, TypeError, KeyError, UnicodeError) as exc:
        raise ProtocolError("invalid effect resolution receipt") from exc
