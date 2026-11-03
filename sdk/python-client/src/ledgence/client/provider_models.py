"""Typed retained provider observations and scheduling metadata (MIT).

These values describe adapter-classified evidence. They do not grant dispatch
or infer a provider result from a task's execution state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
import json
from typing import Literal, TYPE_CHECKING

from . import codec
from .errors import InputError
from .models import ErrorDetail
from .provider_bindings import ProviderTarget, _digest, parse_provider_target

if TYPE_CHECKING:
    from .effect_models import EffectWorkerIdentity, EffectOccurrence


class ProviderFilter(StrEnum):
    ALL = "all"
    PENDING = "pending"
    NEEDS_REVIEW = "needs_review"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    DISPUTED = "disputed"


class ProviderKnowledgeKind(StrEnum):
    UNKNOWN = "unknown"
    PENDING = "pending"
    REQUIRES_ACTION = "requires_action"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    DISPUTED = "disputed"


class ProviderHandlingState(StrEnum):
    UNRESOLVED = "unresolved"
    PROVIDER_TERMINAL = "provider_terminal"
    OPERATOR_CLOSED = "operator_closed"


class ProviderRecoveryStopReason(StrEnum):
    PROVIDER_TERMINAL = "provider_terminal"
    OPERATOR_ASSESSMENT = "operator_assessment"
    WORKFLOW_CANCELLED = "workflow_cancelled"
    WORKFLOW_FAILED = "workflow_failed"
    ACTION_REQUIRED = "action_required"
    LOOKUP_UNAVAILABLE = "lookup_unavailable"
    DISPUTED = "disputed"
    BUDGET_EXHAUSTED = "budget_exhausted"
    DEADLINE = "deadline"
    RECOVERY_FAILED = "recovery_failed"


class ProviderReportIngress(StrEnum):
    LIVE_DISPATCH = "live_dispatch"
    LIVE_PROBE = "live_probe"
    EVIDENCE_ONLY = "evidence_only"


class ProviderProbeFailureKind(StrEnum):
    INCOMPATIBLE_WORKER = "incompatible_worker"
    CONTRACT_MISMATCH = "contract_mismatch"
    CONTEXT_INVALID = "context_invalid"
    RESULT_INVALID = "result_invalid"
    INVOCATION_TIMEOUT = "invocation_timeout"
    INVOCATION_FAILED = "invocation_failed"
    PREPARATION_FAILED = "preparation_failed"
    WORKER_LOST = "worker_lost"
    CANCELLED = "cancelled"
    MISSING_REPORT = "missing_report"


@dataclass(frozen=True, slots=True)
class ProviderUnknown:
    reason: str
    kind: Literal["unknown"] = field(default="unknown", init=False)


@dataclass(frozen=True, slots=True)
class ProviderPending:
    kind: Literal["pending"] = field(default="pending", init=False)


@dataclass(frozen=True, slots=True)
class ProviderRequiresAction:
    action_kind: str
    kind: Literal["requires_action"] = field(default="requires_action", init=False)


@dataclass(frozen=True, slots=True)
class ProviderSucceeded:
    _result: bytes = field(repr=False)
    kind: Literal["succeeded"] = field(default="succeeded", init=False)

    @property
    def result(self):
        """Return an independent copy; explicit JSON null is a valid result."""
        return json.loads(self._result)


@dataclass(frozen=True, slots=True)
class ProviderFailed:
    failure: ErrorDetail
    kind: Literal["failed"] = field(default="failed", init=False)


@dataclass(frozen=True, slots=True)
class ProviderCancelled:
    reason: str
    kind: Literal["cancelled"] = field(default="cancelled", init=False)


ProviderKnowledge = (
    ProviderUnknown
    | ProviderPending
    | ProviderRequiresAction
    | ProviderSucceeded
    | ProviderFailed
    | ProviderCancelled
)


@dataclass(frozen=True, slots=True)
class ProviderReport:
    reported_target: ProviderTarget
    provider_reference: str | None
    provider_request_id: str | None
    knowledge: ProviderKnowledge


@dataclass(frozen=True, slots=True)
class ProviderReportReference:
    report_id: str
    source_id: str
    sequence: int
    recorded_revision: int
    recorded_at: int
    knowledge: ProviderKnowledgeKind


@dataclass(frozen=True, slots=True)
class ProviderReconciliationSummary:
    queue: str
    checks_used: int
    max_checks: int
    generation: int
    active_probe_id: str | None
    active_task_id: str | None
    first_due_at: int
    next_due_at: int | None
    deadline_at: int
    stopped_reason: ProviderRecoveryStopReason | None


@dataclass(frozen=True, slots=True)
class ProviderEffectSummary:
    binding_digest: str
    knowledge: ProviderKnowledgeKind
    selected_report: ProviderReportReference | None
    conflicting_reports: tuple[ProviderReportReference, ...]
    reconciliation: ProviderReconciliationSummary
    handling: ProviderHandlingState
    candidate_revision: int | None


@dataclass(frozen=True, slots=True)
class ProviderProbeAssignment:
    occurrence: EffectOccurrence
    probe_id: str
    generation: int


def parse_provider_probe(raw, scope):
    from .effect_models import _occurrence

    codec.fields(raw, {"occurrence", "probe_id", "generation"})
    return ProviderProbeAssignment(
        _occurrence(raw["occurrence"], scope),
        codec.text(raw["probe_id"], "probe_id"),
        codec.integer(raw["generation"], "generation", 1),
    )


@dataclass(frozen=True, slots=True)
class ProviderDispatchSource:
    claim_id: str
    kind: Literal["dispatch"] = field(default="dispatch", init=False)


@dataclass(frozen=True, slots=True)
class ProviderProbeSource:
    probe_id: str
    generation: int
    task_id: str
    attempt_id: str
    kind: Literal["probe"] = field(default="probe", init=False)


ProviderSourceReference = ProviderDispatchSource | ProviderProbeSource


@dataclass(frozen=True, slots=True)
class _ProviderEntry:
    sequence: int
    recorded_at: int
    recorded_revision: int


@dataclass(frozen=True, slots=True)
class ProviderDispatchEntry(_ProviderEntry):
    claim_id: str
    source_id: str
    report_id: str
    worker: EffectWorkerIdentity
    kind: Literal["provider_dispatch"] = field(default="provider_dispatch", init=False)


@dataclass(frozen=True, slots=True)
class ProviderProbeScheduledEntry(_ProviderEntry):
    probe_id: str
    generation: int
    task_id: str
    due_at: int
    kind: Literal["provider_probe_scheduled"] = field(
        default="provider_probe_scheduled", init=False
    )


@dataclass(frozen=True, slots=True)
class ProviderProbeSourceEntry(_ProviderEntry):
    probe_id: str
    generation: int
    task_id: str
    attempt_id: str
    source_id: str
    report_id: str
    worker: EffectWorkerIdentity
    kind: Literal["provider_probe_source"] = field(
        default="provider_probe_source", init=False
    )


@dataclass(frozen=True, slots=True)
class ProviderReportEntry(_ProviderEntry):
    source_id: str
    source: ProviderSourceReference
    report_id: str
    worker: EffectWorkerIdentity
    ingress: ProviderReportIngress
    report: ProviderReport
    kind: Literal["provider_report"] = field(default="provider_report", init=False)

    @property
    def reference(self):
        return ProviderReportReference(
            self.report_id,
            self.source_id,
            self.sequence,
            self.recorded_revision,
            self.recorded_at,
            ProviderKnowledgeKind(self.report.knowledge.kind),
        )


@dataclass(frozen=True, slots=True)
class ProviderProbeReported:
    report: ProviderReportReference
    kind: Literal["reported"] = field(default="reported", init=False)


@dataclass(frozen=True, slots=True)
class ProviderProbeUnreported:
    failure: ProviderProbeFailureKind
    kind: Literal["unreported"] = field(default="unreported", init=False)


ProviderProbeCompletion = ProviderProbeReported | ProviderProbeUnreported


@dataclass(frozen=True, slots=True)
class ProviderProbeFinishedEntry(_ProviderEntry):
    probe_id: str
    generation: int
    task_id: str
    completion: ProviderProbeCompletion
    kind: Literal["provider_probe_finished"] = field(
        default="provider_probe_finished", init=False
    )


@dataclass(frozen=True, slots=True)
class ProviderRecoveryStoppedEntry(_ProviderEntry):
    reason: ProviderRecoveryStopReason
    kind: Literal["provider_recovery_stopped"] = field(
        default="provider_recovery_stopped", init=False
    )


ProviderEvidenceEntry = (
    ProviderDispatchEntry
    | ProviderProbeScheduledEntry
    | ProviderProbeSourceEntry
    | ProviderReportEntry
    | ProviderProbeFinishedEntry
    | ProviderRecoveryStoppedEntry
)


@dataclass(frozen=True, slots=True)
class ProviderEffectDetail:
    current_report: ProviderReportEntry | None


def _nullable_text(value, name, maximum=128):
    return None if value is None else codec.text(value, name, maximum)


def _timestamp(value, name):
    return codec.integer(value, name, 0, codec.MAX_TIMESTAMP)


def _message(value, name):
    if type(value) is not str or len(value.encode()) > 4096:
        raise InputError(f"invalid provider {name}")
    return value


def parse_provider_report(raw):
    codec.fields(raw, set(ProviderReport.__dataclass_fields__))
    codec.validate_authoritative(raw, 144 * 1024, max_depth=96)
    target = parse_provider_target(raw["reported_target"])
    reference = _nullable_text(raw["provider_reference"], "provider reference", 1024)
    request = _nullable_text(raw["provider_request_id"], "provider request id", 1024)
    knowledge = raw["knowledge"]
    kind = knowledge.get("kind") if type(knowledge) is dict else None
    if kind in ("unknown", "cancelled"):
        codec.fields(knowledge, {"kind", "reason"})
        knowledge = (ProviderUnknown if kind == "unknown" else ProviderCancelled)(
            codec.text(knowledge["reason"], "provider reason", 4096)
        )
    elif kind == "pending":
        codec.fields(knowledge, {"kind"})
        knowledge = ProviderPending()
    elif kind == "requires_action":
        codec.fields(knowledge, {"kind", "action_kind"})
        knowledge = ProviderRequiresAction(
            codec.text(knowledge["action_kind"], "action_kind")
        )
    elif kind == "succeeded":
        codec.fields(knowledge, {"kind", "result"})
        codec.validate_authoritative(knowledge["result"], 128 * 1024)
        knowledge = ProviderSucceeded(
            json.dumps(
                knowledge["result"],
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode()
        )
    elif kind == "failed":
        codec.fields(knowledge, {"kind", "failure"})
        failure = knowledge["failure"]
        codec.fields(failure, {"kind", "message"})
        knowledge = ProviderFailed(
            ErrorDetail(
                codec.text(failure["kind"], "failure kind"),
                _message(failure["message"], "failure message"),
            )
        )
    else:
        raise InputError("invalid provider report knowledge")
    return ProviderReport(target, reference, request, knowledge)


def parse_provider_reference(raw, count, revision):
    codec.fields(raw, set(ProviderReportReference.__dataclass_fields__))
    sequence = codec.integer(raw["sequence"], "report sequence", 1, count)
    recorded = codec.integer(
        raw["recorded_revision"], "report revision", sequence + 1, revision
    )
    knowledge = ProviderKnowledgeKind(raw["knowledge"])
    if knowledge == ProviderKnowledgeKind.DISPUTED:
        raise InputError("a single provider report cannot assert an aggregate dispute")
    return ProviderReportReference(
        codec.text(raw["report_id"], "report_id"),
        codec.text(raw["source_id"], "source_id"),
        sequence,
        recorded,
        _timestamp(raw["recorded_at"], "report recorded_at"),
        knowledge,
    )


def parse_provider_summary(raw, count, revision):
    codec.fields(raw, set(ProviderEffectSummary.__dataclass_fields__))
    knowledge = ProviderKnowledgeKind(raw["knowledge"])
    selected = (
        parse_provider_reference(raw["selected_report"], count, revision)
        if raw["selected_report"] is not None
        else None
    )
    conflicts = raw["conflicting_reports"]
    if type(conflicts) is not list or len(conflicts) not in (0, 2):
        raise InputError("provider dispute needs exactly two report references")
    conflicts = tuple(
        parse_provider_reference(item, count, revision) for item in conflicts
    )
    if (knowledge == ProviderKnowledgeKind.DISPUTED) != bool(conflicts):
        raise InputError("provider dispute references disagree with knowledge")
    if conflicts and (
        selected is not None
        or conflicts[0].sequence >= conflicts[1].sequence
        or conflicts[0].report_id == conflicts[1].report_id
    ):
        raise InputError("invalid provider conflict references")
    if selected is not None and selected.knowledge != knowledge:
        raise InputError("provider selected report differs from knowledge")
    if selected is None and knowledge not in (
        ProviderKnowledgeKind.UNKNOWN,
        ProviderKnowledgeKind.DISPUTED,
    ):
        raise InputError("provider knowledge lacks a source report")
    schedule = raw["reconciliation"]
    codec.fields(schedule, set(ProviderReconciliationSummary.__dataclass_fields__))
    max_checks = codec.integer(schedule["max_checks"], "max_checks", 1, 256)
    used = codec.integer(schedule["checks_used"], "checks_used", 0, max_checks)
    generation = codec.integer(schedule["generation"], "generation")
    active_probe = _nullable_text(schedule["active_probe_id"], "active_probe_id")
    active_task = _nullable_text(schedule["active_task_id"], "active_task_id")
    first = _timestamp(schedule["first_due_at"], "first_due_at")
    deadline = codec.integer(
        schedule["deadline_at"], "deadline_at", first + 1, codec.MAX_TIMESTAMP
    )
    next_due = (
        None
        if schedule["next_due_at"] is None
        else codec.integer(schedule["next_due_at"], "next_due_at", 0, deadline)
    )
    stopped = (
        None
        if schedule["stopped_reason"] is None
        else ProviderRecoveryStopReason(schedule["stopped_reason"])
    )
    if generation != used:
        raise InputError("provider generation differs from its allocated check count")
    if (active_probe is None) != (active_task is None) or (
        active_probe is not None and generation == 0
    ):
        raise InputError("provider active task and generation disagree")
    if stopped is not None and next_due is not None:
        raise InputError("stopped provider recovery cannot schedule another check")
    schedule = ProviderReconciliationSummary(
        codec.text(schedule["queue"], "queue"),
        used,
        max_checks,
        generation,
        active_probe,
        active_task,
        first,
        next_due,
        deadline,
        stopped,
    )
    candidate = (
        None
        if raw["candidate_revision"] is None
        else codec.integer(raw["candidate_revision"], "candidate_revision", 1, revision)
    )
    handling = ProviderHandlingState(raw["handling"])
    terminal = knowledge in (
        ProviderKnowledgeKind.SUCCEEDED,
        ProviderKnowledgeKind.FAILED,
        ProviderKnowledgeKind.CANCELLED,
    )
    if handling == ProviderHandlingState.PROVIDER_TERMINAL and not (
        terminal or knowledge == ProviderKnowledgeKind.DISPUTED
    ):
        raise InputError("provider terminal handling lacks terminal knowledge")
    if handling == ProviderHandlingState.UNRESOLVED and terminal:
        raise InputError("terminal provider knowledge cannot remain unresolved")
    return ProviderEffectSummary(
        _digest(raw["binding_digest"]),
        knowledge,
        selected,
        conflicts,
        schedule,
        handling,
        candidate,
    )


def _source(raw):
    kind = raw.get("kind") if type(raw) is dict else None
    if kind == "dispatch":
        codec.fields(raw, {"kind", "claim_id"})
        return ProviderDispatchSource(codec.text(raw["claim_id"], "claim_id"))
    if kind == "probe":
        codec.fields(raw, {"kind", "probe_id", "generation", "task_id", "attempt_id"})
        return ProviderProbeSource(
            codec.text(raw["probe_id"], "probe_id"),
            codec.integer(raw["generation"], "generation", 1),
            codec.text(raw["task_id"], "task_id"),
            codec.text(raw["attempt_id"], "attempt_id"),
        )
    raise InputError("invalid provider report source")


def parse_provider_entry(raw, summary):
    from .effect_models import _worker

    if summary.provider is None:
        raise InputError("provider entry has no provider occurrence")
    kind = raw.get("kind") if type(raw) is dict else None
    classes = {
        "provider_dispatch": ProviderDispatchEntry,
        "provider_probe_scheduled": ProviderProbeScheduledEntry,
        "provider_probe_source": ProviderProbeSourceEntry,
        "provider_report": ProviderReportEntry,
        "provider_probe_finished": ProviderProbeFinishedEntry,
        "provider_recovery_stopped": ProviderRecoveryStoppedEntry,
    }
    selected = classes.get(kind)
    if selected is None:
        raise InputError("invalid provider entry kind")
    codec.fields(raw, set(selected.__dataclass_fields__))
    codec.validate_authoritative(raw, 256 * 1024, max_depth=96)
    sequence = codec.integer(raw["sequence"], "sequence", 1, summary.entry_count)
    revision = codec.integer(
        raw["recorded_revision"],
        "recorded_revision",
        sequence + 1,
        summary.effect_revision,
    )
    fields = {
        "sequence": sequence,
        "recorded_at": _timestamp(raw["recorded_at"], "recorded_at"),
        "recorded_revision": revision,
    }
    for name in (
        "claim_id",
        "source_id",
        "report_id",
        "probe_id",
        "task_id",
        "attempt_id",
    ):
        if name in raw:
            fields[name] = codec.text(raw[name], name)
    if "worker" in raw:
        fields["worker"] = _worker(raw["worker"], summary.occurrence.scope)
    if "generation" in raw:
        fields["generation"] = codec.integer(raw["generation"], "generation", 1)
    if "due_at" in raw:
        fields["due_at"] = _timestamp(raw["due_at"], "due_at")
    if kind == "provider_report":
        fields.update(
            source=_source(raw["source"]),
            ingress=ProviderReportIngress(raw["ingress"]),
            report=parse_provider_report(raw["report"]),
        )
        if (
            fields["ingress"] == ProviderReportIngress.LIVE_DISPATCH
            and not isinstance(fields["source"], ProviderDispatchSource)
            or fields["ingress"] == ProviderReportIngress.LIVE_PROBE
            and not isinstance(fields["source"], ProviderProbeSource)
        ):
            raise InputError("provider report ingress differs from source")
    elif kind == "provider_probe_finished":
        completion = raw["completion"]
        ck = completion.get("kind") if type(completion) is dict else None
        if ck == "reported":
            codec.fields(completion, {"kind", "report"})
            reference = parse_provider_reference(
                completion["report"], sequence - 1, revision - 1
            )
            fields["completion"] = ProviderProbeReported(reference)
        elif ck == "unreported":
            codec.fields(completion, {"kind", "failure"})
            fields["completion"] = ProviderProbeUnreported(
                ProviderProbeFailureKind(completion["failure"])
            )
        else:
            raise InputError("invalid provider probe completion")
    elif kind == "provider_recovery_stopped":
        fields["reason"] = ProviderRecoveryStopReason(raw["reason"])
    return selected(**fields)


def provider_filter_matches(summary, selected):
    provider = summary.provider
    if provider is None:
        return False
    operator_conflict = (
        summary.operator_resolution is not None
        and summary.operator_resolution.first_conflicting_entry_sequence is not None
    )
    disputed = provider.knowledge == ProviderKnowledgeKind.DISPUTED or operator_conflict
    needs_review = operator_conflict or (
        provider.handling != ProviderHandlingState.OPERATOR_CLOSED
        and (
            provider.knowledge == ProviderKnowledgeKind.DISPUTED
            or (
                provider.handling == ProviderHandlingState.UNRESOLVED
                and provider.reconciliation.stopped_reason
                in (
                    ProviderRecoveryStopReason.ACTION_REQUIRED,
                    ProviderRecoveryStopReason.LOOKUP_UNAVAILABLE,
                    ProviderRecoveryStopReason.BUDGET_EXHAUSTED,
                    ProviderRecoveryStopReason.DEADLINE,
                    ProviderRecoveryStopReason.RECOVERY_FAILED,
                )
            )
        )
    )
    if selected == ProviderFilter.ALL:
        return True
    if selected == ProviderFilter.NEEDS_REVIEW:
        return needs_review
    if selected == ProviderFilter.DISPUTED:
        return disputed
    if selected == ProviderFilter.PENDING:
        return (
            provider.knowledge
            in (ProviderKnowledgeKind.UNKNOWN, ProviderKnowledgeKind.PENDING)
            and provider.handling == ProviderHandlingState.UNRESOLVED
            and provider.reconciliation.stopped_reason is None
        )
    return provider.knowledge == selected
