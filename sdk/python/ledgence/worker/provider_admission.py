"""Approved provider dispatch composed atomically with durable resource admission (MIT)."""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from time import monotonic

from . import admission
from .providers import (
    ProviderOperationTicket,
    _bound_adapter,
    _call,
    _digest,
    _request,
    _rpc,
    _validate_binding,
)
from .workflow import (
    MAX_STATE_BYTES,
    MAX_TIMESTAMP,
    ApprovalProtection,
    ApprovalStatus,
    WorkflowError,
    _encode,
    _fields,
    _freeze,
    _integer,
    _local_owner,
    _text,
    _validate_action,
)

RECORD_BYTES = 64 * 1024
COMMAND_BYTES = 256 * 1024


class ProviderAdmissionOutcomeKind(StrEnum):
    TICKET = "ticket"
    WAIT_REQUIRED = "wait_required"
    NEEDS_REVIEW = "needs_review"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class ProviderAdmissionOutcome:
    """A ticket resumes provider recovery; it is never another dispatch grant."""

    _encoded: bytes = field(repr=False)

    @property
    def kind(self):
        return ProviderAdmissionOutcomeKind(json.loads(self._encoded)["kind"])

    @property
    def ticket(self):
        if self.kind is not ProviderAdmissionOutcomeKind.TICKET:
            raise WorkflowError("provider admission outcome has no ticket")
        return ProviderOperationTicket.from_dict(json.loads(self._encoded)["ticket"])

    @property
    def reason(self):
        if self.kind is not ProviderAdmissionOutcomeKind.CLOSED:
            raise WorkflowError("provider admission outcome has no closure reason")
        reason = json.loads(self._encoded).get("reason")
        return None if reason is None else admission.AdmissionClosedReason(reason)


def _outcome(kind, **values):
    return ProviderAdmissionOutcome(
        _encode({"kind": kind.value, **values}, RECORD_BYTES, 96, authoritative=True)
    )


def validate_subject(binding):
    subject = binding["provider"]
    _fields(subject, {"ticket", "binding", "action"})
    ticket = ProviderOperationTicket.from_dict(subject["ticket"])
    request = subject["binding"]
    _validate_binding(request, resolved=False)
    _validate_action(subject["action"], authoritative=True)
    if request.get("admission") != {"resource": binding["resource"]}:
        raise WorkflowError("provider admission resource differs from reviewed binding")
    if (
        subject["action"]["name"] != request["contract"]["dispatch_callable"]
        or subject["action"]["version"] != request["contract"]["version"]
    ):
        raise WorkflowError("provider admission action differs from reviewed adapter")
    return ticket


def validate_source(source):
    _fields(source, {"admission", "provider"})
    authority, provider = source["admission"], source["provider"]
    admission.validate_source(authority)
    _fields(
        provider,
        {
            "source_id",
            "claim_id",
            "report_id",
            "ticket",
            "worker",
            "accepted_at",
            "first_possible_dispatch_at",
            "reconciliation_deadline",
        },
        {"budget_grant"},
    )
    if "budget_grant" in provider:
        from .workflow import _budget_dispatch_deadline
        _budget_dispatch_deadline(provider["budget_grant"], 0, authorized_at=provider["accepted_at"])
    ticket = ProviderOperationTicket.from_dict(provider["ticket"])
    _text(provider["source_id"], "provider source")
    if (
        provider["worker"] != authority["worker"]
        or provider["claim_id"] != authority["start_id"]
        or provider["report_id"] != authority["finish_id"]
        or ticket.occurrence.scope != authority["occurrence"]["scope"]
        or ticket.occurrence.workflow_id != authority["occurrence"]["workflow_id"]
        or ticket.occurrence.activation_id
        != authority["occurrence"]["origin_activation_id"]
    ):
        raise WorkflowError("composed provider source identities differ")
    for key in ("accepted_at", "first_possible_dispatch_at", "reconciliation_deadline"):
        _integer(provider[key], key, maximum=MAX_TIMESTAMP)
    if (
        provider["accepted_at"] != authority["accepted_at"]
        or provider["first_possible_dispatch_at"] != authority["accepted_at"]
        or provider["reconciliation_deadline"] <= authority["accepted_at"]
    ):
        raise WorkflowError("invalid composed provider source timestamps")


def validate_report(value):
    _fields(value, {"source", "call_completion", "receipt"})
    validate_source(value["source"])
    source, receipt = value["source"]["provider"], value["receipt"]
    if value["call_completion"] not in ("completed", "unknown"):
        raise WorkflowError("invalid provider call completion")
    _fields(
        receipt,
        {
            "ticket",
            "source_id",
            "source",
            "report_id",
            "report_digest",
            "worker",
            "accepted_at",
            "recorded_revision",
            "accepted_via",
            "already_accepted",
        },
    )
    _digest(receipt["report_digest"])
    admission.validate_source(
        {**value["source"]["admission"], "worker": receipt["worker"]}
    )
    _integer(receipt["accepted_at"], "report accepted_at", maximum=MAX_TIMESTAMP)
    _integer(receipt["recorded_revision"], "report recorded_revision", minimum=1)
    if (
        receipt["ticket"] != source["ticket"]
        or receipt["source_id"] != source["source_id"]
        or receipt["report_id"] != source["report_id"]
        or any(
            receipt["worker"].get(k) != source["worker"][k]
            for k in ("audience", "issuer", "subject")
        )
        or receipt["accepted_via"] != "evidence_only"
        or receipt["source"] != {"kind": "dispatch", "claim_id": source["claim_id"]}
        or receipt["accepted_at"] < source["accepted_at"]
        or type(receipt["already_accepted"]) is not bool
    ):
        raise WorkflowError(
            "provider report receipt does not match its original source"
        )
    _encode(value, RECORD_BYTES, 96, authoritative=True)


def validate_record(value):
    _fields(value, {"ticket", "source", "report"})
    ProviderOperationTicket.from_dict(value["ticket"])
    if value["source"] is not None:
        validate_source(value["source"])
        if value["source"]["provider"]["ticket"] != value["ticket"]:
            raise WorkflowError("provider record ticket differs from its source")
    if value["report"] is not None:
        validate_report(value["report"])
        if value["source"] is None or value["report"]["source"] != value["source"]:
            raise WorkflowError("provider record report differs from its source")
    _encode(value, RECORD_BYTES, 96, authoritative=True)


def _matches_source(wake, source):
    validate_source(source)
    authority = source["admission"]
    if (
        source["provider"]["ticket"] != wake["binding"]["provider"]["ticket"]
        or authority["occurrence"] != wake["occurrence"]
    ):
        raise WorkflowError(
            "provider source differs from the original approved occurrence"
        )
    selected = wake["selection"]
    if selected["kind"] == "offered":
        offer = selected["offer"]
        if (
            authority["generation"] != offer["generation"]
            or authority["binding_digest"] != offer["binding_digest"]
            or authority["owner"]["task_id"] != offer["activation_id"]
            or authority["accepted_at"] >= offer["expires_at"]
        ):
            raise WorkflowError("provider source differs from selected resource offer")
    elif (
        selected["kind"] == "needs_review"
        and authority["source_id"] != selected["source_id"]
    ):
        raise WorkflowError("provider source differs from selected review")
    elif selected["kind"] == "provider" and source != selected["record"]["source"]:
        raise WorkflowError("provider source differs from retained record")


def validate_record_for(binding, occurrence, record):
    validate_record(record)
    original = binding["provider"]["ticket"]
    if (
        record["ticket"]["occurrence"]["scope"] != original["occurrence"]["scope"]
        or record["ticket"]["binding_digest"] != original["binding_digest"]
        or original["occurrence"]["scope"] != occurrence["scope"]
        or original["occurrence"]["workflow_id"] != occurrence["workflow_id"]
        or original["occurrence"]["activation_id"] != occurrence["origin_activation_id"]
    ):
        raise WorkflowError("provider record differs from original occurrence")
    if record["source"] is not None and (
        record["source"]["admission"]["occurrence"] != occurrence
        or record["source"]["provider"]["ticket"] != original
    ):
        raise WorkflowError("provider record has a foreign source")


def matches_record(wake, record):
    validate_record_for(wake["binding"], wake["occurrence"], record)
    original = wake["binding"]["provider"]["ticket"]
    if (
        record["ticket"]["occurrence"]["scope"] != original["occurrence"]["scope"]
        or record["ticket"]["binding_digest"] != original["binding_digest"]
        or wake["selection"]["kind"] in ("closed", "finished")
    ):
        raise WorkflowError("provider admission record differs from selected intent")
    if record["source"] is not None:
        _matches_source(wake, record["source"])
    selected = wake["selection"]
    if selected["kind"] == "needs_review" and record["source"] is None:
        raise WorkflowError("review selection requires its original source")
    if selected["kind"] == "provider":
        previous = selected["record"]
        if (
            record["ticket"] != previous["ticket"]
            or record["source"] != previous["source"]
        ):
            raise WorkflowError("provider ticket differs from selected recovery record")
        if previous["report"] is not None:
            report = record["report"]
            if report is None or any(
                report[k] != previous["report"][k]
                for k in ("source", "call_completion")
            ):
                raise WorkflowError("retained provider report changed")
            if any(
                report["receipt"][k] != previous["report"]["receipt"][k]
                for k in ("report_digest", "accepted_at")
            ):
                raise WorkflowError("retained provider report acceptance changed")


def request_provider_admission(ctx, key, *, resume, state, timeout_ms):
    ctx._active()
    if not ctx._provider_admission:
        raise WorkflowError("atomic provider admission is unavailable")
    approved = ctx.approval
    if (
        approved is None
        or approved.status is not ApprovalStatus.APPROVED
        or approved.protection is not ApprovalProtection.PROVIDER_OPERATION
    ):
        raise WorkflowError("provider admission requires its original approved wake")
    raw = approved.to_dict()
    if raw["provider_binding"].get("admission") is None:
        raise WorkflowError("provider approval has no reviewed resource requirement")
    _bound_adapter(ctx._providers, raw["provider_binding"], raw["action"])
    wait = {
        "kind": "resource_admission",
        "key": key,
        "request": {"kind": "provider", "approval_key": raw["key"]},
        "timeout_ms": timeout_ms,
    }
    admission.validate_wait(wait)
    return ctx._decision(
        "wait",
        continuation=ctx._target(resume),
        state=_freeze(state, MAX_STATE_BYTES),
        commands=ctx._commands,
        wait=wait,
    )


def admitted_provider_operation(ctx):
    ctx._active()
    if (
        not ctx._provider_admission
        or ctx._admission_wake is None
        or "provider" not in ctx._admission_wake["binding"]
    ):
        raise WorkflowError("this activation has no governed provider admission")
    wake = ctx._admission_wake
    subject = wake["binding"]["provider"]
    binding = {**subject["binding"], "program": wake["binding"]["program"]}
    adapter = _bound_adapter(ctx._providers, binding, subject["action"])
    if ctx._admission_pending is None:
        # Capture the checked callable before scheduling or awaiting Start.
        # Mutable adapter slots cannot replace the reviewed operation in flight.
        ctx._admission_pending = asyncio.create_task(
            _run(ctx, wake, adapter._dispatch, binding)
        )
    return admission._AdmissionAwaitable(ctx, ctx._admission_pending)


async def _run(ctx, wake, dispatch, binding):
    record = ctx._provider_admission_record
    if record is None and wake["selection"]["kind"] == "provider":
        record = wake["selection"]["record"]
    if record is not None:
        matches_record(wake, record)
        return _outcome(ProviderAdmissionOutcomeKind.TICKET, ticket=record["ticket"])
    selection = wake["selection"]
    if selection["kind"] == "closed":
        return _outcome(ProviderAdmissionOutcomeKind.CLOSED, reason=selection["reason"])
    if selection["kind"] == "needs_review":
        return _outcome(ProviderAdmissionOutcomeKind.NEEDS_REVIEW)
    action = wake["binding"]["provider"]["action"]
    request = _request(action, binding, None)
    start_id, finish_id = "start_" + uuid.uuid4().hex, "report_" + uuid.uuid4().hex
    begun = monotonic()
    try:
        reply = await _rpc(
            ctx,
            "provider_admission.start.v1",
            {"start_id": start_id, "finish_id": finish_id},
        )
        if type(reply) is not dict:
            raise WorkflowError("invalid provider admission Start reply")
        if reply.get("kind") == "existing":
            _fields(reply, {"kind", "audience", "snapshot"})
            snapshot = reply["snapshot"]
            admission.validate_snapshot(snapshot)
            if (
                snapshot["occurrence"] != wake["occurrence"]
                or snapshot["binding"] != wake["binding"]
                or reply["audience"]["scope"] != wake["occurrence"]["scope"]
            ):
                raise WorkflowError("provider admission snapshot differs from its wake")
            if snapshot.get("provider") is not None:
                matches_record(wake, snapshot["provider"])
                return _outcome(
                    ProviderAdmissionOutcomeKind.TICKET,
                    ticket=snapshot["provider"]["ticket"],
                )
            if snapshot["state"] in (
                "closed_before_start",
                "administratively_released",
            ):
                return _outcome(
                    ProviderAdmissionOutcomeKind.CLOSED,
                    reason="administratively_released"
                    if snapshot["state"] == "administratively_released"
                    else admission.budget_closed_reason(snapshot),
                )
            if snapshot["state"] in ("possibly_dispatched", "unknown_occupied"):
                return _outcome(ProviderAdmissionOutcomeKind.NEEDS_REVIEW)
            return _outcome(ProviderAdmissionOutcomeKind.WAIT_REQUIRED)
        _fields(reply, {"kind", "source"})
        if reply["kind"] != "granted":
            raise WorkflowError("invalid composed provider grant")
        source = reply["source"]
        _matches_source(wake, source)
        authority = source["admission"]
        if (
            authority["start_id"] != start_id
            or authority["finish_id"] != finish_id
            or authority["owner"]["task_id"] != ctx.activation_id
        ):
            raise WorkflowError("provider grant does not match current Start")
        deadline = (
            begun + (authority["start_deadline_at"] - authority["accepted_at"]) / 1000
        )
        if "budget_grant" in source["provider"]:
            from .workflow import _budget_dispatch_deadline
            deadline = min(deadline, _budget_dispatch_deadline(source["provider"]["budget_grant"], begun, authorized_at=authority["accepted_at"]))
        if monotonic() >= deadline:
            raise WorkflowError("provider Start expired before callback")
    except BaseException as exc:
        ctx._fatal = exc
        raise
    record = {"ticket": source["provider"]["ticket"], "source": source, "report": None}
    ctx._provider_admission_record = _freeze(
        record, RECORD_BYTES, 96, authoritative=True
    )
    token = _local_owner.set(ctx)
    try:
        try:
            outcome = await _call(
                dispatch,
                action,
                binding,
                None,
                governed=True,
                request=request,
                dispatch_deadline=deadline,
            )
        except Exception:  # noqa: BLE001 - callback failures never prove call completion
            # No invented completion: the grant/recovery ticket is retained,
            # and uncertain call occupancy remains until explicit evidence/release.
            return _outcome(
                ProviderAdmissionOutcomeKind.TICKET, ticket=record["ticket"]
            )
    finally:
        _local_owner.reset(token)
    command = _freeze(
        {"source": source, "outcome": outcome.to_dict()}, COMMAND_BYTES, 96
    )
    try:
        reply = await _rpc(ctx, "provider_admission.report.v1", command)
        validate_report(reply)
        if (
            reply["source"] != source
            or reply["call_completion"] != command["outcome"]["call_completion"]
        ):
            raise WorkflowError("provider admission Report differs from frozen outcome")
        record["report"] = reply
        ctx._provider_admission_record = _freeze(
            record, RECORD_BYTES, 96, authoritative=True
        )
    except BaseException as exc:
        ctx._fatal = exc
        raise
    return _outcome(ProviderAdmissionOutcomeKind.TICKET, ticket=record["ticket"])
