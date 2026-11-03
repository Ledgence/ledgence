"""Typed, scoped operator audit observations; never an authorization input (MIT)."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Literal

from . import codec
from .approval_models import ApprovalStatus, WorkflowApproval, parse_approval
from .errors import InputError, ProtocolError
from .models import Scope, _scope

AUDIT_PAGE_LIMIT = 1024 * 1024
AUDIT_EVENT_LIMIT = 8192
AUDIT_CURSOR_LIMIT = 8192


class AuditResourceKind(StrEnum):
    TASK = "task"
    WORKFLOW = "workflow"


class AuditActorClass(StrEnum):
    OPERATOR = "operator"
    SERVICE = "service"


class AuditReadKind(StrEnum):
    TASK_INSPECT = "task_inspect"
    TASK_RESULT = "task_result"
    WORKFLOW_RESULT = "workflow_result"
    WORKFLOW_INPUT = "workflow_input"
    LOCAL_STEP_INSPECT = "local_step_inspect"
    APPROVAL_INSPECT = "approval_inspect"
    APPROVAL_LIST = "approval_list"
    APPROVAL_DECISION = "approval_decision"


@dataclass(frozen=True, slots=True)
class AuditAudience:
    authority_id: str
    instance_id: str
    scope: Scope


@dataclass(frozen=True, slots=True)
class AuditActor:
    issuer: str
    subject: str
    principal_class: AuditActorClass
    credential_id: str


@dataclass(frozen=True, slots=True)
class AuditTask:
    task_id: str
    kind: Literal["task"] = field(default="task", init=False)


@dataclass(frozen=True, slots=True)
class AuditWorkflow:
    workflow_id: str
    kind: Literal["workflow"] = field(default="workflow", init=False)


@dataclass(frozen=True, slots=True)
class AuditLocalStep:
    workflow_id: str
    activation_id: str
    step_key: str
    kind: Literal["local_step"] = field(default="local_step", init=False)


@dataclass(frozen=True, slots=True)
class AuditApproval:
    workflow_id: str
    key: str
    kind: Literal["approval"] = field(default="approval", init=False)


@dataclass(frozen=True, slots=True)
class AuditApprovalPage:
    workflow_id: str
    keys: tuple[str, ...]
    kind: Literal["approval_page"] = field(default="approval_page", init=False)


AuditResource = AuditTask | AuditWorkflow | AuditLocalStep | AuditApproval | AuditApprovalPage


@dataclass(frozen=True, slots=True)
class AuditAcceptance:
    workflow_id: str
    key: str
    activation_id: str
    revision: int
    decision_id: str
    decision: Literal["approve", "reject"]
    kind: Literal["approval_accepted"] = field(default="approval_accepted", init=False)


@dataclass(frozen=True, slots=True)
class AuditSensitiveResponse:
    read: AuditReadKind
    resource: AuditResource
    kind: Literal["sensitive_response"] = field(default="sensitive_response", init=False)


@dataclass(frozen=True, slots=True)
class AuditEvent:
    event_id: str
    recorded_at: int
    audience: AuditAudience
    actor: AuditActor
    operation: AuditAcceptance | AuditSensitiveResponse


@dataclass(frozen=True, slots=True)
class AuditPage:
    items: tuple[AuditEvent, ...]
    next_cursor: str | None


@dataclass(frozen=True, slots=True, init=False)
class AuditFilter:
    """A bounded filter created with all(), resource(), or approval_acceptance()."""
    _encoded: bytes = field(repr=False)

    def __init__(self):
        raise TypeError("use an AuditFilter factory")

    @classmethod
    def _create(cls, value):
        result = object.__new__(cls)
        object.__setattr__(result, "_encoded", codec.encode(value, AUDIT_EVENT_LIMIT))
        return result

    @classmethod
    def all(cls):
        return cls._create({"kind": "all"})

    @classmethod
    def resource(cls, kind: AuditResourceKind | str, resource_id: str):
        try:
            kind = AuditResourceKind(kind)
        except (ValueError, TypeError) as exc:
            raise InputError("invalid audit resource kind") from exc
        codec.text(resource_id, "resource_id")
        return cls._create({"kind": "resource", "resource_kind": kind.value, "resource_id": resource_id})

    @classmethod
    def approval_acceptance(cls, *, workflow_id: str, key: str, activation_id: str,
                            revision: int, decision_id: str):
        value = {"kind": "approval_acceptance", "workflow_id": workflow_id, "key": key,
                 "activation_id": activation_id, "revision": revision, "decision_id": decision_id}
        for name in ("workflow_id", "key", "activation_id", "decision_id"):
            codec.text(value[name], name)
        codec.integer(revision, "revision")
        return cls._create(value)

    def to_dict(self):
        return codec.decode(self._encoded, AUDIT_EVENT_LIMIT)


def _resource(raw):
    kind = raw.get("kind") if type(raw) is dict else None
    shapes = {
        "task": (AuditTask, ("task_id",)),
        "workflow": (AuditWorkflow, ("workflow_id",)),
        "local_step": (AuditLocalStep, ("workflow_id", "activation_id", "step_key")),
        "approval": (AuditApproval, ("workflow_id", "key")),
        "approval_page": (AuditApprovalPage, ("workflow_id", "keys")),
    }
    if kind not in shapes:
        raise InputError("invalid audit resource")
    constructor, fields = shapes[kind]
    codec.fields(raw, {"kind", *fields})
    values = {}
    for name in fields:
        if name == "keys":
            keys = raw[name]
            if type(keys) is not list or len(keys) > 10:
                raise InputError("invalid audit approval page")
            values[name] = tuple(codec.text(key, "approval key") for key in keys)
            if len(set(values[name])) != len(keys):
                raise InputError("duplicate audit approval key")
        else:
            values[name] = codec.text(raw[name], name)
    return constructor(**values)


def _event(raw, scope):
    codec.fields(raw, {"event_id", "recorded_at", "audience", "actor", "operation"})
    codec.validate_authoritative(raw, AUDIT_EVENT_LIMIT)
    event_id = codec.text(raw["event_id"], "event_id")
    at = codec.integer(raw["recorded_at"], "recorded_at", 0, codec.MAX_TIMESTAMP)
    audience = raw["audience"]
    codec.fields(audience, {"authority_id", "instance_id", "scope"})
    actual_scope = _scope(audience["scope"])
    if actual_scope != scope:
        raise InputError("audit event belongs to another scope")
    audience = AuditAudience(codec.text(audience["authority_id"], "authority_id"),
                             codec.text(audience["instance_id"], "instance_id"), actual_scope)
    actor = raw["actor"]
    codec.fields(actor, {"issuer", "subject", "class", "credential_id"})
    actor = AuditActor(codec.text(actor["issuer"], "issuer", 256),
                       codec.text(actor["subject"], "subject"), AuditActorClass(actor["class"]),
                       codec.text(actor["credential_id"], "credential_id"))
    operation = raw["operation"]
    kind = operation.get("kind") if type(operation) is dict else None
    if kind == "approval_accepted":
        codec.fields(operation, {"kind", "workflow_id", "key", "activation_id", "revision", "decision_id", "decision"})
        if operation["decision"] not in ("approve", "reject"):
            raise InputError("invalid audited decision")
        operation = AuditAcceptance(*(codec.text(operation[name], name) for name in ("workflow_id", "key", "activation_id")),
                                    codec.integer(operation["revision"], "revision"),
                                    codec.text(operation["decision_id"], "decision_id"), operation["decision"])
    elif kind == "sensitive_response":
        codec.fields(operation, {"kind", "read", "resource"})
        operation = AuditSensitiveResponse(AuditReadKind(operation["read"]), _resource(operation["resource"]))
        expected = {
            AuditReadKind.TASK_INSPECT: AuditTask, AuditReadKind.TASK_RESULT: AuditTask,
            AuditReadKind.WORKFLOW_RESULT: AuditWorkflow, AuditReadKind.WORKFLOW_INPUT: AuditWorkflow,
            AuditReadKind.LOCAL_STEP_INSPECT: AuditLocalStep, AuditReadKind.APPROVAL_INSPECT: AuditApproval,
            AuditReadKind.APPROVAL_LIST: AuditApprovalPage, AuditReadKind.APPROVAL_DECISION: AuditApproval,
        }[operation.read]
        if type(operation.resource) is not expected:
            raise InputError("audit read and resource disagree")
    else:
        raise InputError("invalid audit operation")
    return AuditEvent(event_id, at, audience, actor, operation)


def _matches(event, selected):
    operation = event.operation
    if selected["kind"] == "all": return True
    if selected["kind"] == "approval_acceptance":
        return isinstance(operation, AuditAcceptance) and all(
            getattr(operation, name) == selected[name]
            for name in ("workflow_id", "key", "activation_id", "revision", "decision_id"))
    resource = operation if isinstance(operation, AuditAcceptance) else operation.resource
    actual_kind = "task" if isinstance(resource, AuditTask) else "workflow"
    actual_id = resource.task_id if isinstance(resource, AuditTask) else resource.workflow_id
    return actual_kind == selected["resource_kind"] and actual_id == selected["resource_id"]


def _page(raw, scope, selected, cursor, limit):
    try:
        codec.fields(raw, {"items", "next_cursor"})
        if type(raw["items"]) is not list or len(raw["items"]) > limit:
            raise InputError("invalid audit page size")
        items = tuple(_event(item, scope) for item in raw["items"])
        if len({item.event_id for item in items}) != len(items) or any(not _matches(item, selected) for item in items):
            raise InputError("audit page does not match query")
        if items and any(item.audience != items[0].audience for item in items):
            raise InputError("audit page mixes audiences")
        after = raw["next_cursor"]
        if after is not None:
            codec.text(after, "next_cursor", AUDIT_CURSOR_LIMIT)
            if after == cursor or len(items) != limit:
                raise InputError("audit cursor does not advance a full page")
        if selected["kind"] == "approval_acceptance" and (len(items) > 1 or after is not None):
            raise InputError("ambiguous approval acceptance")
        return AuditPage(items, after)
    except (ValueError, TypeError, KeyError, UnicodeError) as exc:
        raise ProtocolError("invalid operator audit response") from exc


class Audit:
    def __init__(self, client): self._client = client

    async def list(self, *, filter: AuditFilter | None = None, cursor: str | None = None, limit: int = 50) -> AuditPage:
        selected = (AuditFilter.all() if filter is None else filter)
        if type(selected) is not AuditFilter:
            raise InputError("filter must be an AuditFilter")
        selected = selected.to_dict()
        codec.integer(limit, "limit", 1, 100)
        if cursor is not None: codec.text(cursor, "cursor", AUDIT_CURSOR_LIMIT)
        if selected["kind"] == "approval_acceptance" and (limit != 2 or cursor is not None):
            raise InputError("exact approval audit lookup requires limit=2 and no cursor")
        scope = self._client.scope
        body = codec.encode({"scope": {"tenant_id": scope.tenant_id, "namespace": scope.namespace},
                             "filter": selected, "cursor": cursor, "limit": limit})
        return await self._client._require_transport().exchange(
            "POST", "/v1/audit/events", body=body,
            deadline=asyncio.get_running_loop().time() + self._client.request_timeout,
            parser=lambda raw: _page(raw, scope, selected, cursor, limit), limit=AUDIT_PAGE_LIMIT)

    async def approval_acceptance(self, approval: WorkflowApproval) -> AuditEvent | None:
        """Read the original verified acceptance, or None after a successful empty lookup.

        Failures propagate; a denied or unavailable audit is never an unverified result.
        The event's actor may differ from this client's credential or reviewer label.
        """
        if (type(approval) is not WorkflowApproval or approval.scope != self._client.scope
                or approval._base_url != self._client.base_url or approval.decision is None
                or approval.status not in (ApprovalStatus.APPROVED, ApprovalStatus.REJECTED)):
            raise InputError("expected an accepted approval from this client endpoint and scope")
        try:
            parse_approval(approval.to_dict(), approval.scope, approval.workflow_id,
                           approval.key, approval._base_url)
        except (ProtocolError, ValueError, TypeError, AttributeError) as exc:
            raise InputError("expected a valid accepted approval") from exc
        query = AuditFilter.approval_acceptance(workflow_id=approval.workflow_id, key=approval.key,
                activation_id=approval.activation_id, revision=approval.revision,
                decision_id=approval.decision.decision_id)
        page = await self.list(filter=query, limit=2)
        event = page.items[0] if page.items else None
        if event is not None and (event.operation.decision != approval.decision.decision
                                  or event.recorded_at != approval.decision.decided_at):
            raise ProtocolError("audit acceptance does not match the observed decision")
        return event
