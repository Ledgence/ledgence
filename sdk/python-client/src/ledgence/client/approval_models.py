"""Immutable action-bound workflow approvals and strict wire observations (MIT)."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
import json

from . import codec
from .errors import InputError, ProtocolError
from .models import Scope, _scope

ACTION_LIMIT = 32 * 1024
APPROVAL_LIMIT = 96 * 1024
APPROVAL_PAGE_LIMIT = 1024 * 1024
MAX_APPROVAL_WAIT_MS = 31_536_000_000


class ApprovalStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class ApprovalAction:
    name: str
    version: str
    _arguments: bytes = field(repr=False)

    @property
    def arguments(self):
        """An independent copy of the exact effective arguments."""
        return json.loads(self._arguments)

    def to_dict(self):
        return {"name": self.name, "version": self.version, "arguments": self.arguments}


@dataclass(frozen=True, slots=True)
class ApprovalDecision:
    decision_id: str
    decision: str
    reviewer: str
    reason: str | None
    decided_at: int


@dataclass(frozen=True, slots=True)
class WorkflowApproval:
    scope: Scope
    workflow_id: str
    key: str
    activation_id: str
    revision: int
    action: ApprovalAction
    created_at: int
    deadline: int
    status: ApprovalStatus
    decision: ApprovalDecision | None
    resumed_activation_id: str | None
    _proposed_arguments: bytes = field(repr=False)
    _base_url: str = field(repr=False, compare=False)

    @property
    def proposed_arguments(self): return json.loads(self._proposed_arguments)

    def to_dict(self):
        return {"scope": {"tenant_id": self.scope.tenant_id, "namespace": self.scope.namespace},
                "workflow_id": self.workflow_id, "key": self.key,
                "activation_id": self.activation_id, "revision": self.revision,
                "action": self.action.to_dict(), "proposed_arguments": self.proposed_arguments,
                "created_at": self.created_at, "deadline": self.deadline, "status": self.status.value,
                "decision": None if self.decision is None else {
                    name: getattr(self.decision, name) for name in ApprovalDecision.__dataclass_fields__},
                "resumed_activation_id": self.resumed_activation_id}


@dataclass(frozen=True, slots=True)
class ApprovalPage:
    items: tuple[WorkflowApproval, ...]
    next_cursor: str | None


@dataclass(frozen=True, init=False)
class ApprovalDecisionCommand:
    """Exact decision bytes, frozen with their endpoint and scoped request."""
    base_url: str
    scope: Scope
    workflow_id: str
    key: str
    _body: bytes = field(repr=False)

    def __init__(self):
        raise TypeError("create commands with workflow.prepare_approval_decision()")

    @classmethod
    def _create(cls, base_url, scope, workflow_id, key, body):
        result = object.__new__(cls)
        for name, value in (("base_url", base_url), ("scope", scope), ("workflow_id", workflow_id),
                            ("key", key), ("_body", body)):
            object.__setattr__(result, name, value)
        return result

    def to_dict(self): return codec.decode(self._body, APPROVAL_LIMIT)


@dataclass(frozen=True, slots=True)
class ApprovalDecisionReceipt:
    approval: WorkflowApproval
    already_accepted: bool


def _json_identity(value):
    # Dict equality conflates 1, 1.0 and True. JSON tokens preserve those
    # distinctions; sorting keys makes object ordering immaterial.
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _action(raw):
    codec.fields(raw, {"name", "version", "arguments"})
    name = codec.text(raw["name"], "action.name", 512)
    version = codec.text(raw["version"], "action.version")
    if type(raw["arguments"]) is not dict:
        raise InputError("approval action arguments must be an object")
    codec.validate_authoritative(raw["arguments"], ACTION_LIMIT)
    codec.validate_authoritative(raw, ACTION_LIMIT, max_depth=96)
    return ApprovalAction(name, version, codec.encode(raw["arguments"], APPROVAL_LIMIT))


def _reason(value):
    if value is not None and (type(value) is not str or len(value.encode("utf-8")) > 4096):
        raise InputError("approval reason must contain at most 4096 UTF-8 bytes")
    return value


def _approval(raw, scope, workflow_id, key, base_url):
    codec.fields(raw, {"scope", "workflow_id", "key", "activation_id", "revision", "action",
                       "proposed_arguments", "created_at", "deadline", "status", "decision",
                       "resumed_activation_id"})
    actual_scope = _scope(raw["scope"])
    actual_id = codec.text(raw["workflow_id"], "workflow_id")
    actual_key = codec.text(raw["key"], "key")
    if actual_scope != scope or actual_id != workflow_id or (key is not None and actual_key != key):
        raise InputError("approval identity does not match the request")
    activation = codec.text(raw["activation_id"], "activation_id")
    revision = codec.integer(raw["revision"], "revision")
    action = _action(raw["action"])
    if raw["proposed_arguments"] is not None and type(raw["proposed_arguments"]) is not dict:
        raise InputError("proposed arguments must be an object or null")
    codec.validate_authoritative(raw["proposed_arguments"], ACTION_LIMIT)
    created = codec.integer(raw["created_at"], "created_at", 0, codec.MAX_TIMESTAMP)
    deadline = codec.integer(raw["deadline"], "deadline", 0, codec.MAX_TIMESTAMP)
    if not 0 <= deadline - created <= MAX_APPROVAL_WAIT_MS:
        raise InputError("invalid approval deadline")
    status = ApprovalStatus(raw["status"])
    decision = raw["decision"]
    if status in (ApprovalStatus.APPROVED, ApprovalStatus.REJECTED):
        codec.fields(decision, set(ApprovalDecision.__dataclass_fields__))
        codec.text(decision["decision_id"], "decision_id")
        codec.text(decision["reviewer"], "reviewer")
        _reason(decision["reason"])
        decided_at = codec.integer(decision["decided_at"], "decided_at", 0, codec.MAX_TIMESTAMP)
        expected = "approve" if status == ApprovalStatus.APPROVED else "reject"
        if decision["decision"] != expected or not created <= decided_at < deadline:
            raise InputError("approval decision contradicts status or deadline")
        decision = ApprovalDecision(**decision)
    elif decision is not None:
        raise InputError("unresolved approval cannot contain a decision")
    resumed = raw["resumed_activation_id"]
    if resumed is not None:
        codec.text(resumed, "resumed_activation_id")
        if status in (ApprovalStatus.PENDING, ApprovalStatus.CANCELLED) or resumed == activation:
            raise InputError("invalid resumed approval activation")
    return WorkflowApproval(actual_scope, actual_id, actual_key, activation, revision, action,
                            created, deadline, status, decision, resumed,
                            codec.encode(raw["proposed_arguments"], APPROVAL_LIMIT), base_url)


def parse_approval(raw, scope, workflow_id, key, base_url):
    try:
        return _approval(raw, scope, workflow_id, key, base_url)
    except (ValueError, TypeError, KeyError, UnicodeError) as exc:
        raise ProtocolError("invalid workflow approval response") from exc


def parse_approval_page(raw, scope, workflow_id, after_key, limit, base_url):
    try:
        codec.fields(raw, {"items", "next_cursor"})
        if type(raw["items"]) is not list or len(raw["items"]) > limit:
            raise InputError("invalid approval page length")
        items = tuple(_approval(item, scope, workflow_id, None, base_url) for item in raw["items"])
        previous = after_key
        for item in items:
            if previous is not None and item.key <= previous:
                raise InputError("approval page is unordered or contains repeated keys")
            previous = item.key
        cursor = raw["next_cursor"]
        if cursor is not None:
            codec.text(cursor, "next_cursor")
            if not items or len(items) != limit or cursor != items[-1].key:
                raise InputError("approval page cursor does not match its final key")
        return ApprovalPage(items, cursor)
    except (ValueError, TypeError, KeyError, UnicodeError) as exc:
        raise ProtocolError("invalid workflow approval page") from exc


def parse_approval_receipt(raw, command):
    try:
        codec.fields(raw, {"approval", "already_accepted"})
        approval = _approval(raw["approval"], command.scope, command.workflow_id, command.key, command.base_url)
        if type(raw["already_accepted"]) is not bool:
            raise InputError("already_accepted must be a boolean")
        expected = command.to_dict()
        actual = approval.to_dict()
        for name in ("activation_id", "revision", "action"):
            if _json_identity(actual[name]) != _json_identity(expected[name]):
                raise InputError("approval receipt changed the request binding")
        if approval.decision is None:
            raise InputError("approval receipt has no accepted decision")
        for name in ("decision_id", "decision", "reviewer", "reason"):
            if actual["decision"][name] != expected[name]:
                raise InputError("approval receipt changed the decision")
        return ApprovalDecisionReceipt(approval, raw["already_accepted"])
    except (ValueError, TypeError, KeyError, UnicodeError) as exc:
        raise ProtocolError("invalid approval decision receipt") from exc
