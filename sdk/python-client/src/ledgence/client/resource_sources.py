"""Audited discovery of unresolved charged admission sources."""

import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Literal

from . import codec
from .admission_models import timestamp
from .errors import InputError, ProtocolError
from .models import Scope, _scope
from .resource_decisions import TaskDecisionSubject


class ResourceSourceState(StrEnum):
    POSSIBLY_DISPATCHED = "possibly_dispatched"
    UNKNOWN_OCCUPIED = "unknown_occupied"


@dataclass(frozen=True, slots=True)
class WorkflowSourceSubject:
    workflow_id: str
    origin_activation_id: str
    key: str
    kind: Literal["workflow"] = field(default="workflow", init=False)


@dataclass(frozen=True, slots=True)
class ProviderSourceSubject:
    workflow_id: str
    origin_activation_id: str
    key: str
    approval_activation_id: str
    approval_key: str
    kind: Literal["provider"] = field(default="provider", init=False)


@dataclass(frozen=True, slots=True)
class ResourceSource:
    scope: Scope
    resource: str
    sequence: int
    revision: int
    updated_at: int
    state: ResourceSourceState
    subject: TaskDecisionSubject | WorkflowSourceSubject | ProviderSourceSubject


@dataclass(frozen=True, slots=True)
class ResourceSourcePage:
    items: tuple[ResourceSource, ...]
    next_cursor: str | None


def _sequence(raw):
    seq = codec.decimal_u64(raw, "sequence", 1)
    return codec.integer(seq, "sequence", 1, 2**63 - 1)


def position(cursor, scope, name, state):
    if cursor is None:
        return None
    codec.text(cursor, "cursor", 4096)
    if not re.fullmatch(r"(?:[0-9a-f]{2})+", cursor):
        raise InputError("invalid resource source cursor")
    try:
        raw = codec.decode(bytes.fromhex(cursor), 2048)
    except ProtocolError as exc:
        raise InputError("invalid resource source cursor") from exc
    codec.fields(raw, {"v", "scope", "name", "state", "sequence"})
    if (type(raw["v"]) is not int or raw["v"] != 1 or _scope(raw["scope"]) != scope
            or raw["name"] != name or raw["state"] != state):
        raise InputError("resource source cursor does not match query")
    return _sequence(raw["sequence"])


def _subject(raw):
    if type(raw) is not dict:
        raise InputError("invalid resource source subject")
    if raw.get("kind") == "task":
        codec.fields(raw, {"kind", "task_id", "generation"})
        return TaskDecisionSubject(codec.text(raw["task_id"], "task_id"),
            codec.integer(raw["generation"], "generation", 1, 2**31 - 1))
    fields = ("workflow_id", "origin_activation_id", "key")
    if raw.get("kind") == "provider":
        fields += ("approval_activation_id", "approval_key")
        model = ProviderSourceSubject
    elif raw.get("kind") == "workflow":
        model = WorkflowSourceSubject
    else:
        raise InputError("unrecognized resource source subject")
    codec.fields(raw, {"kind", *fields})
    return model(*(codec.text(raw[key], key) for key in fields))


def parse_source_page(raw, scope, name, state, before, limit):
    codec.fields(raw, {"items", "next_cursor"})
    if type(raw["items"]) is not list or len(raw["items"]) > limit:
        raise InputError("invalid resource source page size")
    items = []
    for row in raw["items"]:
        codec.fields(row, {"scope", "resource", "sequence", "revision", "updated_at", "state", "subject"})
        sequence = _sequence(row["sequence"])
        source_state = ResourceSourceState(row["state"])
        if (_scope(row["scope"]) != scope or row["resource"] != name
                or state is not None and source_state != state
                or before is not None and sequence >= before):
            raise InputError("resource source page disagrees with query")
        revision = codec.decimal_u64(row["revision"], "revision", 1)
        items.append(ResourceSource(scope, name, sequence,
            codec.integer(revision, "revision", 1, 2**53 - 1), timestamp(row["updated_at"]),
            source_state, _subject(row["subject"])))
        before = sequence
    cursor = raw["next_cursor"]
    if cursor is not None and (not items or position(cursor, scope, name, state) != items[-1].sequence):
        raise InputError("resource source cursor disagrees with page")
    return ResourceSourcePage(tuple(items), cursor)
