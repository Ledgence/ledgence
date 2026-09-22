"""Bounded completion observations and immutable reconciliation commands."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Literal
from urllib.parse import quote, unquote_to_bytes

from . import codec
from .cloud_events import _validate_event_context
from .errors import InputError, ProtocolError
from .models import Scope, _scope, _optional_time

COMPLETION_COMMAND_LIMIT = 4096
COMPLETION_STATUS_LIMIT = 24 * 1024
COMPLETION_EVENT_LIMIT = 16 * 1024


@dataclass(frozen=True)
class CompletionTarget:
    kind: Literal["task", "workflow"]
    id: str

    def __post_init__(self):
        if type(self.kind) is not str or self.kind not in ("task", "workflow"):
            raise InputError("completion target kind must be task or workflow")
        codec.text(self.id, "target.id")


class CompletionState(StrEnum):
    WAITING = "waiting"
    PENDING = "pending"
    DELIVERING = "delivering"
    RETRYING = "retrying"
    DELIVERED = "delivered"
    EXHAUSTED = "exhausted"


@dataclass(frozen=True, init=False)
class CompletionSubscribeCommand:
    """Frozen registration bound to one endpoint and scoped execution."""
    base_url: str
    scope: Scope
    target: CompletionTarget
    destination: str
    idempotency_key: str
    _body: bytes = field(repr=False)

    def __init__(self):
        raise TypeError("prepare commands with task.prepare_subscribe() or client.completions.prepare()")

    @classmethod
    def _create(cls, base_url, scope, target, destination, idempotency_key, body):
        value = object.__new__(cls)
        for name, item in (("base_url", base_url), ("scope", scope), ("target", target),
                           ("destination", destination), ("idempotency_key", idempotency_key),
                           ("_body", body)):
            object.__setattr__(value, name, item)
        return value

    def to_dict(self) -> dict:
        """Return an independent JSON copy suitable for durable storage."""
        return codec.decode(self._body, COMPLETION_COMMAND_LIMIT)


@dataclass(frozen=True, init=False)
class CompletionRetryCommand:
    """Frozen retry of a single exhausted generation; repeats do not rearm again."""
    base_url: str
    scope: Scope
    subscription_id: str
    expected_generation: int
    _body: bytes = field(repr=False)

    def __init__(self):
        raise TypeError("prepare retry commands with subscription.prepare_retry()")

    @classmethod
    def _create(cls, base_url, scope, subscription_id, expected_generation, body):
        value = object.__new__(cls)
        for name, item in (("base_url", base_url), ("scope", scope),
                           ("subscription_id", subscription_id),
                           ("expected_generation", expected_generation), ("_body", body)):
            object.__setattr__(value, name, item)
        return value

    def to_dict(self) -> dict:
        return codec.decode(self._body, COMPLETION_COMMAND_LIMIT)


@dataclass(frozen=True)
class CompletionSubscription:
    subscription_id: str
    command: CompletionSubscribeCommand
    state: CompletionState
    generation: int
    attempts: int
    total_attempts: int
    created_at: int
    activated_at: int | None
    next_attempt_at: int | None
    lease_expires_at: int | None
    delivered_at: int | None
    exhausted_at: int | None
    last_failure: str | None
    event: dict[str, Any] | None = field(repr=False)


def _parse_command(raw, base_url, scope):
    codec.fields(raw, {"scope", "target", "destination", "idempotency_key"})
    if _scope(raw["scope"]) != scope:
        raise InputError("completion subscription scope differs")
    codec.fields(raw["target"], {"kind", "id"})
    target = CompletionTarget(**raw["target"])
    destination = codec.text(raw["destination"], "destination")
    key = codec.text(raw["idempotency_key"], "idempotency_key")
    return CompletionSubscribeCommand._create(base_url, scope, target, destination, key,
                                              codec.encode(raw, COMPLETION_COMMAND_LIMIT))


def _event(raw, scope, target):
    codec.encode(raw, COMPLETION_EVENT_LIMIT)
    _validate_event_context(raw)
    required = {"specversion", "id", "source", "type", "subject", "time", "ldgtenantid",
                "ldgnamespace", "ldgstate", "ldgresultref"}
    optional = {"ldgtaskid", "ldgrunid", "ldgattemptid", "ldgworkflowid", "ldgactivationid",
                "ldgparentworkflowid", "ldgrootworkflowid", "ldgcorrelationkey",
                "ldgcorrelationkeyencoding", "traceparent", "tracestate"}
    codec.fields(raw, required, optional)
    for key in ("id", "subject", "time", "ldgtenantid", "ldgnamespace", "ldgstate", "ldgresultref"):
        codec.text(raw[key], key, 2048 if key == "ldgresultref" else 256)
    expected = {"source": "urn:ledgence:orchestrator",
                "id": f"evt_{target.kind}_completed_{target.id}",
                "type": f"com.ledgence.{target.kind}.completed.v1",
                "subject": f"{target.kind}s/{target.id}",
                "ldgtenantid": scope.tenant_id, "ldgnamespace": scope.namespace,
                f"ldg{target.kind}id": target.id,
                "ldgresultref": f"/v1/{target.kind}s/result?tenant_id={quote(scope.tenant_id, safe='-._~')}"
                                f"&namespace={quote(scope.namespace, safe='-._~')}"
                                f"&{target.kind}_id={quote(target.id, safe='-._~')}"}
    if any(raw.get(key) != value for key, value in expected.items()):
        raise InputError("completion event identity differs")
    if raw["ldgstate"] not in ("succeeded", "failed", "cancelled"):
        raise InputError("completion event is not terminal")
    if target.kind == "task":
        codec.text(raw.get("ldgrunid"), "ldgrunid")
    elif any(key in raw for key in ("ldgtaskid", "ldgrunid", "ldgattemptid", "ldgactivationid")):
        raise InputError("workflow completion impersonates an activation task")
    for key in optional - {"ldgcorrelationkey", "ldgcorrelationkeyencoding", "traceparent", "tracestate"}:
        if key in raw:
            codec.text(raw[key], key)
    parent, root = raw.get("ldgparentworkflowid"), raw.get("ldgrootworkflowid")
    workflow = raw.get("ldgworkflowid")
    if ((parent is None) != (root is None)
            or (parent is not None and (workflow is None or workflow in (parent, root)))
            or ("ldgactivationid" in raw and (workflow is None or raw["ldgactivationid"] != raw.get("ldgtaskid")))
            or (raw["ldgstate"] == "cancelled" and "ldgattemptid" in raw)):
        raise InputError("invalid completion event lineage or deciding attempt")
    if "ldgcorrelationkeyencoding" in raw:
        encoded = codec.text(raw.get("ldgcorrelationkey"), "ldgcorrelationkey", 1536, empty=True)
        if raw["ldgcorrelationkeyencoding"] != "percent":
            raise InputError("unknown completion correlation encoding")
        decoded = unquote_to_bytes(encoded).decode("utf-8")
        codec.text(decoded, "correlation key", 512, empty=True, noncharacters=True)
        if quote(decoded, safe="-._~") != encoded:
            raise InputError("noncanonical completion correlation encoding")
    elif "ldgcorrelationkey" in raw:
        codec.text(raw["ldgcorrelationkey"], "ldgcorrelationkey", 512, empty=True)
    return raw


def parse_completion_subscription(raw, base_url, scope, *, subscription_id=None,
                                  expected_command=None, expected_generation=None):
    try:
        codec.fields(raw, set(CompletionSubscription.__dataclass_fields__))
        identity = codec.text(raw["subscription_id"], "subscription_id")
        if subscription_id is not None and identity != subscription_id:
            raise InputError("completion subscription identity differs")
        command = _parse_command(raw["command"], base_url, scope)
        if expected_command is not None and command.to_dict() != expected_command.to_dict():
            raise InputError("completion subscription binding differs")
        state = CompletionState(raw["state"])
        generation = codec.integer(raw["generation"], "generation", 1, 1000)
        if expected_generation is not None and generation <= expected_generation:
            raise InputError("completion retry did not acknowledge generation")
        attempts = codec.integer(raw["attempts"], "attempts", 0, 8)
        total = codec.integer(raw["total_attempts"], "total_attempts", attempts, generation * 8)
        created = codec.integer(raw["created_at"], "created_at", 0, codec.MAX_TIMESTAMP)
        times = {key: _optional_time(raw[key], key) for key in (
            "activated_at", "next_attempt_at", "lease_expires_at", "delivered_at", "exhausted_at")}
        if any(value is not None and value < created for value in times.values()):
            raise InputError("completion timestamp precedes registration")
        failure = raw["last_failure"]
        if failure is not None:
            codec.text(failure, "last_failure", 256)
        event = None if raw["event"] is None else _event(raw["event"], scope, command.target)
        active = event is not None and times["activated_at"] is not None
        next_at, lease = times["next_attempt_at"], times["lease_expires_at"]
        delivered, exhausted = times["delivered_at"], times["exhausted_at"]
        no_terminal = delivered is None and exhausted is None
        valid = {
            CompletionState.WAITING: event is None and times["activated_at"] is None
                and next_at is None and lease is None and no_terminal and attempts == 0
                and total == 0 and generation == 1 and failure is None,
            CompletionState.PENDING: active and attempts == 0 and next_at is not None
                and lease is None and no_terminal,
            CompletionState.RETRYING: active and 1 <= attempts < 8 and next_at is not None
                and lease is None and no_terminal,
            CompletionState.DELIVERING: active and attempts > 0 and next_at is None
                and lease is not None and no_terminal,
            CompletionState.DELIVERED: active and attempts > 0 and next_at is None
                and lease is None and delivered is not None and exhausted is None,
            CompletionState.EXHAUSTED: active and attempts == 8 and next_at is None
                and lease is None and delivered is None and exhausted is not None,
        }[state]
        if not valid:
            raise InputError("inconsistent completion subscription state")
        return CompletionSubscription(identity, command, state, generation, attempts, total,
                                      created, **times, last_failure=failure, event=event)
    except (ValueError, TypeError, KeyError, UnicodeError) as exc:
        raise ProtocolError("invalid completion subscription response") from exc
