"""Provider-neutral resource policies and durable admission inspection (MIT)."""

import json
import re
from dataclasses import asdict, dataclass, field
from enum import StrEnum

from . import codec
from .errors import InputError, ProtocolError
from .models import Scope, _scope

LIMIT = 256 * 1024
SAFE = (1 << 53) - 1
TIMESTAMP = 253402300799999


@dataclass(frozen=True)
class ResourceRate:
    numerator: int
    period_ms: int
    burst: int

    def __post_init__(self):
        codec.integer(self.numerator, "numerator", 1, 1_000_000)
        codec.integer(self.period_ms, "period_ms", 1, 86_400_000)
        codec.integer(self.burst, "burst", 1, 1_000_000)


@dataclass(frozen=True)
class ResourcePolicy:
    max_occupancy: int
    enabled: bool = True
    rate: ResourceRate | None = None
    offer_timeout_ms: int = 30_000
    start_timeout_ms: int = 5_000

    def __post_init__(self):
        codec.integer(self.max_occupancy, "max_occupancy", 1, 1_000_000)
        if (
            type(self.enabled) is not bool
            or self.rate is not None
            and type(self.rate) is not ResourceRate
        ):
            raise InputError("invalid resource policy")
        codec.integer(self.offer_timeout_ms, "offer_timeout_ms", 1_000, 300_000)
        codec.integer(self.start_timeout_ms, "start_timeout_ms", 100, 30_000)

    def to_dict(self):
        return asdict(self)


@dataclass(frozen=True)
class ResourceRecord:
    scope: Scope
    name: str
    revision: int
    policy: ResourcePolicy
    updated_at: int


@dataclass(frozen=True)
class ResourceSnapshot:
    resource: ResourceRecord
    offered: int
    occupied: int
    unknown: int
    quarantined: bool


@dataclass(frozen=True)
class ResourcePutReceipt:
    resource: ResourceRecord
    already_accepted: bool


@dataclass(frozen=True)
class ResourcePage:
    items: tuple[ResourceSnapshot, ...]
    next: str | None


@dataclass(frozen=True)
class AdmissionOccurrence:
    scope: Scope
    workflow_id: str
    origin_activation_id: str
    key: str

    def __post_init__(self):
        if type(self.scope) is not Scope:
            raise InputError("invalid admission scope")
        for name in ("workflow_id", "origin_activation_id", "key"):
            codec.text(getattr(self, name), name)

    def to_dict(self):
        return asdict(self)


class AdmissionState(StrEnum):
    WAITING = "waiting"
    OFFERED = "offered"
    AWAITING_REWAIT = "awaiting_rewait"
    POSSIBLY_DISPATCHED = "possibly_dispatched"
    UNKNOWN_OCCUPIED = "unknown_occupied"
    FINISHED = "finished"
    CLOSED_BEFORE_START = "closed_before_start"
    ADMINISTRATIVELY_RELEASED = "administratively_released"


@dataclass(frozen=True)
class AdmissionSnapshot:
    """Immutable sensitive inspection. Arguments/results are copied on access."""

    occurrence: AdmissionOccurrence
    state: AdmissionState
    revision: int
    _encoded: bytes = field(repr=False)

    def to_dict(self):
        return json.loads(self._encoded)

    @property
    def binding(self):
        return self.to_dict()["binding"]

    @property
    def source(self):
        return self.to_dict()["source"]

    @property
    def finish(self):
        return self.to_dict()["finish"]


@dataclass(frozen=True)
class AdmissionReleaseReceipt:
    occurrence: AdmissionOccurrence
    source_id: str
    revision: int
    accepted_at: int
    already_accepted: bool


def boolean(value):
    if type(value) is not bool:
        raise InputError("expected boolean")
    return value


def name(value):
    codec.text(value, "resource name")
    if re.fullmatch(r"[A-Za-z0-9_.:-]+", value) is None:
        raise InputError("invalid resource name")
    return value


def integer(value, low=0):
    return codec.integer(value, "admission integer", low, SAFE)


def timestamp(value):
    return codec.integer(value, "admission timestamp", 0, TIMESTAMP)


def digest(value):
    if type(value) is not str or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
        raise InputError("invalid admission digest")


def _record(raw, scope):
    codec.fields(raw, {"scope", "name", "revision", "policy", "updated_at"})
    if _scope(raw["scope"]) != scope:
        raise InputError("foreign resource scope")
    policy = raw["policy"]
    codec.fields(
        policy,
        {"enabled", "max_occupancy", "rate", "offer_timeout_ms", "start_timeout_ms"},
    )
    rate = policy["rate"]
    if rate is not None:
        codec.fields(rate, {"numerator", "period_ms", "burst"})
        rate = ResourceRate(**rate)
    return ResourceRecord(
        scope,
        name(raw["name"]),
        integer(raw["revision"], 1),
        ResourcePolicy(**dict(policy, rate=rate)),
        timestamp(raw["updated_at"]),
    )


def _resource(raw, scope):
    codec.fields(raw, {"resource", "offered", "occupied", "unknown", "quarantined"})
    record = _record(raw["resource"], scope)
    offered, occupied, unknown = (
        integer(raw[k]) for k in ("offered", "occupied", "unknown")
    )
    if offered + unknown > occupied:
        raise InputError("inconsistent resource occupancy")
    return ResourceSnapshot(
        record, offered, occupied, unknown, boolean(raw["quarantined"])
    )


def _occurrence(raw, expected):
    codec.fields(raw, {"scope", "workflow_id", "origin_activation_id", "key"})
    result = AdmissionOccurrence(
        _scope(raw["scope"]),
        raw["workflow_id"],
        raw["origin_activation_id"],
        raw["key"],
    )
    if result != expected:
        raise InputError("foreign admission occurrence")


def _source(raw, occurrence):
    codec.fields(
        raw,
        {
            "occurrence",
            "source_id",
            "start_id",
            "finish_id",
            "generation",
            "binding_digest",
            "owner",
            "worker",
            "accepted_at",
            "start_deadline_at",
        },
    )
    _occurrence(raw["occurrence"], occurrence)
    for key in ("source_id", "start_id", "finish_id"):
        codec.text(raw[key], key)
    integer(raw["generation"], 1)
    digest(raw["binding_digest"])
    owner = raw["owner"]
    codec.fields(
        owner,
        {
            "scope",
            "task_id",
            "attempt_id",
            "lease_id",
            "generation",
            "worker_session_id",
            "consumer_id",
        },
    )
    if _scope(owner["scope"]) != occurrence.scope:
        raise InputError("foreign admission owner")
    for key in ("task_id", "attempt_id", "lease_id", "worker_session_id"):
        codec.text(owner[key], key)
    codec.integer(owner["generation"], "generation", 1, (1 << 32) - 1)
    codec.integer(owner["consumer_id"], "consumer_id", 0, (1 << 32) - 1)
    worker = raw["worker"]
    codec.fields(worker, {"audience", "issuer", "subject", "credential_id"})
    for key in ("issuer", "subject", "credential_id"):
        codec.text(worker[key], key, 256 if key == "issuer" else 128)
    audience = worker["audience"]
    codec.fields(audience, {"authority_id", "instance_id", "scope"})
    for key in ("authority_id", "instance_id"):
        codec.text(audience[key], key)
    if _scope(audience["scope"]) != occurrence.scope:
        raise InputError("foreign admission audience")
    if (
        not 1
        <= timestamp(raw["start_deadline_at"]) - timestamp(raw["accepted_at"])
        <= 30000
    ):
        raise InputError("invalid admission dispatch window")


def _snapshot(raw, occurrence):
    codec.fields(
        raw,
        {
            "occurrence",
            "binding",
            "state",
            "revision",
            "created_at",
            "updated_at",
            "deadline_at",
            "offer",
            "source",
            "finish",
        },
    )
    _occurrence(raw["occurrence"], occurrence)
    state = AdmissionState(raw["state"])
    integer(raw["revision"], 1)
    binding = raw["binding"]
    codec.fields(binding, {"resource", "program", "operation"})
    name(binding["resource"])
    program = binding["program"]
    codec.fields(program, {"program", "digest", "size"})
    digest(program["digest"])
    integer(program["size"], 1)
    codec.fields(program["program"], {"id", "version"})
    for key in ("id", "version"):
        codec.text(program["program"][key], key)
    operation = binding["operation"]
    codec.fields(operation, {"callable", "kind", "version", "arguments"})
    codec.text(operation["callable"], "callable", 512)
    codec.text(operation["version"], "version")
    if (
        operation["kind"] not in ("model", "tool")
        or type(operation["arguments"]) is not dict
    ):
        raise InputError("invalid admission operation")
    codec.validate_authoritative(operation["arguments"], 64 * 1024)
    created, updated, deadline = (
        timestamp(raw[k]) for k in ("created_at", "updated_at", "deadline_at")
    )
    if min(updated, deadline) < created:
        raise InputError("invalid admission timestamps")
    offer, source, finish = (raw[k] for k in ("offer", "source", "finish"))
    if offer is not None:
        codec.fields(
            offer,
            {
                "occurrence",
                "generation",
                "policy_revision",
                "activation_id",
                "expires_at",
                "binding_digest",
            },
        )
        _occurrence(offer["occurrence"], occurrence)
        integer(offer["generation"], 1)
        integer(offer["policy_revision"], 1)
        codec.text(offer["activation_id"], "activation_id")
        digest(offer["binding_digest"])
        if timestamp(offer["expires_at"]) > deadline:
            raise InputError("offer exceeds deadline")
    if source is not None:
        _source(source, occurrence)
        if (
            not created <= source["accepted_at"] < deadline
            or source["accepted_at"] > updated
        ):
            raise InputError("source outside admission lifetime")
    if finish is not None:
        codec.fields(finish, {"source", "output", "accepted_at", "already_accepted"})
        boolean(finish["already_accepted"])
        if (
            source is None
            or finish["source"] != source
            or not source["accepted_at"] <= timestamp(finish["accepted_at"]) <= updated
        ):
            raise InputError("foreign admission finish")
        codec.validate_authoritative(finish["output"], 64 * 1024)
    shape = (offer is not None, source is not None, finish is not None)
    allowed = {
        AdmissionState.WAITING: (False, False, False),
        AdmissionState.AWAITING_REWAIT: (False, False, False),
        AdmissionState.CLOSED_BEFORE_START: (False, False, False),
        AdmissionState.OFFERED: (True, False, False),
        AdmissionState.POSSIBLY_DISPATCHED: (False, True, False),
        AdmissionState.UNKNOWN_OCCUPIED: (False, True, False),
        AdmissionState.FINISHED: (False, True, True),
    }
    if state == AdmissionState.ADMINISTRATIVELY_RELEASED:
        if shape[:2] != (False, True):
            raise InputError("invalid released admission")
    elif shape != allowed[state]:
        raise InputError("inconsistent admission state")
    return AdmissionSnapshot(
        occurrence, state, raw["revision"], codec.encode(raw, LIMIT, max_depth=96)
    )


def parsed(raw, fn):
    try:
        return fn(raw)
    except (InputError, ValueError, TypeError, KeyError) as exc:
        raise ProtocolError("invalid admission response") from exc
