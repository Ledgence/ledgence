"""Source-bound inspection and operator release of standalone task admission."""
from dataclasses import asdict, dataclass, field
from enum import StrEnum
import hashlib
import json

from . import codec
from .admission_models import LIMIT, SAFE, boolean, digest, integer, name, timestamp
from .admissions import _exchange
from .errors import InputError
from .models import Scope, _scope


@dataclass(frozen=True)
class TaskAdmissionOccurrence:
    scope: Scope
    task_id: str
    generation: int

    def __post_init__(self):
        if type(self.scope) is not Scope:
            raise InputError("invalid task admission scope")
        codec.text(self.task_id, "task_id")
        codec.integer(self.generation, "generation", 1, (1 << 31) - 1)

    def to_dict(self):
        return asdict(self)


class TaskAdmissionState(StrEnum):
    WAITING = "waiting"
    OFFERED = "offered"
    PREPARING = "preparing"
    POSSIBLY_DISPATCHED = "possibly_dispatched"
    UNKNOWN_OCCUPIED = "unknown_occupied"
    FINISHED = "finished"
    CLOSED_BEFORE_START = "closed_before_start"
    ADMINISTRATIVELY_RELEASED = "administratively_released"


@dataclass(frozen=True)
class TaskAdmissionSnapshot:
    occurrence: TaskAdmissionOccurrence
    resource: str
    state: TaskAdmissionState
    revision: int
    _encoded: bytes = field(repr=False)
    _base_url: str | None = field(default=None, repr=False)

    def to_dict(self):
        return json.loads(self._encoded)

    @property
    def source(self):
        return self.to_dict()["source"]

    @property
    def offer(self):
        return self.to_dict()["offer"]

    @property
    def close(self):
        return self.to_dict()["close"]


@dataclass(frozen=True)
class TaskAdmissionReleaseReceipt:
    snapshot: TaskAdmissionSnapshot
    already_accepted: bool


def occurrence(raw, expected=None):
    codec.fields(raw, {"scope", "task_id", "generation"})
    value = TaskAdmissionOccurrence(_scope(raw["scope"]), raw["task_id"], raw["generation"])
    if expected is not None and value != expected:
        raise InputError("foreign task admission occurrence")
    return value


def _owner(raw, expected):
    codec.fields(raw, {"scope", "task_id", "attempt_id", "lease_id", "generation",
                       "worker_session_id", "consumer_id"})
    if _scope(raw["scope"]) != expected.scope or raw["task_id"] != expected.task_id:
        raise InputError("foreign task admission owner")
    if codec.integer(raw["generation"], "generation", 1, (1 << 31) - 1) != expected.generation:
        raise InputError("foreign task admission generation")
    for key in ("task_id", "attempt_id", "lease_id", "worker_session_id"):
        codec.text(raw[key], key)
    codec.integer(raw["consumer_id"], "consumer_id", 0, (1 << 32) - 1)


def _source(raw, expected, resource):
    codec.fields(raw, {"owner", "offer_generation", "resource", "binding_digest", "start_id",
                       "source_id", "worker", "accepted_at", "start_deadline_at"})
    _owner(raw["owner"], expected)
    integer(raw["offer_generation"], 1)
    if name(raw["resource"]) != resource:
        raise InputError("foreign task admission resource")
    digest(raw["binding_digest"])
    for key in ("start_id", "source_id"):
        codec.text(raw[key], key)
    worker = raw["worker"]
    codec.fields(worker, {"audience", "issuer", "subject", "credential_id"})
    for key in ("issuer", "subject", "credential_id"):
        codec.text(worker[key], key, 256 if key == "issuer" else 128)
    audience = worker["audience"]
    codec.fields(audience, {"authority_id", "instance_id", "scope"})
    for key in ("authority_id", "instance_id"):
        codec.text(audience[key], key)
    if _scope(audience["scope"]) != expected.scope:
        raise InputError("foreign task admission audience")
    if not 1 <= timestamp(raw["start_deadline_at"]) - timestamp(raw["accepted_at"]) <= 30000:
        raise InputError("invalid task invocation window")


def snapshot(raw, expected, *, base_url=None):
    codec.fields(raw, {"occurrence", "resource", "program", "state", "revision", "created_at",
                       "updated_at", "offer", "owner", "source", "close"})
    occurrence(raw["occurrence"], expected)
    resource = name(raw["resource"])
    state = TaskAdmissionState(raw["state"])
    revision = integer(raw["revision"], 1)
    created = timestamp(raw["created_at"])
    updated = timestamp(raw["updated_at"])
    if updated < created:
        raise InputError("reversed task admission timestamps")
    program = raw["program"]
    codec.fields(program, {"program", "digest", "size"})
    codec.fields(program["program"], {"id", "version"})
    for key in ("id", "version"):
        codec.program_component(program["program"][key], key)
    digest(program["digest"])
    codec.integer(program["size"], "size", 1)
    offer, owner, source, close = (raw[k] for k in ("offer", "owner", "source", "close"))
    if offer is not None:
        codec.fields(offer, {"generation", "policy_revision", "expires_at"})
        integer(offer["generation"], 1)
        integer(offer["policy_revision"], 1)
        timestamp(offer["expires_at"])
    if owner is not None:
        _owner(owner, expected)
    if source is not None:
        _source(source, expected, resource)
        binding = {"occurrence": expected.to_dict(), "resource": resource, "program": program}
        canonical = json.dumps(binding, ensure_ascii=False, allow_nan=False,
                               separators=(",", ":"), sort_keys=True).encode()
        if source["binding_digest"] != "sha256:" + hashlib.sha256(canonical).hexdigest():
            raise InputError("task admission binding digest mismatch")
        if owner != source["owner"] or offer is None or offer["generation"] != source["offer_generation"]:
            raise InputError("task admission source mismatch")
        if (source["accepted_at"] >= offer["expires_at"]
                or source["start_deadline_at"] > offer["expires_at"]
                or not created <= source["accepted_at"] <= updated):
            raise InputError("task admission source timestamp mismatch")
    if close is not None:
        codec.fields(close, {"owner", "offer_generation", "start_id", "operation_id", "outcome",
                             "source", "disposition", "accepted_at", "already_accepted"})
        _owner(close["owner"], expected)
        for key in ("start_id", "operation_id"):
            codec.text(close[key], key)
        integer(close["offer_generation"], 1)
        if boolean(close["already_accepted"]):
            raise InputError("task admission history must retain the original close")
        if (owner != close["owner"] or close["source"] != source or offer is None
                or offer["generation"] != close["offer_generation"]
                or not created <= timestamp(close["accepted_at"]) <= updated):
            raise InputError("task admission close mismatch")
        if source is not None and (source["start_id"] != close["start_id"]
                                   or close["accepted_at"] < source["accepted_at"]):
            raise InputError("task admission close source mismatch")
        outcome, disposition = close["outcome"], close["disposition"]
        if outcome not in {"not_invoked", "quiescent"}:
            raise InputError("invalid task admission close outcome")
        valid = {"deferred": source is None and outcome == "not_invoked",
                 "charged_not_invoked": source is not None and outcome == "not_invoked",
                 "released": source is not None and outcome == "quiescent",
                 "already_released": source is not None, "closed": source is None}
        if not valid.get(disposition, False):
            raise InputError("invalid task admission close disposition")
    valid = {
        TaskAdmissionState.WAITING: offer is None and owner is None and source is None and close is None,
        TaskAdmissionState.OFFERED: offer is not None and owner is None and source is None and close is None,
        TaskAdmissionState.PREPARING: offer is not None and owner is not None and source is None and close is None,
        TaskAdmissionState.POSSIBLY_DISPATCHED: source is not None and close is None,
        TaskAdmissionState.UNKNOWN_OCCUPIED: source is not None and close is None,
        TaskAdmissionState.FINISHED: source is not None and close is not None
        and close["disposition"] in {"released", "charged_not_invoked"},
        TaskAdmissionState.CLOSED_BEFORE_START: source is None and (owner is None) == (offer is None)
        and (close is None or close["disposition"] in {"closed", "deferred"}),
        TaskAdmissionState.ADMINISTRATIVELY_RELEASED: source is not None
        and (close is None or close["disposition"] == "already_released"),
    }
    if not valid[state]:
        raise InputError("inconsistent task admission state")
    return TaskAdmissionSnapshot(expected, resource, state, revision, codec.encode(raw, LIMIT, max_depth=96), base_url)


class TaskAdmissions:
    def __init__(self, client):
        self._client = client

    async def inspect(self, task_id: str, *, generation: int) -> TaskAdmissionSnapshot:
        """Inspect one immutable attempt generation; the initial queued generation is 1."""
        address = TaskAdmissionOccurrence(self._client.scope, task_id, generation)
        return await _exchange(self._client, "/v1/task-admissions/inspect",
                               {"occurrence": address.to_dict()}, lambda raw: snapshot(raw, address, base_url=self._client.base_url))

    async def release(self, inspected: TaskAdmissionSnapshot, *, request_id: str,
                      reason: str) -> TaskAdmissionReleaseReceipt:
        """Explicitly release an inspected source. This does not stop or undo remote work."""
        if (type(inspected) is not TaskAdmissionSnapshot
                or inspected.occurrence.scope != self._client.scope
                or inspected._base_url != self._client.base_url):
            raise InputError("release requires a snapshot from this endpoint and scope")
        source = inspected.source
        if source is None or inspected.state not in {
            TaskAdmissionState.POSSIBLY_DISPATCHED, TaskAdmissionState.UNKNOWN_OCCUPIED,
        }:
            raise InputError("release requires an occupied task admission source")
        codec.text(request_id, "request_id")
        codec.text(reason, "reason", 1024)
        codec.integer(inspected.revision, "revision", 1, SAFE - 1)
        command = {"occurrence": inspected.occurrence.to_dict(), "source_id": source["source_id"],
                   "expected_revision": inspected.revision, "request_id": request_id, "reason": reason}

        def parse(raw):
            codec.fields(raw, {"snapshot", "already_accepted"})
            result = snapshot(raw["snapshot"], inspected.occurrence, base_url=self._client.base_url)
            if (result.state != TaskAdmissionState.ADMINISTRATIVELY_RELEASED
                    or result.revision != inspected.revision + 1
                    or result.source != source):
                raise InputError("task admission release receipt mismatch")
            return TaskAdmissionReleaseReceipt(result, boolean(raw["already_accepted"]))

        return await _exchange(self._client, "/v1/task-admissions/release", command, parse)
