"""Cluster-wide queue concurrency and operator reconciliation."""

from dataclasses import asdict, dataclass, field, replace
from enum import StrEnum

from . import codec
from .admission_models import boolean, timestamp
from .admissions import _exchange
from .errors import InputError
from .models import Scope, _scope

U64 = (1 << 64) - 1
MAX_DURATION = (1 << 31) - 1


@dataclass(frozen=True, slots=True)
class QueueHoldTimeout:
    after_ms: int
    force_after_grace_ms: int | None = None

    def __post_init__(self):
        codec.integer(self.after_ms, "after_ms", 1, MAX_DURATION)
        if self.force_after_grace_ms is not None:
            codec.integer(self.force_after_grace_ms, "grace_ms", 1, MAX_DURATION)

    def to_dict(self):
        mode = (
            {"mode": "confirm"}
            if self.force_after_grace_ms is None
            else {"mode": "after_grace", "grace_ms": self.force_after_grace_ms}
        )
        return {"after_ms": self.after_ms, "release": mode}


@dataclass(frozen=True, slots=True)
class QueueConcurrencyPolicy:
    max_concurrency: int | None
    timeout: QueueHoldTimeout | None = None

    def __post_init__(self):
        if self.max_concurrency is not None:
            codec.integer(self.max_concurrency, "max_concurrency", 0, 1_000_000)
        if self.timeout is not None and type(self.timeout) is not QueueHoldTimeout:
            raise InputError("timeout must be QueueHoldTimeout")

    def to_dict(self):
        return {
            "max_concurrency": self.max_concurrency,
            "timeout": None if self.timeout is None else self.timeout.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class QueuePolicyRecord:
    scope: Scope
    queue: str
    revision: int
    policy: QueueConcurrencyPolicy
    updated_at: int


@dataclass(frozen=True, slots=True)
class QueuePolicySnapshot:
    record: QueuePolicyRecord
    occupied: int
    uncertain: int


class QueueHoldState(StrEnum):
    RESERVED = "reserved"
    RUNNING = "running"
    UNCERTAIN = "uncertain"
    RELEASED = "released"


class QueueHoldReleaseKind(StrEnum):
    CONFIRMED = "confirmed"
    NEVER_STARTED = "never_started"
    OPERATOR = "operator"
    TIMEOUT = "timeout"


@dataclass(frozen=True, slots=True)
class QueueHold:
    scope: Scope
    queue: str
    task_id: str
    attempt_id: str
    worker_session_id: str
    consumer_id: int
    revision: int
    policy_revision: int
    grace_ms: int | None
    force_at: int | None
    state: QueueHoldState
    acquired_at: int
    deadline: int | None
    stop_requested_at: int | None
    released_at: int | None
    release_kind: QueueHoldReleaseKind | None
    _base_url: str = field(repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class QueuePolicyPage:
    items: tuple[QueuePolicySnapshot, ...]
    next: str | None


@dataclass(frozen=True, slots=True)
class QueueHoldPage:
    items: tuple[QueueHold, ...]
    next: str | None


@dataclass(frozen=True, slots=True)
class QueuePolicyReceipt:
    record: QueuePolicyRecord
    already_accepted: bool


@dataclass(frozen=True, slots=True)
class QueueHoldReceipt:
    hold: QueueHold
    already_accepted: bool


def _policy(value):
    codec.fields(value, {"max_concurrency", "timeout"})
    timeout = value["timeout"]
    if timeout is not None:
        codec.fields(timeout, {"after_ms", "release"})
        release = timeout["release"]
        if type(release) is not dict or release.get("mode") not in {
            "confirm",
            "after_grace",
        }:
            raise InputError("unknown queue timeout release mode")
        codec.fields(
            release, {"mode"} if release["mode"] == "confirm" else {"mode", "grace_ms"}
        )
        if release["mode"] == "after_grace":
            codec.integer(release["grace_ms"], "grace_ms", 1, MAX_DURATION)
        timeout = QueueHoldTimeout(timeout["after_ms"], release.get("grace_ms"))
    return QueueConcurrencyPolicy(value["max_concurrency"], timeout)


def _record(raw, scope):
    codec.fields(raw, {"scope", "queue", "revision", "policy", "updated_at"})
    if _scope(raw["scope"]) != scope:
        raise InputError("foreign queue scope")
    return QueuePolicyRecord(
        scope,
        codec.text(raw["queue"], "queue"),
        codec.decimal_u64(raw["revision"], "revision", 1),
        _policy(raw["policy"]),
        timestamp(raw["updated_at"]),
    )


def _snapshot(raw, scope):
    codec.fields(raw, {"record", "occupied", "uncertain"})
    result = QueuePolicySnapshot(
        _record(raw["record"], scope),
        codec.decimal_u64(raw["occupied"], "occupied"),
        codec.decimal_u64(raw["uncertain"], "uncertain"),
    )
    if result.uncertain > result.occupied:
        raise InputError("uncertain count exceeds occupancy")
    return result


def _hold(raw, client, queue):
    codec.fields(
        raw,
        {
            "scope",
            "queue",
            "task_id",
            "attempt_id",
            "worker_session_id",
            "consumer_id",
            "revision",
            "state",
            "acquired_at",
            "deadline",
            "stop_requested_at",
            "released_at",
            "release_kind",
            "policy_revision",
            "grace_ms",
            "force_at",
        },
    )
    if _scope(raw["scope"]) != client.scope or raw["queue"] != queue:
        raise InputError("foreign queue hold")
    state = QueueHoldState(raw["state"])
    kind = (
        None
        if raw["release_kind"] is None
        else QueueHoldReleaseKind(raw["release_kind"])
    )
    acquired = timestamp(raw["acquired_at"])
    deadline, stop, released = (
        None if raw[k] is None else timestamp(raw[k])
        for k in ("deadline", "stop_requested_at", "released_at")
    )
    policy_revision = codec.decimal_u64(raw["policy_revision"], "policy_revision", 1)
    grace = raw["grace_ms"]
    if grace is not None:
        codec.integer(grace, "grace_ms", 1, MAX_DURATION)
        if deadline is None:
            raise InputError("forced release requires a deadline")
    force_at = None if raw["force_at"] is None else timestamp(raw["force_at"])
    if force_at != (stop + grace if stop is not None and grace is not None else None):
        raise InputError(
            "forced release time does not match the recorded stop and grace"
        )
    if (
        (state == QueueHoldState.RELEASED) != (released is not None)
        or (released is None) != (kind is None)
        or any(t is not None and t < acquired for t in (deadline, stop, released))
        or (stop is not None and (deadline is None or stop < deadline))
        or (stop is not None and released is not None and stop > released)
        or (
            kind == QueueHoldReleaseKind.TIMEOUT
            and (force_at is None or released is None or released < force_at)
        )
    ):
        raise InputError("inconsistent queue hold lifecycle")
    return QueueHold(
        client.scope,
        queue,
        codec.text(raw["task_id"], "task_id"),
        codec.text(raw["attempt_id"], "attempt_id"),
        codec.text(raw["worker_session_id"], "worker_session_id"),
        codec.integer(raw["consumer_id"], "consumer_id", 0, (1 << 32) - 1),
        codec.decimal_u64(raw["revision"], "revision", 1),
        policy_revision,
        grace,
        force_at,
        state,
        acquired,
        deadline,
        stop,
        released,
        kind,
        client.base_url,
    )


def _page(raw, limit, after, parser, identity):
    codec.fields(raw, {"items", "next"})
    if type(raw["items"]) is not list or len(raw["items"]) > limit:
        raise InputError("invalid queue policy page")
    items = tuple(parser(item) for item in raw["items"])
    last = after
    for item in items:
        next_key = identity(item)
        if last is not None and last >= next_key:
            raise InputError("invalid queue cursor order")
        last = next_key
    if raw["next"] is not None and (not items or raw["next"] != last):
        raise InputError("invalid next queue cursor")
    return items, raw["next"]


class QueuePolicies:
    def __init__(self, client):
        self._client = client

    async def _call(self, operation, body, parse):
        return await _exchange(
            self._client,
            "/v1/queue-policies/" + operation,
            {"scope": asdict(self._client.scope), **body},
            parse,
            limit=1024 * 1024,
        )

    async def get(self, queue: str) -> QueuePolicySnapshot:
        codec.text(queue, "queue")

        def parse(raw):
            result = _snapshot(raw, self._client.scope)
            if result.record.queue != queue:
                raise InputError("queue inspection returned a different queue")
            return result

        return await self._call("inspect", {"queue": queue}, parse)

    async def put(
        self,
        queue: str,
        *,
        policy: QueueConcurrencyPolicy,
        request_id: str,
        reason: str,
        expected_revision: int | None = None,
    ) -> QueuePolicyReceipt:
        codec.text(queue, "queue")
        codec.text(reason, "reason", 1024)
        codec.text(request_id, "request_id")
        if type(policy) is not QueueConcurrencyPolicy:
            raise InputError("policy must be QueueConcurrencyPolicy")
        if expected_revision is not None:
            codec.integer(expected_revision, "expected_revision", 1, U64 - 1)

        def parse(raw):
            codec.fields(raw, {"record", "already_accepted"})
            record = _record(raw["record"], self._client.scope)
            if (record.queue, record.policy, record.revision) != (
                queue,
                policy,
                (expected_revision or 0) + 1,
            ):
                raise InputError("queue policy receipt mismatch")
            return QueuePolicyReceipt(record, boolean(raw["already_accepted"]))

        return await self._call(
            "put",
            {
                "queue": queue,
                "policy": policy.to_dict(),
                "request_id": request_id,
                "reason": reason,
                "expected_revision": None
                if expected_revision is None
                else str(expected_revision),
            },
            parse,
        )

    async def list(
        self, *, after: str | None = None, limit: int = 50
    ) -> QueuePolicyPage:
        codec.integer(limit, "limit", 1, 100)
        if after is not None:
            codec.text(after, "after")

        def parse(raw):
            return QueuePolicyPage(
                *_page(
                    raw,
                    limit,
                    after,
                    lambda r: _snapshot(r, self._client.scope),
                    lambda s: s.record.queue,
                )
            )

        return await self._call("list", {"after": after, "limit": limit}, parse)

    async def holds(
        self,
        queue: str,
        *,
        after: str | None = None,
        limit: int = 50,
        include_released: bool = False,
    ) -> QueueHoldPage:
        codec.text(queue, "queue")
        codec.integer(limit, "limit", 1, 100)
        boolean(include_released)
        if after is not None:
            codec.text(after, "after")

        def parse(raw):
            items, next_key = _page(
                raw,
                limit,
                after,
                lambda r: _hold(r, self._client, queue),
                lambda h: h.attempt_id,
            )
            if not include_released and any(
                h.state == QueueHoldState.RELEASED for h in items
            ):
                raise InputError("unexpected released queue hold")
            return QueueHoldPage(items, next_key)

        return await self._call(
            "holds",
            {
                "queue": queue,
                "after": after,
                "limit": limit,
                "include_released": include_released,
            },
            parse,
        )

    async def release(
        self, inspected: QueueHold, *, request_id: str, reason: str
    ) -> QueueHoldReceipt:
        if (
            type(inspected) is not QueueHold
            or inspected.scope != self._client.scope
            or inspected._base_url != self._client.base_url
            or inspected.state != QueueHoldState.UNCERTAIN
        ):
            raise InputError(
                "use an uncertain hold inspected on this client destination and scope"
            )
        codec.text(reason, "reason", 1024)
        codec.text(request_id, "request_id")
        codec.integer(inspected.revision, "expected_revision", 1, U64 - 1)

        def parse(raw):
            codec.fields(raw, {"hold", "already_accepted"})
            hold = _hold(raw["hold"], self._client, inspected.queue)
            expected = replace(
                inspected,
                state=QueueHoldState.RELEASED,
                revision=inspected.revision + 1,
                release_kind=QueueHoldReleaseKind.OPERATOR,
                released_at=hold.released_at,
            )
            if hold != expected:
                raise InputError("queue release receipt mismatch")
            return QueueHoldReceipt(hold, boolean(raw["already_accepted"]))

        return await self._call(
            "release",
            {
                "queue": inspected.queue,
                "task_id": inspected.task_id,
                "attempt_id": inspected.attempt_id,
                "expected_revision": str(inspected.revision),
                "request_id": request_id,
                "reason": reason,
            },
            parse,
        )
