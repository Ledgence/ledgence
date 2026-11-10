"""Bounded, resumable observations of one execution's durable lifecycle."""
from __future__ import annotations

import asyncio
from asyncio import sleep as _sleep
from dataclasses import dataclass
from enum import StrEnum
import re
from typing import AsyncIterator, TYPE_CHECKING

from . import codec
from .errors import InputError, LedgenceError, ProtocolError, RequestTimeout
from .models import Scope, TaskState, _scope
from .workflow_models import WorkflowState

if TYPE_CHECKING:
    from .tasks import TaskHandle
    from .workflows import WorkflowHandle

PROGRESS_PAGE_LIMIT = 256 * 1024
PROGRESS_QUERY_LIMIT = 8192
PROGRESS_CURSOR_LIMIT = 4096
PROGRESS_MAX_WAIT_MS = 20_000
_RESPONSE_ALLOWANCE = 1.0
_TASK_REASONS = frozenset({
    "admission_deferred", "admission_stopped", "submitted", "claimed", "dispatch_authorized",
    "cancel_requested", "cancelled", "report_accepted", "cleanup_confirmed", "succeeded",
    "failed", "retry_scheduled", "lease_expired",
})


class ExecutionProgressKind(StrEnum):
    TASK = "task"
    WORKFLOW = "workflow"


@dataclass(frozen=True, slots=True)
class ExecutionProgressTarget:
    kind: ExecutionProgressKind
    id: str


@dataclass(frozen=True, slots=True)
class ExecutionProgressEvent:
    """A committed lifecycle fact, identified by target and sequence.

    Sequences use lossless integers in Python and decimal strings on the wire.
    Timestamps describe observations; they do not define branch dependencies.
    """
    sequence: int
    at: int
    reason: str
    activation_id: str | None
    attempt_id: str | None


@dataclass(frozen=True, slots=True)
class ExecutionProgressPage:
    scope: Scope
    target: ExecutionProgressTarget
    state: TaskState | WorkflowState
    terminal: bool
    high_watermark: int
    events: tuple[ExecutionProgressEvent, ...]
    next_cursor: str
    has_more: bool

    @property
    def caught_up(self) -> bool:
        """All events through this snapshot's watermark have been returned."""
        return not self.has_more


class ProgressTimeout(LedgenceError, TimeoutError):
    """The observer's deadline expired; the execution continues independently.

    last_page is the most recent page yielded to the caller. next_cursor refers
    to that delivered page (or the supplied starting cursor), not an application
    acknowledgment. Persist a cursor only after processing/deduplicating its page.
    """
    def __init__(self, execution: TaskHandle | WorkflowHandle,
                 last_page: ExecutionProgressPage | None, next_cursor: str | None):
        super().__init__("execution progress observation deadline expired")
        self.execution = execution
        self.last_page = last_page
        self.next_cursor = next_cursor


def _target(raw) -> ExecutionProgressTarget:
    codec.fields(raw, {"kind", "id"})
    if type(raw["kind"]) is not str:
        raise InputError("invalid progress target kind")
    try:
        kind = ExecutionProgressKind(raw["kind"])
    except ValueError as exc:
        raise InputError("invalid progress target kind") from exc
    return ExecutionProgressTarget(kind, codec.text(raw["id"], "execution id"))


def _position(cursor, scope: Scope, target: ExecutionProgressTarget) -> int:
    if cursor is None:
        return 0
    codec.text(cursor, "cursor", PROGRESS_CURSOR_LIMIT)
    if re.fullmatch(r"(?:[0-9a-f]{2})+", cursor) is None:
        raise InputError("invalid execution progress cursor")
    try:
        raw = codec.decode(bytes.fromhex(cursor), PROGRESS_CURSOR_LIMIT // 2)
    except ProtocolError as exc:
        raise InputError("invalid execution progress cursor") from exc
    codec.fields(raw, {"version", "scope", "target", "after"})
    if (type(raw["version"]) is not int or raw["version"] != 1
            or _scope(raw["scope"]) != scope or _target(raw["target"]) != target):
        raise InputError("execution progress cursor does not match the query")
    after = codec.decimal_u64(raw["after"], "progress cursor position")
    canonical = {"version": 1,
                 "scope": {"tenant_id": scope.tenant_id, "namespace": scope.namespace},
                 "target": {"kind": target.kind.value, "id": target.id}, "after": str(after)}
    if codec.encode(canonical, PROGRESS_CURSOR_LIMIT // 2).hex() != cursor:
        raise InputError("execution progress cursor is not canonical")
    return after


@dataclass(frozen=True, slots=True)
class _ProgressQuery:
    scope: Scope
    target: ExecutionProgressTarget
    cursor: str | None
    limit: int
    after: int
    wait_ms: int = 0

    @classmethod
    def create(cls, handle, kind, cursor, limit, wait_ms=0):
        target = _target({"kind": kind, "id": handle.id})
        limit = codec.integer(limit, "limit", 1, 100)
        wait_ms = codec.integer(wait_ms, "wait_ms", 0, PROGRESS_MAX_WAIT_MS)
        return cls(handle.scope, target, cursor, limit, _position(cursor, handle.scope, target), wait_ms)

    def encode(self) -> bytes:
        value = {"scope": {"tenant_id": self.scope.tenant_id, "namespace": self.scope.namespace},
                 "target": {"kind": self.target.kind.value, "id": self.target.id},
                 "cursor": self.cursor, "limit": self.limit}
        if self.wait_ms:
            value["wait_ms"] = self.wait_ms
        return codec.encode(value, PROGRESS_QUERY_LIMIT)


def _parse_page(raw, query: _ProgressQuery) -> ExecutionProgressPage:
    codec.fields(raw, {"scope", "target", "state", "terminal", "high_watermark",
                       "events", "next_cursor", "has_more"})
    if _scope(raw["scope"]) != query.scope or _target(raw["target"]) != query.target:
        raise InputError("execution progress response identity changed")
    if type(raw["state"]) is not str:
        raise InputError("invalid execution progress state")
    try:
        state = (TaskState(raw["state"]) if query.target.kind == ExecutionProgressKind.TASK
                 else WorkflowState(raw["state"]))
    except ValueError as exc:
        raise InputError("invalid execution progress state") from exc
    if type(raw["terminal"]) is not bool or raw["terminal"] != state.terminal:
        raise InputError("execution progress terminal flag disagrees with state")
    high_watermark = codec.decimal_u64(raw["high_watermark"], "high_watermark")
    if high_watermark < query.after:
        raise InputError("execution progress watermark precedes the cursor")
    if type(raw["events"]) is not list or len(raw["events"]) > query.limit:
        raise InputError("invalid execution progress page size")
    position = query.after
    events = []
    for row in raw["events"]:
        codec.fields(row, {"sequence", "at", "reason", "activation_id", "attempt_id"})
        sequence = codec.decimal_u64(row["sequence"], "sequence", 1)
        if sequence != position + 1 or sequence > high_watermark:
            raise InputError("execution progress events contain a gap or invalid order")
        at = codec.integer(row["at"], "at", 0, codec.MAX_TIMESTAMP)
        reason = codec.text(row["reason"], "reason")
        if query.target.kind == ExecutionProgressKind.TASK and reason not in _TASK_REASONS:
            raise InputError("invalid task progress reason")
        activation_id, attempt_id = row["activation_id"], row["attempt_id"]
        if activation_id is not None:
            codec.text(activation_id, "activation_id")
        if attempt_id is not None:
            codec.text(attempt_id, "attempt_id")
        if ((query.target.kind == ExecutionProgressKind.TASK and activation_id is not None)
                or (query.target.kind == ExecutionProgressKind.WORKFLOW and attempt_id is not None)):
            raise InputError("execution progress event has another execution kind's identity")
        events.append(ExecutionProgressEvent(sequence, at, reason, activation_id, attempt_id))
        position = sequence
    if not events and position < high_watermark:
        raise InputError("execution progress omitted retained history")
    if (type(raw["has_more"]) is not bool or raw["has_more"] != (position < high_watermark)):
        raise InputError("execution progress has_more disagrees with watermark")
    if raw["next_cursor"] is None or _position(raw["next_cursor"], query.scope, query.target) != position:
        raise InputError("execution progress cursor disagrees with page")
    codec.validate_authoritative(raw, PROGRESS_PAGE_LIMIT)
    return ExecutionProgressPage(query.scope, query.target, state, raw["terminal"],
                                 high_watermark, tuple(events), raw["next_cursor"], raw["has_more"])


async def _read(handle, query: _ProgressQuery, deadline: float) -> ExecutionProgressPage:
    return await handle._client._require_transport().exchange(
        "POST", "/v1/executions/progress", body=query.encode(), deadline=deadline,
        parser=lambda raw: _parse_page(raw, query), limit=PROGRESS_PAGE_LIMIT,
    )


def _bounded_wait(wait_ms: int, remaining: float) -> int:
    # The requested wait is a ceiling, not a promise to block for that duration.
    # Leave time for the final read, response transfer and parsing.
    return min(wait_ms, max(0, int((remaining - _RESPONSE_ALLOWANCE) * 1000)))


async def progress(handle, kind: str, *, cursor: str | None = None,
                   limit: int = 100, wait_ms: int = 0) -> ExecutionProgressPage:
    wait_ms = codec.integer(wait_ms, "wait_ms", 0, PROGRESS_MAX_WAIT_MS)
    query = _ProgressQuery.create(handle, kind, cursor, limit,
                                  _bounded_wait(wait_ms, handle._client.request_timeout))
    deadline = asyncio.get_running_loop().time() + handle._client.request_timeout
    return await _read(handle, query, deadline)


async def follow_progress(handle, kind: str, *, cursor: str | None = None,
                          poll_interval: float = 1.0, timeout: float | None = None,
                          stop_on_terminal: bool = True,
                          limit: int = 100, wait_ms: int = PROGRESS_MAX_WAIT_MS,
                          ) -> AsyncIterator[ExecutionProgressPage]:
    """Pull lifecycle pages; caller processing supplies backpressure.

    Terminal stops at the observed, drained snapshot by default. Late operational
    records can still arrive: resume the saved cursor or set stop_on_terminal=False.
    No request is retried implicitly, and no observer action mutates the execution.
    """
    query = _ProgressQuery.create(handle, kind, cursor, limit)
    wait_ms = codec.integer(wait_ms, "wait_ms", 0, PROGRESS_MAX_WAIT_MS)
    interval = codec.duration(poll_interval, "poll_interval")
    if interval < 0.25:
        raise InputError("poll_interval must be at least 0.25 seconds")
    if type(stop_on_terminal) is not bool:
        raise InputError("stop_on_terminal must be a boolean")
    loop = asyncio.get_running_loop()
    deadline = None if timeout is None else loop.time() + codec.duration(timeout, "timeout")
    last_page = None
    previous = None
    delivered_cursor = cursor
    while True:
        now = loop.time()
        if deadline is not None and now >= deadline:
            raise ProgressTimeout(handle, last_page, delivered_cursor)
        request_deadline = now + handle._client.request_timeout
        if deadline is not None:
            request_deadline = min(request_deadline, deadline)
        # Initial and backlog reads stay immediate. Only caught-up observation
        # asks the service to share bounded waits with other subscribers.
        desired_wait = wait_ms if previous is not None and not previous.has_more else 0
        query = _ProgressQuery.create(handle, kind, query.cursor, limit,
                                      _bounded_wait(desired_wait, request_deadline - now))
        try:
            page = await _read(handle, query, request_deadline)
        except RequestTimeout as exc:
            if deadline is not None and loop.time() >= deadline:
                raise ProgressTimeout(handle, last_page, delivered_cursor) from exc
            raise
        if deadline is not None and loop.time() >= deadline:
            raise ProgressTimeout(handle, last_page, delivered_cursor)
        if previous is not None and (
            page.high_watermark < previous.high_watermark
            or previous.terminal and page.state != previous.state
        ):
            raise ProtocolError("execution progress regressed between observations")
        if (previous is None or page.events
                or (page.state, page.high_watermark) != (previous.state, previous.high_watermark)):
            last_page = page
            delivered_cursor = page.next_cursor
            yield page
        previous = page
        if stop_on_terminal and page.terminal and not page.has_more:
            return
        query = _ProgressQuery.create(handle, kind, page.next_cursor, limit)
        if not page.has_more:
            # Count request and caller processing time toward pacing. A service
            # may return immediately (including terminal keep-following), but a
            # full long poll must not incur an additional fixed polling delay.
            remaining = max(0, interval - (loop.time() - now))
            if deadline is not None:
                remaining = min(remaining, max(0, deadline - loop.time()))
            if remaining:
                await _sleep(remaining)
