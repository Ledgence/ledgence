"""Bounded task discovery queries and strict page validation."""
from __future__ import annotations

from dataclasses import dataclass

from . import codec
from .errors import InputError, ProtocolError
from .models import Scope, TaskPage, TaskState, parse_status

_CURSOR_LIMIT = 8192


@dataclass(frozen=True)
class _TaskQuery:
    state: TaskState | str | None = None
    queue: str | None = None
    correlation_key: str | None = None
    submitted_from: int | None = None
    submitted_until: int | None = None
    limit: int = 50
    cursor: str | None = None

    def __post_init__(self):
        if self.state is not None:
            if type(self.state) not in (str, TaskState):
                raise InputError("state must be a task state")
            try:
                object.__setattr__(self, "state", TaskState(self.state))
            except ValueError as exc:
                raise InputError("state must be a task state") from exc
        if self.queue is not None:
            codec.text(self.queue, "queue")
        if self.correlation_key is not None:
            codec.text(self.correlation_key, "correlation_key", 512, empty=True,
                       noncharacters=True)
        for name in ("submitted_from", "submitted_until"):
            value = getattr(self, name)
            if value is not None:
                codec.integer(value, name, 0, codec.MAX_TIMESTAMP)
        if (self.submitted_from is not None and self.submitted_until is not None
                and self.submitted_from >= self.submitted_until):
            raise InputError("submitted_from must be before submitted_until")
        codec.integer(self.limit, "limit", 1, 100)
        if self.cursor is not None:
            codec.text(self.cursor, "cursor", _CURSOR_LIMIT)

    def parameters(self, scope: Scope) -> dict[str, str]:
        params = {"tenant_id": scope.tenant_id, "namespace": scope.namespace,
                  "limit": str(self.limit)}
        for name in ("state", "queue", "correlation_key", "submitted_from",
                     "submitted_until", "cursor"):
            value = getattr(self, name)
            if value is not None:
                params[name] = str(value)
        return params

    def parse(self, raw, scope: Scope) -> TaskPage:
        try:
            codec.fields(raw, {"items", "next_cursor"})
            if type(raw["items"]) is not list or len(raw["items"]) > self.limit:
                raise InputError("invalid task page size")
            items = []
            seen = set()
            previous = None
            for value in raw["items"]:
                if type(value) is not dict:
                    raise InputError("task page items must be task statuses")
                item = parse_status(value, scope, value["task_id"])
                key = (item.submitted_at, item.task_id)
                if item.task_id in seen or (previous is not None and key >= previous):
                    raise InputError("task page is not strictly ordered")
                if ((self.state is not None and item.state != self.state)
                        or (self.queue is not None and item.queue != self.queue)
                        or (self.correlation_key is not None
                            and item.correlation_key != self.correlation_key)
                        or (self.submitted_from is not None
                            and item.submitted_at < self.submitted_from)
                        or (self.submitted_until is not None
                            and item.submitted_at >= self.submitted_until)):
                    raise InputError("task page item does not match the filters")
                seen.add(item.task_id)
                previous = key
                items.append(item)
            cursor = raw["next_cursor"]
            if cursor is not None:
                codec.text(cursor, "next_cursor", _CURSOR_LIMIT)
                if len(items) != self.limit or cursor == self.cursor:
                    raise InputError("invalid task page continuation")
            return TaskPage(tuple(items), cursor)
        except (ValueError, TypeError, KeyError) as exc:
            raise ProtocolError("invalid task page response") from exc
