"""Portable task observations and immutable submission identities."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
import re
from typing import Any, Literal

from . import codec
from .errors import InputError, ProtocolError


class TaskState(StrEnum):
    QUEUED = "queued"
    ACTIVE = "active"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in (self.SUCCEEDED, self.FAILED, self.CANCELLED)


class Quiescence(StrEnum):
    CONFIRMED = "confirmed"
    UNCONFIRMED = "unconfirmed"


@dataclass(frozen=True)
class Scope:
    tenant_id: str
    namespace: str

    def __post_init__(self):
        codec.text(self.tenant_id, "tenant")
        codec.text(self.namespace, "namespace")


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int
    retry_delay_ms: int

    def __post_init__(self):
        codec.integer(self.max_attempts, "max_attempts", 1, 1000)
        codec.integer(self.retry_delay_ms, "retry_delay_ms", 0, 86_400_000)


@dataclass(frozen=True)
class TraceContext:
    traceparent: str
    tracestate: str | None = None

    def __post_init__(self):
        if type(self.traceparent) is not str or not re.fullmatch(
            r"00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}", self.traceparent
        ) or self.traceparent[3:35] == "0" * 32 or self.traceparent[36:52] == "0" * 16:
            raise InputError("invalid traceparent")
        if self.tracestate is None:
            return
        state = self.tracestate
        if type(state) is not str or not state.isascii() or len(state) > 512:
            raise InputError("invalid tracestate")
        members = state.split(",")
        if len(members) > 32:
            raise InputError("too many tracestate members")
        seen = set()
        for member in members:
            member = member.strip(" ")
            if not member:
                continue
            key, sep, value = member.partition("=")
            if "@" in key:
                tenant, _, system = key.partition("@")
                valid = (re.fullmatch(r"[a-z0-9][a-z0-9_*/-]{0,240}", tenant)
                         and re.fullmatch(r"[a-z][a-z0-9_*/-]{0,13}", system))
            else:
                valid = re.fullmatch(r"[a-z][a-z0-9_*/-]{0,255}", key)
            if not sep or not valid or key in seen or not 1 <= len(value) <= 256 or any(
                ord(c) < 0x20 or ord(c) > 0x7E or c in ",=" for c in value
            ):
                raise InputError("invalid tracestate member")
            seen.add(key)

    def to_dict(self) -> dict[str, str]:
        result = {"traceparent": self.traceparent}
        if self.tracestate is not None:
            result["tracestate"] = self.tracestate
        return result


@dataclass(frozen=True, init=False)
class Submission:
    """A frozen, endpoint-bound command. Create with client.tasks.prepare()."""
    base_url: str
    scope: Scope
    _body: bytes = field(repr=False)

    def __init__(self):
        raise TypeError("create submissions with client.tasks.prepare()")

    @classmethod
    def _create(cls, base_url: str, scope: Scope, body: bytes) -> Submission:
        value = object.__new__(cls)
        object.__setattr__(value, "base_url", base_url)
        object.__setattr__(value, "scope", scope)
        object.__setattr__(value, "_body", body)
        return value

    @property
    def idempotency_key(self) -> str:
        return self.to_dict()["idempotency_key"]

    def to_dict(self) -> dict:
        """Return an independent JSON command copy suitable for durable storage."""
        return codec.decode(self._body, codec.CONTROL_LIMIT)


@dataclass(frozen=True)
class TaskStatus:
    scope: Scope
    task_id: str
    run_id: str
    queue: str
    correlation_key: str | None
    state: TaskState
    attempt_count: int
    current_attempt_id: str | None
    latest_attempt_id: str | None
    submitted_at: int
    available_at: int
    terminal_at: int | None
    cancel_requested_at: int | None
    workflow_id: str | None = None
    workflow_activation_id: str | None = None


@dataclass(frozen=True)
class TaskPage:
    """One bounded page of live task observations, newest submissions first."""
    items: tuple[TaskStatus, ...]
    next_cursor: str | None


@dataclass(frozen=True)
class ErrorDetail:
    kind: str
    message: str


@dataclass(frozen=True)
class ApplicationFailure:
    error: ErrorDetail
    kind: Literal["application"] = field(default="application", init=False)


@dataclass(frozen=True)
class ExecutionFailure:
    error: ErrorDetail
    phase: str
    cleanup_error: ErrorDetail | None
    kind: Literal["execution"] = field(default="execution", init=False)


@dataclass(frozen=True)
class AttemptLost:
    kind: Literal["attempt_lost"] = field(default="attempt_lost", init=False)


@dataclass(frozen=True)
class Succeeded:
    attempt_id: str
    quiescence: Quiescence
    execution_may_have_started: bool
    output: Any = field(repr=False)
    kind: Literal["succeeded"] = field(default="succeeded", init=False)


@dataclass(frozen=True)
class Failed:
    attempt_id: str
    quiescence: Quiescence
    execution_may_have_started: bool
    failure: ApplicationFailure | ExecutionFailure | AttemptLost
    kind: Literal["failed"] = field(default="failed", init=False)


@dataclass(frozen=True)
class Cancelled:
    kind: Literal["cancelled"] = field(default="cancelled", init=False)


@dataclass(frozen=True)
class TaskResult:
    task: TaskStatus
    outcome: Succeeded | Failed | Cancelled | None


def _scope(raw) -> Scope:
    codec.fields(raw, {"tenant_id", "namespace"})
    return Scope(**raw)


def _optional_text(raw, name, maximum=128, **kwargs):
    return None if raw is None else codec.text(raw, name, maximum, **kwargs)


def _optional_time(raw, name):
    return None if raw is None else codec.integer(raw, name, 0, codec.MAX_TIMESTAMP)


def _status(raw, scope: Scope, task_id: str) -> TaskStatus:
    workflow_fields = {"workflow_id", "workflow_activation_id"}
    codec.fields(raw, set(TaskStatus.__dataclass_fields__) - workflow_fields, workflow_fields)
    actual_scope = _scope(raw["scope"])
    if actual_scope != scope or raw["task_id"] != task_id:
        raise InputError("task identity does not match the request")
    state = TaskState(raw["state"])
    count = codec.integer(raw["attempt_count"], "attempt_count", 0, 1000)
    current = _optional_text(raw["current_attempt_id"], "current_attempt_id")
    latest = _optional_text(raw["latest_attempt_id"], "latest_attempt_id")
    terminal = _optional_time(raw["terminal_at"], "terminal_at")
    if (count == 0) != (latest is None) or (state == TaskState.ACTIVE) != (current is not None):
        raise InputError("inconsistent task attempt references")
    if current is not None and current != latest:
        raise InputError("active and latest attempts differ")
    if state.terminal != (terminal is not None):
        raise InputError("inconsistent terminal timestamp")
    if state in (TaskState.SUCCEEDED, TaskState.FAILED) and latest is None:
        raise InputError("terminal task has no execution attempt")
    cancelled_at = _optional_time(raw["cancel_requested_at"], "cancel_requested_at")
    if (state == TaskState.CANCELLED and cancelled_at is None) or (
        cancelled_at is not None and state not in (TaskState.ACTIVE, TaskState.CANCELLED)
    ):
        raise InputError("inconsistent cancellation metadata")
    workflow_id = _optional_text(raw.get("workflow_id"), "workflow_id")
    activation_id = _optional_text(raw.get("workflow_activation_id"), "workflow_activation_id")
    if activation_id is not None and (workflow_id is None or activation_id != task_id):
        raise InputError("inconsistent workflow controller identity")
    return TaskStatus(
        scope=actual_scope, task_id=codec.text(raw["task_id"], "task_id"),
        run_id=codec.text(raw["run_id"], "run_id"), queue=codec.text(raw["queue"], "queue"),
        correlation_key=_optional_text(raw["correlation_key"], "correlation_key", 512,
                                       empty=True, noncharacters=True),
        state=state, attempt_count=count, current_attempt_id=current, latest_attempt_id=latest,
        submitted_at=codec.integer(raw["submitted_at"], "submitted_at", 0, codec.MAX_TIMESTAMP),
        available_at=codec.integer(raw["available_at"], "available_at", 0, codec.MAX_TIMESTAMP),
        terminal_at=terminal, cancel_requested_at=cancelled_at,
        workflow_id=workflow_id, workflow_activation_id=activation_id,
    )


def parse_status(raw, scope: Scope, task_id: str) -> TaskStatus:
    try:
        return _status(raw, scope, task_id)
    except (ValueError, TypeError, KeyError) as exc:
        raise ProtocolError("invalid task status response") from exc


def _error(raw, *, worker: bool) -> ErrorDetail:
    codec.fields(raw, {"kind", "message"})
    if type(raw["kind"]) is not str or type(raw["message"]) is not str:
        raise InputError("invalid error details")
    if worker and raw["kind"] not in {
        "invalid_input", "not_found", "integrity", "incompatible", "unavailable",
        "cancelled", "timed_out", "runtime", "protocol", "io", "capacity",
    }:
        raise InputError("unknown worker error kind")
    return ErrorDetail(**raw)


def _failure(raw):
    if type(raw) is not dict:
        raise InputError("invalid failure")
    match raw.get("kind"):
        case "application":
            codec.fields(raw, {"kind", "error"})
            return ApplicationFailure(_error(raw["error"], worker=False))
        case "execution":
            codec.fields(raw, {"kind", "error", "phase", "cleanup_error"})
            if raw["phase"] not in {"admission", "preparation", "startup", "execution", "cleanup"}:
                raise InputError("unknown execution phase")
            return ExecutionFailure(_error(raw["error"], worker=True), raw["phase"],
                                    None if raw["cleanup_error"] is None else
                                    _error(raw["cleanup_error"], worker=True))
        case "attempt_lost":
            codec.fields(raw, {"kind"})
            return AttemptLost()
        case _:
            raise InputError("unknown task failure")


def parse_result(raw, scope: Scope, task_id: str) -> TaskResult:
    try:
        codec.fields(raw, {"task", "outcome"})
        task = _status(raw["task"], scope, task_id)
        outcome = raw["outcome"]
        if not task.state.terminal:
            if outcome is not None:
                raise InputError("nonterminal task has an outcome")
            return TaskResult(task, None)
        if type(outcome) is not dict or outcome.get("kind") != task.state.value:
            raise InputError("outcome contradicts task state")
        codec.encode(outcome, 8 * 1024 * 1024,
                     max_depth=104 if task.workflow_activation_id is not None else codec.MAX_DEPTH + 8)
        if task.state == TaskState.CANCELLED:
            codec.fields(outcome, {"kind"})
            return TaskResult(task, Cancelled())
        keys = {"kind", "attempt_id", "quiescence", "execution_may_have_started"}
        keys.add("output" if task.state == TaskState.SUCCEEDED else "failure")
        codec.fields(outcome, keys)
        if outcome["attempt_id"] != task.latest_attempt_id:
            raise InputError("outcome belongs to a different attempt")
        started = outcome["execution_may_have_started"]
        if type(started) is not bool:
            raise InputError("invalid execution-start evidence")
        quiescence = Quiescence(outcome["quiescence"])
        if task.state == TaskState.SUCCEEDED:
            if not started:
                raise InputError("success requires execution-start evidence")
            output_depth = 96 if task.workflow_activation_id is not None else codec.MAX_DEPTH
            codec.validate(outcome["output"], 8 * 1024 * 1024, max_depth=output_depth)
            codec.encode(outcome["output"], 8 * 1024 * 1024, max_depth=output_depth)
            value = Succeeded(outcome["attempt_id"], quiescence, started, outcome["output"])
        else:
            failure = _failure(outcome["failure"])
            if isinstance(failure, ApplicationFailure) and not started:
                raise InputError("application failure requires execution-start evidence")
            if isinstance(failure, AttemptLost) and quiescence != Quiescence.UNCONFIRMED:
                raise InputError("lost attempt cannot claim confirmed quiescence")
            value = Failed(outcome["attempt_id"], quiescence, started, failure)
        return TaskResult(task, value)
    except (ValueError, TypeError, KeyError) as exc:
        raise ProtocolError("invalid task result response") from exc
