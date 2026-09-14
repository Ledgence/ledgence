"""Workflow observations, separate from the controller's individual task attempts."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Literal

from . import codec
from .errors import InputError, ProtocolError
from .models import ErrorDetail, Scope, Submission, _optional_text, _optional_time, _scope


class WorkflowState(StrEnum):
    RUNNING = "running"
    WAITING = "waiting"
    FAILING = "failing"
    CANCELLING = "cancelling"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in (self.SUCCEEDED, self.FAILED, self.CANCELLED)


@dataclass(frozen=True, init=False)
class WorkflowSubmission(Submission):
    """Frozen workflow command. Create with client.workflows.prepare()."""

    def __init__(self):
        raise TypeError("create submissions with client.workflows.prepare()")


@dataclass(frozen=True, init=False)
class WorkflowEventCommand:
    """Frozen external event, bound to one endpoint, scope, workflow, and wait key."""
    base_url: str
    scope: Scope
    workflow_id: str
    key: str
    _body: bytes = field(repr=False)

    def __init__(self):
        raise TypeError("create event commands with workflow.prepare_event()")

    @classmethod
    def _create(cls, base_url, scope, workflow_id, key, body):
        value = object.__new__(cls)
        for name, item in (("base_url", base_url), ("scope", scope),
                           ("workflow_id", workflow_id), ("key", key), ("_body", body)):
            object.__setattr__(value, name, item)
        return value

    def to_dict(self) -> dict:
        """Return an independent JSON command copy suitable for durable storage."""
        from .cloud_events import EVENT_COMMAND_LIMIT
        return codec.decode(self._body, EVENT_COMMAND_LIMIT)


@dataclass(frozen=True)
class WorkflowEventReceipt:
    scope: Scope
    workflow_id: str
    key: str
    event_id: str
    event_source: str
    accepted_at: int
    already_accepted: bool


@dataclass(frozen=True)
class WorkflowStatus:
    workflow_id: str
    scope: Scope
    state: WorkflowState
    revision: int
    activation_id: str | None
    submitted_at: int
    terminal_at: int | None
    correlation_key: str | None


@dataclass(frozen=True)
class WorkflowSucceeded:
    output: Any = field(repr=False)
    kind: Literal["succeeded"] = field(default="succeeded", init=False)


@dataclass(frozen=True)
class WorkflowFailure:
    error: ErrorDetail
    kind: Literal["failed"] = field(default="failed", init=False)


@dataclass(frozen=True)
class WorkflowCancellation:
    kind: Literal["cancelled"] = field(default="cancelled", init=False)


@dataclass(frozen=True)
class WorkflowResult:
    workflow: WorkflowStatus
    outcome: WorkflowSucceeded | WorkflowFailure | WorkflowCancellation | None


def _status(raw, scope, workflow_id):
    codec.fields(raw, set(WorkflowStatus.__dataclass_fields__))
    actual_scope = _scope(raw["scope"])
    actual_id = codec.text(raw["workflow_id"], "workflow_id")
    if actual_scope != scope or (workflow_id is not None and actual_id != workflow_id):
        raise InputError("workflow identity does not match the request")
    state = WorkflowState(raw["state"])
    activation = _optional_text(raw["activation_id"], "activation_id")
    terminal = _optional_time(raw["terminal_at"], "terminal_at")
    submitted = codec.integer(raw["submitted_at"], "submitted_at", 0, codec.MAX_TIMESTAMP)
    if (state.terminal != (terminal is not None)
            or (state.terminal and activation is not None)
            or (state == WorkflowState.RUNNING and activation is None)
            or (terminal is not None and terminal < submitted)):
        raise InputError("inconsistent workflow status")
    return WorkflowStatus(
        actual_id, actual_scope, state,
        codec.integer(raw["revision"], "revision", 0, (1 << 64) - 1), activation, submitted,
        terminal, _optional_text(raw["correlation_key"], "correlation_key", 512,
                                 empty=True, noncharacters=True),
    )


def parse_workflow_status(raw, scope: Scope, workflow_id: str | None = None) -> WorkflowStatus:
    try:
        return _status(raw, scope, workflow_id)
    except (ValueError, TypeError, KeyError) as exc:
        raise ProtocolError("invalid workflow status response") from exc


def parse_workflow_result(raw, scope: Scope, workflow_id: str) -> WorkflowResult:
    try:
        codec.fields(raw, {"workflow", "outcome"})
        status = _status(raw["workflow"], scope, workflow_id)
        outcome = raw["outcome"]
        if not status.state.terminal:
            if outcome is not None:
                raise InputError("nonterminal workflow has an outcome")
            return WorkflowResult(status, None)
        if type(outcome) is not dict or outcome.get("kind") != status.state.value:
            raise InputError("outcome contradicts workflow state")
        if status.state == WorkflowState.SUCCEEDED:
            codec.fields(outcome, {"kind", "output"})
            codec.validate(outcome["output"], 256 * 1024)
            codec.encode(outcome["output"], 256 * 1024, max_depth=codec.MAX_DEPTH)
            value = WorkflowSucceeded(outcome["output"])
        elif status.state == WorkflowState.FAILED:
            codec.fields(outcome, {"kind", "error"})
            error = outcome["error"]
            codec.fields(error, {"kind", "message"})
            codec.text(error["kind"], "error.kind")
            if type(error["message"]) is not str or len(error["message"].encode("utf-8")) > 4096:
                raise InputError("invalid workflow error message")
            value = WorkflowFailure(ErrorDetail(**error))
        else:
            codec.fields(outcome, {"kind"})
            value = WorkflowCancellation()
        return WorkflowResult(status, value)
    except (ValueError, TypeError, KeyError) as exc:
        raise ProtocolError("invalid workflow result response") from exc


def parse_workflow_event_receipt(raw, command: WorkflowEventCommand) -> WorkflowEventReceipt:
    """Accept only a receipt bound to the exact frozen event being reconciled."""
    try:
        codec.fields(raw, set(WorkflowEventReceipt.__dataclass_fields__))
        actual_scope = _scope(raw["scope"])
        workflow_id = codec.text(raw["workflow_id"], "workflow_id")
        key = codec.text(raw["key"], "key")
        event_id = codec.text(raw["event_id"], "event_id", 128)
        source = codec.text(raw["event_source"], "event_source", 2048)
        event = command.to_dict()["event"]
        if (actual_scope != command.scope or workflow_id != command.workflow_id
                or key != command.key or event_id != event["id"] or source != event["source"]):
            raise InputError("workflow event receipt identity does not match the command")
        accepted_at = codec.integer(raw["accepted_at"], "accepted_at", 0, codec.MAX_TIMESTAMP)
        if type(raw["already_accepted"]) is not bool:
            raise InputError("already_accepted must be a boolean")
        return WorkflowEventReceipt(actual_scope, workflow_id, key, event_id, source,
                                    accepted_at, raw["already_accepted"])
    except (ValueError, TypeError, KeyError) as exc:
        raise ProtocolError("invalid workflow event receipt") from exc
