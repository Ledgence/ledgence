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
