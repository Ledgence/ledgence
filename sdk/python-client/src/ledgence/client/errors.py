"""Client errors distinguish observation, rejection and uncertain mutation."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import Submission, TaskResult, TaskStatus
    from .tasks import TaskHandle


class LedgenceError(Exception):
    """Base class for errors reported by the client."""

    def __init__(self, message: str, *, request_id: str | None = None):
        super().__init__(message)
        self.request_id = request_id


class InputError(LedgenceError, ValueError):
    """Local input validation failed before network dispatch."""


class ServiceError(LedgenceError):
    """The server definitively rejected this exchange."""

    def __init__(self, message: str, *, code: str, request_id: str | None = None):
        super().__init__(message, request_id=request_id)
        self.code = code


class NotFound(ServiceError):
    """The requested scoped resource was not found."""


class Conflict(ServiceError):
    """The submission key is already bound to different input."""


class TransportError(LedgenceError):
    """An exchange could not establish an authoritative response."""


class ProtocolError(TransportError):
    """The response violates the HTTP or Ledgence wire contract."""


class Unavailable(TransportError):
    """The server reported availability failure, possibly after a commit."""


class RequestTimeout(TransportError, TimeoutError):
    """The aggregate exchange deadline expired; dispatch may have begun."""

    def __init__(self, *, dispatched: bool, request_id: str | None = None):
        super().__init__("request deadline expired", request_id=request_id)
        self.dispatched = dispatched


class ClientClosed(LedgenceError):
    """The client is not open, or its close operation has begun."""


class SubmissionUncertain(LedgenceError):
    """Submission may have committed; resend the same frozen submission."""

    def __init__(self, submission: Submission, cause: TransportError):
        super().__init__("submission acceptance is uncertain", request_id=cause.request_id)
        self.submission = submission
        self.cause = cause


class CancellationUncertain(LedgenceError):
    """Cancellation may have committed; the task identity permits reconciliation."""

    def __init__(self, task: TaskHandle, cause: TransportError):
        super().__init__("cancellation acceptance is uncertain", request_id=cause.request_id)
        self.task = task
        self.cause = cause


class WaitTimeout(LedgenceError, TimeoutError):
    """Observation expired. It did not cancel or fail the remote task."""

    def __init__(self, task: TaskHandle, last_status: TaskStatus | None,
                 last_error: TransportError | None):
        super().__init__("task observation deadline expired")
        self.task = task
        self.last_status = last_status
        self.last_error = last_error


class TaskFailed(LedgenceError):
    """The logical task has an authoritative terminal failure."""

    def __init__(self, result: TaskResult):
        super().__init__("task failed")
        self.result = result
        self.failure = result.outcome.failure


class TaskCancelled(LedgenceError):
    """The logical task has an authoritative terminal cancellation."""

    def __init__(self, result: TaskResult):
        super().__init__("task was cancelled")
        self.result = result


class WorkflowWaitTimeout(WaitTimeout):
    """Observation expired; the remote workflow continues independently."""

    def __init__(self, workflow, last_status, last_error):
        super().__init__(workflow, last_status, last_error)
        self.args = ("workflow observation deadline expired",)
        self.workflow = workflow


class WorkflowCancellationUncertain(CancellationUncertain):
    """Cancellation may have committed; reconcile using the same workflow ID."""

    def __init__(self, workflow, cause):
        super().__init__(workflow, cause)
        self.workflow = workflow


class WorkflowFailed(LedgenceError):
    """The workflow has an authoritative terminal failure."""

    def __init__(self, result):
        super().__init__("workflow failed")
        self.result = result
        self.error = result.outcome.error


class WorkflowCancelled(LedgenceError):
    """The workflow has an authoritative terminal cancellation."""

    def __init__(self, result):
        super().__init__("workflow was cancelled")
        self.result = result


class WorkflowEventUncertain(LedgenceError):
    """Event acceptance is uncertain; explicitly resend the same frozen command."""

    def __init__(self, command, cause: TransportError):
        super().__init__("workflow event acceptance is uncertain", request_id=cause.request_id)
        self.command = command
        self.cause = cause
