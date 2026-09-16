"""Scoped workflow submission and bounded observation without implicit resubmission."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

from . import codec
from .cloud_events import EVENT_COMMAND_LIMIT, validate_event
from .errors import (
    InputError, ProtocolError, RequestTimeout, SubmissionUncertain, TransportError,
    WorkflowCancellationUncertain, WorkflowCancelled, WorkflowEventUncertain, WorkflowFailed, WorkflowWaitTimeout,
)
from .models import RetryPolicy, Scope, TraceContext
from .tasks import _UNSET
from .workflow_models import (
    WorkflowCancellation, WorkflowFailure, WorkflowResult, WorkflowStatus, WorkflowSubmission,
    WorkflowSucceeded, WorkflowEventCommand, WorkflowEventReceipt, parse_workflow_event_receipt,
    parse_workflow_result, parse_workflow_status,
)

if TYPE_CHECKING:
    from .client import AsyncClient


class Workflows:
    def __init__(self, client: AsyncClient):
        self._client = client

    def prepare(self, *, program: str, version: str, queue: str, data: Any,
                idempotency_key: str, correlation_key: str | None = None,
                retry_policy: RetryPolicy | None = None, attempt_timeout_ms: int | None = None,
                origin_trace: TraceContext | None | object = _UNSET) -> WorkflowSubmission:
        """Freeze a workflow command, including the original optional trace context."""
        prepared = self._client.tasks.prepare(
            program=program, version=version, queue=queue, data=data,
            idempotency_key=idempotency_key, correlation_key=correlation_key,
            retry_policy=retry_policy, attempt_timeout_ms=attempt_timeout_ms,
            origin_trace=origin_trace,
        )
        return WorkflowSubmission._create(prepared.base_url, prepared.scope, prepared._body)

    async def submit(self, submission: WorkflowSubmission | None = None, **kwargs) -> WorkflowHandle:
        """Submit once. An uncertain response retains the exact command for reconciliation."""
        deadline = asyncio.get_running_loop().time() + self._client.request_timeout
        transport = self._client._require_transport()
        if submission is not None:
            if kwargs or type(submission) is not WorkflowSubmission:
                raise InputError("supply one WorkflowSubmission or submission keyword arguments")
            if submission.scope != self._client.scope or submission.base_url != self._client.base_url:
                raise InputError("submission belongs to a different endpoint or scope")
        else:
            try:
                submission = self.prepare(**kwargs)
            except TypeError as exc:
                raise InputError("invalid workflow submission arguments") from exc
        correlation = submission.to_dict()["input"].get("correlation_key")

        def parse(raw):
            status = parse_workflow_status(raw, self._client.scope)
            if status.correlation_key != correlation:
                raise ProtocolError("workflow submission response changed correlation")
            if status.parent_workflow_id is not None:
                raise ProtocolError("workflow submission returned an owned child")
            return status

        try:
            status = await transport.exchange(
                "POST", "/v1/workflows", body=submission._body, deadline=deadline,
                parser=parse, limit=codec.STATUS_LIMIT,
            )
        except RequestTimeout as exc:
            if not exc.dispatched:
                raise
            raise SubmissionUncertain(submission, exc) from exc
        except TransportError as exc:
            raise SubmissionUncertain(submission, exc) from exc
        return WorkflowHandle(self._client, status.workflow_id)

    def handle(self, workflow_id: str) -> WorkflowHandle:
        """Construct a scoped reference without checking existence or submitting work."""
        return WorkflowHandle(self._client, codec.text(workflow_id, "workflow_id"))


@dataclass(frozen=True)
class WorkflowHandle:
    _client: AsyncClient = field(repr=False, compare=False)
    id: str
    _scope: Scope = field(init=False, repr=False)
    _base_url: str = field(init=False, repr=False)

    def __post_init__(self):
        object.__setattr__(self, "_scope", self._client.scope)
        object.__setattr__(self, "_base_url", self._client.base_url)
        codec.text(self.id, "workflow_id")

    @property
    def scope(self) -> Scope:
        return self._scope

    def _query(self):
        return {"tenant_id": self.scope.tenant_id, "namespace": self.scope.namespace,
                "workflow_id": self.id}

    async def _status(self, deadline):
        return await self._client._require_transport().exchange(
            "GET", "/v1/workflows/status", query=self._query(), deadline=deadline,
            parser=lambda raw: parse_workflow_status(raw, self.scope, self.id),
            limit=codec.STATUS_LIMIT,
        )

    async def status(self) -> WorkflowStatus:
        return await self._status(asyncio.get_running_loop().time() + self._client.request_timeout)

    async def _outcome(self, deadline):
        return await self._client._require_transport().exchange(
            "GET", "/v1/workflows/result", query=self._query(), deadline=deadline,
            parser=lambda raw: parse_workflow_result(raw, self.scope, self.id),
            limit=512 * 1024,
        )

    async def outcome(self) -> WorkflowResult:
        return await self._outcome(asyncio.get_running_loop().time() + self._client.request_timeout)

    async def wait(self, timeout: float = 60.0) -> WorkflowResult:
        """Observe a terminal outcome; expiration never cancels or resubmits the workflow."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + codec.duration(timeout, "timeout")
        last_status = None
        last_error = None
        while loop.time() < deadline:
            try:
                if last_status is None or not last_status.state.terminal:
                    status = await self._status(min(deadline, loop.time() + self._client.request_timeout))
                    if last_status is not None and (
                        status.revision < last_status.revision
                        or status.submitted_at != last_status.submitted_at
                        or status.correlation_key != last_status.correlation_key
                        or status.parent_workflow_id != last_status.parent_workflow_id
                        or status.root_workflow_id != last_status.root_workflow_id
                    ):
                        raise ProtocolError("workflow metadata regressed between observations")
                    last_status = status
                if last_status.state.terminal:
                    result = await self._outcome(min(deadline, loop.time() + self._client.request_timeout))
                    if result.workflow != last_status:
                        raise ProtocolError("terminal workflow metadata changed between observations")
                    if loop.time() >= deadline:
                        raise WorkflowWaitTimeout(self, last_status, last_error)
                    return result
            except ProtocolError:
                raise
            except TransportError as exc:
                last_error = exc
            remaining = deadline - loop.time()
            if remaining > 0:
                await asyncio.sleep(min(1.0, remaining))
        raise WorkflowWaitTimeout(self, last_status, last_error)

    async def result(self, timeout: float = 60.0) -> Any:
        """Return successful JSON output or raise an authoritative workflow failure."""
        result = await self.wait(timeout)
        if isinstance(result.outcome, WorkflowSucceeded):
            return result.outcome.output
        if isinstance(result.outcome, WorkflowFailure):
            raise WorkflowFailed(result)
        if isinstance(result.outcome, WorkflowCancellation):
            raise WorkflowCancelled(result)
        raise ProtocolError("terminal workflow has no outcome")

    def prepare_event(self, key: str, *, event: Any) -> WorkflowEventCommand:
        """Freeze a full original CloudEvent for one run-global, one-shot wait key.

        Preserve this command before sending when cancellation or transport
        failure might require reconciliation. No event ID is generated or changed.
        """
        key = codec.text(key, "key")
        validate_event(event)
        body = codec.encode({"scope": {"tenant_id": self.scope.tenant_id,
                                       "namespace": self.scope.namespace},
                             "workflow_id": self.id, "key": key, "event": event},
                            EVENT_COMMAND_LIMIT, max_depth=96)
        return WorkflowEventCommand._create(self._base_url, self.scope, self.id, key, body)

    async def send_event(self, command: WorkflowEventCommand | None = None, *,
                         key: str | None = None, event: Any = _UNSET) -> WorkflowEventReceipt:
        """Send once, or reconcile by resending an unchanged prepared command.

        Acceptance may precede the workflow's wait. Changed bindings conflict;
        closed or late keys are rejected by the orchestrator. This does not wait
        for the workflow to consume the event or create another workflow.
        """
        deadline = asyncio.get_running_loop().time() + self._client.request_timeout
        transport = self._client._require_transport()
        if command is None:
            if key is None or event is _UNSET:
                raise InputError("supply an event command or key and full CloudEvent")
            command = self.prepare_event(key, event=event)
        elif (type(command) is not WorkflowEventCommand or key is not None or event is not _UNSET):
            raise InputError("supply one WorkflowEventCommand or key and event")
        elif (command.scope != self.scope or command.base_url != self._base_url
              or command.workflow_id != self.id):
            raise InputError("event command belongs to another endpoint, scope, or workflow")
        try:
            return await transport.exchange(
                "POST", "/v1/workflows/events", body=command._body, deadline=deadline,
                parser=lambda raw: parse_workflow_event_receipt(raw, command),
                limit=EVENT_COMMAND_LIMIT,
            )
        except RequestTimeout as exc:
            if not exc.dispatched:
                raise
            raise WorkflowEventUncertain(command, exc) from exc
        except TransportError as exc:
            raise WorkflowEventUncertain(command, exc) from exc

    async def cancel(self) -> WorkflowStatus:
        deadline = asyncio.get_running_loop().time() + self._client.request_timeout
        body = codec.encode({"scope": {"tenant_id": self.scope.tenant_id,
                                       "namespace": self.scope.namespace}, "workflow_id": self.id})
        try:
            return await self._client._require_transport().exchange(
                "POST", "/v1/workflows/cancel", body=body, deadline=deadline,
                parser=lambda raw: parse_workflow_status(raw, self.scope, self.id),
                limit=codec.STATUS_LIMIT,
            )
        except RequestTimeout as exc:
            if not exc.dispatched:
                raise
            raise WorkflowCancellationUncertain(self, exc) from exc
        except TransportError as exc:
            raise WorkflowCancellationUncertain(self, exc) from exc
