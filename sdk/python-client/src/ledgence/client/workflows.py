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
from .approval_models import (
    APPROVAL_LIMIT, APPROVAL_PAGE_LIMIT, ApprovalDecisionCommand, ApprovalDecisionReceipt,
    ApprovalPage, ApprovalStatus, WorkflowApproval, _reason, parse_approval,
    parse_approval_page, parse_approval_receipt,
)
from .errors import ApprovalDecisionUncertain
from .workflow_models import (
    WorkflowCancellation, WorkflowFailure, WorkflowResult, WorkflowStatus, WorkflowSubmission,
    WorkflowSucceeded, WorkflowEventCommand, WorkflowEventReceipt, parse_workflow_event_receipt,
    parse_workflow_result, parse_workflow_status,
)

if TYPE_CHECKING:
    from .client import AsyncClient
    from .completion_models import CompletionSubscribeCommand
    from .completions import CompletionSubscriptionHandle


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

    async def approval(self, key: str) -> WorkflowApproval:
        """Read an existing scoped approval; this never creates a request."""
        key = codec.text(key, "key")
        return await self._client._require_transport().exchange(
            "POST", "/v1/workflows/approvals/inspect",
            body=codec.encode({"scope": {"tenant_id": self.scope.tenant_id,
                                        "namespace": self.scope.namespace},
                               "workflow_id": self.id, "key": key}),
            deadline=asyncio.get_running_loop().time() + self._client.request_timeout,
            parser=lambda raw: parse_approval(raw, self.scope, self.id, key, self._base_url),
            limit=APPROVAL_LIMIT,
        )

    async def approvals(self, *, after_key: str | None = None, limit: int = 10) -> ApprovalPage:
        """Read a bounded page ordered by approval key; cursor is the last key."""
        if after_key is not None:
            codec.text(after_key, "after_key")
        codec.integer(limit, "limit", 1, 10)
        return await self._client._require_transport().exchange(
            "POST", "/v1/workflows/approvals/list",
            body=codec.encode({"scope": {"tenant_id": self.scope.tenant_id,
                                        "namespace": self.scope.namespace},
                               "workflow_id": self.id, "after_key": after_key, "limit": limit}),
            deadline=asyncio.get_running_loop().time() + self._client.request_timeout,
            parser=lambda raw: parse_approval_page(raw, self.scope, self.id, after_key, limit, self._base_url),
            limit=APPROVAL_PAGE_LIMIT,
        )

    def prepare_approval_decision(self, approval: WorkflowApproval, *, decision_id: str,
                                  decision: str, reviewer: str,
                                  reason: str | None = None) -> ApprovalDecisionCommand:
        """Freeze a decision against the inspected effective action and request.

        reviewer is claimed attribution, not authenticated identity. Deploy this
        endpoint behind an authenticated and authorized application boundary.
        Persist the command before dispatch when reconciliation may be needed.
        """
        if (type(approval) is not WorkflowApproval or approval.scope != self.scope
                or approval.workflow_id != self.id or approval._base_url != self._base_url):
            raise InputError("approval belongs to another endpoint, scope, or workflow")
        if approval.status != ApprovalStatus.PENDING:
            raise InputError("only a pending approval can receive a new decision")
        codec.text(decision_id, "decision_id")
        codec.text(reviewer, "reviewer")
        if type(decision) is not str or decision not in ("approve", "reject"):
            raise InputError("decision must be approve or reject")
        try:
            _reason(reason)
        except UnicodeError as exc:
            raise InputError("reason must contain Unicode scalar values") from exc
        snapshot = approval.to_dict()
        # Revalidate even if an application manually constructed a view.
        parse_approval(snapshot, self.scope, self.id, approval.key, self._base_url)
        body = codec.encode({name: snapshot[name] for name in (
            "scope", "workflow_id", "key", "activation_id", "revision", "action")} | {
                "decision_id": decision_id, "decision": decision, "reviewer": reviewer, "reason": reason},
            APPROVAL_LIMIT, max_depth=96)
        return ApprovalDecisionCommand._create(self._base_url, self.scope, self.id, approval.key, body)

    def restore_approval_decision(self, saved: dict) -> ApprovalDecisionCommand:
        """Restore an unchanged persisted command for explicit reconciliation.

        This performs no network call and does not grant approval. The server
        still verifies the pending request or returns its identical decision.
        Only restore application-owned storage, never untrusted client history.
        """
        from .approval_models import _action
        from .models import _scope
        codec.fields(saved, {"scope", "workflow_id", "key", "activation_id", "revision", "action",
                             "decision_id", "decision", "reviewer", "reason"})
        if _scope(saved["scope"]) != self.scope or saved["workflow_id"] != self.id:
            raise InputError("saved approval command belongs to another scope or workflow")
        for name in ("workflow_id", "key", "activation_id", "decision_id", "reviewer"):
            codec.text(saved[name], name)
        codec.integer(saved["revision"], "revision")
        _action(saved["action"])
        if type(saved["decision"]) is not str or saved["decision"] not in ("approve", "reject"):
            raise InputError("decision must be approve or reject")
        try:
            _reason(saved["reason"])
        except UnicodeError as exc:
            raise InputError("reason must contain Unicode scalar values") from exc
        # Use the same field order as prepare_approval_decision, even if a
        # storage serializer reordered the object's keys.
        body = codec.encode({name: saved[name] for name in (
            "scope", "workflow_id", "key", "activation_id", "revision", "action", "decision_id",
            "decision", "reviewer", "reason")}, APPROVAL_LIMIT, max_depth=96)
        return ApprovalDecisionCommand._create(self._base_url, self.scope, self.id, saved["key"], body)

    async def decide_approval(self, command: ApprovalDecisionCommand) -> ApprovalDecisionReceipt:
        """Send once; reconcile uncertainty by explicitly resending these bytes."""
        if (type(command) is not ApprovalDecisionCommand or command.scope != self.scope
                or command.workflow_id != self.id or command.base_url != self._base_url):
            raise InputError("approval command belongs to another endpoint, scope, or workflow")
        try:
            return await self._client._require_transport().exchange(
                "POST", "/v1/workflows/approvals/decide", body=command._body,
                deadline=asyncio.get_running_loop().time() + self._client.request_timeout,
                parser=lambda raw: parse_approval_receipt(raw, command), limit=APPROVAL_LIMIT + 1024,
            )
        except RequestTimeout as exc:
            if not exc.dispatched:
                raise
            raise ApprovalDecisionUncertain(command, exc) from exc
        except TransportError as exc:
            raise ApprovalDecisionUncertain(command, exc) from exc

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

    def prepare_subscribe(self, *, destination: str, idempotency_key: str) -> CompletionSubscribeCommand:
        """Freeze notification registration for this workflow's terminal outcome."""
        from .completions import prepare_subscription
        return prepare_subscription(self, "workflow", destination=destination,
                                    idempotency_key=idempotency_key)

    async def subscribe(self, command: CompletionSubscribeCommand | None = None,
                        **kwargs) -> CompletionSubscriptionHandle:
        """Register durable delivery to a destination; this does not wait for completion."""
        from .completions import subscribe_handle
        return await subscribe_handle(self, "workflow", command, **kwargs)

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
