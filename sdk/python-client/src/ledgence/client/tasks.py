"""Task submission, observations and caller-owned bounded waiting."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import math
import re
from typing import Any, AsyncIterator, TYPE_CHECKING

from .admission_models import TaskAdmission
from .budgets import RunBudgetPolicy, _policy
from . import codec, otel
from .discovery import _TaskQuery
from .errors import (
    CancellationUncertain, InputError, ProtocolError, RequestTimeout, SubmissionUncertain,
    TaskCancelled, TaskFailed, TransportError, WaitTimeout,
)
from .models import (
    Cancelled, Failed, RetryPolicy, Scope, Submission, Succeeded, TaskResult, TaskState,
    TaskPage, TaskStatus, TraceContext, parse_result, parse_status,
)

if TYPE_CHECKING:
    from .client import AsyncClient
    from .progress import ExecutionProgressPage
    from .completion_models import CompletionSubscribeCommand
    from .completions import CompletionSubscriptionHandle

_UNSET = object()


def _program(program, version):
    return {"id": codec.program_component(program, "program"),
            "version": codec.program_component(version, "version")}


def _trace(raw):
    if raw is None:
        return None
    codec.fields(raw, {"traceparent"}, {"tracestate"})
    return TraceContext(**raw)


def _same(left, right):
    if type(left) is not type(right):
        return False
    if type(left) is dict:
        return left.keys() == right.keys() and all(_same(left[k], right[k]) for k in left)
    if type(left) is list:
        return len(left) == len(right) and all(_same(a, b) for a, b in zip(left, right))
    if type(left) is float and left == 0 and right == 0:
        return math.copysign(1, left) == math.copysign(1, right)
    return left == right


def _submission_reply(raw, submission: Submission) -> tuple[str, str]:
    try:
        codec.fields(raw, {
            "task_id", "run_id", "idempotency_key", "input", "descriptor", "origin_trace",
            "state", "submitted_at", "available_at", "terminal_at", "current_attempt_id",
            "attempt_count", "cancel_requested_at",
        }, {"admission_deferrals"})
        expected = submission.to_dict()
        actual = raw["input"]
        codec.fields(actual, {"tenant_id", "namespace", "queue", "program", "data",
                              "retry_policy", "attempt_timeout_ms"}, {"correlation_key", "admission", "budget"})
        if "budget" in actual:
            _policy(actual["budget"])
        if not _same(actual.get("budget"), expected["input"].get("budget")):
            raise InputError("submission reply changed run budget")
        if "admission" in actual:
            codec.fields(actual["admission"], {"resource"})
            TaskAdmission(**actual["admission"])
        if not _same(actual.get("admission"), expected["input"].get("admission")):
            raise InputError("submission reply changed task admission")
        if raw["idempotency_key"] != expected["idempotency_key"]:
            raise InputError("submission reply changed the key")
        codec.fields(actual["program"], {"id", "version"})
        _program(actual["program"]["id"], actual["program"]["version"])
        codec.fields(actual["retry_policy"], {"max_attempts", "retry_delay_ms"})
        RetryPolicy(**actual["retry_policy"])
        codec.integer(actual["attempt_timeout_ms"], "attempt_timeout_ms", 60_000, 86_400_000)
        codec.validate(actual["data"], codec.DATA_LIMIT)
        codec.encode(actual["data"], codec.DATA_LIMIT, max_depth=codec.MAX_DEPTH)
        for key, value in expected["input"].items():
            if not _same(actual.get(key), value):
                raise InputError("submission reply changed the input")
        if actual.get("correlation_key") is not None:
            codec.text(actual["correlation_key"], "correlation_key", 512, empty=True, noncharacters=True)
        if actual.get("correlation_key") != expected["input"].get("correlation_key"):
            raise InputError("submission reply changed correlation")
        codec.fields(raw["descriptor"], {"program", "digest", "size"})
        if raw["descriptor"]["program"] != actual["program"]:
            raise InputError("descriptor program differs from submitted program")
        digest = raw["descriptor"]["digest"]
        if type(digest) is not str or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise InputError("invalid descriptor digest")
        codec.integer(raw["descriptor"]["size"], "descriptor size", 1)
        _trace(raw["origin_trace"])
        # The first accepted origin wins even when a caller reconstructs a replay.
        state = TaskState(raw["state"])
        governed = "admission" in actual
        count = codec.integer(raw["attempt_count"], "attempt_count", 0,
                              (1 << 31) - 1 if governed else 1000)
        deferrals = codec.integer(raw.get("admission_deferrals", 0), "admission_deferrals", 0, count)
        if (not governed and deferrals) or count - deferrals > actual["retry_policy"]["max_attempts"]:
            raise InputError("inconsistent admission retry accounting")
        current = raw["current_attempt_id"]
        if (state == TaskState.ACTIVE) != (current is not None) or (current is not None and count == 0):
            raise InputError("invalid current attempt")
        if current is not None:
            codec.text(current, "current_attempt_id")
        if state in (TaskState.SUCCEEDED, TaskState.FAILED) and count == 0:
            raise InputError("terminal task has no attempt")
        if state.terminal != (raw["terminal_at"] is not None):
            raise InputError("invalid terminal timestamp")
        for key in ("submitted_at", "available_at", "terminal_at", "cancel_requested_at"):
            if raw[key] is not None:
                codec.integer(raw[key], key, 0, codec.MAX_TIMESTAMP)
            elif key in ("submitted_at", "available_at"):
                raise InputError("missing timestamp")
        if (state == TaskState.CANCELLED and raw["cancel_requested_at"] is None) or (
            raw["cancel_requested_at"] is not None and state not in (TaskState.ACTIVE, TaskState.CANCELLED)
        ):
            raise InputError("inconsistent cancellation metadata")
        return codec.text(raw["task_id"], "task_id"), codec.text(raw["run_id"], "run_id")
    except (ValueError, TypeError, KeyError) as exc:
        raise ProtocolError("invalid submission response") from exc


class Tasks:
    def __init__(self, client: AsyncClient):
        self._client = client

    def prepare(self, *, program: str, version: str, queue: str, data: Any,
                idempotency_key: str, correlation_key: str | None = None,
                retry_policy: RetryPolicy | None = None, attempt_timeout_ms: int | None = None,
                admission: TaskAdmission | None = None, budget: RunBudgetPolicy | None = None,
                origin_trace: TraceContext | None | object = _UNSET) -> Submission:
        """Freeze a local command before transmission, including context absence."""
        codec.text(idempotency_key, "idempotency_key", 255)
        codec.text(queue, "queue")
        codec.validate(data, codec.DATA_LIMIT)
        codec.encode(data, codec.DATA_LIMIT, max_depth=codec.MAX_DEPTH)
        payload = {"tenant_id": self._client.scope.tenant_id,
                   "namespace": self._client.scope.namespace, "queue": queue,
                   "program": _program(program, version), "data": data}
        if budget is not None:
            if type(budget) is not RunBudgetPolicy:
                raise InputError("budget must be a RunBudgetPolicy")
            payload["budget"] = budget.to_dict()
        if admission is not None:
            if type(admission) is not TaskAdmission:
                raise InputError("admission must be a TaskAdmission")
            payload["admission"] = admission.to_dict()
        if correlation_key is not None:
            payload["correlation_key"] = codec.text(correlation_key, "correlation_key", 512,
                                                     empty=True, noncharacters=True)
        if retry_policy is not None:
            if type(retry_policy) is not RetryPolicy:
                raise InputError("retry_policy must be a RetryPolicy")
            payload["retry_policy"] = {"max_attempts": retry_policy.max_attempts,
                                       "retry_delay_ms": retry_policy.retry_delay_ms}
        if attempt_timeout_ms is not None:
            payload["attempt_timeout_ms"] = codec.integer(attempt_timeout_ms, "attempt_timeout_ms",
                                                          60_000, 86_400_000)
        origin = otel._capture() if origin_trace is _UNSET else origin_trace
        if origin is not None and type(origin) is not TraceContext:
            raise InputError("origin_trace must be a TraceContext or None")
        command = {"idempotency_key": idempotency_key, "input": payload}
        if origin is not None:
            command["origin_trace"] = origin.to_dict()
        return Submission._create(self._client.base_url, self._client.scope, codec.encode(command))

    async def submit(self, submission: Submission | None = None, **kwargs) -> TaskHandle:
        """Submit keyword input or a frozen Submission; never automatically replay."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._client.request_timeout
        transport = self._client._require_transport()
        if submission is not None:
            if kwargs or type(submission) is not Submission:
                raise InputError("supply one Submission or submission keyword arguments")
            if submission.scope != self._client.scope or submission.base_url != self._client.base_url:
                raise InputError("submission belongs to a different endpoint or scope")
        else:
            try:
                submission = self.prepare(**kwargs)
            except TypeError as exc:
                raise InputError("invalid submission arguments") from exc
        try:
            task_id, run_id = await transport.exchange(
                "POST", "/v1/tasks", body=submission._body, deadline=deadline,
                parser=lambda raw: _submission_reply(raw, submission),
            )
        except RequestTimeout as exc:
            if not exc.dispatched:
                raise
            raise SubmissionUncertain(submission, exc) from exc
        except TransportError as exc:
            raise SubmissionUncertain(submission, exc) from exc
        return TaskHandle(self._client, task_id, run_id)

    async def list(self, *, state: TaskState | str | None = None,
                   queue: str | None = None, correlation_key: str | None = None,
                   submitted_from: int | None = None, submitted_until: int | None = None,
                   limit: int = 50, cursor: str | None = None) -> TaskPage:
        """Read one live page; repeat the filters when using its opaque cursor."""
        deadline = asyncio.get_running_loop().time() + self._client.request_timeout
        transport = self._client._require_transport()
        query = _TaskQuery(state=state, queue=queue, correlation_key=correlation_key,
                           submitted_from=submitted_from, submitted_until=submitted_until,
                           limit=limit, cursor=cursor)
        return await transport.exchange(
            "GET", "/v1/tasks", query=query.parameters(self._client.scope), deadline=deadline,
            parser=lambda raw: query.parse(raw, self._client.scope), limit=codec.TASK_PAGE_LIMIT,
        )

    def handle(self, task_id: str) -> TaskHandle:
        """Construct a local scoped reference; this does not check existence."""
        return TaskHandle(self._client, codec.text(task_id, "task_id"))


@dataclass(frozen=True)
class TaskHandle:
    _client: AsyncClient = field(repr=False, compare=False)
    id: str
    _run_id: str | None = field(default=None, repr=False, compare=False)
    _scope: Scope = field(init=False, repr=False)
    _base_url: str = field(init=False, repr=False)

    def __post_init__(self):
        object.__setattr__(self, "_scope", self._client.scope)
        object.__setattr__(self, "_base_url", self._client.base_url)
        codec.text(self.id, "task_id")
        if self._run_id is not None:
            codec.text(self._run_id, "run_id")

    @property
    def scope(self) -> Scope:
        return self._scope

    def _query(self):
        return {"tenant_id": self.scope.tenant_id, "namespace": self.scope.namespace,
                "task_id": self.id}

    def _check_run(self, status):
        if self._run_id is not None and status.run_id != self._run_id:
            raise ProtocolError("task run identity changed")
        return status

    async def _status(self, deadline):
        transport = self._client._require_transport()
        return await transport.exchange(
            "GET", "/v1/tasks/status", query=self._query(), deadline=deadline,
            parser=lambda raw: self._check_run(parse_status(raw, self.scope, self.id)),
            limit=codec.STATUS_LIMIT,
        )

    async def status(self) -> TaskStatus:
        return await self._status(asyncio.get_running_loop().time() + self._client.request_timeout)

    async def _outcome(self, deadline):
        def parse(raw):
            result = parse_result(raw, self.scope, self.id)
            self._check_run(result.task)
            return result
        return await self._client._require_transport().exchange(
            "GET", "/v1/tasks/result", query=self._query(), deadline=deadline, parser=parse,
        )

    async def outcome(self) -> TaskResult:
        return await self._outcome(asyncio.get_running_loop().time() + self._client.request_timeout)

    async def progress(self, *, cursor: str | None = None,
                       limit: int = 100) -> ExecutionProgressPage:
        """Read one durable lifecycle page without loading application payloads."""
        from .progress import progress
        return await progress(self, "task", cursor=cursor, limit=limit)

    def follow_progress(self, *, cursor: str | None = None,
                        poll_interval: float = 1.0, timeout: float | None = None,
                        stop_on_terminal: bool = True,
                        limit: int = 100) -> AsyncIterator[ExecutionProgressPage]:
        """Yield bounded pages; persist next_cursor after processing each page.

        By default stop at a caught-up terminal snapshot. Late cleanup may add
        events afterward; resume the cursor or set stop_on_terminal=False.
        Timeout or cancellation stops this observer only, never the execution.
        """
        from .progress import follow_progress
        return follow_progress(self, "task", cursor=cursor, poll_interval=poll_interval,
                               timeout=timeout, stop_on_terminal=stop_on_terminal, limit=limit)

    async def wait(self, timeout: float = 60.0) -> TaskResult:
        """Observe to completion without cancelling the remote task on expiration."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + codec.duration(timeout, "timeout")
        last_status = None
        last_error = None
        while loop.time() < deadline:
            try:
                if last_status is None or not last_status.state.terminal:
                    status = await self._status(min(deadline, loop.time() + self._client.request_timeout))
                    if last_status is not None and status.run_id != last_status.run_id:
                        raise ProtocolError("task run identity changed between observations")
                    last_status = status
                if last_status.state.terminal:
                    result = await self._outcome(min(deadline, loop.time() + self._client.request_timeout))
                    if result.task != last_status:
                        raise ProtocolError("terminal task metadata changed between observations")
                    if loop.time() >= deadline:
                        raise WaitTimeout(self, last_status, last_error)
                    return result
            except ProtocolError:
                raise
            except TransportError as exc:
                last_error = exc
            remaining = deadline - loop.time()
            if remaining > 0:
                await asyncio.sleep(min(1.0, remaining))
        raise WaitTimeout(self, last_status, last_error)

    async def result(self, timeout: float = 60.0) -> Any:
        """Return successful JSON output; raise task-level errors only when terminal."""
        result = await self.wait(timeout)
        if isinstance(result.outcome, Succeeded):
            return result.outcome.output
        if isinstance(result.outcome, Failed):
            raise TaskFailed(result)
        if isinstance(result.outcome, Cancelled):
            raise TaskCancelled(result)
        raise ProtocolError("terminal task has no outcome")

    def prepare_subscribe(self, *, destination: str, idempotency_key: str) -> CompletionSubscribeCommand:
        """Freeze notification registration for this task's terminal outcome."""
        from .completions import prepare_subscription
        return prepare_subscription(self, "task", destination=destination,
                                    idempotency_key=idempotency_key)

    async def subscribe(self, command: CompletionSubscribeCommand | None = None,
                        **kwargs) -> CompletionSubscriptionHandle:
        """Register durable delivery to a destination; this does not wait for completion."""
        from .completions import subscribe_handle
        return await subscribe_handle(self, "task", command, **kwargs)

    async def cancel(self) -> TaskState:
        deadline = asyncio.get_running_loop().time() + self._client.request_timeout
        body = codec.encode({"scope": {"tenant_id": self.scope.tenant_id,
                                       "namespace": self.scope.namespace}, "task_id": self.id})
        def parse(raw):
            try:
                return TaskState(raw)
            except (ValueError, TypeError) as exc:
                raise ProtocolError("invalid cancellation response") from exc
        try:
            return await self._client._require_transport().exchange(
                "POST", "/v1/tasks/cancel", body=body, deadline=deadline, parser=parse,
            )
        except RequestTimeout as exc:
            if not exc.dispatched:
                raise
            raise CancellationUncertain(self, exc) from exc
        except TransportError as exc:
            raise CancellationUncertain(self, exc) from exc
