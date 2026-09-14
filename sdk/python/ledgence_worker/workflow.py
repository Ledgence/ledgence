"""Explicit checkpoint workflows and durable local steps (MIT).

Workflow state is explicit JSON. Python frames and ordinary local variables are
not checkpointed. A local step may execute again if its effect succeeds before
its result is durably acknowledged; use an external idempotency key when needed.
"""
from __future__ import annotations

import asyncio
from contextvars import ContextVar
import inspect
import json
import math
import re
import unicodedata
from typing import Any

SCHEMA = "ledgence.workflow.activation.v1"
MAX_STEPS = 128
MAX_COMMANDS = 64
MAX_RECORD_BYTES = 128 * 1024
MAX_RECORDS_BYTES = 256 * 1024
MAX_DECISION_BYTES = 256 * 1024
MAX_STATE_BYTES = 64 * 1024
MAX_CONTEXT_BYTES = 640 * 1024


class WorkflowError(Exception):
    """A workflow binding, decision, or runtime acknowledgement is invalid."""


_workflow: ContextVar[WorkflowContext | None] = ContextVar("ledgence_workflow", default=None)
_local_owner: ContextVar[WorkflowContext | None] = ContextVar("ledgence_local_owner", default=None)


def _text(value, name, limit=128):
    if (type(value) is not str or not value or len(value.encode("utf-8")) > limit
            or any(unicodedata.category(char) == "Cc" or 0xFDD0 <= ord(char) <= 0xFDEF
                   or ord(char) & 0xFFFE == 0xFFFE for char in value)):
        raise WorkflowError(f"{name} must be a nonempty string of at most {limit} UTF-8 bytes")
    return value


def _integer(value, name, minimum=0, maximum=(1 << 64) - 1):
    if type(value) is not int or not minimum <= value <= maximum:
        raise WorkflowError(f"invalid {name}")
    return value


def _fields(value, required, optional=()):
    if type(value) is not dict or not required <= value.keys() or value.keys() - required - set(optional):
        raise WorkflowError("invalid workflow object fields")


def _encode(value, limit, max_depth=64):
    remaining = limit

    def check(item, depth=0, ancestors=None):
        nonlocal remaining
        remaining -= 1
        if remaining < 0:
            raise WorkflowError("workflow value exceeds its byte limit")
        if item is None or type(item) is bool:
            return
        if type(item) is str:
            if len(item) > remaining:
                raise WorkflowError("workflow string exceeds its byte limit")
            remaining -= len(item.encode("utf-8", errors="strict"))
            if remaining < 0:
                raise WorkflowError("workflow string exceeds its byte limit")
            return
        if type(item) is int:
            _integer(item, "JSON integer", -(1 << 63))
            return
        if type(item) is float:
            if not math.isfinite(item):
                raise WorkflowError("JSON number must be finite")
            return
        if type(item) not in (dict, list, tuple) or depth >= max_depth:
            raise WorkflowError("workflow values must be bounded JSON")
        ancestors = set() if ancestors is None else ancestors
        if id(item) in ancestors:
            raise WorkflowError("workflow values cannot contain cycles")
        ancestors.add(id(item))
        try:
            if type(item) is dict:
                for key, child in item.items():
                    if type(key) is not str:
                        raise WorkflowError("JSON keys must be strings")
                    check(key, depth + 1, ancestors)
                    check(child, depth + 1, ancestors)
            else:
                for child in item:
                    check(child, depth + 1, ancestors)
        finally:
            ancestors.remove(id(item))
    try:
        check(value)
        chunks = []
        length = 0
        encoder = json.JSONEncoder(ensure_ascii=False, allow_nan=False,
                                   separators=(",", ":"), sort_keys=True)
        for part in encoder.iterencode(value):
            chunk = part.encode("utf-8")
            length += len(chunk)
            if length > limit:
                raise WorkflowError("workflow value exceeds its byte limit")
            chunks.append(chunk)
        return b"".join(chunks)
    except (ValueError, TypeError, UnicodeError, OverflowError, RecursionError) as exc:
        raise WorkflowError("invalid workflow JSON value") from exc


def _freeze(value, limit, max_depth=64):
    return json.loads(_encode(value, limit, max_depth))


def workflow_context() -> WorkflowContext:
    """Get this activation's context; fail outside a workflow invocation."""
    context = _workflow.get()
    if context is None or context._closed:
        raise WorkflowError("no Ledgence workflow activation is active")
    return context


get_workflow_context = workflow_context


class TaskRef:
    """A staged child key; creation performs no external dispatch."""
    __slots__ = ("key", "_context")

    def __init__(self, key, context):
        self.key = key
        self._context = context


class _LocalResult:
    def __init__(self, task, observed_failures):
        self._task = task
        self._observed_failures = observed_failures

    async def _result(self):
        # A cancelled observer cannot abandon a running local operation. The
        # activation retains ownership and drains it before process reuse.
        try:
            return _freeze(await asyncio.shield(self._task), MAX_RECORD_BYTES)
        except BaseException as exc:
            if (self._task.done() and not self._task.cancelled()
                    and self._task.exception() is exc):
                self._observed_failures.add(self._task)
            raise

    def __await__(self):
        return self._result().__await__()


class WorkflowContext:
    """One explicit activation. Constructed by the worker, not application code."""

    def __init__(self, payload, rpc):
        payload = _freeze(payload, MAX_CONTEXT_BYTES, 96)
        _fields(payload, {"v", "workflow_id", "activation_id", "revision", "continuation",
                          "state", "inputs", "local_steps"})
        if type(payload["v"]) is not int or payload["v"] != 1:
            raise WorkflowError("unsupported workflow activation version")
        self.workflow_id = _text(payload["workflow_id"], "workflow_id")
        self.activation_id = _text(payload["activation_id"], "activation_id")
        self.revision = _integer(payload["revision"], "revision")
        self.continuation = _text(payload["continuation"], "continuation")
        self._state = _freeze(payload["state"], MAX_STATE_BYTES)
        self._inputs = payload["inputs"]
        if type(self._inputs) is not dict or len(self._inputs) > MAX_COMMANDS:
            raise WorkflowError("workflow inputs must be a bounded object")
        _encode(self._inputs, MAX_DECISION_BYTES, 96)
        for key, item in self._inputs.items():
            _text(key, "input key")
            _fields(item, {"task_id", "state", "outcome"})
            _text(item["task_id"], "task_id")
            if item["state"] not in ("queued", "active", "succeeded", "failed", "cancelled"):
                raise WorkflowError("invalid child task state")
        records = payload["local_steps"]
        if type(records) is not list or len(records) > MAX_STEPS:
            raise WorkflowError("too many local step records")
        _encode(records, MAX_RECORDS_BYTES, 96)
        self._records = {}
        for record in records:
            self._validate_record(record)
            if record["key"] in self._records:
                raise WorkflowError("duplicate local step record")
            self._records[record["key"]] = record
        self._rpc = rpc
        self._loop = asyncio.get_running_loop()
        self._commit_lock = asyncio.Lock()
        self._pending = {}
        self._observed_failures = set()
        self._commands = []
        self._closed = False
        self._fatal = None

    @property
    def state(self):
        return _freeze(self._state, MAX_STATE_BYTES)

    @property
    def inputs(self):
        return _freeze(self._inputs, MAX_DECISION_BYTES, 96)

    def _active(self):
        if _local_owner.get() is not None:
            raise WorkflowError("durable local callables cannot stage workflow operations; use the controller")
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError as exc:
            raise WorkflowError("workflow operations require the controller event loop") from exc
        if loop is not self._loop:
            raise WorkflowError("workflow operations require the controller event loop")
        if self._closed:
            raise WorkflowError("workflow activation is closed")
        if self._fatal is not None:
            raise WorkflowError("workflow runtime acknowledgement failed") from self._fatal

    @staticmethod
    def _validate_record(record):
        _fields(record, {"key", "callable", "input", "output"})
        _text(record["key"], "local key")
        _text(record["callable"], "local callable", 512)
        _encode(record["input"], MAX_RECORD_BYTES)
        _encode(record["output"], MAX_RECORD_BYTES)
        _encode(record, MAX_RECORD_BYTES, 96)

    def local(self, key, fn, **kwargs):
        """Start a local step; await its durably acknowledged JSON result.

        Use explicit JSON keyword arguments for every value affecting the
        operation. A callable's closure is not part of its persisted binding.
        Concurrent calls sharing a key and binding share one owned execution.
        """
        self._active()
        _text(key, "local key")
        if not callable(fn):
            raise WorkflowError("local step requires a callable")
        module = getattr(fn, "__module__", None)
        name = getattr(fn, "__qualname__", None)
        if not module or not name:
            raise WorkflowError("local callable requires a stable module and qualified name")
        binding = {"key": key, "callable": _text(module + ":" + name, "local callable", 512),
                   "input": _freeze(kwargs, MAX_RECORD_BYTES)}
        binding_bytes = _encode(binding, MAX_RECORD_BYTES, 96)
        previous = self._records.get(key)
        if previous is not None:
            actual = {field: previous[field] for field in ("key", "callable", "input")}
            if _encode(actual, MAX_RECORD_BYTES, 96) != binding_bytes:
                raise WorkflowError("local step key was reused with a different binding")
        if key in self._pending:
            known, task = self._pending[key]
            if known != binding_bytes:
                raise WorkflowError("local step key was reused with a different binding")
            return _LocalResult(task, self._observed_failures)
        if len(set(self._records) | set(self._pending)) >= MAX_STEPS and previous is None:
            raise WorkflowError("too many local steps in one activation")
        task = asyncio.create_task(self._run_local(binding, fn, previous))
        self._pending[key] = (binding_bytes, task)
        return _LocalResult(task, self._observed_failures)

    async def _run_local(self, binding, fn, previous):
        if previous is not None:
            return previous["output"]
        # Sync functions retain normal synchronous semantics. Async functions
        # can overlap I/O without allocating additional subprocesses or threads.
        token = _local_owner.set(self)
        try:
            output = fn(**_freeze(binding["input"], MAX_RECORD_BYTES))
            if inspect.isawaitable(output):
                output = await output
        finally:
            _local_owner.reset(token)
        record = dict(binding, output=_freeze(output, MAX_RECORD_BYTES))
        self._validate_record(record)
        async with self._commit_lock:
            _encode([*self._records.values(), record], MAX_RECORDS_BYTES, 96)
            try:
                result = await self._rpc("local_step.commit", record)
                if type(result) is not dict or result != {"committed": True} or type(result["committed"]) is not bool:
                    raise WorkflowError("invalid local step commit acknowledgement")
            except BaseException as exc:
                self._fatal = exc
                raise
            self._records[binding["key"]] = record
        return record["output"]

    async def gather(self, *operations):
        """Wait for all supplied local operations, retaining ownership on error."""
        self._active()
        if len(operations) > MAX_STEPS:
            raise WorkflowError("too many gathered operations")
        results = await asyncio.gather(*operations, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result
        return results

    def task(self, key, *, program, version, queue, data, retry_policy=None,
             attempt_timeout_ms=300000):
        """Stage a workflow-scoped child key for the next atomic checkpoint.

        Reusing a key reuses the original child only if its binding is identical.
        Include an iteration suffix when a loop should create another child.
        """
        self._active()
        _text(key, "child key")
        for name, value in (("program", program), ("version", version)):
            if type(value) is not str or value in (".", "..") or not re.fullmatch(r"[a-z0-9._-]{1,128}", value):
                raise WorkflowError(f"invalid {name}")
        _text(queue, "queue")
        policy = {"max_attempts": 3, "retry_delay_ms": 5000} if retry_policy is None else retry_policy
        policy = _freeze(policy, 1024)
        _fields(policy, {"max_attempts", "retry_delay_ms"})
        _integer(policy["max_attempts"], "max_attempts", 1, 1000)
        _integer(policy["retry_delay_ms"], "retry_delay_ms", 0, 86400000)
        _integer(attempt_timeout_ms, "attempt_timeout_ms", 60000, 86400000)
        command = {"key": key, "program": {"id": program, "version": version}, "queue": queue,
                   "data": _freeze(data, MAX_DECISION_BYTES), "retry_policy": policy,
                   "attempt_timeout_ms": attempt_timeout_ms}
        for previous in self._commands:
            if previous["key"] == key:
                if _encode(previous, MAX_DECISION_BYTES, 96) != _encode(command, MAX_DECISION_BYTES, 96):
                    raise WorkflowError("child key reused with a different binding")
                return TaskRef(key, self)
        if len(self._commands) >= MAX_COMMANDS:
            raise WorkflowError("too many staged child tasks")
        _encode([*self._commands, command], MAX_DECISION_BYTES, 96)
        self._commands.append(command)
        return TaskRef(key, self)

    def _key(self, ref):
        if isinstance(ref, TaskRef):
            if ref._context is not self:
                raise WorkflowError("child reference belongs to another activation")
            return ref.key
        return _text(ref, "child key")

    def get_result(self, ref):
        """Return a successful child's output; fail if absent or not successful."""
        self._active()
        key = self._key(ref)
        item = self._inputs.get(key)
        if item is None:
            raise WorkflowError("child result is absent from this activation")
        outcome = item["outcome"]
        if (item["state"] != "succeeded" or type(outcome) is not dict
                or outcome.get("kind") != "succeeded" or "output" not in outcome):
            raise WorkflowError("child did not succeed; inspect inputs for its outcome")
        return _freeze(outcome["output"], MAX_DECISION_BYTES)

    def _decision(self, kind, **fields):
        self._active()
        return _freeze({"v": 1, "activation_id": self.activation_id, "revision": self.revision,
                        "kind": kind, **fields}, MAX_DECISION_BYTES, 96)

    def suspend(self, *, continuation, state, until=()):
        """Checkpoint and wait until every specified child is terminal."""
        self._active()
        if type(until) not in (list, tuple) or len(until) > MAX_COMMANDS:
            raise WorkflowError("until must be a bounded list or tuple of child references")
        keys = [self._key(ref) for ref in until]
        if len(set(keys)) != len(keys):
            raise WorkflowError("wait references must be distinct child keys")
        # A key can refer to a child created by an earlier activation, absent
        # from this frozen input batch. The orchestrator verifies its existence.
        return self._decision("suspend", continuation=_text(continuation, "continuation"),
                              state=_freeze(state, MAX_STATE_BYTES), commands=self._commands,
                              until=keys)

    def continue_(self, *, continuation, state):
        """Commit a checkpoint and request another activation without a wait."""
        return self._decision("continue", continuation=_text(continuation, "continuation"),
                              state=_freeze(state, MAX_STATE_BYTES), commands=self._commands)

    def complete(self, output):
        _encode(output, MAX_DECISION_BYTES)
        if self._commands:
            raise WorkflowError("complete cannot discard staged child commands; checkpoint first")
        return self._decision("complete", output=output)

    def fail(self, kind, message):
        if type(message) is not str or len(message.encode("utf-8")) > 4096:
            raise WorkflowError("error message must be at most 4096 UTF-8 bytes")
        return self._decision("fail", error={"kind": _text(kind, "error kind"), "message": message})

    def _validate_decision(self, decision):
        self._active()
        common = {"v", "activation_id", "revision", "kind"}
        if type(decision) is not dict:
            raise WorkflowError("workflow handler must return a workflow decision")
        kind = decision.get("kind")
        extra = {"complete": {"output"}, "fail": {"error"},
                 "continue": {"continuation", "state", "commands"},
                 "suspend": {"continuation", "state", "commands", "until"}}.get(kind)
        if extra is None:
            raise WorkflowError("invalid workflow decision kind")
        if kind == "complete" and self._commands:
            raise WorkflowError("complete cannot discard staged child commands")
        _fields(decision, common | extra)
        if (type(decision["v"]) is not int or decision["v"] != 1
                or decision["activation_id"] != self.activation_id
                or type(decision["revision"]) is not int or decision["revision"] != self.revision):
            raise WorkflowError("workflow decision changed activation identity")
        if kind == "complete":
            _encode(decision["output"], MAX_DECISION_BYTES)
        if kind in ("suspend", "continue"):
            if (_encode(decision["commands"], MAX_DECISION_BYTES, 96)
                    != _encode(self._commands, MAX_DECISION_BYTES, 96)):
                raise WorkflowError("workflow decision does not contain the staged child commands")
            _encode(decision["state"], MAX_STATE_BYTES)
            for command in decision["commands"]:
                _encode(command["data"], MAX_DECISION_BYTES)
        return _freeze(decision, MAX_DECISION_BYTES, 96)

    async def _drain(self):
        # Retain controller-owned operations started by asynchronous controller
        # work while an earlier batch is draining. Local callables cannot stage work.
        while True:
            tasks = [task for _, task in self._pending.values()]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for task, result in zip(tasks, results):
                if isinstance(result, BaseException) and task not in self._observed_failures:
                    if isinstance(result, Exception):
                        raise result
                    raise WorkflowError("an owned local operation was cancelled") from result
            if len(tasks) == len(self._pending):
                return

    async def _finish(self, cancel=False):
        self._closed = True
        tasks = [task for _, task in self._pending.values()]
        if cancel:
            for task in tasks:
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
