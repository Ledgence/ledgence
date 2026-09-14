"""Explicit checkpoint workflows and durable local steps (MIT).

Workflow state is explicit JSON. Python frames and ordinary local variables are
not checkpointed. A local step may execute again if its effect succeeds before
its result is durably acknowledged; use an external idempotency key when needed.
"""
from __future__ import annotations

import asyncio
import calendar
import ipaddress
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
MAX_WAIT_MS = 31_536_000_000
MAX_EVENT_BYTES = 64 * 1024
MAX_TIMESTAMP = 253402300799999


class WorkflowError(Exception):
    """A workflow binding, decision, or runtime acknowledgement is invalid."""


_workflow: ContextVar[WorkflowContext | None] = ContextVar("ledgence_workflow", default=None)
_local_owner: ContextVar[WorkflowContext | None] = ContextVar("ledgence_local_owner", default=None)


def _text(value, name, limit=128):
    try:
        raw = value.encode("utf-8") if type(value) is str else b""
    except UnicodeError as exc:
        raise WorkflowError(f"{name} must contain Unicode scalar values") from exc
    if (type(value) is not str or not value or len(raw) > limit
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


def _encode(value, limit, max_depth=64, *, authoritative=False):
    remaining = limit
    float_slack = 0

    def check(item, depth=0, ancestors=None):
        nonlocal remaining, float_slack
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
            if authoritative:
                # Rust's canonical finite float token uses at least three
                # bytes. CPython JSON emits float.__repr__ for this exact type.
                # Allow only that per-token possible expansion when copying
                # already accepted values; authored writes remain strict.
                width = len(repr(item))
                if width > 32:
                    raise WorkflowError("unsupported floating-point representation")
                float_slack += max(0, width - 3)
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
        encoded_limit = limit + float_slack
        chunks = []
        length = 0
        encoder = json.JSONEncoder(ensure_ascii=False, allow_nan=False,
                                   separators=(",", ":"), sort_keys=True)
        for part in encoder.iterencode(value):
            chunk = part.encode("utf-8")
            length += len(chunk)
            if length > encoded_limit:
                raise WorkflowError("workflow value exceeds its byte limit")
            chunks.append(chunk)
        return b"".join(chunks)
    except (ValueError, TypeError, UnicodeError, OverflowError, RecursionError) as exc:
        raise WorkflowError("invalid workflow JSON value") from exc


def _freeze(value, limit, max_depth=64, *, authoritative=False):
    return json.loads(_encode(value, limit, max_depth, authoritative=authoritative))


def _validate_event_uri(value, *, absolute=False):
    """Validate RFC 3986 URI references without rewriting their identity."""
    atom = r"(?:[A-Za-z0-9._~!$&'()*+,;=\-]|%[0-9A-Fa-f]{2})"
    pchar = rf"(?:{atom}|[:@])"
    path_query, fragment_mark, fragment = value.partition("#")
    if absolute and fragment_mark:
        raise WorkflowError("dataschema must be an absolute URI without a fragment")
    path, query_mark, query = path_query.partition("?")
    if not re.fullmatch(rf"(?:{pchar}|[/?])*", fragment) or not re.fullmatch(rf"(?:{pchar}|[/?])*", query):
        raise WorkflowError("invalid CloudEvent URI query or fragment")
    scheme = re.match(r"[A-Za-z][A-Za-z0-9+.-]*:", path)
    if absolute and scheme is None:
        raise WorkflowError("dataschema must be an absolute URI")
    if scheme is not None:
        path = path[scheme.end():]
    authority = path.startswith("//")
    if authority:
        host, slash, rest = path[2:].partition("/")
        path = slash + rest
        if "@" in host:
            user, _, host = host.partition("@")
            if not re.fullmatch(rf"(?:{atom}|:)*", user):
                raise WorkflowError("invalid CloudEvent URI user information")
        if host.startswith("["):
            address, close, port = host[1:].partition("]")
            if not close or (port and not re.fullmatch(r":[0-9]*", port)):
                raise WorkflowError("invalid CloudEvent URI host")
            if re.fullmatch(r"v[0-9A-Fa-f]+\.[A-Za-z0-9._~!$&'()*+,;=:\-]+", address, re.IGNORECASE) is None:
                try:
                    if "%" in address:
                        raise ValueError("zone identifier is not an RFC3986 address")
                    ipaddress.IPv6Address(address)
                except ValueError as exc:
                    raise WorkflowError("invalid CloudEvent URI address") from exc
        else:
            name, colon, port = host.partition(":")
            if not re.fullmatch(rf"{atom}*", name) or (colon and not re.fullmatch(r"[0-9]*", port)):
                raise WorkflowError("invalid CloudEvent URI host or port")
    if not re.fullmatch(rf"(?:{pchar}|/)*", path):
        raise WorkflowError("invalid CloudEvent URI path")
    if scheme is None and not authority and ":" in path.partition("/")[0]:
        raise WorkflowError("relative CloudEvent URI first segment cannot contain a colon")


def _validate_event_trace(event):
    trace = event.get("traceparent")
    if trace is not None and (type(trace) is not str
            or not re.fullmatch(r"00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}", trace)
            or trace[3:35] == "0" * 32 or trace[36:52] == "0" * 16):
        raise WorkflowError("invalid CloudEvent traceparent")
    if "tracestate" not in event:
        return
    state = event["tracestate"]
    if trace is None or type(state) is not str or not state.isascii() or len(state) > 512:
        raise WorkflowError("invalid CloudEvent tracestate")
    members = state.split(",")
    if len(members) > 32:
        raise WorkflowError("too many CloudEvent tracestate members")
    seen = set()
    for member in members:
        member = member.strip(" ")
        if not member:
            continue
        key, equal, item = member.partition("=")
        if (not equal or not item or len(item) > 256 or key in seen
                or not re.fullmatch(r"(?:[a-z][a-z0-9_*/-]{0,255}|[a-z0-9][a-z0-9_*/-]{0,240}@[a-z][a-z0-9_*/-]{0,13})", key)
                or any(not 0x20 <= ord(char) <= 0x7e or char in ",=" for char in item)):
            raise WorkflowError("invalid CloudEvent tracestate member")
        seen.add(key)


def _valid_event_leap_second(year, month, day, hour, minute, offset_minutes):
    # Match time's final-UTC-second-of-a-month stand-in. Integer/calendar
    # arithmetic preserves year0000 and rollover into UTC year-1, unlike datetime.
    day_change, utc_minute = divmod(hour * 60 + minute - offset_minutes, 24 * 60)
    if utc_minute != 23 * 60 + 59:
        return False
    day += day_change
    if day < 1:
        month -= 1
        if month == 0:
            year, month = year - 1, 12
        day += calendar.monthrange(year, month)[1]
    elif day > calendar.monthrange(year, month)[1]:
        day -= calendar.monthrange(year, month)[1]
        month += 1
        if month == 13:
            year, month = year + 1, 1
    return -9999 <= year <= 9999 and day == calendar.monthrange(year, month)[1]


def _validate_event(value, *, authoritative=False):
    # Keep this portable common profile aligned with worker-api's borrowed
    # CloudEvent validator. Execution identity requirements do not apply here.
    _encode(value, MAX_EVENT_BYTES, 96, authoritative=authoritative)
    if type(value) is not dict:
        raise WorkflowError("CloudEvent must be an object")
    for name, item in value.items():
        if name == "data":
            continue
        if re.fullmatch(r"[a-z0-9]+", name) is None:
            raise WorkflowError("invalid CloudEvent context name")
        if type(item) is str:
            if any(unicodedata.category(c) == "Cc" or 0xFDD0 <= ord(c) <= 0xFDEF
                   or ord(c) & 0xFFFE == 0xFFFE for c in item):
                raise WorkflowError("invalid CloudEvent context string")
        elif type(item) is not bool and (type(item) is not int or not -(1 << 31) <= item < (1 << 31)):
            raise WorkflowError("invalid CloudEvent context value")
    if value.get("specversion") != "1.0" or value.get("datacontenttype") != "application/json" or "data" not in value:
        raise WorkflowError("CloudEvent requires version1.0 and JSON data")
    for name in ("id", "source", "type"):
        if type(value.get(name)) is not str or not value[name]:
            raise WorkflowError(f"CloudEvent requires nonempty {name}")
    if len(value["id"].encode("utf-8")) > 128 or len(value["source"].encode("utf-8")) > 2048:
        raise WorkflowError("CloudEvent id/source exceed their UTF-8 byte limits")
    _validate_event_uri(value["source"])
    for name in ("subject", "dataschema", "time"):
        if name in value and (type(value[name]) is not str or not value[name]):
            raise WorkflowError(f"CloudEvent {name} must be a nonempty string")
    if "dataschema" in value:
        _validate_event_uri(value["dataschema"], absolute=True)
    if "time" in value:
        match = re.fullmatch(r"([0-9]{4})-([0-9]{2})-([0-9]{2})[Tt]([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.[0-9]+)?(?:[Zz]|([+-])([0-9]{2}):([0-9]{2}))", value["time"])
        if match is None:
            raise WorkflowError("CloudEvent time must be RFC3339")
        year, month, day, hour, minute, second = map(int, match.groups()[:6])
        if (not 1 <= month <= 12 or not 1 <= day <= calendar.monthrange(year, month)[1]
                or hour > 23 or minute > 59 or second > 60
                or (match[7] is not None and (int(match[8]) > 23 or int(match[9]) > 59))):
            raise WorkflowError("invalid CloudEvent RFC3339 timestamp")
        offset = 0 if match[7] is None else (int(match[8]) * 60 + int(match[9]))
        if match[7] == "-":
            offset = -offset
        if second == 60 and not _valid_event_leap_second(year, month, day, hour, minute, offset):
            raise WorkflowError("CloudEvent leap second must be the final UTC second of a month")
    _validate_event_trace(value)
    _encode(value["data"], MAX_EVENT_BYTES, authoritative=authoritative)
    return value


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
            return _freeze(await asyncio.shield(self._task), MAX_RECORD_BYTES, authoritative=True)
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
        payload = _freeze(payload, MAX_CONTEXT_BYTES, 96, authoritative=True)
        _fields(payload, {"v", "workflow_id", "activation_id", "revision", "continuation",
                          "state", "inputs", "local_steps"}, {"wake"})
        if type(payload["v"]) is not int or payload["v"] != 1:
            raise WorkflowError("unsupported workflow activation version")
        self.workflow_id = _text(payload["workflow_id"], "workflow_id")
        self.activation_id = _text(payload["activation_id"], "activation_id")
        self.revision = _integer(payload["revision"], "revision")
        self.continuation = _text(payload["continuation"], "continuation")
        self._state = _freeze(payload["state"], MAX_STATE_BYTES, authoritative=True)
        self._inputs = payload["inputs"]
        if type(self._inputs) is not dict or len(self._inputs) > MAX_COMMANDS:
            raise WorkflowError("workflow inputs must be a bounded object")
        self._wake = payload.get("wake")
        if self._wake is not None:
            self._validate_wake(self._wake)
        # Preserve old payloads; wake and child inputs share one bounded batch.
        combined = self._inputs if self._wake is None else {"inputs": self._inputs, "wake": self._wake}
        _encode(combined, MAX_DECISION_BYTES, 96, authoritative=True)
        for key, item in self._inputs.items():
            _text(key, "input key")
            _fields(item, {"task_id", "state", "outcome"})
            _text(item["task_id"], "task_id")
            if item["state"] not in ("queued", "active", "succeeded", "failed", "cancelled"):
                raise WorkflowError("invalid child task state")
        records = payload["local_steps"]
        if type(records) is not list or len(records) > MAX_STEPS:
            raise WorkflowError("too many local step records")
        _encode(records, MAX_RECORDS_BYTES, 96, authoritative=True)
        self._records = {}
        for record in records:
            self._validate_record(record, authoritative=True)
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
        return _freeze(self._state, MAX_STATE_BYTES, authoritative=True)

    @property
    def inputs(self):
        return _freeze(self._inputs, MAX_DECISION_BYTES, 96, authoritative=True)

    @property
    def wake(self):
        """Return the event, timeout, or timer that resumed this activation."""
        return _freeze(self._wake, MAX_DECISION_BYTES, 96, authoritative=True)

    @staticmethod
    def _validate_wake(wake):
        if type(wake) is not dict:
            raise WorkflowError("invalid workflow wake")
        kind = wake.get("kind")
        if kind == "event":
            _fields(wake, {"kind", "key", "event", "accepted_at"})
            _validate_event(wake["event"], authoritative=True)
            _integer(wake["accepted_at"], "accepted_at", maximum=MAX_TIMESTAMP)
        elif kind in ("timeout", "timer"):
            _fields(wake, {"kind", "key", "deadline"})
            _integer(wake["deadline"], "deadline", maximum=MAX_TIMESTAMP)
        else:
            raise WorkflowError("invalid workflow wake kind")
        _text(wake["key"], "wake key")

    @staticmethod
    def _validate_wait(wait):
        if type(wait) is not dict:
            raise WorkflowError("invalid workflow wait")
        kind = wait.get("kind")
        if kind == "event":
            _fields(wait, {"kind", "key", "timeout_ms"})
            if wait["timeout_ms"] is not None:
                _integer(wait["timeout_ms"], "timeout_ms", maximum=MAX_WAIT_MS)
        elif kind == "timer":
            _fields(wait, {"kind", "key", "delay_ms"})
            _integer(wait["delay_ms"], "delay_ms", maximum=MAX_WAIT_MS)
        else:
            raise WorkflowError("invalid workflow wait kind")
        _text(wait["key"], "wait key")

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
    def _validate_record(record, *, authoritative=False):
        _fields(record, {"key", "callable", "input", "output"})
        _text(record["key"], "local key")
        _text(record["callable"], "local callable", 512)
        _encode(record["input"], MAX_RECORD_BYTES, authoritative=authoritative)
        _encode(record["output"], MAX_RECORD_BYTES, authoritative=authoritative)
        _encode(record, MAX_RECORD_BYTES, 96, authoritative=authoritative)

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
        previous = self._records.get(key)
        # Exact replay may copy an accepted Rust-size-boundary binding whose
        # Python float text is longer. A new binding never receives this slack.
        binding = {"key": key, "callable": _text(module + ":" + name, "local callable", 512),
                   "input": _freeze(kwargs, MAX_RECORD_BYTES, authoritative=previous is not None)}
        binding_bytes = _encode(binding, MAX_RECORD_BYTES, 96, authoritative=previous is not None)
        if previous is not None:
            actual = {field: previous[field] for field in ("key", "callable", "input")}
            if _encode(actual, MAX_RECORD_BYTES, 96, authoritative=True) != binding_bytes:
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
        return _freeze(outcome["output"], MAX_DECISION_BYTES, authoritative=True)

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

    def wait_event(self, key, *, continuation, state, timeout_ms=None):
        """Checkpoint and wait for one external event, optionally until a deadline.

        The key is one-shot across the workflow run. Return this decision from
        the controller; preparing it does not dispatch or wait in Python.
        """
        self._active()
        wait = {"kind": "event", "key": key, "timeout_ms": timeout_ms}
        self._validate_wait(wait)
        return self._decision("wait", continuation=_text(continuation, "continuation"),
                              state=_freeze(state, MAX_STATE_BYTES),
                              commands=self._commands, wait=wait)

    def sleep(self, key, delay_ms, *, continuation, state):
        """Checkpoint a durable timer and release this invocation's worker slot.

        Return this decision. The orchestrator owns the timer; this method does
        not block, sleep, or keep a Python process alive until the deadline.
        """
        self._active()
        wait = {"kind": "timer", "key": key, "delay_ms": delay_ms}
        self._validate_wait(wait)
        return self._decision("wait", continuation=_text(continuation, "continuation"),
                              state=_freeze(state, MAX_STATE_BYTES),
                              commands=self._commands, wait=wait)

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
        if type(kind) is not str:
            raise WorkflowError("invalid workflow decision kind")
        extra = {"complete": {"output"}, "fail": {"error"},
                 "continue": {"continuation", "state", "commands"},
                 "suspend": {"continuation", "state", "commands", "until"},
                 "wait": {"continuation", "state", "commands", "wait"}}.get(kind)
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
        if kind in ("suspend", "continue", "wait"):
            _text(decision["continuation"], "continuation")
            if (_encode(decision["commands"], MAX_DECISION_BYTES, 96)
                    != _encode(self._commands, MAX_DECISION_BYTES, 96)):
                raise WorkflowError("workflow decision does not contain the staged child commands")
            _encode(decision["state"], MAX_STATE_BYTES)
            for command in decision["commands"]:
                _encode(command["data"], MAX_DECISION_BYTES)
        if kind == "wait":
            self._validate_wait(decision["wait"])
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
