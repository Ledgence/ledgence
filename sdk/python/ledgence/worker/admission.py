"""Registered callback admission and durable resource waits (MIT).

Admission bounds explicit registered callback authorizations. Hidden retries or
requests performed inside a callback require their own adapter accounting.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import re
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from time import monotonic
from types import MappingProxyType

from .workflow import (
    MAX_STATE_BYTES,
    MAX_TIMESTAMP,
    MAX_WAIT_MS,
    OperationKind,
    WorkflowError,
    _callable_name,
    _effective_arguments,
    _encode,
    _fields,
    _freeze,
    _local_owner,
    _text,
)
from .workflow import (
    _integer as _workflow_integer,
)

BINDING_BYTES = 64 * 1024
OPERATION_BYTES = BINDING_BYTES + 2048
RESULT_BYTES = 64 * 1024
CONTEXT_BYTES = 160 * 1024


@dataclass(frozen=True, slots=True)
class AdmissionOperation:
    """Frozen operation metadata and effective arguments, not a permission."""

    _encoded: bytes = field(repr=False)
    _registered: object

    def to_dict(self):
        return json.loads(self._encoded)


@dataclass(frozen=True, slots=True)
class RegisteredOperation:
    """Artifact-owned callable registered by ``Workflow.operation``."""

    _fn: object
    _kind: OperationKind
    _version: str
    _owner: object

    def __call__(self, *args, **kwargs):
        """An ordinary Python call; admission requires the workflow context."""
        return self._fn(*args, **kwargs)

    def bind(self, **arguments):
        """Freeze effective JSON arguments, including defaults, before parking."""
        if self._owner._built is None:
            raise WorkflowError(
                "build the workflow before binding an admitted operation"
            )
        _encode(arguments, BINDING_BYTES)
        effective = _freeze(
            _effective_arguments(self._fn, arguments, label="admission"), BINDING_BYTES
        )
        value = {
            "callable": _callable_name(self._fn, label="admission"),
            "kind": self._kind.value,
            "version": self._version,
            "arguments": effective,
        }
        return AdmissionOperation(_encode(value, OPERATION_BYTES, 96), self)


def register(workflow, kind, version):
    if workflow._built is not None:
        raise WorkflowError("workflow registry is frozen")
    if type(kind) is not OperationKind:
        raise WorkflowError("operation kind must be an OperationKind member")
    version = _text(version, "operation version")

    def decorate(fn):
        if workflow._built is not None:
            raise WorkflowError("workflow registry is frozen")
        name = _callable_name(fn, label="admission")
        if not inspect.isfunction(fn) or "<" in name or fn.__closure__:
            raise WorkflowError(
                "admitted operations require module-level named functions"
            )
        if name in workflow._operations:
            raise WorkflowError("duplicate registered operation")
        result = RegisteredOperation(fn, kind, version, workflow)
        workflow._operations[name] = result
        return result

    return decorate


def registry(operations):
    return MappingProxyType(dict(operations))


def validate_artifact_registry(handler, root):
    for operation in getattr(handler, "_ledgence_operations", {}).values():
        source = inspect.getsourcefile(operation._fn)
        if source is None or not Path(source).resolve().is_relative_to(root.resolve()):
            raise WorkflowError(
                "admitted operations must belong to the prepared program artifact"
            )


def validate_operation(value, *, authoritative=False):
    _fields(value, {"callable", "kind", "version", "arguments"})
    _text(value["callable"], "admission callable", 512)
    _text(value["version"], "admission version")
    if value["kind"] not in ("model", "tool") or type(value["arguments"]) is not dict:
        raise WorkflowError("invalid admitted operation")
    _encode(value["arguments"], BINDING_BYTES, authoritative=authoritative)
    _encode(value, OPERATION_BYTES, 96, authoritative=authoritative)


def bound_operation(ctx, operation, value):
    """Check the exact registered callable against authoritative saved arguments."""
    validate_operation(value, authoritative=True)
    if (
        type(operation) is not RegisteredOperation
        or ctx._operations.get(value["callable"]) is not operation
        or operation._kind.value != value["kind"]
        or operation._version != value["version"]
    ):
        raise WorkflowError("admission operation differs from its registered binding")
    effective = _effective_arguments(
        operation._fn, value["arguments"], label="admission"
    )
    if _encode(effective, BINDING_BYTES, authoritative=True) != _encode(
        value["arguments"], BINDING_BYTES, authoritative=True
    ):
        raise WorkflowError(
            "admission callable defaults differ from the saved arguments"
        )
    return operation._fn


def _integer(value, name, minimum=0, maximum=(1 << 53) - 1):
    return _workflow_integer(value, name, minimum=minimum, maximum=maximum)


def _digest(value):
    if type(value) is not str or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
        raise WorkflowError("invalid admission digest")


def _resource_name(value):
    _text(value, "resource")
    if re.fullmatch(r"[A-Za-z0-9_.:-]+", value) is None:
        raise WorkflowError("invalid resource name")


def _scope(value):
    _fields(value, {"tenant_id", "namespace"})
    for key in value:
        _text(value[key], key)


def validate_occurrence(value):
    _fields(value, {"scope", "workflow_id", "origin_activation_id", "key"})
    _scope(value["scope"])
    for key in ("workflow_id", "origin_activation_id", "key"):
        _text(value[key], key)


def validate_binding(value):
    _fields(value, {"resource", "program", "operation"})
    _resource_name(value["resource"])
    validate_operation(value["operation"], authoritative=True)
    program = value["program"]
    _fields(program, {"program", "digest", "size"})
    _fields(program["program"], {"id", "version"})
    for key in ("id", "version"):
        _text(program["program"][key], "program " + key)
    _digest(program["digest"])
    _integer(program["size"], "program size", minimum=1)
    _encode(value, BINDING_BYTES + 4096, 96, authoritative=True)


def validate_source(value):
    _fields(
        value,
        {
            "occurrence",
            "source_id",
            "start_id",
            "finish_id",
            "generation",
            "binding_digest",
            "owner",
            "worker",
            "accepted_at",
            "start_deadline_at",
        },
    )
    validate_occurrence(value["occurrence"])
    for key in ("source_id", "start_id", "finish_id"):
        _text(value[key], key)
    _integer(value["generation"], "generation", minimum=1)
    _digest(value["binding_digest"])
    owner = value["owner"]
    _fields(
        owner,
        {
            "scope",
            "task_id",
            "attempt_id",
            "lease_id",
            "generation",
            "worker_session_id",
            "consumer_id",
        },
    )
    _scope(owner["scope"])
    for key in ("task_id", "attempt_id", "lease_id", "worker_session_id"):
        _text(owner[key], key)
    _integer(owner["generation"], "lease generation", minimum=1, maximum=(1 << 32) - 1)
    _integer(owner["consumer_id"], "consumer_id", maximum=(1 << 32) - 1)
    worker = value["worker"]
    _fields(worker, {"audience", "issuer", "subject", "credential_id"})
    _fields(worker["audience"], {"scope", "authority_id", "instance_id"})
    _scope(worker["audience"]["scope"])
    for key in ("authority_id", "instance_id"):
        _text(worker["audience"][key], key)
    for key in ("issuer", "subject", "credential_id"):
        _text(worker[key], key, 256 if key == "issuer" else 128)
    if (
        not owner["scope"]
        == worker["audience"]["scope"]
        == value["occurrence"]["scope"]
    ):
        raise WorkflowError("admission source scope mismatch")
    _integer(value["accepted_at"], "accepted_at", maximum=MAX_TIMESTAMP)
    _integer(value["start_deadline_at"], "start_deadline_at", maximum=MAX_TIMESTAMP)
    if not 1 <= value["start_deadline_at"] - value["accepted_at"] <= 30_000:
        raise WorkflowError("invalid admission start window")


def validate_finish(value):
    _fields(value, {"source", "output", "accepted_at", "already_accepted"})
    validate_source(value["source"])
    _encode(value["output"], RESULT_BYTES, authoritative=True)
    _integer(value["accepted_at"], "accepted_at", maximum=MAX_TIMESTAMP)
    if (
        value["accepted_at"] < value["source"]["accepted_at"]
        or type(value["already_accepted"]) is not bool
    ):
        raise WorkflowError("invalid admission result acceptance")


def validate_offer(value):
    _fields(
        value,
        {
            "occurrence",
            "generation",
            "policy_revision",
            "activation_id",
            "expires_at",
            "binding_digest",
        },
    )
    validate_occurrence(value["occurrence"])
    for key in ("generation", "policy_revision"):
        _integer(value[key], key, minimum=1)
    _text(value["activation_id"], "offer activation")
    _integer(value["expires_at"], "offer expiry", maximum=MAX_TIMESTAMP)
    _digest(value["binding_digest"])


def validate_wake(value):
    _fields(
        value,
        {
            "kind",
            "key",
            "occurrence",
            "binding",
            "waiting_activation_id",
            "resumed_activation_id",
            "resumed_revision",
            "selected_at",
            "selection",
        },
    )
    if value["kind"] != "resource_admission":
        raise WorkflowError("invalid admission wake kind")
    validate_occurrence(value["occurrence"])
    validate_binding(value["binding"])
    for key in ("key", "waiting_activation_id", "resumed_activation_id"):
        _text(value[key], key)
    _integer(value["resumed_revision"], "resumed revision", minimum=1)
    _integer(value["selected_at"], "selection timestamp", maximum=MAX_TIMESTAMP)
    if value["waiting_activation_id"] == value["resumed_activation_id"]:
        raise WorkflowError("admission wake reuses its parking activation")
    selection = value["selection"]
    kind = selection.get("kind") if type(selection) is dict else None
    if kind == "offered":
        _fields(selection, {"kind", "offer"})
        offer = selection["offer"]
        validate_offer(offer)
        if (
            offer["occurrence"] != value["occurrence"]
            or offer["activation_id"] != value["resumed_activation_id"]
            or offer["expires_at"] <= value["selected_at"]
        ):
            raise WorkflowError("admission offer does not match selected wake")
    elif kind == "finished":
        _fields(selection, {"kind", "receipt"})
        validate_finish(selection["receipt"])
        if selection["receipt"]["source"]["occurrence"] != value["occurrence"]:
            raise WorkflowError("admission result belongs to another occurrence")
    elif kind == "closed":
        _fields(selection, {"kind", "reason"})
        if selection["reason"] not in (
            "cancelled",
            "deadline",
            "administratively_released",
        ):
            raise WorkflowError("invalid admission closure")
    elif kind == "needs_review":
        _fields(selection, {"kind", "source_id"})
        _text(selection["source_id"], "admission source")
    else:
        raise WorkflowError("invalid admission selection")
    _encode(value, CONTEXT_BYTES, 96, authoritative=True)


def validate_wait(value):
    _fields(value, {"kind", "key", "request", "timeout_ms"})
    if value["kind"] != "resource_admission":
        raise WorkflowError("invalid admission wait kind")
    _text(value["key"], "admission wait key")
    _integer(value["timeout_ms"], "admission timeout", maximum=MAX_WAIT_MS)
    request = value["request"]
    kind = request.get("kind") if type(request) is dict else None
    if kind == "new":
        _fields(request, {"kind", "resource", "operation"})
        _resource_name(request["resource"])
        validate_operation(request["operation"])
    elif kind == "existing":
        _fields(request, {"kind", "occurrence"})
        validate_occurrence(request["occurrence"])
    else:
        raise WorkflowError("invalid admission request")


class AdmissionOutcomeKind(StrEnum):
    SUCCEEDED = "succeeded"
    WAIT_REQUIRED = "wait_required"
    NEEDS_REVIEW = "needs_review"
    CLOSED = "closed"


class AdmissionClosedReason(StrEnum):
    CANCELLED = "cancelled"
    DEADLINE = "deadline"
    ADMINISTRATIVELY_RELEASED = "administratively_released"


@dataclass(frozen=True, slots=True)
class AdmissionOutcome:
    """Immutable application result; a closed/unknown operation is not success."""

    _encoded: bytes = field(repr=False)

    @property
    def kind(self) -> AdmissionOutcomeKind:
        return AdmissionOutcomeKind(json.loads(self._encoded)["kind"])

    @property
    def result(self):
        if self.kind != AdmissionOutcomeKind.SUCCEEDED:
            raise WorkflowError("admission outcome has no completed result")
        return json.loads(self._encoded)["result"]

    @property
    def reason(self) -> AdmissionClosedReason | None:
        if self.kind != AdmissionOutcomeKind.CLOSED:
            raise WorkflowError("admission outcome has no closure reason")
        value = json.loads(self._encoded).get("reason")
        return None if value is None else AdmissionClosedReason(value)


def outcome(kind, **values):
    return AdmissionOutcome(
        _encode({"kind": kind.value, **values}, RESULT_BYTES + 1024, authoritative=True)
    )


class _AdmissionAwaitable:
    def __init__(self, ctx, task):
        self.ctx, self.task = ctx, task

    async def _result(self):
        try:
            return await asyncio.shield(self.task)
        except BaseException as exc:
            if (
                self.task.done()
                and not self.task.cancelled()
                and self.task.exception() is exc
            ):
                self.ctx._observed_failures.add(self.task)
            raise

    def __await__(self):
        return self._result().__await__()


def request_admission(ctx, key, *, resource, operation, resume, state, timeout_ms):
    ctx._active()
    if not ctx._resource_admission:
        raise WorkflowError("resource admission is unavailable")
    if (
        type(operation) is not AdmissionOperation
        or ctx._operations.get(operation.to_dict()["callable"])
        is not operation._registered
    ):
        raise WorkflowError(
            "admission requires an operation registered by this workflow"
        )
    wait = {
        "kind": "resource_admission",
        "key": key,
        "request": {
            "kind": "new",
            "resource": resource,
            "operation": operation.to_dict(),
        },
        "timeout_ms": timeout_ms,
    }
    validate_wait(wait)
    return ctx._decision(
        "wait",
        continuation=ctx._target(resume),
        state=_freeze(state, MAX_STATE_BYTES),
        commands=ctx._commands,
        wait=wait,
    )


def wait_admission(ctx, *, resume, state, timeout_ms, key=None):
    ctx._active()
    if not ctx._resource_admission or ctx._admission_wake is None:
        raise WorkflowError("this activation has no admission occurrence")
    wake = ctx._admission_wake
    wait = {
        "kind": "resource_admission",
        "key": wake["key"] if key is None else key,
        "request": {"kind": "existing", "occurrence": wake["occurrence"]},
        "timeout_ms": timeout_ms,
    }
    validate_wait(wait)
    return ctx._decision(
        "wait",
        continuation=ctx._target(resume),
        state=_freeze(state, MAX_STATE_BYTES),
        commands=ctx._commands,
        wait=wait,
    )


def admitted_operation(ctx, operation):
    ctx._active()
    if not ctx._resource_admission or ctx._admission_wake is None:
        raise WorkflowError("this activation has no admitted operation")
    wake = ctx._admission_wake
    fn = bound_operation(ctx, operation, wake["binding"]["operation"])
    if ctx._admission_pending is None:
        ctx._admission_pending = asyncio.create_task(_run(ctx, fn, wake))
    return _AdmissionAwaitable(ctx, ctx._admission_pending)


def _matches_finish(wake, finish):
    validate_finish(finish)
    if finish["source"]["occurrence"] != wake["occurrence"]:
        raise WorkflowError("admission result belongs to another occurrence")
    selection = wake["selection"]
    if selection["kind"] == "offered":
        offer = selection["offer"]
        if (
            finish["source"]["binding_digest"] != offer["binding_digest"]
            or finish["source"]["generation"] != offer["generation"]
            or finish["source"]["owner"]["task_id"] != offer["activation_id"]
            or finish["source"]["accepted_at"] >= offer["expires_at"]
        ):
            raise WorkflowError("admission result differs from selected offer")

    elif selection["kind"] == "needs_review":
        if finish["source"]["source_id"] != selection["source_id"]:
            raise WorkflowError("admission result differs from selected source")
    elif selection["kind"] == "finished":
        previous = selection["receipt"]
        if (
            finish["source"] != previous["source"]
            or finish["accepted_at"] != previous["accepted_at"]
            or _encode(finish["output"], RESULT_BYTES, authoritative=True)
            != _encode(previous["output"], RESULT_BYTES, authoritative=True)
        ):
            raise WorkflowError("admission result differs from selected completion")
    elif selection["kind"] == "closed":
        raise WorkflowError("closed admission cannot preload a result")


async def _run(ctx, fn, wake):
    if ctx._admission_result is not None:
        _matches_finish(wake, ctx._admission_result)
        return outcome(
            AdmissionOutcomeKind.SUCCEEDED, result=ctx._admission_result["output"]
        )
    selected = wake["selection"]
    if selected["kind"] == "finished":
        _matches_finish(wake, selected["receipt"])
        return outcome(
            AdmissionOutcomeKind.SUCCEEDED, result=selected["receipt"]["output"]
        )
    if selected["kind"] == "closed":
        return outcome(AdmissionOutcomeKind.CLOSED, reason=selected["reason"])
    if selected["kind"] == "needs_review":
        return outcome(AdmissionOutcomeKind.NEEDS_REVIEW)
    # Prepare copied arguments before acquiring the short-lived Start authority.
    # Nothing potentially expensive should sit between the final budget check
    # and callback invocation.
    arguments = _freeze(
        wake["binding"]["operation"]["arguments"], BINDING_BYTES, authoritative=True
    )
    offer = selected["offer"]
    start_id, finish_id = "start_" + uuid.uuid4().hex, "finish_" + uuid.uuid4().hex
    begun = monotonic()
    try:
        reply = await ctx._rpc(
            "resource_admission.start.v1",
            {"start_id": start_id, "finish_id": finish_id},
        )
        if type(reply) is not dict:
            raise WorkflowError("invalid admission start receipt")
        if reply.get("kind") == "existing":
            _fields(reply, {"kind", "snapshot"})
            snapshot = reply["snapshot"]
            validate_snapshot(snapshot)
            if snapshot["occurrence"] != wake["occurrence"] or _encode(
                snapshot["binding"], BINDING_BYTES + 4096, 96, authoritative=True
            ) != _encode(wake["binding"], BINDING_BYTES + 4096, 96, authoritative=True):
                raise WorkflowError("admission snapshot binding mismatch")
            if snapshot["finish"] is not None:
                _matches_finish(wake, snapshot["finish"])
                return outcome(
                    AdmissionOutcomeKind.SUCCEEDED, result=snapshot["finish"]["output"]
                )
            if snapshot["state"] in ("possibly_dispatched", "unknown_occupied"):
                return outcome(AdmissionOutcomeKind.NEEDS_REVIEW)
            if snapshot["state"] in (
                "administratively_released",
                "closed_before_start",
            ):
                return outcome(
                    AdmissionOutcomeKind.CLOSED,
                    reason=(
                        "administratively_released"
                        if snapshot["state"] == "administratively_released"
                        else None
                    ),
                )
            return outcome(AdmissionOutcomeKind.WAIT_REQUIRED)
        _fields(reply, {"kind", "source"})
        if reply["kind"] != "granted":
            raise WorkflowError("invalid admission permission")
        source = reply["source"]
        validate_source(source)
        if (
            source["occurrence"] != wake["occurrence"]
            or source["generation"] != offer["generation"]
            or source["binding_digest"] != offer["binding_digest"]
            or source["owner"]["task_id"] != ctx.activation_id
            or source["start_id"] != start_id
            or source["finish_id"] != finish_id
        ):
            raise WorkflowError("admission grant identity mismatch")
        if (monotonic() - begun) * 1000 >= source["start_deadline_at"] - source[
            "accepted_at"
        ]:
            raise WorkflowError("admission start permission expired before callback")
    except BaseException as exc:
        ctx._fatal = exc
        raise
    token = _local_owner.set(ctx)
    try:
        result = fn(**arguments)
        if inspect.isawaitable(result):
            result = await result
        result = _freeze(result, RESULT_BYTES)
    finally:
        _local_owner.reset(token)
    try:
        receipt = await ctx._rpc(
            "resource_admission.finish.v1", {"source": source, "output": result}
        )
        validate_finish(receipt)
        if receipt["source"] != source or _encode(
            receipt["output"], RESULT_BYTES, authoritative=True
        ) != _encode(result, RESULT_BYTES, authoritative=True):
            raise WorkflowError("admission Finish receipt mismatch")
        ctx._admission_result = _freeze(receipt, CONTEXT_BYTES, 96, authoritative=True)
        return outcome(AdmissionOutcomeKind.SUCCEEDED, result=receipt["output"])
    except BaseException as exc:
        ctx._fatal = exc
        raise


def validate_snapshot(value):
    _fields(
        value,
        {
            "occurrence",
            "binding",
            "state",
            "revision",
            "created_at",
            "updated_at",
            "deadline_at",
            "offer",
            "source",
            "finish",
        },
    )
    validate_occurrence(value["occurrence"])
    validate_binding(value["binding"])
    if value["state"] not in (
        "waiting",
        "offered",
        "awaiting_rewait",
        "possibly_dispatched",
        "unknown_occupied",
        "finished",
        "closed_before_start",
        "administratively_released",
    ):
        raise WorkflowError("invalid admission state")
    _integer(value["revision"], "admission revision", minimum=1)
    for key in ("created_at", "updated_at", "deadline_at"):
        _integer(value[key], key, maximum=MAX_TIMESTAMP)
    if (
        value["updated_at"] < value["created_at"]
        or value["deadline_at"] < value["created_at"]
    ):
        raise WorkflowError("invalid admission timestamps")
    if value["offer"] is not None:
        validate_offer(value["offer"])
        if value["offer"]["occurrence"] != value["occurrence"]:
            raise WorkflowError("foreign admission offer")
    if value["source"] is not None:
        validate_source(value["source"])
        if (
            value["source"]["occurrence"] != value["occurrence"]
            or value["source"]["accepted_at"] > value["updated_at"]
        ):
            raise WorkflowError("foreign admission source")
    if value["finish"] is not None:
        validate_finish(value["finish"])
        if (
            value["finish"]["source"] != value["source"]
            or value["finish"]["accepted_at"] > value["updated_at"]
        ):
            raise WorkflowError("foreign admission result")
    shape = tuple(value[key] is not None for key in ("offer", "source", "finish"))
    expected = {
        "waiting": (False, False, False),
        "awaiting_rewait": (False, False, False),
        "closed_before_start": (False, False, False),
        "offered": (True, False, False),
        "possibly_dispatched": (False, True, False),
        "unknown_occupied": (False, True, False),
        "finished": (False, True, True),
    }
    if value["state"] == "administratively_released":
        if shape[:2] != (False, True):
            raise WorkflowError("inconsistent released admission")
    elif shape != expected[value["state"]]:
        raise WorkflowError("inconsistent admission result state")
    _encode(value, CONTEXT_BYTES, 96, authoritative=True)
