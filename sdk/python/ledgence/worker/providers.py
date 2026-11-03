"""Approval-bound provider adapters and durable, bounded recovery (MIT).

Adapters classify provider responses. Ledgence preserves that evidence and
coordinates read-only probes without re-dispatching an uncertain mutation.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import StrEnum
import hashlib
import inspect
import json
from pathlib import Path
import re
from time import monotonic
from types import MappingProxyType
import uuid

from .workflow import (
    ApprovalAction, ApprovalProtection, ApprovalStatus, EffectOccurrence,
    EffectResolutionOutcome, MAX_APPROVAL_BYTES, MAX_TIMESTAMP, MAX_WAIT_MS,
    WorkflowContext, WorkflowError, _callable_name, _encode, _fields, _freeze,
    _integer, _local_owner, _text,
)

PROBE_SCHEMA = "ledgence.provider.probe.v1"
BINDING_REQUEST_BYTES = 12 * 1024
BINDING_BYTES = 16 * 1024
REPORT_BYTES = 144 * 1024
WAKE_BYTES = 160 * 1024


class ProviderLookup(StrEnum):
    BY_BINDING = "by_binding"
    BY_REFERENCE = "by_reference"
    UNAVAILABLE = "unavailable"


class ProviderKnowledgeKind(StrEnum):
    UNKNOWN = "unknown"
    PENDING = "pending"
    REQUIRES_ACTION = "requires_action"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ProviderSelectionKind(StrEnum):
    PROVIDER_SUCCEEDED = "provider_succeeded"
    PROVIDER_FAILED = "provider_failed"
    PROVIDER_CANCELLED = "provider_cancelled"
    OPERATOR_ASSESSMENT = "operator_assessment"
    DISPUTED = "disputed"
    NEEDS_REVIEW = "needs_review"


def _digest(value):
    if type(value) is not str or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
        raise WorkflowError("invalid provider digest")


def _dimensions(value):
    if type(value) is not dict or len(value) > 8:
        raise WorkflowError("provider dimensions require at most eight pairs")
    for key, item in value.items():
        _text(key, "dimension key")
        _text(item, "dimension value", 256)


def _target(value):
    _fields(value, {"provider", "environment", "account", "api_variant", "operation",
                    "endpoint_identity", "dimensions"})
    for key in ("provider", "api_variant", "operation"):
        _text(value[key], key)
    for key, limit in (("environment", 64), ("account", 256), ("endpoint_identity", 512)):
        _text(value[key], key, limit)
    _dimensions(value["dimensions"])


def _validate_binding(value, *, resolved):
    fields = {"v", "business", "contract", "target", "deduplication",
              "credential_reference", "reconciliation"}
    version = value.get("v") if type(value) is dict else None
    if type(version) is not int or version not in (1, 2):
        raise WorkflowError("unsupported provider binding version")
    _fields(value, fields | ({"program"} if resolved else set()) | ({"admission"} if version == 2 else set()))
    if version == 2:
        ProviderAdmissionRequirement.from_dict(value["admission"])
    _fields(value["business"], {"namespace", "key"})
    _text(value["business"]["namespace"], "business namespace")
    _text(value["business"]["key"], "business key", 256)
    contract = value["contract"]
    _fields(contract, {"adapter_id", "version", "digest", "completion_criterion",
                       "dispatch_callable", "reconcile_callable", "lookup"})
    for key in ("adapter_id", "version"):
        _text(contract[key], key)
    _digest(contract["digest"])
    _text(contract["completion_criterion"], "completion criterion", 1024)
    if not contract["completion_criterion"].strip():
        raise WorkflowError("provider completion criterion cannot be blank")
    _text(contract["dispatch_callable"], "dispatch callable", 512)
    lookup = contract["lookup"]
    if lookup == ProviderLookup.UNAVAILABLE:
        if contract["reconcile_callable"] is not None:
            raise WorkflowError("unavailable lookup cannot register a probe")
    elif lookup in (ProviderLookup.BY_BINDING, ProviderLookup.BY_REFERENCE):
        _text(contract["reconcile_callable"], "reconcile callable", 512)
        if contract["reconcile_callable"] == contract["dispatch_callable"]:
            raise WorkflowError("provider probe must differ from dispatch")
    else:
        raise WorkflowError("invalid provider lookup")
    _target(value["target"])
    dedup = value["deduplication"]
    if dedup is not None:
        _fields(dedup, {"domain", "key", "horizon"})
        _fields(dedup["domain"], {"provider", "namespace", "dimensions"})
        for key in ("provider", "namespace"):
            _text(dedup["domain"][key], "deduplication " + key)
        _dimensions(dedup["domain"]["dimensions"])
        if dedup["domain"]["provider"] != value["target"]["provider"]:
            raise WorkflowError("deduplication domain belongs to another provider")
        _text(dedup["key"], "deduplication key", 256)
        horizon = dedup["horizon"]
        if type(horizon) is not dict:
            raise WorkflowError("invalid deduplication horizon")
        if horizon.get("kind") == "minimum_duration":
            _fields(horizon, {"kind", "duration_ms"})
            _integer(horizon["duration_ms"], "deduplication duration", minimum=1, maximum=MAX_WAIT_MS)
        elif horizon.get("kind") in ("unknown", "resource_lifetime"):
            _fields(horizon, {"kind"})
        else:
            raise WorkflowError("invalid deduplication horizon")
    _text(value["credential_reference"], "credential reference", 256)
    policy = value["reconciliation"]
    _fields(policy, {"queue", "initial_delay_ms", "interval_ms", "maximum_interval_ms",
                     "max_checks", "max_elapsed_ms", "check_timeout_ms"})
    _text(policy["queue"], "probe queue")
    for key in ("initial_delay_ms", "interval_ms", "maximum_interval_ms"):
        _integer(policy[key], key, minimum=1000, maximum=86_400_000)
    _integer(policy["max_checks"], "max_checks", minimum=1, maximum=256)
    _integer(policy["max_elapsed_ms"], "max_elapsed_ms", minimum=60_000, maximum=604_800_000)
    _integer(policy["check_timeout_ms"], "check_timeout_ms", minimum=1000, maximum=30_000)
    if (policy["maximum_interval_ms"] < policy["interval_ms"]
            or policy["initial_delay_ms"] >= policy["max_elapsed_ms"]):
        raise WorkflowError("invalid provider reconciliation intervals")
    if resolved:
        program = value["program"]
        _fields(program, {"program", "digest", "size"})
        _fields(program["program"], {"id", "version"})
        for key in ("id", "version"):
            _text(program["program"][key], "program " + key)
        _digest(program["digest"])
        _integer(program["size"], "program size", minimum=1)
    _encode(value, BINDING_BYTES if resolved else BINDING_REQUEST_BYTES, 96, authoritative=resolved)


@dataclass(frozen=True, slots=True)
class ProviderAdmissionRequirement:
    """Named shared resource frozen into the action reviewed for dispatch."""
    resource: str

    def __post_init__(self):
        from .admission import _resource_name
        _resource_name(self.resource)

    @classmethod
    def from_dict(cls, value):
        _fields(value, {"resource"})
        return cls(value["resource"])

    def to_dict(self):
        return {"resource": self.resource}


@dataclass(frozen=True, slots=True)
class ProviderBindingRequest:
    """Frozen proposed binding; the backend adds the immutable program descriptor."""
    _encoded: bytes

    @classmethod
    def from_dict(cls, value):
        _validate_binding(value, resolved=False)
        return cls(_encode(value, BINDING_REQUEST_BYTES, 96))

    def to_dict(self):
        return json.loads(self._encoded)


@dataclass(frozen=True, slots=True)
class ProviderOperationTicket:
    """Durable occurrence identity. A ticket does not assert a provider outcome."""
    _encoded: bytes

    @classmethod
    def from_dict(cls, value):
        _fields(value, {"occurrence", "binding_digest"})
        EffectOccurrence.from_dict(value["occurrence"])
        _digest(value["binding_digest"])
        return cls(_encode(value, 8192))

    def to_dict(self):
        return json.loads(self._encoded)

    @property
    def occurrence(self):
        return EffectOccurrence.from_dict(self.to_dict()["occurrence"])

    @property
    def binding_digest(self):
        return self.to_dict()["binding_digest"]


@dataclass(frozen=True, slots=True)
class ProviderReport:
    """One classified response, including the target actually contacted."""
    _encoded: bytes

    @classmethod
    def from_dict(cls, value):
        _fields(value, {"reported_target", "provider_reference", "provider_request_id", "knowledge"})
        _target(value["reported_target"])
        for key in ("provider_reference", "provider_request_id"):
            if value[key] is not None:
                _text(value[key], key, 1024)
        knowledge = value["knowledge"]
        if type(knowledge) is not dict:
            raise WorkflowError("invalid provider knowledge")
        kind = knowledge.get("kind")
        if kind in ("unknown", "cancelled"):
            _fields(knowledge, {"kind", "reason"})
            _text(knowledge["reason"], "provider reason", 4096)
        elif kind == "pending":
            _fields(knowledge, {"kind"})
        elif kind == "requires_action":
            _fields(knowledge, {"kind", "action_kind"})
            _text(knowledge["action_kind"], "provider action kind")
        elif kind == "succeeded":
            _fields(knowledge, {"kind", "result"})
            _encode(knowledge["result"], 128 * 1024)
        elif kind == "failed":
            _fields(knowledge, {"kind", "failure"})
            WorkflowContext._validate_error(knowledge["failure"], application=True)
        else:
            raise WorkflowError("invalid provider knowledge kind")
        return cls(_encode(value, REPORT_BYTES, 96))

    @classmethod
    def _make(cls, knowledge, *, target, provider_reference=None, provider_request_id=None):
        return cls.from_dict({"reported_target": target, "provider_reference": provider_reference,
                              "provider_request_id": provider_request_id, "knowledge": knowledge})

    @classmethod
    def unknown(cls, reason, **source):
        return cls._make({"kind": "unknown", "reason": reason}, **source)

    @classmethod
    def pending(cls, **source):
        return cls._make({"kind": "pending"}, **source)

    @classmethod
    def requires_action(cls, action_kind, **source):
        return cls._make({"kind": "requires_action", "action_kind": action_kind}, **source)

    @classmethod
    def succeeded(cls, result, **source):
        return cls._make({"kind": "succeeded", "result": result}, **source)

    @classmethod
    def failed(cls, kind, message, **source):
        return cls._make({"kind": "failed", "failure": {"kind": kind, "message": message}}, **source)

    @classmethod
    def cancelled(cls, reason, **source):
        return cls._make({"kind": "cancelled", "reason": reason}, **source)

    def to_dict(self):
        return json.loads(self._encoded)

    @property
    def kind(self):
        return ProviderKnowledgeKind(self.to_dict()["knowledge"]["kind"])


class ProviderCallCompletion(StrEnum):
    COMPLETED = "completed"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ProviderDispatchOutcome:
    """Original call completion, independent of the reported business outcome."""
    _encoded: bytes = field(repr=False)

    @classmethod
    def from_dict(cls, value):
        _fields(value, {"call_completion", "report"})
        if value["call_completion"] not in ("completed", "unknown"):
            raise WorkflowError("invalid original provider call completion")
        report = ProviderReport.from_dict(value["report"])
        return cls(_encode({"call_completion": value["call_completion"], "report": report.to_dict()}, REPORT_BYTES + 1024, 96))

    @classmethod
    def _make(cls, completion, report):
        if type(report) is not ProviderReport:
            raise WorkflowError("dispatch outcome requires a ProviderReport")
        return cls.from_dict({"call_completion": completion, "report": report.to_dict()})

    @classmethod
    def completed(cls, report):
        """The original call finished; Pending still means business work remains."""
        return cls._make("completed", report)

    @classmethod
    def unknown(cls, report):
        """Business knowledge does not prove original call completion."""
        return cls._make("unknown", report)

    @property
    def completion(self):
        return ProviderCallCompletion(self.to_dict()["call_completion"])

    @property
    def report(self):
        return ProviderReport.from_dict(self.to_dict()["report"])

    def to_dict(self):
        return json.loads(self._encoded)


@dataclass(frozen=True, slots=True)
class ProviderRequest:
    """Copied approved arguments and binding. Credential values are never supplied."""
    _encoded: bytes

    @property
    def arguments(self):
        return json.loads(self._encoded)["arguments"]

    @property
    def binding(self):
        return json.loads(self._encoded)["binding"]

    @property
    def target(self):
        return self.binding["target"]

    @property
    def provider_reference(self):
        return json.loads(self._encoded)["provider_reference"]


class ProviderAdapter:
    """Register dispatch and read-only recovery in the owning workflow package."""

    def __init__(self, adapter_id, *, version, lookup, completion_criterion):
        if type(lookup) is not ProviderLookup:
            raise WorkflowError("lookup must be a ProviderLookup member")
        self._id = _text(adapter_id, "adapter id")
        self._version = _text(version, "adapter version")
        self._lookup = lookup
        self._criterion = _text(completion_criterion, "completion criterion", 1024)
        if not self._criterion.strip():
            raise WorkflowError("provider completion criterion cannot be blank")
        self._dispatch = None
        self._probe = None
        self._contract = None
        self._frozen_callbacks = None

    def _register(self, fn, role):
        if self._contract is not None:
            raise WorkflowError("provider registry is frozen")
        if getattr(self, role) is not None:
            raise WorkflowError("provider callback is already registered")
        if role == "_probe" and self._lookup == ProviderLookup.UNAVAILABLE:
            raise WorkflowError("unavailable lookup cannot register a probe")
        name = _callable_name(fn, label="provider")
        if not inspect.isfunction(fn) or "<" in name:
            raise WorkflowError("provider callbacks require module-level named functions")
        try:
            inspect.signature(fn).bind(object())
        except (ValueError, TypeError) as exc:
            raise WorkflowError("provider callback must accept one request") from exc
        setattr(self, role, fn)
        return fn

    def dispatch(self, fn):
        return self._register(fn, "_dispatch")

    def probe(self, fn):
        return self._register(fn, "_probe")

    def _freeze(self):
        if self._contract is not None:
            return
        if self._dispatch is None or (self._lookup != ProviderLookup.UNAVAILABLE and self._probe is None):
            raise WorkflowError("provider adapter is missing a required callback")
        if self._probe is self._dispatch:
            raise WorkflowError("provider probe must differ from dispatch")
        contract = {"adapter_id": self._id, "version": self._version,
                    "completion_criterion": self._criterion, "lookup": self._lookup.value,
                    "dispatch_callable": _callable_name(self._dispatch),
                    "reconcile_callable": None if self._probe is None else _callable_name(self._probe)}
        contract["digest"] = "sha256:" + hashlib.sha256(_encode(contract, BINDING_REQUEST_BYTES)).hexdigest()
        self._contract = _encode(contract, BINDING_REQUEST_BYTES)
        self._frozen_callbacks = (self._dispatch, self._probe)

    def binding(self, *, business, target, credential_reference, queue, deduplication=None,
                reconciliation=None, admission=None):
        if self._contract is None:
            raise WorkflowError("build the workflow before creating provider bindings")
        policy = {"queue": queue, "initial_delay_ms": 30_000, "interval_ms": 5000,
                  "maximum_interval_ms": 300_000, "max_checks": 12,
                  "max_elapsed_ms": 3_600_000, "check_timeout_ms": 10_000}
        if reconciliation is not None:
            if type(reconciliation) is not dict or set(reconciliation) - (set(policy) - {"queue"}):
                raise WorkflowError("invalid provider reconciliation overrides")
            policy.update(reconciliation)
        value = {"v": 1, "business": business,
            "contract": json.loads(self._contract), "target": target, "deduplication": deduplication,
            "credential_reference": credential_reference, "reconciliation": policy}
        if admission is not None:
            if type(admission) is not ProviderAdmissionRequirement:
                raise WorkflowError("provider admission requires a ProviderAdmissionRequirement")
            value.update(v=2, admission=admission.to_dict())
        return ProviderBindingRequest.from_dict(value)


def _registry(adapters):
    if type(adapters) not in (list, tuple):
        raise WorkflowError("workflow providers must be a list or tuple")
    result = {}
    for adapter in adapters:
        if type(adapter) is not ProviderAdapter or adapter._id in result:
            raise WorkflowError("workflow providers require distinct registered adapters")
        adapter._freeze()
        result[adapter._id] = adapter
    return MappingProxyType(result)


def _validate_artifact_registry(handler, root):
    for adapter in getattr(handler, "_ledgence_providers", {}).values():
        for callback in (adapter._dispatch, adapter._probe):
            if callback is None:
                continue
            source = inspect.getsourcefile(callback)
            if source is None or not Path(source).resolve().is_relative_to(root.resolve()):
                raise WorkflowError("provider callbacks must belong to the prepared program artifact")


def _bound_adapter(registry, binding, action):
    _validate_binding(binding, resolved=True)
    adapter = registry.get(binding["contract"]["adapter_id"])
    if (adapter is None or _encode(binding["contract"], BINDING_REQUEST_BYTES) != adapter._contract
            or adapter._frozen_callbacks is None
            or adapter._dispatch is not adapter._frozen_callbacks[0]
            or adapter._probe is not adapter._frozen_callbacks[1]
            or action["name"] != binding["contract"]["dispatch_callable"]
            or action["version"] != binding["contract"]["version"]):
        raise WorkflowError("provider contract differs from the registered adapter")
    return adapter


class _InvalidReport(WorkflowError):
    """The callback returned a value outside the frozen report contract."""


def _request(action, binding, reference):
    return ProviderRequest(_encode({"arguments": action["arguments"], "binding": binding,
                                   "provider_reference": reference}, 64 * 1024, 96, authoritative=True))


async def _call(callback, action, binding, reference, *, governed=False, request=None, dispatch_deadline=None):
    request = _request(action, binding, reference) if request is None else request
    def invoke():
        # Check inside the thread too: queueing for a busy executor cannot
        # extend an admission Start's conservative monotonic send window.
        if dispatch_deadline is not None and monotonic() >= dispatch_deadline:
            raise WorkflowError("provider admission start expired before callback")
        return callback(request)
    # Sync I/O belongs to a thread. The Rust runtime owns the hard invocation
    # deadline, including a callback that blocks or ignores Python cancellation.
    if inspect.iscoroutinefunction(callback):
        result = await invoke()
    else:
        result = await asyncio.to_thread(invoke)
        if inspect.isawaitable(result):
            result = await result
    try:
        expected = ProviderDispatchOutcome if governed else ProviderReport
        if type(result) is not expected:
            raise WorkflowError("governed dispatch requires ProviderDispatchOutcome; ordinary callbacks require ProviderReport")
        result = expected.from_dict(result.to_dict())
        report = result.report if governed else result
        if report.to_dict()["reported_target"] != binding["target"]:
            raise WorkflowError("provider report target differs from the approved target")
    except Exception as exc:
        raise _InvalidReport("invalid provider callback report") from exc
    return result


class _OperationResult:
    def __init__(self, task, observed_failures):
        self._task = task
        self._observed_failures = observed_failures

    async def _result(self):
        try:
            return await asyncio.shield(self._task)
        except BaseException as exc:
            if self._task.done() and not self._task.cancelled() and self._task.exception() is exc:
                self._observed_failures.add(self._task)
            raise

    def __await__(self):
        return self._result().__await__()


def approved_operation(context):
    context._active()
    if not context._provider_operation:
        raise WorkflowError("provider operations are unavailable")
    approval = context.approval
    if (approval is None or approval.status != ApprovalStatus.APPROVED
            or approval.protection != ApprovalProtection.PROVIDER_OPERATION):
        raise WorkflowError("provider operation requires its approved provider wake")
    raw = approval.to_dict()
    if raw["provider_binding"].get("admission") is not None:
        raise WorkflowError("governed provider dispatch requires request_provider_admission and admitted_provider_operation")
    adapter = _bound_adapter(context._providers, raw["provider_binding"], raw["action"])
    if context._provider_pending is None:
        # Bind the checked callable before task scheduling and the Claim await.
        task = context._loop.create_task(_dispatch(context, raw, adapter._dispatch))
        context._provider_pending = task
    return _OperationResult(context._provider_pending, context._observed_failures)


async def _rpc(context, operation, request):
    async with context._commit_lock:
        if context._fatal is not None:
            raise WorkflowError("workflow runtime acknowledgement failed") from context._fatal
        try:
            return await context._rpc(operation, request)
        except BaseException as exc:
            context._fatal = exc
            raise


async def _dispatch(context, approval, dispatch):
    request = {"approval_key": approval["key"], "action": approval["action"],
               "binding": approval["provider_binding"], "claim_id": "claim_" + uuid.uuid4().hex,
               "report_id": "report_" + uuid.uuid4().hex}
    reply = await _rpc(context, "provider_operation.claim.v1", request)
    try:
        ticket, source = _claim_reply(context, approval, request, reply)
    except BaseException as exc:
        context._fatal = exc
        raise
    if source is None:
        return ticket
    token = _local_owner.set(context)
    try:
        # A callback exception is neither failure evidence nor authority to
        # dispatch again. The durable recovery obligation already exists.
        try:
            report = await _call(dispatch, approval["action"], approval["provider_binding"], None)
        except Exception:
            return ticket
    finally:
        _local_owner.reset(token)
    report_request = {"ticket": ticket.to_dict(), "source_id": source["source_id"],
                      "report_id": request["report_id"], "report": report.to_dict()}
    reply = await _rpc(context, "provider_operation.report.v1", report_request)
    try:
        _report_reply(report_request, reply)
    except BaseException as exc:
        context._fatal = exc
        raise
    return ticket


def _claim_reply(context, approval, request, reply):
    if type(reply) is not dict or reply.get("kind") not in ("dispatch", "existing", "existing_operation"):
        raise WorkflowError("invalid provider claim acknowledgement")
    _fields(reply, {"kind", "ticket"} | ({"source"} if reply["kind"] == "dispatch" else set()))
    _encode(reply, 16 * 1024, 96, authoritative=True)
    ticket = ProviderOperationTicket.from_dict(reply["ticket"])
    expected = {"scope": approval["scope"], "workflow_id": context.workflow_id,
                "activation_id": context.activation_id, "approval_key": approval["key"]}
    actual = ticket.occurrence.to_dict()
    if actual["scope"] != expected["scope"] or (actual == expected) != (reply["kind"] != "existing_operation"):
        raise WorkflowError("provider ticket belongs to another occurrence")
    source = reply.get("source")
    if source is not None:
        _fields(source, {"source_id", "claim_id", "report_id", "ticket", "accepted_at",
                         "first_possible_dispatch_at", "reconciliation_deadline"})
        _text(source["source_id"], "provider source id")
        if (source["ticket"] != reply["ticket"] or source["claim_id"] != request["claim_id"]
                or source["report_id"] != request["report_id"]):
            raise WorkflowError("provider dispatch source differs from its claim")
        for key in ("accepted_at", "first_possible_dispatch_at", "reconciliation_deadline"):
            _integer(source[key], key, maximum=MAX_TIMESTAMP)
        if (source["accepted_at"] != source["first_possible_dispatch_at"]
                or source["reconciliation_deadline"] <= source["accepted_at"]):
            raise WorkflowError("invalid provider dispatch timestamps")
    return ticket, source


def _report_reply(request, reply):
    _fields(reply, {"kind", "ticket", "source_id", "report_id", "accepted_at",
                    "recorded_revision", "accepted_via", "already_accepted"})
    _encode(reply, 16 * 1024, 96, authoritative=True)
    if (reply["kind"] not in ("recorded", "recorded_elsewhere")
            or reply["ticket"] != request["ticket"] or reply["source_id"] != request["source_id"]
            or reply["report_id"] != request["report_id"]
            or type(reply["already_accepted"]) is not bool
            or reply["accepted_via"] not in ("live_dispatch", "evidence_only")
            or (reply["kind"] == "recorded") != (reply["accepted_via"] == "live_dispatch")):
        raise WorkflowError("provider report acknowledgement differs from its command")
    _integer(reply["accepted_at"], "accepted_at", maximum=MAX_TIMESTAMP)
    _integer(reply["recorded_revision"], "recorded_revision", minimum=1)


async def _invoke_probe(handler, event, attempt_id, payload):
    try:
        _fields(payload, {"ticket", "binding", "action", "probe_id", "generation", "task_id",
                          "attempt_id", "source_id", "report_id", "provider_reference", "check_timeout_ms"})
        _encode(payload, 64 * 1024, 96, authoritative=True)
        ticket = ProviderOperationTicket.from_dict(payload["ticket"])
        _validate_binding(payload["binding"], resolved=True)
        from .workflow import _validate_action
        _validate_action(payload["action"], authoritative=True)
        for key in ("probe_id", "task_id", "attempt_id", "source_id", "report_id"):
            _text(payload[key], key)
        _integer(payload["generation"], "probe generation", minimum=1)
        _integer(payload["check_timeout_ms"], "probe timeout", minimum=1000, maximum=30_000)
        if payload["provider_reference"] is not None:
            _text(payload["provider_reference"], "provider reference", 1024)
        scope = ticket.occurrence.scope
        binding = payload["binding"]
        if (payload["task_id"] != event.get("ldgtaskid") or payload["attempt_id"] != attempt_id
                or ticket.occurrence.workflow_id != event.get("ldgworkflowid")
                or event.get("ldgactivationid") is not None
                or scope["tenant_id"] != event.get("ldgtenantid") or scope["namespace"] != event.get("ldgnamespace")
                or payload["check_timeout_ms"] != binding["reconciliation"]["check_timeout_ms"]
                or binding["contract"]["lookup"] == ProviderLookup.UNAVAILABLE
                or (binding["contract"]["lookup"] == ProviderLookup.BY_REFERENCE and payload["provider_reference"] is None)):
            raise WorkflowError("provider probe context differs from its invocation")
    except Exception:
        return {"v": 1, "kind": "rejected", "failure": "context_invalid"}
    try:
        adapter = _bound_adapter(getattr(handler, "_ledgence_providers", {}), binding, payload["action"])
    except Exception:
        return {"v": 1, "kind": "rejected", "failure": "contract_mismatch"}
    try:
        result = await _call(adapter._probe, payload["action"], binding, payload["provider_reference"])
    except _InvalidReport:
        return {"v": 1, "kind": "rejected", "failure": "result_invalid"}
    except Exception:
        return {"v": 1, "kind": "rejected", "failure": "invocation_failed"}
    return {"v": 1, "kind": "report", "report": result.to_dict()}


def _report_reference(value, kind, revision, selected_at):
    _fields(value, {"report_id", "source_id", "sequence", "recorded_revision", "recorded_at", "knowledge"})
    for key in ("report_id", "source_id"):
        _text(value[key], key)
    for key in ("sequence", "recorded_revision"):
        _integer(value[key], key, minimum=1)
    _integer(value["recorded_at"], "recorded_at", maximum=MAX_TIMESTAMP)
    if (value["knowledge"] != kind or value["sequence"] >= value["recorded_revision"]
            or value["recorded_revision"] > revision or value["recorded_at"] > selected_at):
        raise WorkflowError("provider report reference contradicts its selection")


def _selection(value, revision, selected_at):
    if type(value) is not dict:
        raise WorkflowError("invalid provider selection")
    kind = value.get("kind")
    if type(kind) is not str:
        raise WorkflowError("invalid provider selection kind")
    field = {"provider_succeeded": "result", "provider_failed": "failure",
             "provider_cancelled": "reason"}.get(kind)
    if field is not None:
        _fields(value, {"kind", "report_ref", field})
        _report_reference(value["report_ref"], kind.removeprefix("provider_"), revision, selected_at)
        if field == "result":
            _encode(value[field], 128 * 1024, authoritative=True)
        elif field == "failure":
            WorkflowContext._validate_error(value[field], application=True)
        else:
            _text(value[field], "provider cancellation reason", 4096)
    elif kind == "operator_assessment":
        _fields(value, {"kind", "reference", "outcome"})
        reference = value["reference"]
        _fields(reference, {"resolution_id", "sequence", "basis_revision", "accepted_revision",
                            "accepted_at", "assessment_kind", "supersedes_resolution_id", "acceptance_event_id"})
        _text(reference["resolution_id"], "resolution id")
        _text(reference["acceptance_event_id"], "acceptance event id")
        supersedes = reference["supersedes_resolution_id"]
        if supersedes is not None:
            _text(supersedes, "supersedes resolution id")
            if supersedes == reference["resolution_id"]:
                raise WorkflowError("provider assessment cannot supersede itself")
        for key in ("sequence", "basis_revision", "accepted_revision"):
            _integer(reference[key], key, minimum=1)
        _integer(reference["accepted_at"], "accepted_at", maximum=MAX_TIMESTAMP)
        expected = {"record_success": "recorded_success", "record_failure": "recorded_failure",
                    "close_unknown": "closed_unknown"}.get(reference["assessment_kind"])
        outcome = EffectResolutionOutcome.from_dict(value["outcome"])
        if (expected is None or outcome.kind not in (expected, "disputed")
                or reference["basis_revision"] + 1 != reference["accepted_revision"]
                or reference["sequence"] >= reference["accepted_revision"]
                or reference["accepted_revision"] > revision or reference["accepted_at"] > selected_at):
            raise WorkflowError("provider operator assessment contradicts its selection")
        if outcome.kind == "disputed" and (
                reference["assessment_kind"] == "close_unknown"
                or not reference["sequence"] < outcome.first_conflicting_entry_sequence < revision):
            raise WorkflowError("provider dispute contradicts its assessment or selected evidence")
    elif kind == "disputed":
        _fields(value, {"kind", "first_conflicting_entry_sequence"})
        _integer(value["first_conflicting_entry_sequence"], "conflicting sequence", minimum=1,
                 maximum=revision - 1)
    elif kind == "needs_review":
        _fields(value, {"kind", "reason", "checks_used", "deadline_at"})
        if value["reason"] not in ("action_required", "lookup_unavailable", "budget_exhausted",
                                   "deadline", "recovery_failed"):
            raise WorkflowError("invalid provider review reason")
        _integer(value["checks_used"], "checks used", maximum=256)
        _integer(value["deadline_at"], "deadline", maximum=MAX_TIMESTAMP)
        if value["reason"] == "deadline" and value["deadline_at"] > selected_at:
            raise WorkflowError("provider deadline has not elapsed")
    else:
        raise WorkflowError("invalid provider selection kind")


@dataclass(frozen=True, slots=True)
class ProviderOperationResult:
    """Evidence selected once when a provider wait resumes its workflow."""
    _encoded: bytes

    @classmethod
    def from_dict(cls, value):
        _fields(value, {"kind", "key", "occurrence", "parking_activation_id", "parking_revision",
                        "resumed_activation_id", "resumed_revision", "selected_at",
                        "selected_effect_revision", "selection"})
        if value["kind"] != "provider_operation":
            raise WorkflowError("invalid provider operation wake kind")
        EffectOccurrence.from_dict(value["occurrence"])
        for key in ("key", "parking_activation_id", "resumed_activation_id"):
            _text(value[key], key)
        _integer(value["parking_revision"], "parking revision", maximum=(1 << 64) - 2)
        _integer(value["resumed_revision"], "resumed revision", minimum=1)
        _integer(value["selected_effect_revision"], "selected effect revision", minimum=1)
        _integer(value["selected_at"], "selected at", maximum=MAX_TIMESTAMP)
        if (value["parking_revision"] + 1 != value["resumed_revision"]
                or value["parking_activation_id"] == value["resumed_activation_id"]):
            raise WorkflowError("provider wake has inconsistent activation identity")
        _selection(value["selection"], value["selected_effect_revision"], value["selected_at"])
        return cls(_encode(value, WAKE_BYTES, 96, authoritative=True))

    def to_dict(self):
        return json.loads(self._encoded)

    @property
    def kind(self):
        return ProviderSelectionKind(self.selection["kind"])

    @property
    def selection(self):
        return self.to_dict()["selection"]

    @property
    def occurrence(self):
        return EffectOccurrence.from_dict(self.to_dict()["occurrence"])

    @property
    def selected_effect_revision(self):
        return self.to_dict()["selected_effect_revision"]

    @property
    def result(self):
        if self.kind != ProviderSelectionKind.PROVIDER_SUCCEEDED:
            raise WorkflowError("this provider selection has no successful result")
        return self.selection["result"]
