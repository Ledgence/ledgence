"""Read-only provider admission evidence. A ticket is not dispatch authority (MIT)."""
import json
from dataclasses import dataclass, field
from enum import StrEnum

from . import codec
from .errors import InputError


class ProviderCallCompletion(StrEnum):
    COMPLETED = "completed"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ProviderAdmissionRecord:
    _encoded: bytes = field(repr=False)

    def to_dict(self):
        return json.loads(self._encoded)

    @property
    def ticket(self):
        return self.to_dict()["ticket"]

    @property
    def call_completion(self):
        report = self.to_dict()["report"]
        return None if report is None else ProviderCallCompletion(report["call_completion"])


def _ticket(raw, scope):
    from .effect_models import _occurrence
    from .provider_bindings import _digest
    codec.fields(raw, {"occurrence", "binding_digest"})
    _digest(raw["binding_digest"])
    return _occurrence(raw["occurrence"], scope)


def validate_subject(binding, occurrence):
    from .provider_bindings import parse_provider_binding
    subject = binding["provider"]
    codec.fields(subject, {"ticket", "binding", "action"})
    ticket = _ticket(subject["ticket"], occurrence.scope)
    if ticket.workflow_id != occurrence.workflow_id or ticket.activation_id != occurrence.origin_activation_id:
        raise InputError("foreign provider subject")
    if "program" in subject["binding"]:
        raise InputError("provider request contains program")
    codec.validate_authoritative(subject["binding"], 12 * 1024, max_depth=96)
    resolved = parse_provider_binding(dict(subject["binding"], program=binding["program"]))
    if resolved.admission is None or resolved.admission.resource != binding["resource"]:
        raise InputError("provider admission resource mismatch")
    action = subject["action"]
    codec.fields(action, {"name", "version", "arguments"})
    codec.text(action["name"], "action", 512)
    codec.text(action["version"], "version")
    if type(action["arguments"]) is not dict:
        raise InputError("approval arguments must be an object")
    codec.validate_authoritative(action, 32 * 1024, max_depth=96)
    if action["name"] != resolved.contract.dispatch_callable or action["version"] != resolved.contract.version:
        raise InputError("provider action mismatch")


def validate_record(raw, binding, occurrence, source, updated, state):
    from .admission_models import _source, timestamp, digest, boolean
    from .effect_models import _worker
    codec.fields(raw, {"ticket", "source", "report"})
    _ticket(raw["ticket"], occurrence.scope)
    if raw["ticket"]["binding_digest"] != binding["provider"]["ticket"]["binding_digest"]:
        raise InputError("provider ticket binding mismatch")
    linked = raw["source"]
    if linked is None:
        if source is not None or raw["report"] is not None or state != "closed_before_start":
            raise InputError("invalid deduplicated admission")
        return
    codec.fields(linked, {"admission", "provider"})
    _source(linked["admission"], occurrence)
    if linked["admission"] != source or raw["ticket"] != binding["provider"]["ticket"]:
        raise InputError("unlinked admission source")
    provider = linked["provider"]
    codec.fields(provider, {"source_id", "claim_id", "report_id", "ticket", "worker", "accepted_at", "first_possible_dispatch_at", "reconciliation_deadline"})
    for key in ("source_id", "claim_id", "report_id"):
        codec.text(provider[key], key)
    _worker(provider["worker"], occurrence.scope)
    if (provider["ticket"] != raw["ticket"] or provider["worker"] != source["worker"] or
        provider["claim_id"] != source["start_id"] or provider["report_id"] != source["finish_id"] or
        timestamp(provider["accepted_at"]) != source["accepted_at"] or
        timestamp(provider["first_possible_dispatch_at"]) != source["accepted_at"] or
        timestamp(provider["reconciliation_deadline"]) <= source["accepted_at"]):
        raise InputError("inconsistent provider source")
    report = raw["report"]
    if report is not None:
        codec.fields(report, {"source", "call_completion", "receipt"})
        ProviderCallCompletion(report["call_completion"])
        if report["source"] != linked:
            raise InputError("foreign composed receipt")
        receipt = report["receipt"]
        codec.fields(receipt, {"ticket", "source_id", "source", "report_id", "report_digest", "worker", "accepted_at", "recorded_revision", "accepted_via", "already_accepted"})
        codec.fields(receipt["source"], {"kind", "claim_id"})
        digest(receipt["report_digest"])
        codec.integer(receipt["recorded_revision"], "provider revision", 1)
        boolean(receipt["already_accepted"])
        worker = _worker(receipt["worker"], occurrence.scope)
        expected_worker = _worker(provider["worker"], occurrence.scope)
        if (receipt["ticket"] != raw["ticket"] or receipt["source_id"] != provider["source_id"] or
            receipt["source"] != {"kind": "dispatch", "claim_id": provider["claim_id"]} or
            receipt["report_id"] != provider["report_id"] or receipt["accepted_via"] != "evidence_only" or
            worker.audience != expected_worker.audience or worker.issuer != expected_worker.issuer or worker.subject != expected_worker.subject or
            not source["accepted_at"] <= timestamp(receipt["accepted_at"]) <= updated):
            raise InputError("unlinked provider receipt")
    valid = (state == "possibly_dispatched" and report is None or
        state == "unknown_occupied" and (report is None or report["call_completion"] == "unknown") or
        state == "finished" and report is not None and report["call_completion"] == "completed" or
        state == "administratively_released")
    if not valid:
        raise InputError("inconsistent provider admission state")
