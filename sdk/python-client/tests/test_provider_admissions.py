"""Governed provider bindings and capacity evidence remain strict and separate (MIT)."""
import copy
import unittest

import test_admissions as admissions
from test_provider_effects import binding
from ledgence.client import AdmissionOccurrence, InputError, ProviderCallCompletion, Scope
from ledgence.client.admission_models import _snapshot
from ledgence.client.provider_bindings import parse_provider_binding


def governed_snapshot():
    value = admissions.snapshot()
    original = value["occurrence"]
    approved = binding()
    approved["v"] = 2
    approved["admission"] = {"resource": "model"}
    program = approved.pop("program")
    ticket = {"occurrence": {"scope": original["scope"], "workflow_id": original["workflow_id"], "activation_id": original["origin_activation_id"], "approval_key": "approve"}, "binding_digest": admissions.DIGEST}
    value["binding"] = {"resource": "model", "program": program, "provider": {"ticket": ticket, "binding": approved, "action": {"name": approved["contract"]["dispatch_callable"], "version": "1", "arguments": {"amount": 100}}}}
    source = value["source"]
    provider = {"source_id": "provider-source", "claim_id": source["start_id"], "report_id": source["finish_id"], "ticket": ticket, "worker": copy.deepcopy(source["worker"]), "accepted_at": 1000, "first_possible_dispatch_at": 1000, "reconciliation_deadline": 60000}
    linked = {"admission": copy.deepcopy(source), "provider": provider}
    receipt = {"ticket": ticket, "source_id": provider["source_id"], "source": {"kind": "dispatch", "claim_id": provider["claim_id"]}, "report_id": provider["report_id"], "report_digest": admissions.DIGEST, "worker": copy.deepcopy(source["worker"]), "accepted_at": 1100, "recorded_revision": 18446744073709551615, "accepted_via": "evidence_only", "already_accepted": False}
    value.update(state="finished", updated_at=1100, provider={"ticket": ticket, "source": linked, "report": {"source": copy.deepcopy(linked), "call_completion": "completed", "receipt": receipt}})
    return value


class ProviderAdmissionTests(unittest.TestCase):
    def parse(self, value):
        occurrence = value["occurrence"]
        return _snapshot(value, AdmissionOccurrence(Scope(**occurrence["scope"]), occurrence["workflow_id"], occurrence["origin_activation_id"], occurrence["key"]))

    def test_v1_serialization_is_unchanged_and_v2_requires_resource(self):
        legacy = binding()
        self.assertEqual(parse_provider_binding(legacy).to_dict(), legacy)
        governed = dict(legacy, v=2, admission={"resource": "exports.shared"})
        self.assertEqual(parse_provider_binding(governed).admission.resource, "exports.shared")
        self.assertEqual(parse_provider_binding(governed).to_dict(), governed)
        for altered in (dict(legacy, admission=None), dict(legacy, admission={"resource": "exports"}), dict(legacy, v=2)):
            with self.assertRaises(InputError):
                parse_provider_binding(altered)

    def test_readonly_evidence_distinguishes_call_completion_from_business_result(self):
        value = governed_snapshot()
        snapshot = self.parse(value)
        self.assertIsNone(snapshot.finish)
        self.assertEqual(snapshot.provider.call_completion, ProviderCallCompletion.COMPLETED)
        snapshot.provider.ticket["binding_digest"] = "changed"
        self.assertEqual(snapshot.provider.ticket["binding_digest"], admissions.DIGEST)
        unknown = copy.deepcopy(value)
        unknown["state"] = "unknown_occupied"
        unknown["provider"]["report"]["call_completion"] = "unknown"
        self.assertEqual(self.parse(unknown).provider.call_completion, ProviderCallCompletion.UNKNOWN)
        unknown["state"] = "finished"
        with self.assertRaises(InputError):
            self.parse(unknown)

    def test_mixed_subject_foreign_source_and_missing_receipt_fields_are_rejected(self):
        cases = []
        mixed = governed_snapshot(); mixed["binding"]["operation"] = admissions.binding()["operation"]; cases.append(mixed)
        wrong = governed_snapshot(); wrong["provider"]["source"]["provider"]["worker"]["subject"] = "foreign"; cases.append(wrong)
        changed = governed_snapshot(); changed["provider"]["report"]["receipt"]["source"]["claim_id"] = "foreign"; cases.append(changed)
        null = governed_snapshot(); null["provider"] = None; cases.append(null)
        missing = governed_snapshot(); del missing["provider"]["report"]["call_completion"]; cases.append(missing)
        oversized = governed_snapshot(); oversized["binding"]["provider"]["action"]["arguments"] = {"text": "x" * (32 * 1024)}; cases.append(oversized)
        for value in cases:
            with self.subTest(value=value), self.assertRaises((InputError, KeyError)):
                self.parse(value)
