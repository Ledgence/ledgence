"""Operator assessments stay immutable, destination-bound and explicitly reconciled (MIT)."""
import asyncio
from dataclasses import replace
import json
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, patch

import test_workflows
from support import response
from test_effects import IDENTITY, detail, occurrence, summary, claim, observation, inspect_cursor
from ledgence.client import (
    AsyncClient, Conflict, EffectAssessmentEntry, EffectAssessmentKind,
    EffectFilter, EffectOccurrence, EffectResolutionAssessment, EffectResolutionCommand,
    EffectResolutionUncertain, Forbidden, InputError, ProtocolError, RequestTimeout,
)
from ledgence.client.effect_models import parse_effect_detail


def assessment_entry(**changes):
    return {"kind": "assessment", "sequence": 3, "recorded_at": 16, "recorded_revision": 4,
        "resolution_id": "resolution-1", "basis_revision": 3, "supersedes_resolution_id": None,
        "assessment": {"kind": "record_success", "result": {"accepted": True}},
        "reason": "Matched receipt", "evidence_references": ["provider:receipt:1"],
        "actor": {"issuer": "urn:operator", "subject": "reviewer", "class": "operator", "credential_id": "credential"},
        "acceptance_event_id": "audit-resolution-1", **changes}


def reference(entry=None):
    entry = entry or assessment_entry()
    return {"resolution_id": entry["resolution_id"], "sequence": entry["sequence"], "accepted_at": entry["recorded_at"],
        "accepted_revision": entry["recorded_revision"], "basis_revision": entry["basis_revision"],
        "supersedes_resolution_id": entry["supersedes_resolution_id"], "assessment_kind": entry["assessment"]["kind"],
        "acceptance_event_id": entry["acceptance_event_id"]}


def assessed_detail():
    entry = assessment_entry()
    return detail(summary=summary(entry_count=3, effect_revision=4, updated_at=16,
        operator_resolution={"reference": reference(entry), "first_conflicting_entry_sequence": None}),
        entries=[claim(), observation(), entry], current_assessment=entry)


def receipt(body, **changes):
    command = json.loads(body)
    return {"occurrence": command["occurrence"], "resolution_id": command["resolution_id"],
        "accepted_at": 20, "accepted_revision": command["expected_effect_revision"] + 1,
        "basis_revision": command["expected_effect_revision"], "supersedes_resolution_id": command["supersedes_resolution_id"],
        "assessment_kind": command["assessment"]["kind"], "acceptance_event_id": "audit-resolve", "already_accepted": False, **changes}


class EffectResolutionTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflows.WorkflowClientTests.asyncSetUp

    def inspected(self, raw=None, endpoint=None):
        return parse_effect_detail(raw or detail(), EffectOccurrence(self.client.scope, **IDENTITY),
            None, 4, endpoint or self.url)

    def command(self, observed=None, **changes):
        args = {"resolution_id": "resolution-new", "assessment": EffectResolutionAssessment.record_success({"done": True}),
                "reason": "Matched the action", "evidence_references": ["provider:receipt:1"]}
        return self.client.effects.prepare_resolution(observed or self.inspected(), **(args | changes))

    async def test_shared_console_export_restores_exact_wire_body_without_number_coercion(self):
        path = Path(__file__).with_name("fixtures") / "console-effect-resolution.json"
        saved = path.read_text(encoding="utf-8")
        envelope = json.loads(saved)
        client = AsyncClient(envelope["base_url"], tenant="acme", namespace="demo")
        command = client.effects.restore_resolution(saved)
        self.assertEqual(command._body, envelope["body"].encode("utf-8"))
        self.assertEqual(json.loads(command.to_json()), envelope)
        body = command.to_dict()
        self.assertEqual(body["expected_effect_revision"], 2**64 - 2)
        self.assertEqual(body["assessment"]["result"]["integer"], 2**64 - 1)
        self.assertIs(type(body["assessment"]["result"]["float"]), float)
        self.assertEqual(self.requests, [])

    async def test_prepare_freezes_original_occurrence_revision_action_and_values(self):
        value = {"integer": 2**64 - 1, "float": 1.0, "nul": "\0", "nested": [None]}
        assessment = EffectResolutionAssessment.record_success(value)
        value["nested"].append("changed")
        command = self.command(assessment=assessment, reason="  kept exactly\n", evidence_references=["second", "first"])
        wire = command.to_dict()
        self.assertEqual(wire["occurrence"], occurrence())
        self.assertEqual(wire["expected_effect_revision"], 3)
        self.assertIsNone(wire["supersedes_resolution_id"])
        self.assertEqual(wire["action"], detail()["binding"]["approval"]["action"])
        self.assertEqual(wire["reason"], "  kept exactly\n")
        self.assertEqual(wire["evidence_references"], ["second", "first"])
        self.assertEqual(wire["assessment"]["result"]["nested"], [None])
        wire["assessment"]["result"]["nested"].append("mutated")
        self.assertEqual(command.to_dict()["assessment"]["result"]["nested"], [None])
        self.assertIs(type(command.to_dict()["assessment"]["result"]["float"]), float)
        with self.assertRaises(AttributeError): command.base_url = "http://other"
        with self.assertRaises(TypeError): EffectResolutionCommand()
        self.assertEqual(self.requests, [])

    async def test_factories_keep_variants_distinct_and_do_not_make_provider_claims(self):
        unknown = EffectResolutionAssessment.close_unknown()
        self.assertEqual(unknown.kind, EffectAssessmentKind.CLOSE_UNKNOWN)
        self.assertEqual(unknown.to_dict(), {"kind": "close_unknown"})
        failure = EffectResolutionAssessment.record_failure("declined", "Operator found a rejection")
        self.assertEqual(failure.failure.kind, "declined")
        with self.assertRaises(InputError): _ = failure.result
        with self.assertRaises(InputError): _ = unknown.failure
        self.assertIsNone(EffectResolutionAssessment.record_success(None).result)
        self.command(assessment=unknown, evidence_references=[])

    async def test_supersession_uses_current_header_even_when_not_in_the_selected_page(self):
        raw = assessed_detail()
        raw["entries"] = [claim()]
        raw["next_cursor"] = inspect_cursor(1)
        observed = self.inspected(raw)
        self.assertIsInstance(observed.current_assessment, EffectAssessmentEntry)
        self.assertEqual(observed.current_assessment.actor.subject, "reviewer")
        self.assertEqual(observed.current_assessment.assessment.result, {"accepted": True})
        command = self.command(observed)
        self.assertEqual(command.to_dict()["supersedes_resolution_id"], "resolution-1")
        self.assertEqual(command.to_dict()["expected_effect_revision"], 4)

    async def test_saved_envelope_preserves_original_body_bytes_and_destination(self):
        command = self.command(assessment=EffectResolutionAssessment.record_success(None))
        saved = json.loads(command.to_json())
        self.assertEqual(set(saved), {"format", "base_url", "body"})
        self.assertEqual(saved["format"], "ledgence.effect-resolution.v1")
        self.assertEqual(saved["body"].encode(), command._body)
        # An application-owned serialization may have whitespace or escaped Unicode.
        body = json.dumps(command.to_dict(), indent=2, ensure_ascii=True)
        saved["body"] = body
        for encoded in (json.dumps(saved), json.dumps(saved).encode()):
            restored = self.client.effects.restore_resolution(encoded)
            self.assertEqual(restored._body, body.encode())
            self.assertEqual(restored.to_dict(), command.to_dict())
        self.assertEqual(self.requests, [])

    async def test_restore_rejects_alias_noncanonical_cross_scope_and_body_only_before_network(self):
        command = self.command()
        envelope = json.loads(command.to_json())
        cases = [command.to_dict(), json.dumps(command.to_dict()), b'{}', json.dumps(envelope | {"extra": True}),
            json.dumps(envelope | {"format": "ledgence.effect-resolution.v2"}),
            json.dumps(envelope | {"base_url": self.url + "/"}),
            json.dumps(envelope | {"base_url": self.url.replace("127.0.0.1", "localhost")}),
            json.dumps(envelope | {"body": command.to_dict()}),
            json.dumps(envelope | {"body": "{"}), '"' + "x" * (1024 * 1024) + '"',
            command.to_json()[:-1] + ',"format":"ledgence.effect-resolution.v1"}']
        changed = command.to_dict()
        changed["occurrence"]["scope"]["namespace"] = "foreign"
        cases.append(json.dumps(envelope | {"body": json.dumps(changed)}))
        for saved in cases:
            with self.subTest(saved_type=type(saved)), self.assertRaises(InputError):
                self.client.effects.restore_resolution(saved)
        self.assertEqual(self.requests, [])

    async def test_restore_rejects_malformed_command_fields_without_rewriting(self):
        envelope = json.loads(self.command().to_json())
        base = json.loads(envelope["body"])
        for changes in ({"expected_effect_revision": True}, {"expected_effect_revision": 1.0},
                        {"expected_effect_revision": 0}, {"expected_effect_revision": 2**64 - 1},
                        {"supersedes_resolution_id": base["resolution_id"]}, {"reason": " \n"},
                        {"evidence_references": []}, {"evidence_references": ["x"] * 9},
                        {"evidence_references": ["x" * 1025]}, {"extra": 1},
                        {"assessment": {"kind": "record_success"}}, {"assessment": {"kind": "record_failure", "failure": None}}):
            with self.subTest(changes=changes), self.assertRaises(InputError):
                self.client.effects.restore_resolution(json.dumps(envelope | {"body": json.dumps(base | changes)}))
        for key in ("supersedes_resolution_id", "assessment", "evidence_references"):
            missing = {k: v for k, v in base.items() if k != key}
            with self.assertRaises(InputError):
                self.client.effects.restore_resolution(json.dumps(envelope | {"body": json.dumps(missing)}))
        duplicate = envelope["body"][:-1] + ',"resolution_id":"duplicate"}'
        with self.assertRaises(InputError):
            self.client.effects.restore_resolution(json.dumps(envelope | {"body": duplicate}))
        self.assertEqual(self.requests, [])

    async def test_foreign_detail_and_prepared_command_reject_without_network(self):
        with self.assertRaises(InputError): self.command(self.inspected(endpoint="http://localhost:9999"))
        with self.assertRaises(InputError): self.command(replace(self.inspected(), _base_url=""))
        other = AsyncClient("http://127.0.0.1:9999", tenant=self.client.scope.tenant_id, namespace=self.client.scope.namespace)
        async with other:
            with self.assertRaises(InputError): await other.effects.resolve(self.command())
        self.assertEqual(self.requests, [])

    async def test_uncertain_response_reconciles_explicit_same_bytes_with_original_receipt(self):
        command = self.command()
        count = 0
        async def serve(request, body):
            nonlocal count
            count += 1
            return response({"code": "unavailable"}, code=503) if count == 1 else response(receipt(body, already_accepted=True))
        self.callback = serve
        with self.assertRaises(EffectResolutionUncertain) as caught: await self.client.effects.resolve(command)
        self.assertIs(caught.exception.command, command)
        self.assertEqual(count, 1)
        restored = self.client.effects.restore_resolution(command.to_json())
        accepted = await self.client.effects.resolve(restored)
        self.assertTrue(accepted.already_accepted)
        self.assertEqual(accepted.basis_revision, 3)
        self.assertEqual(accepted.accepted_revision, 4)
        self.assertEqual(self.requests[0][3], self.requests[1][3])
        self.assertEqual(self.requests[0][1], "/v1/workflows/approved-effects/resolve")

    async def test_definite_denial_is_not_uncertain_and_is_not_retried(self):
        for status, code, error in ((403, "forbidden", Forbidden), (409, "conflict", Conflict)):
            self.callback = AsyncMock(side_effect=lambda request, body: response({"code": code}, code=status))
            with self.assertRaises(error): await self.client.effects.resolve(self.command())
        self.assertEqual(len(self.requests), 2)

    async def test_invalid_success_receipt_remains_uncertain(self):
        for changes in ({"accepted_revision": 99}, {"basis_revision": 2}, {"already_accepted": 1},
                        {"resolution_id": "other"}, {"supersedes_resolution_id": "other"},
                        {"assessment_kind": "close_unknown"}, {"result": "leaked"},
                        {"occurrence": occurrence() | {"activation_id": "new-activation"}}):
            self.callback = AsyncMock(side_effect=lambda request, body: response(receipt(body, **changes)))
            with self.subTest(changes=changes), self.assertRaises(EffectResolutionUncertain) as caught:
                await self.client.effects.resolve(self.command())
            self.assertIsInstance(caught.exception.cause, ProtocolError)

    async def test_timeout_before_dispatch_remains_definite_and_cancellation_is_not_retried(self):
        for dispatched in (False, True):
            with patch.object(self.client._require_transport(), "exchange", new=AsyncMock(side_effect=RequestTimeout(dispatched=dispatched))):
                with self.assertRaises(EffectResolutionUncertain if dispatched else RequestTimeout):
                    await self.client.effects.resolve(self.command())
        started, released, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
        async def serve(request, body):
            started.set()
            await released.wait()
            finished.set()
            return response(receipt(body))
        self.callback = serve
        command = self.command()
        running = asyncio.create_task(self.client.effects.resolve(command))
        await started.wait()
        running.cancel()
        with self.assertRaises(asyncio.CancelledError): await running
        released.set()
        await finished.wait()
        await asyncio.sleep(0)
        self.assertEqual(len(self.requests), 1)

    async def test_malformed_current_assessment_reference_actor_or_timeline_is_rejected(self):
        original = assessed_detail()
        mutations = [
            lambda d: d.update(current_assessment=None),
            lambda d: d["summary"].update(operator_resolution=None),
            lambda d: d["current_assessment"].update(resolution_id="different"),
            lambda d: d["current_assessment"]["actor"].update(**{"class": "worker"}),
            lambda d: d["current_assessment"].update(basis_revision=4),
            lambda d: d["current_assessment"].update(reason=""),
            lambda d: d["entries"][-1].update(reason="Changed timeline"),
            lambda d: d["summary"]["operator_resolution"].update(first_conflicting_entry_sequence=3),
        ]
        for mutation in mutations:
            changed = json.loads(json.dumps(original))
            mutation(changed)
            with self.assertRaises(ProtocolError): self.inspected(changed)

    async def test_header_comparison_ignores_object_order_but_preserves_json_number_kinds(self):
        raw = assessed_detail()
        raw["current_assessment"]["assessment"]["result"] = {"a": 1.0, "b": -0.0}
        # Header and timeline originate from separate serialized projections.
        raw["entries"][-1] = dict(raw["current_assessment"], assessment={"result": {"b": -0.0, "a": 1.0}, "kind": "record_success"})
        self.assertEqual(self.inspected(raw).current_assessment.assessment.result, {"a": 1.0, "b": -0.0})
        for changed in ({"a": 1, "b": -0.0}, {"a": 1.0, "b": 0.0}, {"a": True, "b": -0.0}):
            raw["entries"][-1]["assessment"]["result"] = changed
            with self.subTest(changed=changed), self.assertRaises(ProtocolError): self.inspected(raw)

    async def test_assessment_timeline_requires_a_current_header(self):
        raw = assessed_detail()
        raw.pop("current_assessment")
        raw["summary"].pop("operator_resolution")
        with self.assertRaises(ProtocolError): self.inspected(raw)

    async def test_historical_assessment_sequence_is_below_its_accepted_revision(self):
        raw = assessed_detail()
        old = assessment_entry(sequence=2, recorded_revision=2, basis_revision=1, resolution_id="prior")
        raw["entries"] = [old]
        raw["next_cursor"] = inspect_cursor(2)
        raw["current_assessment"]["supersedes_resolution_id"] = "prior"
        raw["summary"]["operator_resolution"]["reference"]["supersedes_resolution_id"] = "prior"
        with self.assertRaises(ProtocolError):
            parse_effect_detail(raw, EffectOccurrence(self.client.scope, **IDENTITY), inspect_cursor(1), 4, self.url)

    async def test_disputed_inbox_filter_uses_current_assessment_conflict_not_historic_flag(self):
        item = assessed_detail()["summary"]
        item.update(entry_count=4, effect_revision=5)
        item["operator_resolution"]["first_conflicting_entry_sequence"] = 4
        self.callback = AsyncMock(side_effect=lambda request, body: response({"items": [item], "next_cursor": None}))
        page = await self.client.effects.inbox(filter=EffectFilter.DISPUTED)
        self.assertEqual(page.items[0].operator_resolution.first_conflicting_entry_sequence, 4)
        item["operator_resolution"]["first_conflicting_entry_sequence"] = None
        item["has_discrepancy"] = True
        with self.assertRaises(ProtocolError): await self.client.effects.inbox(filter="disputed")

    async def test_limits_and_null_success_are_explicit(self):
        self.command(reason="x" * 4096, evidence_references=["r" * 1024] * 8)
        with self.assertRaises(InputError): self.command(reason="é" * 2049)
        with self.assertRaises(InputError): EffectResolutionAssessment.record_success("x" * 131071)
        self.command(assessment=EffectResolutionAssessment.record_success("x" * 131070))
        for assessment in (EffectResolutionAssessment.record_success(None), EffectResolutionAssessment.record_failure("no", "Unavailable")):
            with self.assertRaises(InputError): self.command(assessment=assessment, evidence_references=[])


if __name__ == "__main__": unittest.main()
