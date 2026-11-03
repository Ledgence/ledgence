"""Retained effect observations keep their identity, provenance and exact values (MIT)."""
import hashlib
import json
import unittest
from unittest.mock import AsyncMock

import test_workflows
from support import SCOPE, response
from test_approvals import snapshot
from ledgence.client import (
    EffectClaimEntry, EffectFilter, EffectJournalState, EffectLegacyJournal,
    EffectObservationEntry, EffectObservationIngress, Forbidden, InputError,
    NotFound, ProtocolError, Unavailable,
)

IDENTITY = {"workflow_id": "workflow", "activation_id": "effect-activation", "approval_key": "refund"}


def occurrence(): return {"scope": dict(SCOPE), **IDENTITY}


def worker():
    return {"audience": {"authority_id": "authority", "instance_id": "instance", "scope": dict(SCOPE)},
            "issuer": "urn:worker", "subject": "pool-1", "credential_id": "credential-1"}


def summary(**changes):
    return {"occurrence": occurrence(), "program": {"id": "refund", "version": "1"},
            "callable": "program:refund", "action_version": "1",
            "dispatch_provenance": {"kind": "verified_claim", "claim_id": "claim-1"},
            "journal_state": "confirmed", "has_discrepancy": False, "entry_count": 2,
            "effect_revision": 3, "created_at": 0, "updated_at": 15, **changes}


def binding():
    approval = snapshot(status="approved", resumed_activation_id=IDENTITY["activation_id"],
        decision={"decision_id": "decision-1", "decision": "approve", "reviewer": "operator",
                  "reason": None, "decided_at": 11}, protection="external_effect")
    return {"occurrence": occurrence(), "approval": approval,
            "program": {"program": {"id": "refund", "version": "1"}, "digest": "sha256:" + "a" * 64, "size": 1},
            "key": "_approval:" + hashlib.sha256(b"refund").hexdigest(), "lookup": "program:lookup",
            "approval_acceptance_event_id": "audit-1"}


def claim(**changes):
    return {"kind": "claim", "sequence": 1, "recorded_at": 12, "recorded_revision": 2,
            "claim_id": "claim-1", "attempt_id": "attempt-1", "worker": worker(),
            "initial_permission": "dispatch", **changes}


def observation(**changes):
    return {"kind": "observation", "sequence": 2, "recorded_at": 15, "recorded_revision": 3,
            "observation_id": "observation-1", "source": {"kind": "verified_worker_claim", "claim_id": "claim-1",
                "worker": worker(), "reported_callback": "dispatch", "ingress": "live_confirm"},
            "output": {"number": 2**64 - 1, "float": 1.0, "nul": "\x00", "nested": [None]}, **changes}


def detail(**changes):
    return {"binding": binding(), "summary": summary(), "entries": [claim(), observation()],
            "next_cursor": None, **changes}


def cursor(value): return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode().hex()


def inspect_cursor(sequence=1, identity=None):
    return cursor({"version": 1, "occurrence": identity or occurrence(), "after_sequence": sequence})


def inbox_cursor(item=None, selected="all", workflow_id=None):
    item = item or summary()
    return cursor({"version": 1, "scope": dict(SCOPE), "filter": selected, "workflow_id": workflow_id,
                   "position": {"updated_at": item["updated_at"], **{key: item["occurrence"][key] for key in IDENTITY}}})


class EffectTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflows.WorkflowClientTests.asyncSetUp

    def serve(self, value, status=200):
        self.callback = AsyncMock(side_effect=lambda request, body: response(value, code=status))

    async def test_typed_metadata_query_preserves_scope_filters_and_full_u64(self):
        item = summary(entry_count=2**64 - 2, effect_revision=2**64 - 1, has_discrepancy=True)
        self.serve({"items": [item], "next_cursor": None})
        page = await self.client.effects.inbox(filter=EffectFilter.DISCREPANT, workflow_id="workflow", limit=100)
        self.assertEqual(page.items[0].entry_count, 2**64 - 2)
        self.assertEqual(page.items[0].effect_revision, 2**64 - 1)
        self.assertEqual(page.items[0].journal_state, EffectJournalState.CONFIRMED)
        self.assertEqual(self.requests[0][:3], ("POST", "/v1/workflows/approved-effects/inbox", {}))
        self.assertEqual(json.loads(self.requests[0][3]), {"scope": SCOPE, "filter": "discrepant", "workflow_id": "workflow", "cursor": None, "limit": 100})
        with self.assertRaises(AttributeError): page.items[0].has_discrepancy = False

    async def test_inspection_keeps_frozen_binding_sources_and_immutable_application_values(self):
        self.serve(detail())
        inspected = await self.client.effects.inspect(**IDENTITY)
        self.assertIsInstance(inspected.entries[0], EffectClaimEntry)
        self.assertIsInstance(inspected.entries[1], EffectObservationEntry)
        self.assertEqual(inspected.entries[1].source.ingress, EffectObservationIngress.LIVE_CONFIRM)
        self.assertEqual(inspected.entries[1].source.worker.credential_id, "credential-1")
        output = inspected.entries[1].output
        output["nested"].append("changed")
        self.assertEqual(inspected.entries[1].output["nested"], [None])
        self.assertEqual(inspected.entries[1].output["number"], 2**64 - 1)
        self.assertIs(type(inspected.entries[1].output["float"]), float)
        self.assertEqual(inspected.entries[1].output["nul"], "\x00")
        self.assertEqual(inspected.binding.approval.action.arguments, {"amount": 50})
        self.assertNotIn("output", repr(inspected.entries[1]))
        self.assertEqual(self.requests[0][:3], ("POST", "/v1/workflows/approved-effects/inspect", {}))
        self.assertEqual(json.loads(self.requests[0][3]), {"occurrence": occurrence(), "cursor": None, "limit": 4})

    async def test_whole_entry_short_pages_and_legacy_provenance_remain_distinct(self):
        token = inspect_cursor()
        self.serve(detail(entries=[claim()], next_cursor=token))
        first = await self.client.effects.inspect(**IDENTITY, limit=4)
        self.assertEqual(first.next_cursor, token)  # Byte-limited pages need not fill their count limit.
        legacy = observation(source={"kind": "legacy_journal", "attempt_id": "old-attempt", "journal_accepted_at": 0}, output=None)
        self.serve(detail(entries=[legacy], summary=summary(dispatch_provenance={"kind": "legacy_attempt", "attempt_id": "old-attempt"})))
        second = await self.client.effects.inspect(**IDENTITY, cursor=first.next_cursor)
        self.assertIsInstance(second.entries[0].source, EffectLegacyJournal)
        self.assertIsNone(second.entries[0].output)
        self.assertEqual(json.loads(self.requests[-1][3])["cursor"], token)
        self.serve(detail(entries=[], summary=summary(entry_count=0, effect_revision=1,
            journal_state="unconfirmed", dispatch_provenance={"kind": "legacy_fence"})))
        legacy_open = await self.client.effects.inspect(**IDENTITY)
        self.assertEqual(legacy_open.entries, ())
        self.assertEqual(legacy_open.summary.dispatch_provenance.kind, "legacy_fence")

    async def test_inbox_short_pages_advance_by_scope_bound_position(self):
        token = inbox_cursor()
        self.serve({"items": [summary()], "next_cursor": token})
        first = await self.client.effects.inbox()
        self.assertEqual(first.next_cursor, token)
        self.serve({"items": [summary(updated_at=14)], "next_cursor": None})
        await self.client.effects.inbox(cursor=token)
        self.assertEqual(json.loads(self.requests[-1][3])["cursor"], token)
        self.serve({"items": [summary()], "next_cursor": None})
        with self.assertRaises(ProtocolError): await self.client.effects.inbox(cursor=token)

    async def test_denied_unavailable_and_missing_are_not_empty_evidence_or_retried(self):
        for code, status, error in [("forbidden", 403, Forbidden), ("unavailable", 503, Unavailable), ("not_found", 404, NotFound)]:
            self.serve({"code": code} | ({"message": "Unavailable"} if status == 503 else {}), status)
            for operation in (self.client.effects.inbox, lambda: self.client.effects.inspect(**IDENTITY)):
                with self.assertRaises(error): await operation()
        self.assertEqual(len(self.requests), 6)

    async def test_invalid_arguments_and_foreign_cursors_fail_before_network(self):
        for options in ({"limit": 0}, {"limit": 101}, {"limit": True}, {"filter": "pending"},
                        {"workflow_id": ""}, {"cursor": "abcd"}, {"cursor": "ff" * 4097},
                        {"cursor": inbox_cursor(selected="confirmed")},
                        {"cursor": inbox_cursor(workflow_id="workflow")}):
            with self.subTest(options=options), self.assertRaises(InputError): await self.client.effects.inbox(**options)
        for options in ({"limit": 0}, {"limit": 5}, {"limit": True}, {"activation_id": ""},
                        {"cursor": inspect_cursor(identity=occurrence() | {"workflow_id": "other"})},
                        {"cursor": inspect_cursor(sequence=0)}):
            with self.subTest(options=options), self.assertRaises(InputError): await self.client.effects.inspect(**(IDENTITY | options))
        self.assertEqual(self.requests, [])

    async def test_malformed_scope_identity_sources_and_page_boundaries_are_rejected(self):
        cases = []
        paths = [
            (("summary", "effect_revision"), 2), (("summary", "entry_count"), True),
            (("summary", "updated_at"), 253402300800000),
            (("binding", "approval", "resumed_activation_id"), "other"),
            (("binding", "key"), "_approval:wrong"), (("binding", "program", "size"), 0),
            (("binding", "program", "program", "id"), "other"),
            (("entries", 0, "worker", "audience", "scope", "namespace"), "other"),
            (("entries", 1, "source", "worker", "audience", "scope", "tenant_id"), "other"),
            (("entries", 1, "source", "kind"), "verified_provider"),
            (("entries", 1, "source", "reported_callback"), "retry"),
            (("entries", 1, "source", "ingress"), "direct"),
            (("entries", 1, "recorded_revision"), 2), (("entries", 1, "sequence"), 1),
            (("entries", 1, "output"), "x" * (128 * 1024)),
        ]
        for path, value in paths:
            changed = detail(); target = changed
            for key in path[:-1]: target = target[key]
            target[path[-1]] = value
            cases.append(changed)
        changed = detail(); changed["binding"]["owner"] = "never-public"; cases.append(changed)
        changed = detail(); changed["entries"][0]["command"] = {}; cases.append(changed)
        changed = detail(); del changed["binding"]["lookup"]; cases.append(changed)
        changed = detail(); del changed["next_cursor"]; cases.append(changed)
        cases.extend([
            detail(entries=[observation()]),  # Missing sequence 1 is not an empty first page.
            detail(entries=[]),
            detail(entries=[claim()]),  # Remaining evidence requires a next cursor.
            detail(entries=[claim(), claim(sequence=2, recorded_revision=3)]),
            detail(entries=[], next_cursor=inspect_cursor()),
            detail(entries=[claim()], next_cursor=inspect_cursor(2)),
            detail(next_cursor=inspect_cursor(2)),
            detail(entries=[claim()], next_cursor=inspect_cursor(identity=occurrence() | {"activation_id": "other"})),
        ])
        for index, value in enumerate(cases):
            self.serve(value)
            with self.subTest(index=index), self.assertRaises(ProtocolError): await self.client.effects.inspect(**IDENTITY)
        self.serve(detail(entries=[claim()]))
        with self.assertRaises(ProtocolError): await self.client.effects.inspect(**IDENTITY, cursor=inspect_cursor())
        self.serve(detail(entries=[]))
        with self.assertRaises(ProtocolError): await self.client.effects.inspect(**IDENTITY, cursor=inspect_cursor(3))

    async def test_cursor_page_rejects_entry_revision_not_after_its_sequence(self):
        # A page must validate its first entry independently of earlier pages.
        for factory in (claim, observation):
            for revision in (1, 2):
                self.serve(detail(entries=[factory(sequence=2, recorded_revision=revision)]))
                with self.subTest(kind=factory.__name__, revision=revision), self.assertRaises(ProtocolError):
                    await self.client.effects.inspect(**IDENTITY, cursor=inspect_cursor(1))
            self.serve(detail(entries=[factory(sequence=2, recorded_revision=3)]))
            page = await self.client.effects.inspect(**IDENTITY, cursor=inspect_cursor(1))
            self.assertEqual(page.entries[0].recorded_revision, 3)

    async def test_large_detail_sequence_and_complete_cursor_are_preserved(self):
        maximum = 2**64 - 1
        self.serve(detail(summary=summary(entry_count=maximum - 1, effect_revision=maximum),
                          entries=[observation(sequence=maximum - 1, recorded_revision=maximum)]))
        result = await self.client.effects.inspect(**IDENTITY, cursor=inspect_cursor(maximum - 2))
        self.assertEqual(result.entries[0].sequence, maximum - 1)
        self.assertEqual(result.entries[0].recorded_revision, maximum)
        self.assertIsNone(result.next_cursor)
        self.serve(detail(summary=summary(entry_count=maximum - 1, effect_revision=maximum), entries=[]))
        result = await self.client.effects.inspect(**IDENTITY, cursor=inspect_cursor(maximum - 1))
        self.assertEqual(result.entries, ())
        self.assertIsNone(result.next_cursor)

    async def test_metadata_rejects_wrong_filters_duplicates_order_and_cursor_binding(self):
        cases = [
            {"items": [summary(), summary()], "next_cursor": None},
            {"items": [summary(updated_at=14), summary()], "next_cursor": None},
            {"items": [], "next_cursor": inbox_cursor()},
            {"items": [summary()], "next_cursor": inbox_cursor(summary(updated_at=14))},
        ]
        foreign = summary(); foreign["occurrence"]["scope"]["tenant_id"] = "other"
        cases.append({"items": [foreign], "next_cursor": None})
        extra = summary() | {"output": "must-not-leak"}
        cases.append({"items": [extra], "next_cursor": None})
        for value in cases:
            self.serve(value)
            with self.subTest(value=value), self.assertRaises(ProtocolError): await self.client.effects.inbox()
        self.serve({"items": [summary()], "next_cursor": None})
        with self.assertRaises(ProtocolError): await self.client.effects.inbox(filter="unconfirmed")
        with self.assertRaises(ProtocolError): await self.client.effects.inbox(filter="discrepant")
        with self.assertRaises(ProtocolError): await self.client.effects.inbox(workflow_id="other")

    async def test_response_budget_is_enforced_before_decoding_sensitive_values(self):
        self.serve(detail(entries=[observation(output="x" * (1024 * 1024))]))
        with self.assertRaises(ProtocolError): await self.client.effects.inspect(**IDENTITY)
