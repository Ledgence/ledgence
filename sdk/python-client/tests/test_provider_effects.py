"""Provider approvals/read surfaces retain identity, provenance and numeric values (MIT)."""

import copy
import json
import unittest
from unittest.mock import AsyncMock

import test_effects as effects
import test_workflows
from test_approvals import snapshot, receipt
from support import SCOPE, response, status
from ledgence.client import (
    ApprovalDecisionUncertain,
    ApprovalProtection,
    InputError,
    ProtocolError,
    ProviderFilter,
    ProviderKnowledgeKind,
    ProviderReportEntry,
    ProviderSucceeded,
    ProviderLookup,
    ProviderProbeAssignment,
)
from ledgence.client.models import parse_status


def binding():
    return {
        "v": 1,
        "business": {"namespace": "exports", "key": "daily:1042"},
        "contract": {
            "adapter_id": "example.export",
            "version": "1",
            "digest": "sha256:" + "b" * 64,
            "completion_criterion": "The CSV artifact is committed",
            "dispatch_callable": "program:refund",
            "reconcile_callable": "program:probe",
            "lookup": "by_binding",
        },
        "target": {
            "provider": "example",
            "environment": "local",
            "account": "demo",
            "api_variant": "v1",
            "operation": "export",
            "endpoint_identity": "loopback-export",
            "dimensions": {"region": "local"},
        },
        "deduplication": {
            "domain": {
                "provider": "example",
                "namespace": "account",
                "dimensions": {"account": "demo"},
            },
            "key": "export:1042",
            "horizon": {"kind": "minimum_duration", "duration_ms": 1000},
        },
        "credential_reference": "example-local",
        "reconciliation": {
            "queue": "provider-checks",
            "initial_delay_ms": 1000,
            "interval_ms": 1000,
            "maximum_interval_ms": 5000,
            "max_checks": 3,
            "max_elapsed_ms": 60000,
            "check_timeout_ms": 1000,
        },
        "program": effects.binding()["program"],
    }


def provider_report(**changes):
    return {
        "reported_target": binding()["target"],
        "provider_reference": "export-1042",
        "provider_request_id": None,
        "knowledge": {"kind": "succeeded", "result": None},
        **changes,
    }


def report_entry(**changes):
    return {
        "kind": "provider_report",
        "sequence": 2,
        "recorded_at": 15,
        "recorded_revision": 3,
        "source_id": "source-1",
        "source": {"kind": "dispatch", "claim_id": "claim-1"},
        "report_id": "report-1",
        "worker": effects.worker(),
        "ingress": "live_dispatch",
        "report": provider_report(),
        **changes,
    }


def report_reference(entry=None):
    entry = entry or report_entry()
    return {
        name: entry[name]
        for name in (
            "source_id",
            "report_id",
            "sequence",
            "recorded_revision",
            "recorded_at",
        )
    } | {"knowledge": entry["report"]["knowledge"]["kind"]}


def provider_summary(**changes):
    return {
        "binding_digest": "sha256:" + "c" * 64,
        "knowledge": "succeeded",
        "selected_report": report_reference(),
        "conflicting_reports": [],
        "reconciliation": {
            "queue": "provider-checks",
            "checks_used": 0,
            "max_checks": 3,
            "generation": 0,
            "active_probe_id": None,
            "active_task_id": None,
            "first_due_at": 1000,
            "next_due_at": None,
            "deadline_at": 60000,
            "stopped_reason": "provider_terminal",
        },
        "handling": "provider_terminal",
        "candidate_revision": 3,
        **changes,
    }


def detail():
    bound = effects.binding()
    bound["lookup"] = None
    bound["approval"].update(
        protection="provider_operation", provider_binding=binding()
    )
    dispatch = {
        "kind": "provider_dispatch",
        "sequence": 1,
        "recorded_at": 12,
        "recorded_revision": 2,
        "claim_id": "claim-1",
        "source_id": "source-1",
        "report_id": "report-1",
        "worker": effects.worker(),
    }
    return {
        "binding": bound,
        "summary": effects.summary(
            journal_state="unconfirmed", provider=provider_summary()
        ),
        "entries": [dispatch, report_entry()],
        "next_cursor": None,
        "provider": {"current_report": report_entry()},
    }


class ProviderClientTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflows.WorkflowClientTests.asyncSetUp

    def serve(self, value):
        self.callback = AsyncMock(side_effect=lambda request, body: response(value))

    async def test_provider_metadata_cannot_falsely_confirm_local_journal(self):
        item = detail()["summary"]
        item["journal_state"] = "confirmed"
        self.serve({"items": [item], "next_cursor": None})
        with self.assertRaises(ProtocolError):
            await self.client.effects.inbox(provider_filter=ProviderFilter.ALL)

    async def test_stopped_recovery_is_not_pending_and_operator_handling_requires_assessment(
        self,
    ):
        item = detail()["summary"]
        item["provider"]["handling"] = "operator_closed"
        self.serve({"items": [item], "next_cursor": None})
        with self.assertRaises(ProtocolError):
            await self.client.effects.inbox(provider_filter=ProviderFilter.ALL)
        item = detail()["summary"]
        provider = item["provider"]
        provider.update(
            knowledge="pending", handling="unresolved", candidate_revision=None
        )
        provider["selected_report"]["knowledge"] = "pending"
        provider["reconciliation"]["stopped_reason"] = "workflow_cancelled"
        self.serve({"items": [item], "next_cursor": None})
        with self.assertRaises(ProtocolError):
            await self.client.effects.inbox(provider_filter=ProviderFilter.PENDING)
        page = await self.client.effects.inbox(provider_filter=ProviderFilter.ALL)
        self.assertEqual(
            page.items[0].provider.knowledge, ProviderKnowledgeKind.PENDING
        )

    async def test_terminal_dispute_preserves_closed_handling(self):
        item = detail()["summary"]
        item.update(entry_count=3, effect_revision=4)
        second = report_reference()
        second.update(
            source_id="source-2",
            report_id="report-2",
            sequence=3,
            recorded_revision=4,
            knowledge="failed",
        )
        item["provider"].update(
            knowledge="disputed",
            selected_report=None,
            conflicting_reports=[report_reference(), second],
            candidate_revision=4,
        )
        self.serve({"items": [item], "next_cursor": None})
        page = await self.client.effects.inbox(provider_filter=ProviderFilter.DISPUTED)
        self.assertEqual(
            page.items[0].provider.knowledge, ProviderKnowledgeKind.DISPUTED
        )
        # Two incompatible nonterminal references can also produce a dispute;
        # that does not invent a previously observed terminal provider result.
        item["provider"]["handling"] = "unresolved"
        item["provider"]["reconciliation"]["stopped_reason"] = "disputed"
        for reference in item["provider"]["conflicting_reports"]:
            reference["knowledge"] = "pending"
        self.serve({"items": [item], "next_cursor": None})
        page = await self.client.effects.inbox(provider_filter=ProviderFilter.DISPUTED)
        self.assertEqual(page.items[0].provider.handling, "unresolved")
        invalid = detail()["summary"]
        invalid["provider"]["handling"] = "unresolved"
        self.serve({"items": [invalid], "next_cursor": None})
        with self.assertRaises(ProtocolError):
            await self.client.effects.inbox(provider_filter=ProviderFilter.ALL)

    async def test_clean_assessment_closes_review_without_hiding_disputed_history(self):
        item = detail()["summary"]
        item.update(entry_count=5, effect_revision=6)
        conflicting = report_reference() | {
            "source_id": "source-2",
            "report_id": "report-2",
            "sequence": 3,
            "recorded_revision": 4,
            "knowledge": "pending",
        }
        item["provider"].update(
            knowledge="disputed",
            selected_report=None,
            conflicting_reports=[report_reference(), conflicting],
            handling="operator_closed",
            candidate_revision=5,
        )
        item["operator_resolution"] = {
            "reference": {
                "resolution_id": "assessment",
                "sequence": 4,
                "accepted_at": 16,
                "accepted_revision": 5,
                "basis_revision": 4,
                "supersedes_resolution_id": None,
                "assessment_kind": "record_success",
                "acceptance_event_id": "accepted-assessment",
            },
            "first_conflicting_entry_sequence": None,
        }
        self.serve({"items": [item], "next_cursor": None})
        page = await self.client.effects.inbox(provider_filter=ProviderFilter.DISPUTED)
        self.assertEqual(
            page.items[0].provider.knowledge, ProviderKnowledgeKind.DISPUTED
        )
        with self.assertRaises(ProtocolError):
            await self.client.effects.inbox(provider_filter=ProviderFilter.NEEDS_REVIEW)
        item["operator_resolution"]["first_conflicting_entry_sequence"] = 5
        self.serve({"items": [item], "next_cursor": None})
        page = await self.client.effects.inbox(
            provider_filter=ProviderFilter.NEEDS_REVIEW
        )
        self.assertEqual(
            page.items[0].operator_resolution.first_conflicting_entry_sequence, 5
        )

    async def test_provider_approval_freezes_full_binding_and_explicit_retry_preserves_bytes(
        self,
    ):
        handle = self.client.workflows.handle("workflow")
        pending = snapshot(protection="provider_operation", provider_binding=binding())
        bodies = []

        async def serve(request, body):
            if request.path.endswith("/inspect"):
                return response(pending)
            bodies.append(body)
            if len(bodies) == 1:
                return response({"code": "unavailable"}, code=503)
            accepted = receipt(body, duplicate=True)
            accepted["approval"]["provider_binding"] = json.loads(body)[
                "provider_binding"
            ]
            return response(accepted)

        self.callback = serve
        approval = await handle.approval("refund")
        self.assertIs(approval.protection, ApprovalProtection.PROVIDER_OPERATION)
        self.assertEqual(approval.provider_binding.to_dict(), binding())
        self.assertIs(
            approval.provider_binding.contract.lookup, ProviderLookup.BY_BINDING
        )
        copied = approval.to_dict()
        copied["provider_binding"]["target"]["account"] = "other"
        self.assertEqual(approval.provider_binding.target.account, "demo")
        command = handle.prepare_approval_decision(
            approval, decision_id="decision", decision="approve", reviewer="alice"
        )
        restored = handle.restore_approval_decision(command.to_dict())
        self.assertEqual(restored._body, command._body)
        with self.assertRaises(ApprovalDecisionUncertain):
            await handle.decide_approval(command)
        accepted = await handle.decide_approval(restored)
        self.assertEqual(accepted.approval.provider_binding, approval.provider_binding)
        self.assertEqual(bodies, [command._body, command._body])

    async def test_provider_binding_is_biconditional_and_validated_before_restore_or_response(
        self,
    ):
        handle = self.client.workflows.handle("workflow")
        invalid = [
            snapshot(protection="provider_operation"),
            snapshot(provider_binding=binding()),
            snapshot(protection="provider_operation", provider_binding=None),
            snapshot(protection="external_effect", provider_binding=binding()),
        ]
        for path, replacement in [
            (("contract", "dispatch_callable"), "other"),
            (("contract", "reconcile_callable"), None),
            (("contract", "lookup"), "unknown"),
            (("contract", "digest"), "invalid"),
            (("v",), True),
            (("target", "dimensions"), {str(i): "v" for i in range(9)}),
            (("deduplication", "domain", "provider"), "other"),
            (("reconciliation", "max_checks"), 0),
            (("reconciliation", "check_timeout_ms"), 30001),
            (("reconciliation", "initial_delay_ms"), 60000),
            (("program", "size"), 0),
        ]:
            raw = binding()
            selected = raw
            for part in path[:-1]:
                selected = selected[part]
            selected[path[-1]] = replacement
            invalid.append(
                snapshot(protection="provider_operation", provider_binding=raw)
            )
        for value in invalid:
            with self.subTest(value=value):
                self.serve(value)
                with self.assertRaises(ProtocolError):
                    await handle.approval("refund")
                command = {
                    name: value[name]
                    for name in (
                        "scope",
                        "workflow_id",
                        "key",
                        "activation_id",
                        "revision",
                        "action",
                    )
                }
                command.update(
                    {
                        key: value[key]
                        for key in ("protection", "provider_binding")
                        if key in value
                    }
                )
                command.update(
                    decision_id="decision",
                    decision="approve",
                    reviewer="alice",
                    reason=None,
                )
                with self.assertRaises(InputError):
                    handle.restore_approval_decision(command)

    async def test_no_lookup_and_no_provider_key_are_explicit_nullable_binding_values(
        self,
    ):
        raw = binding()
        raw["contract"].update(lookup="unavailable", reconcile_callable=None)
        raw["deduplication"] = None
        self.serve(snapshot(protection="provider_operation", provider_binding=raw))
        actual = await self.client.workflows.handle("workflow").approval("refund")
        self.assertEqual(actual.provider_binding.to_dict(), raw)
        for key in ("deduplication",):
            incomplete = copy.deepcopy(raw)
            incomplete.pop(key)
            self.serve(
                snapshot(protection="provider_operation", provider_binding=incomplete)
            )
            with self.assertRaises(ProtocolError):
                await self.client.workflows.handle("workflow").approval("refund")

    async def test_binding_drift_in_accepted_decision_remains_uncertain(self):
        self.serve(
            snapshot(protection="provider_operation", provider_binding=binding())
        )
        handle = self.client.workflows.handle("workflow")
        approval = await handle.approval("refund")
        command = handle.prepare_approval_decision(
            approval, decision_id="decision", decision="approve", reviewer="alice"
        )

        async def serve(request, body):
            accepted = receipt(body)
            accepted["approval"]["provider_binding"] = binding()
            accepted["approval"]["provider_binding"]["target"]["account"] = "different"
            return response(accepted)

        self.callback = serve
        with self.assertRaises(ApprovalDecisionUncertain):
            await handle.decide_approval(command)

    async def test_provider_detail_preserves_null_and_lossless_copied_application_values(
        self,
    ):
        for result in (
            None,
            {"integer": 2**64 - 1, "float": 1.0, "negative_zero": -0.0, "nul": "\x00"},
        ):
            raw = detail()
            raw["entries"][1]["report"]["knowledge"]["result"] = result
            raw["provider"]["current_report"] = copy.deepcopy(raw["entries"][1])
            self.serve(raw)
            observed = await self.client.effects.inspect(**effects.IDENTITY)
            self.assertIsInstance(observed.provider.current_report, ProviderReportEntry)
            self.assertIsInstance(
                observed.provider.current_report.report.knowledge, ProviderSucceeded
            )
            self.assertEqual(
                observed.provider.current_report.report.knowledge.result, result
            )
            if result is not None:
                copied = observed.provider.current_report.report.knowledge.result
                copied["integer"] = 0
                self.assertEqual(
                    observed.provider.current_report.report.knowledge.result["integer"],
                    2**64 - 1,
                )
            self.assertEqual(
                observed.summary.provider.knowledge, ProviderKnowledgeKind.SUCCEEDED
            )
            self.assertEqual(
                observed.binding.approval.provider_binding,
                observed.binding.approval.provider_binding,
            )

    async def test_current_provider_header_and_timeline_cannot_lie_about_identity_or_value(
        self,
    ):
        invalid = []
        for path, value in [
            (("provider", "current_report", "report_id"), "different"),
            (("provider", "current_report", "report", "knowledge", "result"), 1),
            (
                ("provider", "current_report", "report", "reported_target", "account"),
                "other",
            ),
            (("summary", "provider", "selected_report", "source_id"), "other"),
            (("summary", "provider", "reconciliation", "generation"), 1),
            (("summary", "provider", "reconciliation", "next_due_at"), 1200),
            (("summary", "journal_state"), "confirmed"),
            (("binding", "lookup"), "program:probe"),
            (("provider",), None),
            (("summary", "provider"), None),
        ]:
            raw = detail()
            target = raw
            for part in path[:-1]:
                target = target[part]
            target[path[-1]] = value
            invalid.append(raw)
        raw = detail()
        raw.pop("provider")
        invalid.append(raw)
        raw = detail()
        raw["entries"][0]["worker"]["audience"]["scope"]["namespace"] = "other"
        invalid.append(raw)
        for raw in invalid:
            with self.subTest(raw=raw):
                self.serve(raw)
                with self.assertRaises(ProtocolError):
                    await self.client.effects.inspect(**effects.IDENTITY)

    async def test_provider_filter_uses_versioned_bound_cursor_and_never_changes_old_requests(
        self,
    ):
        item = detail()["summary"]
        position = {"updated_at": item["updated_at"], **effects.IDENTITY}
        cursor = effects.cursor(
            {
                "version": 2,
                "scope": SCOPE,
                "filter": "all",
                "workflow_id": None,
                "provider_filter": "succeeded",
                "position": position,
            }
        )
        self.serve({"items": [item], "next_cursor": cursor})
        page = await self.client.effects.inbox(provider_filter=ProviderFilter.SUCCEEDED)
        self.assertEqual(page.next_cursor, cursor)
        self.assertEqual(
            json.loads(self.requests[-1][3])["provider_filter"], "succeeded"
        )
        before = len(self.requests)
        for kwargs in (
            {"provider_filter": "failed", "cursor": cursor},
            {"provider_filter": "succeeded", "cursor": effects.inbox_cursor(item)},
            {"cursor": cursor},
            {"filter": "unconfirmed", "provider_filter": "all"},
        ):
            with self.assertRaises(InputError):
                await self.client.effects.inbox(**kwargs)
        self.assertEqual(len(self.requests), before)
        self.serve({"items": [item], "next_cursor": None})
        with self.assertRaises(ProtocolError):
            await self.client.effects.inbox(provider_filter="failed")
        await self.client.effects.inbox()
        self.assertNotIn("provider_filter", json.loads(self.requests[-1][3]))

    async def test_all_typed_provider_outcomes_decode_without_inventing_failure_from_unknown(
        self,
    ):
        from ledgence.client.provider_models import parse_provider_report

        for knowledge in (
            {"kind": "unknown", "reason": "lookup temporarily missing"},
            {"kind": "pending"},
            {"kind": "requires_action", "action_kind": "confirm_account"},
            {"kind": "succeeded", "result": None},
            {
                "kind": "failed",
                "failure": {"kind": "invalid_dataset", "message": "not found"},
            },
            {"kind": "cancelled", "reason": "provider cancelled"},
        ):
            parsed = parse_provider_report(provider_report(knowledge=knowledge))
            self.assertEqual(parsed.knowledge.kind, knowledge["kind"])
        for invalid in (
            {"kind": "disputed"},
            {"kind": "succeeded"},
            {"kind": "pending", "result": None},
        ):
            with self.assertRaises(InputError):
                parse_provider_report(provider_report(knowledge=invalid))

    async def test_trusted_probe_task_marker_has_strict_scope_and_parent_identity(self):
        raw = status() | {
            "workflow_id": "workflow",
            "provider_probe": {
                "occurrence": effects.occurrence(),
                "probe_id": "probe-1",
                "generation": 2**64 - 1,
            },
        }
        parsed = parse_status(raw, self.client.scope, "task")
        self.assertIsInstance(parsed.provider_probe, ProviderProbeAssignment)
        self.assertEqual(parsed.provider_probe.generation, 2**64 - 1)
        for value in (
            raw | {"provider_probe": None},
            raw | {"workflow_id": "different"},
            raw | {"workflow_activation_id": "task"},
        ):
            with self.assertRaises(ProtocolError):
                parse_status(value, self.client.scope, "task")


class ProviderAssessmentConflictTests(unittest.TestCase):
    def test_failure_dispute_requires_typed_provider_evidence(self):
        from ledgence.client.effect_resolution_models import parse_resolution_summary

        reference = {
            "resolution_id": "r",
            "sequence": 1,
            "accepted_at": 1,
            "accepted_revision": 2,
            "basis_revision": 1,
            "supersedes_resolution_id": None,
            "assessment_kind": "record_failure",
            "acceptance_event_id": "audit",
        }
        raw = {"reference": reference, "first_conflicting_entry_sequence": 2}
        self.assertEqual(
            parse_resolution_summary(
                raw, 2, 3, provider=True
            ).first_conflicting_entry_sequence,
            2,
        )
        with self.assertRaises(InputError):
            parse_resolution_summary(raw, 2, 3)
        reference["assessment_kind"] = "close_unknown"
        with self.assertRaises(InputError):
            parse_resolution_summary(raw, 2, 3, provider=True)
