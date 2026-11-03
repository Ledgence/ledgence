"""Admission client boundaries using real loopback HTTP; no provider services."""

import json
import unittest
from unittest.mock import AsyncMock

import test_workflows
from ledgence.client import (
    AdmissionOccurrence,
    AdmissionState,
    AuditActorClass,
    AuditAdmission,
    AuditAdmissionReleased,
    AuditFilter,
    AuditResourceKind,
    AuditResourcePolicy,
    AuditResourcePolicyAccepted,
    Forbidden,
    InputError,
    ProtocolError,
    ResourcePolicy,
    ResourceRate,
    Scope,
    Unavailable,
)
from support import SCOPE, response

DIGEST = "sha256:" + "a" * 64


def occurrence():
    return {
        "scope": SCOPE,
        "workflow_id": "workflow",
        "origin_activation_id": "original",
        "key": "model:1",
    }


def record(**changes):
    return {
        "scope": SCOPE,
        "name": "model",
        "revision": 1,
        "policy": ResourcePolicy(2).to_dict(),
        "updated_at": 1000,
        **changes,
    }


def resource(**changes):
    return {
        "resource": record(),
        "offered": 1,
        "occupied": 2,
        "unknown": 1,
        "quarantined": False,
        **changes,
    }


def binding():
    return {
        "resource": "model",
        "program": {
            "program": {"id": "workflow", "version": "1"},
            "digest": DIGEST,
            "size": 1,
        },
        "operation": {
            "callable": "program:model",
            "kind": "model",
            "version": "1",
            "arguments": {"prompt": "hello"},
        },
    }


def source():
    return {
        "occurrence": occurrence(),
        "source_id": "source",
        "start_id": "start",
        "finish_id": "finish",
        "generation": 1,
        "binding_digest": DIGEST,
        "owner": {
            "scope": SCOPE,
            "task_id": "activation",
            "attempt_id": "attempt",
            "lease_id": "lease",
            "generation": 1,
            "worker_session_id": "worker",
            "consumer_id": 0,
        },
        "worker": {
            "audience": {
                "scope": SCOPE,
                "authority_id": "authority",
                "instance_id": "instance",
            },
            "issuer": "issuer",
            "subject": "worker",
            "credential_id": "credential",
        },
        "accepted_at": 1000,
        "start_deadline_at": 6000,
    }


def snapshot(**changes):
    return {
        "occurrence": occurrence(),
        "binding": binding(),
        "state": "unknown_occupied",
        "revision": 3,
        "created_at": 0,
        "updated_at": 1000,
        "deadline_at": 10000,
        "offer": None,
        "source": source(),
        "finish": None,
        **changes,
    }


class AdmissionClientTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflows.WorkflowClientTests.asyncSetUp

    def serve(self, value, status=200):
        self.callback = AsyncMock(
            side_effect=lambda req, body: response(value, code=status)
        )

    def occurrence(self):
        return AdmissionOccurrence(self.client.scope, "workflow", "original", "model:1")

    async def test_policy_create_and_exact_retry_keep_request_id(self):
        self.serve({"resource": record(), "already_accepted": False})
        receipt = await self.client.resources.put(
            "model",
            policy=ResourcePolicy(2),
            request_id="create",
            reason="bounded model calls",
        )
        self.assertEqual(receipt.resource.policy.max_occupancy, 2)
        body = self.requests[-1][3]
        self.serve({"resource": record(), "already_accepted": True})
        await self.client.resources.put(
            "model",
            policy=ResourcePolicy(2),
            request_id="create",
            reason="bounded model calls",
        )
        self.assertEqual(body, self.requests[-1][3])
        self.assertEqual(self.requests[0][1], "/v1/resources/put")

    async def test_cas_receipt_mismatch_is_rejected(self):
        self.serve({"resource": record(revision=1), "already_accepted": False})
        with self.assertRaises(ProtocolError):
            await self.client.resources.put(
                "model",
                policy=ResourcePolicy(2),
                request_id="edit",
                reason="increase",
                expected_revision=1,
            )

    async def test_resource_snapshot_exposes_occupied_including_offers_and_unknown(
        self,
    ):
        self.serve(resource())
        result = await self.client.resources.get("model")
        self.assertEqual((result.offered, result.occupied, result.unknown), (1, 2, 1))
        self.serve(resource(occupied=1))
        with self.assertRaises(ProtocolError):
            await self.client.resources.get("model")

    async def test_list_is_ordered_scoped_and_cursor_bound(self):
        self.serve({"items": [resource()], "next": "model"})
        page = await self.client.resources.list(limit=1)
        self.assertEqual(page.next, "model")
        self.serve({"items": [resource()], "next": None})
        with self.assertRaises(ProtocolError):
            await self.client.resources.list(after="model")
        self.serve({"items": [resource()], "next": "other"})
        with self.assertRaises(ProtocolError):
            await self.client.resources.list()

    async def test_inspection_returns_copies_without_sensitive_repr(self):
        self.serve(snapshot())
        result = await self.client.admissions.inspect(self.occurrence())
        self.assertEqual(result.state, AdmissionState.UNKNOWN_OCCUPIED)
        result.binding["operation"]["arguments"]["prompt"] = "changed"
        self.assertEqual(result.binding["operation"]["arguments"]["prompt"], "hello")
        self.assertNotIn("hello", repr(result))
        self.assertEqual(self.requests[-1][1], "/v1/admissions/inspect")

    async def test_late_finish_retains_admin_release_state(self):
        self.serve(
            snapshot(
                state="administratively_released",
                updated_at=2000,
                finish={
                    "source": source(),
                    "output": {"number": 1.0},
                    "accepted_at": 2000,
                    "already_accepted": False,
                },
            )
        )
        result = await self.client.admissions.inspect(self.occurrence())
        self.assertEqual(result.state, AdmissionState.ADMINISTRATIVELY_RELEASED)
        self.assertEqual(result.finish["output"], {"number": 1.0})

    async def test_malformed_or_foreign_snapshot_is_rejected(self):
        for changes in [
            {"state": "finished"},
            {"offer": {}},
            {"occurrence": dict(occurrence(), key="other")},
            {"revision": True},
            {"revision": 2**53},
            {"updated_at": -1},
        ]:
            self.serve(snapshot(**changes))
            with self.assertRaises(ProtocolError):
                await self.client.admissions.inspect(self.occurrence())

    async def test_release_is_explicit_fenced_once_and_never_asserts_success(self):
        self.serve(
            {
                "occurrence": occurrence(),
                "source_id": "source",
                "revision": 4,
                "accepted_at": 2000,
                "already_accepted": False,
            }
        )
        receipt = await self.client.admissions.release(
            self.occurrence(),
            source_id="source",
            expected_revision=3,
            request_id="operator-1",
            reason="external completion independently checked",
        )
        self.assertEqual(receipt.revision, 4)
        self.assertEqual(len(self.requests), 1)
        body = json.loads(self.requests[-1][3])
        self.assertEqual(body["expected_revision"], 3)
        self.assertNotIn("output", body)

    async def test_foreign_scope_invalid_policy_or_overflow_fail_before_http(self):
        for args in [
            {"max_occupancy": 0},
            {"max_occupancy": True},
            {"max_occupancy": 1, "enabled": 1},
            {"max_occupancy": 1, "start_timeout_ms": 99},
        ]:
            with self.assertRaises(InputError):
                ResourcePolicy(**args)
        with self.assertRaises(InputError):
            ResourceRate(1, 0, 1)
        foreign = AdmissionOccurrence(
            Scope("other", "tests"), "workflow", "original", "model:1"
        )
        with self.assertRaises(InputError):
            await self.client.admissions.inspect(foreign)
        with self.assertRaises(InputError):
            await self.client.admissions.release(
                self.occurrence(),
                source_id="source",
                expected_revision=2**53,
                request_id="id",
                reason="reviewed",
            )
        self.assertEqual(self.requests, [])

    async def test_denial_unavailability_are_not_empty_or_retried(self):
        for code, status, error in [
            ("forbidden", 403, Forbidden),
            ("unavailable", 503, Unavailable),
        ]:
            self.serve(
                {"code": code}
                | ({"message": "not available"} if status == 503 else {}),
                status,
            )
            before = len(self.requests)
            with self.assertRaises(error):
                await self.client.resources.get("model")
            self.assertEqual(len(self.requests), before + 1)

    async def test_public_resource_audit_filter_decodes_actor_revision_and_rejects_foreign_events(
        self,
    ):
        event = {
            "event_id": "policy-audit",
            "recorded_at": 2000,
            "audience": {
                "authority_id": "authority",
                "instance_id": "instance",
                "scope": SCOPE,
            },
            "actor": {
                "issuer": "issuer",
                "subject": "operator",
                "class": "operator",
                "credential_id": "credential",
            },
            "operation": {
                "kind": "resource_policy_accepted",
                "name": "model",
                "request_id": "create",
                "revision": 1,
                "policy_digest": DIGEST,
                "reason": "capacity",
            },
        }
        self.serve({"items": [event], "next_cursor": None})
        selected = AuditFilter.resource(AuditResourceKind.RESOURCE, "model")
        page = await self.client.audit.list(filter=selected)
        accepted = page.items[0]
        self.assertIsInstance(accepted.operation, AuditResourcePolicyAccepted)
        self.assertEqual(
            (accepted.operation.revision, accepted.operation.request_id), (1, "create")
        )
        self.assertIs(accepted.actor.principal_class, AuditActorClass.OPERATOR)
        self.assertEqual(
            (accepted.actor.subject, accepted.actor.credential_id),
            ("operator", "credential"),
        )
        self.assertEqual(json.loads(self.requests[-1][3])["filter"], selected.to_dict())
        self.assertEqual(AuditResourcePolicy("model").kind, "resource")
        self.assertEqual(
            AuditAdmission(self.occurrence()).occurrence, self.occurrence()
        )
        self.assertEqual(
            AuditAdmissionReleased(
                self.occurrence(), "source", "request", 2, "reviewed"
            ).kind,
            "admission_released",
        )
        for operation in [
            dict(event["operation"], name="other"),
            dict(event["operation"], revision=True),
        ]:
            self.serve(
                {"items": [dict(event, operation=operation)], "next_cursor": None}
            )
            with self.assertRaises(ProtocolError):
                await self.client.audit.list(filter=selected)

    async def test_audit_parses_policy_release_and_sensitive_inspection(self):
        from ledgence.client.audit import _event, _matches

        base = {
            "event_id": "audit",
            "recorded_at": 2000,
            "audience": {
                "authority_id": "authority",
                "instance_id": "instance",
                "scope": SCOPE,
            },
            "actor": {
                "issuer": "issuer",
                "subject": "operator",
                "class": "operator",
                "credential_id": "credential",
            },
        }
        for operation, kind, id in [
            (
                {
                    "kind": "resource_policy_accepted",
                    "name": "model",
                    "request_id": "edit",
                    "revision": 1,
                    "policy_digest": DIGEST,
                    "reason": "limited",
                },
                "resource",
                "model",
            ),
            (
                {
                    "kind": "admission_released",
                    "occurrence": occurrence(),
                    "source_id": "source",
                    "request_id": "release",
                    "revision": 4,
                    "reason": "reviewed",
                },
                "workflow",
                "workflow",
            ),
            (
                {
                    "kind": "sensitive_response",
                    "read": "admission_inspect",
                    "resource": {"kind": "admission", "occurrence": occurrence()},
                },
                "workflow",
                "workflow",
            ),
        ]:
            event = _event(dict(base, operation=operation), self.client.scope)
            self.assertTrue(
                _matches(
                    event,
                    {"kind": "resource", "resource_kind": kind, "resource_id": id},
                )
            )


class PublicExportTests(unittest.TestCase):
    def test_public_exports_resolve_without_duplicate_names(self):
        import ledgence.client as client

        self.assertEqual(len(client.__all__), len(set(client.__all__)))
        for name in client.__all__:
            self.assertTrue(hasattr(client, name), name)
