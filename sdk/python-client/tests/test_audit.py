"""Operator audit metadata never changes approval bytes or supplies authority (MIT)."""
import json
from dataclasses import replace
import unittest
from unittest.mock import AsyncMock

import test_workflows
from support import SCOPE, response
from test_approvals import snapshot
from ledgence.client import (
    AuditAcceptance, AuditActorClass, AuditFilter, AuditReadKind, AuditSensitiveResponse,
    Forbidden, InputError, ProtocolError, Unavailable,
)
from ledgence.client.approval_models import parse_approval


def accepted():
    return snapshot(status="approved", decision={"decision_id": "decision-1", "decision": "approve",
                    "reviewer": "claimed-name", "reason": None, "decided_at": 11}, revision=2**64 - 1)


def event(**changes):
    return {"event_id": "acceptance-1", "recorded_at": 11,
            "audience": {"authority_id": "authority", "instance_id": "instance", "scope": dict(SCOPE)},
            "actor": {"issuer": "urn:ledgence:authority:authority", "subject": "original-operator",
                      "class": "operator", "credential_id": "original-credential"},
            "operation": {"kind": "approval_accepted", "workflow_id": "workflow", "key": "refund",
                          "activation_id": "requested", "revision": 2**64 - 1,
                          "decision_id": "decision-1", "decision": "approve"}, **changes}


def page(*items, cursor=None): return {"items": list(items), "next_cursor": cursor}


class AuditTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflows.WorkflowClientTests.asyncSetUp

    def approval(self, **changes):
        return parse_approval(accepted() | changes, self.client.scope, "workflow", "refund", self.url)

    async def test_exact_acceptance_is_typed_scoped_and_does_not_modify_approval(self):
        self.callback = AsyncMock(side_effect=lambda request, body: response(page(event())))
        observed = self.approval()
        before = observed.to_dict()
        result = await self.client.audit.approval_acceptance(observed)
        self.assertEqual(result.actor.subject, "original-operator")
        self.assertEqual(result.actor.principal_class, AuditActorClass.OPERATOR)
        self.assertEqual(result.actor.credential_id, "original-credential")
        self.assertIsInstance(result.operation, AuditAcceptance)
        self.assertEqual(result.operation.revision, 2**64 - 1)
        self.assertEqual(observed.to_dict(), before)
        method, route, _, body = self.requests[0]
        self.assertEqual((method, route), ("POST", "/v1/audit/events"))
        self.assertEqual(json.loads(body), {"scope": SCOPE, "filter": {
            "kind": "approval_acceptance", "workflow_id": "workflow", "key": "refund", "activation_id": "requested",
            "revision": 2**64 - 1, "decision_id": "decision-1"}, "cursor": None, "limit": 2})

    async def test_empty_is_unverified_but_denied_or_unavailable_is_not(self):
        self.callback = AsyncMock(side_effect=lambda request, body: response(page()))
        self.assertIsNone(await self.client.audit.approval_acceptance(self.approval()))
        for code, status, error in [("forbidden", 403, Forbidden), ("unavailable", 503, Unavailable)]:
            payload = {"code": code} | ({"message": "Audit unavailable"} if status == 503 else {})
            self.callback = AsyncMock(side_effect=lambda request, body, payload=payload, status=status: response(payload, code=status))
            with self.assertRaises(error): await self.client.audit.approval_acceptance(self.approval())
        self.assertEqual(len(self.requests), 3)

    async def test_exact_binding_rejects_mismatched_occurrence_decision_time_and_scope(self):
        bad = []
        for field in ("workflow_id", "key", "activation_id", "revision", "decision_id", "decision"):
            changed = event(); changed["operation"][field] = 0 if field == "revision" else "reject" if field == "decision" else "other"
            bad.append(page(changed))
        changed = event(); changed["audience"]["scope"]["namespace"] = "other"; bad.append(page(changed))
        bad.extend([page(event(recorded_at=12)), page(event(), event(event_id="duplicate")), page(event(), cursor="unexpected")])
        for result in bad:
            self.callback = AsyncMock(side_effect=lambda request, body, result=result: response(result))
            with self.subTest(result=result), self.assertRaises(ProtocolError):
                await self.client.audit.approval_acceptance(self.approval())

    async def test_strict_event_shape_and_resource_read_pairing(self):
        bad = []
        for field in ("actor", "audience", "event_id", "recorded_at", "operation"):
            changed = event(); changed.pop(field); bad.append(changed)
        for path, value in [("class", "worker"), ("subject", ""), ("issuer", "x" * 257), ("credential_id", "x" * 129)]:
            changed = event(); changed["actor"][path] = value; bad.append(changed)
        changed = event(); changed["actor"]["token"] = "forbidden-field"; bad.append(changed)
        changed = event(); changed["operation"]["revision"] = True; bad.append(changed)
        bad.append(event(operation={"kind": "sensitive_response", "read": "task_result", "resource": {"kind": "workflow", "workflow_id": "workflow"}}))
        bad.append(event(operation={"kind": "sensitive_response", "read": "approval_list", "resource": {"kind": "approval_page", "workflow_id": "workflow", "keys": ["same", "same"]}}))
        for item in bad:
            self.callback = AsyncMock(side_effect=lambda request, body, item=item: response(page(item)))
            with self.subTest(item=item), self.assertRaises(ProtocolError): await self.client.audit.list()

    async def test_scoped_resource_listing_handles_every_read_variant_and_opaque_cursor(self):
        variants = [
            ("task_inspect", {"kind": "task", "task_id": "task"}),
            ("task_result", {"kind": "task", "task_id": "task"}),
            ("workflow_result", {"kind": "workflow", "workflow_id": "workflow"}),
            ("workflow_input", {"kind": "workflow", "workflow_id": "workflow"}),
            ("local_step_inspect", {"kind": "local_step", "workflow_id": "workflow", "activation_id": "a", "step_key": "step"}),
            ("approval_inspect", {"kind": "approval", "workflow_id": "workflow", "key": "refund"}),
            ("approval_list", {"kind": "approval_page", "workflow_id": "workflow", "keys": ["refund"]}),
            ("approval_decision", {"kind": "approval", "workflow_id": "workflow", "key": "refund"}),
        ]
        for read, resource in variants:
            item = event(operation={"kind": "sensitive_response", "read": read, "resource": resource})
            self.callback = AsyncMock(side_effect=lambda request, body, item=item: response(page(item, cursor="next")))
            selected = AuditFilter.resource("task" if resource["kind"] == "task" else "workflow", resource.get("task_id", "workflow"))
            result = await self.client.audit.list(filter=selected, limit=1, cursor="prior")
            self.assertEqual(result.next_cursor, "next")
            self.assertIsInstance(result.items[0].operation, AuditSensitiveResponse)
            self.assertEqual(result.items[0].operation.read, AuditReadKind(read))
        self.callback = AsyncMock(side_effect=lambda request, body: response(page(event(), cursor="same")))
        with self.assertRaises(ProtocolError): await self.client.audit.list(cursor="same", limit=1)
        self.callback = AsyncMock(side_effect=lambda request, body: response(page(event(), event())))
        with self.assertRaises(ProtocolError): await self.client.audit.list()
        other = event(event_id="other"); other["audience"]["instance_id"] = "other"
        self.callback = AsyncMock(side_effect=lambda request, body: response(page(event(), other)))
        with self.assertRaises(ProtocolError): await self.client.audit.list()

    async def test_invalid_queries_fail_without_dispatch(self):
        for limit in (0, 101, True, 1.5):
            with self.assertRaises(InputError): await self.client.audit.list(limit=limit)
        for cursor in ("", "x" * 8193, 3):
            with self.assertRaises(InputError): await self.client.audit.list(cursor=cursor)
        with self.assertRaises(InputError): await self.client.audit.list(filter={"kind": "all"})
        exact = AuditFilter.approval_acceptance(workflow_id="workflow", key="refund", activation_id="requested", revision=1, decision_id="d")
        with self.assertRaises(InputError): await self.client.audit.list(filter=exact)
        with self.assertRaises(InputError): await self.client.audit.list(filter=exact, limit=2, cursor="next")
        pending = parse_approval(snapshot(), self.client.scope, "workflow", "refund", self.url)
        with self.assertRaises(InputError): await self.client.audit.approval_acceptance(pending)
        foreign = parse_approval(accepted(), self.client.scope, "workflow", "refund", "https://other.example")
        with self.assertRaises(InputError): await self.client.audit.approval_acceptance(foreign)
        inconsistent = replace(self.approval(), status="invented")
        with self.assertRaises(InputError): await self.client.audit.approval_acceptance(inconsistent)
        inconsistent = replace(self.approval(), decision=replace(self.approval().decision, decision="reject"))
        with self.assertRaises(InputError): await self.client.audit.approval_acceptance(inconsistent)
        self.assertEqual(self.requests, [])
