"""Scoped approval observations, immutable commands and uncertain decisions (MIT)."""
import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

import test_workflows
from ledgence.client import (
    ApprovalDecisionCommand, ApprovalDecisionReceipt, ApprovalDecisionUncertain,
    ApprovalStatus, AsyncClient, Conflict, InputError, ProtocolError, RequestTimeout,
    ServiceError,
)
from ledgence.client.approval_models import parse_approval
from support import SCOPE, response


def snapshot(**changes):
    return {"scope": dict(SCOPE), "workflow_id": "workflow", "key": "refund",
            "activation_id": "requested", "revision": 1,
            "action": {"name": "program:refund", "version": "1", "arguments": {"amount": 50}},
            "proposed_arguments": {"amount": 100}, "created_at": 10, "deadline": 100,
            "status": "pending", "decision": None, "resumed_activation_id": None, **changes}


def receipt(body, *, duplicate=False):
    command = json.loads(body)
    approval = snapshot(**{name: command[name] for name in (
        "scope", "workflow_id", "key", "activation_id", "revision", "action")})
    approval["status"] = "approved" if command["decision"] == "approve" else "rejected"
    approval["decision"] = {name: command[name] for name in ("decision_id", "decision", "reviewer", "reason")}
    approval["decision"]["decided_at"] = 11
    return {"approval": approval, "already_accepted": duplicate}


class ApprovalClientTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflows.WorkflowClientTests.asyncSetUp

    def approval(self, **changes):
        return parse_approval(snapshot(**changes), self.client.scope, "workflow", "refund", self.url)

    def command(self, approval=None, **changes):
        return self.client.workflows.handle("workflow").prepare_approval_decision(
            approval or self.approval(), **({"decision_id": "review-1", "decision": "approve",
                                           "reviewer": "alice", "reason": "Reviewed"} | changes))

    async def test_inspect_typed_copies_and_bounded_page_use_scoped_posts(self):
        async def serve(request, body):
            if request.path.endswith("/inspect"): return response(snapshot())
            return response({"items": [snapshot()], "next_cursor": "refund"})
        self.callback = serve
        handle = self.client.workflows.handle("workflow")
        approval = await handle.approval("refund")
        self.assertIs(approval.status, ApprovalStatus.PENDING)
        approval.action.arguments["amount"] = 999
        approval.proposed_arguments["amount"] = 999
        approval.to_dict()["action"]["arguments"]["amount"] = 999
        self.assertEqual(approval.action.arguments, {"amount": 50})
        self.assertEqual(approval.proposed_arguments, {"amount": 100})
        with self.assertRaises(AttributeError): approval.status = ApprovalStatus.APPROVED
        page = await handle.approvals(after_key="a", limit=1)
        self.assertEqual(page.items, (approval,))
        self.assertEqual(page.next_cursor, "refund")
        self.assertEqual(self.requests[0][:3], ("POST", "/v1/workflows/approvals/inspect", {}))
        self.assertEqual(json.loads(self.requests[0][3]), {"scope": SCOPE, "workflow_id": "workflow", "key": "refund"})
        self.assertEqual(json.loads(self.requests[1][3]), {"scope": SCOPE, "workflow_id": "workflow", "after_key": "a", "limit": 1})

    async def test_frozen_decision_and_uncertainty_reconciliation_send_identical_bytes(self):
        bodies = []
        async def serve(request, body):
            bodies.append(body)
            if len(bodies) == 1: return response({"code": "unavailable"}, code=503)
            return response(receipt(body, duplicate=True))
        self.callback = serve
        handle = self.client.workflows.handle("workflow")
        command = self.command()
        command.to_dict()["action"]["arguments"]["amount"] = 999
        self.assertIs(type(command), ApprovalDecisionCommand)
        with self.assertRaises(TypeError): ApprovalDecisionCommand()
        with self.assertRaises(AttributeError): command.key = "other"
        with self.assertRaises(ApprovalDecisionUncertain) as raised: await handle.decide_approval(command)
        self.assertIs(raised.exception.command, command)
        self.assertEqual(len(bodies), 1)
        result = await handle.decide_approval(command)
        self.assertIs(type(result), ApprovalDecisionReceipt)
        self.assertIs(result.approval.status, ApprovalStatus.APPROVED)
        self.assertEqual(result.approval.action.arguments, {"amount": 50})
        self.assertTrue(result.already_accepted)
        self.assertEqual(bodies[0], bodies[1])

    async def test_saved_command_survives_client_restart_without_reinspecting_pending_state(self):
        handle = self.client.workflows.handle("workflow")
        command = self.command()
        saved = json.loads(json.dumps(command.to_dict()))
        restored = handle.restore_approval_decision(saved)
        saved["action"]["arguments"]["amount"] = 999
        self.assertEqual(restored._body, command._body)
        async def serve(request, body): return response(receipt(body, duplicate=True))
        self.callback = serve
        self.assertTrue((await handle.decide_approval(restored)).already_accepted)
        for changed in ({"scope": {**SCOPE, "tenant_id": "other"}}, {"workflow_id": "other"},
                        {"decision": "allow"}, {"extra": True}, {"revision": True}):
            with self.assertRaises(InputError): handle.restore_approval_decision(command.to_dict() | changed)

    async def test_wrong_scope_endpoint_request_and_nonpending_decision_never_dispatch(self):
        handle = self.client.workflows.handle("workflow")
        command = self.command()
        with self.assertRaises(InputError): await handle.decide_approval({})
        with self.assertRaises(InputError): await self.client.workflows.handle("other").decide_approval(command)
        for url, tenant in ((self.url, "other"), ("http://127.0.0.1:9", "tenant")):
            async with AsyncClient(url, tenant=tenant, namespace="tests") as client:
                other = client.workflows.handle("workflow")
                with self.assertRaises(InputError): await other.decide_approval(command)
                with self.assertRaises(InputError):
                    other.prepare_approval_decision(self.approval(), decision_id="d", decision="approve", reviewer="a")
        for status in ("expired", "cancelled"):
            with self.assertRaises(InputError): self.command(self.approval(status=status))
        for changes in ({"decision_id": ""}, {"reviewer": ""}, {"reviewer": "a\n"},
                        {"decision": True}, {"decision": "approved"}, {"reason": "x" * 4097}):
            with self.assertRaises(InputError): self.command(**changes)
        self.assertEqual(self.requests, [])

    async def test_receipt_checks_exact_numeric_action_and_decision_binding(self):
        handle = self.client.workflows.handle("workflow")
        mutations = [
            lambda a: a.update(workflow_id="other"),
            lambda a: a.update(scope={**SCOPE, "tenant_id": "other"}),
            lambda a: a.update(activation_id="other"),
            lambda a: a.update(revision=2),
            lambda a: a["action"].update(version="2"),
            lambda a: a["action"]["arguments"].update(amount=50.0),
            lambda a: a["action"]["arguments"].update(amount=True),
            lambda a: a["decision"].update(reviewer="other"),
            lambda a: a["decision"].update(decision_id="other"),
            lambda a: a["decision"].update(reason=None),
            lambda a: a["decision"].update(decided_at=100),
        ]
        for mutate in mutations:
            async def serve(request, body):
                value = receipt(body)
                mutate(value["approval"])
                return response(value)
            self.callback = serve
            with self.assertRaises(ApprovalDecisionUncertain) as raised:
                await handle.decide_approval(self.command())
            self.assertIsInstance(raised.exception.cause, ProtocolError)

    async def test_malformed_snapshot_and_pagination_are_observation_errors(self):
        handle = self.client.workflows.handle("workflow")
        changes = ({"status": "unknown"}, {"deadline": 9}, {"decision": {}},
                   {"status": "approved"}, {"revision": True}, {"resumed_activation_id": "a"},
                   {"action": {"name": "a", "version": "1", "arguments": []}},
                   {"scope": {**SCOPE, "namespace": "other"}}, {"extra": True},
                   {"proposed_arguments": []}, {"proposed_arguments": 100})
        for change in changes:
            async def serve(request, body): return response(snapshot(**change))
            self.callback = serve
            with self.assertRaises(ProtocolError): await handle.approval("refund")
        for page in ({"items": [], "next_cursor": "refund"},
                     {"items": [snapshot(), snapshot()], "next_cursor": None},
                     {"items": [snapshot()], "next_cursor": "wrong"}):
            async def serve(request, body): return response(page)
            self.callback = serve
            with self.assertRaises(ProtocolError): await handle.approvals(limit=1)
        for limit in (0, True, 11, 1.0):
            with self.assertRaises(InputError): await handle.approvals(limit=limit)

    async def test_definitive_expiry_conflict_and_rejection(self):
        handle = self.client.workflows.handle("workflow")
        for code, error in (("conflict", Conflict), ("obsolete_operation", ServiceError)):
            async def serve(request, body): return response({"code": code}, code=409)
            self.callback = serve
            with self.assertRaises(error): await handle.decide_approval(self.command())
        async def rejected(request, body): return response(receipt(body))
        self.callback = rejected
        result = await handle.decide_approval(self.command(decision="reject"))
        self.assertIs(result.approval.status, ApprovalStatus.REJECTED)

    async def test_timeout_and_cancellation_preserve_command_without_retry(self):
        handle = self.client.workflows.handle("workflow")
        command = self.command()
        for dispatched in (False, True):
            with patch.object(self.client._require_transport(), "exchange",
                              new=AsyncMock(side_effect=RequestTimeout(dispatched=dispatched))):
                expected = ApprovalDecisionUncertain if dispatched else RequestTimeout
                with self.assertRaises(expected): await handle.decide_approval(command)
        started = asyncio.Event()
        async def serve(request, body):
            started.set()
            await asyncio.sleep(.1)
            return response(receipt(body))
        self.callback = serve
        task = asyncio.create_task(handle.decide_approval(command))
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        async def reconcile(request, body): return response(receipt(body, duplicate=True))
        self.callback = reconcile
        self.assertTrue((await handle.decide_approval(command)).already_accepted)
        self.assertEqual(self.requests[0][3], self.requests[1][3])


if __name__ == "__main__": unittest.main()
