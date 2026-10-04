"""Action-bound durable approvals and guarded local execution (MIT)."""
import asyncio
import unittest

import test_workflow
from ledgence.worker.workflow import (
    ApprovalAction, ApprovalStatus, MAX_APPROVAL_BYTES, MAX_WAIT_MS, WorkflowError,
)


def refund(amount, currency="USD"):
    return {"amount": amount, "currency": currency}


def snapshot(fn=refund, status="approved", **changes):
    return {"scope": {"tenant_id": "tenant", "namespace": "tests"},
            "workflow_id": "workflow-1", "key": "refund", "activation_id": "requested",
            "revision": 0, "action": ApprovalAction.for_callable(
                fn, version="1", arguments={"amount": 50, "currency": "USD"}).to_dict(),
            "proposed_arguments": {"amount": 100}, "created_at": 10, "deadline": 20,
            "status": status, "decision": {
                "decision_id": "decision-1", "decision": "approve" if status == "approved" else "reject",
                "reviewer": "alice", "reason": None, "decided_at": 11,
            } if status in ("approved", "rejected") else None,
            "resumed_activation_id": "activation-1", **changes}


class ApprovalTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflow.WorkflowTests.asyncSetUp
    asyncTearDown = test_workflow.WorkflowTests.asyncTearDown
    context = test_workflow.WorkflowTests.context

    def approved_context(self, approval=None, **changes):
        return self.context(test_workflow.payload(
            revision=1, wake={"kind": "approval", "approval": approval or snapshot()}, **changes))

    async def test_request_freezes_normalized_arguments_and_defaults(self):
        original = {"amount": 100}
        effective = {"amount": min(original["amount"], 50)}
        action = ApprovalAction.for_callable(refund, version="1", arguments=effective)
        effective["amount"] = 999
        action.arguments["amount"] = 999
        context = self.context()
        decision = context.request_approval("refund", action=action, proposed_arguments=original,
                                            resume="after", state=None, timeout_ms=100)
        original["amount"] = 999
        self.assertEqual(decision["wait"]["action"]["arguments"], {"amount": 50, "currency": "USD"})
        self.assertEqual(decision["wait"]["proposed_arguments"], {"amount": 100})
        self.assertEqual(context._validate_decision(decision), decision)
        self.assertEqual(self.commits, [])
        for timeout in (0, MAX_WAIT_MS):
            self.assertEqual(context.wait_approval("refund", action=action, continuation="after",
                                                   state=None, timeout_ms=timeout)["wait"]["timeout_ms"], timeout)

    async def test_approved_local_uses_private_saved_arguments_and_replays_after_restart(self):
        calls = []
        def operation(amount, currency):
            calls.append((amount, currency))
            return {"refunded": amount}
        raw = snapshot(operation)
        context = self.approved_context(raw)
        raw["action"]["arguments"]["amount"] = 999
        context.wake["approval"]["action"]["arguments"]["amount"] = 999
        context.approval.action.arguments["amount"] = 999
        context.approval.to_dict()["action"]["arguments"]["amount"] = 999
        self.assertEqual(context.approval.status, ApprovalStatus.APPROVED)
        self.assertEqual(await context.approved_local(operation, version="1"), {"refunded": 50})
        resumed = self.approved_context(snapshot(operation), local_steps=self.commits)
        self.assertEqual(await resumed.approved_local(operation, version="1"), {"refunded": 50})
        self.assertEqual(calls, [(50, "USD")])
        self.assertEqual(len(self.commits), 1)
        with self.assertRaises(AttributeError): context.approval._encoded = b"changed"

    async def test_pending_rejected_expired_cancelled_and_wrong_binding_never_execute(self):
        for status in ("rejected", "expired"):
            context = self.approved_context(snapshot(status=status))
            self.assertEqual(context.approval.status, ApprovalStatus(status))
            with self.assertRaises(WorkflowError): context.approved_local(refund, version="1")
        with self.assertRaises(WorkflowError): self.approved_context(snapshot(status="pending"))
        with self.assertRaises(WorkflowError): self.approved_context(snapshot(status="cancelled"))
        with self.assertRaises(WorkflowError): self.context().approved_local(refund, version="1")
        context = self.approved_context()
        with self.assertRaises(WorkflowError): context.approved_local(refund, version="2")
        with self.assertRaises(WorkflowError): context.approved_local(lambda amount: amount, version="1")
        with self.assertRaises(TypeError): context.approved_local(refund, version="1", amount=99)
        with self.assertRaises(WorkflowError): context.local("_approval:forged", refund, amount=50)
        self.assertEqual(self.commits, [])

    async def test_same_concurrent_binding_shares_execution_and_different_callable_is_rejected(self):
        calls = []
        async def operation(amount, currency):
            calls.append(amount)
            await asyncio.sleep(0)
            return amount
        context = self.approved_context(snapshot(operation))
        first = context.approved_local(operation, version="1")
        second = context.approved_local(operation, version="1")
        def impostor(amount, currency): self.fail("second callable executed")
        impostor.__module__ = operation.__module__
        impostor.__qualname__ = operation.__qualname__
        with self.assertRaises(WorkflowError): context.approved_local(impostor, version="1")
        self.assertEqual(await context.gather(first, second), [50, 50])
        self.assertEqual(calls, [50])

    async def test_unreviewed_defaults_or_new_parameters_cannot_be_added_on_resume(self):
        raw = snapshot()
        # Generic action construction is valid, but approved_local cannot
        # silently apply an unreviewed Python default at execution time.
        raw["action"]["arguments"] = {"amount": 50}
        with self.assertRaisesRegex(WorkflowError, "defaults"):
            self.approved_context(raw).approved_local(refund, version="1")
        def added_parameter(amount, currency="USD", destination="new"):
            self.fail("new unreviewed parameter executed")
        added_parameter.__module__ = refund.__module__
        added_parameter.__qualname__ = refund.__qualname__
        with self.assertRaisesRegex(WorkflowError, "defaults"):
            self.approved_context().approved_local(added_parameter, version="1")
        self.assertEqual(self.commits, [])

    async def test_frozen_defaults_override_later_changes_and_kwargs_are_flattened(self):
        calls = []
        def operation(amount, currency="USD", **kwargs):
            calls.append((amount, currency, kwargs))
            return currency
        raw = snapshot(operation)
        operation.__defaults__ = ("EUR",)
        self.assertEqual(await self.approved_context(raw).approved_local(operation, version="1"), "USD")
        self.assertEqual(calls, [(50, "USD", {})])

    async def test_failed_operation_never_creates_successful_record(self):
        def failure(amount, currency): raise RuntimeError("provider failed")
        context = self.approved_context(snapshot(failure))
        with self.assertRaisesRegex(RuntimeError, "provider failed"):
            await context.approved_local(failure, version="1")
        self.assertEqual(self.commits, [])

    async def test_wake_validation_rejects_changed_identity_and_contradictory_audit(self):
        changes = ({"workflow_id": "other"}, {"resumed_activation_id": "other"},
                   {"activation_id": "activation-1"}, {"revision": 1}, {"revision": True},
                   {"deadline": 10}, {"created_at": 12}, {"status": "unknown"},
                   {"decision": None}, {"extra": True},
                   {"action": {"name": "refund", "version": "1", "arguments": []}})
        for changed in changes:
            with self.subTest(changed=changed), self.assertRaises(WorkflowError):
                self.approved_context(snapshot(**changed))
        for field, value in (("reviewer", ""), ("decision", "reject"), ("decided_at", 20),
                             ("reason", "x" * 4097), ("decision_id", True)):
            raw = snapshot()
            raw["decision"][field] = value
            with self.subTest(field=field), self.assertRaises(WorkflowError): self.approved_context(raw)

    async def test_request_shape_size_and_finite_deadline_are_enforced(self):
        context = self.context()
        action = ApprovalAction.for_callable(refund, version="1", arguments={"amount": 50})
        for timeout in (None, True, -1, 1.0, MAX_WAIT_MS + 1):
            with self.assertRaises(WorkflowError):
                context.request_approval("refund", action=action, continuation="after", state=None, timeout_ms=timeout)
        for changes in ({}, {"continuation": "after", "resume": "other"}):
            with self.assertRaises(WorkflowError):
                context.request_approval("refund", action=action, state=None, timeout_ms=1, **changes)
        for proposed in ([], "x", 100, {"x": "x" * MAX_APPROVAL_BYTES}):
            with self.assertRaises(WorkflowError):
                context.request_approval("refund", action=action, continuation="after", state=None,
                                         timeout_ms=1, proposed_arguments=proposed)
        for args in ([], {"amount": float("nan")}, {"amount": "x" * MAX_APPROVAL_BYTES}):
            with self.assertRaises(WorkflowError): ApprovalAction("refund", version="1", arguments=args)
        with self.assertRaises(WorkflowError): ApprovalAction.for_callable(refund, version="1", arguments={})


if __name__ == "__main__": unittest.main()
