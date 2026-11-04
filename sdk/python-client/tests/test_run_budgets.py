"""Run budget submission, lineage, paging and errors over a real loopback socket."""

import json
import unittest

import test_workflows
from ledgence.client import (
    InputError,
    ProtocolError,
    RunBudgetDenial,
    RunBudgetDenied,
    RunBudgetPolicy,
    TaskAdmission,
)
from support import SCOPE, response

ROOT = {"kind": "workflow", "workflow_id": "root"}
CHILD = {"kind": "task", "task_id": "child"}
MAX = (1 << 64) - 1
ARGS = {
    "program": "model",
    "version": "1",
    "queue": "support",
    "data": {"budget": "user input"},
    "idempotency_key": "same",
}


def row(seq, **kw):
    return {
        "scope": SCOPE,
        "root": ROOT,
        "subject": CHILD,
        "sequence": str(seq),
        "action": "call",
        "outcome": "authorized",
        "reason": None,
        "source_id": "start-1",
        "usage": {"kind": "reported", "input_tokens": 0, "output_tokens": 13},
        "recorded_at": 500,
        **kw,
    }


class RunBudgetTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflows.WorkflowClientTests.asyncSetUp

    def serve(self, value, code=200):
        async def callback(request, body):
            return response(value, code=code)

        self.callback = callback

    def test_new_submission_preserves_absence_and_explicit_zero_limits(self):
        policy = RunBudgetPolicy(max_calls=0, max_delegations=1, deadline_at_ms=100)
        for api, extra in [
            (self.client.workflows, {}),
            (self.client.tasks, {"admission": TaskAdmission("model")}),
        ]:
            default = api.prepare(**ARGS, **extra).to_dict()
            self.assertNotIn("budget", default["input"])
            bounded = api.prepare(**ARGS, budget=policy, **extra)
            self.assertEqual(
                bounded.to_dict()["input"]["budget"],
                {"max_calls": 0, "max_delegations": 1, "deadline_at_ms": 100},
            )
            self.assertEqual(
                bounded.to_dict()["input"]["data"], {"budget": "user input"}
            )
            bounded.to_dict()["input"]["budget"]["max_calls"] = 5
            self.assertEqual(bounded.to_dict()["input"]["budget"]["max_calls"], 0)
        for invalid in [
            {},
            {"deadline_at_ms": -1},
            {"max_calls": True},
            {"max_delegations": 1_000_001},
        ]:
            with self.assertRaises(InputError):
                RunBudgetPolicy(**invalid)

    async def test_descendant_snapshot_and_lossless_cursor(self):
        record = {
            "scope": SCOPE,
            "subject": CHILD,
            "root": ROOT,
            "policy": {"max_calls": 1},
            "calls": "1",
            "delegations": "1",
            "observed_at": 500,
        }
        self.serve(record)
        snapshot = await self.client.budgets.inspect(task_id="child")
        self.assertEqual(snapshot.root.id, "root")
        self.assertEqual(snapshot.calls, 1)
        wire = {
            "scope": SCOPE,
            "subject": CHILD,
            "root": ROOT,
            "items": [
                row(MAX),
                row(
                    MAX - 1,
                    action="delegation",
                    outcome="denied",
                    reason="delegations_exhausted",
                    usage={"kind": "unknown"},
                ),
            ],
            "next": str(MAX - 1),
        }
        self.serve(wire)
        page = await self.client.budgets.entries(task_id="child", limit=2)
        self.assertEqual(page.next, MAX - 1)
        self.assertEqual(page.items[0].usage.input_tokens, 0)
        self.assertEqual(page.items[1].reason, RunBudgetDenial.DELEGATIONS_EXHAUSTED)
        self.serve({**wire, "items": [], "next": None})
        await self.client.budgets.entries(task_id="child", limit=2, before=page.next)
        self.assertEqual(json.loads(self.requests[-1][3])["before"], str(MAX - 1))

    async def test_unlimited_dimension_preserves_u64_and_rejects_malformed_counts(self):
        base = {
            "scope": SCOPE,
            "subject": CHILD,
            "root": ROOT,
            "policy": {"deadline_at_ms": 100},
            "observed_at": 20,
            "calls": str((1 << 53) + 1),
            "delegations": str((1 << 32) + 1),
        }
        self.serve(base)
        observed = await self.client.budgets.inspect(task_id="child")
        self.assertEqual(observed.calls, (1 << 53) + 1)
        self.assertEqual(observed.delegations, (1 << 32) + 1)
        for invalid in [0, "00", "+1", "-1", str(MAX + 1), None]:
            self.serve({**base, "calls": invalid})
            with self.assertRaises(ProtocolError):
                await self.client.budgets.inspect(task_id="child")
        self.serve({**base, "policy": {"max_calls": 1}, "calls": "2"})
        with self.assertRaises(ProtocolError):
            await self.client.budgets.inspect(task_id="child")

    async def test_approval_occurrence_binds_call_subject_and_scope(self):
        origin = {
            "scope": SCOPE,
            "workflow_id": ROOT["workflow_id"],
            "activation_id": "a/1",
            "approval_key": "review:1",
        }
        entry = row(3, subject=ROOT, approval=origin)
        base = {
            "scope": SCOPE,
            "subject": CHILD,
            "root": ROOT,
            "items": [entry],
            "next": None,
        }
        self.serve(base)
        page = await self.client.budgets.entries(task_id="child")
        self.assertEqual(page.items[0].approval.to_dict(), origin)
        for changes in [
            {"approval": None},
            {"approval": {**origin, "workflow_id": "other"}},
            {"approval": {**origin, "scope": {**SCOPE, "namespace": "other"}}},
            {"subject": CHILD},
            {"action": "delegation", "usage": {"kind": "unknown"}},
        ]:
            self.serve({**base, "items": [{**entry, **changes}]})
            with self.assertRaises(ProtocolError):
                await self.client.budgets.entries(task_id="child")

    async def test_malformed_or_rebound_ledger_fails_closed(self):
        base = {
            "scope": SCOPE,
            "subject": CHILD,
            "root": ROOT,
            "items": [row(4)],
            "next": None,
        }
        for changed in [
            {"scope": {**SCOPE, "namespace": "foreign"}},
            {"root": CHILD},
            {"subject": ROOT},
            {"items": [row(3), row(4)]},
            {"items": [row(4, reason="calls_exhausted")]},
            {"items": [row(4, usage={"kind": "unknown", "input_tokens": 0})]},
            {
                "items": [
                    row(
                        4,
                        outcome="denied",
                        reason="delegations_exhausted",
                        usage={"kind": "unknown"},
                    )
                ]
            },
            {"next": "5"},
        ]:
            self.serve({**base, **changed})
            with self.assertRaises(ProtocolError):
                await self.client.budgets.entries(task_id="child")
        with self.assertRaises(InputError):
            await self.client.budgets.entries(task_id="child", workflow_id="root")

    async def test_budget_exhaustion_is_definitive_and_never_retries(self):
        self.serve({"code": "run_budget_denied", "message": "deadline_exceeded"}, 409)
        with self.assertRaises(RunBudgetDenied) as error:
            await self.client.workflows.submit(
                **ARGS, budget=RunBudgetPolicy(max_calls=0)
            )
        self.assertEqual(error.exception.reason, RunBudgetDenial.DEADLINE_EXCEEDED)
        self.assertEqual(len(self.requests), 1)
        self.serve({"code": "run_budget_denied", "message": "cancelled"}, 409)
        with self.assertRaises(RunBudgetDenied) as cancelled:
            await self.client.workflows.submit(
                **ARGS, budget=RunBudgetPolicy(max_calls=1)
            )
        self.assertEqual(cancelled.exception.reason, RunBudgetDenial.CANCELLED)
        self.serve({"code": "run_budget_denied", "message": "unknown"}, 409)
        with self.assertRaises(ProtocolError):
            await self.client.budgets.inspect(workflow_id="root")
