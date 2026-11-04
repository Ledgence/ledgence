"""Public Python fairness policy and offer-history contracts over loopback HTTP (MIT)."""

import json
import unittest

import test_workflows
from ledgence.client import (
    InputError,
    ProtocolError,
    ResourceFairGroup,
    ResourceFairSharing,
    ResourcePolicy,
    TaskDecisionSubject,
    WorkflowDecisionSubject,
)
from support import SCOPE, response
from test_admissions import record, resource

FIFO = {
    "max_occupancy": 5,
    "enabled": True,
    "rate": None,
    "offer_timeout_ms": 30_000,
    "start_timeout_ms": 5_000,
}
SHARING = {
    "groups": [
        {"name": "support", "weight": 3, "queues": ["interactive"]},
        {"name": "batch", "weight": 1, "queues": []},
    ],
    "default_group": "batch",
}
MAX_U64 = (1 << 64) - 1


def policy():
    return ResourcePolicy(
        5,
        fair_sharing=ResourceFairSharing(
            (
                ResourceFairGroup("support", 3, ["interactive"]),
                ResourceFairGroup("batch", 1),
            ),
            "batch",
        ),
    )


def decision(sequence, *, kind="task", **changes):
    subject = {"kind": kind, "task_id": "task", "generation": 1}
    if kind in ("workflow", "provider"):
        subject = {
            "kind": kind,
            "workflow_id": "workflow",
            "origin_activation_id": "activation",
            "key": "call:0",
        }
    return {
        "scope": SCOPE,
        "resource": "model",
        "sequence": str(sequence),
        "policy_revision": str(MAX_U64),
        "group": "support",
        "weight": 3,
        "eligible_groups": 2,
        "cycle_round": 2,
        "reason": "weighted",
        "queue": "interactive",
        "subject": subject,
        "offer_generation": str(MAX_U64),
        "offered_at": 7,
        **changes,
    }


class ResourceFairnessTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflows.WorkflowClientTests.asyncSetUp

    def serve(self, value):
        async def handler(request, body):
            return response(value)

        self.callback = handler

    async def test_fifo_and_weighted_policy_exact_wire_bytes_and_replay(self):
        for configured, wire in (
            (ResourcePolicy(5), FIFO),
            (policy(), {**FIFO, "fair_sharing": SHARING}),
        ):
            command = {
                "scope": SCOPE,
                "name": "model",
                "policy": wire,
                "request_id": "configure",
                "reason": "share capacity",
                "expected_revision": None,
            }
            expected = json.dumps(command, separators=(",", ":")).encode()
            for replay in (False, True):
                self.serve(
                    {"resource": record(policy=wire), "already_accepted": replay}
                )
                receipt = await self.client.resources.put(
                    "model",
                    policy=configured,
                    request_id="configure",
                    reason="share capacity",
                )
                self.assertEqual(receipt.resource.policy, configured)
                self.assertEqual(receipt.already_accepted, replay)
                self.assertEqual(self.requests[-1][0:2], ("POST", "/v1/resources/put"))
                self.assertEqual(self.requests[-1][3], expected)

    async def test_policy_validation_precedes_http(self):
        base = ResourceFairGroup("support", 3, ("queue",))
        for make in (
            lambda: ResourceFairGroup("support", True),
            lambda: ResourceFairGroup("support", 1001),
            lambda: ResourceFairGroup("support", 0),
            lambda: ResourceFairGroup("support", 1, "abc"),
            lambda: ResourceFairGroup("support", 1, ("a", "a")),
            lambda: ResourceFairGroup("bad name", 1),
            lambda: ResourceFairSharing((), "support"),
            lambda: ResourceFairSharing((base,), "other"),
            lambda: ResourceFairSharing((base, base), "support"),
            lambda: ResourceFairSharing(
                (base, ResourceFairGroup("batch", 1, ["queue"])), "support"
            ),
            lambda: ResourceFairSharing(
                tuple(ResourceFairGroup(f"g{i}", 1) for i in range(17)), "g0"
            ),
            lambda: ResourceFairSharing(
                (
                    ResourceFairGroup("a", 1, tuple(map(str, range(64)))),
                    ResourceFairGroup("b", 1, ["extra"]),
                ),
                "a",
            ),
            lambda: ResourcePolicy(5, fair_sharing=SHARING),
        ):
            with self.subTest(factory=make), self.assertRaises(InputError):
                make()
        self.assertEqual(self.requests, [])

    async def test_malformed_policy_and_receipt_fail_closed(self):
        for bad in (
            None,
            {**SHARING, "unknown": 1},
            {**SHARING, "groups": []},
            {**SHARING, "default_group": "missing"},
        ):
            with self.subTest(policy=bad):
                self.serve(
                    resource(resource=record(policy={**FIFO, "fair_sharing": bad}))
                )
                with self.assertRaises(ProtocolError):
                    await self.client.resources.get("model")
        self.serve({"resource": record(policy=FIFO), "already_accepted": False})
        with self.assertRaises(ProtocolError):
            await self.client.resources.put(
                "model", policy=policy(), request_id="create", reason="test"
            )

    async def test_decisions_mixed_subjects_and_lossless_cursor(self):
        seq = MAX_U64 - 1
        self.serve(
            {
                "items": [
                    decision(seq),
                    decision(seq - 1, kind="workflow"),
                    decision(
                        seq - 2,
                        kind="provider",
                        reason="sole_eligible",
                        eligible_groups=1,
                    ),
                ],
                "next": str(seq - 2),
            }
        )
        page = await self.client.resources.decisions("model", before=MAX_U64, limit=3)
        self.assertEqual(page.next, seq - 2)
        self.assertEqual(
            [item.sequence for item in page.items], [seq, seq - 1, seq - 2]
        )
        self.assertTrue(
            all(
                item.offer_generation == item.policy_revision == MAX_U64
                for item in page.items
            )
        )
        self.assertEqual(page.items[0].subject, TaskDecisionSubject("task", 1))
        for item, kind in zip(page.items[1:], ("workflow", "provider"), strict=True):
            self.assertEqual(
                item.subject,
                WorkflowDecisionSubject(kind, "workflow", "activation", "call:0"),
            )
        self.assertEqual(self.requests[-1][:2], ("POST", "/v1/resources/decisions"))
        self.assertEqual(
            json.loads(self.requests[-1][3]),
            {"scope": SCOPE, "name": "model", "before": str(MAX_U64), "limit": 3},
        )
        self.serve({"items": [], "next": None})
        following = await self.client.resources.decisions("model", before=page.next)
        self.assertEqual(json.loads(self.requests[-1][3])["before"], str(seq - 2))
        self.assertEqual(following.items, ())
        self.assertIsNone(following.next)

    async def test_decision_request_validation_precedes_http(self):
        for kwargs in (
            {"before": 0},
            {"before": MAX_U64 + 1},
            {"before": True},
            {"before": "3"},
            {"limit": 101},
            {"limit": 0},
        ):
            with self.subTest(args=kwargs), self.assertRaises(InputError):
                await self.client.resources.decisions("model", **kwargs)
        self.assertEqual(self.requests, [])

    async def test_decision_page_rejects_malformed_or_foreign_data(self):
        task = {"kind": "task", "task_id": "task", "generation": 1}
        invalid_rows = [
            decision(2, scope={**SCOPE, "namespace": "other"}),
            decision(2, resource="foreign"),
            decision(2, reason="new_reason"),
            decision(2, eligible_groups=1),
            decision(2, cycle_round=0),
            decision(2, cycle_round=4),
            decision("01"),
            decision(str(MAX_U64 + 1)),
            decision(2, offer_generation=3),
            decision(2, policy_revision="0"),
            decision(2, unknown="field"),
            decision(2, subject={**task, "args": {"secret": 1}}),
            decision(2, subject={**task, "generation": 1 << 31}),
            decision(
                2,
                subject={
                    "kind": "workflow",
                    "workflow_id": "workflow",
                    "key": "call:0",
                },
            ),
            decision(
                2,
                subject={
                    "kind": "unknown",
                    "workflow_id": "w",
                    "origin_activation_id": "a",
                    "key": "k",
                },
            ),
        ]
        pages = [{"items": [row], "next": None} for row in invalid_rows]
        pages += [
            {"items": [decision(2), decision(2)], "next": None},
            {"items": [decision(1), decision(2)], "next": None},
            {"items": [decision(3)], "next": None},  # before is exclusive.
            {"items": [decision(2), decision(1)], "next": "2"},
            {"items": [], "next": "2"},
            {"items": [decision(2)], "next": 2},
            {"items": [], "next": None, "extra": True},
            {"items": [decision(2), decision(1), decision(1)], "next": None},
        ]
        for page in pages:
            with self.subTest(page=page):
                self.serve(page)
                with self.assertRaises(ProtocolError):
                    await self.client.resources.decisions("model", before=3, limit=2)
