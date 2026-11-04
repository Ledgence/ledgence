"""Public queue policies through a real loopback client/transport (MIT)."""

import json
from dataclasses import replace
import unittest
from unittest.mock import AsyncMock

from ledgence.client import (
    Forbidden,
    InputError,
    ProtocolError,
    QueueConcurrencyPolicy,
    QueueHoldReleaseKind,
    QueueHoldTimeout,
)
from support import SCOPE, response
from test_workflows import WorkflowClientTests as _WorkflowClientTests


def record(**updates):
    return {
        "scope": SCOPE,
        "queue": "payments",
        "revision": "1",
        "updated_at": 100,
        "policy": QueueConcurrencyPolicy(5).to_dict(),
        **updates,
    }


def hold(**updates):
    return {
        "scope": SCOPE,
        "queue": "payments",
        "worker_session_id": "worker",
        "consumer_id": 0,
        "task_id": "task",
        "attempt_id": "attempt",
        "acquired_at": 10,
        "policy_revision": "7",
        "revision": "9007199254740993",
        "state": "uncertain",
        "grace_ms": 10,
        "deadline": 30,
        "stop_requested_at": 50,
        "force_at": 60,
        "release_kind": None,
        "released_at": None,
        **updates,
    }


class QueueClientTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = _WorkflowClientTests.asyncSetUp

    def serve(self, data, status=200):
        self.callback = AsyncMock(
            side_effect=lambda req, body: response(data, code=status)
        )

    async def test_retry_preserves_command_and_lossless_precondition(self):
        policy = QueueConcurrencyPolicy(5, QueueHoldTimeout(12_000, 2_000))
        revision = 2**53 + 1
        for replay in (False, True):
            self.serve(
                {
                    "already_accepted": replay,
                    "record": record(
                        policy=policy.to_dict(), revision=str(revision + 1)
                    ),
                }
            )
            receipt = await self.client.queue_policies.put(
                "payments",
                policy=policy,
                reason="edit once",
                expected_revision=revision,
                request_id="update-7",
            )
            self.assertEqual(receipt.record.revision, revision + 1)
            self.assertEqual(receipt.already_accepted, replay)
        self.assertEqual(self.requests[0], self.requests[1])
        self.assertEqual(
            json.loads(self.requests[0][3])["expected_revision"], str(revision)
        )

    async def test_list_and_inspect_reject_foreign_and_invalid_totals(self):
        snapshot = {"record": record(), "occupied": "9", "uncertain": "2"}
        self.serve(snapshot)
        self.assertEqual((await self.client.queue_policies.get("payments")).occupied, 9)
        for invalid in [
            dict(snapshot, uncertain="10"),
            dict(snapshot, occupied="09"),
            dict(snapshot, record=record(queue="foreign")),
        ]:
            self.serve(invalid)
            with self.assertRaises(ProtocolError):
                await self.client.queue_policies.get("payments")
        self.serve({"items": [snapshot], "next": "payments"})
        with self.assertRaises(ProtocolError):
            await self.client.queue_policies.list(after="payments")

    async def test_release_binds_scope_source_and_frozen_schedule(self):
        self.serve({"items": [hold()], "next": None})
        client = self.client.queue_policies
        inspected = (await client.holds("payments")).items[0]
        for foreign in (
            replace(inspected, _base_url="http://localhost:9"),
            replace(inspected, state="running"),
        ):
            with self.assertRaises(InputError):
                await client.release(
                    foreign, request_id="once", reason="confirmed out of band"
                )
        self.assertEqual(len(self.requests), 1)
        data = hold(
            state="released",
            release_kind="operator",
            released_at=55,
            revision=str(inspected.revision + 1),
        )
        self.serve({"already_accepted": True, "hold": data})
        receipt = await client.release(
            inspected, request_id="once", reason="confirmed out of band"
        )
        self.assertEqual(receipt.hold.release_kind, QueueHoldReleaseKind.OPERATOR)
        self.assertTrue(receipt.already_accepted)
        self.assertEqual(
            json.loads(self.requests[-1][3])["expected_revision"], str(2**53 + 1)
        )
        for updates in (
            {"policy_revision": "6"},
            {"task_id": "other"},
            {"grace_ms": None, "force_at": None},
        ):
            self.serve({"already_accepted": True, "hold": dict(data, **updates)})
            with self.assertRaises(ProtocolError):
                await client.release(
                    inspected, request_id="once", reason="confirmed out of band"
                )

    async def test_force_requires_persisted_stop_and_elapsed_grace(self):
        client = self.client.queue_policies
        row = hold(state="released", release_kind="timeout", released_at=60)
        self.serve({"items": [row], "next": None})
        self.assertEqual(
            (await client.holds("payments", include_released=True))
            .items[0]
            .release_kind,
            QueueHoldReleaseKind.TIMEOUT,
        )
        for bad in (
            dict(row, released_at=59),
            dict(row, force_at=59),
            dict(row, stop_requested_at=None),
            row,
        ):
            self.serve({"items": [bad], "next": None})
            with self.assertRaises(ProtocolError):
                await client.holds("payments", include_released=bad is not row)
        self.serve({"code": "forbidden"}, 403)
        with self.assertRaises(Forbidden):
            await client.holds("payments")

    async def test_invalid_limit_or_grace_rejected_before_http(self):
        for maximum in (-1, True, 1_000_001):
            with self.assertRaises(InputError):
                QueueConcurrencyPolicy(maximum)
        with self.assertRaises(InputError):
            QueueHoldTimeout(30, 0)
        self.assertEqual(self.requests, [])


del _WorkflowClientTests
