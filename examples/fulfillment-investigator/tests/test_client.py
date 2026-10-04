"""Decision persistence and binding at the companion's network boundary (MIT)."""
from dataclasses import dataclass
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import client
from fulfillment.config import APPROVAL_KEY


class Command:
    def __init__(self, body): self.body = body
    def to_dict(self): return json.loads(json.dumps(self.body))


class View:
    def __init__(self, digest):
        self.action = SimpleNamespace(arguments={"candidate_ref": {"sha256": digest}})
    def to_dict(self): return {"status": "approved"}


@dataclass
class Receipt:
    approval: View
    already_accepted: bool


class Handle:
    def __init__(self, args):
        self.args, self.sent, self.loaded = args, [], 0
    async def approval(self, key):
        assert key == APPROVAL_KEY
        self.loaded += 1
        return View("a" * 64)
    def prepare_approval_decision(self, approval, **fields):
        return Command({"workflow_id": self.args.workflow, "key": APPROVAL_KEY,
                        "action": {"arguments": approval.action.arguments}, **fields})
    def restore_approval_decision(self, body):
        if body["workflow_id"] != self.args.workflow:
            raise ValueError("wrong workflow")
        return Command(body)
    async def decide_approval(self, command):
        # The exact action must already survive client process failure.
        assert json.loads(self.args.file.read_text()) == command.to_dict()
        self.sent.append(command.to_dict())
        return Receipt(View("a" * 64), len(self.sent) > 1)


class ClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.args = SimpleNamespace(file=Path(self.directory.name) / "decision.json", workflow="wf-1",
            decision="approve", decision_id="review-1", reviewer="operator", reason=None, candidate_sha256="a" * 64)

    async def test_persist_before_send_and_retry_frozen_command_without_new_read(self):
        handle = Handle(self.args)
        first = await client.decide(handle, self.args)
        second = await client.decide(handle, self.args)
        self.assertFalse(first["already_accepted"])
        self.assertTrue(second["already_accepted"])
        self.assertEqual(handle.loaded, 1)
        self.assertEqual(handle.sent[0], handle.sent[1])
        json.dumps(second)  # Private encoded dataclass fields must not escape.

    async def test_wrong_candidate_or_changed_retry_never_sent(self):
        handle = Handle(self.args)
        self.args.candidate_sha256 = "b" * 64
        with self.assertRaisesRegex(ValueError, "reviewed candidate"):
            await client.decide(handle, self.args)
        self.assertFalse(self.args.file.exists())
        self.args.candidate_sha256 = "a" * 64
        await client.decide(handle, self.args)
        self.args.decision = "reject"
        with self.assertRaisesRegex(ValueError, "saved decision differs"):
            await client.decide(handle, self.args)
        self.assertEqual(len(handle.sent), 1)
        self.assertEqual(json.loads(self.args.file.read_text())["decision"], "approve")

    def test_source_signal_is_replayable_and_bound_to_workflow(self):
        expected = client.source_event("wf-1", "warehouse:1")
        self.assertEqual(client.source_event("wf-1", "warehouse:1"), expected)
        self.assertEqual(expected["data"], {"workflow_id": "wf-1", "source": "warehouse", "revision": "corrected"})
        with self.assertRaises(ValueError): client.source_event("wf-1", "bad\nidentity")


if __name__ == "__main__": unittest.main()
