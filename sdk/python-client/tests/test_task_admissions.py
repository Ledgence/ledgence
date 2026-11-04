"""Standalone admission is immutable input; deferral history is not retry use."""
import copy
import json
from pathlib import Path
import unittest
from unittest.mock import AsyncMock

import test_workflows

from ledgence.client import AsyncClient, InputError, ProtocolError, Scope, TaskAdmission
from ledgence.client.models import parse_status
from ledgence.client.tasks import _submission_reply
from ledgence.client.task_admissions import occurrence, snapshot
from support import response, status, submitted


class TaskAdmissionTests(unittest.TestCase):
    def prepare(self, **kwargs):
        client = AsyncClient("http://localhost:8080", tenant="tenant", namespace="tests")
        return client.tasks.prepare(program="hello", version="1.0.0", queue="queue",
                                    data={"resource": "user-owned"}, idempotency_key="same", **kwargs)

    def test_optional_binding_is_frozen_outside_application_data(self):
        plain = self.prepare()
        self.assertNotIn("admission", plain.to_dict()["input"])
        bound = self.prepare(admission=TaskAdmission("model.requests"))
        copied = bound.to_dict()
        copied["input"]["admission"]["resource"] = "changed"
        self.assertEqual(bound.to_dict()["input"]["admission"], {"resource": "model.requests"})
        self.assertEqual(bound.to_dict()["input"]["data"], {"resource": "user-owned"})
        self.assertEqual(_submission_reply(submitted(bound.to_dict()), bound), ("task", "run"))

    def test_response_cannot_drop_replace_or_inject_requirement(self):
        plain = self.prepare()
        bound = self.prepare(admission=TaskAdmission("model.requests"))
        for command, value in [(bound, None), (bound, {"resource": "other"}),
                               (plain, {"resource": "unexpected"})]:
            reply = submitted(command.to_dict())
            if value is None:
                reply["input"].pop("admission", None)
            else:
                reply["input"]["admission"] = value
            with self.subTest(value=value), self.assertRaises(ProtocolError):
                _submission_reply(reply, command)

    def test_strict_requirement_before_network(self):
        for value in ("", "with space", "a/b", "é", "x" * 129, None, 1, True):
            with self.subTest(value=value), self.assertRaises(InputError):
                TaskAdmission(value)
        with self.assertRaises(InputError):
            self.prepare(admission={"resource": "untyped"})

    def test_deferrals_preserve_history_without_exhausting_retry_count(self):
        raw = status("active", count=1002)
        raw.update(admission={"resource": "model.requests"}, admission_deferrals=1001)
        parsed = parse_status(raw, Scope("tenant", "tests"), "task")
        self.assertEqual(parsed.attempt_count, 1002)
        self.assertEqual(parsed.retry_attempts_used, 1)
        self.assertEqual(parsed.admission, TaskAdmission("model.requests"))
        for changes in ({"admission_deferrals": 1003}, {"admission_deferrals": -1},
                        {"admission_deferrals": True}, {"admission_deferrals": 1},
                        {"admission": None}, {"workflow_id": "workflow"},
                        {"attempt_count": 1 << 32}):
            bad = copy.deepcopy(raw)
            bad.update(changes)
            with self.subTest(changes=changes), self.assertRaises(ProtocolError):
                parse_status(bad, Scope("tenant", "tests"), "task")
        del raw["admission"]
        with self.assertRaises(ProtocolError):
            parse_status(raw, Scope("tenant", "tests"), "task")

    def test_rust_ledger_fixtures_and_foreign_source_rejection(self):
        path = Path(__file__).parent / "fixtures/task-admission-v1.json"
        records = json.loads(path.read_text())["snapshots"]
        for key, raw in records.items():
            with self.subTest(key=key):
                result = snapshot(raw, occurrence(raw["occurrence"]))
                self.assertEqual(result.to_dict(), raw)
        for key, changes in (("waiting", {"offer": records["offered"]["offer"]}),
                             ("finished", {"close": None}),
                             ("finished", {"close": {**records["finished"]["close"], "already_accepted": True}}),
                             ("deferred", {"owner": None, "close": None}),
                             ("released", {"close": records["finished"]["close"]})):
            with self.subTest(key=key), self.assertRaises(InputError):
                snapshot({**records[key], **changes}, occurrence(records[key]["occurrence"]))
        started = records["started"]
        for foreign in ("resource", "binding_digest", "start_deadline_at"):
            bad = copy.deepcopy(started)
            bad["source"][foreign] = {"resource": "foreign", "binding_digest": "sha256:" + "0" * 64,
                                      "start_deadline_at": bad["source"]["accepted_at"]}[foreign]
            with self.subTest(foreign=foreign), self.assertRaises(InputError):
                snapshot(bad, occurrence(started["occurrence"]))

        for expiry in (started["source"]["accepted_at"], started["source"]["start_deadline_at"] - 1):
            bad = copy.deepcopy(started)
            bad["offer"]["expires_at"] = expiry
            with self.subTest(expiry=expiry), self.assertRaises(InputError):
                snapshot(bad, occurrence(started["occurrence"]))

    def test_replayed_submission_checks_actual_retry_budget(self):
        command = self.prepare(admission=TaskAdmission("model.requests"))
        raw = submitted(command.to_dict())
        raw.update(attempt_count=1002, admission_deferrals=1001)
        self.assertEqual(_submission_reply(raw, command), ("task", "run"))
        raw["admission_deferrals"] = 1
        with self.assertRaises(ProtocolError):
            _submission_reply(raw, command)


class TaskReleaseTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflows.WorkflowClientTests.asyncSetUp

    async def test_release_binds_original_endpoint_source_and_inspected_revision(self):
        records = json.loads((Path(__file__).parent / "fixtures/task-admission-v1.json").read_text())["snapshots"]
        arguments = {"request_id": "release-1", "reason": "remote completion independently verified"}
        async with AsyncClient(self.url, tenant="acme", namespace="demo") as operator:
            self.callback = AsyncMock(return_value=response(records["unknown"]))
            inspected = await operator.task_admissions.inspect("task", generation=1)
            other = AsyncClient("http://127.0.0.1:1", tenant="acme", namespace="demo")
            with self.assertRaises(InputError):
                await other.task_admissions.release(inspected, **arguments)
            for replayed in (False, True):
                self.callback = AsyncMock(return_value=response({
                    "already_accepted": replayed, "snapshot": records["released"],
                }))
                receipt = await operator.task_admissions.release(inspected, **arguments)
                self.assertEqual(receipt.already_accepted, replayed)
                self.assertEqual(receipt.snapshot.source, inspected.source)
                self.assertEqual(json.loads(self.requests[-1][3]), {
                    "occurrence": inspected.occurrence.to_dict(), "source_id": inspected.source["source_id"],
                    "expected_revision": inspected.revision, **arguments,
                })


if __name__ == "__main__":
    unittest.main()
