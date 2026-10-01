"""Companion decisions and safe portable evidence, without provider calls (MIT)."""
import copy
from dataclasses import dataclass
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from change_review.candidate import BASE_SOURCE, make_candidate
from change_review.inputs import submission
from change_review import steps
from change_review.report import money, render_report

spec = importlib.util.spec_from_file_location("change_review_client", HERE / "client.py")
client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(client)


def execution():
    return {"provider": "codex", "model": "gpt-6-luna", "cli_version": "0.0.0-fixture",
            "thread_id": "offline-test", "cli_invocations": 1,
            "usage": {"input_tokens": 0, "cached_input_tokens": 0,
                      "output_tokens": 0, "reasoning_output_tokens": None}}


async def packet():
    candidate = make_candidate("shipping-100", BASE_SOURCE.replace("> 10000", ">= 10000"),
                               "Include the threshold.", execution())
    common = {"candidate_sha256": candidate["sha256"], "execution": execution(),
              "started_at_ms": 100, "finished_at_ms": 101, "pid": 1}
    return steps.assemble_bundle({"workflow_id": "wf-demo-1", "candidate": candidate,
        "tests": await steps.run_tests(candidate=candidate),
        "comparison": await steps.compare_candidate(candidate=candidate),
        "review": {**common, "verdict": "approve", "summary": "The exact boundary is correct.", "findings": []},
        "note": {**common, "title": "Free shipping at $100", "body": "Orders of $100 now qualify for free shipping."}})


class ClientTests(unittest.IsolatedAsyncioTestCase):
    def test_normalization_and_bounded_approval_timeout(self):
        value = {"change_id": "shipping-100"}
        normalized = submission(value)
        self.assertEqual(normalized["model"], "gpt-6-luna")
        self.assertEqual(normalized["approval_timeout_ms"], 3_600_000)
        self.assertEqual(value, {"change_id": "shipping-100"})
        for invalid in (None, {}, {"change_id": ""}, {"change_id": "a b"},
                        {"change_id": "x", "model": False}, {"change_id": "x", "extra": 1},
                        *({"change_id": "x", "approval_timeout_ms": bad} for bad in (True, -1, 86_400_001, "10"))):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                submission(invalid)
        self.assertEqual(submission({"change_id": "x", "approval_timeout_ms": 0})["approval_timeout_ms"], 0)

    async def test_export_keeps_exact_candidate_and_never_overwrites_a_result(self):
        bundle = await packet()
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "bundle"
            result = client.export_bundle(bundle, output)
            self.assertEqual(result["candidate_sha256"], bundle["candidate"]["sha256"])
            self.assertEqual(result["workflow_id"], "wf-demo-1")
            self.assertEqual((output / "shipping.py").read_text(), bundle["candidate"]["source"])
            self.assertEqual((output / "change.patch").read_text(), bundle["candidate"]["patch"])
            self.assertEqual(json.loads((output / "review.json").read_text()), bundle)
            self.assertEqual(Path(result["report"]), (output / "review.html").resolve())
            self.assertIn("Awaiting your decision", (output / "review.html").read_text())
            with self.assertRaises(FileExistsError):
                client.export_bundle(bundle, output)
            for name in ("candidate", "tests", "note", "comparison", "review"):
                tampered = copy.deepcopy(bundle)
                key = "sha256" if name == "candidate" else "candidate_sha256"
                tampered[name][key] = "0" * 64
                with self.subTest(name=name), self.assertRaises(ValueError):
                    client.export_bundle(tampered, Path(temporary) / "bad")
                self.assertFalse((Path(temporary) / "bad").exists())
            tampered = dict(bundle, status="approved")
            with self.assertRaises(ValueError):
                client.export_bundle(tampered, Path(temporary) / "bad")
            self.assertFalse((Path(temporary) / "bad").exists())

    async def test_report_escapes_model_text_and_labels_offline_and_pending_evidence(self):
        bundle = await packet()
        bundle["note"]["title"] = '<img src=x onerror="alert(1)">'
        bundle["note"]["body"] = '<script>alert("untrusted")</script>'
        bundle["review"]["summary"] = '<svg onload="alert(2)">'
        html = render_report(steps.assemble_bundle({key: bundle[key] for key in steps.BUNDLE_INPUTS}))
        self.assertNotIn("<script>", html)
        self.assertNotIn("<img", html)
        self.assertNotIn("<svg", html)
        self.assertIn("&lt;script&gt;", html)
        self.assertIn("Offline fixture · no provider call", html)
        self.assertIn("Draft wording", html)
        self.assertIn("Awaiting your decision", html)
        self.assertIn("$5.00", html)
        self.assertIn("$0.00", html)
        self.assertIn("default-src 'none'", html)

    async def test_report_preserves_failed_comparison_errors_and_mixed_execution_provenance(self):
        bundle = await packet()
        bundle["comparison"]["cases"][1]["after"] = {"value": None, "error": "ValueError: <untrusted>"}
        bundle["note"]["execution"]["cli_version"] = "0.158.0"
        bundle = steps.assemble_bundle({key: bundle[key] for key in steps.BUNDLE_INPUTS})
        html = render_report(bundle)
        self.assertIn("Needs changes", html)
        self.assertIn("the measured comparison", html)
        self.assertIn("ValueError: &lt;untrusted&gt;", html)
        self.assertIn("Mixed execution", html)
        self.assertNotIn("no provider call", html)
        self.assertNotIn("<untrusted>", html)

    def test_currency_keeps_exact_cents_for_large_failed_candidate_results(self):
        self.assertEqual(money(2**63 - 1), "$92,233,720,368,547,758.07")
        self.assertEqual(money(-101), "-$1.01")
        self.assertEqual(money(0), "$0.00")

    def test_decision_event_binds_workflow_and_exact_candidate(self):
        for approved in (True, False):
            event = client.decision_event("wf-demo", "a" * 64, approved, "shipping-100:decision:1")
            self.assertEqual(event["data"], {"workflow_id": "wf-demo", "candidate_sha256": "a" * 64,
                                             "approved": approved})
            self.assertEqual(event["id"], "shipping-100:decision:1")
        for bad in (None, "", "A" * 64, "a" * 63, "z" * 64):
            with self.assertRaises(ValueError):
                client.decision_event("wf-demo", bad, True, "event-1")
        with self.assertRaises(ValueError):
            client.decision_event("wf-demo", "a" * 64, 1, "event-1")

    async def test_review_uses_task_result_without_waiting_for_workflow_completion(self):
        bundle = await packet()
        task = SimpleNamespace(result=AsyncMock(return_value=bundle))
        connection = SimpleNamespace(tasks=SimpleNamespace(handle=Mock(return_value=task)),
                                     workflows=SimpleNamespace(handle=Mock()))
        manager = AsyncMock()
        manager.__aenter__.return_value = connection
        with tempfile.TemporaryDirectory() as temporary, patch("ledgence.client.AsyncClient", return_value=manager):
            args = client.parser().parse_args(["review", "--task", "task-review", "--output", temporary + "/report"])
            result = await client.execute(args)
        self.assertEqual(result["status"], "waiting_for_approval")
        connection.tasks.handle.assert_called_once_with("task-review")
        connection.workflows.handle.assert_not_called()

    async def test_approve_and_reject_send_stable_prepared_event_without_resubmission(self):
        @dataclass
        class Receipt:
            already_accepted: bool = False
        for name, approved in (("approve", True), ("reject", False)):
            workflow = SimpleNamespace(prepare_event=Mock(return_value="frozen-command"),
                                       send_event=AsyncMock(return_value=Receipt()))
            connection = SimpleNamespace(workflows=SimpleNamespace(handle=Mock(return_value=workflow), submit=Mock()))
            manager = AsyncMock()
            manager.__aenter__.return_value = connection
            args = client.parser().parse_args([name, "--workflow", "wf-demo", "--candidate-sha256", "a" * 64,
                                               "--event-id", "decision-1"])
            with patch("ledgence.client.AsyncClient", return_value=manager):
                await client.execute(args)
            event = workflow.prepare_event.call_args.kwargs["event"]
            self.assertEqual(event["data"]["approved"], approved)
            self.assertEqual(event["id"], "decision-1")
            workflow.send_event.assert_awaited_once_with("frozen-command")
            connection.workflows.submit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
