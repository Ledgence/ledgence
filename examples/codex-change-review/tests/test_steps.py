"""Offline tests for immutable candidates and independent acceptance (MIT)."""
import copy
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from change_review import candidate, steps
from change_review.config import DEFAULT_MODEL, MAX_CANDIDATE_BYTES
from change_review.processes import ProcessError


def execution():
    return {"provider": "codex", "model": DEFAULT_MODEL, "cli_version": "0.158.0-alpha.2.1",
            "thread_id": "thread-fixture", "cli_invocations": 1,
            "usage": {"input_tokens": 20, "cached_input_tokens": 0, "output_tokens": 10,
                      "reasoning_output_tokens": None}}


def proposed(*, fixed=True):
    source = (candidate.BASE_SOURCE.replace("item_count // 100 + 1", "(item_count + 99) // 100")
              if fixed else candidate.BASE_SOURCE)
    return candidate.make_candidate("document-pages", source, "Avoid empty pages in document search.", execution())


def reviewed(value, *, approve=True):
    return {"candidate_sha256": value["sha256"], "verdict": "approve" if approve else "request_changes",
            "summary": "The boundary is correct." if approve else "An exact page boundary creates an empty page.",
            "findings": [] if approve else [{"severity": "high", "line": 9, "message": "Do not add an empty page for exactly 100 results."}],
            "execution": execution(), "started_at_ms": 100, "finished_at_ms": 101, "pid": 1}


class CandidateTests(unittest.TestCase):
    def test_source_digest_and_canonical_diff_are_bound_and_copied(self):
        value = proposed()
        copy_value = candidate.validate_candidate(value)
        self.assertEqual(value["base_sha256"], candidate.BASE_SHA256)
        self.assertIn("-    return item_count // 100 + 1", value["patch"])
        self.assertIn("+    return (item_count + 99) // 100", value["patch"])
        copy_value["execution"]["usage"]["input_tokens"] = 99
        self.assertEqual(value["execution"]["usage"]["input_tokens"], 20)

    def test_malformed_binding_execution_and_byte_limits_are_rejected(self):
        value = proposed()
        mutations = ({"sha256": "0" * 64}, {"base_sha256": "0" * 64}, {"patch": "unrelated"},
                     {"source": "bad source\n"}, {"summary": "x" * 1025}, {"extra": True},
                     {"change_id": "not an identifier"}, {"source": "\ud800"},
                     {"summary": "x" * MAX_CANDIDATE_BYTES})
        for change in mutations:
            with self.subTest(change=list(change)), self.assertRaises(ValueError):
                candidate.validate_candidate(dict(value, **change))
        for field, bad in (("cli_invocations", True), ("provider", "other"), ("thread_id", "")):
            altered = copy.deepcopy(value)
            altered["execution"][field] = bad
            with self.assertRaises(ValueError):
                candidate.validate_candidate(altered)
        altered = copy.deepcopy(value)
        altered["execution"]["usage"]["output_tokens"] = True
        with self.assertRaises(ValueError):
            candidate.validate_candidate(altered)


class StepsTests(unittest.IsolatedAsyncioTestCase):
    async def test_implementation_canonicalizes_source_from_one_schema_checked_turn(self):
        value = proposed()
        response = {"output": {"source": value["source"], "summary": value["summary"]}, "execution": execution()}
        with patch.object(steps, "run_codex", AsyncMock(return_value=response)) as runner:
            result = await steps.implement_task({"data": {"change_id": "document-pages"}})
        self.assertEqual(result, value)
        self.assertEqual(runner.await_count, 1)
        self.assertIn("PHASE: IMPLEMENT", runner.call_args.args[0])
        self.assertIn(candidate.BASE_SOURCE, runner.call_args.args[0])
        self.assertEqual(runner.call_args.kwargs["model"], DEFAULT_MODEL)
        for bad in ({"source": value["source"]}, {"source": value["source"], "summary": "x", "extra": 1}):
            with patch.object(steps, "run_codex", AsyncMock(return_value={"output": bad, "execution": execution()})):
                with self.assertRaises(ValueError):
                    await steps.implement_task({"data": {"change_id": "document-pages"}})

    async def test_acceptance_detects_empty_pages_and_validates_all_input_types(self):
        for fixed in (False, True):
            value = proposed(fixed=fixed)
            report = await steps.run_tests(candidate=value)
            self.assertEqual(report["candidate_sha256"], value["sha256"])
            self.assertEqual(report["passed"], fixed)
            self.assertEqual(report["total"], 6)
            self.assertEqual(report["failures"], 0 if fixed else 2)
            self.assertEqual(report["errors"], 0)
            self.assertIn("test_exact_page_boundaries", report["output"])
        incomplete = candidate.make_candidate("bad-inputs", "def page_count(item_count):\n    return (item_count + 99) // 100\n", "Missing input validation.", execution())
        report = await steps.run_tests(candidate=incomplete)
        self.assertFalse(report["passed"])
        self.assertGreaterEqual(report["failures"] + report["errors"], 3)

    async def test_syntax_and_non_integer_return_are_test_data(self):
        source = "def page_count(item_count)\n    return 0\n"
        report = await steps.run_tests(candidate=candidate.make_candidate("syntax", source, "Malformed source.", execution()))
        self.assertFalse(report["passed"])
        self.assertEqual(report["errors"], 6)
        value = proposed()
        source = value["source"].replace("return (item_count + 99) // 100", "return False")
        report = await steps.run_tests(candidate=candidate.make_candidate("boolean-output", source, "Wrong output type.", execution()))
        self.assertFalse(report["passed"])

    async def test_every_test_execution_uses_a_new_directory(self):
        real = steps.collect
        directories = []
        async def observe(*args, **kwargs):
            directories.append(kwargs["cwd"])
            return await real(*args, **kwargs)
        with patch.object(steps, "collect", observe):
            await steps.run_tests(candidate=proposed())
            await steps.run_tests(candidate=proposed())
        self.assertEqual(len(set(directories)), 2)
        self.assertTrue(all(not Path(directory).exists() for directory in directories))

    async def test_review_checks_same_digest_in_fresh_turn_and_validates_findings(self):
        value = proposed()
        response = {"output": {"verdict": "approve", "summary": "Correct boundary and validation.", "findings": []},
                    "execution": execution()}
        with patch.object(steps, "run_codex", AsyncMock(return_value=response)) as runner:
            review = await steps.review_candidate(candidate=value, model=DEFAULT_MODEL)
        self.assertEqual(review["candidate_sha256"], value["sha256"])
        self.assertEqual(review["verdict"], "approve")
        self.assertIn("PHASE: REVIEW", runner.call_args.args[0])
        self.assertIn(value["sha256"], runner.call_args.args[0])
        self.assertIn(value["source"], runner.call_args.args[0])
        self.assertEqual(review["pid"], steps.os.getpid())
        for update in ({"verdict": "request_changes"}, {"candidate_sha256": "0" * 64},
                       {"findings": [{"severity": "high", "line": 1000, "message": "bad"}]}):
            with self.subTest(update=update), self.assertRaises(ValueError):
                steps.validate_review(dict(review, **update), value)

    async def packet_data(self, *, fixed=True, approve=True):
        value = proposed(fixed=fixed)
        return {"workflow_id": "wf-document-pages", "candidate": value,
                "comparison": await steps.compare_candidate(candidate=value),
                "tests": await steps.run_tests(candidate=value), "review": reviewed(value, approve=approve),
                "note": {"candidate_sha256": value["sha256"], "title": "No more empty search pages",
                         "body": "Exactly 100 search results now fit on one page.",
                         "execution": execution(), "started_at_ms": 100, "finished_at_ms": 101, "pid": 1},
                "decision": None}

    def decision(self, data, outcome="approved"):
        return {"workflow_id": data["workflow_id"], "candidate_sha256": data["candidate"]["sha256"],
                "outcome": outcome, "event_id": None if outcome == "expired" else "human-decision:1"}

    async def test_comparison_measures_real_before_and_after_for_fixed_and_broken_code(self):
        for fixed in (False, True):
            value = proposed(fixed=fixed)
            report = await steps.compare_candidate(candidate=value)
            self.assertEqual(report["candidate_sha256"], value["sha256"])
            self.assertEqual([row["before"]["value"] for row in report["cases"]], [1, 2, 2])
            self.assertEqual([row["after"]["value"] for row in report["cases"]], [1, 1 if fixed else 2, 2])
            self.assertTrue(all(row["after"]["error"] is None for row in report["cases"]))
        sources = ("def page_count(item_count)\n    return 0\n",
                   "def page_count(item_count):\n    return False\n",
                   "def page_count(item_count):\n    raise ValueError('measured error')\n")
        for source in sources:
            value = candidate.make_candidate("bad-comparison", source, "Invalid candidate", execution())
            report = await steps.compare_candidate(candidate=value)
            self.assertTrue(all(row["after"]["value"] is None for row in report["cases"]))
            self.assertTrue(all(row["after"]["error"] for row in report["cases"]))
        self.assertIn("ValueError: measured error", report["cases"][1]["after"]["error"])

    async def test_comparison_rejects_malformed_protocol_and_retires_uncertain_process(self):
        for response in ((2, b"{}", b"private"), (0, b'{"cases":[]}', b"private")):
            with patch.object(steps, "collect", AsyncMock(return_value=response)):
                with self.assertRaises(SystemExit) as caught:
                    await steps.compare_candidate(candidate=proposed())
                self.assertNotIn("private", str(caught.exception))
        with patch.object(steps, "collect", AsyncMock(side_effect=ProcessError("timeout"))):
            with self.assertRaises(SystemExit):
                await steps.compare_candidate(candidate=proposed())

    async def test_note_is_a_bounded_fresh_turn_for_the_exact_candidate(self):
        value = proposed()
        response = {"output": {"title": "No more empty search pages", "body": "Exactly 100 search results fit on one page."},
                    "execution": execution()}
        with patch.object(steps, "run_codex", AsyncMock(return_value=response)) as runner:
            report = await steps.draft_note(candidate=value)
        self.assertEqual(report["candidate_sha256"], value["sha256"])
        self.assertIn("PHASE: NOTE", runner.call_args.args[0])
        self.assertIn("DRAFT", runner.call_args.args[0])
        self.assertIn(value["source"], runner.call_args.args[0])
        self.assertEqual(runner.await_count, 1)
        for change in ({"candidate_sha256": "0" * 64}, {"title": "x" * 161},
                       {"title": "two\nlines"}, {"body": "null\x00body"}, {"body": "x" * 2049}, {"extra": True}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                steps.validate_note(dict(report, **change), value)

    async def test_finalize_is_deterministic_and_binds_all_reports(self):
        data = await self.packet_data()
        bundle = await steps.finalize_task({"data": data})
        self.assertEqual(bundle, await steps.finalize_task({"data": data}))
        self.assertEqual(bundle["status"], "waiting_for_approval")
        self.assertIsNone(bundle["decision"])
        self.assertIsNone(bundle["pull_request"]["url"])
        self.assertIn(data["candidate"]["sha256"], bundle["pull_request"]["body"])
        self.assertEqual(steps.validate_bundle(bundle), bundle)
        for name in ("comparison", "tests", "review", "note"):
            bad = copy.deepcopy(data)
            bad[name]["candidate_sha256"] = "0" * 64
            with self.subTest(report=name), self.assertRaises(ValueError):
                steps.assemble_bundle(bad)
        for change in ({"status": "approved"}, {"change_id": "other"}, {"extra": True}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                steps.validate_bundle(dict(bundle, **change))
        # Workflow identity has no external expected value until an event binds it.
        rebound = dict(bundle, workflow_id="other")
        rebound["decision"] = self.decision(data)
        with self.assertRaises(ValueError):
            steps.validate_bundle(rebound)

    async def test_decision_binds_exact_candidate_and_workflow_and_cannot_override_failed_checks(self):
        data = await self.packet_data()
        for outcome in ("approved", "rejected", "expired"):
            decided = {**data, "decision": self.decision(data, outcome)}
            self.assertEqual(steps.assemble_bundle(decided)["status"], outcome)
        for update in ({"workflow_id": "wf-other"}, {"candidate_sha256": "0" * 64},
                       {"outcome": "other"}, {"event_id": None}, {"extra": True}):
            with self.subTest(update=update), self.assertRaises(ValueError):
                steps.assemble_bundle({**data, "decision": {**self.decision(data), **update}})
        failed = await self.packet_data(fixed=False)
        self.assertEqual(steps.assemble_bundle(failed)["status"], "needs_changes")
        with self.assertRaisesRegex(ValueError, "cannot override"):
            steps.assemble_bundle({**failed, "decision": self.decision(failed)})
        inconsistent = copy.deepcopy(data)
        inconsistent["comparison"]["cases"][1]["after"]["value"] = 2
        self.assertEqual(steps.assemble_bundle(inconsistent)["status"], "needs_changes")
        with self.assertRaisesRegex(ValueError, "cannot override"):
            steps.assemble_bundle({**inconsistent, "decision": self.decision(inconsistent)})

    async def test_report_structure_counters_and_expected_cases_cannot_be_changed(self):
        data = await self.packet_data()
        for mutate in (
                lambda value: value["comparison"]["cases"][1].update(expected_pages=2),
                lambda value: value["comparison"]["cases"][1]["after"].update(value=True),
                lambda value: value["comparison"]["cases"][1]["after"].update(error="failure"),
                lambda value: value["tests"].update(passed=False),
                lambda value: value.update(unexpected=True)):
            bad = copy.deepcopy(data)
            mutate(bad)
            with self.assertRaises(ValueError):
                steps.assemble_bundle(bad)

    async def test_only_human_approved_bundle_can_invoke_optional_publisher(self):
        publisher = AsyncMock(side_effect=lambda bundle, _: bundle)
        module = types.ModuleType("change_review.publishing")
        module.publish = publisher
        data = await self.packet_data()
        data["publication"] = {"repository": "example/sample"}
        with patch.dict(sys.modules, {"change_review.publishing": module}):
            self.assertEqual((await steps.finalize_task({"data": data}))["status"], "waiting_for_approval")
            publisher.assert_not_called()
            for outcome in ("rejected", "expired"):
                data["decision"] = self.decision(data, outcome)
                self.assertEqual((await steps.finalize_task({"data": data}))["status"], outcome)
                publisher.assert_not_called()
            data["decision"] = self.decision(data)
            self.assertEqual((await steps.finalize_task({"data": data}))["status"], "approved")
            self.assertEqual(publisher.await_count, 1)
            # A separate explicit publication target is required even after approval.
            del data["publication"]
            await steps.finalize_task({"data": data})
            self.assertEqual(publisher.await_count, 1)

    async def test_uncertain_codex_or_test_process_retires_session(self):
        with patch.object(steps, "run_codex", AsyncMock(side_effect=steps.CodexError("fixed failure"))):
            with self.assertRaises(SystemExit):
                await steps.implement_task({"data": {"change_id": "document-pages"}})
        with patch.object(steps, "collect", AsyncMock(side_effect=ProcessError("deadline exceeded"))):
            with self.assertRaises(SystemExit):
                await steps.run_tests(candidate=proposed())
        with patch.object(steps, "collect", AsyncMock(return_value=(2, b"", b"private diagnostic"))):
            with self.assertRaises(SystemExit) as caught:
                await steps.run_tests(candidate=proposed())
            self.assertNotIn("private diagnostic", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
