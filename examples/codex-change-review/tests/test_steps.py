"""Offline tests for immutable candidates and independent acceptance (MIT)."""
import copy
import json
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
    source = candidate.BASE_SOURCE.replace("total_cents > 10000", "total_cents >= 10000") if fixed else candidate.BASE_SOURCE
    return candidate.make_candidate("shipping-1", source, "Include the free-shipping threshold.", execution())


def reviewed(value, *, approve=True):
    return {"candidate_sha256": value["sha256"], "verdict": "approve" if approve else "request_changes",
            "summary": "The boundary is correct." if approve else "The exact threshold is incorrect.",
            "findings": [] if approve else [{"severity": "high", "line": 9, "message": "Include 10000 in free shipping."}],
            "execution": execution(), "started_at_ms": 100, "finished_at_ms": 101, "pid": 1}


class CandidateTests(unittest.TestCase):
    def test_source_digest_and_canonical_diff_are_bound_and_copied(self):
        value = proposed()
        copy_value = candidate.validate_candidate(value)
        self.assertEqual(value["base_sha256"], candidate.BASE_SHA256)
        self.assertIn("-    return 0 if total_cents > 10000", value["patch"])
        self.assertIn("+    return 0 if total_cents >= 10000", value["patch"])
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
            result = await steps.implement_task({"data": {"change_id": "shipping-1"}})
        self.assertEqual(result, value)
        self.assertEqual(runner.await_count, 1)
        self.assertIn("PHASE: IMPLEMENT", runner.call_args.args[0])
        self.assertIn(candidate.BASE_SOURCE, runner.call_args.args[0])
        self.assertEqual(runner.call_args.kwargs["model"], DEFAULT_MODEL)
        for bad in ({"source": value["source"]}, {"source": value["source"], "summary": "x", "extra": 1}):
            with patch.object(steps, "run_codex", AsyncMock(return_value={"output": bad, "execution": execution()})):
                with self.assertRaises(ValueError):
                    await steps.implement_task({"data": {"change_id": "shipping-1"}})

    async def test_acceptance_detects_threshold_bug_and_validates_all_input_types(self):
        for fixed in (False, True):
            value = proposed(fixed=fixed)
            report = await steps.run_tests(candidate=value)
            self.assertEqual(report["candidate_sha256"], value["sha256"])
            self.assertEqual(report["passed"], fixed)
            self.assertEqual(report["total"], 6)
            self.assertEqual(report["failures"], 0 if fixed else 1)
            self.assertEqual(report["errors"], 0)
            self.assertIn("test_exact_threshold", report["output"])
        incomplete = candidate.make_candidate("bad-inputs", "def shipping_cost(total_cents):\n    return 0 if total_cents >= 10000 else 500\n", "Missing input validation.", execution())
        report = await steps.run_tests(candidate=incomplete)
        self.assertFalse(report["passed"])
        self.assertGreaterEqual(report["failures"] + report["errors"], 3)

    async def test_syntax_and_non_integer_return_are_test_data(self):
        source = "def shipping_cost(total_cents)\n    return 0\n"
        report = await steps.run_tests(candidate=candidate.make_candidate("syntax", source, "Malformed source.", execution()))
        self.assertFalse(report["passed"])
        self.assertEqual(report["errors"], 6)
        value = proposed()
        source = value["source"].replace("return 0 if", "return False if")
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

    async def test_finalize_is_deterministic_and_binds_both_reports(self):
        value = proposed()
        tests = await steps.run_tests(candidate=value)
        data = {"candidate": value, "tests": tests, "review": reviewed(value)}
        bundle = await steps.finalize_task({"data": data})
        self.assertEqual(bundle, await steps.finalize_task({"data": data}))
        self.assertEqual(bundle["status"], "ready_for_review")
        self.assertIsNone(bundle["pull_request"]["url"])
        self.assertIn(value["sha256"], bundle["pull_request"]["body"])
        for name in ("tests", "review"):
            bad = copy.deepcopy(data)
            bad[name]["candidate_sha256"] = "0" * 64
            with self.assertRaises(ValueError):
                await steps.finalize_task({"data": bad})
        bad = copy.deepcopy(tests)
        bad["passed"] = False
        with self.assertRaises(ValueError):
            steps.validate_tests(bad, value)

    async def test_only_ready_bundle_can_invoke_optional_publisher(self):
        value = proposed()
        tests = await steps.run_tests(candidate=value)
        publisher = AsyncMock(side_effect=lambda bundle, _: dict(bundle, publication={"mock": True}))
        module = types.ModuleType("change_review.publishing")
        module.publish = publisher
        with patch.dict(sys.modules, {"change_review.publishing": module}):
            data = {"candidate": value, "tests": tests, "review": reviewed(value, approve=False), "publication": {"repository": "example/sample"}}
            self.assertEqual((await steps.finalize_task({"data": data}))["status"], "needs_changes")
            publisher.assert_not_called()
            data["review"] = reviewed(value)
            self.assertTrue((await steps.finalize_task({"data": data}))["publication"]["mock"])
            self.assertEqual(publisher.await_count, 1)
        bad = proposed(fixed=False)
        data = {"candidate": bad, "tests": await steps.run_tests(candidate=bad), "review": reviewed(bad)}
        self.assertEqual((await steps.finalize_task({"data": data}))["status"], "needs_changes")

    async def test_uncertain_codex_or_test_process_retires_session(self):
        with patch.object(steps, "run_codex", AsyncMock(side_effect=steps.CodexError("fixed failure"))):
            with self.assertRaises(SystemExit):
                await steps.implement_task({"data": {"change_id": "shipping-1"}})
        with patch.object(steps, "collect", AsyncMock(side_effect=ProcessError("deadline exceeded"))):
            with self.assertRaises(SystemExit):
                await steps.run_tests(candidate=proposed())
        with patch.object(steps, "collect", AsyncMock(return_value=(2, b"", b"private diagnostic"))):
            with self.assertRaises(SystemExit) as caught:
                await steps.run_tests(candidate=proposed())
            self.assertNotIn("private diagnostic", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
