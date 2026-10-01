"""Companion input and artifact behavior, independent of provider availability (MIT)."""
import copy
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
from change_review.candidate import BASE_SOURCE, make_candidate
from change_review.inputs import submission

spec = importlib.util.spec_from_file_location("change_review_client", HERE / "client.py")
client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(client)


class ClientTests(unittest.TestCase):
    def test_normalization_is_shared_and_does_not_mutate_caller(self):
        value = {"change_id": "shipping-100"}
        normalized = submission(value)
        self.assertEqual(normalized["model"], "gpt-6-luna")
        self.assertEqual(value, {"change_id": "shipping-100"})
        for invalid in (None, {}, {"change_id": ""}, {"change_id": "a b"},
                        {"change_id": "x", "model": False}, {"change_id": "x", "extra": 1}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                submission(invalid)

    def test_export_keeps_exact_candidate_and_never_overwrites_a_result(self):
        execution = {"provider": "codex", "model": "gpt-6-luna", "cli_version": "0.0.0-fixture",
                     "thread_id": "offline-test", "cli_invocations": 1,
                     "usage": {"input_tokens": 0, "cached_input_tokens": 0,
                               "output_tokens": 0, "reasoning_output_tokens": None}}
        candidate = make_candidate("shipping-100", BASE_SOURCE.replace("> 10000", ">= 10000"),
                                   "Include the threshold.", execution)
        bundle = {"candidate": candidate, "status": "ready_for_review",
                  "pull_request": {"title": "Shipping fix", "body": "Verified evidence.", "url": None}}
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "bundle"
            result = client.export_bundle(bundle, output)
            self.assertEqual(result["candidate_sha256"], candidate["sha256"])
            self.assertEqual((output / "shipping.py").read_text(), candidate["source"])
            self.assertEqual((output / "change.patch").read_text(), candidate["patch"])
            self.assertEqual(json.loads((output / "review.json").read_text()), bundle)
            self.assertIn("Verified evidence.", (output / "pull-request.md").read_text())
            with self.assertRaises(FileExistsError):
                client.export_bundle(bundle, output)
            tampered = copy.deepcopy(bundle)
            tampered["candidate"]["sha256"] = "0" * 64
            with self.assertRaises(ValueError):
                client.export_bundle(tampered, Path(temporary) / "bad")
            self.assertFalse((Path(temporary) / "bad").exists())


if __name__ == "__main__":
    unittest.main()
