"""Exercise physical calls separately from provider business knowledge (MIT)."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


program = load("admitted_export_example", ROOT / "program.py")
ExportProvider = load("admitted_export_service", ROOT.parent / "provider-recovery/provider.py").ExportProvider


class AdapterTests(unittest.TestCase):
    def request(self, provider):
        return SimpleNamespace(
            arguments={"dataset": "customers"},
            target={"provider": "example", "environment": "local", "account": "demo",
                    "api_variant": "v1", "operation": "export", "dimensions": {},
                    "endpoint_identity": provider.url},
            binding={"deduplication": {"key": "test"}},
        )

    def test_pending_business_has_a_completed_call(self):
        with tempfile.TemporaryDirectory() as directory, ExportProvider(directory, completion_delay=10) as provider:
            outcome = program.start_export(self.request(provider)).to_dict()
            self.assertEqual(outcome["call_completion"], "completed")
            self.assertEqual(outcome["report"]["knowledge"], {"kind": "pending"})
            self.assertEqual(provider.census()["mutation_http_requests"], 1)
            self.assertEqual(provider.census()["committed_effects"], 1)
            self.assertEqual(provider.census()["completed_effects"], 0)

    def test_lost_response_never_claims_call_completion_or_retries(self):
        with tempfile.TemporaryDirectory() as directory, ExportProvider(directory) as provider:
            provider.set_fault("test", post_response="drop")
            outcome = program.start_export(self.request(provider)).to_dict()
            self.assertEqual(outcome["call_completion"], "unknown")
            self.assertEqual(outcome["report"]["knowledge"]["kind"], "unknown")
            self.assertEqual(provider.census()["mutation_http_requests"], 1)
            self.assertEqual(provider.census()["committed_effects"], 1)

    def test_completed_error_and_redirect_responses_do_not_prove_business_failure(self):
        for mode in ("500", "redirect"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory, ExportProvider(directory) as provider:
                provider.set_fault("test", post_response=mode)
                outcome = program.start_export(self.request(provider)).to_dict()
                self.assertEqual(outcome["call_completion"], "completed")
                self.assertEqual(outcome["report"]["knowledge"]["kind"], "unknown")
                self.assertEqual(provider.census()["mutation_http_requests"], 1)
                self.assertEqual(provider.census()["committed_effects"], 1)

    def test_probe_is_read_only_and_reports_no_original_call_completion(self):
        with tempfile.TemporaryDirectory() as directory, ExportProvider(directory, completion_delay=10) as provider:
            request = self.request(provider)
            program.start_export(request)
            report = program.inspect_export(request).to_dict()
            self.assertEqual(report["knowledge"], {"kind": "pending"})
            self.assertNotIn("call_completion", report)
            self.assertEqual(provider.census()["mutation_http_requests"], 1)
            self.assertEqual(provider.census()["status_gets"], 1)

    def test_null_success_and_malformed_business_evidence_are_distinct(self):
        with tempfile.TemporaryDirectory() as directory, ExportProvider(directory) as provider:
            target = self.request(provider).target
            success = program.classify(target, {"id": "export", "status": "succeeded", "result": None})
            self.assertEqual(success.to_dict()["knowledge"], {"kind": "succeeded", "result": None})
            for value in (None, {}, {"id": "export", "status": "failed", "failure": {}},
                          {"id": "export", "status": "cancelled"},
                          {"id": "export", "status": "succeeded"}):
                with self.subTest(value=value):
                    self.assertEqual(program.classify(target, value).to_dict()["knowledge"]["kind"], "unknown")

    def test_invalid_origin_fails_before_any_provider_request(self):
        with tempfile.TemporaryDirectory() as directory, ExportProvider(directory) as provider:
            request = self.request(provider)
            request.target["endpoint_identity"] += "/another-path"
            with self.assertRaises(ValueError):
                program.start_export(request)
            self.assertEqual(provider.census()["mutation_http_requests"], 0)


if __name__ == "__main__":
    unittest.main()
