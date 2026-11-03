"""Reference adapter classifications are conservative and issue one HTTP call (MIT)."""

import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import program
from provider import ExportProvider


class AdapterTests(unittest.TestCase):
    def request(self, provider):
        return SimpleNamespace(
            arguments={"dataset": "customers"},
            target={
                "provider": "example",
                "environment": "local",
                "account": "demo",
                "api_variant": "v1",
                "operation": "export",
                "endpoint_identity": provider.url,
                "dimensions": {},
            },
            binding={"deduplication": {"key": "test"}},
            provider_reference=None,
        )

    def test_nonterminal_and_ambiguous_http_never_claim_success(self):
        for mode in ("drop", "500", "redirect"):
            with (
                self.subTest(mode=mode),
                tempfile.TemporaryDirectory() as directory,
                ExportProvider(directory) as provider,
            ):
                provider.set_fault("test", post_response=mode)
                report = program.start_export(self.request(provider)).to_dict()
                self.assertEqual(report["knowledge"]["kind"], "unknown")
                self.assertEqual(provider.census()["mutation_http_requests"], 1)
                self.assertEqual(provider.census()["committed_effects"], 1)

    def test_pending_and_missing_lookup_are_distinct_without_post_retries(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            ExportProvider(directory, completion_delay=10) as provider,
        ):
            request = self.request(provider)
            self.assertEqual(
                program.start_export(request).to_dict()["knowledge"],
                {"kind": "pending"},
            )
            provider.set_fault("test", hidden_gets=1)
            self.assertEqual(
                program.inspect_export(request).to_dict()["knowledge"]["kind"],
                "unknown",
            )
            self.assertEqual(provider.census()["mutation_http_requests"], 1)
            self.assertEqual(provider.census()["status_gets"], 1)

    def test_explicit_null_is_success_and_unrecognized_states_are_unknown(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            ExportProvider(directory) as provider,
        ):
            request = self.request(provider)
            self.assertEqual(
                program.classified(
                    request, {"id": "export", "status": "succeeded", "result": None}
                ).to_dict()["knowledge"],
                {"kind": "succeeded", "result": None},
            )
            self.assertEqual(
                program.classified(
                    request, {"id": "export", "status": "mystery"}
                ).to_dict()["knowledge"]["kind"],
                "unknown",
            )


if __name__ == "__main__":
    unittest.main()
