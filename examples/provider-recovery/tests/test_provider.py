"""Exercise independent provider accounting and real artifacts, without Ledgence (MIT)."""

import hashlib
import http.client
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from provider import ExportProvider


def exchange(provider, method, path="/exports", *, key="key-1", body=None):
    origin = urlsplit(provider.url)
    connection = http.client.HTTPConnection(origin.hostname, origin.port, timeout=3)
    try:
        connection.request(
            method,
            path,
            body=body,
            headers={
                "X-Provider-Account": "demo",
                "Idempotency-Key": key,
                "Content-Type": "application/json",
            },
        )
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def completed(provider):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        status, body = exchange(provider, "GET", "/exports?key=key-1")
        value = json.loads(body)
        if status == 200 and value["status"] == "succeeded":
            return value
        time.sleep(0.01)
    raise AssertionError("provider did not complete its export")


class ProviderTests(unittest.TestCase):
    def test_real_csv_and_provider_census_survive_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            with ExportProvider(directory, completion_delay=0.03) as provider:
                status, _ = exchange(provider, "POST", body=b'{"dataset":"customers"}')
                self.assertEqual(status, 202)
                value = completed(provider)
                status, artifact = exchange(
                    provider, "GET", "/artifacts/" + value["result"]["artifact"]
                )
                self.assertEqual(status, 200)
                self.assertEqual(
                    hashlib.sha256(artifact).hexdigest(), value["result"]["sha256"]
                )
                self.assertIn(b"customers,2,20", artifact)
                before = provider.census()
            with ExportProvider(directory) as provider:
                after = provider.census()
                self.assertEqual(before, after)
                self.assertEqual(after["mutation_http_requests"], 1)
                self.assertEqual(after["committed_effects"], 1)
                self.assertEqual(after["artifact_gets"], 1)
                self.assertEqual(after["completed_effects"], 1)

    def test_lost_post_reply_and_temporarily_hidden_get_do_not_erase_committed_effect(
        self,
    ):
        with (
            tempfile.TemporaryDirectory() as directory,
            ExportProvider(directory) as provider,
        ):
            provider.set_fault("key-1", post_response="drop", hidden_gets=1)
            with self.assertRaises(http.client.RemoteDisconnected):
                exchange(provider, "POST", body=b'{"dataset":"customers"}')
            self.assertEqual(exchange(provider, "GET", "/exports?key=key-1")[0], 404)
            completed(provider)
            census = provider.census()
            self.assertEqual(census["mutation_http_requests"], 1)
            self.assertEqual(census["committed_effects"], 1)
            self.assertGreaterEqual(census["status_gets"], 2)

    def test_expired_provider_key_really_allows_duplicate_if_caller_repeats_post(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            ExportProvider(directory) as provider,
        ):
            provider.set_fault("key-1", key_lifetime=1)
            body = b'{"dataset":"customers"}'
            exchange(provider, "POST", body=body)
            exchange(provider, "POST", body=body)
            self.assertEqual(provider.census()["committed_effects"], 1)
            provider.advance_clock(2)
            exchange(provider, "POST", body=body)
            self.assertEqual(provider.census()["committed_effects"], 2)
            self.assertEqual(provider.census()["mutation_http_requests"], 3)

    def test_invalid_requests_are_counted_without_committed_effects(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            ExportProvider(directory) as provider,
        ):
            self.assertEqual(exchange(provider, "POST", body=b"not-json")[0], 400)
            census = provider.census()
            self.assertEqual(census["mutation_http_requests"], 1)
            self.assertEqual(census["committed_effects"], 0)


if __name__ == "__main__":
    unittest.main()
