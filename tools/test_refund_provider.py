"""Qualify the synthetic destination independently of the worker (MIT)."""
import http.client
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from http_acceptance.harness import exchange
from workflow_acceptance.refund_provider import RefundProvider


class RefundProviderTests(unittest.TestCase):
    def setUp(self):
        directory = self.enterContext(tempfile.TemporaryDirectory())
        self.ledger = Path(directory) / "provider.jsonl"
        self.pool = self.enterContext(ThreadPoolExecutor(max_workers=1))
        self.request = {"effect_key": "refund-1", "arguments": {"amount": 100, "currency": "USD"}}

    def accept_with_lost_reply(self, cooperative):
        provider = self.enterContext(RefundProvider(self.ledger, cooperative=cooperative))
        first = self.pool.submit(exchange, provider.url, "POST", "/refunds", self.request)
        self.assertTrue(provider.accepted.wait(timeout=5))
        self.assertFalse(first.done())
        [original] = provider.census()["refunds"]
        # The destination must be observable before any reply reaches the client.
        status, _, body = exchange(provider.url, "GET", "/refunds?effect_key=refund-1")
        self.assertEqual(status, 200 if cooperative else 501)
        if cooperative:
            self.assertEqual(body, original)
        provider.drop_reply()
        with self.assertRaises(http.client.RemoteDisconnected):
            first.result(timeout=5)
        return provider, original

    def test_malformed_submission_is_counted_without_creating_a_refund(self):
        provider = self.enterContext(RefundProvider(self.ledger, cooperative=True))
        self.assertEqual(exchange(provider.url, "POST", "/refunds", b"malformed")[0], 400)
        self.assertFalse(provider.accepted.is_set())
        self.assertEqual(provider.census(), {"post_requests": 1, "lookup_requests": 0, "refunds": []})

    def test_replaying_a_key_checks_the_original_arguments(self):
        provider, original = self.accept_with_lost_reply(cooperative=True)
        status, _, retry = exchange(provider.url, "POST", "/refunds", self.request)
        self.assertEqual((status, retry), (200, original))
        changed = {**self.request, "arguments": {"amount": 50, "currency": "USD"}}
        self.assertEqual(exchange(provider.url, "POST", "/refunds", changed)[0], 409)
        self.assertEqual(provider.census(), {
            "post_requests": 3, "lookup_requests": 1, "refunds": [original]})

    def test_uncooperative_provider_creates_another_refund_on_resend(self):
        provider, original = self.accept_with_lost_reply(cooperative=False)
        status, _, retry = exchange(provider.url, "POST", "/refunds", self.request)
        self.assertEqual(status, 201)
        self.assertNotEqual(retry["refund_id"], original["refund_id"])
        self.assertEqual(provider.census(), {
            "post_requests": 2, "lookup_requests": 1, "refunds": [original, retry]})


if __name__ == "__main__":
    unittest.main()
