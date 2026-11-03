"""Verify which synchronous provider responses the refund reference trusts (MIT)."""
import http.server
import json
import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk/python"))
from ledgence.worker.workflow import EffectOutcomeUnknown
from workflow_acceptance.refund_program import issue_refund, lookup_refund


class RefundResponseTests(unittest.TestCase):
    def setUp(self):
        owner = self
        self.key = "refund-test"
        self.receipt = {"effect_key": self.key, "arguments": {"amount": 50, "currency": "USD"},
                        "refund_id": "r-1"}
        self.requests = []
        self.status = 200
        self.body = json.dumps(self.receipt).encode()
        self.truncate = False

        class Handler(http.server.BaseHTTPRequestHandler):
            def respond(self):
                owner.requests.append((self.command, self.path))
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                if owner.status is None:  # Drop the connection before response headers.
                    return
                self.send_response(owner.status)
                self.send_header("Content-Length", str(len(owner.body)))
                self.send_header("Location", "/elsewhere")
                self.end_headers()
                self.wfile.write(owner.body[:-1] if owner.truncate else owner.body)

            do_POST = do_GET = respond

            def log_message(self, *_):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.02})
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def execute(self, lookup):
        operation = lookup_refund if lookup else issue_refund
        return operation(provider=f"http://127.0.0.1:{self.server.server_port}",
                         effect_key=self.key, amount=50)

    def test_only_final_status_with_receipt_confirms(self):
        for lookup, status in ((False, 200), (False, 201), (True, 200)):
            with self.subTest(lookup=lookup, status=status):
                self.status = status
                self.assertEqual(self.execute(lookup), {"state": "confirmed", "receipt": self.receipt})
        for lookup, status in ((True, 201), (False, 202), (True, 202), (False, 307), (True, 302)):
            with self.subTest(lookup=lookup, status=status):
                self.status = status
                before = len(self.requests)
                with self.assertRaises(EffectOutcomeUnknown):
                    self.execute(lookup)
                self.assertEqual(len(self.requests), before + 1)
                self.assertNotEqual(self.requests[-1][1], "/elsewhere")

    def test_invalid_receipts_stay_unknown(self):
        for value in (b"invalid", b"null", json.dumps({**self.receipt, "effect_key": "different"}).encode(),
                      json.dumps({**self.receipt, "arguments": {"amount": 50.0, "currency": "USD"}}).encode(),
                      json.dumps({**self.receipt, "refund_id": ""}).encode()):
            with self.subTest(value=value):
                self.body = value
                with self.assertRaises(EffectOutcomeUnknown):
                    self.execute(True)

    def test_duplicate_and_non_json_fields_cannot_confirm_a_refund(self):
        valid = json.dumps(self.receipt)
        for body in (
            valid.replace('"effect_key":', '"effect_key": "other", "effect_key":'),
            valid.replace('"arguments":', '"arguments": {}, "arguments":'),
            valid.replace('"amount":', '"amount": 1, "amount":'),
            valid.replace('"refund_id":', '"unexpected": NaN, "refund_id":'),
        ):
            with self.subTest(body=body):
                self.body = body.encode()
                with self.assertRaises(EffectOutcomeUnknown):
                    self.execute(True)

    def test_incomplete_or_missing_replies_stay_unknown(self):
        self.truncate = True
        with self.assertRaises(EffectOutcomeUnknown):
            self.execute(False)
        self.status = None
        with self.assertRaises(EffectOutcomeUnknown):
            self.execute(False)


if __name__ == "__main__":
    unittest.main()
