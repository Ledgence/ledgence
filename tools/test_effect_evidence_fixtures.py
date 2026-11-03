"""The fault fixture forwards credentials only after explicit local opt-in (MIT)."""
from concurrent.futures import ThreadPoolExecutor
import http.client
import http.server
import json
from pathlib import Path
import tempfile
import threading
import unittest
from urllib.parse import urlsplit

from effect_evidence.scenarios import RECOVERY_REPORT_FIELDS, public_recovery
from http_acceptance.harness import FaultProxy
from workflow_acceptance.refund_provider import RefundProvider


class CredentialProxyTests(unittest.TestCase):
    def test_forwarding_is_opt_in_and_never_retained(self):
        seen = []
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                seen.append((self.headers.get("Authorization"), self.headers.get("Cookie")))
                body = b'{"ready":true}'
                self.send_response(200)
                self.send_header("Content-Length",str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def log_message(self,*args): pass
        server = http.server.ThreadingHTTPServer(("127.0.0.1",0),Handler)
        thread = threading.Thread(target=server.serve_forever,daemon=True); thread.start()
        try:
            for authorized in (False,True):
                proxy = FaultProxy(f"http://127.0.0.1:{server.server_port}", forward_authorization=authorized)
                endpoint = urlsplit(proxy.url)
                connection = http.client.HTTPConnection(endpoint.hostname, endpoint.port, timeout=5)
                try:
                    connection.request("GET","/v1/access",headers={"Authorization":"Bearer fixture-secret", "Cookie":"never-forward"})
                    response = connection.getresponse(); self.assertEqual(response.status,200); response.read()
                    self.assertNotIn("fixture-secret",repr(proxy.records))
                    self.assertNotIn("never-forward",repr(proxy.records))
                finally:
                    connection.close(); proxy.close()
            self.assertEqual(seen,[(None,None),("Bearer fixture-secret",None)])
        finally:
            server.shutdown();server.server_close();thread.join(timeout=5)
        for invalid in (1,None,"yes"):
            with self.assertRaises(ValueError): FaultProxy("http://127.0.0.1",forward_authorization=invalid)

    def test_gate_report_excludes_all_private_commands_and_unreviewed_fields(self):
        source = {name: name for name in RECOVERY_REPORT_FIELDS}
        source.update(retained_claim_command={"owner":{"lease_id":"private-claim-lease"}},
                      retained_confirm_command={"owner":{"lease_id":"private-confirm-lease"}},
                      unexpected_future_field={"token":"not-reportable"})
        report = public_recovery(source)
        self.assertEqual(set(report),set(RECOVERY_REPORT_FIELDS))
        serialized = json.dumps(report)
        for forbidden in ("retained_claim_command","retained_confirm_command","lease_id","private-claim-lease",
                          "private-confirm-lease","unexpected_future_field","not-reportable"):
            self.assertNotIn(forbidden,serialized)

    def test_provider_census_counts_requests_even_when_idempotency_returns_existing_receipt(self):
        with tempfile.TemporaryDirectory(prefix="effect-provider-test-") as temporary:
            with RefundProvider(Path(temporary)/"provider.jsonl",cooperative=True) as provider:
                endpoint = urlsplit(provider.url)
                body = json.dumps({"effect_key":"one","arguments":{"amount":50,"currency":"USD"}})
                def post():
                    connection = http.client.HTTPConnection(endpoint.hostname,endpoint.port,timeout=5)
                    try:
                        connection.request("POST","/refunds",body,{"Content-Type":"application/json"})
                        response=connection.getresponse()
                        return response.status,json.loads(response.read())
                    finally: connection.close()
                with ThreadPoolExecutor(max_workers=1) as executor:
                    first=executor.submit(post)
                    self.assertTrue(provider.accepted.wait(5))
                    provider.drop_reply()
                    with self.assertRaises(http.client.RemoteDisconnected): first.result(timeout=5)
                status,receipt=post()
                self.assertEqual(status,200)
                census=provider.census()
                self.assertEqual(census["post_requests"],2)
                self.assertEqual(census["refunds"],[receipt])



if __name__ == "__main__": unittest.main(verbosity=2)
