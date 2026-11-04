"""Wheel-installed public client discovery over loopback HTTP."""

import json
import unittest

import test_workflows
from ledgence.client import InputError, ProtocolError, ProviderSourceSubject, ResourceSourceState
from support import SCOPE, response


def token(seq, state="unknown_occupied", name="model"):
    return json.dumps({"v":1, "scope":SCOPE, "name":name, "state":state,
                       "sequence":str(seq)}, separators=(",", ":")).encode().hex()


class SourceTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflows.WorkflowClientTests.asyncSetUp

    def serve(self, page):
        async def handler(request, body):
            return response(page)
        self.callback = handler

    async def test_lossless_typed_cursor_and_occurrence(self):
        seq = 2**63 - 1
        subject = {"kind":"provider","workflow_id":"wf","origin_activation_id":"origin",
                   "key":"call","approval_activation_id":"origin","approval_key":"decision"}
        row = {"scope":SCOPE,"resource":"model","sequence":str(seq),"revision":"11",
               "updated_at":25,"state":"unknown_occupied","subject":subject}
        self.serve({"items":[row],"next_cursor":token(seq)})
        page = await self.client.resources.sources("model", state=ResourceSourceState.UNKNOWN_OCCUPIED, limit=1)
        self.assertEqual(self.requests[-1][:2], ("POST", "/v1/resources/sources"))
        self.assertEqual(json.loads(self.requests[-1][3]), {"scope":SCOPE,"name":"model",
            "state":"unknown_occupied","cursor":None,"limit":1})
        self.assertEqual(page.items[0].sequence, seq)
        self.assertEqual(page.items[0].subject, ProviderSourceSubject("wf","origin","call","origin","decision"))
        self.serve({"items":[],"next_cursor":None})
        self.assertFalse((await self.client.resources.sources("model", state="unknown_occupied", cursor=page.next_cursor)).items)
        self.assertEqual(json.loads(self.requests[-1][3])["cursor"],page.next_cursor)
        calls = len(self.requests)
        for options in ({"state":"finished"},{"limit":101},{"cursor":token(seq)},{"cursor":"AA"},
                        {"state":"unknown_occupied","cursor":token(seq,name="other")}):
            with self.assertRaises(InputError):
                await self.client.resources.sources("model", **options)
        self.assertEqual(len(self.requests),calls)

    async def test_reject_foreign_malformed_or_nonadvancing_pages(self):
        row = {"scope":SCOPE,"resource":"model","sequence":"8","revision":"2",
               "updated_at":25,"state":"unknown_occupied","subject":{"kind":"task","task_id":"one","generation":1}}
        pages = [
            {"items":[],"next_cursor":token(8)},
            {"items":[row,row],"next_cursor":None},
            {"items":[{**row,"resource":"foreign"}],"next_cursor":None},
            {"items":[{**row,"state":"possibly_dispatched"}],"next_cursor":None},
            {"items":[{**row,"sequence":8}],"next_cursor":None},
            {"items":[{**row,"payload":{}}],"next_cursor":None},
            {"items":[{**row,"subject":{"kind":"task","task_id":"t","generation":0}}],"next_cursor":None},
            {"items":[row],"next_cursor":token(7)},
        ]
        for page in pages:
            self.serve(page)
            with self.assertRaises(ProtocolError):
                await self.client.resources.sources("model", state="unknown_occupied")
