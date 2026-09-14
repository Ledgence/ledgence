import asyncio
from dataclasses import FrozenInstanceError
import json
import time
import unittest
from unittest.mock import patch

from aiohttp import web

from ledgence.client import (
    AsyncClient, InputError, ProtocolError, RequestTimeout, TaskPage, TaskState,
    TaskStatus, Unavailable,
)
from ledgence.client import codec, discovery
from support import response, status


class DiscoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests = []
        self.reply = {"items": [], "next_cursor": None}
        self.callback = None

        async def handle(request):
            self.requests.append((request.method, request.path, dict(request.query),
                                  await request.read()))
            if self.callback is not None:
                return await self.callback(request)
            return response(self.reply)

        app = web.Application()
        app.router.add_route("*", "/{path:.*}", handle)
        self.runner = web.AppRunner(app, shutdown_timeout=.1, max_line_size=32 * 1024)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        self.addAsyncCleanup(self.runner.cleanup)
        self.client = await self.open_client()

    async def open_client(self, **kwargs):
        client = AsyncClient(self.url, tenant="tenant", namespace="tests", **kwargs)
        await client.__aenter__()
        self.addAsyncCleanup(client.close)
        return client

    def item(self, task_id="task", **kwargs):
        item = status(task_id=task_id)
        item.update(kwargs)
        return item

    async def test_default_empty_page_is_one_scoped_bodyless_get(self):
        page = await self.client.tasks.list()
        self.assertIsInstance(page, TaskPage)
        self.assertEqual(page.items, ())
        self.assertIsNone(page.next_cursor)
        self.assertEqual(self.requests, [("GET", "/v1/tasks", {
            "tenant_id": "tenant", "namespace": "tests", "limit": "50",
        }, b"")])
        with self.assertRaises(FrozenInstanceError):
            page.next_cursor = "changed"

    async def test_exact_filters_and_explicit_opaque_continuation(self):
        item = status("failed", task_id="newest")
        item.update(queue="billing & reconciliation", correlation_key="INV-1042 / caf\u00e9",
                    submitted_at=20)
        self.reply = {"items": [item], "next_cursor": "version-next:/opaque?cursor=\u00e9"}
        filters = dict(state=TaskState.FAILED, queue=item["queue"],
                       correlation_key=item["correlation_key"], submitted_from=10,
                       submitted_until=30)
        page = await self.client.tasks.list(**filters, limit=1)
        self.assertEqual(len(self.requests), 1)
        self.assertIsInstance(page.items[0], TaskStatus)
        self.assertEqual(page.items[0].task_id, "newest")
        self.assertEqual(page.items[0].state, TaskState.FAILED)
        self.assertEqual(self.requests[0][2], {
            "tenant_id": "tenant", "namespace": "tests", "state": "failed",
            "queue": item["queue"], "correlation_key": item["correlation_key"],
            "submitted_from": "10", "submitted_until": "30", "limit": "1",
        })
        with self.assertRaises(FrozenInstanceError):
            page.items[0].state = TaskState.QUEUED
        self.reply = {"items": [], "next_cursor": None}
        final = await self.client.tasks.list(**filters, limit=100, cursor=page.next_cursor)
        self.assertEqual(final.items, ())
        self.assertEqual(self.requests[1][2]["cursor"], page.next_cursor)
        self.assertEqual(self.requests[1][2]["limit"], "100")
        self.assertEqual(len(self.requests), 2)

    async def test_empty_correlation_and_time_boundaries_are_preserved(self):
        self.reply = {"items": [self.item(correlation_key="", submitted_at=0)],
                      "next_cursor": None}
        page = await self.client.tasks.list(correlation_key="", submitted_from=0,
                                            submitted_until=253402300799999)
        self.assertEqual(page.items[0].correlation_key, "")
        self.assertEqual(self.requests[0][2]["correlation_key"], "")
        self.assertEqual(self.requests[0][2]["submitted_from"], "0")
        self.assertEqual(self.requests[0][2]["submitted_until"], "253402300799999")
        self.reply = {"items": [], "next_cursor": None}
        await self.client.tasks.list(submitted_from=253402300799999)
        self.assertEqual(self.requests[-1][2]["submitted_from"], "253402300799999")
        await self.client.tasks.list(submitted_until=0)
        self.assertEqual(self.requests[-1][2]["submitted_until"], "0")

    async def test_invalid_filters_never_dispatch(self):
        invalid = [
            {"state": "running"}, {"state": True}, {"state": []},
            {"queue": ""}, {"queue": 1}, {"queue": "q" * 129},
            {"queue": "bad\nqueue"}, {"correlation_key": False},
            {"correlation_key": "\u00e9" * 257}, {"correlation_key": "bad\nkey"},
            {"submitted_from": -1}, {"submitted_until": 253402300800000},
            {"submitted_from": True}, {"submitted_until": 1.0},
            {"submitted_from": "1"}, {"submitted_from": 5, "submitted_until": 5},
            {"submitted_from": 6, "submitted_until": 5},
            {"limit": 0}, {"limit": 101}, {"limit": True}, {"limit": 1.0},
            {"limit": "1"}, {"cursor": ""}, {"cursor": 5},
            {"cursor": "\u00e9" * 4097}, {"cursor": "\ud800"},
        ]
        for query in invalid:
            with self.subTest(query=repr(query)), self.assertRaises(InputError):
                await self.client.tasks.list(**query)
        self.assertEqual(self.requests, [])

    async def test_input_cursor_size_is_utf8_bytes_and_has_no_encoding_assumption(self):
        for cursor in ("x" * 8192, "\u00e9" * 4096, "new-version:opaque/??"):
            await self.client.tasks.list(cursor=cursor)
            self.assertEqual(self.requests[-1][2]["cursor"], cursor)

    async def test_tied_timestamps_have_strict_descending_task_id_order(self):
        self.reply = {"items": [self.item("task_z", submitted_at=2),
                                 self.item("\u00e9", submitted_at=1),
                                 self.item("task_b", submitted_at=1),
                                 self.item("task_a", submitted_at=1)],
                      "next_cursor": None}
        page = await self.client.tasks.list()
        self.assertEqual([item.task_id for item in page.items],
                         ["task_z", "\u00e9", "task_b", "task_a"])
        for items in ([self.item("a"), self.item("b")],
                      [self.item("b", submitted_at=1), self.item("a", submitted_at=2)],
                      [self.item("a", submitted_at=2), self.item("a", submitted_at=1)]):
            self.reply = {"items": items, "next_cursor": None}
            with self.subTest(items=items), self.assertRaises(ProtocolError):
                await self.client.tasks.list()

    async def test_page_status_timestamp_boundaries_without_time_filters(self):
        maximum = 253402300799999
        fields = ("submitted_at", "available_at", "terminal_at", "cancel_requested_at")
        valid = status("cancelled")
        valid.update({field: maximum for field in fields})
        self.reply = {"items": [valid], "next_cursor": None}
        page = await self.client.tasks.list()
        for field in fields:
            self.assertEqual(getattr(page.items[0], field), maximum)
            self.reply = {"items": [{**valid, field: maximum + 1}], "next_cursor": None}
            with self.subTest(field=field), self.assertRaises(ProtocolError):
                await self.client.tasks.list()

    async def test_response_must_match_every_filter_and_scope(self):
        filters = {"state": "queued", "queue": "queue", "correlation_key": "",
                   "submitted_from": 10, "submitted_until": 20}
        base = self.item(correlation_key="", submitted_at=10)
        other_state = status("failed")
        invalid = [
            {**base, "scope": {"tenant_id": "other", "namespace": "tests"}},
            {**base, "scope": {"tenant_id": "tenant", "namespace": "other"}},
            {**other_state, "correlation_key": "", "submitted_at": 10},
            {**base, "queue": "different"}, {**base, "correlation_key": None},
            {**base, "correlation_key": "different"}, {**base, "submitted_at": 9},
            {**base, "submitted_at": 20},
        ]
        for item in invalid:
            self.reply = {"items": [item], "next_cursor": None}
            with self.subTest(item=item), self.assertRaises(ProtocolError):
                await self.client.tasks.list(**filters)
        self.reply = {"items": [base], "next_cursor": None}
        self.assertEqual((await self.client.tasks.list(**filters)).items[0].submitted_at, 10)

    async def test_malformed_pages_and_item_statuses_are_protocol_errors(self):
        malformed = [
            None, [], {}, {"items": []}, {"next_cursor": None},
            {"items": [], "next_cursor": None, "extra": True},
            {"items": {}, "next_cursor": None}, {"items": [None], "next_cursor": None},
            {"items": [{}], "next_cursor": None},
            {"items": [self.item(task_id=7)], "next_cursor": None},
            {"items": [self.item(attempt_count=True)], "next_cursor": None},
            {"items": [self.item(extra=True)], "next_cursor": None},
            {"items": [self.item("b"), self.item("a")], "next_cursor": None},
        ]
        for reply in malformed:
            self.reply = reply
            with self.subTest(reply=reply), self.assertRaises(ProtocolError) as raised:
                await self.client.tasks.list(limit=1)
            self.assertEqual(raised.exception.request_id, "request")

    async def test_continuations_require_full_page_and_bounded_new_text(self):
        for cursor in ("", 5, True, [], "\u00e9" * 4097, "prior"):
            self.reply = {"items": [self.item()], "next_cursor": cursor}
            with self.subTest(cursor=repr(cursor)), self.assertRaises(ProtocolError):
                await self.client.tasks.list(limit=1, cursor="prior")
        for items in ([], [self.item()]):
            self.reply = {"items": items, "next_cursor": "next"}
            with self.subTest(items=items), self.assertRaises(ProtocolError):
                await self.client.tasks.list(limit=2)
        self.reply = {"items": [self.item()], "next_cursor": "\u00e9" * 4096}
        self.assertEqual(len((await self.client.tasks.list(limit=1)).next_cursor), 4096)
        self.reply = {"items": [self.item()], "next_cursor": None}
        self.assertIsNone((await self.client.tasks.list(limit=1)).next_cursor)

    async def test_page_response_body_has_its_own_two_mib_limit(self):
        padding = codec.TASK_PAGE_LIMIT - len(json.dumps(self.reply).encode())
        body = json.dumps(self.reply).encode() + b" " * padding

        async def padded(request):
            return web.Response(body=body, content_type="application/json")

        self.callback = padded
        self.assertEqual((await self.client.tasks.list()).items, ())
        body += b" "
        with self.assertRaises(ProtocolError):
            await self.client.tasks.list()

    async def test_read_failure_is_returned_without_page_retry_or_mutation_uncertainty(self):
        async def unavailable(request):
            return response({"code": "unavailable", "message": "read unavailable"}, code=503)

        self.callback = unavailable
        with self.assertRaises(Unavailable) as raised:
            await self.client.tasks.list()
        self.assertEqual(raised.exception.request_id, "request")
        self.assertEqual(len(self.requests), 1)

    async def test_list_deadline_includes_query_preparation(self):
        client = await self.open_client(request_timeout=.01)
        original = discovery._TaskQuery

        def delayed(*args, **kwargs):
            time.sleep(.03)
            return original(*args, **kwargs)

        with patch("ledgence.client.tasks._TaskQuery", delayed):
            with self.assertRaises(RequestTimeout) as raised:
                await client.tasks.list()
        self.assertFalse(raised.exception.dispatched)
        self.assertEqual(self.requests, [])

    async def test_list_deadline_includes_transfer_and_page_validation(self):
        client = await self.open_client(request_timeout=.03)

        async def delayed(request):
            await asyncio.sleep(.1)
            return response(self.reply)

        self.callback = delayed
        with self.assertRaises(RequestTimeout) as raised:
            await client.tasks.list()
        self.assertTrue(raised.exception.dispatched)
        self.callback = None
        original = discovery._TaskQuery.parse

        def delayed_parse(query, raw, scope):
            time.sleep(.1)
            return original(query, raw, scope)

        with patch.object(discovery._TaskQuery, "parse", delayed_parse):
            with self.assertRaises(RequestTimeout) as raised:
                await client.tasks.list()
        self.assertTrue(raised.exception.dispatched)
        self.assertEqual(len(self.requests), 2)
