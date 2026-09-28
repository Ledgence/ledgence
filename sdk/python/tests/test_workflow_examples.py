"""Public workflow examples: bounded IO, replay, and terminal child handling (MIT)."""
import asyncio
from contextlib import suppress
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import test_workflow
from ledgence.worker.workflow import (
    MAX_DECISION_BYTES, MAX_RECORDS_BYTES, WorkflowContext, _workflow,
)


ROOT = Path(__file__).resolve().parents[3]


def load_example(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / "examples" / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CHECKPOINT = load_example("checkpoint_example", "checkpoint-workflow/controller/program.py")
OWNED = load_example("owned_example", "owned-subworkflows/program.py")
MIXED = load_example("mixed_example", "mixed-workflow/program.py")


def child(state="succeeded", output=None, *, workflow=False):
    outcome = {"kind": state}
    if state != "cancelled":
        if workflow:
            outcome.update(output=output) if state == "succeeded" else outcome.update(
                error={"kind": "example_failure", "message": "child failed"})
        else:
            outcome.update(attempt_id="attempt-child", quiescence="confirmed",
                           execution_may_have_started=True)
            outcome.update(output=output) if state == "succeeded" else outcome.update(
                failure={"kind": "application", "error": {"kind": "ValueError", "message": "child failed"}})
    identity = {"kind": "workflow", "workflow_id": "workflow-child"} if workflow else {"task_id": "task-child"}
    return {**identity, "state": state, "outcome": outcome}


class MixedPreparationTests(unittest.TestCase):
    def prepare(self, output):
        return subprocess.run([sys.executable, "-B", str(ROOT / "examples/mixed-workflow/prepare.py"), str(output)],
                              capture_output=True, text=True, timeout=10)

    def test_prepares_current_source_in_fresh_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "program"
            result = self.prepare(output)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual({file.name for file in output.iterdir()}, {"ledgence-program.json", "program.py"})
            manifest = json.loads((output / "ledgence-program.json").read_text())
            self.assertEqual(manifest["program"], {"id": "mixed-workflow", "version": "1.0.1"})
            self.assertEqual(manifest["runtime"]["protocol"], 3)
            self.assertEqual((output / "program.py").read_bytes(),
                             (ROOT / "examples/mixed-workflow/program.py").read_bytes())

    def test_existing_directory_and_file_remain_untouched(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "program"
            output.mkdir()
            stale = output / "stale.py"
            stale.write_text("do not package me")
            result = self.prepare(output)
            self.assertEqual(result.returncode, 2)
            self.assertIn("output already exists", result.stderr)
            self.assertEqual(list(output.iterdir()), [stale])
            self.assertEqual(stale.read_text(), "do not package me")
            result = self.prepare(stale)
            self.assertEqual(result.returncode, 2)
            self.assertIn("output already exists", result.stderr)
            self.assertEqual(stale.read_text(), "do not package me")


class WorkflowExampleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.contexts, self.requests, self.servers = [], [], []
        self.connections = set()
        self.http_requests = 0
        self.request_started = asyncio.Event()
        self.connection_closed = asyncio.Event()

    async def asyncTearDown(self):
        for context in self.contexts:
            await context._finish(cancel=True)
        for server in self.servers:
            server.close()
            await server.wait_closed()
        for task in self.connections:
            task.cancel()
        if self.connections:
            await asyncio.gather(*self.connections, return_exceptions=True)

    async def invoke(self, module, data=None, **payload):
        async def rpc(operation, request):
            self.requests.append((operation, request))
            if operation == "workflow.fork":
                return {"committed": True, "key": request["key"],
                        "branch_keys": [item["key"] for item in request["branches"]]}
            self.assertEqual(operation, "local_step.commit")
            return {"committed": True}
        context = WorkflowContext(test_workflow.payload(**payload), rpc)
        self.contexts.append(context)
        token = _workflow.set(context)
        try:
            decision = await module.handle({"data": data})
            await context._drain()
            return context._validate_decision(decision)
        finally:
            _workflow.reset(token)

    async def server(self, response, *, leave_open=False):
        async def handle(reader, writer):
            task = asyncio.current_task()
            self.connections.add(task)
            try:
                await reader.readuntil(b"\r\n\r\n")
                self.http_requests += 1
                self.request_started.set()
                writer.write(response)
                await writer.drain()
                if leave_open:
                    await reader.read()
            except (ConnectionError, asyncio.IncompleteReadError):
                pass
            finally:
                writer.close()
                with suppress(ConnectionError):
                    await writer.wait_closed()
                self.connections.discard(task)
                self.connection_closed.set()
        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        self.servers.append(server)
        return f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/page"

    async def test_checkpoint_maximum_escaped_pages_fit_and_replay_without_http(self):
        body = b"\x00" * CHECKPOINT.MAX_PAGE_BYTES
        url = await self.server(b"HTTP/1.1 200 OK\r\nContent-Length: 8192\r\n\r\n" + body)
        url += "?" + "x" * (CHECKPOINT.MAX_URL_LENGTH - len(url) - 1)
        data = {"urls": [url] * CHECKPOINT.MAX_PAGES, "queue": "workflows"}
        decision = await self.invoke(CHECKPOINT, data)
        self.assertEqual(decision["continuation"], "collect")
        self.assertEqual(decision["until"], ["summarize"])
        self.assertEqual(decision["state"], {"page_count": 4})
        self.assertEqual(decision["commands"][0]["program"]["version"], "1.0.0")
        records = [request for _, request in self.requests]
        self.assertEqual(len(records), 4)
        self.assertLess(len(json.dumps(records, separators=(",", ":")).encode()), MAX_RECORDS_BYTES)
        self.assertLess(len(json.dumps(decision, separators=(",", ":")).encode()), MAX_DECISION_BYTES)
        self.assertEqual(await self.invoke(CHECKPOINT, data, local_steps=records), decision)
        self.assertEqual(self.http_requests, 4)
        self.assertEqual(len(self.requests), 4)

    async def test_checkpoint_http_timeout_remains_retryable_and_closes_connection(self):
        url = await self.server(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\na", leave_open=True)
        with patch.object(CHECKPOINT, "FETCH_TIMEOUT_SECONDS", 0.1):
            with self.assertRaises(TimeoutError):
                await self.invoke(CHECKPOINT, {"urls": [url], "queue": "workflows"})
        await asyncio.wait_for(self.connection_closed.wait(), 1)
        self.assertEqual(self.requests, [])

    async def test_checkpoint_fetch_cancellation_closes_connection(self):
        url = await self.server(b"", leave_open=True)
        task = asyncio.create_task(CHECKPOINT.fetch(url))
        await asyncio.wait_for(self.request_started.wait(), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await asyncio.wait_for(self.connection_closed.wait(), 1)

    async def test_checkpoint_rejects_unbounded_or_malformed_http_responses(self):
        responses = [
            b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\n\r\n",
            b"HTTP/1.1 200 OK\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: -1\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 8193\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nTransfer-Encoding: chunked\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nContent-Encoding: gzip\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nx",
            b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n\xff",
            b"HTTP/1.1 200 OK\r\nX-Long: " + b"x" * (17 * 1024) + b"\r\n\r\n",
        ]
        for response in responses:
            with self.subTest(response=response[:80]):
                url = await self.server(response)
                with self.assertRaises((ValueError, RuntimeError, asyncio.IncompleteReadError, asyncio.LimitOverrunError)):
                    await CHECKPOINT.fetch(url)

    async def test_checkpoint_invalid_input_fails_before_io(self):
        invalid_urls = (None, [], ["http://example.test"] * 5, [None], ["https://example.test"],
                        ["http://user:password@example.test"], ["http://example.test/#fragment"],
                        ["http://example.test:0/"], ["http://example.test:99999/"],
                        ["http://example.test/\r\nInjected: yes"], ["http://example.test/" + "x" * 2048])
        for urls in invalid_urls:
            with self.subTest(urls=urls):
                result = await self.invoke(CHECKPOINT, {"urls": urls, "queue": "workflows"})
                self.assertEqual(result["error"]["kind"], "invalid_input")
        self.assertEqual(self.requests, [])

    async def test_owned_stages_distinct_workflow_and_task(self):
        decision = await self.invoke(OWNED, {"urls": ["http://example.test/"], "queue": "workflows"})
        self.assertEqual(decision["continuation"], "collect")
        self.assertEqual(decision["until"], ["pages", "metadata"])
        self.assertEqual([(item.get("kind", "task"), item["program"]["id"], item["program"]["version"])
                          for item in decision["commands"]],
                         [("workflow", "workflow-example", "1.0.1"), ("task", "workflow-summary", "1.0.0")])
        self.assertEqual(self.requests, [])

    async def test_terminal_children_fail_explicitly_without_controller_exceptions(self):
        cases = [
            (CHECKPOINT, {"page_count": 1}, {"summarize": child(output={})}, "summary"),
            (OWNED, {}, {"pages": child(output={}, workflow=True), "metadata": child(output={})}, "child"),
            (MIXED, {"local": {}}, {"double:0": child(output=2, workflow=True),
                                   "triple:0": child(output=3, workflow=True)}, "branch"),
        ]
        for module, state, successful, prefix in cases:
            for key, original in successful.items():
                for terminal in ("failed", "cancelled"):
                    with self.subTest(module=module.__name__, child=key, terminal=terminal):
                        inputs = dict(successful, **{key: child(terminal, workflow=original.get("kind") == "workflow")})
                        result = await self.invoke(module, continuation="collect", revision=1, state=state, inputs=inputs)
                        self.assertEqual(result["kind"], "fail")
                        self.assertEqual(result["error"]["kind"], prefix + "_" + terminal)
        self.assertEqual(self.requests, [])

    async def test_successful_child_outputs_are_preserved(self):
        summary = {"characters": 15, "pages": 1}
        result = await self.invoke(CHECKPOINT, continuation="collect", revision=1, state={"page_count": 1},
                                   inputs={"summarize": child(output=summary)})
        self.assertEqual(result["output"], {"page_count": 1, "summary": summary})
        result = await self.invoke(OWNED, continuation="collect", revision=1, state={},
                                   inputs={"pages": child(output=summary, workflow=True), "metadata": child(output=42)})
        self.assertEqual(result["output"], {"pages": summary, "metadata": 42})
        result = await self.invoke(MIXED, continuation="collect", revision=1, state={"local": {"sum": 6}},
                                   inputs={"double:0": child(output=12, workflow=True), "triple:0": child(output=18, workflow=True)})
        self.assertEqual(result["output"], {"local": {"sum": 6}, "double": 12, "triple": 18})

    async def test_mixed_validates_numeric_inputs_before_fork(self):
        for values in (None, [], [True], [1.5], ["1"], [1_000_001], [-1_000_001], [1] * 1001):
            with self.subTest(values=values):
                result = await self.invoke(MIXED, {"values": values, "queue": "workflows"})
                self.assertEqual(result["error"]["kind"], "invalid_input")
        self.assertEqual(self.requests, [])
        result = await self.invoke(MIXED, {"values": [-1_000_000, 1_000_000] * 500, "queue": "workflows"})
        self.assertEqual(result["state"], {"local": {"count": 1000, "sum": 0}})
        self.assertEqual([operation for operation, _ in self.requests], ["workflow.fork", "local_step.commit"])
        self.assertEqual(result["until"], ["double:0", "triple:0"])

    async def test_mixed_local_error_uses_activation_retry_after_registration(self):
        def fail_locally(values):
            raise RuntimeError("local operation failed")
        with patch.object(MIXED, "summarize", fail_locally):
            with self.assertRaisesRegex(RuntimeError, "local operation failed"):
                await self.invoke(MIXED, {"values": [1, 2, 3], "queue": "workflows"})
        self.assertEqual([operation for operation, _ in self.requests], ["workflow.fork"])

    async def test_owned_rejects_oversized_url_input_before_staging(self):
        for urls in ([], ["http://example.test"] * 5, [None], ["http://example.test/" + "x" * 2048]):
            result = await self.invoke(OWNED, {"urls": urls, "queue": "workflows"})
            self.assertEqual(result["error"]["kind"], "invalid_input")
        self.assertEqual(self.requests, [])

    async def test_invalid_queue_and_missing_data_fail_before_scheduling(self):
        for module in (CHECKPOINT, OWNED, MIXED):
            for data in (None, [], {}, {"urls": ["http://example.test/"], "values": [1], "queue": "\n"},
                         {"urls": ["http://example.test/"], "values": [1], "queue": "q" * 129}):
                with self.subTest(module=module.__name__, data=data):
                    result = await self.invoke(module, data)
                    self.assertEqual(result["error"]["kind"], "invalid_input")
        self.assertEqual(self.requests, [])


if __name__ == "__main__":
    unittest.main()
