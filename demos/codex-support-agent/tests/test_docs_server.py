"""Exercise the actual isolated MCP stdio subprocess using only local files."""

import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest

SERVER = Path(__file__).resolve().parents[1] / "agent" / "docs_server.py"
SPEC = importlib.util.spec_from_file_location("codex_docs_server", SERVER)
server = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(server)


def request(method, params=None, identifier=1):
    result = {"jsonrpc": "2.0", "id": identifier, "method": method}
    if params is not None:
        result["params"] = params
    return result


def initialize(version="2025-11-25"):
    return [request("initialize", {"protocolVersion": version, "capabilities": {},
                                   "clientInfo": {"name": "offline-test", "version": "1"}}),
            {"jsonrpc": "2.0", "method": "notifications/initialized"}]


def call(name, arguments, identifier=2):
    return request("tools/call", {"name": name, "arguments": arguments}, identifier)


class McpTests(unittest.TestCase):
    def run_server(self, messages, *, audit_path=None, expected_code=0):
        with tempfile.TemporaryDirectory() as directory:
            path = audit_path if audit_path is not None else Path(directory) / "audit.json"
            payload = "".join((message if isinstance(message, str) else json.dumps(message)) + "\n" for message in messages)
            completed = subprocess.run(
                [sys.executable, "-I", str(SERVER), "--audit-path", str(path)], input=payload,
                text=True, encoding="utf-8", capture_output=True, cwd=directory, timeout=5,
                env={**os.environ, "PYTHONPATH": "/untrusted/path-is-ignored"},
            )
            self.assertEqual(completed.returncode, expected_code, completed.stderr)
            if expected_code == 0:
                self.assertEqual(completed.stderr, "")
            outputs = [json.loads(line) for line in completed.stdout.splitlines()]
            audit = json.loads(path.read_text()) if path.exists() else None
            return outputs, audit, completed

    def test_protocol_negotiation_and_only_two_read_only_tools(self):
        for version in (*server.PROTOCOL_VERSIONS, "unknown-newer-version"):
            with self.subTest(version=version):
                output, audit, _ = self.run_server([*initialize(version), request("tools/list", identifier=2), request("ping", identifier=3)])
                expected = version if version in server.PROTOCOL_VERSIONS else server.PROTOCOL_VERSIONS[-1]
                self.assertEqual(output[0]["result"]["protocolVersion"], expected)
                self.assertEqual(output[0]["result"]["serverInfo"]["name"], "ledgence_docs")
                tools = output[1]["result"]["tools"]
                self.assertEqual({tool["name"] for tool in tools}, {"search_docs", "read_doc"})
                for tool in tools:
                    self.assertTrue(tool["annotations"]["readOnlyHint"])
                    self.assertFalse(tool["annotations"]["openWorldHint"])
                    self.assertFalse(tool["inputSchema"]["additionalProperties"])
                self.assertEqual(output[2]["result"], {})
                self.assertEqual(audit["tool_calls"], 0)

    def test_real_tools_return_docs_and_persist_only_successful_read_evidence(self):
        output, audit, _ = self.run_server([
            *initialize(), call("search_docs", {"query": "task result timeout"}),
            call("read_doc", {"document_id": "task-results"}, 3),
            call("read_doc", {"document_id": "python-client"}, 4),
        ])
        matches = json.loads(output[1]["result"]["content"][0]["text"])["matches"]
        self.assertIn("task-results", [item["id"] for item in matches])
        read = json.loads(output[2]["result"]["content"][0]["text"])
        self.assertEqual(read["location"], "docs/task-results.md")
        self.assertIn("timeout", read["text"])
        self.assertIn("AsyncClient", output[3]["result"]["content"][0]["text"])
        self.assertEqual(audit, {"version": 1, "tool_calls": 3, "searched": True,
                                 "read_ids": ["python-client", "task-results"], "exhausted": False})

    def test_invalid_tools_and_arguments_count_but_do_not_fabricate_reads(self):
        calls = [call("read_doc", {"document_id": "task-results"}), call("unknown", {}),
                 call("search_docs", {"query": "task", "extra": "ignored"}),
                 call("search_docs", {"query": "task"}), call("read_doc", {"document_id": "../program.py"})]
        output, audit, _ = self.run_server([*initialize(), *calls])
        self.assertEqual([item["result"]["isError"] for item in output[1:]], [True, True, True, False, True])
        self.assertEqual(audit["tool_calls"], 5)
        self.assertEqual(audit["read_ids"], [])
        self.assertNotIn("../program.py", output[-1]["result"]["content"][0]["text"])

    def test_ninth_call_marks_exhaustion_and_does_not_execute(self):
        messages = [*initialize(), call("search_docs", {"query": "task"}), call("read_doc", {"document_id": "task-results"})]
        messages += [call("search_docs", {"query": "task"}, index + 3) for index in range(7)]
        output, audit, _ = self.run_server(messages)
        self.assertEqual(audit["tool_calls"], 8)
        self.assertTrue(audit["exhausted"])
        self.assertTrue(output[-1]["result"]["isError"])
        self.assertIn("budget", output[-1]["result"]["content"][0]["text"])

    def test_restart_resumes_remaining_budget_and_existing_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.json"
            first = [*initialize(), call("search_docs", {"query": "task"}), call("read_doc", {"document_id": "task-results"})]
            first += [call("search_docs", {"query": "task"}) for _ in range(5)]
            self.run_server(first, audit_path=path)
            output, audit, _ = self.run_server([
                *initialize(), call("read_doc", {"document_id": "python-client"}), call("search_docs", {"query": "task"})],
                audit_path=path,
            )
            self.assertFalse(output[-2]["result"]["isError"])
            self.assertTrue(output[-1]["result"]["isError"])
            self.assertEqual(audit["read_ids"], ["python-client", "task-results"])
            self.assertEqual((audit["tool_calls"], audit["exhausted"]), (8, True))

    def test_protocol_errors_are_fixed_and_notifications_do_not_execute_tools(self):
        output, audit, _ = self.run_server([
            "private invalid JSON", '{"jsonrpc":"2.0","jsonrpc":"2.0"}', [],
            request("tools/list"), *initialize(), request("unknown/method", identifier=7),
            {"jsonrpc": "2.0", "method": "tools/call", "params": {"name": "search_docs", "arguments": {"query": "task"}}},
            request("ping", identifier=8),
        ])
        self.assertEqual([item["error"]["code"] for item in output[:4]], [-32700, -32700, -32600, -32002])
        self.assertEqual(output[-2]["error"]["code"], -32601)
        self.assertEqual(output[-1]["result"], {})
        self.assertEqual(audit["tool_calls"], 0)
        self.assertNotIn("private", json.dumps(output))

    def test_oversized_message_is_bounded_and_stops_server(self):
        output, audit, _ = self.run_server(["x" * (server.MAX_MESSAGE_BYTES + 1)], expected_code=2)
        self.assertEqual(len(output), 1)
        self.assertIn("size limit", output[0]["error"]["message"])
        self.assertEqual(audit["tool_calls"], 0)

    def test_large_request_id_cannot_make_overflow_error_unbounded(self):
        large_id = "x" * (server.MAX_MESSAGE_BYTES - 100)
        output, _, completed = self.run_server([*initialize(), request("tools/list", identifier=large_id)])
        self.assertIsNone(output[-1]["id"])
        self.assertEqual(output[-1]["error"]["code"], -32603)
        self.assertTrue(all(len(line.encode()) < server.MAX_MESSAGE_BYTES for line in completed.stdout.splitlines()))

    def test_eof_exits_and_audit_is_private_without_temporary_files(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.json"
            self.run_server(initialize(), audit_path=path)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual({entry.name for entry in path.parent.iterdir()}, {"audit.json", "audit.lock"})
            # The first server exited on stdin EOF and released its OS lock.
            self.run_server(initialize(), audit_path=path)

    def test_concurrent_server_cannot_reset_or_share_an_invocation_budget(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.json"
            child = subprocess.Popen([sys.executable, "-I", str(SERVER), "--audit-path", str(path)],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                child.stdin.write(json.dumps(initialize()[0]) + "\n")
                child.stdin.flush()
                self.assertEqual(json.loads(child.stdout.readline())["result"]["serverInfo"]["name"], "ledgence_docs")
                _, audit, failure = self.run_server(initialize(), audit_path=path, expected_code=1)
                self.assertIn("could not maintain", failure.stderr)
                self.assertEqual(audit["tool_calls"], 0)
            finally:
                if child.poll() is None:
                    child.stdin.close()
                    child.wait(timeout=5)
                child.stdout.close()
                child.stderr.close()

    def test_corrupted_existing_audit_fails_without_resetting_it(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.json"
            original = '{"version":true}'
            path.write_text(original)
            output, _, failure = self.run_server(initialize(), audit_path=path, expected_code=1)
            self.assertEqual(output, [])
            self.assertIn("could not maintain", failure.stderr)
            self.assertEqual(path.read_text(), original)


if __name__ == "__main__":
    unittest.main()
