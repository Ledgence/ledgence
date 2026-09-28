"""Offline Codex protocol and bounded subprocess tests (MIT)."""
import asyncio
import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from change_review import codex
from change_review.processes import ProcessError, collect


def events():
    return [{"type": "thread.started", "thread_id": "thread-fixture"}, {"type": "turn.started"},
            {"type": "item.completed", "item": {"type": "agent_message", "text": '{"source":"x","summary":"y"}'}},
            {"type": "turn.completed", "usage": {"input_tokens": 20, "cached_input_tokens": 10, "output_tokens": 3}}]


def encoded(items):
    return b"\n".join(json.dumps(item).encode() for item in items)


class ProtocolTests(unittest.TestCase):
    def test_completed_tool_free_turn_returns_only_structured_output_and_usage(self):
        output, thread, usage = codex.parse_events(encoded(events()))
        self.assertEqual(output, {"source": "x", "summary": "y"})
        self.assertEqual(thread, "thread-fixture")
        self.assertEqual(usage["reasoning_output_tokens"], None)
        warning = {"type": "item.completed", "item": {"type": "error", "message": "ordinary CLI warning"}}
        self.assertEqual(codex.parse_events(encoded([warning, *events(), warning]))[0], output)

    def test_incomplete_out_of_order_tool_usage_and_malformed_events_are_rejected(self):
        variants = [events()[:-1], events()[1:], [*events(), events()[-1]],
                    [events()[0], events()[0], *events()[1:]],
                    [events()[0], events()[2], events()[1], events()[3]],
                    [{"type": "error", "message": "private provider detail"}]]
        for item_type in ("command_execution", "mcp_tool_call", "file_change", "web_search", "todo_list"):
            items = events()
            items[2]["item"]["type"] = item_type
            variants.append(items)
        for value in (True, -1, "20", 2**63):
            items = events()
            items[-1]["usage"]["input_tokens"] = value
            variants.append(items)
        items = events()
        items[-1]["usage"]["cached_input_tokens"] = 21
        variants.append(items)
        items = events()
        items[2]["item"]["text"] = '{"summary":1,"summary":2}'
        variants.append(items)
        items = events()
        items[2]["item"]["text"] = '{"value":NaN}'
        variants.append(items)
        items = events()
        items[2]["item"] = {"type": "error", "message": "model rerouted: other-model"}
        variants.append(items)
        for items in variants:
            with self.subTest(items=items), self.assertRaises(codex.CodexError) as caught:
                codex.parse_events(encoded(items))
            self.assertNotIn("private provider detail", str(caught.exception))
        for raw in (b"\xff", b"not-json", b"[]", b"{\"type\":\"turn.started\",\"type\":\"turn.completed\"}"):
            with self.assertRaises(codex.CodexError):
                codex.parse_events(raw)

    def test_environment_preserves_login_location_without_api_or_appserver_credentials(self):
        with patch.dict(os.environ, {"HOME": "/fake/home", "CODEX_HOME": "/fake/codex",
                                     "OPENAI_API_KEY": "known-test-secret", "CODEX_REMOTE_URL": "private"}, clear=True):
            environment = codex.environment()
        self.assertEqual(environment["HOME"], "/fake/home")
        self.assertEqual(environment["CODEX_HOME"], "/fake/codex")
        self.assertNotIn("OPENAI_API_KEY", environment)
        self.assertNotIn("CODEX_REMOTE_URL", environment)


class ProcessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)

    async def asyncTearDown(self):
        self.temporary.cleanup()

    def fake_cli(self, body):
        path = self.directory / "fake-codex"
        path.write_text("#!" + sys.executable + "\nimport json, os, sys\n"
                        "if '--version' in sys.argv:\n    print('codex-cli 0.158.0-alpha.2.1')\n    raise SystemExit(0)\n" + body)
        path.chmod(0o700)
        return path

    async def test_runner_uses_new_sessions_schema_stdin_and_no_inherited_api_key(self):
        log = self.directory / "calls.jsonl"
        body = ("import uuid\n"
                f"with open({str(log)!r}, 'a') as stream:\n"
                "    stream.write(json.dumps({'argv':sys.argv,'prompt':sys.stdin.read(),'api_key_present':'OPENAI_API_KEY' in os.environ})+'\\n')\n"
                f"items = {events()!r}\n"
                "items[0]['thread_id'] = str(uuid.uuid4())\n"
                "for item in items: print(json.dumps(item))\n")
        binary = self.fake_cli(body)
        with patch.dict(os.environ, {"LEDGENCE_CODEX_BIN": str(binary), "OPENAI_API_KEY": "known-test-key"}):
            first = await codex.run_codex("PHASE: IMPLEMENT", {"type": "object"})
            second = await codex.run_codex("PHASE: REVIEW", {"type": "object"})
        self.assertNotEqual(first["execution"]["thread_id"], second["execution"]["thread_id"])
        calls = [json.loads(line) for line in log.read_text().splitlines()]
        self.assertEqual([call["prompt"] for call in calls], ["PHASE: IMPLEMENT", "PHASE: REVIEW"])
        directories = []
        for call in calls:
            args = call["argv"]
            for expected in ("--ephemeral", "--ignore-user-config", "--ignore-rules", "--strict-config", "read-only"):
                self.assertIn(expected, args)
            self.assertIn("forced_login_method=\"chatgpt\"", args)
            self.assertIn("features.shell_tool=false", args)
            self.assertFalse(call["api_key_present"])
            directory = args[args.index("--cd") + 1]
            directories.append(directory)
            self.assertFalse(Path(directory).exists())
        self.assertEqual(len(set(directories)), 2)

    async def test_cli_deadline_reaps_direct_child_and_does_not_leak_diagnostic(self):
        pid_path = self.directory / "pid"
        binary = self.fake_cli(f"import time\nopen({str(pid_path)!r},'w').write(str(os.getpid()))\nprint('private output',file=sys.stderr,flush=True)\ntime.sleep(20)\n")
        with patch.dict(os.environ, {"LEDGENCE_CODEX_BIN": str(binary)}):
            with self.assertRaises(codex.CodexError) as caught:
                await codex.run_codex("hello", {"type": "object"}, timeout=1.0)
        self.assertNotIn("private output", str(caught.exception))
        with self.assertRaises(ProcessLookupError):
            os.kill(int(pid_path.read_text()), 0)

    async def test_collect_bounds_both_streams_and_handles_deadline_during_input(self):
        for stream in ("stdout", "stderr"):
            with self.subTest(stream=stream), self.assertRaisesRegex(ProcessError, "output limit"):
                await collect([sys.executable, "-c", f"import sys;sys.{stream}.write('x'*9000)"],
                              cwd=self.directory, environment={}, stdout_limit=1024, stderr_limit=1024)
        with self.assertRaisesRegex(ProcessError, "deadline"):
            await collect([sys.executable, "-c", "import time;time.sleep(20)"], cwd=self.directory,
                          environment={}, data=b"x" * (1024 * 1024), timeout=0.1)

    async def test_cancellation_reaps_direct_child(self):
        pid_path = self.directory / "cancelled-pid"
        task = asyncio.create_task(collect([sys.executable, "-c",
            f"import os,time;open({str(pid_path)!r},'w').write(str(os.getpid()));time.sleep(20)"],
            cwd=self.directory, environment={}))
        for _ in range(100):
            if pid_path.exists():
                break
            await asyncio.sleep(0.01)
        self.assertTrue(pid_path.exists())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        with self.assertRaises(ProcessLookupError):
            os.kill(int(pid_path.read_text()), 0)

    async def test_nonzero_exit_invalid_event_and_invalid_configuration_fail_closed(self):
        for body in ("print('private detail',file=sys.stderr)\nraise SystemExit(7)\n", "print('malformed JSON')\n"):
            binary = self.fake_cli(body)
            with patch.dict(os.environ, {"LEDGENCE_CODEX_BIN": str(binary)}):
                with self.assertRaises(codex.CodexError):
                    await codex.run_codex("hello", {"type": "object"})
        with patch.dict(os.environ, {"LEDGENCE_CODEX_BIN": "relative-path"}):
            with self.assertRaises(codex.CodexError):
                await codex.run_codex("hello", {"type": "object"})
        for timeout in (True, float("nan"), 0, 121):
            with self.assertRaises(codex.CodexError):
                await codex.run_codex("hello", {"type": "object"}, timeout=timeout)


if __name__ == "__main__":
    unittest.main()
