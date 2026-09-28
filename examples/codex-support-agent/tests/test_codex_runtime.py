"""Offline subprocess tests; fixtures never call a model or inspect credentials."""
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / "agent" / "codex_runtime.py"
spec = importlib.util.spec_from_file_location("support_codex_runtime", SOURCE)
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)


def events():
    return [
        {"type": "thread.started", "thread_id": "thread-demo-1"},
        {"type": "turn.started"},
        {"type": "item.started", "item": {"id": "call1", "type": "mcp_tool_call", "server": "ledgence_docs", "tool": "search_docs", "status": "in_progress"}},
        {"type": "item.completed", "item": {"id": "call1", "type": "mcp_tool_call", "server": "ledgence_docs", "tool": "search_docs", "status": "completed"}},
        {"type": "item.completed", "item": {"id": "answer", "type": "agent_message", "text": '{"reply":"Ready"}'}},
        {"type": "turn.completed", "usage": {"input_tokens": 300, "cached_input_tokens": 200, "output_tokens": 90, "reasoning_output_tokens": 30}},
    ]


def encoded(items):
    return "\n".join(json.dumps(item) for item in items).encode()


class EventsTests(unittest.TestCase):
    def test_diagnostic_categories_are_a_closed_set(self):
        self.assertEqual(runtime.CodexError("private detail").category, "runtime")
        for category in ["private-secret", None, {}, []]:
            with self.subTest(category=category):
                self.assertEqual(runtime.CodexError("private detail", category=category).category, "runtime")
        for category in runtime.ERROR_CATEGORIES:
            self.assertEqual(runtime.CodexError("private detail", category=category).category, category)

    def test_cli_failure_categories_are_safe_and_distinct(self):
        cases = [
            ("Error loading config.toml: private-secret", "configuration"),
            ("The model private-secret is not supported when using Codex with a ChatGPT account", "model_unavailable"),
            ("Authentication failed: private-secret", "authentication"),
            ("Unexpected status 401: private-secret", "authentication"),
            ("Unexpected status code: 429 private-secret", "quota"),
            ("You have hit your usage limit. private-secret", "quota"),
            ("MCP server ledgence_docs failed: private-secret", "mcp"),
            ("tools/list returned an invalid response: private-secret", "mcp"),
            ("error sending request private-secret", "network"),
            ("Unexpected HTTP status 503: private-secret", "provider"),
            ("private-secret", "cli_exit"),
        ]
        for message, category in cases:
            with self.subTest(category=category):
                self.assertEqual(runtime._failure_category(b"", message.encode()), category)
                self.assertEqual(runtime._failure_category(encoded([{"type": "error", "message": message}]), b""), category)
                self.assertEqual(runtime._failure_category(encoded([{"type": "turn.failed", "error": {"message": message}}]), b""), category)

    def test_diagnostics_ignore_successful_activity_and_documentation_content(self):
        values = events()
        values[4]["item"]["text"] = "Authentication failed: example from a public documentation page"
        stderr = b"Authentication complete\nMCP server ledgence_docs ready\nAn unrelated operation failed\n"
        self.assertEqual(runtime._failure_category(encoded(values), stderr), "cli_exit")
        self.assertEqual(runtime._failure_category(b"private-secret\xff", stderr), "cli_exit")
        # A structured fatal error wins over potentially unrelated stderr logs.
        self.assertEqual(runtime._failure_category(encoded([{"type": "error", "message": "model example is not supported"}]), b"MCP server failed"), "model_unavailable")

    def test_parser_categories_distinguish_protocol_from_cli_failure(self):
        unknown_item = events()
        unknown_item[2]["item"]["type"] = "private-secret"
        bad_usage = events()
        bad_usage[-1]["usage"]["output_tokens"] = -1
        cases = [
            (b"\xff", "event_encoding"),
            (b"not-json private-secret", "event_json"),
            (b"[]", "event_shape"),
            (b'{"type":[]}', "event_shape"),
            (b'{"type":"private-secret"}', "event_type"),
            (encoded(unknown_item), "item_type"),
            (encoded(events()[:-1]), "event_incomplete"),
            (encoded(events()[1:]), "event_missing_thread"),
            (encoded(bad_usage), "event_usage"),
        ]
        for raw, category in cases:
            with self.subTest(category=category), self.assertRaises(runtime.CodexError) as caught:
                runtime._parse_events(raw)
            self.assertEqual(caught.exception.category, category)
            self.assertNotIn("private-secret", str(caught.exception))

    def test_nonfatal_warning_items_are_allowed_outside_the_turn(self):
        # Mirrors upstream collect_warning/ConfigWarning/DeprecationNotice:
        # item.completed with error payload is informational, not a tool call.
        warning = {"type": "item.completed", "item": {"id": "warning", "type": "error", "message": "private-secret"}}
        for index in (0, 1, 2, len(events())):
            values = events()
            values.insert(index, warning)
            with self.subTest(index=index):
                result = runtime._parse_events(encoded(values))
                self.assertEqual(result, runtime._parse_events(encoded(events())))
                self.assertNotIn("private-secret", json.dumps(result))

    def test_warnings_cannot_complete_a_turn_or_mask_fatal_events(self):
        warning = {"type": "item.completed", "item": {"id": "warning", "type": "error", "message": "private-secret"}}
        variants = [[warning], [warning, *events()[:-1]], *[
            [*events()[:index], fatal, warning, *events()[index:]]
            for index in (0, 2, len(events()))
            for fatal in [{"type": "error", "message": "private-secret"}, {"type": "turn.failed", "error": {"message": "private-secret"}}]
        ]]
        for values in variants:
            with self.subTest(values=len(values)), self.assertRaises(runtime.CodexError) as caught:
                runtime._parse_events(encoded(values))
            self.assertNotIn("private-secret", str(caught.exception))

    def test_model_reroute_warning_cannot_misreport_the_requested_model(self):
        rerouted = {"type": "item.completed", "item": {"id": "warning", "type": "error", "message": "model rerouted: gpt-6-luna -> private-secret (HighRisk)"}}
        for index in (0, 2, len(events())):
            values = events()
            values.insert(index, rerouted)
            with self.subTest(index=index), self.assertRaises(runtime.CodexError) as caught:
                runtime._parse_events(encoded(values))
            self.assertEqual(caught.exception.category, "model_rerouted")
            self.assertNotIn("private-secret", str(caught.exception))

    def test_only_well_formed_completed_warning_items_are_allowed(self):
        variants = [
            {"type": "item.started", "item": {"type": "error", "message": "private-secret"}},
            {"type": "item.updated", "item": {"type": "error", "message": "private-secret"}},
            {"type": "item.completed", "item": {"type": "error", "message": None}},
            {"type": "item.completed", "item": {"type": "error", "message": "x" * (runtime.MAX_FINAL_BYTES + 1)}},
        ]
        for warning in variants:
            with self.subTest(kind=warning["type"]), self.assertRaises(runtime.CodexError) as caught:
                runtime._parse_events(encoded([warning, *events()]))
            self.assertEqual(caught.exception.category, "event_shape")

    def test_normal_items_and_turn_lifecycle_keep_precise_order_checks(self):
        valid = events()
        cases = [
            ([valid[0], valid[2], *valid[1:]], "event_before_turn"),
            ([*valid, valid[2]], "event_after_turn"),
            ([valid[0], valid[1], *valid[1:]], "event_repeated_start"),
            ([*valid, valid[-1]], "event_repeated_completion"),
            (valid[1:], "event_missing_thread"),
            ([valid[0], *valid], "event_repeated_thread"),
            ([valid[0], valid[-1], *valid[1:]], "event_before_turn"),
        ]
        for values, category in cases:
            with self.subTest(category=category), self.assertRaises(runtime.CodexError) as caught:
                runtime._parse_events(encoded(values))
            self.assertEqual(caught.exception.category, category)

    def test_success_preserves_real_usage_and_final(self):
        result = runtime._parse_events(encoded(events()))
        self.assertEqual(result["text"], '{"reply":"Ready"}')
        self.assertEqual(result["thread_id"], "thread-demo-1")
        self.assertEqual(result["usage"]["reasoning_output_tokens"], 30)

    def test_absent_reasoning_usage_is_unknown_not_zero(self):
        values = events()
        del values[-1]["usage"]["reasoning_output_tokens"]
        self.assertIsNone(runtime._parse_events(encoded(values))["usage"]["reasoning_output_tokens"])

    def test_reasoning_usage_is_reported_without_inferring_provider_accounting(self):
        values = events()
        values[-1]["usage"]["reasoning_output_tokens"] = 1000
        self.assertEqual(runtime._parse_events(encoded(values))["usage"]["reasoning_output_tokens"], 1000)

    def test_nonfinal_commentary_does_not_replace_final(self):
        values = events()
        values.insert(2, {"type": "item.completed", "item": {"type": "agent_message", "text": "Searching documentation"}})
        self.assertEqual(runtime._parse_events(encoded(values))["text"], '{"reply":"Ready"}')

    def test_invalid_counters_fail_closed(self):
        for key, value in [("input_tokens", True), ("output_tokens", -1), ("cached_input_tokens", 301), ("output_tokens", 2**63), ("reasoning_output_tokens", 2**63), ("reasoning_output_tokens", "1")]:
            with self.subTest(key=key, value=value):
                values = events()
                values[-1]["usage"][key] = value
                with self.assertRaises(runtime.CodexError):
                    runtime._parse_events(encoded(values))

    def test_non_documentation_tools_are_rejected(self):
        for kind in ("command_execution", "file_change", "web_search", "collab_tool_call", "todo_list"):
            values = events()
            values[2]["item"]["type"] = kind
            with self.subTest(kind=kind), self.assertRaisesRegex(runtime.CodexError, "outside"):
                runtime._parse_events(encoded(values))
        for key, value in [("server", "another_server"), ("tool", "shell")]:
            values = events()
            values[2]["item"][key] = value
            with self.subTest(key=key), self.assertRaisesRegex(runtime.CodexError, "outside"):
                runtime._parse_events(encoded(values))

    def test_failed_tools_and_provider_errors_use_safe_messages(self):
        values = events()
        values[3]["item"].update(status="failed", error={"message": "private-secret"})
        for raw in [encoded(values), b'{"type":"error","message":"private-secret"}', b'{"type":"turn.failed","error":{"message":"private-secret"}}']:
            with self.subTest(raw=raw), self.assertRaises(runtime.CodexError) as caught:
                runtime._parse_events(raw)
            self.assertNotIn("private-secret", str(caught.exception))

    def test_missing_duplicate_or_misordered_terminal_events_are_rejected(self):
        valid = events()
        variants = [valid[:-1], valid[1:], valid + [valid[-1]], [valid[0], valid[-1], *valid[1:]], valid + [valid[4]]]
        for values in variants:
            with self.subTest(values=values), self.assertRaises(runtime.CodexError):
                runtime._parse_events(encoded(values))

    def test_invalid_json_encoding_duplicates_and_size_are_rejected(self):
        variants = [b'\xff', b'[]', b'{"type":"error","type":"thread.started"}', b'{"type":NaN}', encoded([events()[0]] * (runtime.MAX_EVENTS + 1))]
        values = events()
        values[4]["item"]["text"] = "x" * (runtime.MAX_FINAL_BYTES + 1)
        variants.append(encoded(values))
        for raw in variants:
            with self.subTest(length=len(raw)), self.assertRaises(runtime.CodexError):
                runtime._parse_events(raw)


class SubprocessTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="ledgence-codex-offline-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.binary = self.directory / "fake codex"
        self.tool = self.directory / "docs_server.py"
        self.tool.write_text("# This fixture is never invoked.\n")
        self.audit = self.directory / "audit.json"
        self.config = self.directory / "fixture.json"
        self.config.write_text(json.dumps({"mode": "success", "events": events()}))
        self.binary.write_text(f"#!{sys.executable}\n" + '''
import json, os, signal, sys, time
from pathlib import Path
fixture = json.loads(Path("fixture.json").read_text())
if sys.argv[1:] == ["--version"]:
    print(fixture.get("version", "codex-cli 0.158.0-alpha.2.1"))
    raise SystemExit(0)
Path("child.pid").write_text(str(os.getpid()))
if fixture["mode"] == "never_read":
    time.sleep(10)
prompt = sys.stdin.read()
Path("invocation.json").write_text(json.dumps({"args":sys.argv[1:], "env":dict(os.environ), "prompt":prompt, "group":os.getpgrp()}))
if fixture["mode"] == "timeout":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    time.sleep(10)
elif fixture["mode"] == "stdout_flood":
    os.write(1, b"x" * (2 * 1024 * 1024))
elif fixture["mode"] == "stderr_flood":
    os.write(2, b"private-secret" * (128 * 1024))
elif fixture["mode"] == "failure":
    print(fixture.get("stderr", "private-secret"), file=sys.stderr)
    for item in fixture.get("failure_events", []):
        print(json.dumps(item), flush=True)
    raise SystemExit(7)
else:
    for item in fixture["events"]:
        print(json.dumps(item), flush=True)
''')
        self.binary.chmod(0o700)
        self.environment = patch.dict(os.environ, {
            "LEDGENCE_CODEX_BIN": str(self.binary),
            "OPENAI_API_KEY": "private-openai-key", "GOOGLE_API_KEY": "private-google-key",
            "CODEX_ACCESS_TOKEN": "private-token", "DATABASE_URL": "private-database",
            "PYTHONPATH": "private-python-path", "CODEX_THREAD_ID": "private-thread",
        })
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def invoke(self, **changes):
        arguments = dict(prompt="Ticket with $(no shell) and `literal` text", schema={"type": "object"}, tool_server=self.tool, audit_path=self.audit, workdir=self.directory)
        arguments.update(changes)
        return runtime.run_codex(**arguments)

    def mode(self, name, **extra):
        self.config.write_text(json.dumps({"mode": name, "events": events(), **extra}))

    def assert_reaped(self):
        pid = int((self.directory / "child.pid").read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_real_offline_child_argv_and_environment_contract(self):
        result = self.invoke()
        self.assertEqual(result["cli_version"], "0.158.0-alpha.2.1")
        record = json.loads((self.directory / "invocation.json").read_text())
        self.assertEqual(record["prompt"], "Ticket with $(no shell) and `literal` text")
        self.assertEqual(record["group"], os.getpgrp())
        args = record["args"]
        for flag in ["--ignore-user-config", "--ignore-rules", "--ephemeral", "--skip-git-repo-check", "--strict-config", "--json"]:
            self.assertIn(flag, args)
        for setting in ['forced_login_method="chatgpt"', 'features.shell_tool=false', 'features.unbounded_connection_retries=false', 'features.code_mode=false']:
            self.assertIn(setting, args)
        self.assertIn('mcp_servers.ledgence_docs.omit_tools_from=["deferred", "code_mode"]', args)
        self.assertIn('mcp_servers.ledgence_docs.enabled_tools=["read_doc", "search_docs"]', args)
        for key in ["OPENAI_API_KEY", "GOOGLE_API_KEY", "CODEX_ACCESS_TOKEN", "DATABASE_URL", "PYTHONPATH", "CODEX_THREAD_ID"]:
            self.assertNotIn(key, record["env"])
        self.assertFalse((self.directory / "codex-output-schema.json").exists())
        self.assert_reaped()

    def test_failure_never_exposes_stderr_and_reaps_child(self):
        self.mode("failure")
        with self.assertRaises(runtime.CodexError) as caught:
            self.invoke()
        self.assertNotIn("private-secret", str(caught.exception))
        self.assertEqual(caught.exception.category, "cli_exit")
        self.assert_reaped()

    def test_nonzero_exit_reports_only_the_recognized_category(self):
        self.mode("failure", stderr="INFO authentication complete; private-secret", failure_events=[{"type": "turn.failed", "error": {"message": "MCP server failed: private-secret"}}])
        with self.assertRaises(runtime.CodexError) as caught:
            self.invoke()
        self.assertEqual(caught.exception.category, "mcp")
        self.assertNotIn("private-secret", str(caught.exception))
        self.assertNotIn("private-secret", caught.exception.category)
        self.assertFalse((self.directory / "codex-output-schema.json").exists())
        self.assert_reaped()

    def test_output_bounds_both_streams(self):
        for mode in ["stdout_flood", "stderr_flood"]:
            self.mode(mode)
            with self.subTest(mode=mode), self.assertRaisesRegex(runtime.CodexError, "size limit"):
                self.invoke()
            self.assert_reaped()

    def test_timeout_kills_unresponsive_child(self):
        self.mode("timeout")
        started = time.monotonic()
        with self.assertRaisesRegex(runtime.CodexError, "execution budget"):
            self.invoke(timeout=0.2)
        self.assertLess(time.monotonic() - started, 4)
        self.assert_reaped()

    def test_deadline_applies_while_child_refuses_stdin(self):
        self.mode("never_read")
        with self.assertRaisesRegex(runtime.CodexError, "execution budget"):
            self.invoke(timeout=0.2, prompt="x" * runtime.MAX_PROMPT_BYTES)
        self.assert_reaped()

    def test_cancellation_reaps_inherited_group_child(self):
        original = runtime.selectors.DefaultSelector.select
        count = 0
        def interrupt(selector, timeout=None):
            nonlocal count
            count += 1
            # Version preflight completes before the fake generation records PID.
            if (self.directory / "child.pid").exists():
                raise asyncio.CancelledError
            return original(selector, timeout)
        self.mode("timeout")
        with patch.object(runtime.selectors.DefaultSelector, "select", interrupt):
            with self.assertRaises(asyncio.CancelledError):
                self.invoke()
        self.assertGreater(count, 0)
        self.assert_reaped()

    def test_existing_schema_is_preserved(self):
        schema = self.directory / "codex-output-schema.json"
        schema.write_text("original")
        with self.assertRaises(runtime.CodexError):
            self.invoke()
        self.assertEqual(schema.read_text(), "original")

    def test_invalid_schema_does_not_create_files_or_start_generation(self):
        for schema in [{"bad": object()}, {"bad": float("nan")}, {"bad": "\ud800"}, {"bad": "x" * runtime.MAX_FINAL_BYTES}]:
            with self.subTest(schema_type=type(schema["bad"])), self.assertRaisesRegex(runtime.CodexError, "schema"):
                self.invoke(schema=schema)
        self.assertFalse((self.directory / "codex-output-schema.json").exists())
        self.assertFalse((self.directory / "child.pid").exists())

    def test_invalid_cli_and_inputs_fail_before_generation(self):
        variants = [{"prompt": ""}, {"prompt": "\ud800"}, {"prompt": "x" * (runtime.MAX_PROMPT_BYTES + 1)}, {"timeout": float("nan")}, {"timeout": 0}, {"timeout": True}, {"timeout": 121}, {"model": "bad model"}, {"audit_path": self.directory.parent / "audit.json"}]
        for values in variants:
            with self.subTest(values=list(values)), self.assertRaises(runtime.CodexError):
                self.invoke(**values)
        for value in ["", "relative-codex", str(self.directory / "absent")]:
            with patch.dict(os.environ, {"LEDGENCE_CODEX_BIN": value}), self.assertRaises(runtime.CodexError):
                self.invoke()
        self.mode("success", version="other executable")
        with self.assertRaises(runtime.CodexError):
            self.invoke()
        self.assertFalse((self.directory / "child.pid").exists())


if __name__ == "__main__":
    unittest.main()
