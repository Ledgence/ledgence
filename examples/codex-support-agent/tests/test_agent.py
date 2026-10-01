"""Offline agent contract tests; no Codex invocation or model connection."""

import hashlib
import importlib.util
import json
from pathlib import Path
import socket
import stat
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

AGENT = Path(__file__).resolve().parents[1] / "agent" / "program.py"
SPEC = importlib.util.spec_from_file_location("codex_support_agent_program", AGENT)
program = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(program)


def event(**changes):
    return {"data": {"ticket_id": "ticket-1", "question": "Does result timeout cancel my task?", **changes}}


def draft(**changes):
    return {"classification": "how_to", "reply": "The observation timeout does not cancel a task. [task-results]",
            "source_ids": ["task-results"], **changes}


def audit(**changes):
    return {"version": 1, "tool_calls": 2, "searched": True, "read_ids": ["task-results"], "exhausted": False, **changes}


def result(**changes):
    return {"text": json.dumps(draft()), "cli_version": "0.125.0", "thread_id": "thread-123",
            "usage": {"input_tokens": 100, "cached_input_tokens": 20, "output_tokens": 30,
                      "reasoning_output_tokens": None}, **changes}


class AgentTests(unittest.TestCase):
    def test_default_model_and_unicode_ticket(self):
        ticket = program.ticket_input(event(question="Una pregunta\ncon Unicode café\ty tab"))
        self.assertEqual(ticket["model"], "gpt-6-luna")

    def test_invalid_input_never_starts_runtime(self):
        invalid = [None, {}, {"data": None}, {"data": {}}, event(ticket_id="two words"),
                   event(ticket_id="é"), event(ticket_id="x" * 129), event(question=" "),
                   event(question="é" * 4097), event(question="x\x00"), event(question="x\x85"),
                   event(question="x\ufdd0"), event(question="\ud800"), event(model=""),
                   event(model="x\n"), event(model="x" * 129), event(api_key="not-accepted")]
        with patch.object(program, "_run_codex") as runtime:
            for value in invalid:
                with self.subTest(value=repr(value)[:60]), self.assertRaises(program.AgentError):
                    program.handle(value)
            runtime.assert_not_called()

    def test_current_workflow_and_client_guides_are_searchable_and_readable(self):
        docs = program.Documentation()
        matches = docs.search_docs("typed entrypoints durable forks")["matches"]
        self.assertIn("workflow-entrypoints", [item["id"] for item in matches])
        guide = docs.read_doc("workflow-entrypoints")
        self.assertIn("await ctx.fork(", guide["text"])
        self.assertIn("ctx.join(", guide["text"])
        self.assertIn("@workflow.entrypoint", docs.read_doc("workflows")["text"])
        self.assertIn("WaitTimeout", docs.read_doc("python-client")["text"])

    def test_corpus_matches_recorded_source_bytes(self):
        manifest = json.loads((program.CORPUS / "SOURCE.json").read_text())
        self.assertRegex(manifest["source_revision"], r"^[a-f0-9]{40}$")
        self.assertEqual(set(manifest["documents"]), set(program.DOCUMENTS))
        for key, entry in manifest["documents"].items():
            self.assertEqual(entry["location"], program.DOCUMENTS[key]["location"])
            self.assertEqual(entry["sha256"], hashlib.sha256((program.CORPUS / f"{key}.md").read_bytes()).hexdigest())

    def test_documentation_search_read_and_sdk_excerpt(self):
        docs = program.Documentation()
        self.assertIn("task-results", [entry["id"] for entry in docs.search_docs("task result timeout")["matches"]])
        self.assertIn("timeout", docs.read_doc("task-results")["text"])
        self.assertIn("AsyncClient", docs.read_doc("python-client")["text"])
        self.assertEqual(docs.audit(), audit(tool_calls=3, read_ids=["python-client", "task-results"]))

    def test_tools_count_invalid_attempts_and_stop_before_ninth(self):
        docs = program.Documentation()
        with self.assertRaisesRegex(program.AgentError, "Search"):
            docs.read_doc("task-results")
        docs.search_docs("task")
        for key in ("../program.py", "/etc/passwd", None):
            with self.assertRaisesRegex(program.AgentError, "Unknown"):
                docs.read_doc(key)
        for _ in range(3):
            docs.search_docs("task")
        with self.assertRaisesRegex(program.AgentError, "budget"):
            docs.search_docs("task")
        self.assertEqual(docs.tool_calls, 8)
        self.assertTrue(docs.exhausted)
        self.assertFalse(docs.read_ids)

    def run_fake(self, outcome=None, evidence=None):
        paths = []
        def runtime(prompt, schema, tool_server, audit_path, workdir, *, timeout, model):
            self.assertIn('"ticket_id": "ticket-1"', prompt)
            self.assertIn("ledgence_docs", prompt)
            self.assertEqual(schema, program.OUTPUT_SCHEMA)
            self.assertEqual(tool_server, AGENT.with_name("docs_server.py"))
            self.assertEqual((timeout, model), (120, "gpt-6-luna"))
            self.assertEqual(stat.S_IMODE(workdir.stat().st_mode), 0o700)
            self.assertEqual(audit_path.parent, workdir)
            paths.append(workdir)
            if evidence is not False:
                audit_path.write_text(json.dumps(audit() if evidence is None else evidence))
            if isinstance(outcome, BaseException):
                raise outcome
            return result() if outcome is None else outcome
        with patch.object(program, "_run_codex", side_effect=runtime) as call, \
                patch.object(socket.socket, "connect", side_effect=AssertionError("No network")) as network:
            try:
                return program.handle(event())
            finally:
                self.assertEqual(call.call_count, 1)
                network.assert_not_called()
                self.assertTrue(paths)
                self.assertTrue(all(not path.exists() for path in paths))

    def test_success_uses_private_per_attempt_directory_and_canonical_metadata(self):
        for _ in range(2):
            output = self.run_fake()
            self.assertEqual(output["sources"], [program.DOCUMENTS["task-results"]])
            self.assertEqual(output["execution"], {
                "provider": "codex", "cli_invocations": 1, "cli_version": "0.125.0", "thread_id": "thread-123",
                "tool_calls": 2, "usage": result()["usage"],
            })
            self.assertNotIn("model_calls", output)
            self.assertNotIn("http_attempts", output)

    def test_success_without_tool_evidence_is_rejected(self):
        with self.assertRaisesRegex(program.AgentError, "evidence"):
            self.run_fake(evidence=False)

    def test_audit_rejects_impossible_unread_and_exhausted_evidence(self):
        invalid = [[], {}, audit(version=True), audit(tool_calls=True), audit(tool_calls=1), audit(tool_calls=9),
                   audit(searched=False), audit(read_ids=[]), audit(read_ids=["fabricated"]),
                   audit(read_ids=["task-results", "task-results"]), audit(read_ids=[{}]),
                   audit(tool_calls=2, read_ids=["task-results", "python-client"]), audit(extra=True),
                   audit(tool_calls=8, exhausted=True), audit(exhausted=0)]
        for value in invalid:
            with self.subTest(value=value), self.assertRaisesRegex(program.AgentError, "evidence"):
                self.run_fake(evidence=value)

    def test_audit_file_is_bounded_and_strict_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.json"
            for raw in (b"x" * 8193, b'\xff', b'{"version":1,"version":1}', b'{"version":NaN}'):
                path.write_bytes(raw)
                with self.assertRaises(program.AgentError):
                    program.read_audit(path)

    def test_audit_failure_diagnostics_distinguish_missing_tool_progress(self):
        cases = (
            (audit(tool_calls=0, searched=False, read_ids=[]), "contains no calls"),
            (audit(tool_calls=2, searched=False, read_ids=[]), "no successful search"),
            (audit(tool_calls=2, read_ids=[]), "no successful document read"),
            (audit(tool_calls=8, exhausted=True), "exhausted budget"),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.json"
            for value, expected in cases:
                path.write_text(json.dumps(value))
                with self.subTest(expected=expected), self.assertRaisesRegex(program.AgentError, expected) as failure:
                    program.read_audit(path)
                self.assertIn(f"tool_calls={value['tool_calls']}", str(failure.exception))
                self.assertIn(f"searched={value['searched']}", str(failure.exception))
                self.assertEqual(program.read_audit(path, complete=False), value)

    def test_invalid_audit_never_exposes_unvalidated_values_in_diagnostics(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.json"
            for value in (audit(tool_calls="private-request-body"), audit(read_ids=["private-ticket"]),
                          audit(searched="private-provider-output"), audit(extra="private-credential")):
                path.write_text(json.dumps(value))
                with self.subTest(value=value), self.assertRaisesRegex(program.AgentError, "invalid structure") as failure:
                    program.read_audit(path)
                self.assertNotIn("private", str(failure.exception))
                self.assertNotIn("tool_calls=", str(failure.exception))

    def test_draft_rejects_bad_json_schema_and_unread_sources(self):
        invalid = ["not JSON", "```json\n{}\n```", '{"classification":"how_to","classification":"how_to"}']
        invalid += [json.dumps(value) for value in (
            [], draft(extra=True), draft(classification=[]), draft(classification="approved"),
            draft(reply=""), draft(reply="é" * 4097), draft(reply="x\x00"), draft(reply="\ud800"),
            draft(source_ids=[]), draft(source_ids=["python-client"]), draft(source_ids=[{}]),
            draft(source_ids=["task-results", "task-results"]), draft(source_ids="task-results"),
        )]
        for raw in invalid:
            with self.subTest(raw=raw[:100]), self.assertRaises(program.AgentError):
                self.run_fake(result(text=raw))

    def test_metadata_rejects_missing_usage_and_invalid_counter_types(self):
        invalid = [{}, result(extra=True), result(cli_version=""), result(cli_version="x\n"), result(thread_id="two words")]
        for key in result()["usage"]:
            for value in (-1, True, 1.5, "1"):
                usage = result()["usage"]
                usage[key] = value
                invalid.append(result(usage=usage))
        usage = result()["usage"]
        del usage["reasoning_output_tokens"]
        invalid.extend((result(usage=usage), result(usage={**result()["usage"], "cached_input_tokens": 101})))
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(program.AgentError):
                self.run_fake(value)

    def test_inline_citations_must_match_declared_read_documents(self):
        for reply in ("No citation", "Unsupported source [workflows]", "Unknown [fictional-doc]",
                      "Extra source [task-results] [python-client]"):
            with self.subTest(reply=reply), self.assertRaisesRegex(program.AgentError, "citations"):
                self.run_fake(result(text=json.dumps(draft(reply=reply))))
        output = self.run_fake(result(text=json.dumps(draft(reply="Read item[0]. [task-results]"))))
        self.assertIn("item[0]", output["reply"])

    def test_generic_runtime_error_retires_and_directory_is_removed(self):
        with self.assertRaisesRegex(SystemExit, "retiring the worker session") as raised:
            self.run_fake(RuntimeError("private provider text"))
        self.assertNotIn("private", str(raised.exception))

    def test_codex_failure_retires_worker_session_instead_of_reusing_it(self):
        class CodexError(ValueError):
            pass
        fake = SimpleNamespace(CodexError=CodexError, run_codex=lambda *args, **kwargs: (_ for _ in ()).throw(CodexError("private CLI output")))
        with patch.dict(sys.modules, {"codex_runtime": fake}):
            with self.assertRaisesRegex(SystemExit, "retiring the worker session") as raised:
                program.handle(event())
        self.assertNotIn("private", str(raised.exception))

    def test_unclassified_runtime_and_cleanup_errors_also_retire_worker_session(self):
        for failure in (OSError("private OS detail"), subprocess.SubprocessError("private child detail"),
                        subprocess.TimeoutExpired("private child command", 2)):
            def fail(*args, **kwargs):
                raise failure
            fake = SimpleNamespace(run_codex=fail)
            with self.subTest(failure=type(failure).__name__), patch.dict(sys.modules, {"codex_runtime": fake}):
                with self.assertRaisesRegex(SystemExit, "retiring the worker session") as raised:
                    program.handle(event())
                self.assertNotIn("private", str(raised.exception))

    def test_only_fixed_runtime_categories_survive_retirement(self):
        for category, expected in (("mcp", "mcp"), ("private-provider-detail", "runtime"), ([], "runtime")):
            failure = ValueError("private CLI output")
            failure.category = category
            def fail(*args, **kwargs):
                raise failure
            fake = SimpleNamespace(run_codex=fail, ERROR_CATEGORIES=frozenset({"runtime", "mcp"}))
            with self.subTest(category=category), patch.dict(sys.modules, {"codex_runtime": fake}):
                with self.assertRaises(SystemExit) as raised:
                    program.handle(event())
                self.assertIn(f"({expected})", str(raised.exception))
                self.assertNotIn("private", str(raised.exception))

    def test_temporary_directory_cleanup_cannot_downgrade_session_retirement(self):
        create_directory = tempfile.TemporaryDirectory
        class FailingCleanup:
            def __init__(self, **kwargs):
                self.directory = create_directory(**kwargs)
            def __enter__(self):
                return self.directory.__enter__()
            def __exit__(self, *error):
                self.directory.__exit__(*error)
                raise OSError("private directory cleanup detail")
        def fail(*args, **kwargs):
            raise OSError("private child cleanup detail")
        with patch.dict(sys.modules, {"codex_runtime": SimpleNamespace(run_codex=fail)}), \
                patch.object(program.tempfile, "TemporaryDirectory", FailingCleanup):
            with self.assertRaisesRegex(SystemExit, "retiring the worker session") as raised:
                program.handle(event())
        self.assertNotIn("private", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
