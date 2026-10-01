"""Demo input, approval identity and private configuration regressions."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


def load(name):
    path = Path(__file__).resolve().parents[1] / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"support_demo_{name}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


client = load("client")
worker = load("run_worker")


class ClientConfigTests(unittest.TestCase):
    def test_review_reconstruction_preserves_exact_command_identity(self):
        expected = client.review_event("SUP-1", "task_draft", True, "review-1")
        self.assertEqual(expected, client.review_event("SUP-1", "task_draft", True, "review-1"))
        self.assertEqual(expected["data"], {"ticket_id": "SUP-1", "draft_task_id": "task_draft", "approved": True})
        self.assertNotEqual(expected, client.review_event("SUP-1", "task_draft", False, "review-1"))
        self.assertNotIn("time", expected)

    def test_ticket_uses_user_fields_only(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ticket.json"
            ticket = {"ticket_id": "SUP-1", "question": "How do I recover a result?"}
            path.write_text(json.dumps(ticket))
            self.assertEqual(client.read_ticket(path), ticket)
            for invalid in ({**ticket, "GOOGLE_API_KEY": "test-only"}, {**ticket, "question": ""},
                            {**ticket, "question": "é" * 4097}, [ticket]):
                path.write_text(json.dumps(invalid, ensure_ascii=False))
                with self.assertRaises(ValueError):
                    client.read_ticket(path)

    def test_worker_cli_forwards_default_and_custom_queue_to_connected_worker(self):
        class ExecIntercepted(RuntimeError):
            pass

        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "prepared.json").write_text("{}")
            for options, expected in (([], "codex-support-demo"), (["--queue", "support-custom"], "support-custom")):
                with self.subTest(queue=expected), \
                        patch.object(worker, "worker_environment", return_value={"DEMO_TEST": "yes"}), \
                        patch.object(worker.sys, "version_info", (3, 13, 0)), \
                        patch.object(worker.os, "execve", side_effect=ExecIntercepted) as execute:
                    with self.assertRaises(ExecIntercepted):
                        worker.main(["--directory", directory, *options])
                    executable, arguments, environment = execute.call_args.args
                    self.assertEqual(arguments.count("--queue"), 1)
                    self.assertEqual(arguments[arguments.index("--queue") + 1], expected)
                    self.assertEqual(arguments[arguments.index("--concurrency") + 1], "1")
                    self.assertEqual(executable, arguments[0])
                    self.assertEqual(Path(executable).name, "ledgence")
                    self.assertEqual(arguments[1:3], ["worker", "connect"])
                    self.assertEqual(environment, {"DEMO_TEST": "yes"})

    def test_codex_path_is_explicit_and_provider_api_keys_are_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "codex"
            path.write_text("#!/bin/sh\nexit 0\n")
            path.chmod(0o700)
            inherited = {"PATH": "/usr/bin", "CODEX_HOME": "/private/auth-not-read",
                         "OPENAI_API_KEY": "test-only", "GOOGLE_API_KEY": "test-google",
                         "GEMINI_API_KEY": "test-gemini"}
            environment = worker.worker_environment(path, inherited)
            self.assertEqual(environment["LEDGENCE_CODEX_BIN"], str(path.resolve()))
            self.assertEqual(environment["CODEX_HOME"], "/private/auth-not-read")
            self.assertFalse(any(name.endswith("API_KEY") for name in environment))
            self.assertEqual(inherited["OPENAI_API_KEY"], "test-only")
            self.assertEqual(worker.worker_environment(None, {"LEDGENCE_CODEX_BIN": str(path)})[
                "LEDGENCE_CODEX_BIN"], str(path.resolve()))

    def test_invalid_codex_path_does_not_echo_its_contents_or_search_path(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "not-executable-private-path"
            path.touch()
            for configured in (None, Path("relative-private-path"), path, path.parent,
                               path.parent / "missing-private-path"):
                with self.subTest(configured=configured), self.assertRaises(ValueError) as error:
                    worker.worker_environment(configured, {"PATH": directory})
                self.assertNotIn("private-path", str(error.exception))

    def test_explicit_path_takes_precedence_without_reading_authentication_storage(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "codex"
            path.touch()
            path.chmod(0o700)
            with patch.object(Path, "read_text", side_effect=AssertionError("must not read login files")):
                environment = worker.worker_environment(path, {
                    "LEDGENCE_CODEX_BIN": "/missing/stale-cli", "CODEX_HOME": "/private/auth"})
            self.assertEqual(environment["LEDGENCE_CODEX_BIN"], str(path.resolve()))


if __name__ == "__main__":
    unittest.main()
