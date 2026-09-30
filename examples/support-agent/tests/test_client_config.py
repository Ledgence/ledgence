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
            for options, expected in (([], "support-demo"), (["--queue", "support-custom"], "support-custom")):
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

    def test_credentials_are_read_as_data_and_only_in_worker_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "key.env"
            # Authorization-key syntax is not limited to legacy alphanumeric API keys.
            path.write_text('# local test value\nGOOGLE_API_KEY="test.auth-key_value"\nGOOGLE_GENAI_USE_VERTEXAI=FALSE\n')
            inherited = {"PATH": "/usr/bin"}
            environment = worker.worker_environment(path, inherited)
            self.assertEqual(environment["GOOGLE_API_KEY"], "test.auth-key_value")
            self.assertEqual(inherited, {"PATH": "/usr/bin"})
            self.assertEqual(environment["GOOGLE_GENAI_USE_VERTEXAI"], "FALSE")
            path.write_text('GOOGLE_API_KEY=$(never-execute)\n')
            self.assertEqual(worker.worker_environment(path, {})["GOOGLE_API_KEY"], "$(never-execute)")

    def test_invalid_secret_configuration_never_echoes_its_contents(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "key.env"
            for content in ('GOOGLE_API_KEY=\n', 'GOOGLE_API_KEY=secret with spaces\n',
                            'GOOGLE_API_KEY=test-only\nGOOGLE_API_KEY=another\n',
                            'GOOGLE_API_KEY=test-only\nGOOGLE_GENAI_USE_VERTEXAI=true\n',
                            'GOOGLE_API_KEY=test-only\nARBITRARY=secret-value\n'):
                path.write_text(content)
                with self.assertRaises(ValueError) as error:
                    worker.worker_environment(path, {})
                self.assertNotIn("test-only", str(error.exception))
                self.assertNotIn("secret-value", str(error.exception))


if __name__ == "__main__":
    unittest.main()
