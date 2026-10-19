"""Release smoke validates generated task behavior, not only process exits."""
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import smoke


class StarterSmokeTests(unittest.TestCase):
    def exercise(self, *, wrong_output=False, reused=True):
        with tempfile.TemporaryDirectory() as temporary:
            bundle = Path(temporary) / "relocated"
            (bundle / "bin").mkdir(parents=True)
            (bundle / "bin/ledgence").touch()
            commands = []
            generated = "# The release must package the bytes the candidate scaffold generated.\n"

            def run(command, *, cwd, env, timeout, text):
                command = list(map(str, command))
                commands.append(command)
                if command[-1] == "--help":
                    return "Ledgence program worker orchestrator task approval mcp"
                if command[-1] == "--version":
                    return "ledgence 0.5.0"
                if command[1:3] == ["program", "new"]:
                    directory = Path(command[3])
                    directory.mkdir()
                    (directory / "program.py").write_text(generated)
                    return json.dumps({"directory": str(directory), "program": {"id": "smoke-task", "version": "0.1.0"}})
                if command[1:3] == ["program", "publish"]:
                    package = Path(command[command.index("--source") + 1])
                    self.assertEqual((package / "program.py").read_text(), generated)
                    manifest = json.loads((package / "ledgence-program.json").read_text())
                    self.assertEqual(manifest["runtime"]["protocol"], 3)
                    return "{}"
                if command[1:3] == ["worker", "run"]:
                    tasks = json.loads(Path(command[command.index("--tasks") + 1]).read_text())
                    self.assertEqual([task["event"]["data"] for task in tasks], [{"name": "Ada"}, {"name": "Grace"}])
                    self.assertEqual(len({task["event"]["ldgtaskid"] for task in tasks}), 2)
                    return "\n".join(json.dumps({"report": {"process_id": 71, "reused_process": reused,
                        "outcome": {"status": "success", "output": {"message": "incorrect" if wrong_output else f"Hello, {name}!"}}}})
                        for name in ("Ada", "Grace"))
                raise AssertionError(command)

            manifest = {"schema_version": 1, "program": {"id": "smoke-task", "version": "0.1.0"},
                        "runtime": {"kind": "python", "python": "3.14", "protocol": 3},
                        "handler": "program:handle", "platform": {"os": "linux", "arch": "x86_64"}}
            with patch.object(sys, "argv", ["smoke.py", "--directory", str(bundle)]), \
                    patch.object(smoke.subprocess, "check_output", side_effect=run), \
                    patch("program_fixture.host_manifest", return_value=manifest), \
                    patch.object(smoke, "console_smoke") as console, contextlib.redirect_stdout(io.StringIO()):
                smoke.main()
                console.assert_called_once()
            self.assertEqual(sum(command[1:3] == ["program", "new"] for command in commands), 1)
            self.assertFalse(any("docker" in command for command in commands))

    def test_generated_program_is_published_and_run(self):
        self.exercise()

    def test_success_status_without_correct_business_output_is_rejected(self):
        with self.assertRaises(AssertionError):
            self.exercise(wrong_output=True)

    def test_fresh_process_for_second_invocation_is_rejected(self):
        with self.assertRaises(AssertionError):
            self.exercise(reused=False)


if __name__ == "__main__":
    unittest.main()
