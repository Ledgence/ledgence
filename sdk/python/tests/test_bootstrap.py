"""Executable protocol tests; no third-party packages required (MIT)."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


BOOTSTRAP = Path(__file__).resolve().parents[1] / "ledgence_worker" / "bootstrap.py"


class BootstrapTests(unittest.TestCase):
    def launch(self, source, messages, input_limit=4096, output_limit=4096,
               handler="program:handle", module="program.py", files=None):
        with tempfile.TemporaryDirectory() as package:
            for name, contents in {module: source, **(files or {})}.items():
                path = Path(package, name)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(contents)
            result = subprocess.run(
                [sys.executable, "-I", "-S", str(BOOTSTRAP),
                 "--package-root", package, "--handler", handler,
                 "--python-version", "%d.%d" % sys.version_info[:2],
                 "--max-input-bytes", str(input_limit),
                 "--max-output-bytes", str(output_limit)],
                input=b"".join(json.dumps(m).encode() + b"\n" for m in messages),
                cwd=package,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10,
            )
            self.assertFalse(list(Path(package).rglob("__pycache__")),
                             "bootstrap imports must not alter a writable artifact")
        frames = [json.loads(line) for line in result.stdout.splitlines()]
        return result, frames

    def invocation(self, event_id="evt-1", attempt_id="attempt-1", data=None):
        return {
            "v": 1, "type": "invoke", "event_id": event_id,
            "attempt_id": attempt_id,
            "event": {"specversion": "1.0", "source": "/tests", "id": event_id,
                      "type": "example.run", "subject": "order/42",
                      "ldgattemptid": attempt_id,
                      "traceparent": "00-" + "1" * 32 + "-" + "2" * 16 + "-01",
                      "customfield": "preserved", "data": data or {"value": 42}},
        }

    def test_reuses_process_preserves_event_and_clears_context(self):
        first = self.invocation()
        second = self.invocation("evt-2", "attempt-2")
        result, frames = self.launch(
            "import contextvars, os\n"
            "from ledgence_worker import current_invocation\n"
            "state = contextvars.ContextVar('state', default='clean')\n"
            "def handle(event):\n"
            "    previous = state.get()\n"
            "    state.set('dirty')\n"
            "    print('python log')\n"
            "    os.write(1, b'native log\\n')\n"
            "    return {'event': event, 'pid': os.getpid(), 'previous': previous,"
            " 'attempt': current_invocation().attempt_id}\n",
            [first, second, {"v": 1, "type": "shutdown"}],
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([f["type"] for f in frames], ["ready", "result", "result", "closing"])
        self.assertEqual(frames[1]["output"]["event"], first["event"])
        self.assertEqual(frames[2]["output"]["event"], second["event"])
        self.assertEqual(frames[1]["output"]["pid"], frames[2]["output"]["pid"])
        self.assertEqual(frames[2]["output"]["previous"], "clean")
        self.assertEqual(frames[2]["output"]["attempt"], "attempt-2")
        self.assertIn(b"python log", result.stderr)
        self.assertIn(b"native log", result.stderr)

    def test_business_error_keeps_session_usable(self):
        result, frames = self.launch(
            "def handle(event):\n"
            "    if event['id'] == 'evt-1': raise ValueError('expected failure')\n"
            "    return 42\n",
            [self.invocation(), self.invocation("evt-2", "attempt-2")],
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(frames[1]["error"]["kind"], "business_error")
        self.assertEqual(frames[2]["output"], 42)

    def test_invalid_outputs_are_typed_failures(self):
        for expression in ["float('nan')", "object()", "'x' * 5000"]:
            with self.subTest(expression=expression):
                result, frames = self.launch(
                    "def handle(event): return " + expression, [self.invocation()]
                )
                self.assertEqual(result.returncode, 0)
                self.assertEqual(frames[1]["error"]["kind"], "invalid_output")

    def test_coroutine_return_is_rejected_without_warning(self):
        result, frames = self.launch(
            "async def async_result(): return 1\n"
            "def handle(event): return async_result()\n", [self.invocation()]
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(frames[1]["status"], "error")
        self.assertNotIn(b"was never awaited", result.stderr)

    def test_handler_cannot_resolve_to_a_preloaded_module(self):
        for function in ("handle", "dumps"):
            with self.subTest(function=function):
                result, frames = self.launch(
                    "def " + function + "(event): return 'UPLOADED HANDLER'\n",
                    [self.invocation()], module="json.py", handler="json:" + function,
                )
                self.assertEqual(result.returncode, 70)
                self.assertEqual(frames, [])
                self.assertIn(b"conflicts with a preloaded module", result.stderr)

    def test_handler_must_exist_in_the_artifact(self):
        result, frames = self.launch(
            "def handle(event): return 1\n", [self.invocation()],
            handler="fractions:Fraction",
        )
        self.assertEqual(result.returncode, 70)
        self.assertEqual(frames, [])
        self.assertIn(b"not found in the artifact", result.stderr)

    def test_dotted_and_namespace_handlers_resolve_inside_the_artifact(self):
        for files in ({}, {"package/__init__.py": "", "package/sub/__init__.py": ""}):
            with self.subTest(namespace=not files):
                result, frames = self.launch(
                    "from .dependency import VALUE\ndef handle(event): return VALUE\n",
                    [self.invocation()], module="package/sub/program.py",
                    handler="package.sub.program:handle",
                    files={**files, "package/sub/dependency.py": "VALUE = 42\n"},
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(frames[1]["output"], 42)

    def test_wire_invalid_output_is_typed_and_the_process_remains_usable(self):
        expressions = [
            "{1: 'first', '1': 'second'}", "{1.5: 'first', '1.5': 'second'}",
            "'\\ud800'", "{'\\udfff': 1}", "1 << 64", "-(1 << 63) - 1",
            "10**400", "nested(65)", "float('inf')",
        ]
        for expression in expressions:
            with self.subTest(expression=expression):
                result, frames = self.launch(
                    "def nested(depth):\n"
                    "    value = 0\n"
                    "    for _ in range(depth): value = [value]\n"
                    "    return value\n"
                    "def handle(event):\n"
                    "    if event['id'] == 'evt-1': return " + expression + "\n"
                    "    return 42\n",
                    [self.invocation(), self.invocation("evt-2", "attempt-2")],
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(frames[1]["error"]["kind"], "invalid_output")
                self.assertEqual(frames[2]["output"], 42)

    def test_wire_boundaries_accept_unicode_numbers_and_64_containers(self):
        result, frames = self.launch(
            "def handle(event):\n"
            "    value = 0\n"
            "    for _ in range(64): value = [value]\n"
            "    if event['id'] == 'evt-1': return value\n"
            "    return [-(1 << 63), (1 << 64) - 1, 1.7976931348623157e308,"
            " '😀\\x00\\ufffe', {'': None}, (True, False)]\n",
            [self.invocation(), self.invocation("evt-2", "attempt-2")],
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        value = frames[1]["output"]
        for _ in range(64):
            self.assertIsInstance(value, list)
            value = value[0]
        self.assertEqual(value, 0)
        self.assertEqual(frames[2]["output"], [-(1 << 63), (1 << 64) - 1,
                         1.7976931348623157e308, "😀\x00\ufffe", {"": None}, [True, False]])

    def test_business_failure_fits_encoded_budget_and_allows_reuse(self):
        for message in ("x" * 500, '"\\😀' * 500, "\ud800" * 500):
            with self.subTest(message=ascii(message[:3])):
                result, frames = self.launch(
                    "def handle(event):\n"
                    "    if event['id'] == 'evt-1': raise ValueError(" + repr(message) + ")\n"
                    "    return 42\n",
                    [self.invocation(), self.invocation("evt-2", "attempt-2")],
                    output_limit=256,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(frames[1]["error"]["kind"], "business_error")
                self.assertEqual(frames[2]["output"], 42)
                self.assertTrue(all(len(line) + 1 <= 256 for line in result.stdout.splitlines()))

    def test_impossible_failure_identity_budget_rejects_before_handler(self):
        result, frames = self.launch(
            "def handle(event):\n    print('HANDLER_RAN')\n    return 1\n",
            [self.invocation("e" * 500, "a" * 500)], output_limit=256,
        )
        self.assertEqual(result.returncode, 70)
        self.assertEqual(len(frames), 1)
        self.assertNotIn(b"HANDLER_RAN", result.stderr)

    def test_mismatched_identity_terminates_protocol(self):
        message = self.invocation()
        message["event_id"] = "wrong"
        result, frames = self.launch("def handle(event): return 1", [message])
        self.assertEqual(result.returncode, 70)
        self.assertEqual(len(frames), 1)
        self.assertIn(b"identity", result.stderr)

    def test_input_limit_terminates_protocol(self):
        result, frames = self.launch(
            "def handle(event): return 1", [self.invocation(data={"blob": "x" * 5000})]
        )
        self.assertEqual(result.returncode, 70)
        self.assertEqual(len(frames), 1)

    def test_mismatched_attempt_terminates_protocol(self):
        message = self.invocation()
        message["attempt_id"] = "wrong"
        result, frames = self.launch("def handle(event): return 1", [message])
        self.assertEqual(result.returncode, 70)
        self.assertEqual(len(frames), 1)

    def test_shutdown_acknowledges_before_waiting_for_parent_termination(self):
        with tempfile.TemporaryDirectory() as package:
            Path(package, "program.py").write_text("def handle(event): return 1")
            with subprocess.Popen(
                [sys.executable, "-I", "-S", str(BOOTSTRAP),
                 "--package-root", package, "--handler", "program:handle",
                 "--python-version", "%d.%d" % sys.version_info[:2]],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                cwd=package,
            ) as process:
                self.assertEqual(json.loads(process.stdout.readline())["type"], "ready")
                process.stdin.write(b'{"v":1,"type":"shutdown"}\n')
                process.stdin.flush()
                self.assertEqual(json.loads(process.stdout.readline())["type"], "closing")
                self.assertIsNone(process.poll(), "child must await parent termination")
                process.kill()
                process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
