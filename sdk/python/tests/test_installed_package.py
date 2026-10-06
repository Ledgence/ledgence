"""Self-contained installed-wheel tests; no checkout or third-party fixtures (MIT).

After installing the wheel, set LEDGENCE_WORKER_INSTALLED_TESTS=1 and run this
suite outside the checkout with `python -I -m unittest discover -s tests -v`.
The repository build gate does this in a fresh virtual environment. Ordinary
source test discovery skips these installation-specific tests.
"""
import asyncio
import importlib
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


def activation(**changes):
    return {"v": 1, "workflow_id": "workflow-1", "activation_id": "activation-1",
            "revision": 0, "continuation": "start", "state": None,
            "inputs": {}, "local_steps": [], **changes}


@unittest.skipUnless(os.environ.get("LEDGENCE_WORKER_INSTALLED_TESTS") == "1",
                     "run by the isolated worker package gate")
class InstalledPackageTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.worker = importlib.import_module("ledgence.worker")
        self.workflow = importlib.import_module("ledgence.worker.workflow")
        self.helper = Path(self.worker.__file__).resolve().parent
        self.assertTrue(self.helper.is_relative_to(Path(sys.prefix).resolve()))
        self.assertNotEqual(sys.prefix, sys.base_prefix, "qualification requires a clean venv")
        self.assertIsNone(importlib.import_module("ledgence").__spec__.origin)

    def test_distribution_and_complete_module_origins(self):
        distribution = importlib.metadata.distribution("ledgence-worker")
        self.assertIsNone(distribution.metadata.get("Requires-Dist"))
        self.assertEqual(distribution.metadata["License-Expression"], "MIT")
        self.assertEqual(distribution.metadata["Requires-Python"], ">=3.11")
        self.assertTrue((self.helper / "py.typed").is_file())
        for name in ("_logging", "_observations", "_protocol", "bootstrap", "otel", "workflow"):
            module = importlib.import_module("ledgence.worker." + name)
            self.assertEqual(Path(module.__file__).resolve().parent, self.helper)
        self.assertIsNone(importlib.util.find_spec("ledgence.client"))
        self.assertNotIn("opentelemetry", sys.modules)
        self.assertNotIn("aiohttp", sys.modules)

    def test_invocation_and_optional_tracing_are_inactive_without_runtime(self):
        with self.assertRaisesRegex(RuntimeError, "no Ledgence invocation"):
            self.worker.current_invocation()
        with self.assertRaises(self.workflow.WorkflowError):
            self.workflow.workflow_context()
        otel = importlib.import_module("ledgence.worker.otel")
        self.assertIsNone(otel._api)
        with self.assertRaises(ModuleNotFoundError):
            otel.enable_context()
        self.assertIsNone(otel._api)

    async def test_checkpoint_freezes_state_and_staged_child_inputs(self):
        async def unexpected_rpc(*args):
            self.fail("checkpoint construction dispatched work")
        context = self.workflow.WorkflowContext(activation(), unexpected_rpc)
        self.addAsyncCleanup(context._finish, cancel=True)
        data, state = {"amount": 42}, {"phase": "waiting"}
        child = context.task("invoice", program="invoice", version="1.0", queue="billing", data=data)
        decision = context.suspend(continuation="collect", state=state, until=[child])
        data["amount"], state["phase"] = 99, "mutated"
        self.assertEqual(decision["kind"], "suspend")
        self.assertEqual(decision["state"], {"phase": "waiting"})
        self.assertEqual(decision["commands"][0]["data"], {"amount": 42})
        self.assertEqual(decision["until"], ["invoice"])
        with self.assertRaises(self.workflow.WorkflowError):
            context.complete(None)

    async def test_local_result_waits_for_ack_and_replays_without_execution(self):
        commits, calls = [], []
        entered, acknowledge = asyncio.Event(), asyncio.Event()

        async def operation(value):
            calls.append(value)
            return {"value": value}

        async def rpc(name, record):
            self.assertEqual(name, "local_step.commit")
            entered.set()
            await acknowledge.wait()
            commits.append(json.loads(json.dumps(record)))
            return {"committed": True}

        context = self.workflow.WorkflowContext(activation(), rpc)
        self.addAsyncCleanup(context._finish, cancel=True)
        pending = asyncio.ensure_future(context.local("read", operation, value=42))
        await asyncio.wait_for(entered.wait(), 2)
        self.assertFalse(pending.done())
        acknowledge.set()
        result = await asyncio.wait_for(pending, 2)
        result["value"] = "mutated"
        resumed = self.workflow.WorkflowContext(activation(local_steps=commits), rpc)
        self.addAsyncCleanup(resumed._finish, cancel=True)
        self.assertEqual(await resumed.local("read", operation, value=42), {"value": 42})
        self.assertEqual(calls, [42])
        self.assertEqual(len(commits), 1)
        with self.assertRaises(self.workflow.WorkflowError):
            resumed.local("read", operation, value=43)

    async def test_failed_ack_cannot_become_successful_completion(self):
        async def rpc(name, record):
            return {"committed": False}
        context = self.workflow.WorkflowContext(activation(), rpc)
        self.addAsyncCleanup(context._finish, cancel=True)
        with self.assertRaises(self.workflow.WorkflowError):
            await context.local("read", lambda: 42)
        with self.assertRaises(self.workflow.WorkflowError):
            context.complete("incorrect-success")

    def test_structured_log_snapshots_invocation_identity(self):
        logging = importlib.import_module("ledgence.worker._logging")

        class Sink:
            protocol_version = 3
            log_limit = 16384
            frames = []

            def offer_log(self, encoded):
                self.frames.append(json.loads(encoded))
                return True

            def drop_log(self):
                raise AssertionError("unexpected dropped log")

        previous = logging._sink
        self.addCleanup(setattr, logging, "_sink", previous)
        logging._sink = Sink()
        logger = self.worker.get_logger("installed-package-qualification")
        attributes = {"phase": "running"}
        token = self.worker._invocation.set(self.worker.InvocationContext(
            event_id="evt", attempt_id="attempt", workflow_id="workflow", activation_id="activation"))
        try:
            logger.info("checkpoint %s", "saved", extra={"attributes": attributes})
        finally:
            self.worker._invocation.reset(token)
        attributes["phase"] = "mutated"
        self.assertEqual(len(logging._sink.frames), 1)
        frame = logging._sink.frames[0]
        self.assertEqual(frame["message"], "checkpoint saved")
        self.assertEqual(frame["attributes"], {"phase": "running"})
        self.assertEqual(frame["invocation"], {"event_id": "evt", "attempt_id": "attempt",
                                              "workflow_id": "workflow", "activation_id": "activation"})

    def test_installed_bytes_in_dedicated_isolated_runtime(self):
        # Never use site-packages itself as the runtime helper root. The Rust
        # worker delivers a dedicated helper; reproduce that layout from the
        # installed wheel's bytes, without pulling unrelated venv packages in.
        with tempfile.TemporaryDirectory(prefix="installed-worker-runtime-") as directory:
            root = Path(directory).resolve()
            helper = root / "helper/ledgence/worker"
            shutil.copytree(self.helper, helper, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
            for path in helper.iterdir():
                self.assertEqual(path.read_bytes(), (self.helper / path.name).read_bytes())
            program = root / "program"
            program.mkdir()
            (program / "program.py").write_text('''import asyncio, os
from pathlib import Path
import ledgence, ledgence.worker
from ledgence.worker import current_invocation, get_logger, register_shutdown
from ledgence.worker.workflow import workflow_context, WorkflowError
assert ledgence.__spec__.origin is None
assert Path(ledgence.worker.__file__).resolve().is_relative_to(Path(__file__).resolve().parent.parent / "helper")
logger = get_logger("installed-runtime")
register_shutdown(lambda: print("shutdown-callback"))
async def local(value):
    return {"value": value}
async def handle_async(event):
    current = current_invocation()
    assert current.event_id == event["id"]
    assert current.attempt_id == event["ldgattemptid"]
    logger.info("installed runtime invocation")
    result = {"event": event, "pid": os.getpid()}
    if "ldgworkflowid" in event:
        context = workflow_context()
        assert current.workflow_id == context.workflow_id
        result["local"] = await context.local("read", local, value=42)
        return context.complete(result)
    try:workflow_context()
    except WorkflowError:pass
    else:raise AssertionError("workflow context leaked into ordinary invocation")
    return result
def handle(event):
    return asyncio.run(handle_async(event))
''')
            for version in (1, 2, 3):
                with self.subTest(protocol=version):
                    messages = []
                    for number in (1, 2):
                        event = {"specversion": "1.0", "id": f"evt-{number}", "source": "/installed-test",
                                 "type": "test.invoke", "ldgattemptid": f"attempt-{number}", "data": {"value": 42}}
                        message = {"v": version, "type": "invoke", "event_id": event["id"],
                                   "attempt_id": event["ldgattemptid"], "event": event}
                        if version >= 2:
                            message["processing_context"] = None
                        if version == 3 and number == 1:
                            event.update(ldgworkflowid="workflow-1", ldgactivationid="activation-1", ldgtaskid="activation-1")
                            message["extension"] = {"schema": self.workflow.SCHEMA, "payload": activation()}
                        messages.append(message)
                        if version == 3 and number == 1:
                            messages.append({"v": 3, "type": "runtime_reply", "id": 1,
                                             "event_id": "evt-1", "attempt_id": "attempt-1", "result": {"committed": True}})
                    messages.append({"v": version, "type": "shutdown"})
                    result = subprocess.run(
                        [sys.executable, "-I", "-S", "-B", str(helper / "bootstrap.py"),
                         "--package-root", str(program), "--handler", "program:handle_async" if version == 3 else "program:handle",
                         "--python-version", "%d.%d" % sys.version_info[:2], "--protocol-version", str(version)],
                        cwd=root, input=b"".join(json.dumps(m).encode() + b"\n" for m in messages),
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
                    self.assertEqual(result.returncode, 0, result.stderr.decode(errors="replace"))
                    frames = [json.loads(line) for line in result.stdout.splitlines()]
                    controls = [frame for frame in frames if frame["type"] != "log"]
                    expected = ["ready", "runtime_request", "result", "result", "closing"] if version == 3 else ["ready", "result", "result", "closing"]
                    self.assertEqual([frame["type"] for frame in controls], expected)
                    if version == 3:
                        self.assertEqual(controls[1]["operation"], "local_step.commit")
                        self.assertEqual(controls[1]["payload"]["output"], {"value": 42})
                    outputs = []
                    for number, frame in enumerate((frame for frame in controls if frame["type"] == "result"), 1):
                        self.assertEqual(frame["status"], "success", frame)
                        output = frame["output"]
                        if version == 3 and number == 1:
                            self.assertEqual(output["kind"], "complete")
                            output = output["output"]
                            self.assertEqual(output["local"], {"value": 42})
                        self.assertEqual(output["event"]["id"], f"evt-{number}")
                        outputs.append(output)
                    self.assertEqual([output["pid"] for output in outputs], [controls[0]["pid"]] * 2)
                    if version >= 2:
                        self.assertIn(b"shutdown-callback", result.stderr)
                    self.assertFalse(list(root.rglob("__pycache__")))


if __name__ == "__main__":
    unittest.main()
