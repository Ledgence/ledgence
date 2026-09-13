"""v2 frame, isolation, logging, and backpressure acceptance tests (MIT)."""

import contextvars
import io
import json
import logging
import os
from pathlib import Path
import sys
import threading
import time
import unittest

import test_bootstrap

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ledgence_worker import InvocationContext, TraceContext, _invocation, get_logger
from ledgence_worker import _logging
from ledgence_worker._protocol import MAX_LOG_BYTES, MAX_LOG_RECORDS, ProtocolWriter

TRACE = "00-4bf92f3577b34da6a3ce929d0e0e4736-0123456789abcdef-01"


class V2Tests(unittest.TestCase):
    launch = test_bootstrap.BootstrapTests.launch

    def invocation(self, identity="1", processing=TRACE):
        message = test_bootstrap.BootstrapTests.invocation(self, "evt-" + identity, "attempt-" + identity)
        message["v"] = 2
        message["processing_context"] = None if processing is None else {
            "traceparent": processing, "tracestate": "vendor=value"}
        message["event"].update(ldgtenantid="tenant", ldgnamespace="billing", ldgrunid="run",
                                ldgtaskid="task-" + identity, ldgattemptno=1)
        return message

    def test_context_origin_preservation_reuse_and_failure_reset(self):
        first, second, third = self.invocation(), self.invocation("2"), self.invocation("3", None)
        result, frames = self.launch(
            "import contextvars, os, sys\n"
            "from dataclasses import asdict\n"
            "from ledgence_worker import current_invocation\n"
            "state = contextvars.ContextVar('state', default='clean')\n"
            "def handle(event):\n"
            "    old = state.get()\n"
            "    state.set('dirty')\n"
            "    if event['id'] == 'evt-1': raise ValueError('first')\n"
            "    return {'ctx': asdict(current_invocation()), 'event': event, 'old': old,"
            " 'pid': os.getpid(), 'otel_imported': 'opentelemetry' in sys.modules}\n",
            [first, second, third, {"v": 2, "type": "shutdown"}], protocol=2)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(frames[1]["error"]["kind"], "business_error")
        a, b = frames[2]["output"], frames[3]["output"]
        self.assertEqual(a["event"], second["event"])
        self.assertEqual(a["ctx"]["processing_context"]["traceparent"], TRACE)
        self.assertEqual(a["ctx"]["task_id"], "task-2")
        self.assertEqual(a["old"], "clean")
        self.assertEqual(a["pid"], b["pid"])
        self.assertIsNone(b["ctx"]["processing_context"])
        self.assertFalse(b["otel_imported"])

    def test_invalid_processing_carrier_rejected_before_handler(self):
        for carrier in ({}, {"traceparent": "bad"}, {"traceparent": "00-" + "0" * 32 + "-" + "1" * 16 + "-01"},
                        {"traceparent": TRACE, "tracestate": "same=1,same=2"},
                        {"traceparent": TRACE, "unknown": 1}, "string"):
            with self.subTest(carrier=carrier):
                message = self.invocation()
                message["processing_context"] = carrier
                result, frames = self.launch(
                    "def handle(event): print('RAN'); return 1", [message], protocol=2)
                self.assertEqual(result.returncode, 70)
                self.assertEqual(len(frames), 1)
                self.assertNotIn(b"RAN", result.stderr)
        message = self.invocation()
        del message["processing_context"]
        result, _ = self.launch("def handle(event): return 1", [message], protocol=2)
        self.assertEqual(result.returncode, 70)

    def test_logs_never_change_business_result_and_control_frames_are_complete(self):
        result, frames = self.launch(
            "import threading, time\nfrom ledgence_worker import get_logger\n"
            "log = get_logger('example')\n"
            "log.info('before handler')\n"
            "def flood():\n"
            "    for _ in range(300): log.info('😀'*10000, extra={'attributes': {'x': float('nan')}})\n"
            "def handle(event):\n"
            "    threads = [threading.Thread(target=flood) for _ in range(4)]\n"
            "    for thread in threads: thread.start()\n"
            "    log.info('in handler', extra={'attributes': {'task_id': 'user-value'}})\n"
            "    for thread in threads: thread.join()\n"
            "    time.sleep(.02)\n"
            "    return event['id']\n",
            [self.invocation(), self.invocation("2"), {"v": 2, "type": "shutdown"}],
            protocol=2, output_limit=2048)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(all(len(line) + 1 <= 2048 for line in result.stdout.splitlines()))
        self.assertEqual([f["type"] for f in frames if f["type"] != "log"],
                         ["ready", "result", "result", "closing"])
        logs = [f for f in frames if f["type"] == "log"]
        self.assertTrue(any(f["message"] == "before handler" and "invocation" not in f for f in logs))
        inside = [f for f in logs if f["message"] == "in handler"]
        self.assertEqual({f["invocation"]["event_id"] for f in inside}, {"evt-1", "evt-2"})
        self.assertTrue(all(f["trace_id"] == TRACE[3:35] for f in inside))
        self.assertTrue(all(f["attributes"]["task_id"] == "user-value" for f in inside))

    def test_large_identity_drops_optional_logs_without_losing_result(self):
        message = self.invocation()
        message["event"]["ldgtaskid"] = "t" * 3000
        result, frames = self.launch(
            "from ledgence_worker import get_logger\n"
            "def handle(event): get_logger('example').info('hello'); return 42\n",
            [message, {"v": 2, "type": "shutdown"}], protocol=2, output_limit=512)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([f["type"] for f in frames], ["ready", "result", "closing"])
        self.assertEqual(frames[1]["output"], 42)

    def test_shutdown_callback_runs_once_before_ack(self):
        result, frames = self.launch(
            "from ledgence_worker import register_shutdown\n"
            "def closing(): print('provider shutdown')\n"
            "register_shutdown(closing)\nregister_shutdown(closing)\n"
            "def handle(event): return 42\n",
            [self.invocation(), {"v": 2, "type": "shutdown"}], protocol=2)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr.count(b"provider shutdown"), 1)
        self.assertEqual(frames[-1]["type"], "closing")


    @unittest.skipUnless(os.environ.get("LEDGENCE_PYTHON_OTEL_TEST_PACKAGES"),
                         "set reviewed API/SDK-only package directory for optional OTel tests")
    def test_optional_otel_child_parentage_and_empty_context_after_failure(self):
        first, second, third = self.invocation(), self.invocation("2", None), self.invocation("3")
        result, frames = self.launch(
            "import time\n"
            "from ledgence_worker import get_logger\n"
            "from ledgence_worker.otel import enable_context\n"
            "from opentelemetry import trace\n"
            "from opentelemetry.sdk.trace import TracerProvider\n"
            "from opentelemetry.sdk.trace.export import SimpleSpanProcessor\n"
            "from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter\n"
            "provider = TracerProvider()\nexporter = InMemorySpanExporter()\n"
            "provider.add_span_processor(SimpleSpanProcessor(exporter))\n"
            "trace.set_tracer_provider(provider)\nenable_context()\n"
            "tracer = trace.get_tracer('application')\nlog = get_logger('otel.child')\n"
            "def handle(event):\n"
            "    active = trace.get_current_span().get_span_context()\n"
            "    before = format(active.span_id, '016x') if active.is_valid else None\n"
            "    with tracer.start_as_current_span('application.child') as child:\n"
            "        log.info(event['id'])\n"
            "        time.sleep(.02)\n"
            "        if event['id'] == 'evt-1': raise ValueError('first')\n"
            "        child_id = format(child.get_span_context().span_id, '016x')\n"
            "    spans = exporter.get_finished_spans()\n"
            "    return {'before': before, 'child': child_id, 'event': event,\n"
            "            'parents': [format(s.parent.span_id, '016x') if s.parent else None for s in spans]}\n",
            [first, second, third, {"v": 2, "type": "shutdown"}], protocol=2,
            vendor=os.environ["LEDGENCE_PYTHON_OTEL_TEST_PACKAGES"])
        self.assertEqual(result.returncode, 0, result.stderr)
        results = [f for f in frames if f["type"] == "result"]
        self.assertEqual(results[0]["error"]["kind"], "business_error")
        self.assertIsNone(results[1]["output"]["before"])
        self.assertEqual(results[2]["output"]["before"], TRACE[36:52])
        self.assertEqual(results[2]["output"]["parents"], [TRACE[36:52], None, TRACE[36:52]])
        self.assertEqual(results[2]["output"]["event"], third["event"])
        logs = [f for f in frames if f["type"] == "log" and f["message"] == "evt-3"]
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0]["span_id"], results[2]["output"]["child"])
        self.assertNotEqual(logs[0]["span_id"], TRACE[36:52])


    @unittest.skipUnless(os.environ.get("LEDGENCE_PYTHON_OTEL_TEST_PACKAGES"),
                         "set reviewed API/SDK-only package directory for optional OTel tests")
    def test_api_only_activation_does_not_install_a_provider_or_inherit_origin(self):
        result, frames = self.launch(
            "import sys\nfrom ledgence_worker.otel import enable_context\n"
            "from opentelemetry import trace\nenable_context()\n"
            "def handle(event):\n"
            "    active = trace.get_current_span().get_span_context()\n"
            "    return {'span_id': format(active.span_id, '016x') if active.is_valid else None,\n"
            "            'sdk_loaded': 'opentelemetry.sdk.trace' in sys.modules}\n",
            [self.invocation(), self.invocation("2", None)], protocol=2,
            vendor=os.environ["LEDGENCE_PYTHON_OTEL_TEST_PACKAGES"])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(frames[1]["output"]["span_id"], TRACE[36:52])
        self.assertIsNone(frames[2]["output"]["span_id"])
        self.assertFalse(frames[1]["output"]["sdk_loaded"])
        self.assertFalse(frames[2]["output"]["sdk_loaded"])


    @unittest.skipUnless(os.environ.get("LEDGENCE_PYTHON_OTEL_TEST_PACKAGES"),
                         "set reviewed API/SDK-only package directory for optional OTel tests")
    def test_packaged_in_memory_example_with_sampled_unsampled_and_absent_context(self):
        example = Path(__file__).resolve().parents[3] / "examples/python-otel/program.py"
        result, frames = self.launch(example.read_text(),
            [self.invocation(), self.invocation("2", TRACE[:-2] + "00"), self.invocation("3", None),
             {"v": 2, "type": "shutdown"}], protocol=2,
            vendor=os.environ["LEDGENCE_PYTHON_OTEL_TEST_PACKAGES"])
        self.assertEqual(result.returncode, 0, result.stderr)
        outcomes = [f for f in frames if f["type"] == "result"]
        self.assertEqual(len(outcomes), 3)
        self.assertTrue(all(f["status"] == "success" for f in outcomes))
        self.assertEqual([f["output"]["task_id"] for f in outcomes], ["task-1", "task-2", "task-3"])


class WriterTests(unittest.TestCase):
    def test_blocked_log_queue_is_bounded_and_result_wins_over_backlog(self):
        class Pipe:
            def __init__(self):
                self.blocked, self.release = threading.Event(), threading.Event()
                self.frames = []
            def write(self, data):
                value = bytes(data)
                if value == b"log\n":
                    self.blocked.set()
                    if not self.release.wait(5):
                        raise OSError("test writer not released")
                self.frames.append(value)
                return len(value)
        pipe = Pipe()
        writer = ProtocolWriter(pipe, 4096)
        writer.control(b"ready\n")
        writer.offer_log(b"log\n")
        self.assertTrue(pipe.blocked.wait(2))
        for _ in range(1000):
            writer.offer_log(b"backlog\n")
        with writer._condition:
            self.assertEqual(len(writer._logs), MAX_LOG_RECORDS)
            self.assertLessEqual(writer._log_bytes, MAX_LOG_BYTES)
        result = threading.Thread(target=writer.control, args=(b"result\n",))
        result.start()
        for _ in range(100):
            with writer._condition:
                if writer._control is not None:
                    break
            time.sleep(.001)
        with writer._condition:
            self.assertIsNotNone(writer._control)
        pipe.release.set()
        result.join(2)
        self.assertFalse(result.is_alive())
        writer.control(b"closing\n", closing=True)
        writer._thread.join(2)
        self.assertEqual(pipe.frames[:3], [b"ready\n", b"log\n", b"result\n"])
        self.assertGreaterEqual(writer.dropped_logs, 1000 - MAX_LOG_RECORDS)

    def test_short_writes_remain_complete_and_late_snapshot_keeps_old_ids(self):
        class Pipe:
            def __init__(self): self.output = bytearray()
            def write(self, data):
                length = min(3, len(data))
                self.output.extend(data[:length])
                return length
        pipe = Pipe()
        writer = ProtocolWriter(pipe, 4096)
        previous_sink = _logging._sink
        _logging._sink = writer
        logger = get_logger("test.ledgence.snapshots")
        original_root = logging.root.handlers[:]
        custom = logging.NullHandler()
        logger.addHandler(custom)
        try:
            old = InvocationContext("evt-old", "att-old", "urn:test", "tenant", "ns", "run", "task", 1,
                                    TraceContext(TRACE))
            token = _invocation.set(old)
            captured = contextvars.copy_context()
            attributes = {"mutable": "old"}
            logger.info("snapshot", extra={"attributes": attributes})
            attributes["mutable"] = "changed"
            _invocation.reset(token)
            token = _invocation.set(InvocationContext("evt-new", "att-new"))
            captured.run(logger.info, "late")
            _invocation.reset(token)
            # Logs emitted while the ready gate is closed are already snapshots.
            writer.control(b'{"type":"ready"}\n')
            for _ in range(200):
                if b'"late"' in pipe.output: break
                time.sleep(.001)
            writer.control(b'{"type":"closing"}\n', closing=True)
            writer._thread.join(2)
            frames = [json.loads(line) for line in pipe.output.splitlines()]
            logs = [f for f in frames if f["type"] == "log"]
            self.assertEqual(len(logs), 2)
            self.assertTrue(all(f["invocation"]["event_id"] == "evt-old" for f in logs))
            self.assertEqual(logs[0]["attributes"]["mutable"], "old")
            self.assertEqual(logging.root.handlers, original_root)
            self.assertIn(custom, logger.handlers)
        finally:
            _logging._sink = previous_sink
            logger.removeHandler(custom)


if __name__ == "__main__":
    unittest.main()
