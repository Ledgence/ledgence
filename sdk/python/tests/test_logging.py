"""Structured logs remain one event across ordinary logging propagation (MIT)."""

from concurrent.futures import ThreadPoolExecutor
import json
import logging
from pathlib import Path
import sys
import threading
import unittest

import test_bootstrap

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ledgence.worker import _logging, get_logger


class Sink:
    protocol_version = 2
    log_limit = 16384

    def __init__(self):
        self.frames = []
        self.dropped = 0
        self.reject = False
        self.lock = threading.Lock()

    def offer_log(self, encoded):
        with self.lock:
            if self.reject:
                self.dropped += 1
                return False
            self.frames.append(json.loads(encoded))
            return True

    def drop_log(self):
        with self.lock:
            self.dropped += 1


class ApplicationHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


class LoggingTests(unittest.TestCase):
    def setUp(self):
        self.original_sink = _logging._sink
        self.sink = Sink()
        _logging._sink = self.sink
        self.original_loggers = {}

    def tearDown(self):
        _logging._sink = self.original_sink
        for logger, (handlers, level, propagate, disabled) in self.original_loggers.items():
            logger.handlers[:] = handlers
            logger.setLevel(level)
            logger.propagate = propagate
            logger.disabled = disabled

    def logger(self, name):
        logger = logging.getLogger(name)
        if logger not in self.original_loggers:
            self.original_loggers[logger] = (
                logger.handlers[:], logger.level, logger.propagate, logger.disabled)
        return logger

    def hierarchy(self, child_first=False):
        names = ("", "ledgence_test", "ledgence_test.agent", "ledgence_test.agent.io")
        for name in names:
            self.logger(name)
        for name in reversed(names) if child_first else names:
            get_logger(name)
        return [self.logger(name) for name in names]

    def assert_configuration_preserved(self, child_first):
        root, parent, middle, child = self.hierarchy(child_first)
        root.setLevel(logging.ERROR)
        parent.setLevel(logging.WARNING)
        middle.setLevel(logging.INFO)
        child.setLevel(logging.DEBUG)
        expected_levels = [logger.level for logger in (root, parent, middle, child)]
        applications = []
        for logger in (root, parent, middle, child):
            app = ApplicationHandler()
            logger.addHandler(app)
            applications.append(app)
            get_logger(logger.name)
            get_logger(logger.name)
            self.assertIn(app, logger.handlers)
            self.assertEqual(sum(isinstance(item, _logging._Handler)
                                 for item in logger.handlers), 1)
        self.assertEqual([logger.level for logger in (root, parent, middle, child)],
                         expected_levels)
        child.debug("one event")
        self.assertEqual(len(self.sink.frames), 1)
        self.assertEqual(self.sink.frames[0]["message"], "one event")
        self.assertEqual([len(app.records) for app in applications], [1, 1, 1, 1])
        self.assertTrue(all(logger.propagate for logger in (parent, middle, child)))

    def test_ancestor_first_configuration_preserves_application_handlers(self):
        self.assert_configuration_preserved(child_first=False)

    def test_child_first_configuration_preserves_application_handlers(self):
        self.assert_configuration_preserved(child_first=True)

    def test_rejected_helper_falls_back_and_changed_propagation_keeps_local_coverage(self):
        root, parent, middle, child = self.hierarchy()
        child_helper = next(item for item in child.handlers if isinstance(item, _logging._Handler))
        middle_helper = next(item for item in middle.handlers if isinstance(item, _logging._Handler))
        parent_helper = next(item for item in parent.handlers if isinstance(item, _logging._Handler))
        reject = lambda record: False
        child_helper.addFilter(reject)
        middle_helper.setLevel(logging.ERROR)
        parent.removeHandler(parent_helper)
        child.info("fallback to root")
        self.assertEqual([frame["message"] for frame in self.sink.frames], ["fallback to root"])
        child_helper.removeFilter(reject)
        middle.propagate = False
        child.info("stopped at middle")
        child.propagate = False
        child.info("stopped at child")
        self.assertEqual([frame["message"] for frame in self.sink.frames],
                         ["fallback to root", "stopped at middle", "stopped at child"])

    def test_multiple_helpers_drop_each_malformed_oversized_or_rejected_record_once(self):
        _, _, _, child = self.hierarchy()
        child.addHandler(_logging._Handler())
        child.info("bad %d", "not an integer")
        self.assertEqual(self.sink.dropped, 1)
        self.sink.log_limit = 1
        child.info("cannot fit mandatory fields")
        self.assertEqual(self.sink.dropped, 2)
        self.sink.log_limit = 16384
        self.sink.reject = True
        child.info("full writer queue")
        self.assertEqual(self.sink.dropped, 3)
        self.assertEqual(self.sink.frames, [])

    def test_redispatch_preserves_application_delivery_without_another_snapshot(self):
        root, _, _, child = self.hierarchy()
        app = ApplicationHandler()
        root.addHandler(app)
        record = child.makeRecord(child.name, logging.INFO, __file__, 1, "same event", (), None)
        child.handle(record)
        child.handle(record)
        root.handle(record)
        self.assertEqual(len(self.sink.frames), 1)
        self.assertEqual(app.records, [record, record, record])
        # The private marker must not add non-serializable resources to a record
        # that an application's own JSON formatter might serialize.
        json.dumps(record.__dict__)
        child.info("same event")
        child.info("same event")
        self.assertEqual(len(self.sink.frames), 3)
        self.assertEqual(len(app.records), 5)

    def test_no_sink_does_not_claim_an_event(self):
        _, _, _, child = self.hierarchy()
        record = child.makeRecord(child.name, logging.INFO, __file__, 1, "later sink", (), None)
        _logging._sink = None
        child.handle(record)
        self.assertEqual(self.sink.frames, [])
        _logging._sink = self.sink
        child.handle(record)
        self.assertEqual([frame["message"] for frame in self.sink.frames], ["later sink"])

    def test_concurrent_identical_calls_emit_independently_and_shared_record_emits_once(self):
        root, _, _, child = self.hierarchy()
        app = ApplicationHandler()
        root.addHandler(app)
        record = child.makeRecord(child.name, logging.INFO, __file__, 1, "shared", (), None)

        def log_batch():
            for _ in range(20):
                child.info("independent")
                child.handle(record)

        with ThreadPoolExecutor(max_workers=8) as pool:
            for future in [pool.submit(log_batch) for _ in range(8)]:
                future.result(timeout=5)
        messages = [frame["message"] for frame in self.sink.frames]
        self.assertEqual(messages.count("independent"), 160)
        self.assertEqual(messages.count("shared"), 1)
        self.assertEqual(len(app.records), 320)


class ProtocolLoggingTests(unittest.TestCase):
    launch = test_bootstrap.BootstrapTests.launch

    def test_hierarchy_emits_one_real_protocol_frame_per_call_and_keeps_user_handlers(self):
        for child_first in (False, True):
            with self.subTest(child_first=child_first):
                request = test_bootstrap.BootstrapTests.invocation(self)
                request.update(v=2, processing_context=None)
                request["event"].update(ldgtenantid="tenant", ldgnamespace="tests",
                                        ldgrunid="run", ldgtaskid="task", ldgattemptno=1)
                names = ["", "example", "example.agent", "example.agent.io"]
                if child_first:
                    names.reverse()
                source = '''import json, logging, time
from ledgence.worker import _logging, get_logger
for name in NAMES:
    get_logger(name)
root = logging.getLogger()
child = get_logger("example.agent.io")
received = []
class Application(logging.Handler):
    def emit(self, record):
        received.append({"message": record.getMessage(), "logger": record.name})
        json.dumps(record.__dict__)
root.addHandler(Application())
logging.getLogger("example").addHandler(Application())
root.setLevel(logging.ERROR)
logging.getLogger("example").setLevel(logging.WARNING)
def handle(event):
    child.info("one application log event")
    writer = _logging._sink
    deadline = time.monotonic() + 5
    with writer._condition:
        while writer._logs:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError("log writer did not dequeue the event")
            writer._condition.wait(min(remaining, .01))
    return {"received": received, "propagate": child.propagate}
'''.replace("NAMES", repr(names))
                process, frames = self.launch(
                    source, [request, {"v": 2, "type": "shutdown"}], protocol=2)
                self.assertEqual(process.returncode, 0, process.stderr)
                logs = [frame for frame in frames if frame["type"] == "log"]
                results = [frame for frame in frames if frame["type"] == "result"]
                self.assertEqual(len(logs), 1)
                self.assertEqual(logs[0]["message"], "one application log event")
                self.assertEqual(logs[0]["logger"], "example.agent.io")
                self.assertEqual(logs[0]["invocation"]["task_id"], "task")
                self.assertEqual(len(results), 1)
                self.assertEqual(results[0]["status"], "success")
                self.assertEqual(results[0]["output"], {
                    "received": [{"message": "one application log event",
                                  "logger": "example.agent.io"}] * 2,
                    "propagate": True})


if __name__ == "__main__":
    unittest.main()
