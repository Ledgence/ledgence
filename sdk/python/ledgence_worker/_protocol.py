"""The v2 protocol's single writer and bounded optional log queue (MIT)."""

from collections import deque
import threading

MAX_LOG_RECORDS = 64
MAX_LOG_BYTES = 1024 * 1024
MAX_LOG_FRAME_BYTES = 16 * 1024
_MAX_DROPPED = (1 << 64) - 1


class ProtocolWriter:
    def __init__(self, protocol, frame_limit):
        self.log_limit = min(frame_limit, MAX_LOG_FRAME_BYTES)
        self._protocol = protocol
        self._condition = threading.Condition()
        self._logs = deque()
        self._log_bytes = 0
        self._control = None
        self._error = None
        self._ready = False
        self._stopped = False
        self._dropped = 0
        self._thread = threading.Thread(target=self._run, name="ledgence-protocol", daemon=True)
        self._thread.start()

    @property
    def dropped_logs(self):
        with self._condition:
            return self._dropped

    def drop_log(self):
        with self._condition:
            self._dropped = min(_MAX_DROPPED, self._dropped + 1)

    def offer_log(self, encoded):
        with self._condition:
            if (self._stopped or self._error is not None
                    or len(encoded) > self.log_limit
                    or len(self._logs) >= MAX_LOG_RECORDS
                    or self._log_bytes + len(encoded) > MAX_LOG_BYTES):
                self._dropped = min(_MAX_DROPPED, self._dropped + 1)
                return False
            self._logs.append(encoded)
            self._log_bytes += len(encoded)
            self._condition.notify()
            return True

    def control(self, encoded, closing=False):
        done = threading.Event()
        with self._condition:
            if self._error is not None:
                raise self._error
            if self._control is not None:
                raise RuntimeError("concurrent protocol control writes")
            if closing:
                self._stopped = True
                self._dropped = min(_MAX_DROPPED, self._dropped + len(self._logs))
                self._logs.clear()
                self._log_bytes = 0
            self._control = (encoded, done)
            self._condition.notify()
        done.wait()  # IPC backpressure only; optional logs never occupy this slot.
        if self._error is not None:
            raise self._error

    def _run(self):
        while True:
            with self._condition:
                while self._control is None and not (self._ready and self._logs):
                    if self._stopped:
                        return
                    self._condition.wait()
                if self._control is not None:
                    encoded, done = self._control
                    self._control = None
                else:
                    encoded = self._logs.popleft()
                    self._log_bytes -= len(encoded)
                    done = None
            try:
                pending = memoryview(encoded)
                while pending:
                    count = self._protocol.write(pending)
                    if count is None or count <= 0:
                        raise OSError("protocol output pipe closed")
                    pending = pending[count:]
            except Exception as error:
                with self._condition:
                    self._error = error
                    if self._control is not None:
                        self._control[1].set()
                    self._logs.clear()
                    self._log_bytes = 0
                if done is not None:
                    done.set()
                return
            if done is not None:
                with self._condition:
                    self._ready = True
                done.set()
