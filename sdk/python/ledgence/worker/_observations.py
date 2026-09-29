"""Bounded optional invocation measurements, independent of replay (MIT).

Measurements describe this Python process, including all its threads, excluding
child processes. Memory is a lifetime high-water mark, never an invocation peak.
An abrupt exit may lose the whole buffer. It contains no inputs or outputs.
"""

from contextvars import ContextVar
import json
import sys
import time

_current = ContextVar("ledgence_invocation_observations", default=None)
MAX_LOCAL_STEPS = 128


def _usage():
    try:
        import resource
        value = resource.getrusage(resource.RUSAGE_SELF)
        rss = value.ru_maxrss * (1024 if sys.platform.startswith("linux") else 1)
        if sys.platform != "darwin" and not sys.platform.startswith("linux"):
            rss = None  # Units are deliberately not guessed on other platforms.
        return (round(value.ru_utime * 1_000_000), round(value.ru_stime * 1_000_000), rss)
    except (ImportError, OSError, ValueError):
        return (None, None, None)


class Observations:
    def __init__(self):
        self.started_at = time.time_ns() // 1_000_000
        self.started = time.monotonic_ns()
        self.usage = _usage()
        self.locals = []
        self.truncated = False

    def begin(self, binding, replayed=False):
        if len(self.locals) >= MAX_LOCAL_STEPS:
            self.truncated = True
            return None
        record = {"key": binding["key"], "callable": binding["callable"],
                  "started_at_ms": time.time_ns() // 1_000_000,
                  "elapsed_us": 0, "state": "replayed" if replayed else "cancelled"}
        self.locals.append(record)
        return (record, time.monotonic_ns())

    @staticmethod
    def finish(token, state):
        if token is not None:
            record, started = token
            record.update(elapsed_us=max(0, (time.monotonic_ns() - started) // 1000), state=state)

    def snapshot(self):
        usage = _usage()
        def delta(index):
            before, after = self.usage[index], usage[index]
            return None if before is None or after is None or after < before else after - before
        value = {"runtime_started_at_ms": self.started_at,
                "runtime_elapsed_us": max(0, (time.monotonic_ns() - self.started) // 1000),
                "process_cpu_user_us": delta(0), "process_cpu_system_us": delta(1),
                "process_lifetime_peak_rss_bytes": usage[2],
                "local_steps": list(self.locals), "local_steps_truncated": self.truncated}
        # Escaped identifier bytes count too. Always keep resources even when the
        # optional local buffer must be truncated; never alter the replay journal.
        while len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()) > 128 * 1024:
            value["local_steps"].pop()
            value["local_steps_truncated"] = True
        return value


def begin_local(binding, replayed=False):
    observations = _current.get()
    if observations is None:
        return None
    try:
        return observations.begin(binding, replayed)
    except Exception:
        observations.truncated = True
        return None


def finish_local(token, state):
    try:
        Observations.finish(token, state)
    except Exception:
        observations = _current.get()
        if observations is not None:
            observations.truncated = True
