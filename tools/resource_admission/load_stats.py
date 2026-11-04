"""Scoped PostgreSQL and host observations for bounded admission trials (MIT)."""

import json
import math
import runpy
import subprocess
import threading
import time
from pathlib import Path

from workflow_acceptance.owned_scenarios import quote

# Reuse the existing reset/eviction validation, without launching its separate load generator.
PERF = runpy.run_path(str(Path(__file__).resolve().parents[1] / "check-performance.py"))


def statements(d):
    return json.loads(
        d.sql(PERF["STATEMENTS"].replace(str(PERF["STATEMENT_LIMIT"]), "4096"))
    )


def statement_delta(before, after):
    result = PERF["statement_delta"](before, after)
    if not result["complete"]:
        raise RuntimeError(
            f"incomplete statement statistics: {result['invalidity_reasons']}"
        )
    result["note"] = (
        "Per-database deltas from before first arrival until every task is observed terminal "
        "and the sampler is stopped. Includes background queries and fixture observations. "
        "Counters are not reset. The final snapshot observes its own cost only afterward."
    )
    return result


def percentiles(values):
    data = sorted(values)
    return {
        "count": len(data),
        **{
            f"p{p}": data[math.ceil(len(data) * p / 100) - 1] if data else None
            for p in (50, 95, 99)
        },
    }


def task_rows(d, tag):
    sql = (
        """SELECT row_to_json(x) FROM (
      SELECT t.task_id,t.idempotency_key AS key,t.state,t.attempt_count,t.submitted_at_ms AS submitted,
             t.terminal_at_ms AS terminal,t.admission_resource AS resource,a.state AS admission_state,
             a.source_id,a.owner_attempt_id,b.root_id AS budget_root,b.calls AS charged_calls,
             (SELECT array_agg(e.source_id ORDER BY e.sequence) FROM run_budget_entries e
              WHERE (e.tenant_id,e.namespace,e.root_kind,e.root_id)=(b.tenant_id,b.namespace,b.root_kind,b.root_id)
              AND e.action='call') AS charged_sources,
             (convert_from(a.snapshot_bytes,'UTF8')::jsonb #>> '{source,accepted_at}')::bigint AS start,
             (SELECT min(h.at_ms) FROM task_history h WHERE h.task_id=t.task_id AND h.reason='claimed') AS claim
      FROM tasks t LEFT JOIN task_admission_requests a ON a.task_id=t.task_id AND a.generation=1
      LEFT JOIN run_budgets b ON (b.tenant_id,b.namespace,b.root_kind,b.root_id)=
                                (t.tenant_id,t.namespace,'task',t.task_id)
      WHERE t.correlation_key="""
        + quote(tag)
        + """ ORDER BY t.idempotency_key) x"""
    )
    return [json.loads(line) for line in d.sql(sql).splitlines()]


def peak(rows):
    points = [(r["start_ns"], 1) for r in rows] + [(r["end_ns"], -1) for r in rows]
    n = maximum = 0
    for _, delta in sorted(points):
        n += delta
        maximum = max(maximum, n)
    assert n == 0
    return maximum


class Observer:
    """Finite samples; Lock captures waiter observations, not exact wait duration."""

    def __init__(self, deployment):
        self.d = deployment
        self.events = []
        self.stop = threading.Event()
        self.error = None
        self.thread = threading.Thread(target=self.sample, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def sample(self):
        try:
            while not self.stop.is_set():
                stamp = time.monotonic_ns()
                waiting = int(
                    self.d.sql(
                        "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                        "AND pid<>pg_backend_pid() AND wait_event_type='Lock'"
                    )
                )
                listing = subprocess.run(
                    ["ps", "-axo", "pid=,ppid=,rss=,time="],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=5,
                ).stdout
                procs = {}
                for line in listing.splitlines():
                    pid, ppid, rss, elapsed = line.split()
                    parts = elapsed.split("-")
                    seconds = 86400 * int(parts[0]) if len(parts) == 2 else 0
                    for i, number in enumerate(reversed(parts[-1].split(":"))):
                        seconds += float(number) * (60**i)
                    procs[int(pid)] = (int(ppid), int(rss), round(1000 * seconds))
                owned = {
                    p.process.pid for p in self.d.processes if p.process.poll() is None
                }
                while True:
                    children = {
                        pid for pid, (parent, *_) in procs.items() if parent in owned
                    }
                    if children.issubset(owned):
                        break
                    owned.update(children)
                self.events.append(
                    {
                        "at_ns": stamp,
                        "pg_lock_waiters": waiting,
                        "rss_kib": sum(procs[p][1] for p in owned if p in procs),
                        "cpu_ms_by_pid": {
                            str(p): procs[p][2] for p in owned if p in procs
                        },
                    }
                )
                self.stop.wait(0.4)
        except (OSError, ValueError, AssertionError, subprocess.SubprocessError) as exc:
            self.error = exc

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=45)
        if self.thread.is_alive() or self.error is not None:
            raise RuntimeError("load observer failed") from self.error

    def summary(self):
        cpus = {}
        for sample in self.events:
            for pid, value in sample["cpu_ms_by_pid"].items():
                cpus.setdefault(pid, []).append(value)
        return {
            "samples": len(self.events),
            "peak_sampled_lock_waiters": max(
                (s["pg_lock_waiters"] for s in self.events), default=None
            ),
            "peak_sampled_service_rss_kib": max(
                (s["rss_kib"] for s in self.events), default=None
            ),
            "sampled_service_cpu_ms": sum(
                max(times) - min(times) for times in cpus.values()
            ),
        }
