"""Native equal-work, parked-population and throttling scenarios (MIT)."""

import concurrent.futures
import json
import threading
import time
from pathlib import Path

from http_acceptance.harness import eventually
from program_fixture import prepare_package
from queue_capacity.fixture import CapacityDeployment
from workflow_acceptance.owned_scenarios import quote

from .load_stats import (
    Observer,
    peak,
    percentiles,
    statement_delta,
    statements,
    task_rows,
)

SLOTS = 4
RATE = 16
MODES = ("ordinary", "hot", "independent", "budget")


class Deployment(CapacityDeployment):
    def publish_programs(self):
        source = Path(__file__).with_name("load_program.py").read_text()
        for name, handler in (("load-task", "task"), ("load-retry", "workflow")):
            directory = prepare_package(
                self.directory / name,
                name,
                "1.0.0",
                source,
                python=self.python,
                protocol=3,
                handler="program:" + handler,
            )
            self.command(
                "ledgence",
                [
                    "program",
                    "publish",
                    "--source",
                    str(directory),
                    "--store",
                    str(self.store),
                ],
            )

    def submit_call(
        self, tag, index, provider, *, resource=None, budget=False, ms=5, queue=None
    ):
        key = f"{tag}-{index:04d}"
        c = self.submission(key, program="load-task")
        c["input"].update(
            queue=queue or self.queue,
            correlation_key=tag,
            data={"key": key, "port": provider.port, "ms": ms},
        )
        c["input"]["retry_policy"] = {"max_attempts": 1, "retry_delay_ms": 0}
        if resource is not None:
            c["input"]["admission"] = {"resource": resource}
        if budget:
            c["input"]["budget"] = {"max_calls": 1}
        return self.checked("POST", "/v1/tasks", c)["task_id"]

    def configure(self, name, enabled=True, *, groups=False, limit=SLOTS):
        query = {"scope": self.scope, "name": name}
        status, _, previous = self.request(
            "POST", "/v1/resources/query", query, credentials=self.operator
        )
        assert status in (200, 404)
        policy = {
            "enabled": enabled,
            "max_occupancy": limit,
            "rate": None,
            "offer_timeout_ms": 30000,
            "start_timeout_ms": 5000,
        }
        if groups:
            policy["fair_sharing"] = {
                "groups": [
                    {"name": "support", "weight": 3, "queues": ["load-support"]},
                    {"name": "batch", "weight": 1, "queues": []},
                ],
                "default_group": "batch",
            }
        revision = previous["resource"]["revision"] if status == 200 else None
        self.checked(
            "POST",
            "/v1/resources/put",
            {
                **query,
                "policy": policy,
                "expected_revision": revision,
                "request_id": f"setup-{name}-{revision}",
                "reason": "local admission load qualification",
            },
        )

    def drain(self, tag, total):
        def done():
            row = self.sql(
                "SELECT count(*) FROM tasks WHERE correlation_key="
                + quote(tag)
                + " AND terminal_at_ms IS NOT NULL"
            )
            return int(row) == total

        eventually(
            done,
            timeout=max(90, total * 1.5),
            description="cohort reaches terminal state",
        )


def warm(d, provider):
    jobs = [d.submit_call("warm", i, provider) for i in range(SLOTS)]
    try:
        eventually(
            lambda: len(provider.records("warm")) == SLOTS,
            timeout=40,
            description="all four runtime slots warm",
        )
    finally:
        provider.warm.set()
    d.drain("warm", len(jobs))


def check_calls(rows, provider, tag, expected):
    observed = provider.records(tag)
    assert len(rows) == len(observed) == expected, (len(rows), len(observed), expected)
    assert all(
        row["state"] == "succeeded" and row["attempt_count"] == 1 for row in rows
    )
    wanted = {(r["key"], r["task_id"]) for r in rows}
    assert (
        len(wanted) == expected and {(r["key"], r["task"]) for r in observed} == wanted
    )
    assert all(
        r["status"] == 200
        and r["ordinal"] == 1
        and r["new_effect"]
        and "end_ns" in r
        and r["attempt"]
        and r["workflow"] is None
        for r in observed
    )
    return observed


def timing_arm(d, provider, mode, tag, total):
    # Matching fixed-rate arrivals. Four submitters; a saturated generator fails explicitly.
    lsn = d.sql("SELECT pg_current_wal_insert_lsn()")
    before = statements(d)

    def submit(i, due):
        issued = time.monotonic_ns()
        resource = (
            None
            if mode == "ordinary"
            else f"load-{i % 8}"
            if mode == "independent"
            else "load-hot"
        )
        task = d.submit_call(
            tag,
            i,
            provider,
            resource=resource,
            budget=mode == "budget",
            ms=150 if i % 8 == 0 else 5,
        )
        return {
            "task": task,
            "due_ns": due,
            "issued_ns": issued,
            "returned_ns": time.monotonic_ns(),
        }

    with (
        Observer(d) as observer,
        concurrent.futures.ThreadPoolExecutor(max_workers=SLOTS) as pool,
    ):
        futures = []
        first_ns = time.monotonic_ns()
        for i in range(total):
            due = first_ns + round(i * 1e9 / RATE)
            wait = due - time.monotonic_ns()
            if wait > 0:
                time.sleep(wait / 1e9)
            if sum(not f.done() for f in futures) >= SLOTS:
                raise RuntimeError("submitter capacity exceeded; arm invalid")
            futures.append(pool.submit(submit, i, due))
        arrivals = [f.result() for f in futures]
        d.drain(tag, total)
        terminal_ns = time.monotonic_ns()
    after = statements(d)
    wal = int(
        d.sql("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn()," + quote(lsn) + ")")
    )
    delta = statement_delta(before, after)
    rows = task_rows(d, tag)
    physical = check_calls(rows, provider, tag, total)
    governed = mode != "ordinary"
    assert all((r["resource"] is not None) == governed for r in rows)
    if governed:
        assert all(
            r["admission_state"] == "finished" and r["start"] >= r["submitted"]
            for r in rows
        )
    else:
        assert all(r["start"] is None for r in rows)
    usage = sum(r["charged_calls"] or 0 for r in rows)
    physical_by_id = {r["task"]: r for r in physical}
    for row in rows:
        if mode == "budget":
            assert row["budget_root"] == row["task_id"] and row["charged_calls"] == 1
            assert row["source_id"] and row["charged_sources"] == [row["source_id"]]
        else:
            assert (
                row["budget_root"]
                is row["charged_calls"]
                is row["charged_sources"]
                is None
            )
        if governed:
            assert (
                row["source_id"]
                and physical_by_id[row["task_id"]]["attempt"] == row["owner_attempt_id"]
            )
        index = int(row["key"].rsplit("-", 1)[1])
        expected_resource = (
            f"load-{index % 8}"
            if mode == "independent"
            else "load-hot"
            if governed
            else None
        )
        assert row["resource"] == expected_resource
        assert row["submitted"] <= row["claim"] <= row["terminal"]
    lateness = [(a["issued_ns"] - a["due_ns"]) / 1e6 for a in arrivals]
    assert max(lateness) <= 125, (
        "arrival generator missed its preregistered timing bound"
    )
    n = peak(physical)
    assert n <= SLOTS
    (d.directory / (tag + "-raw.json")).write_text(
        json.dumps(
            {
                "arrivals": arrivals,
                "tasks": rows,
                "provider": physical,
                "host_samples": observer.events,
                "sql": delta,
            },
            indent=2,
        )
        + "\n"
    )
    result = {
        "case": tag,
        "mode": mode,
        "offered": total,
        "accepted": len(arrivals),
        "succeeded": sum(r["state"] == "succeeded" for r in rows),
        "worker_attempts": sum(r["attempt_count"] for r in rows),
        "physical_calls": len(physical),
        "effects": sum(r["new_effect"] for r in physical),
        "budget_charges": usage,
        "peak_physical": n,
        "sampled": observer.summary(),
        "sql": delta["totals"],
        "whole_cluster_wal_bytes": wal,
        "elapsed_seconds": (terminal_ns - first_ns) / 1e9,
        "completions_per_s_including_arrivals": total * 1e9 / (terminal_ns - first_ns),
        "submit_lateness_ms": percentiles(lateness),
        "maximum_submit_lateness_ms": max(lateness),
        "accepted_to_terminal_ms": percentiles(
            [r["terminal"] - r["submitted"] for r in rows]
        ),
        "accepted_to_claim_ms": percentiles(
            [r["claim"] - r["submitted"] for r in rows]
        ),
        "accepted_to_governed_start_ms": percentiles(
            [r["start"] - r["submitted"] for r in rows if r["start"] is not None]
        ),
        "provider_busy_fraction": sum(r["end_ns"] - r["start_ns"] for r in physical)
        / (SLOTS * (terminal_ns - first_ns)),
    }
    return result


def parked(d, provider, total):
    tag = "parked"
    d.configure("disabled", False)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        jobs = list(
            pool.map(
                lambda i: d.submit_call(tag, i, provider, resource="disabled", ms=1),
                range(total),
            )
        )

    def waiting():
        return int(
            d.sql(
                "SELECT count(*) FROM task_admission_requests a JOIN tasks t USING(tenant_id,namespace,task_id) "
                "WHERE resource_name='disabled' AND a.state='waiting' AND a.owner_attempt_id IS NULL AND t.attempt_count=0"
            )
        )

    assert waiting() == total and not provider.records(tag)
    # A separate ordinary task proves the same runtime slots can continue to serve.
    d.submit_call("free", 0, provider)
    d.drain("free", 1)
    check_calls(task_rows(d, "free"), provider, "free", 1)
    d.server.kill()
    d.start_server()
    assert waiting() == total and not provider.records(tag)
    started = time.monotonic()
    d.configure("disabled", True)
    d.drain(tag, total)
    elapsed = time.monotonic() - started
    completed_rows = task_rows(d, tag)
    physical = check_calls(completed_rows, provider, tag, total)
    (d.directory / "parked-raw.json").write_text(
        json.dumps(
            {"submitted": jobs, "tasks": completed_rows, "provider": physical}, indent=2
        )
        + "\n"
    )
    assert len(set(jobs)) == total and peak(physical) <= SLOTS
    live = int(
        d.sql(
            "SELECT count(*) FROM task_admission_requests WHERE resource_name='disabled' AND state<>'finished'"
        )
    )
    assert live == 0
    return {
        "parked": total,
        "physical_before_release": 0,
        "ordinary_witness": 1,
        "completed_after_restart": len(physical),
        "peak_provider": peak(physical),
        "drain_s": elapsed,
    }


def throttle(d, provider):
    result = []
    for count in (1, 2):
        tag = f"throttle-{count}"
        c = d.submission(tag, program="load-retry")
        c["input"].update(
            correlation_key=tag,
            data={"port": provider.port, "ms": 2, "key": tag + "-0", "throttle": count},
            budget={"max_calls": 2},
        )
        wf = d.checked("POST", "/v1/workflows", c)["workflow_id"]

        def done(wf=wf):
            status = d.checked("GET", d.query("/v1/workflows/status", workflow_id=wf))[
                "state"
            ]
            return status if status in ("succeeded", "failed", "cancelled") else None

        state = eventually(
            done, timeout=60, description="durable Retry-After scenario completes"
        )
        calls = provider.records(tag)
        assert len(calls) == 2 and [r["status"] for r in calls] == [
            429,
            200 if count == 1 else 429,
        ]
        effects = sum(r["new_effect"] for r in calls)
        assert effects == (1 if count == 1 else 0)
        assert [r["ordinal"] for r in calls] == [1, 2] and all(
            r["key"] == tag + "-0" and r["workflow"] == wf for r in calls
        )
        sources = [
            json.loads(line)
            for line in d.sql(
                "SELECT json_build_object('source',s->>'source_id','task',s#>>'{owner,task_id}','attempt',s#>>'{owner,attempt_id}', "
                "'step',logical_key,'charged',e.source_id) FROM admission_requests a "
                "CROSS JOIN LATERAL (SELECT convert_from(a.snapshot_bytes,'UTF8')::jsonb->'source' AS s) source "
                "LEFT JOIN run_budget_entries e ON (e.tenant_id,e.namespace,e.root_kind,e.root_id,e.action,e.source_id) = "
                "(a.tenant_id,a.namespace,'workflow',a.workflow_id,'call',s->>'source_id') "
                "WHERE a.workflow_id="
                + quote(wf)
                + " AND state='finished' ORDER BY logical_key"
            ).splitlines()
        ]
        assert len(sources) == 2 and len({s["source"] for s in sources}) == 2
        for number, (source, request) in enumerate(zip(sources, calls, strict=True), 1):
            assert (
                source["source"]
                and source["charged"] == source["source"]
                and source["step"] == f"send-{number}"
            )
            assert (
                source["task"] == request["task"]
                and source["attempt"] == request["attempt"]
            )
        assert state == ("succeeded" if count == 1 else "failed")
        gap = calls[1]["start_ns"] - calls[0]["end_ns"]
        assert gap >= 1_000_000_000, "physical retry occurred before Retry-After"
        q = {"scope": d.scope, "subject": {"kind": "workflow", "workflow_id": wf}}
        assert d.checked("POST", "/v1/budgets/inspect", q)["calls"] == "2"
        assert d.checked("POST", "/v1/workflows", c)["workflow_id"] == wf
        assert len(provider.records(tag)) == 2
        result.append(
            {
                "workflow": wf,
                "outcome": state,
                "charges": 2,
                "gap_ms": gap / 1e6,
                "requests": calls,
                "effects": effects,
                "sources": sources,
            }
        )
    return result


def skew(d, provider):
    # One resource, two actual queue-bound worker sessions, unequal request durations.
    tag = "skew"
    d.configure(tag, groups=True, limit=2)
    d.consumer("support", "load-support", 2)
    for i in range(2):
        provider.held[f"{tag}-{i:04d}"] = threading.Event()
    try:
        for i in range(2):
            d.submit_call(tag, i, provider, resource=tag, ms=10)
        eventually(
            lambda: len(provider.records(tag)) == 2,
            description="two active batch holders",
        )
        for i in range(2, 14):
            d.submit_call(tag, i, provider, resource=tag, ms=180 if i % 2 else 5)
        for i in range(14, 22):
            d.submit_call(tag, i, provider, resource=tag, ms=5, queue="load-support")
        eventually(
            lambda: (
                int(
                    d.sql(
                        "SELECT count(*) FROM task_admission_requests WHERE resource_name='skew' AND state='waiting'"
                    )
                )
                == 20
            ),
            description="both groups waiting before releasing holders",
        )
    finally:
        for gate in provider.held.values():
            gate.set()
    d.drain(tag, 22)
    rows = task_rows(d, tag)
    physical = check_calls(rows, provider, tag, 22)
    page = d.checked(
        "POST",
        "/v1/resources/decisions",
        {"scope": d.scope, "name": tag, "limit": 100, "before": None},
    )
    offers = list(reversed(page["items"]))
    assert page["next"] is None and len(offers) == 22 and peak(physical) == 2
    assert [r["group"] for r in offers[:2]] == ["batch", "batch"]
    opportunity = [r["group"] for r in offers[2:]].index("support") + 1
    contended = [r["group"] for r in offers if r["eligible_groups"] == 2]
    assert opportunity <= 4 and len(contended) >= 4
    assert all(
        contended[i : i + 4].count("support") == 3 for i in range(len(contended) - 3)
    )
    (d.directory / "skew-raw.json").write_text(
        json.dumps({"offers": offers, "tasks": rows, "provider": physical}, indent=2)
        + "\n"
    )
    return {
        "backlog_before_release": {"batch": 12, "support": 8},
        "physical_sends": len(physical),
        "peak_provider": 2,
        "first_support_opportunity": opportunity,
        "contended": contended,
    }
