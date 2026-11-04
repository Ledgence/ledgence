"""Observable behavior of the production worker with a shared physical pool. MIT."""
from collections import Counter
import math
import json
import time

from .fixture import acquired, identity, plan_log, publish_plan, trace, wait_worker

# Freeze the horizon and gates before sampling. Network completions can reorder
# at most N outstanding polls across a prefix relative to local selection.
SAMPLE = 512
CONSUMERS = 12
LOTTERY_ALPHA = 1e-7
ACQUIRE = "/v1/acquisitions"
APPLIED = "worker poll plan loaded"
REJECTED = "worker poll plan rejected"


def stable(d, strategy):
    phase = f"stable-{strategy}"
    a, b = f"{phase}-a", f"{phase}-b"
    proxy = d.observing_proxy()
    faulted = []
    def mark_fault(body):
        faulted.append((body["worker_session_id"], body["consumer_id"], body["sequence"]))
        return True
    lost = proxy.lose_once(ACQUIRE, mark_fault)
    file = d.directory / f"{phase}.json"
    publish_plan(file, 1, strategy, {a: 7, b: 3})
    proc = d.worker(phase, file, CONSUMERS, proxy)
    # Both queues are still empty when the first committed response gets lost.
    wait_worker(proc, lost.is_set, "lost committed Empty response")
    durations = {a: 10, b: 110}
    tasks = {d.submit_to(q, f"{phase}-{q}-{i}", duration_ms=ms): q
             for i in range(3) for q, ms in durations.items()}
    wait_worker(proc, lambda: len({identity(r) for r in acquired(proxy)}) >= SAMPLE // 2,
                "first half of the continuous production window", timeout=90)
    publish_plan(file, 2, strategy, {b: 6, a: 14})
    wait_worker(proc, lambda: plan_log(proc, APPLIED, revision=2), "equivalent reordered revision accepted")
    wait_worker(proc, lambda: len({identity(r) for r in acquired(proxy)}) >= SAMPLE,
                f"{SAMPLE} fresh production polls", timeout=120)
    pre_stop = len(acquired(proxy))
    proc.stop()
    rows = trace(d, proxy, (a, b), CONSUMERS, phase)
    # A signal may reconcile an additional pending request. Audit every replay
    # in trace(), and require that the intentionally dropped identity is among them.
    assert len(faulted) == 1
    observed_fault = [r for r in rows if (r["session"], r["consumer"], r["sequence"]) == faulted[0]]
    assert len(observed_fault) >= 2 and all(r["result"] == "empty" for r in observed_fault)
    assert observed_fault[0]["replay"] is False and all(r["replay"] for r in observed_fault[1:])
    fresh_indices = [index for index, row in enumerate(rows) if not row["replay"]]
    window_end = fresh_indices[SAMPLE - 1] + 1
    fresh = [r for r in rows if not r["replay"]][:SAMPLE]
    assert len(fresh) == SAMPLE
    assignments = [r for r in fresh if r["result"] == "assigned"]
    assert len(assignments) == len(tasks) and {r["task"]: r["queue"] for r in assignments} == tasks
    assert all(any(r["queue"] == q and r["result"] == "empty" for r in fresh) for q in (a, b))
    counts = Counter(r["queue"] for r in fresh)
    per_consumer = {slot: sorted({r["queue"] for r in fresh if r["consumer"] == slot})
                    for slot in range(CONSUMERS)}
    assert any(len(routes) == 2 for routes in per_consumer.values()), "physical consumer never switched queues"
    seen_a, max_lag = 0, 0
    for n, row in enumerate(fresh, 1):
        seen_a += int(row["queue"] == a)
        max_lag = max(max_lag, abs(10 * seen_a - 7 * n))
    # The acceptance oracle sees committed HTTP replies, not the order of
    # issuance at the worker. A later arrival can cross a sample boundary.
    reorder = CONSUMERS / SAMPLE
    if strategy == "stride":
        assert max_lag <= 10 * (1 + CONSUMERS), max_lag
    else:
        tolerance = math.sqrt(math.log(2 / LOTTERY_ALPHA) / (2 * SAMPLE)) + reorder
        assert abs(seen_a / SAMPLE - .7) <= tolerance, counts
    outputs = [d.output_for(task, q, durations[q]) for task, q in tasks.items()]
    return {"fresh_window": SAMPLE, "physical_window": window_end, "replays_in_window": window_end - SAMPLE,
            "total_wire_requests": len(rows), "total_wire_replays": sum(r["replay"] for r in rows),
            "replies_seen_at_stop": pre_stop, "subsequent_replies": len(rows) - pre_stop,
            "injected_empty_replays": len(observed_fault) - 1,
            "counts": dict(counts), "empty": SAMPLE - len(assignments), "reordered_refresh": True,
            "observed_max_prefix_lag": max_lag / 10, "physical_consumers": per_consumer,
            "elapsed_ms": {q: [(out["finished_ns"]-out["started_ns"])/1e6
                               for out in outputs if out["queue"] == q] for q in (a, b)}}


def refresh_and_replay(d):
    phase, a, b = "refresh", "refresh-a", "refresh-b"
    gate = d.directory / "release-active"
    task_a = d.submit_to(a, "refresh-active", gate=gate)
    task_b = d.submit_to(b, "refresh-pending")
    proxy = d.observing_proxy()
    lost = proxy.lose_once(ACQUIRE, block_after=True)
    file = d.directory / "refresh.json"
    publish_plan(file, 1, "stride", {a: 1, b: 0})
    proc = d.worker(phase, file, 1, proxy)
    wait_worker(proc, lost.is_set, "proxy committed the assignment before losing reply")
    try:
        publish_plan(file, 2, "stride", {a: 0, b: 0})
        wait_worker(proc, lambda: plan_log(proc, APPLIED, revision=2), "worker acknowledged idle plan")
    finally:
        with proxy.lock:
            proxy.blocked = False
    # The now-idle worker must still reconcile its original A assignment.
    wait_worker(proc, lambda: d.invocations(task_a), "original A handler entered after the plan changed")
    waiting = d.task(task_b)
    assert waiting["state"] == "queued" and waiting["attempt_count"] == 0
    assert d.task(task_a)["cancel_requested_at"] is None

    # Reject a stale plan, then reject a conflicting replay of the active epoch.
    # Observe both distinct writes before releasing the handler.
    publish_plan(file, 1, "stride", {a: 0, b: 1})
    wait_worker(proc, lambda: plan_log(proc, REJECTED), "stale plan rejected")
    publish_plan(file, 2, "stride", {a: 0, b: 1})
    wait_worker(proc, lambda: plan_log(proc, REJECTED, after=1), "conflicting revision rejected")
    gate.touch()
    wait_worker(proc, lambda: d.task(task_a)["state"] == "succeeded", "A settled while idle")
    # Allow an entire immediate-empty pacing and plan refresh cycle.
    time.sleep(1.25)
    assert len({identity(r) for r in acquired(proxy)}) == 1, "idle revision issued a new poll"
    assert d.task(task_b)["attempt_count"] == 0
    publish_plan(file, 3, "stride", {b: 1, a: 0})
    wait_worker(proc, lambda: d.task(task_b)["state"] == "succeeded", "free consumer picked B")
    proc.stop()
    rows = trace(d, proxy, (a, b), 1, phase)
    replays = [r for r in rows if r["replay"]]
    assert any(r["result"] == "assigned" and r["task"] == task_a for r in replays)
    assigned = [r for r in rows if not r["replay"] and r["result"] == "assigned"]
    assert [(r["queue"], r["task"]) for r in assigned] == [(a, task_a), (b, task_b)]
    assert all(len(d.invocations(t)) == 1 for t in (task_a, task_b))
    d.output_for(task_a, a)
    d.output_for(task_b, b)
    return {"dropped_assignment_reconciled": True, "original_attempt": assigned[0]["attempt"],
            "idle_fresh_polls": 0, "stale_and_conflicting_revisions_rejected": True,
            "old_task": task_a, "next_task": task_b}


def physical_observation(d, proxy, proc, entered, primary, secondary):
    """Read the actual persisted Console view while both route handlers are still gated."""
    sessions = {r["json"]["queue"]: json.loads(r["response"])["id"]
                for r in proxy.commands("/v1/worker-sessions")}
    assert set(sessions) == {primary, secondary}

    def inspect(queue):
        return d.checked("GET", "/v1/console/workers/inspect?worker_session_id=" + sessions[queue])

    expected = next(row for row in entered if row["queue"] == primary)
    def ready():
        # An early sample may describe preparation. Wait for the real primary
        # executing identity and its masked sibling from the same snapshot.
        detail = inspect(primary)
        items = detail["slots"]["items"]
        if (detail["worker"]["occupied_process_slots"] == 2 and len(items) == 2
                and any(row["task_id"] == expected["task"] and row["attempt_id"] == expected["attempt"]
                        and row["state"] == "executing" for row in items)
                and any(row["state"] == "unknown" for row in items)):
            return detail
        return None

    detail = wait_worker(proc, ready, "both executing slots and scope-correct observation", timeout=20)
    assert detail["worker"]["active_consumers"] == 2 and detail["worker"]["capacity"] == 2
    visible = [row for row in detail["slots"]["items"] if row["task_id"] == expected["task"]]
    assert len(visible) == 1 and visible[0]["attempt_id"] == expected["attempt"]
    assert visible[0]["consumer_id"] is not None and visible[0]["link_diagnostic"] is None
    other, = [row for row in detail["slots"]["items"] if row["slot_id"] != visible[0]["slot_id"]]
    assert other["state"] == "unknown"
    assert all(other[key] is None for key in ("task_id", "attempt_id", "consumer_id", "link_diagnostic"))
    # The secondary route must not claim or publish an additional physical pool.
    second = inspect(secondary)
    assert second["worker"]["snapshot_sequence"] is None and second["slots"]["items"] == []
    return {"occupied": detail["worker"]["occupied_process_slots"], "primary_attempt": visible[0]["attempt_id"],
            "secondary_slot_state": other["state"], "secondary_has_separate_snapshot": False}


def shared_capacity(d):
    a, b, cap = "burst-a", "burst-b", 2
    gate = d.directory / "release-burst"
    tasks = {d.submit_to(q, f"burst-{q}-{i}", duration_ms=25, gate=gate): q
             for i in range(3) for q in (a, b)}
    proxy = d.observing_proxy()
    file = d.directory / "burst.json"
    publish_plan(file, 1, "stride", {a: 1, b: 1})
    proc = d.worker("burst", file, cap, proxy)
    try:
        wait_worker(proc, lambda: len([r for r in d.invocations() if r["task"] in tasks]) >= cap,
                    "both physical slots entered gated handlers")
        before = [r for r in d.invocations() if r["task"] in tasks]
        assert len(before) == len({r["pid"] for r in before}) == cap
        assert {r["queue"] for r in before} == {a, b}, before
        assert sum(d.task(t)["attempt_count"] for t in tasks) == cap
        assert len({identity(r) for r in acquired(proxy)}) == cap
        observation = physical_observation(d, proxy, proc, before, a, b)
        publish_plan(file, 2, "stride", {a: 0, b: 1})
        wait_worker(proc, lambda: plan_log(proc, APPLIED, revision=2), "plan changes while all slots busy")
        assert all(d.task(row["task"])["cancel_requested_at"] is None for row in before)
    finally:
        gate.touch()
    wait_worker(proc, lambda: all(d.task(t)["state"] == "succeeded" for t, q in tasks.items() if q == b),
                "B receives all new capacity")
    original_a = next(row["task"] for row in before if row["queue"] == a)
    assert all(d.task(t)["attempt_count"] == 0 for t, q in tasks.items() if q == a and t != original_a)
    publish_plan(file, 3, "stride", {a: 1, b: 0})
    wait_worker(proc, lambda: all(d.task(t)["state"] == "succeeded" for t in tasks), "all burst results")
    proc.stop()
    rows = trace(d, proxy, (a, b), cap, "burst")
    owners = {r["task"]: r["consumer"] for r in rows if r["result"] == "assigned"}
    assert set(owners) == set(tasks)
    intervals = [d.output_for(t, q) for t, q in tasks.items()]
    for consumer in range(cap):
        ordered = sorted((x["started_ns"], x["finished_ns"]) for x in intervals if owners[x["task"]] == consumer)
        assert ordered and all(right <= left for (_, right), (left, _) in zip(ordered, ordered[1:]))
    changes = [(r["started_ns"], 1) for r in intervals] + [(r["finished_ns"], -1) for r in intervals]
    peak = active = 0
    for _, delta in sorted(changes):
        active += delta
        peak = max(peak, active)
    assert active == 0 and peak == cap, "queue sessions multiplied or lost physical slots"
    assert len([r for r in d.invocations() if r["task"] in tasks]) == len(tasks)
    return {"physical_capacity": cap, "actual_peak_handlers": peak, "held_attempts": cap,
            "successful_once": len(tasks), "first_routes": sorted(x["queue"] for x in before),
            "physical_observation": observation}


def secondary_admission(d):
    """Start and Close must use the actual admitted secondary queue's session and owner."""
    from resource_admission.task import CLOSE, INSPECT, SESSIONS, START
    a, b = "admitted-a", "admitted-b"
    gate = d.directory / "release-governed"
    d.policy(capacity=1, offer_ms=30_000)
    task = d.submit_to(b, "second-route-governed", gate=gate, resource=d.resource_name)
    occurrence = {"scope": d.scope, "task_id": task, "generation": 1}
    proxy = d.observing_proxy()
    file = d.directory / "admitted.json"
    publish_plan(file, 1, "stride", {a: 0, b: 1})
    proc = d.worker("admitted", file, 1, proxy)
    try:
        wait_worker(proc, lambda: d.invocations(task), "secondary governed handler entered", timeout=45)
        snapshot = d.checked("POST", INSPECT, {"occurrence": occurrence})
        assert snapshot["state"] == "possibly_dispatched" and d.resource()["occupied"] == 1
        capable = {r["json"]["queue"]: json.loads(r["response"]) for r in proxy.commands(SESSIONS)}
        assert set(capable) == {a, b} and all("task_admission.v1" in s["features"] for s in capable.values())
        assert not proxy.commands("/v1/worker-sessions")
        source = snapshot["source"]
        owner = source["owner"]
        assert owner["worker_session_id"] == capable[b]["id"] != capable[a]["id"]
        assert owner["consumer_id"] == 0 and owner["task_id"] == task and owner["scope"] == d.scope
        starts = proxy.commands(START)
        assert len(starts) == 1 and starts[0]["json"]["owner"] == owner and starts[0]["status"] == 200
        assert next(iter(d.invocations(task)))["attempt"] == owner["attempt_id"]
    finally:
        gate.touch()
    wait_worker(proc, lambda: d.checked("POST", INSPECT, {"occurrence": occurrence})["state"] == "finished",
                "secondary source closed with terminal authority")
    wait_worker(proc, lambda: d.task(task)["state"] == "succeeded", "governed task result")
    proc.stop()
    closes = proxy.commands(CLOSE)
    assert len(closes) == 1 and closes[0]["status"] == 200
    close = json.loads(closes[0]["response"])
    assert closes[0]["json"]["owner"] == close["owner"] == owner and close["source"] == source
    final = d.checked("POST", INSPECT, {"occurrence": occurrence})
    assert final["source"] == source and final["state"] == "finished"
    assert len(d.invocations(task)) == 1 and d.resource()["occupied"] == 0
    d.output_for(task, b)
    rows = trace(d, proxy, (a, b), 1, "admitted")
    assert all(row["queue"] == b for row in rows)
    return {"task": task, "attempt": owner["attempt_id"], "actual_queue": b, "session": capable[b]["id"],
            "source": source["source_id"], "starts": len(starts), "closes": len(closes),
            "terminal": final["state"], "resource_occupied_after_close": 0}
