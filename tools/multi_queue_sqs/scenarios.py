"""Compare native dedicated and shared workers using the same routes, slots and workload (MIT)."""

from collections import Counter
import json
import math
import time

from http_acceptance.harness import eventually
from worker_poll_plan.fixture import plan_log
from .fixture import PAY, REPORT, QUEUES


def completed(d, tasks):
    eventually(
        lambda: all(d.task(t)["state"] == "succeeded" for t in tasks),
        timeout=60,
        description="each submitted task succeeded",
    )


def peak(outputs):
    events = [
        (output[key], delta)
        for output in outputs
        for key, delta in (("started_ns", 1), ("finished_ns", -1))
    ]
    active = highest = 0
    for _, delta in sorted(events):
        active += delta
        highest = max(highest, active)
    assert active == 0
    return highest


def issued(worker):
    counts = Counter()
    for line in worker.stderr_path.read_bytes().splitlines(keepends=True):
        if not line.endswith(b"\n"):
            continue
        row = json.loads(line)
        f = row.get("fields", {})
        if f.get("event") == "worker.poll.issued":
            counts[f["queue"]] += 1
    return counts


def overlap(d, label):
    gates = {q: d.directory / (label + "-" + q + ".gate") for q in QUEUES}
    tasks = {q: [] for q in QUEUES}
    try:
        for i in range(6):
            tasks[PAY].append(d.submit_to(PAY, f"{label}-paid-{i}", gate=gates[PAY]))
        eventually(
            lambda: len([r for r in d.invocations() if r["task"] in tasks[PAY]]) == 5,
            timeout=45,
            description="five real payments handlers active",
        )
        for i in range(3):
            tasks[REPORT].append(
                d.submit_to(REPORT, f"{label}-reports-{i}", gate=gates[REPORT])
            )
        eventually(
            lambda: (
                len([r for r in d.invocations() if r["task"] in tasks[REPORT]]) == 3
            ),
            timeout=45,
            description="all other slots do reports while payments is saturated",
        )
        assert d.inspect(PAY)["occupied"] == "5"
        held = d.holds(PAY)
        assert len(held) == 5 and all(h["task_id"] in tasks[PAY] for h in held)
    finally:
        for gate in gates.values():
            gate.touch()
    completed(d, [t for queue in QUEUES for t in tasks[queue]])
    outputs = {q: [d.output_for(t, q) for t in tasks[q]] for q in QUEUES}
    assert peak(outputs[PAY]) == 5
    assert peak(outputs[PAY] + outputs[REPORT]) == 8
    return {"payment_handler_peak": 5, "total_handler_peak": 8, "concurrent_reports": 3}


def benchmark(d, label, workers):
    # Identical replayable sequence on both topologies: one worker already hot
    # or both dedicated workers already hot, same total eight consumers, wait,
    # task mix, package and measured process/orchestrator/broker boundaries.
    submit_clock = {}
    tasks = {q: [] for q in QUEUES}
    durations = {PAY: 150, REPORT: 70}
    before = time.monotonic_ns()
    offsets = [issued(w) for w in workers]
    for i in range(16):
        for q in QUEUES:
            sending = time.monotonic_ns()
            task = d.submit_to(q, f"{label}-work-{q}-{i}", duration_ms=durations[q])
            submit_clock[task] = sending
            tasks[q].append(task)
    completed(d, [t for q in QUEUES for t in tasks[q]])
    end = time.monotonic_ns()
    planned = Counter()
    for w, previous in zip(workers, offsets, strict=True):
        planned.update(issued(w) - previous)
    # Observe all outcomes for receives first issued *inside* the measured
    # interval, including calls which answer after the last task settles.
    eventually(
        lambda: all(
            r["end"] is not None
            for r in d.meter.snapshot()
            if before <= r["start"] <= end
        ),
        timeout=12,
        description="benchmark receive cohort finished",
    )
    wire = d.meter.counts(before, end)
    entered_before = Counter(
        r["queue"]
        for r in d.meter.snapshot()
        if r["start"] < before and r["end"] is not None and r["end"] >= before
    )
    all_outputs = []
    metrics = {}
    for q in QUEUES:
        outputs = [d.output_for(t, q, durations[q]) for t in tasks[q]]
        all_outputs.extend(outputs)
        delays = sorted(
            (o["started_ns"] - submit_clock[o["task"]]) / 1e6 for o in outputs
        )
        assert len(delays) == 16 and delays[0] >= 0
        metrics[q] = {
            "completed": len(outputs),
            "submit_to_handler_ms": delays,
            "p95_submit_to_handler_ms": round(
                delays[math.ceil(0.95 * len(delays)) - 1], 2
            ),
            "active_handler_peak": peak(outputs),
            "wire_receive_requests": dict(wire.get(q, {})),
            "receives_in_flight_at_measurement_start": entered_before[q],
            "logical_polls": planned[q] if label == "shared" else None,
        }
    total_peak = peak(all_outputs)
    assert 1 <= total_peak <= 8 and metrics[PAY]["active_handler_peak"] <= 5
    assert all(
        metrics[q]["wire_receive_requests"].get("nonempty", 0) >= 1 for q in QUEUES
    )
    if label == "shared":
        assert all(planned[q] > 0 for q in QUEUES)
    return {
        "per_queue": metrics,
        "active_handler_peak": total_peak,
        "elapsed_ms": round((end - before) / 1e6, 2),
        "logical_poll_observation": "worker.poll.issued"
        if label == "shared"
        else "single-queue workers do not emit planner decisions",
    }


def reports_burst(d, label, worker, plan):
    # Eight reporting handlers stay inside their process until the observed
    # target is reached. Only the shared topology can borrow the five slots
    # otherwise dedicated to the idle payments queue.
    if plan:
        d.set_plan(plan, 2, {PAY: 1, REPORT: 0})
        eventually(
            lambda: plan_log(worker, "worker poll plan loaded", revision=2),
            description="cold-only revision",
        )
        eventually(
            lambda: (
                sum(r["queue"] == PAY and r["end"] is None for r in d.meter.snapshot())
                == 8
            ),
            timeout=18,
            description="eight in-flight cold receives",
        )
    target = 8 if plan else 3
    gate = d.directory / (label + "-reports-burst.gate")
    submitted = {}
    try:
        if plan:
            d.set_plan(plan, 3, {PAY: 0, REPORT: 1})
        for i in range(8):
            sent = time.monotonic_ns()
            task = d.submit_to(REPORT, f"{label}-burst-{i}", gate=gate)
            submitted[task] = sent

        def running():
            starts = [row for row in d.invocations() if row["task"] in submitted]
            return starts if len(starts) == target else None

        active = eventually(
            running, timeout=30, description="expected reports-only concurrent handlers"
        )
        assert len({row["pid"] for row in active}) == target
    finally:
        gate.touch()
    completed(d, submitted)
    outputs = [d.output_for(t, REPORT) for t in submitted]
    assert peak(outputs) == target
    starts = sorted(
        (row["started_ns"] - submitted[row["task"]]) / 1e6 for row in active
    )
    return {
        "tasks": 8,
        "active_report_handlers_before_gate_opened": target,
        "report_handler_peak": target,
        "gated_first_wave_submit_to_handler_ms": starts,
        "shared_plan_change": "payments-only -> reports-only" if plan else None,
    }


def compare(d):
    d.change(PAY, 5, None)
    results = {}
    for label in ("dedicated", "shared"):
        if label == "dedicated":
            workers = [d.start_consumer("each-pay", 5, queue=PAY)]
            workers.append(d.start_consumer("each-reports", 3, queue=REPORT))
        else:
            path = d.directory / "shared-plan.json"
            d.set_plan(path, 1, {PAY: 5, REPORT: 3})
            workers = [d.start_consumer("shared", 8, plan=path)]
        warm = [d.submit_to(q, f"{label}-warm-{q}") for q in QUEUES]
        completed(d, warm)
        high_water = overlap(d, label)
        metrics = benchmark(d, label, workers)
        burst = reports_burst(d, label, workers[0], path if label == "shared" else None)
        for w in workers:
            w.stop()
        eventually(
            lambda: d.inspect(PAY)["occupied"] == "0",
            timeout=20,
            description="released payments capacity",
        )
        results[label] = {
            "reports_only_burst": burst,
            "gated_saturation": high_water,
            "workload": metrics,
            "concurrency": 8,
            "wait_ms": 1000,
        }
    return results


def rebound(d):
    path = d.directory / "rebind-plan.json"
    d.set_plan(path, 1, {PAY: 1, REPORT: 0})
    proxy = d.observing_proxy()
    gate = d.directory / "original-payment.gate"
    pay = d.submit_to(PAY, "reconcile-before-new-route", gate=gate)
    lost = proxy.lose_once(
        "/v1/dispatch/claim",
        lambda c: c["dispatch"]["task_id"] == pay,
        block_after=True,
    )
    start = time.monotonic_ns()
    w = d.start_consumer("reconcile", 1, plan=path, wait=None, proxy=proxy)
    try:
        assert lost.wait(timeout=35), "forwarded claim did not lose its response"
        d.set_plan(path, 2, {PAY: 0, REPORT: 1})
        eventually(
            lambda: plan_log(w, "worker poll plan loaded", revision=2),
            description="revised plan while claim unresolved",
        )
        other = d.submit_to(REPORT, "after-uncertain-claim")
    finally:
        with proxy.lock:
            proxy.blocked = False
    try:
        original = d.started(pay)[0]
        # The recovered original crossed dispatch despite its route now having
        # zero weight. Pause every route with the real handler still active.
        d.set_plan(path, 3, {PAY: 0, REPORT: 0})
        eventually(
            lambda: plan_log(w, "worker poll plan loaded", revision=3),
            description="pause during execution",
        )
        assert d.task(pay)["state"] == "active"
        assert d.task(other)["attempt_count"] == 0
    finally:
        gate.touch()
    completed(d, [pay])
    claims = proxy.commands(
        "/v1/dispatch/claim", lambda c: c["dispatch"]["task_id"] == pay
    )
    assert len(claims) >= 2 and all(row["body"] == claims[0]["body"] for row in claims)
    assert (
        claims[0]["json"]["acquisition"]["queue"] == PAY
        and len(d.invocations(pay)) == 1
    )
    assert d.output_for(pay, PAY)["attempt"] == original["attempt"]
    receives = [r for r in d.meter.snapshot() if r["start"] >= start]
    assert receives and all(r["wait_seconds"] == 1 for r in receives), (
        "implicit shared SQS wait differs from 1s"
    )
    count = len(d.meter.snapshot())
    delayed = d.submit_to(REPORT, "paused-until-explicit-revision")
    time.sleep(1.25)
    assert len(d.meter.snapshot()) == count and d.task(delayed)["attempt_count"] == 0
    d.set_plan(path, 4, {PAY: 1, REPORT: 1})
    completed(d, [delayed, other])
    assert d.output_for(delayed, REPORT) and d.output_for(other, REPORT)
    w.stop()
    return {
        "plan_changed_while_original_handler_running": True,
        "same_claim_bytes": True,
        "claim_requests_for_original": len(claims),
        "one_original_invocation": True,
        "implicit_receive_wait_seconds": 1,
        "idle_new_receives": 0,
    }
