"""Same logical arrival order, work and release barriers; wall-clock request times are measured. MIT."""

import http.client
import json
import math
import time
from collections import Counter

from http_acceptance.harness import Process, eventually, exchange, unused_port
from workflow_acceptance.owned_scenarios import quote

CAP = 5
COHORT = 16
DURATION_MS = 60
RATE = {"numerator": 5, "period_ms": 500, "burst": 5}
GROUPS = {
    "default_group": "batch",
    "groups": [
        {"name": "support", "weight": 3, "queues": ["support"]},
        {"name": "batch", "weight": 1, "queues": []},
    ],
}


def replica(d):
    args = d.server_args(d.config)
    port = unused_port()
    args[args.index("--bind") + 1] = f"127.0.0.1:{port}"
    d.counter += 1
    process = Process(args, d.directory, f"replica-{d.counter}", d.environment)
    d.processes.append(process)

    def ready():
        assert process.process.poll() is None, "replica exited before readiness"
        try:
            return (
                exchange(f"http://127.0.0.1:{port}", "GET", "/health/ready", timeout=1)[
                    0
                ]
                == 200
            )
        except (OSError, http.client.HTTPException):
            return False

    eventually(ready, description="second coordinator ready")
    return process


def await_entries(d, tasks, count):
    def entered():
        entries = [entry for entry in d.invocations() if entry["task"] in tasks]
        assert len(entries) <= count, "more handlers entered than released slots"
        return entries if len(entries) == count else None

    return eventually(
        entered, timeout=50, description="bounded underlying handler starts"
    )


def wait_terminal(d, tasks):
    pending = set(tasks)

    def finished():
        for task in list(pending):
            state = d.task(task)["state"]
            assert state not in ("failed", "cancelled"), f"task {task} {state}"
            if state == "succeeded":
                pending.remove(task)
        return not pending

    eventually(finished, timeout=55, description="all fixture tasks complete")


def prewarm(d):
    gate = d.directory / "warm"
    tasks = [
        d.submit_to(q, f"warm-{q}-{i}", gate=gate)
        for q in ("batch", "support")
        for i in range(CAP)
    ]
    await_entries(d, tasks, 2 * CAP)
    gate.touch()
    wait_terminal(d, tasks)


def configure(d, mode, *, capacity=CAP):
    policy = {
        "enabled": True,
        "max_occupancy": capacity,
        "rate": RATE,
        "offer_timeout_ms": 30_000,
        "start_timeout_ms": 15_000,
    }
    if mode == "weighted":
        policy["fair_sharing"] = GROUPS
    command = {
        "scope": d.scope,
        "name": d.resource_name,
        "request_id": f"policy-{d.resource_name}",
        "expected_revision": None,
        "reason": "local fair sharing comparison",
        "policy": policy,
    }
    receipt = d.checked("POST", "/v1/resources/put", command)
    replay = d.checked("POST", "/v1/resources/put", command)
    assert replay["already_accepted"] and not receipt["already_accepted"]
    return receipt


def observations(d):
    rows = d.sql(
        "SELECT row_to_json(x) FROM (SELECT task,ticket,requested,offered,generation "
        "FROM fixture_offers WHERE resource="
        + quote(d.resource_name)
        + " ORDER BY seq) x"
    )
    return [json.loads(row) for row in rows.splitlines()]


def history(d):
    cursor = None
    descending = []
    while True:
        page = d.checked(
            "POST",
            "/v1/resources/decisions",
            {"scope": d.scope, "name": d.resource_name, "limit": 7, "before": cursor},
        )
        items = page["items"]
        assert len(items) <= 7
        descending.extend(items)
        cursor = page["next"]
        if cursor is None:
            break
        assert items and int(cursor) == int(items[-1]["sequence"])
    seqs = [int(item["sequence"]) for item in descending]
    assert seqs == sorted(set(seqs), reverse=True)
    return list(reversed(descending))


def occupancy_peak(d):
    return int(
        d.sql(
            "SELECT max(occupied) FROM fixture_occupancy WHERE resource="
            + quote(d.resource_name)
        )
    )


def execution_peak(outputs):
    points = [(value["started_ns"], 1) for value in outputs]
    points += [(value["finished_ns"], -1) for value in outputs]
    total = peak = 0
    for _, delta in sorted(
        points
    ):  # Half-open intervals: a finish precedes a simultaneous start.
        total += delta
        peak = max(total, peak)
    assert total == 0
    return peak


def started_credits(d, tasks):
    # Neither the durable offer nor the Python entry timestamp debits rate.
    # Observe each source accepted by Start and its resource update in the
    # same PostgreSQL transaction, then reproduce the fixed-point bucket.
    resource = quote(d.resource_name)
    rows = d.sql(
        "SELECT row_to_json(x) FROM (SELECT s.task,s.source,s.accepted,d.at_ms,d.credit "
        "FROM fixture_starts s FULL JOIN fixture_debits d USING(xid,resource) "
        "WHERE resource=" + resource + " ORDER BY d.seq) x"
    )
    samples = [json.loads(row) for row in rows.splitlines()]
    assert Counter(row["task"] for row in samples) == Counter(tasks)
    assert len({row["source"] for row in samples}) == len(tasks)
    token = RATE["period_ms"]
    burst = RATE["burst"] * token
    credit = burst
    previous = samples[0]["accepted"]
    for row in samples:
        now = row["accepted"]
        assert row["source"] and now == row["at_ms"] and now >= previous
        credit = min(burst, credit + (now - previous) * RATE["numerator"])
        assert credit >= token, "Start exceeded the configured rate"
        credit -= token
        assert credit == row["credit"], "Start debited the wrong credit"
        previous = now
    return samples


def utilization(outputs, cohort, start, end):
    # Sum half-open handler intervals clipped to the observation window. This
    # measures occupied handler slots (including fixture gates), not CPU time.
    busy = Counter({"holders": 0, "batch": 0, "support": 0})
    for out in outputs:
        left = max(start, out["started_ns"])
        right = min(end, out["finished_ns"])
        busy[cohort.get(out["task"], "holders")] += max(0, right - left)
    capacity_ns = CAP * (end - start)
    assert 0 < sum(busy.values()) <= capacity_ns
    return {
        "window_ms": round((end - start) / 1e6, 3),
        "slot_capacity_ms": round(capacity_ns / 1e6, 3),
        "busy_slot_ms": round(sum(busy.values()) / 1e6, 3),
        "busy_slot_ms_by_work": {g: round(ns / 1e6, 3) for g, ns in busy.items()},
        "busy_slot_fraction": round(sum(busy.values()) / capacity_ns, 5),
    }


def distribution(waits):
    ordered = sorted(waits)
    return {
        f"p{p}": ordered[math.ceil(p * len(ordered) / 100) - 1] for p in (50, 95, 99)
    }


def summarize(
    d, mode, blockers, cohort, entered_at_restart, outputs, release_ns, observed_ns
):
    rows = observations(d)
    all_tasks = set(blockers) | set(cohort)
    assert len(rows) == len(all_tasks) and {r["task"] for r in rows} == all_tasks
    assert all(
        row["generation"] == 1 and row["offered"] >= row["requested"] for row in rows
    )
    starts = started_credits(d, all_tasks)
    assert {r["task"] for r in rows[:CAP]} == set(blockers)
    offers = rows[CAP:]
    sequence = [cohort[row["task"]] for row in offers]
    for group in ("batch", "support"):
        tickets = [row["ticket"] for row in offers if cohort[row["task"]] == group]
        assert tickets == sorted(tickets), "FIFO within each group"
    # All 32 requests were queued behind the five held sources. Both groups
    # remain eligible until the last offer to whichever group drains first.
    counts = Counter()
    contended = []
    max_error = 0
    for group in sequence:
        if max(counts.values(), default=0) == COHORT:
            break
        counts[group] += 1
        contended.append(group)
        max_error = max(max_error, abs(counts["support"] - 0.75 * len(contended)))
    assert len(contended) >= COHORT
    if mode == "weighted":
        decisions = history(d)
        assert len(decisions) == len(rows)
        for i, (decision, row) in enumerate(zip(decisions, rows, strict=True)):
            group = cohort.get(row["task"], "batch")
            both = CAP <= i < CAP + len(contended)
            assert decision["subject"] == {
                "kind": "task",
                "task_id": row["task"],
                "generation": 1,
            }
            assert (
                decision["scope"] == d.scope and decision["resource"] == d.resource_name
            )
            assert int(decision["policy_revision"]) == 1
            assert decision["queue"] == group and decision["group"] == group
            assert decision["weight"] == (3 if group == "support" else 1)
            assert 1 <= decision["cycle_round"] <= decision["weight"]
            assert decision["eligible_groups"] == (2 if both else 1)
            assert decision["reason"] == ("weighted" if both else "sole_eligible")
            assert int(decision["offer_generation"]) == row["generation"]
            assert int(decision["offered_at"]) == row["offered"]
        assert all(
            contended[i : i + 4].count("support") == 3
            for i in range(len(contended) - 3)
        )
        assert max_error <= 0.75 and sequence.index("support") < 4
    else:
        assert sequence == ["batch"] * COHORT + ["support"] * COHORT
    peak = occupancy_peak(d)
    underlying = execution_peak(outputs)
    assert d.resource()["occupied"] == 0 and peak == CAP and underlying == CAP
    # A successful single Ledgence attempt alone does not prove the underlying
    # handler was entered only once. Match independent entry markers to results.
    entries = [entry for entry in d.invocations() if entry["task"] in all_tasks]
    assert Counter(entry["task"] for entry in entries) == Counter(all_tasks)
    by_task = {out["task"]: out for out in outputs}
    for entry in entries:
        assert all(by_task[entry["task"]][k] == v for k, v in entry.items())
    elapsed = (observed_ns - release_ns) / 1e9
    witness = {
        "arrivals": list(cohort.values()),
        "offers": rows,
        "accepted_starts": starts,
        "outcomes": outputs,
        "first_release_ns": release_ns,
        "all_terminal_observed_ns": observed_ns,
    }
    if mode == "weighted":
        witness["decisions"] = decisions
    (d.directory / f"{mode}-samples.json").write_text(
        json.dumps(witness, indent=2) + "\n"
    )
    return {
        "capacity": CAP,
        "rate": RATE,
        "held_before_contention": len(blockers),
        "ready_at_release": dict(Counter(cohort.values())),
        "entered_before_coordinators_restarted": entered_at_restart,
        "contended_opportunities": {g: counts[g] for g in ("batch", "support")},
        "contended_count": len(contended),
        "first_support_opportunity": 1 + sequence.index("support"),
        "max_absolute_support_offer_error_against_3_to_1": max_error,
        "offer_order": sequence,
        "admission_peak": peak,
        "handler_peak": underlying,
        "verified_starts_and_rate_debits": len(starts),
        "handler_utilization": {
            "first_holder_entry_to_last_handler_exit": utilization(
                outputs,
                cohort,
                min(o["started_ns"] for o in outputs),
                max(o["finished_ns"] for o in outputs),
            ),
            "first_release_to_all_terminal_observed_including_restart": utilization(
                outputs, cohort, release_ns, observed_ns
            ),
        },
        "registration_to_offer_ms": {
            g: distribution(
                [
                    r["offered"] - r["requested"]
                    for r in offers
                    if cohort[r["task"]] == g
                ]
            )
            for g in ("batch", "support")
        },
        "cohort_release_to_terminal_seconds": round(elapsed, 3),
        "cohort_completions_per_second_including_restart": round(
            len(cohort) / elapsed, 2
        ),
        "single_attempt_successes": len(outputs),
        "observed_handler_entries": len(entries),
    }


def scenario(d, mode):
    d.resource_name = f"fixture-{mode}"
    configure(d, mode)
    secondary = replica(d)
    release = [d.directory / f"{mode}-release-{i}" for i in range(CAP)]
    blockers = [
        d.submit_to("batch", f"{mode}-holder-{i}", gate=gate, resource=d.resource_name)
        for i, gate in enumerate(release)
    ]
    active = await_entries(d, blockers, CAP)
    assert (
        len({entry["pid"] for entry in active}) == CAP
        and d.resource()["occupied"] == CAP
    )
    cohort_gate = d.directory / f"{mode}-cohort"
    cohort = {
        d.submit_to(
            q,
            f"{mode}-{q}-{i}",
            duration_ms=DURATION_MS,
            gate=cohort_gate,
            resource=d.resource_name,
        ): q
        for q in ("batch", "support")
        for i in range(COHORT)
    }
    waiting = d.sql(
        "SELECT count(*) FROM task_admission_requests WHERE state='waiting' "
        "AND resource_name=" + quote(d.resource_name)
    )
    assert int(waiting) == len(cohort) and len(observations(d)) == CAP
    release_ns = time.monotonic_ns()
    for i in range(2):
        release[i].touch()
        await_entries(d, cohort, i + 1)
        assert d.resource()["occupied"] == CAP
    secondary.kill()
    d.server.kill()
    d.start_server()
    secondary = replica(d)
    assert len(observations(d)) == CAP + 2 and d.resource()["occupied"] == CAP
    for gate in release[2:]:
        gate.touch()
    cohort_gate.touch()
    wait_terminal(d, [*blockers, *cohort])
    observed_ns = time.monotonic_ns()
    outputs = [d.output_for(t, "batch") for t in blockers]
    outputs += [d.output_for(t, q, DURATION_MS) for t, q in cohort.items()]
    secondary.stop()
    return summarize(d, mode, blockers, cohort, 2, outputs, release_ns, observed_ns)
