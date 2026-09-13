"""Separate-binary PostgreSQL/Python acceptance scenarios, not mock-service tests."""

import http.client
import json
import math
import os
import signal
import time
import threading
import urllib.parse

from .harness import eventually, exchange, unused_port


def report(attempt):
    return attempt["settlement"]["command"]["report"]["report"]


def success(d, name="success"):
    command = d.submission(name, exact=[9007199254740993, 18446744073709551615, -0.0, "\0"])
    task = d.submit(command)
    _, attempt = d.terminal(task["task_id"])
    result = report(attempt)
    assert result["outcome"]["output"]["event"]["data"] == command["input"]["data"]
    exact = result["outcome"]["output"]["event"]["data"]["exact"]
    assert type(exact[0]) is int and type(exact[1]) is int
    assert type(exact[2]) is float and math.copysign(1, exact[2]) == -1
    assert result["outcome"]["output"]["dependency"] == "packaged"
    assert result["outcome"]["output"]["event"]["traceparent"] == command["origin_trace"]["traceparent"]
    assert result["digest"] == task["descriptor"]["digest"]
    assert len(d.invocations(task["task_id"])) == 1
    return task, attempt


def warm_cache_and_cli(d):
    before = d.artifacts.downloads()
    worker = d.start_worker()
    first, a = success(d, "warm-first")
    second, b = success(d, "warm-second")
    assert report(a)["process_id"] == report(b)["process_id"]
    assert report(b)["reused_process"]
    assert d.artifacts.downloads() == before + 1
    for command, extra in [("inspect", []), ("attempt", ["--attempt", a["event"]["ldgattemptid"]]),
                           ("history", ["--after", "0"])]:
        result = d.command("ledgence", ["task", command, "--server", d.server_url,
                                       "--tenant", d.scope["tenant_id"], "--namespace",
                                       d.scope["namespace"], "--task", first["task_id"]] + extra)
        assert result
    events = d.history(first["task_id"])
    assert [x["sequence"] for x in events] == list(range(1, len(events) + 1))
    assert d.history(first["task_id"], events[-1]["sequence"]) == []
    assert worker.stop() == 1
    replacement = d.start_worker()
    _, c = success(d, "warm-restart")
    assert report(c)["process_id"] != report(b)["process_id"]
    assert d.artifacts.downloads() == before + 1
    replacement.stop()
    return "remote store, full data/trace, warm PID reuse, cache across restart, operator inspection"


def committed_response_loss(d):
    proxy = d.proxy()
    lost = [proxy.lose_once("/v1/acquisitions"),
            proxy.lose_once("/v1/renewals", lambda c: c["intent"] == "dispatch"),
            proxy.lose_once("/v1/settlements")]
    task = d.submit(d.submission("lost-control-replies"))
    worker = d.start_worker(proxy.url)
    d.terminal(task["task_id"])
    assert all(event.is_set() for event in lost)
    # Durable terminal state can be visible before the worker learns the receipt.
    worker.stop()
    assert len(d.invocations(task["task_id"])) == 1
    for path in ("/v1/acquisitions", "/v1/renewals", "/v1/settlements"):
        records = proxy.commands(path)
        first = records[0]
        assert first["status"] == 200, first
        retries = [r for r in records if r["body"] == first["body"]]
        assert len(retries) >= 2, (path, records)
        assert len({r["request_id"] for r in retries}) >= 2
    reasons = [x["event"]["reason"] for x in d.history(task["task_id"])]
    for reason in ("claimed", "dispatch_authorized", "report_accepted", "succeeded"):
        assert reasons.count(reason) == 1, reasons
    lost_submit = proxy.lose_once("/v1/tasks")
    command = d.submission(" invoice:é:lost-submission ")
    try:
        exchange(proxy.url, "POST", "/v1/tasks", command)
        raise AssertionError("expected closed connection after committed submission")
    except (OSError, http.client.HTTPException):
        pass
    assert lost_submit.is_set()
    accepted = d.submit(command)
    replay = d.submit(command)
    assert accepted["task_id"] == replay["task_id"]
    assert accepted["idempotency_key"] == command["idempotency_key"]
    changed = json.loads(json.dumps(command))
    changed["input"]["data"]["changed"] = True
    assert exchange(d.server_url, "POST", "/v1/tasks", changed)[0] == 409
    assert d.cancel(accepted["task_id"]) == "cancelled"
    registration = dict(scope=d.scope, queue=d.queue, concurrency=1)
    lost_session = proxy.lose_once("/v1/worker-sessions")
    try:
        exchange(proxy.url, "POST", "/v1/worker-sessions", registration)
        raise AssertionError("expected lost session-open response")
    except (OSError, http.client.HTTPException):
        pass
    assert lost_session.is_set()
    first_session = json.loads(proxy.commands("/v1/worker-sessions")[-1]["response"])
    status, _, second_session = exchange(proxy.url, "POST", "/v1/worker-sessions", registration)
    assert status == 200
    assert first_session["id"] != second_session["id"]
    return "socket loss after committed acquisition, dispatch, settlement and submission; immutable retries"


def normalized_submission(d):
    command = d.submission("normalized-input")
    command["input"]["data"] = {"a": 1.0, "b": -0.0}
    accepted = d.submit(command)
    equivalent = json.loads(json.dumps(command))
    equivalent["input"]["attempt_timeout_ms"] = 300_000
    equivalent["input"]["correlation_key"] = None
    encoded = json.dumps(equivalent, sort_keys=True).replace(': 1.0,', ': 1e0,').replace(': -0.0}', ': -0e0}').encode()
    status, _, replay = exchange(d.server_url, "POST", "/v1/tasks", encoded)
    assert status == 200 and replay["task_id"] == accepted["task_id"]
    for data in ({"a": 1, "b": -0.0}, {"a": 1.0, "b": 0.0}):
        changed = json.loads(json.dumps(command))
        changed["input"]["data"] = data
        assert exchange(d.server_url, "POST", "/v1/tasks", changed)[0] == 409
    assert d.cancel(accepted["task_id"]) == "cancelled"
    return "normalized defaults/object order/exponents replay; integer/float and signed zero remain distinct"


def cancellation_and_failures(d):
    queued = d.submit(d.submission("cancel-queued"))
    assert d.cancel(queued["task_id"]) == "cancelled"
    assert d.task(queued["task_id"])["attempt_count"] == 0
    worker = d.start_worker()
    gated = d.submit(d.submission("cancel-active", "gate", gate=str(d.directory / "gate-cancel")))
    started = d.started(gated["task_id"])[0]
    assert d.cancel(gated["task_id"]) in ("active", "cancelled")
    d.terminal(gated["task_id"], "cancelled", timeout=45)
    assert len(d.invocations(gated["task_id"])) == 1
    eventually(lambda: not pid_exists(started["pid"]), description="cancelled Python process to exit")

    business = d.submit(d.submission("business-failure", "business"))
    task, attempt = d.terminal(business["task_id"], "failed")
    assert task["attempt_count"] == 1
    assert report(attempt)["outcome"]["status"] == "failure"

    transient = d.submit(d.submission("runtime-retry", "interrupt_once"))
    task, attempt = d.terminal(transient["task_id"])
    assert task["attempt_count"] == 2
    assert attempt["descriptor"] == transient["descriptor"]
    assert [r["number"] for r in d.invocations(transient["task_id"])] == [1, 2]

    exhausted = d.submission("runtime-exhaustion", "interrupt")
    exhausted["input"]["retry_policy"]["max_attempts"] = 2
    task = d.submit(exhausted)
    final, _ = d.terminal(task["task_id"], "failed")
    assert final["attempt_count"] == 2

    delayed = d.submission("cancel-delayed", "interrupt")
    delayed["input"]["retry_policy"]["retry_delay_ms"] = 30_000
    task = d.submit(delayed)
    eventually(lambda: any(e["event"]["reason"] == "retry_scheduled" for e in d.history(task["task_id"])),
               description="delayed retry")
    assert d.cancel(task["task_id"]) == "cancelled"
    assert d.task(task["task_id"])["attempt_count"] == 1
    worker.stop()
    return "queued/active/delayed cancellation, business failure, infrastructure retry, bounded attempts"


def pid_exists(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def worker_crash_recovery(d):
    scanner, base = d.start_server(unused_port())
    worker = d.start_worker()
    submitted = d.submit(d.submission("worker-crash", "first_gate", gate=str(d.directory / "gate-crash")))
    first = d.started(submitted["task_id"])[0]
    initial = d.attempt(submitted["task_id"])
    worker.kill()
    replacement = d.start_worker(base)
    task, attempt = d.terminal(submitted["task_id"], timeout=100)
    assert task["attempt_count"] == 2
    old = d.attempt(task["task_id"], first["attempt"])
    assert old["state"] == "lost"
    assert attempt["descriptor"] == submitted["descriptor"]
    assert attempt["event"]["ldgrunid"] == initial["event"]["ldgrunid"]
    assert attempt["event"]["id"] != initial["event"]["id"]
    assert attempt["lease"]["owner"]["worker_session_id"] != initial["lease"]["owner"]["worker_session_id"]
    assert [r["number"] for r in d.invocations(task["task_id"])] == [1, 2]
    assert sum(x["event"]["reason"] == "lease_expired" for x in d.history(task["task_id"])) == 1
    replacement.stop()
    scanner.stop()
    return "SIGKILL worker; production 60s lease expiry and automatic scanner retry with fresh session"


def orchestrator_restart(d):
    queued = d.submit(d.submission("server-restart-queued"))
    d.server.kill()
    d.server, _ = d.start_server()
    assert d.task(queued["task_id"])["descriptor"] == queued["descriptor"]
    proxy = d.proxy()
    worker = d.start_worker(proxy.url)
    d.terminal(queued["task_id"])

    gated = d.submit(d.submission("server-restart-active", "gate", gate=str(d.directory / "gate-server")))
    d.started(gated["task_id"])
    before = d.attempt(gated["task_id"])
    d.server.kill()
    d.server, _ = d.start_server()
    (d.directory / "gate-server").touch()
    _, after = d.terminal(gated["task_id"])
    assert after["event"]["ldgattemptid"] == before["event"]["ldgattemptid"]
    assert len(d.invocations(gated["task_id"])) == 1

    loss = proxy.lose_once("/v1/settlements", block_after=True)
    settled = d.submit(d.submission("server-restart-accepted"))
    assert loss.wait(30)
    receipt = d.attempt(settled["task_id"])["settlement"]
    d.server.kill()
    d.server, _ = d.start_server()
    with proxy.lock:
        proxy.blocked = False
    d.terminal(settled["task_id"])
    assert d.attempt(settled["task_id"])["settlement"] == receipt
    worker.stop()
    receipts = proxy.commands("/v1/settlements", lambda c: c["owner"]["attempt_id"] ==
                              receipt["command"]["owner"]["attempt_id"])
    assert len(receipts) >= 2
    assert any(json.loads(r["response"])["already_accepted"] for r in receipts[1:])
    assert len(d.invocations(settled["task_id"])) == 1
    return "server process restart preserves queued, active and durably accepted task outcomes"


def network_outage(d):
    proxy = d.proxy()
    worker = d.start_worker(proxy.url)
    task = d.submit(d.submission("control-outage", "first_gate", gate=str(d.directory / "gate-outage")))
    first = d.started(task["task_id"])[0]
    with proxy.lock:
        proxy.blocked = True
    # The actual driver must stop work from local authority; no test clock or DB timestamp edits.
    eventually(lambda: not pid_exists(first["pid"]), timeout=85,
               description="Python stop after loss of conservative authority")
    with proxy.lock:
        proxy.blocked = False
    final, attempt = d.terminal(task["task_id"], timeout=100)
    assert final["attempt_count"] == 2
    assert attempt["event"]["ldgattemptid"] != first["attempt"]
    worker.stop()
    return "prolonged real control disconnection stops old work and recovers a new attempt"


def capacity_and_multiple_servers(d):
    d.publish("alternate", "1.0.0")
    second, base = d.start_server(unused_port())
    worker = d.start_worker(base, concurrency=2)
    gate = d.directory / "gate-capacity"
    tasks = [d.submit(d.submission(f"capacity-{i}", "gate", program="invoice" if i % 2 else "alternate",
                                  gate=str(gate))) for i in range(6)]
    task_ids = {task["task_id"] for task in tasks}
    started = eventually(lambda: (r if len(r := [r for r in d.invocations() if r["task"] in task_ids]) >= 2
                                  else None), description="two reserved consumers")
    assert len(started) == 2
    assert len({r["pid"] for r in started}) == 2
    assert sum(d.task(task["task_id"])["state"] == "active" for task in tasks) == 2
    # Observe several idle-poll opportunities while both Python invocations retain capacity.
    time.sleep(2.2)
    assert len([r for r in d.invocations() if r["task"] in task_ids]) == 2
    gate.touch()
    for task in tasks:
        d.terminal(task["task_id"])
        assert len(d.invocations(task["task_id"])) == 1
    worker.stop()
    # Independent workers on different servers compete for the same queue.
    other = d.start_worker(d.server_url, concurrency=1, cache="cache-competitor")
    worker = d.start_worker(base, concurrency=1)
    competing_gate = d.directory / "gate-competing"
    competing = [d.submit(d.submission(f"compete-{i}", "gate", gate=str(competing_gate)))
                 for i in range(4)]
    ids = {task["task_id"] for task in competing}
    eventually(lambda: len([r for r in d.invocations() if r["task"] in ids]) == 2,
               description="competing workers to claim")
    active = [d.attempt(task["task_id"]) for task in competing if d.task(task["task_id"])["state"] == "active"]
    assert len({a["lease"]["owner"]["worker_session_id"] for a in active}) == 2
    competing_gate.touch()
    for task in competing:
        d.terminal(task["task_id"])
        assert len(d.invocations(task["task_id"])) == 1
        assert sum(x["event"]["reason"] == "claimed" for x in d.history(task["task_id"])) == 1
    worker.stop()
    other.stop()
    second.stop()
    return "two servers/scanners, N=2 across programs, and competing workers with distinct durable owners"


def database_outage(d):
    proxy = d.proxy()
    worker = d.start_worker(proxy.url)
    gate = d.directory / "gate-database"
    task = d.submit(d.submission("database-outage", "gate", gate=str(gate)))
    d.started(task["task_id"])
    idle = d.start_worker(proxy.url, cache="cache-database-idle")
    eventually(lambda: len(proxy.commands("/v1/worker-sessions")) == 2,
               description="second session before DB outage")
    database = urllib.parse.urlsplit(d.database_url).path[1:]
    assert database.startswith("ledgence_http_") and database.replace("_", "").isalnum()
    try:
        d.sql(f'ALTER DATABASE "{database}" ALLOW_CONNECTIONS false', administrative=True)
        d.sql(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{database}'",
              administrative=True)
        eventually(lambda: exchange(d.server_url, "GET", "/health/ready")[0] == 503,
                   timeout=40, description="recovery readiness to degrade")
        for path in ("/v1/acquisitions", "/v1/renewals"):
            eventually(lambda: any(r["status"] == 503 for r in proxy.commands(path)),
                       timeout=35, description=f"typed unavailable {path}")
        gate.touch()
        failed = eventually(lambda: next((r for r in proxy.commands("/v1/settlements")
                                          if r["status"] == 503), None),
                            description="unavailable settlement retained for replay")
        assert json.loads(failed["response"])["code"] == "unavailable"
    finally:
        d.sql(f'ALTER DATABASE "{database}" ALLOW_CONNECTIONS true', administrative=True)
    eventually(lambda: exchange(d.server_url, "GET", "/health/ready")[0] == 200,
               description="recovery readiness to recover")
    d.terminal(task["task_id"])
    worker.stop()
    idle.stop()
    assert any(r["status"] == 200 and r["body"] == failed["body"]
               for r in proxy.commands("/v1/settlements"))
    assert len(d.invocations(task["task_id"])) == 1
    return "isolated DB outage across acquire/renew/settle; typed unavailability, retained report and health recovery"


def preparation_and_startup_outage(d):
    for stage in ("preparation", "startup"):
        gate = d.directory / f"gate-{stage}"
        version = "2.0.0" if stage == "preparation" else "3.0.0"
        d.publish("invoice", version, startup_gate=gate if stage == "startup" else None)
        entered, release = threading.Event(), threading.Event()
        if stage == "preparation":
            d.artifacts.block = (entered, release)
        proxy = d.proxy()
        worker = d.start_worker(proxy.url, cache=f"cache-{stage}")
        task = d.submit(d.submission(f"outage-{stage}", version=version))
        if stage == "preparation":
            assert entered.wait(30), "download did not begin"
        else:
            eventually(lambda: gate.with_suffix(".pid").exists(), description="Python startup gate")
        original = d.attempt(task["task_id"])
        with proxy.lock:
            proxy.blocked = True
        try:
            eventually(lambda: d.attempt(task["task_id"], original["event"]["ldgattemptid"])["state"] == "lost",
                       timeout=95, description=f"production lease expiry during {stage}")
            assert d.invocations(task["task_id"]) == []
            if stage == "startup":
                pid = int(gate.with_suffix(".pid").read_text())
                assert not pid_exists(pid), "startup process outlived local authority"
        finally:
            release.set()
            d.artifacts.block = None
            gate.touch()
            with proxy.lock:
                proxy.blocked = False
        if stage == "preparation":
            # The fetch remains supervised after its 30s observation timeout.
            # Unconfirmed preparation drains the worker by contract, retaining
            # ownership until it can reconcile and finish that same fetch.
            assert worker.process.wait(timeout=40) == 1
            drained = json.loads(worker.stdout_path.read_text())["delivery"]
            assert drained["finished"] is True and drained["lost_attempts"] == 1, drained
            worker = d.start_worker(proxy.url, cache=f"cache-{stage}")
        final, attempt = d.terminal(task["task_id"], timeout=45)
        assert final["attempt_count"] == 2
        assert attempt["descriptor"] == task["descriptor"]
        if stage == "preparation":
            assert attempt["lease"]["owner"]["worker_session_id"] != original["lease"]["owner"]["worker_session_id"]
        assert [r["number"] for r in d.invocations(task["task_id"])] == [2]
        worker.stop()
    return "download/startup outages prevent stale dispatch; retained preparation drains, replacement resumes after real expiry"


def shutdown_reconciliation(d):
    worker = d.start_worker()
    task = d.submit(d.submission("shutdown-active", "gate", gate=str(d.directory / "gate-shutdown")))
    first = d.started(task["task_id"])[0]
    assert worker.stop() == 1
    assert not pid_exists(first["pid"])
    attempt = d.attempt(task["task_id"], first["attempt"])
    assert attempt["settlement"] is not None
    assert attempt["quiescence"] == "confirmed"
    d.cancel(task["task_id"])

    proxy = d.proxy()
    loss = proxy.lose_once("/v1/settlements", block_after=True)
    worker = d.start_worker(proxy.url)
    settled = d.submit(d.submission("shutdown-unacknowledged"))
    assert loss.wait(30)
    receipt = d.attempt(settled["task_id"])["settlement"]
    worker.process.send_signal(signal.SIGTERM)
    time.sleep(1)
    assert worker.process.poll() is None, "first signal discarded unresolved settlement"
    worker.process.send_signal(signal.SIGINT)
    assert worker.process.wait(timeout=5) != 0
    assert "force" in worker.stderr_path.read_text().lower()
    assert d.attempt(settled["task_id"])["settlement"] == receipt
    with proxy.lock:
        proxy.blocked = False
    return "first signal drains active cleanup/report; second signal forces nonzero exit with unresolved acknowledgement"


def history_pagination(d):
    worker = d.start_worker()
    gate = d.directory / "gate-history"
    command = d.submission("history-pagination", "interrupt", gate=str(gate), interrupt_gate_at=26)
    command["input"]["retry_policy"]["max_attempts"] = 26
    task = d.submit(command)
    records = d.started(task["task_id"], count=26)
    assert d.task(task["task_id"])["state"] == "active"
    first = d.history(task["task_id"])
    assert len(first) == 100, len(first)
    second = d.history(task["task_id"], first[-1]["sequence"])
    assert second
    combined = first + second
    assert d.history(task["task_id"], combined[-1]["sequence"]) == []
    older = d.attempt(task["task_id"], records[0]["attempt"])
    assert older["settlement"] is not None
    gate.touch()
    final, _ = d.terminal(task["task_id"], "failed", timeout=35)
    assert final["attempt_count"] == 26
    later = d.history(task["task_id"], combined[-1]["sequence"])
    assert later, "polling the previous cursor must reveal newly committed history"
    combined += later
    assert d.history(task["task_id"], combined[-1]["sequence"]) == []
    assert [x["sequence"] for x in combined] == list(range(1, len(combined) + 1))
    assert len([x for x in combined if x["event"]["reason"] == "claimed"]) == 26
    worker.stop()
    return "100-record pages during active work, older attempt inspection, and later history after task completion"


SCENARIOS = [warm_cache_and_cli, committed_response_loss, normalized_submission, cancellation_and_failures,
             worker_crash_recovery, orchestrator_restart, network_outage,
             capacity_and_multiple_servers, history_pagination, database_outage,
             preparation_and_startup_outage, shutdown_reconciliation]
