"""Real subprocess task entrypoint and durable capacity acceptance, independent of broker (MIT)."""

import json
import time
from pathlib import Path

from http_acceptance.harness import eventually
from workflow_acceptance.owned_scenarios import quote
from .harness import classifier
from .task import CLOSE, RELEASE, SESSIONS, START
from .task_surfaces import public_surfaces


def parked_restart_and_cancel(d, _):
    d.policy(enabled=False, offer_ms=30_000)
    worker = d.worker()
    pending = d.begin_task("task-parked")
    d.ledger_state(pending, "waiting")
    ordinary = d.begin_task("ordinary-beside-wait", governed=False)
    d.outcome(ordinary)
    d.assert_marker(ordinary, [1])
    assert d.status(pending)["attempt_count"] == 0
    d.assert_marker(pending, [])
    cancelled = d.begin_task("task-cancel-wait")
    d.ledger_state(cancelled, "waiting")
    d.cancel_task(cancelled)
    assert d.outcome(cancelled, "cancelled")["task"]["attempt_count"] == 0
    d.ledger_state(cancelled, "closed_before_start")
    d.server.stop()
    d.start_server()
    assert d.ledger(pending)["state"] == "waiting"
    d.policy(offer_ms=30_000)
    d.outcome(pending)
    d.ledger_state(pending, "finished")
    d.assert_marker(pending, [1])
    d.assert_marker(cancelled, [])
    worker.stop()
    return {
        "parked": pending,
        "ordinary": ordinary,
        "cancelled_before_assignment": cancelled,
    }


def competing_capacity(d, _):
    d.policy(capacity=1, offer_ms=30_000)
    first, second = d.worker(), d.worker()
    gate = d.directory / "hold-first-handler"
    try:
        held = d.begin_task("task-held", mode="gate", gate=str(gate))
        d.started(held)
        waiting = d.begin_task("task-second")
        d.ledger_state(waiting, "waiting")
        ordinary = d.begin_task("task-ordinary-while-held", governed=False)
        d.outcome(ordinary)
        assert d.ledger(held)["state"] == "possibly_dispatched"
        assert d.status(waiting)["attempt_count"] == 0 and d.resource()["occupied"] == 1
        d.assert_marker(waiting, [])
    finally:
        gate.touch()
    d.outcome(held)
    d.outcome(waiting)
    d.ledger_state(held, "finished")
    d.ledger_state(waiting, "finished")
    d.assert_marker(held, [1])
    d.assert_marker(waiting, [1])
    d.assert_marker(ordinary, [1])
    assert d.resource()["occupied"] == 0
    first.stop()
    second.stop()
    return {"held": held, "waiting": waiting, "ordinary_progress": ordinary}


def shared_workflow_capacity(d, _):
    # The documented workflow requests this same named resource. A held task
    # occupies the only slot; queue the workflow operation before a younger task.
    d.resource_name = "support-model"
    d.policy(capacity=1, offer_ms=30_000)
    first, second = d.worker(), d.worker()
    gate = d.directory / "hold-mixed-handler"
    provider = classifier(d.root)
    text = "mixed-workflow-invoice"
    provider.hold(text)
    try:
        held = d.begin_task("task-mixed-holder", mode="gate", gate=str(gate))
        d.started(held)
        workflow = d.begin(text, provider)
        eventually(
            lambda: (row := d.admission(workflow)) and row["state"] == "waiting",
            description="older workflow operation durably waits behind the task",
        )
        younger = d.begin_task("task-mixed-younger")
        d.ledger_state(younger, "waiting")
        gate.touch()
        eventually(
            lambda: provider.census()["requests"].get(text) == 1,
            description="older workflow operation enters the shared resource",
        )
        assert d.status(younger)["attempt_count"] == 0
        d.assert_marker(younger, [])
        assert d.resource()["occupied"] == 1
        provider.release(text)
        d.outcome(held)
        d.outcome(younger)

        def workflow_done():
            result = d.checked(
                "GET", d.query("/v1/workflows/result", workflow_id=workflow)
            )
            if result["outcome"] is None:
                return None
            assert result["outcome"] == {
                "kind": "succeeded",
                "output": {"category": "billing"},
            }
            return result

        eventually(
            workflow_done, description="observed workflow and task both complete"
        )
        assert provider.census()["requests"][text] == 1
        assert d.admission(workflow)["state"] == "finished"
        d.ledger_state(younger, "finished")
        d.assert_marker(held, [1])
        d.assert_marker(younger, [1])
        assert d.resource()["occupied"] == 0
    finally:
        gate.touch()
        provider.close()
    first.stop()
    second.stop()
    return {
        "holder": held,
        "older_workflow": workflow,
        "younger_task": younger,
        "one_shared_slot": True,
    }


def cold_preparation(d, _):
    d.policy(offer_ms=1_000)
    proxy = d.authenticated_proxy()
    worker = d.worker(proxy)
    gate = d.cold_gates["cold-ready"]
    try:
        task = d.begin_task("task-cold", program="cold-ready")
        eventually(
            lambda: Path(str(gate) + ".pid").is_file(),
            description="cold import began before runtime-ready Start",
        )
        preparing = d.ledger_state(task, "preparing")
        assert preparing["source"] is None and preparing["owner"]["generation"] == 1
        # Hold *after* acquisition for longer than the original unclaimed offer.
        # The marker independently distinguishes module import from handler entry.
        time.sleep(1.1)
        assert d.ledger(task)["state"] == "preparing" and not proxy.commands(START)
        d.assert_marker(task, [])
    finally:
        gate.touch()
    result = d.outcome(task)
    d.ledger_state(task, "finished")
    d.assert_marker(task, [1])
    assert (
        result["task"]["attempt_count"] == 1
        and result["task"].get("admission_deferrals", 0) == 0
    )
    assert len(proxy.commands(START)) == 1 and any(
        "task_admission.v1" in json.loads(row["response"])["features"]
        for row in proxy.commands(SESSIONS)
    )
    worker.stop()
    return {
        "task": task,
        "cold_preparation_survived_unclaimed_offer": True,
        "starts": 1,
    }


def revoked_preparation_preserves_retry(d, _):
    d.policy(offer_ms=30_000)
    worker = d.worker()
    gate = d.cold_gates["cold-revoke"]
    try:
        task = d.begin_task("task-revoked-cold", program="cold-revoke", attempts=1)
        eventually(
            lambda: Path(str(gate) + ".pid").is_file(),
            description="revoked cold import began",
        )
        previous = d.ledger_state(task, "preparing")
        d.policy(enabled=False, offer_ms=30_000)
    finally:
        gate.touch()

    def deferred():
        status = d.status(task)
        assert status["state"] not in ("failed", "succeeded"), (
            "preparation consumed the only execution retry"
        )
        return (
            status
            if status["state"] == "queued" and status.get("admission_deferrals", 0) == 1
            else None
        )

    eventually(deferred, description="revocation durably defers an unused preparation")
    original = d.ledger(task)
    assert original["state"] == "closed_before_start" and original["source"] is None
    d.assert_marker(task, [])
    d.policy(offer_ms=30_000)
    result = d.outcome(task)
    current = d.ledger_state(task, "finished", 2)
    assert (
        result["task"]["attempt_count"] == 2
        and result["task"]["admission_deferrals"] == 1
    )
    assert current["owner"]["attempt_id"] != previous["owner"]["attempt_id"]
    assert current["source"]["accepted_at"] >= original["updated_at"] + 1_000, (
        "admission-only retry bypassed durable backoff"
    )
    records = d.assert_marker(task, [2])
    assert records[0]["attempt"] == current["owner"]["attempt_id"]
    worker.stop()
    return {"task": task, "allocated": 2, "charged": 1, "handler_entries": 1}


def lost_start(d, _):
    period_ms = 3_600_000
    d.policy(offer_ms=30_000, rate={"numerator": 1, "period_ms": period_ms, "burst": 2})
    proxy = d.authenticated_proxy()
    lost = proxy.lose_once(START)
    worker = d.worker(proxy)
    task = d.begin_task("task-start-reply-lost", attempts=1)
    eventually(
        lost.is_set, description="real task Start committed before response loss"
    )
    result = d.outcome(task, "failed", timeout=90)
    closed = d.ledger_state(task, "finished")
    assert (
        closed["source"] is not None
        and closed["close"]["disposition"] == "charged_not_invoked"
    )
    assert (
        result["task"]["attempt_count"] == 1
        and result["task"].get("admission_deferrals", 0) == 0
    )
    assert len(proxy.commands(START)) == 1 and d.resource()["occupied"] == 0
    d.assert_marker(task, [])
    original = proxy.commands(START)[0]["json"]
    # The original lease may already be closed. Neither replay path can grant permission.
    code, _, reply = d.request(
        "POST", START, original, credentials=d.worker_credentials
    )
    assert (code == 200 and reply["kind"] == "existing") or (
        code == 409 and reply["code"] in ("ownership_lost", "conflict")
    ), "replayed Start must not issue new authority"
    credit = int(
        d.sql(
            "SELECT credit FROM admission_resources WHERE tenant_id="
            + quote(d.scope["tenant_id"])
            + " AND namespace="
            + quote(d.scope["namespace"])
            + " AND resource_name="
            + quote(d.resource_name)
        )
    )
    assert period_ms <= credit < 2 * period_ms, (
        "exactly one rate debit must survive lost Start and replay"
    )
    worker.stop()
    return {
        "task": task,
        "handler_entries": 0,
        "charged": 1,
        "durable_source": closed["source"]["source_id"],
    }


def real_retry(d, _):
    d.policy(offer_ms=30_000)
    worker = d.worker()
    task = d.begin_task("task-real-retry", mode="interrupt_once", attempts=2)
    result = d.outcome(task, timeout=90)
    first = d.ledger_state(task, "finished")
    second = d.ledger_state(task, "finished", 2)
    d.assert_marker(task, [1, 2])
    assert (
        result["task"]["attempt_count"] == 2
        and result["task"].get("admission_deferrals", 0) == 0
    )
    assert (
        first["source"]["source_id"] != second["source"]["source_id"]
        and d.resource()["occupied"] == 0
    )
    worker.stop()
    return {"task": task, "handler_entries": 2, "separate_sources": True}


def old_session_and_broker_wake(d, _):
    d.policy(offer_ms=30_000)
    capable = d.checked(
        "POST",
        SESSIONS,
        {"scope": d.scope, "queue": d.queue, "concurrency": 1},
        credentials=d.worker_credentials,
    )
    assert (
        "task_admission.v1" in capable["features"]
    )  # registered but never polls/claims
    task = d.begin_task("task-legacy-session")
    d.ledger_state(task, "offered")
    session = d.checked(
        "POST",
        "/v1/worker-sessions",
        {"scope": d.scope, "queue": d.queue, "concurrency": 1},
        credentials=d.worker_credentials,
    )
    assert "task_admission.v1" not in session.get("features", [])
    acquisition = {
        "scope": d.scope,
        "queue": d.queue,
        "worker_session_id": session["id"],
        "consumer_id": 0,
        "sequence": 1,
    }
    if hasattr(d, "delivery"):
        command = {
            "acquisition": acquisition,
            "dispatch": {
                "scope": d.scope,
                "queue": d.queue,
                "task_id": task,
                "generation": 1,
            },
        }
        # This already-published broker message remains in ElasticMQ. A real
        # capable consumer will later receive it after the legacy handoff.
        eventually(
            lambda: (
                d.sql(
                    "SELECT count(*) FROM dispatch_intents WHERE task_id="
                    + quote(task)
                    + " AND last_confirmed_at_ms IS NOT NULL"
                )
                == "1"
            ),
            description="initial broker publication confirmed",
        )
        before = int(
            d.sql("SELECT (extract(epoch FROM clock_timestamp())*1000)::bigint")
        )
        reply = d.checked(
            "POST", "/v1/dispatch/claim", command, credentials=d.worker_credentials
        )
        assert (
            reply["command"] == command and reply["disposition"]["kind"] == "deferred"
        )
        available_at = reply["disposition"]["available_at"]
        assert available_at >= before + 1_000
        assert (
            d.checked(
                "POST", "/v1/dispatch/claim", command, credentials=d.worker_credentials
            )
            == reply
        )
        assert d.sql(
            "SELECT available_at_ms FROM task_admission_requests WHERE task_id="
            + quote(task)
            + " AND generation=1"
        ) == str(available_at)
        assert (
            d.sql("SELECT count(*) FROM dispatch_intents WHERE task_id=" + quote(task))
            == "0"
        )
    else:
        reply = d.checked(
            "POST", "/v1/acquisitions", acquisition, credentials=d.worker_credentials
        )
        assert reply["disposition"] == "empty"
    assert d.status(task)["attempt_count"] == 0
    d.policy(enabled=False, offer_ms=30_000)
    d.ledger_state(task, "waiting")
    worker = d.worker()
    if hasattr(d, "delivery"):
        eventually(
            lambda: any(
                row["disposition"]["kind"] == "admission_deferred"
                and row["disposition"]["occurrence"] == d.occurrence(task)
                for row in d.claims(task)
            ),
            description="actual worker durably deferred the stale broker record",
        )
    ordinary = d.begin_task("task-ordinary-after-legacy-defer", governed=False)
    d.outcome(ordinary)
    d.assert_marker(ordinary, [1])
    assert d.status(task)["attempt_count"] == 0
    d.assert_marker(task, [])
    d.policy(offer_ms=30_000)
    d.outcome(task)
    d.ledger_state(task, "finished")
    d.assert_marker(task, [1])
    worker.stop()
    return {
        "task": task,
        "legacy_excluded": True,
        "same_generation_woke": True,
        "broker_wait_receipt": hasattr(d, "delivery"),
    }


def late_original_cleanup(d, _):
    d.policy(offer_ms=30_000)
    proxy, captured, commands = d.hold_original_close()
    worker = d.worker(proxy)
    task = d.begin_task("task-late-original-close", attempts=1)
    eventually(
        captured.is_set,
        description="original worker confirmed actual handler cleanup before network partition",
    )
    # The independently delivered immutable result must be visible although
    # every worker Close is still captured before it reaches the orchestrator.
    result = d.outcome(task)
    assert result["outcome"]["output"]["event"]["ldgtaskid"] == task
    worker.kill()
    assert commands and all(body == commands[0] for body in commands)
    frozen = json.loads(commands[0])
    assert frozen["outcome"] == "quiescent" and frozen["owner"]["task_id"] == task
    unknown = d.ledger_state(task, "unknown_occupied", timeout=90)
    assert d.resource()["occupied"] == 1 and d.resource()["unknown"] == 1
    d.collect_task(task, retained=True)
    assert d.ledger(task)["source"] == unknown["source"]
    release = d.release_source(d.ledger(task))
    assert d.resource()["occupied"] == 0
    d.collect_task(task, retained=False)
    assert d.ledger(task)["state"] == "administratively_released"
    d.checked("POST", CLOSE, frozen, credentials=d.other_worker, expected=403)
    first = d.checked("POST", CLOSE, frozen, credentials=d.worker_credentials)
    replay = d.checked("POST", CLOSE, frozen, credentials=d.worker_credentials)
    assert not first["already_accepted"] and replay["already_accepted"]
    assert (
        first["source"] == unknown["source"]
        and first["disposition"] == "already_released"
    )
    assert d.checked("POST", RELEASE, release)["already_accepted"]
    d.assert_marker(task, [1])
    assert d.resource()["occupied"] == 0 and d.resource()["unknown"] == 0
    return {
        "task": task,
        "successful_result_before_close": True,
        "visible_while_occupied": True,
        "retired_after_release": True,
        "accepted_original_late_close": True,
        "released_once": True,
    }


CASES = [
    parked_restart_and_cancel,
    competing_capacity,
    shared_workflow_capacity,
    cold_preparation,
    revoked_preparation_preserves_retry,
    lost_start,
    real_retry,
    old_session_and_broker_wake,
    public_surfaces,
    late_original_cleanup,
]
