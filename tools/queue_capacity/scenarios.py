"""Public-entry-point acceptance for live queue limits (MIT)."""

from collections import Counter
import json
import os
import uuid

from http_acceptance.harness import eventually
from mcp_acceptance.harness import McpSession


def workers_and_limits(d):
    queue = d.queue
    other = "queue-unrestricted"
    policy_rev = d.change(queue, 5, None)
    # Start both independent processes before publishing work. Neither worker
    # owns enough local slots to produce the measured peak by itself.
    left = d.consumer("limit-one", queue, 3)
    right = d.consumer("limit-two", queue, 3)
    gates = {}
    for i in range(8):
        gate = d.directory / f"hold-{i}"
        task = d.submit_to(queue, f"limited-{i}", gate=gate)
        gates[task] = gate

    def entries():
        return [row for row in d.invocations() if row["task"] in gates]

    first = eventually(
        lambda: rows if len(rows := entries()) >= 5 else None,
        timeout=45,
        description="five running handlers",
    )
    assert len(first) == 5 and len({r["task"] for r in first}) == 5
    held = d.holds(queue)
    assert len(held) == 5 and len({row["worker_session_id"] for row in held}) == 2
    assert d.inspect(queue)["occupied"] == "5"
    if hasattr(d, "delivery"):
        # An idle destination would make the sixth worker irrelevant. Exercise
        # at least one real full-queue claim while the first five remain held.
        eventually(
            lambda: (
                int(
                    d.sql(
                        "SELECT count(*) FROM dispatch_claim_receipts WHERE convert_from(reply_bytes,'UTF8')::jsonb #>> '{disposition,kind}' = 'deferred'"
                    )
                )
                > 0
            ),
            timeout=35,
            description="durable deferred broker claim",
        )
    d.consumer("free", other, 1)
    independent = d.submit_to(other, "unlimited-other")
    eventually(
        lambda: d.task(independent)["state"] == "succeeded",
        timeout=25,
        description="independent queue progresses",
    )
    policy_rev = d.change(queue, 2, policy_rev)
    for row in first[:3]:
        gates[row["task"]].touch()
    eventually(
        lambda: d.inspect(queue)["occupied"] == "2",
        timeout=20,
        description="drain from five to two",
    )
    assert len(entries()) == 5
    policy_rev = d.change(queue, 0, policy_rev)
    for row in first[3:]:
        gates[row["task"]].touch()
    eventually(
        lambda: d.inspect(queue)["occupied"] == "0",
        timeout=20,
        description="drain paused queue",
    )
    assert len(entries()) == 5
    d.change(queue, None, policy_rev)
    for gate in gates.values():
        gate.touch()
    for task in gates:
        eventually(
            lambda: d.task(task)["state"] == "succeeded",
            timeout=45,
            description="resume queued task",
        )
        d.output_for(task, queue)
    assert Counter(r["task"] for r in entries()) == Counter(gates.keys())
    left.stop()
    right.stop()
    return {
        "peak": 5,
        "participating_sessions": len({row["worker_session_id"] for row in held}),
        "independent_queue_succeeded": True,
        "lowered_to": 2,
        "pause_occupied": 0,
        "distinct_tasks": len(gates),
    }


def surfaces(d):
    q = "management-test"
    mcp = McpSession(
        d, "queue-operator", credentials_file=d.operator, allow_resource_control=True
    )
    d.sessions.append(mcp)
    catalog = {r["name"] for r in mcp.request("tools/list", {})["result"]["tools"]}
    assert (
        "ledgence_queue_policy_put" in catalog
        and "ledgence_queue_hold_release" in catalog
    )
    body = {
        "queue": q,
        "request_id": "mcp-create-queue",
        "expected_revision": None,
        "policy": {"max_concurrency": 5, "timeout": None},
        "reason": "native capacity review",
    }
    created = mcp.tool("ledgence_queue_policy_put", body)
    replay = mcp.tool("ledgence_queue_policy_put", body)
    assert not created["already_accepted"] and replay["already_accepted"]
    snapshot = mcp.tool("ledgence_queue_policy_inspect", {"queue": q})
    assert snapshot == d.inspect(q)
    args = [
        "--server",
        d.server_url,
        "--tenant",
        d.scope["tenant_id"],
        "--namespace",
        d.scope["namespace"],
        "--credentials-file",
        str(d.operator),
    ]
    assert d.command("ledgence", ["queue", "inspect", *args, "--queue", q]) == snapshot
    change = dict(
        body,
        scope=d.scope,
        request_id="cli-update",
        expected_revision=snapshot["record"]["revision"],
        policy={"max_concurrency": 2, "timeout": None},
    )
    file = d.directory / "cli-update.json"
    file.write_text(
        json.dumps(
            {
                "format": "ledgence.queue-policy.command.v1",
                "base_url": d.server_url,
                "body": json.dumps(change),
            }
        )
    )
    first = d.command("ledgence", ["queue", "put", *args, "--file", str(file)])
    replay = d.command("ledgence", ["queue", "put", *args, "--file", str(file)])
    assert (
        first["record"] == replay["record"]
        and replay["already_accepted"]
        and not first["already_accepted"]
    )
    assert d.inspect(q)["record"]["policy"]["max_concurrency"] == 2
    rejected = d.request(
        "POST",
        "/v1/queue-policies/put",
        {**body, "scope": d.scope},
        credentials=d.reader,
    )
    assert rejected[0] == 403
    d.checked(
        "POST",
        "/v1/queue-policies/holds",
        {
            "scope": d.scope,
            "queue": q,
            "after": None,
            "limit": 10,
            "include_released": False,
        },
        credentials=d.reader,
        expected=403,
    )
    readonly = McpSession(
        d, "queue-readonly", credentials_file=d.reader, read_only=True
    )
    d.sessions.append(readonly)
    read_names = {
        r["name"] for r in readonly.request("tools/list", {})["result"]["tools"]
    }
    assert (
        "ledgence_queue_policy_put" not in read_names
        and "ledgence_queue_hold_release" not in read_names
    )
    readonly.invalid("ledgence_queue_policy_put", body)
    readonly.close()
    mcp.close()
    audit = d.checked(
        "POST",
        "/v1/audit/events",
        {
            "scope": d.scope,
            "filter": {"kind": "resource", "resource_kind": "queue", "resource_id": q},
            "cursor": None,
            "limit": 30,
        },
    )
    assert any(
        row["operation"]["kind"] == "queue_policy_accepted" for row in audit["items"]
    )
    return {
        "queue": q,
        "policy_revision": snapshot["record"]["revision"],
        "mcp_exact_replay": True,
        "read_only_rejected": True,
        "attributed_queue_edit": True,
    }


async def installed_client(d):
    from ledgence.client import (
        AsyncClient,
        AuditFilter,
        QueueConcurrencyPolicy,
        QueueHoldTimeout,
    )

    async with AsyncClient(
        d.server_url,
        tenant=d.scope["tenant_id"],
        namespace=d.scope["namespace"],
        credentials_file=d.operator,
    ) as client:
        q = "installed-sdk"
        plan = QueueConcurrencyPolicy(5, QueueHoldTimeout(60_000))
        cmd = dict(
            policy=plan,
            request_id="sdk-first-policy",
            reason="five available connections",
        )
        first = await client.queue_policies.put(q, **cmd)
        replay = await client.queue_policies.put(q, **cmd)
        assert (
            not first.already_accepted
            and replay.already_accepted
            and first.record == replay.record
        )
        assert (await client.queue_policies.get(q)).record == first.record
        assert first.record in [
            s.record for s in (await client.queue_policies.list()).items
        ]
        assert not (await client.queue_policies.holds(q)).items
        events = await client.audit.list(filter=AuditFilter.resource("queue", q))
        assert {e.operation.kind for e in events.items} >= {
            "sensitive_response",
            "queue_policy_accepted",
        }
        return {"installed_client": True, "queue_filtered_audit": True}


def fake_started(d, q, label):
    task = d.submit_to(q, label)
    # Synthetic owner obtains a real grant, but never reports completion. This
    # models the server's evidence after a worker disappears without leaking a process.
    session = d.checked(
        "POST",
        "/v1/worker-sessions",
        {"scope": d.scope, "queue": q, "concurrency": 1},
        credentials=d.worker_credentials,
    )
    claim = {
        "scope": d.scope,
        "queue": q,
        "worker_session_id": session["id"],
        "consumer_id": 0,
        "sequence": 1,
        "wait_ms": 0,
    }
    assigned = d.checked(
        "POST", "/v1/acquisitions", claim, credentials=d.worker_credentials
    )["assignment"]
    assert (
        assigned["queue_concurrency"] and assigned["lease"]["owner"]["task_id"] == task
    )
    dispatch = {
        "intent": "dispatch",
        "sequence": assigned["authority"]["renew_sequence"] + 1,
        "owner": assigned["lease"]["owner"],
    }
    authority = d.checked(
        "POST", "/v1/renewals", dispatch, credentials=d.worker_credentials
    )
    assert authority["dispatch_allowed"]
    return task, assigned["lease"]["owner"]


def timeout_modes(d):
    evidence = {}
    for mode in ("confirm", "after_grace"):
        q = "partition-" + mode
        grace = 1200 if mode == "after_grace" else None
        action = {"mode": mode} if grace is None else {"mode": mode, "grace_ms": grace}
        d.change(q, 1, None, timeout={"after_ms": 1500, "release": action})
        task, owner = fake_started(d, q, "disconnected-" + mode)
        stop = eventually(
            lambda: (
                h
                if (h := d.holds(q, released=True)[0])["stop_requested_at"] is not None
                else None
            ),
            timeout=15,
            description="recorded stop for original attempt",
        )
        assert stop["task_id"] == task and int(stop["policy_revision"]) == 1
        assert stop["stop_requested_at"] >= stop["deadline"]
        if grace is not None:
            final = eventually(
                lambda: (
                    h
                    if (h := d.holds(q, released=True)[0])["state"] == "released"
                    else None
                ),
                timeout=15,
                description="forced release after actual grace",
            )
            assert (
                final["release_kind"] == "timeout"
                and final["released_at"] >= stop["stop_requested_at"] + grace
            )
        else:
            assert d.inspect(q)["occupied"] == "1" and stop["state"] == "uncertain"
            # A fresh process knows only PostgreSQL. It must keep the original
            # source and refuse replacement work while an uncertain slot is held.
            d.server.stop()
            d.start_server()
            recovered = d.holds(q)[0]
            assert recovered == stop and d.inspect(q)["occupied"] == "1"
            d.submit_to(q, "queued-during-disconnect")
            session = d.checked(
                "POST",
                "/v1/worker-sessions",
                {"scope": d.scope, "queue": q, "concurrency": 1},
                credentials=d.worker_credentials,
            )
            denied = d.checked(
                "POST",
                "/v1/acquisitions",
                {
                    "scope": d.scope,
                    "queue": q,
                    "worker_session_id": session["id"],
                    "consumer_id": 0,
                    "sequence": 1,
                    "wait_ms": 0,
                },
                credentials=d.worker_credentials,
            )
            assert denied["disposition"] == "empty"
            evidence["restart_preserved_uncertain_limit"] = True
            command = {
                "scope": d.scope,
                "queue": q,
                "task_id": task,
                "attempt_id": owner["attempt_id"],
                "expected_revision": stop["revision"],
                "request_id": uuid.uuid4().hex,
                "reason": "operator accepts unconfirmed stop",
            }
            first = d.checked("POST", "/v1/queue-policies/release", command)
            replay = d.checked("POST", "/v1/queue-policies/release", command)
            assert replay["already_accepted"] and replay["hold"] == first["hold"]
            final = first["hold"]
            assert final["release_kind"] == "operator"
        late = d.checked(
            "POST",
            "/v1/queue-policies/close",
            {"queue": q, "owner": owner},
            credentials=d.worker_credentials,
        )
        assert late["already_closed"] and late["release_kind"] == final["release_kind"]
        assert d.inspect(q)["occupied"] == "0"
        evidence[mode] = {
            "release_kind": final["release_kind"],
            "deadline": stop["deadline"],
            "stop_at": stop["stop_requested_at"],
            "released_at": final["released_at"],
        }
    # Now exercise the actual worker/managed host deadline separately.
    q = "worker-stop"
    d.change(q, 1, None, timeout={"after_ms": 12000, "release": {"mode": "confirm"}})
    w = d.consumer("deadline", q, 1)
    gate = d.directory / "worker-stop-gate"
    task = d.submit_to(q, "worker-stops-after-timeout", gate=gate)

    def began():
        if d.invocations(task):
            return True
        state = d.task(task)["state"]
        if state in ("failed", "cancelled", "succeeded"):
            result = d.checked("GET", d.query("/v1/tasks/result", task_id=task))
            (d.directory / "worker-stop-result.json").write_text(
                json.dumps(result, indent=2)
            )
            raise AssertionError(
                "worker became terminal before the managed handler started"
            )
        return False

    eventually(began, timeout=20, description="real handler began")
    eventually(
        lambda: d.task(task)["state"] in ("failed", "cancelled"),
        timeout=50,
        description="worker stopped timed-out handler",
    )
    eventually(
        lambda: d.inspect(q)["occupied"] == "0",
        timeout=15,
        description="confirmed worker cleanup releases cap",
    )
    assert all(
        h["release_kind"] in ("confirmed", "never_started")
        for h in d.holds(q, released=True)
    )
    pid = d.invocations(task)[0]["pid"]

    def process_stopped():
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        return False

    eventually(
        process_stopped, timeout=10, description="original managed process reaped"
    )
    gate.touch()
    w.stop()
    evidence["managed_process_stopped"] = True
    evidence["lost_worker"] = lost_worker(d)
    return evidence


def lost_worker(d):
    """Lose a real worker after its handler entered, before any cleanup receipt."""
    q = "crashed-worker"
    d.change(q, 1, None, timeout={"after_ms": 12000, "release": {"mode": "confirm"}})
    worker = d.consumer("lost", q, 1)
    gate = d.directory / "crashed-worker-gate"
    task = d.submit_to(q, "worker-crash", gate=gate)
    try:
        event = d.started(task)[0]
        worker.kill()
        replacement = d.consumer("replacement", q, 1)
        next_task = d.submit_to(q, "replacement-after-crash")
        source = eventually(
            lambda: next(
                (
                    row
                    for row in d.holds(q)
                    if row["attempt_id"] == event["attempt"]
                    and row["state"] == "uncertain"
                    and row["stop_requested_at"] is not None
                ),
                None,
            ),
            timeout=30,
            description="unconfirmed capacity after a real worker exit",
        )
        assert source["task_id"] == task and d.inspect(q)["occupied"] == "1"
        assert d.task(next_task)["attempt_count"] == 0 and not d.invocations(next_task)
        # The dead worker cannot confirm cleanup. The operator releases only the
        # inspected source, explicitly accepting the residual uncertainty.
        gate.touch()
        cmd = {
            "scope": d.scope,
            "queue": q,
            "task_id": task,
            "attempt_id": source["attempt_id"],
            "expected_revision": source["revision"],
            "request_id": "release-crashed-worker",
            "reason": "local fixture worker stopped; operator accepts residual risk",
        }
        receipt = d.checked("POST", "/v1/queue-policies/release", cmd)
        assert receipt["hold"]["release_kind"] == "operator"
        eventually(
            lambda: d.task(next_task)["state"] == "succeeded",
            timeout=20,
            description="replacement task finishes",
        )
        d.output_for(next_task, q)
        replacement.stop()
        return {"replacement_blocked_until_release": True, "release_kind": "operator"}
    finally:
        gate.touch()
