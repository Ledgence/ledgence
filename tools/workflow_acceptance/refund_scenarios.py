"""Crash one complete worker between provider acceptance and local settlement (MIT)."""
import asyncio
import contextlib
import json
import os
import subprocess

from http_acceptance.harness import eventually
from .agent_scenarios import journal
from .owned_scenarios import quote
from .refund_provider import RefundProvider


SCENARIO = "agent-refund-recovery"
ARGUMENTS = {"amount": 50, "currency": "USD"}


def parked_state(d, workflow_id, previous_key=None):
    row = json.loads(d.sql(
        "SELECT json_build_object('status', state, 'key', external_wait_key, "
        "'checkpoint', convert_from(checkpoint_bytes,'UTF8')::json) FROM workflow_runs "
        "WHERE workflow_id=" + quote(workflow_id)))
    key = row["key"]
    if row["status"] == "waiting" and key and key.startswith("unresolved:") and key != previous_key:
        return row
    return None


def dispatch_grant(d, workflow_id):
    return json.loads(d.sql(
        "SELECT json_build_object('claimed', effect_dispatch_claimed, "
        "'attempt_id', effect_dispatch_attempt_id) FROM workflow_approvals "
        "WHERE wait_key='refund' AND workflow_id=" + quote(workflow_id)))


def runtime_alive(pid, session):
    # A worker starts its own OS session. A reused PID cannot designate an
    # unrelated process for termination because this check only observes it.
    try:
        if os.getsid(pid) != session:
            return False
    except ProcessLookupError:
        return False
    status = subprocess.run(["ps", "-p", str(pid), "-o", "stat="], capture_output=True,
                            text=True, check=False, timeout=5)
    return bool(status.stdout.strip()) and not status.stdout.strip().startswith("Z")


async def run(d, record, records, snapshot):
    from ledgence.client import AsyncClient, ApprovalStatus, RetryPolicy

    options = dict(tenant=d.scope["tenant_id"], namespace=d.scope["namespace"])
    with contextlib.ExitStack() as stack:
        providers = {
            key: stack.enter_context(RefundProvider(d.directory / (key + "-provider.jsonl"),
                                                    cooperative=cooperative))
            for key, cooperative in [("refund-lookup", True), ("refund-opaque", False)]
        }
        worker = d.start_worker(concurrency=2)
        async with AsyncClient(d.server_url, **options) as client:
            handles, first_attempts = {}, {}
            for key, provider in providers.items():
                handle = await client.workflows.submit(
                    program="refund-controller", version="1.0.0", queue=d.queue,
                    data={"effect_key": key, "provider": provider.url, "amount": ARGUMENTS["amount"],
                          "marker": str(d.directory / "workflow-markers.jsonl")},
                    idempotency_key=key, retry_policy=RetryPolicy(max_attempts=2, retry_delay_ms=0),
                )
                handles[key] = handle
                await asyncio.to_thread(eventually, lambda: snapshot(d, handle.id)["state"] == "waiting",
                                        timeout=130, description="pending refund approval")
                proposal = await handle.approval("refund")
                assert proposal.status == ApprovalStatus.PENDING and not provider.census()["post_requests"]
                assert proposal.action.arguments == {"provider": provider.url, "effect_key": key, **ARGUMENTS}
                await handle.decide_approval(handle.prepare_approval_decision(
                    proposal, decision_id="authorize-" + key, decision="approve", reviewer="fixture-operator",
                ))

            for key, provider in providers.items():
                assert await asyncio.to_thread(provider.accepted.wait, 130), key
                first, = records(d, key, "apply")
                first_attempts[key] = first
                proposal = await handles[key].approval("refund")
                assert proposal.status == ApprovalStatus.APPROVED
                assert proposal.resumed_activation_id == first["activation"]
                assert dispatch_grant(d, handles[key].id) == {"claimed": True, "attempt_id": first["attempt_id"]}
                assert journal(d, first["activation"]) == []
                assert provider.census()["refunds"][0]["arguments"] == ARGUMENTS
            # Only kill our unreaped Popen child. Release the held provider replies
            # so the orphaned Python runtimes observe their closed worker transport.
            session = worker.process.pid
            assert worker.process.poll() is None
            assert all(runtime_alive(row["pid"], session) for row in first_attempts.values())
            await asyncio.to_thread(worker.kill)
            for provider in providers.values():
                provider.drop_reply()
            await asyncio.to_thread(eventually,
                lambda: all(not runtime_alive(row["pid"], session) for row in first_attempts.values()),
                timeout=20, description="orphaned refund runtimes exited after the worker died")
            replacement = d.start_worker(concurrency=2, cache="refund-replacement-cache")
            result = await handles["refund-lookup"].result(timeout=160)
            opaque = handles["refund-opaque"]
            parked = await asyncio.to_thread(eventually, lambda: parked_state(d, opaque.id),
                                             timeout=160, description="opaque provider leaves refund pending")
            unknown = {"state": "unknown", "effect_key": "refund-opaque"}
            assert parked["checkpoint"] == unknown, parked
            # Even a correctly addressed ordinary event claiming success cannot settle the refund.
            await opaque.send_event(opaque.prepare_event(parked["key"], event={
                "specversion": "1.0", "source": "urn:ledgence:refund-test", "id": "false-resolution",
                "type": "test.resolution", "datacontenttype": "application/json", "data": {"state": "confirmed"},
            }))
            parked = await asyncio.to_thread(eventually, lambda: parked_state(d, opaque.id, parked["key"]),
                                             timeout=130, description="unauthorized resolution stays parked")
            assert parked["checkpoint"] == unknown, parked

            evidence = {}
            for key, provider in providers.items():
                first, second = records(d, key, "apply")
                assert first == first_attempts[key] and first["activation"] == second["activation"]
                assert [first["attempt"], second["attempt"]] == [1, 2]
                assert first["attempt_id"] != second["attempt_id"]
                grant = dispatch_grant(d, handles[key].id)
                assert grant == {"claimed": True, "attempt_id": first["attempt_id"]}, grant
                initial = d.attempt(first["activation"], first["attempt_id"])
                recovered = d.attempt(second["activation"], second["attempt_id"])
                sessions = [attempt["lease"]["owner"]["worker_session_id"] for attempt in (initial, recovered)]
                assert initial["state"] == "lost" and sessions[0] != sessions[1], (initial, recovered)
                census = provider.census()
                receipt, = census["refunds"]
                assert census["post_requests"] == census["lookup_requests"] == 1, census
                assert receipt["effect_key"] == key and receipt["arguments"] == ARGUMENTS, census
                expected = {"state": "confirmed", "receipt": receipt} if key == "refund-lookup" else unknown
                committed = journal(d, first["activation"])
                assert [row["output"] for row in committed] == ([expected] if key == "refund-lookup" else []), committed
                finishes = int(d.sql(
                    "SELECT count(*) FROM workflow_activations WHERE applied_at_ms IS NOT NULL "
                    "AND convert_from(context_bytes,'UTF8')::json->>'continuation'='finish' "
                    "AND workflow_id=" + quote(handles[key].id)))
                assert finishes == int(key == "refund-lookup"), (key, finishes)
                if key == "refund-lookup":
                    assert result == expected, result
                evidence[key] = {"workflow_id": handles[key].id, "worker_sessions": sessions,
                                 "dispatch_grant": grant, "committed_result_count": len(committed),
                                 "send_attempt_id": first["attempt_id"], "recovery_attempt_id": second["attempt_id"],
                                 "approved_arguments": ARGUMENTS, "destination": census,
                                 "completed_continuations": finishes, "observed_outcome": expected}
            record(SCENARIO, evidence)
            await asyncio.to_thread(replacement.stop)
