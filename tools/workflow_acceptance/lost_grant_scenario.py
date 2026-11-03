"""Drop the committed dispatch-grant reply before the worker can contact the provider (MIT)."""
import asyncio
import json

from http_acceptance.harness import eventually
from .refund_provider import RefundProvider
from .refund_scenarios import ARGUMENTS, effect_evidence, parked_state

SCENARIO = "agent-refund-lost-grant"
CLAIM = "/v1/workflows/approved-effects/claim"


async def run(d, record, snapshot):
    from ledgence.client import AsyncClient, RetryPolicy

    key = "refund-lost-grant"
    with RefundProvider(d.directory / (key + "-provider.jsonl"), cooperative=True) as provider:
        # Drop only the reply; keep the worker, its lease and all other routes healthy.
        # A normal retry after failure must reconcile without sending a mutation.
        proxy = d.proxy()
        lost = proxy.lose_once(CLAIM)
        worker = d.start_worker(server=proxy.url)
        async with AsyncClient(d.server_url, tenant=d.scope["tenant_id"], namespace=d.scope["namespace"]) as client:
            handle = await client.workflows.submit(
                program="refund-controller", version="1.0.0", queue=d.queue,
                data={"effect_key": key, "provider": provider.url, "amount": ARGUMENTS["amount"],
                      "marker": str(d.directory / "workflow-markers.jsonl")},
                idempotency_key=key, retry_policy=RetryPolicy(max_attempts=2, retry_delay_ms=0),
            )
            await asyncio.to_thread(eventually, lambda: snapshot(d, handle.id)["state"] == "waiting",
                                    timeout=130, description="refund waits for approval")
            proposal = await handle.approval("refund")
            await handle.decide_approval(handle.prepare_approval_decision(
                proposal, decision_id="approve-" + key, decision="approve", reviewer="fixture-operator",
            ))
            assert await asyncio.to_thread(lost.wait, 130), "worker never claimed the grant"
            parked = await asyncio.to_thread(eventually, lambda: parked_state(d, handle.id),
                                             timeout=160, description="lost grant parks without sending a refund")
            await asyncio.to_thread(worker.stop)

        sent, retried = proxy.commands(CLAIM)
        assert sent["status"] == retried["status"] == 200
        permit = json.loads(sent["response"])
        assert permit["disposition"] == "dispatch" and permit["workflow_id"] == handle.id, permit
        recovery = json.loads(retried["response"])
        assert recovery["disposition"] == "lookup_only" and recovery["workflow_id"] == handle.id, recovery
        assert recovery["activation_id"] == permit["activation_id"] and recovery["attempt_id"] != permit["attempt_id"]
        assert effect_evidence(d, handle.id) == {
            "claimed": True, "attempt_id": permit["attempt_id"], "state": "open", "confirmed_attempt_id": None,
        }
        assert parked["checkpoint"] == {"state": "unknown", "effect_key": key}, parked
        census = provider.census()
        assert census == {"post_requests": 0, "lookup_requests": 1, "refunds": []}, census
        record(SCENARIO, {"workflow_id": handle.id, "activation_id": permit["activation_id"],
                          "first_attempt": permit["attempt_id"], "lookup_attempt": recovery["attempt_id"],
                          "destination": census, "checkpoint": parked["checkpoint"]})
