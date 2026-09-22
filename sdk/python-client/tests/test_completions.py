"""Durable subscription commands and strict delivery observations over real HTTP."""
import asyncio
import copy
import json
import unittest
from unittest.mock import AsyncMock, patch
from urllib.parse import quote

import test_workflows
from ledgence.client import (
    AsyncClient, CompletionRetryCommand, CompletionRetryUncertain, CompletionState,
    CompletionSubscribeCommand, CompletionSubscriptionUncertain, CompletionTarget,
    Conflict, InputError, ProtocolError, RequestTimeout,
)
from support import SCOPE, response


def command(kind="task", id="task"):
    return {"scope": dict(SCOPE), "target": {"kind": kind, "id": id},
            "destination": "billing-results", "idempotency_key": "completion"}


def completion_event(cmd):
    scope, target = cmd["scope"], cmd["target"]
    kind, identity = target["kind"], target["id"]
    event = {"specversion": "1.0", "id": f"evt_{kind}_completed_{identity}",
             "source": "urn:ledgence:orchestrator", "type": f"com.ledgence.{kind}.completed.v1",
             "subject": f"{kind}s/{identity}", "time": "2026-09-22T00:00:00Z",
             "ldgtenantid": scope["tenant_id"], "ldgnamespace": scope["namespace"],
             f"ldg{kind}id": identity, "ldgstate": "succeeded",
             "ldgresultref": f"/v1/{kind}s/result?tenant_id={quote(scope['tenant_id'], safe='-._~')}"
                             f"&namespace={quote(scope['namespace'], safe='-._~')}"
                             f"&{kind}_id={quote(identity, safe='-._~')}"}
    if kind == "task": event["ldgrunid"] = "run"
    return event


def snapshot(cmd=None, state="waiting", **changes):
    cmd = copy.deepcopy(cmd or command())
    activated = state != "waiting"
    attempts = {"waiting": 0, "pending": 0, "delivering": 1, "retrying": 1,
                "delivered": 1, "exhausted": 8}[state]
    return {"subscription_id": "subscription", "command": cmd, "state": state,
            "generation": 1, "attempts": attempts, "total_attempts": attempts, "created_at": 10,
            "activated_at": 20 if activated else None,
            "next_attempt_at": 20 if state in ("pending", "retrying") else None,
            "lease_expires_at": 40 if state == "delivering" else None,
            "delivered_at": 30 if state == "delivered" else None,
            "exhausted_at": 30 if state == "exhausted" else None,
            "last_failure": "transport_failure" if state in ("retrying", "exhausted") else None,
            "event": completion_event(cmd) if activated else None, **changes}


class CompletionClientTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflows.WorkflowClientTests.asyncSetUp

    async def test_task_and_workflow_subscription_freeze_and_scope_commands(self):
        async def accepted(request, body): return response(snapshot(json.loads(body)))
        self.callback = accepted
        for kind, accessor in (("task", self.client.tasks), ("workflow", self.client.workflows)):
            handle = accessor.handle("work +/é")
            prepared = handle.prepare_subscribe(destination="billing-results", idempotency_key="completion")
            self.assertIs(type(prepared), CompletionSubscribeCommand)
            prepared.to_dict()["target"]["id"] = "mutated"
            with self.assertRaises(AttributeError): prepared.destination = "mutated"
            subscribed = await handle.subscribe(prepared)
            self.assertEqual(subscribed.id, "subscription")
            self.assertEqual(subscribed.scope, self.client.scope)
            self.assertEqual(json.loads(self.requests[-1][3]), command(kind, "work +/é"))
            self.assertEqual(await handle.subscribe(destination="billing-results", idempotency_key="completion"), subscribed)
        with self.assertRaises(TypeError): CompletionSubscribeCommand()

    async def test_reference_handle_reads_each_state_without_background_work(self):
        handle = self.client.completions.handle("subscription")
        for state in CompletionState:
            async def current(request, body): return response(snapshot(state=state.value))
            self.callback = current
            result = await handle.status()
            self.assertEqual(result.state, state)
            self.assertEqual(result.command.target, CompletionTarget("task", "task"))
            self.assertEqual(result.event is None, state == CompletionState.WAITING)
            if result.event is not None: self.assertNotIn("data", result.event)
        self.assertEqual(len(self.requests), len(CompletionState))
        self.assertTrue(all(r[:3] == ("GET", "/v1/completion-subscriptions/status",
                                     {**SCOPE, "subscription_id": "subscription"}) for r in self.requests))

    async def test_subscription_uncertainty_keeps_exact_command_without_resend(self):
        async def accepted(request, body):
            if len(self.requests) == 1: return response({"code": "unavailable"}, code=503)
            return response(snapshot(json.loads(body), state="delivered"))
        self.callback = accepted
        task = self.client.tasks.handle("task")
        prepared = task.prepare_subscribe(destination="billing-results", idempotency_key="completion")
        with self.assertRaises(CompletionSubscriptionUncertain) as raised:
            await task.subscribe(prepared)
        self.assertIs(raised.exception.command, prepared)
        self.assertEqual(len(self.requests), 1)
        self.assertEqual((await self.client.completions.subscribe(raised.exception.command)).id, "subscription")
        self.assertEqual(self.requests[0][3], self.requests[1][3])

    async def test_retry_reconciles_same_generation_without_retrying_execution(self):
        async def retried(request, body):
            if len(self.requests) == 1: return response({"code": "unavailable"}, code=503)
            return response(snapshot(state="pending", generation=2, total_attempts=8))
        self.callback = retried
        handle = self.client.completions.handle("subscription")
        prepared = handle.prepare_retry(expected_generation=1)
        self.assertIs(type(prepared), CompletionRetryCommand)
        prepared.to_dict()["expected_generation"] = 2
        with self.assertRaises(CompletionRetryUncertain) as raised: await handle.retry(prepared)
        self.assertIs(raised.exception.command, prepared)
        self.assertEqual(len(self.requests), 1)
        result = await handle.retry(raised.exception.command)
        self.assertEqual(result.generation, 2)
        self.assertEqual(json.loads(self.requests[0][3]), {"scope": SCOPE,
                         "subscription_id": "subscription", "expected_generation": 1})
        self.assertEqual(self.requests[0][3], self.requests[1][3])
        self.assertTrue(all(r[1] == "/v1/completion-subscriptions/retry" for r in self.requests))
        with self.assertRaises(TypeError): CompletionRetryCommand()

    async def test_retry_stale_acknowledgment_and_wrong_identity_are_uncertain(self):
        for changed in ({"generation": 1, "total_attempts": 0}, {"subscription_id": "other"},
                        {"command": command("workflow")}, {"generation": True}):
            value = snapshot(state="pending", generation=2, total_attempts=8)
            value.update(changed)
            # A recovered generic handle cannot know the target; use a known binding here.
            async def accepted(request, body): return response(snapshot(json.loads(body)))
            self.callback = accepted
            bound = await self.client.tasks.handle("task").subscribe(destination="billing-results", idempotency_key="completion")
            async def current(request, body): return response(value)
            self.callback = current
            with self.subTest(changed=changed), self.assertRaises(CompletionRetryUncertain) as raised:
                await bound.retry(expected_generation=1)
            self.assertIsInstance(raised.exception.cause, ProtocolError)

    async def test_invalid_commands_and_cross_scope_cross_resource_replay_never_dispatch(self):
        task = self.client.tasks.handle("task")
        prepared = task.prepare_subscribe(destination="billing-results", idempotency_key="completion")
        for handle in (self.client.tasks.handle("other"), self.client.workflows.handle("task")):
            with self.assertRaises(InputError): await handle.subscribe(prepared)
        for url, tenant in ((self.url, "other"), ("http://127.0.0.1:9", "tenant")):
            async with AsyncClient(url, tenant=tenant, namespace="tests") as other:
                with self.assertRaises(InputError): await other.completions.subscribe(prepared)
                retry = self.client.completions.handle("subscription").prepare_retry(expected_generation=1)
                with self.assertRaises(InputError): await other.completions.handle("subscription").retry(retry)
        with self.assertRaises(InputError): await task.subscribe(prepared, destination="other")
        with self.assertRaises(InputError): await self.client.completions.subscribe(target={"kind": "task", "id": "task"}, destination="x", idempotency_key="x")
        with self.assertRaises(InputError): await task.subscribe()
        for value in (True, 0, -1, 1000, 1.0, "1", None):
            with self.assertRaises(InputError): await self.client.completions.handle("subscription").retry(expected_generation=value)
        for value in ("", "x" * 129, "bad\n", "\ufdd0", True):
            with self.assertRaises(InputError): task.prepare_subscribe(destination=value, idempotency_key="x")
            with self.assertRaises(InputError): task.prepare_subscribe(destination="x", idempotency_key=value)
        self.assertEqual(self.requests, [])

    async def test_malformed_snapshots_and_envelopes_fail_closed(self):
        handle = self.client.completions.handle("subscription")
        invalid = [snapshot(state="pending", event=None), snapshot(attempts=1, total_attempts=1),
                   snapshot(created_at=True), snapshot(state="delivered", delivered_at=1),
                   snapshot(generation=0), snapshot(state="exhausted", attempts=7),
                   snapshot(state="retrying", attempts=8, total_attempts=8),
                   snapshot(extra=True), snapshot(state="delivering", lease_expires_at=None)]
        for changes in ({"data": None}, {"ldgtenantid": "other"}, {"ldgstate": "active"},
                        {"id": "new-event"}, {"ldgresultref": "https://elsewhere/"},
                        {"time": "2026-02-30T00:00:00Z"},
                        {"time": "2026-01-01T00:00:00." + "0" * 250 + "Z"},
                        {"ldgparentworkflowid": "parent"},
                        {"ldgstate": "cancelled", "ldgattemptid": "attempt"},
                        {"ldgcorrelationkeyencoding": "unknown"},
                        {"ldgcorrelationkeyencoding": "percent", "ldgcorrelationkey": "%ef%B7%90"},
                        {"ldgcorrelationkeyencoding": "percent", "ldgcorrelationkey": "%ZZ"},
                        {"ldgcorrelationkeyencoding": "percent", "ldgcorrelationkey": "%0A"},
                        {"ldgcorrelationkeyencoding": "percent", "ldgcorrelationkey": "%FF"},
                        {"traceparent": "bad"}):
            item = snapshot(state="pending"); item["event"].update(changes); invalid.append(item)
        missing = snapshot(); del missing["event"]; invalid.append(missing)
        for value in invalid:
            async def current(request, body): return response(value)
            self.callback = current
            with self.subTest(value=value), self.assertRaises(ProtocolError): await handle.status()

    async def test_malformed_mutation_response_is_uncertain_and_conflict_is_definitive(self):
        async def changed(request, body):
            value = snapshot(json.loads(body)); value["command"]["destination"] = "changed"
            return response(value)
        self.callback = changed
        task = self.client.tasks.handle("task")
        with self.assertRaises(CompletionSubscriptionUncertain) as raised:
            await task.subscribe(destination="billing-results", idempotency_key="completion")
        self.assertIsInstance(raised.exception.cause, ProtocolError)
        async def conflict(request, body): return response({"code": "conflict"}, code=409)
        self.callback = conflict
        with self.assertRaises(Conflict): await task.subscribe(destination="billing-results", idempotency_key="completion")
        with self.assertRaises(Conflict): await self.client.completions.handle("subscription").retry(expected_generation=1)

    async def test_dispatched_timeout_and_predispatch_expiration_remain_distinct(self):
        async def delayed(request, body):
            await asyncio.sleep(.2)
            return response(snapshot(json.loads(body)))
        self.callback = delayed
        async with AsyncClient(self.url, tenant="tenant", namespace="tests", request_timeout=.03) as client:
            with self.assertRaises(CompletionSubscriptionUncertain) as raised:
                await client.tasks.handle("task").subscribe(destination="billing-results", idempotency_key="completion")
            self.assertIsInstance(raised.exception.cause, RequestTimeout)
        with patch.object(self.client._require_transport(), "exchange", new=AsyncMock(side_effect=RequestTimeout(dispatched=False))):
            with self.assertRaises(RequestTimeout):
                await self.client.tasks.handle("task").subscribe(destination="billing-results", idempotency_key="completion")
            with self.assertRaises(RequestTimeout):
                await self.client.completions.handle("subscription").retry(expected_generation=1)
        self.assertEqual(len(self.requests), 1)

    async def test_reference_event_preserves_long_ids_trace_lineage_and_correlation(self):
        identity = "é" * 64
        cmd = command("workflow", identity)
        value = snapshot(cmd, state="pending")
        value["event"].update(ldgparentworkflowid="parent", ldgrootworkflowid="root",
                              ldgcorrelationkey="%EF%B7%90", ldgcorrelationkeyencoding="percent",
                              traceparent="00-" + "1" * 32 + "-" + "2" * 16 + "-01",
                              tracestate="vendor=value")
        async def current(request, body): return response(value)
        self.callback = current
        result = await self.client.completions.handle("subscription").status()
        self.assertEqual(result.event, value["event"])
        self.assertGreater(len(result.event["id"].encode()), 128)

    async def test_caller_cancellation_preserves_prepared_subscription_for_reconciliation(self):
        entered = asyncio.Event()
        async def delayed(request, body):
            entered.set()
            await asyncio.sleep(.2)
            return response(snapshot(json.loads(body)))
        self.callback = delayed
        task = self.client.tasks.handle("task")
        prepared = task.prepare_subscribe(destination="billing-results", idempotency_key="completion")
        pending = asyncio.create_task(task.subscribe(prepared))
        await entered.wait()
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError): await pending
        async def accepted(request, body): return response(snapshot(json.loads(body), state="pending"))
        self.callback = accepted
        self.assertEqual((await task.subscribe(prepared)).id, "subscription")
        self.assertEqual(self.requests[0][3], self.requests[1][3])

    async def test_recovered_handle_query_and_snapshot_preserve_unicode_scope(self):
        scope = {"tenant_id": "tenant +%/é", "namespace": "billing /é"}
        cmd = command("workflow", "workflow +/é"); cmd["scope"] = scope
        value = snapshot(cmd, state="pending", subscription_id="subscription +/é")
        async def current(request, body): return response(value)
        self.callback = current
        async with AsyncClient(self.url, tenant=scope["tenant_id"], namespace=scope["namespace"]) as client:
            observed = await client.completions.handle(value["subscription_id"]).status()
        self.assertEqual(self.requests[0][2], {**scope, "subscription_id": value["subscription_id"]})
        self.assertEqual(observed.event["ldgresultref"], value["event"]["ldgresultref"])

    async def test_lineage_rejects_self_ancestry_and_unowned_activation(self):
        handle = self.client.completions.handle("subscription")
        for cmd, extra in (
            (command(), {"ldgactivationid": "task"}),
            (command(), {"ldgworkflowid": "flow", "ldgparentworkflowid": "flow", "ldgrootworkflowid": "ancestor"}),
            (command("workflow", "flow"), {"ldgparentworkflowid": "parent", "ldgrootworkflowid": "flow"}),
        ):
            value = snapshot(cmd, state="pending"); value["event"].update(extra)
            async def current(request, body): return response(value)
            self.callback = current
            with self.subTest(extra=extra), self.assertRaises(ProtocolError): await handle.status()
        for extra in ({"ldgworkflowid": "root", "ldgactivationid": "task"},
                      {"ldgworkflowid": "child", "ldgactivationid": "task",
                       "ldgparentworkflowid": "root", "ldgrootworkflowid": "root"}):
            value = snapshot(state="pending"); value["event"].update(extra)
            async def current(request, body): return response(value)
            self.callback = current
            self.assertEqual((await handle.status()).event, value["event"])
