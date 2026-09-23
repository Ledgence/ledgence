"""Frozen external workflow events and exact-command reconciliation over HTTP."""
import asyncio
import json
from pathlib import Path
import tracemalloc
import unittest
from unittest.mock import AsyncMock, patch

import test_workflows
from ledgence.client import (
    AsyncClient, Conflict, InputError, ProtocolError, RequestTimeout, ServiceError,
    WorkflowEventCommand, WorkflowEventReceipt, WorkflowEventUncertain,
)
from ledgence.client.cloud_events import EVENT_LIMIT, validate_event
from support import SCOPE, response


def event(**changes):
    return {"specversion": "1.0", "id": "approval-1", "source": "/external",
            "type": "invoice.approved", "datacontenttype": "application/json",
            "data": {"approved": True}, **changes}


def receipt(body, **changes):
    command = json.loads(body)
    return {"scope": command["scope"], "workflow_id": command["workflow_id"],
            "key": command["key"], "event_id": command["event"]["id"],
            "event_source": command["event"]["source"], "accepted_at": 10,
            "already_accepted": False, **changes}


class WorkflowEventClientTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = test_workflows.WorkflowClientTests.asyncSetUp

    async def test_frozen_event_preserves_full_original_envelope_and_receipt(self):
        async def accepted(request, body): return response(receipt(body))
        self.callback = accepted
        workflow = self.client.workflows.handle("workflow")
        original = event(data={"values": [1, -0.0]}, subject="invoice/1",
                         time="2024-02-29T00:00:00Z", dataschema="urn:invoice",
                         ldgtaskid="forwarded-task", customflag=True,
                         traceparent="00-" + "1" * 32 + "-" + "2" * 16 + "-01")
        expected = json.loads(json.dumps(original))
        command = workflow.prepare_event("approval", event=original)
        self.assertIs(type(command), WorkflowEventCommand)
        original["data"]["values"].append(99)
        command.to_dict()["event"]["id"] = "mutated"
        result = await workflow.send_event(command)
        self.assertIsInstance(result, WorkflowEventReceipt)
        self.assertEqual(result.scope, self.client.scope)
        self.assertEqual((result.workflow_id, result.key, result.event_id, result.event_source),
                         ("workflow", "approval", "approval-1", "/external"))
        self.assertEqual(result.accepted_at, 10)
        self.assertIs(result.already_accepted, False)
        sent = self.requests[0]
        self.assertEqual(sent[:3], ("POST", "/v1/workflows/events", {}))
        self.assertEqual(json.loads(sent[3]), {"scope": SCOPE, "workflow_id": "workflow",
                                              "key": "approval", "event": expected})
        with self.assertRaises(AttributeError): command.key = "changed"
        with self.assertRaises(TypeError): WorkflowEventCommand()
        self.assertEqual(await workflow.send_event(key="approval", event=expected), result)

    async def test_uncertainty_reconciliation_reuses_exact_bytes_without_automatic_resend(self):
        accepted = []
        async def handle(request, body):
            accepted.append(body)
            if len(accepted) == 1:
                return response({"code": "unavailable", "message": "lost after durable acceptance"}, code=503)
            return response(receipt(body, already_accepted=True))
        self.callback = handle
        workflow = self.client.workflows.handle("workflow")
        command = workflow.prepare_event("approval", event=event(data={"values": [1, -0.0]}))
        with self.assertRaises(WorkflowEventUncertain) as raised:
            await workflow.send_event(command)
        self.assertIs(raised.exception.command, command)
        self.assertEqual(len(accepted), 1)
        reconciled = await workflow.send_event(raised.exception.command)
        self.assertTrue(reconciled.already_accepted)
        self.assertEqual(accepted[0], accepted[1])
        self.assertEqual([r[1] for r in self.requests], ["/v1/workflows/events"] * 2)

    async def test_wrong_command_kind_identity_endpoint_and_kwargs_never_dispatch(self):
        workflow = self.client.workflows.handle("workflow")
        prepared = workflow.prepare_event("approval", event=event())
        task = self.client.tasks.prepare(program="echo", version="1", queue="q", data=None, idempotency_key="x")
        for supplied in (task, {}, "approval"):
            with self.assertRaises(InputError): await workflow.send_event(supplied)
        with self.assertRaises(InputError): await workflow.send_event(prepared, key="approval")
        with self.assertRaises(InputError): await workflow.send_event(prepared, event=event())
        with self.assertRaises(InputError): await workflow.send_event()
        with self.assertRaises(InputError): await workflow.send_event(key="approval")
        with self.assertRaises(InputError): await self.client.workflows.handle("different").send_event(prepared)
        for url, tenant in ((self.url, "other"), ("http://127.0.0.1:9", "tenant")):
            async with AsyncClient(url, tenant=tenant, namespace="tests") as other:
                with self.assertRaises(InputError):
                    await other.workflows.handle("workflow").send_event(prepared)
        self.assertEqual(self.requests, [])

    async def test_malformed_or_mismatched_receipt_is_uncertain_not_accepted(self):
        changes = ({"scope": {**SCOPE, "tenant_id": "other"}}, {"workflow_id": "other"},
                   {"key": "other"}, {"event_id": "other"}, {"event_source": "/other"},
                   {"accepted_at": -1}, {"accepted_at": True}, {"accepted_at": 1 << 64},
                   {"already_accepted": 1}, {"already_accepted": None}, {"extra": 1})
        workflow = self.client.workflows.handle("workflow")
        for changed in changes:
            async def handle(request, body): return response(receipt(body, **changed))
            self.callback = handle
            with self.subTest(changed=changed), self.assertRaises(WorkflowEventUncertain) as raised:
                await workflow.send_event(key="approval", event=event())
            self.assertIsInstance(raised.exception.cause, ProtocolError)
        async def missing(request, body):
            value = receipt(body); del value["event_id"]
            return response(value)
        self.callback = missing
        with self.assertRaises(WorkflowEventUncertain):
            await workflow.send_event(key="approval", event=event())

    async def test_definitive_conflict_and_late_event_are_not_uncertain(self):
        workflow = self.client.workflows.handle("workflow")
        for code, error in (("conflict", Conflict), ("obsolete_operation", ServiceError)):
            async def handle(request, body): return response({"code": code}, code=409)
            self.callback = handle
            with self.assertRaises(error) as raised:
                await workflow.send_event(key="approval", event=event())
            self.assertEqual(raised.exception.code, code)

    async def test_dispatched_timeout_and_predispatch_expiration_are_distinct(self):
        async def delayed(request, body):
            await asyncio.sleep(.2)
            return response(receipt(body))
        self.callback = delayed
        async with AsyncClient(self.url, tenant="tenant", namespace="tests", request_timeout=.03) as client:
            workflow = client.workflows.handle("workflow")
            command = workflow.prepare_event("approval", event=event())
            with self.assertRaises(WorkflowEventUncertain) as raised:
                await workflow.send_event(command)
            self.assertIsInstance(raised.exception.cause, RequestTimeout)
            self.assertTrue(raised.exception.cause.dispatched)
        workflow = self.client.workflows.handle("workflow")
        with patch.object(self.client._require_transport(), "exchange",
                          new=AsyncMock(side_effect=RequestTimeout(dispatched=False))):
            with self.assertRaises(RequestTimeout) as raised:
                await workflow.send_event(key="approval", event=event())
            self.assertFalse(raised.exception.dispatched)
        self.assertEqual(len(self.requests), 1)

    async def test_caller_cancel_retains_prepared_event_for_manual_reconciliation(self):
        entered = asyncio.Event()
        async def delayed(request, body):
            entered.set()
            await asyncio.sleep(.2)
            return response(receipt(body))
        self.callback = delayed
        workflow = self.client.workflows.handle("workflow")
        command = workflow.prepare_event("approval", event=event())
        pending = asyncio.create_task(workflow.send_event(command))
        await entered.wait()
        pending.cancel()
        with self.assertRaises(asyncio.CancelledError): await pending
        async def reconcile(request, body): return response(receipt(body, already_accepted=True))
        self.callback = reconcile
        self.assertTrue((await workflow.send_event(command)).already_accepted)
        self.assertEqual(self.requests[0][3], self.requests[1][3])

    async def test_boundary_event_identity_receipt_and_depth64_cross_http(self):
        async def handle(request, body): return response(receipt(body))
        self.callback = handle
        nested = None
        for _ in range(64): nested = [nested]
        workflow = self.client.workflows.handle("workflow")
        large_id = "é" * 64
        result = await workflow.send_event(key="approval", event=event(id=large_id, source="/" + "a" * 2047, data=nested))
        self.assertEqual(result.event_id, large_id)
        with self.assertRaises(InputError):
            await workflow.send_event(key="approval", event=event(data=[nested]))
        self.assertEqual(len(self.requests), 1)

    async def test_timestamp_parity_preserves_accepted_values_and_never_dispatches_rejections(self):
        vectors = json.loads(Path(__file__).with_name("workflow-event-times.json").read_text())
        async def handle(request, body): return response(receipt(body))
        self.callback = handle
        workflow = self.client.workflows.handle("workflow")
        for timestamp in vectors["accepted"]:
            with self.subTest(timestamp=timestamp):
                await workflow.send_event(key="approval", event=event(time=timestamp))
                self.assertEqual(json.loads(self.requests[-1][3])["event"]["time"], timestamp)
        sent = len(self.requests)
        for timestamp in vectors["rejected"]:
            with self.subTest(timestamp=timestamp), self.assertRaises(InputError):
                await workflow.send_event(key="approval", event=event(time=timestamp))
        self.assertEqual(len(self.requests), sent)

    async def test_invalid_event_or_key_rejected_before_dispatch(self):
        workflow = self.client.workflows.handle("workflow")
        for item in invalid_events():
            with self.subTest(item=type(item).__name__), self.assertRaises(InputError):
                await workflow.send_event(key="approval", event=item)
        for key in (True, "", "x" * 129, "bad\n", "\ufdd0"):
            with self.assertRaises(InputError): await workflow.send_event(key=key, event=event())
        self.assertEqual(self.requests, [])


def invalid_events():
    values = [None, [], event(specversion="0.3"), event(id=""), event(id=1), event(id="é" * 65), event(id="a" * (20 * 1024)),
              event(source="/" + "a" * 2048),
              event(source="http://bad host"), event(source="/bad%xx"), event(source="http://[bad]/"),
              event(source="1:relative"), event(source="http://x:bad/"),
              event(dataschema="relative"), event(dataschema="https://x#f"),
              event(time="2023-02-29T00:00:00Z"), event(time="2024-01-01T24:00:00Z"),
              event(time="2024-01-01T00:00:00+25:00"), event(subject=True), event(subject=""),
              event(datacontenttype="text/plain"), event(custom=None), event(custom=[]),
              event(custom=1.0), event(custom=2147483648), event(custom=-2147483649),
              event(custom="bad\n"), event(custom="\ufdd0"), event(**{"Bad": "name"}),
              event(tracestate="a=b"), event(traceparent="00-" + "0" * 32 + "-" + "1" * 16 + "-01"),
              event(data="x" * EVENT_LIMIT), event(data=float("nan"))]
    missing = event(); del missing["data"]; values.append(missing)
    return values


class CloudEventValidationTests(unittest.TestCase):
    def test_common_profile_preserves_valid_uri_time_trace_and_context_extensions(self):
        for source in ("/relative", "../relative", "?query", "#fragment", "urn:invoice:1",
                       "https://user:pass@example.test:443/a%20b?q=x#f", "http://[::1]/",
                       "http://[v1.address]/", "https://example.test", "//example.test/path"):
            value = event(source=source, dataschema="urn:invoice:schema", time="2024-02-29t23:59:60.5z",
                          ldgattemptno=True, customflag=False, count=2147483647,
                          traceparent="00-" + "1" * 32 + "-" + "2" * 16 + "-01",
                          tracestate="vendor=one, tenant@system=two")
            self.assertIs(validate_event(value), value)

    def test_complete_event_byte_boundary_counts_the_envelope(self):
        value = event(data="")
        overhead = len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())
        value["data"] = "x" * (EVENT_LIMIT - overhead)
        self.assertIs(validate_event(value), value)
        value["data"] += "x"
        with self.assertRaises(InputError): validate_event(value)

    def test_oversized_event_rejects_before_full_encoding_allocation(self):
        value = event(data="x" * (8 * 1024 * 1024))
        tracemalloc.start()
        try:
            with self.assertRaises(InputError): validate_event(value)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertLess(peak, 256 * 1024)
