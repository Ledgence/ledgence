"""Public workflow client behavior across the real HTTP transport."""
import asyncio
import json
import unittest

from aiohttp import web
from ledgence.client import (
    AsyncClient, CancellationUncertain, Conflict, InputError, ProtocolError, RetryPolicy,
    SubmissionUncertain, WaitTimeout, WorkflowCancellationUncertain, WorkflowCancelled,
    WorkflowFailed, WorkflowState, WorkflowSubmission, WorkflowWaitTimeout,
)
from ledgence.client.workflow_models import parse_workflow_result, parse_workflow_status
from support import SCOPE, response


def snapshot(state="running", **changes):
    terminal = state in {"succeeded", "failed", "cancelled"}
    return {"workflow_id": "workflow", "scope": dict(SCOPE), "state": state,
            "revision": 1 if terminal else 0,
            "activation_id": "activation" if state == "running" else None,
            "submitted_at": 1, "terminal_at": 3 if terminal else None,
            "correlation_key": None, **changes}


def completed(state="succeeded", output=None):
    outcome = {"kind": state}
    if state == "succeeded": outcome["output"] = output
    if state == "failed": outcome["error"] = {"kind": "declined", "message": "no approval"}
    if state not in {"succeeded", "failed", "cancelled"}: outcome = None
    return {"workflow": snapshot(state), "outcome": outcome}


class WorkflowClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests = []
        self.callback = None
        async def handle(request):
            body = await request.read()
            self.requests.append((request.method, request.path, dict(request.query), body))
            if self.callback is not None:
                return await self.callback(request, body)
            if request.path == "/v1/workflows":
                value = snapshot(correlation_key=json.loads(body)["input"].get("correlation_key"))
                return response(value)
            if request.path == "/v1/workflows/status": return response(snapshot("succeeded"))
            if request.path == "/v1/workflows/result": return response(completed(output={"answer": 42}))
            if request.path == "/v1/workflows/cancel": return response(snapshot("cancelling"))
            return response({"code": "not_found"}, code=404)
        app = web.Application(client_max_size=3 * 1024 * 1024)
        app.router.add_route("*", "/{path:.*}", handle)
        runner = web.AppRunner(app, shutdown_timeout=.1)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        self.url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        self.client = AsyncClient(self.url, tenant="tenant", namespace="tests")
        await self.client.__aenter__()
        self.addAsyncCleanup(self.client.close)

    def args(self, data=None):
        return {"program": "controller", "version": "1.0.0", "queue": "queue",
                "data": data, "idempotency_key": "submit"}

    async def test_submit_result_and_reconnect_preserve_scoped_identity(self):
        workflow = await self.client.workflows.submit(**self.args({"invoice": "INV-1"}))
        self.assertEqual(workflow.id, "workflow")
        self.assertEqual(workflow.scope, self.client.scope)
        self.assertEqual(await workflow.result(), {"answer": 42})
        resumed = self.client.workflows.handle(workflow.id)
        self.assertEqual(resumed, workflow)
        self.assertEqual((await resumed.status()).state, WorkflowState.SUCCEEDED)
        self.assertEqual((await resumed.outcome()).outcome.kind, "succeeded")
        self.assertEqual([r[0] for r in self.requests], ["POST", "GET", "GET", "GET", "GET"])
        self.assertEqual(self.requests[1][2], {**SCOPE, "workflow_id": "workflow"})

    async def test_frozen_command_and_trace_survive_data_mutation(self):
        data = {"nested": [1, -0.0]}
        frozen = self.client.workflows.prepare(**self.args(data), correlation_key="INV-1",
                                              retry_policy=RetryPolicy(2, 10))
        self.assertIs(type(frozen), WorkflowSubmission)
        data["nested"].append(99)
        frozen.to_dict()["input"]["data"]["nested"][0] = 99
        await self.client.workflows.submit(frozen)
        sent = json.loads(self.requests[0][3])
        self.assertEqual(sent["input"]["data"], {"nested": [1, -0.0]})
        self.assertEqual(sent["input"]["retry_policy"], {"max_attempts": 2, "retry_delay_ms": 10})
        with self.assertRaises(AttributeError): frozen.scope = None
        with self.assertRaises(TypeError): WorkflowSubmission()

    async def test_wrong_submission_kind_endpoint_scope_and_kwargs_never_dispatch(self):
        task = self.client.tasks.prepare(**self.args())
        workflow = self.client.workflows.prepare(**self.args())
        with self.assertRaises(InputError): await self.client.workflows.submit(task)
        with self.assertRaises(InputError): await self.client.tasks.submit(workflow)
        with self.assertRaises(InputError): await self.client.workflows.submit(workflow, data=None)
        for url, tenant in ((self.url, "other"), ("http://127.0.0.1:9", "tenant")):
            async with AsyncClient(url, tenant=tenant, namespace="tests") as other:
                with self.assertRaises(InputError): await other.workflows.submit(workflow)
        self.assertEqual(self.requests, [])

    async def test_invalid_input_does_not_dispatch(self):
        for changes in ({"queue": ""}, {"data": float("nan")}, {"retry_policy": {}},
                        {"attempt_timeout_ms": True}, {"version": "../bad"}):
            with self.assertRaises(InputError):
                await self.client.workflows.submit(**dict(self.args(), **changes))
        with self.assertRaises(InputError): await self.client.workflows.submit()
        self.assertEqual(self.requests, [])

    async def test_uncertain_submission_retains_body_without_automatic_resend(self):
        accepted = []
        async def handle(request, body):
            accepted.append(body)
            if len(accepted) == 1:
                return response({"code": "unavailable", "message": "lost after acceptance"}, code=503)
            return response(snapshot())
        self.callback = handle
        frozen = self.client.workflows.prepare(**self.args({"values": [1, -0.0]}))
        with self.assertRaises(SubmissionUncertain) as uncertain:
            await self.client.workflows.submit(frozen)
        self.assertIs(uncertain.exception.submission, frozen)
        self.assertEqual(len(accepted), 1)
        resumed = await self.client.workflows.submit(uncertain.exception.submission)
        self.assertEqual(resumed.id, "workflow")
        self.assertEqual(accepted[0], accepted[1])

    async def test_bad_submission_identity_or_correlation_is_uncertain(self):
        for changes in ({"scope": {**SCOPE, "tenant_id": "other"}},
                        {"correlation_key": "other"}, {"workflow_id": ""}, {"extra": 1}):
            async def handle(request, body): return response(snapshot(**changes))
            self.callback = handle
            with self.assertRaises(SubmissionUncertain) as raised:
                await self.client.workflows.submit(**self.args())
            self.assertIsInstance(raised.exception.cause, ProtocolError)

    async def test_definitive_conflict_is_not_uncertain(self):
        async def handle(request, body): return response({"code": "conflict"}, code=409)
        self.callback = handle
        with self.assertRaises(Conflict): await self.client.workflows.submit(**self.args())

    async def test_observation_timeout_keeps_workflow_reusable_and_never_resubmits(self):
        async def waiting(request, body): return response(snapshot("waiting"))
        self.callback = waiting
        workflow = self.client.workflows.handle("workflow")
        with self.assertRaises(WorkflowWaitTimeout) as raised:
            await workflow.result(timeout=.05)
        self.assertIsInstance(raised.exception, WaitTimeout)
        self.assertIs(raised.exception.workflow, workflow)
        self.assertEqual(raised.exception.last_status.state, "waiting")
        self.assertTrue(all(r[0] == "GET" for r in self.requests))
        self.callback = None
        self.assertEqual(await workflow.result(timeout=1), {"answer": 42})
        self.assertTrue(all(r[0] == "GET" for r in self.requests))

    async def test_terminal_result_fetch_retries_observation_without_repeating_status(self):
        calls = 0
        async def handle(request, body):
            nonlocal calls
            if request.path.endswith("/status"): return response(snapshot("succeeded"))
            calls += 1
            if calls == 1:
                return response({"code": "unavailable", "message": "temporary"}, code=503)
            return response(completed(output=None))
        self.callback = handle
        workflow = self.client.workflows.handle("workflow")
        self.assertIsNone(await workflow.result(timeout=2))
        self.assertEqual([r[1] for r in self.requests],
                         ["/v1/workflows/status", "/v1/workflows/result", "/v1/workflows/result"])

    async def test_changed_terminal_metadata_fails_without_retry(self):
        async def handle(request, body):
            if request.path.endswith("/status"): return response(snapshot("succeeded"))
            value = completed(output=42)
            value["workflow"]["revision"] += 1
            return response(value)
        self.callback = handle
        with self.assertRaises(ProtocolError):
            await self.client.workflows.handle("workflow").result(timeout=.2)
        self.assertEqual(len(self.requests), 2)

    async def test_terminal_failure_and_cancellation_are_distinct_from_observation(self):
        for state, error in (("failed", WorkflowFailed), ("cancelled", WorkflowCancelled)):
            async def handle(request, body):
                return response(snapshot(state) if request.path.endswith("/status") else completed(state))
            self.callback = handle
            with self.assertRaises(error) as raised:
                await self.client.workflows.handle("workflow").result(timeout=1)
            self.assertEqual(raised.exception.result.workflow.state, state)
            if state == "failed": self.assertEqual(raised.exception.error.kind, "declined")

    async def test_cancellation_returns_snapshot_and_preserves_uncertainty(self):
        workflow = self.client.workflows.handle("workflow")
        self.assertEqual((await workflow.cancel()).state, "cancelling")
        self.assertEqual(json.loads(self.requests[0][3]), {"scope": SCOPE, "workflow_id": "workflow"})
        async def handle(request, body):
            return response({"code": "unavailable", "message": "unknown"}, code=503)
        self.callback = handle
        with self.assertRaises(WorkflowCancellationUncertain) as raised:
            await workflow.cancel()
        self.assertIsInstance(raised.exception, CancellationUncertain)
        self.assertIs(raised.exception.workflow, workflow)
        self.assertEqual(len(self.requests), 2)

    async def test_status_identity_and_impossible_metadata_are_rejected(self):
        cases = ({"workflow_id": "other"}, {"scope": {**SCOPE, "namespace": "other"}},
                 {"revision": True}, {"revision": 1 << 64}, {"state": "unknown"},
                 {"activation_id": None}, {"terminal_at": 3}, {"submitted_at": -1},
                 {"correlation_key": "\n"}, {"extra": 1})
        for changes in cases:
            with self.subTest(changes=changes), self.assertRaises(ProtocolError):
                parse_workflow_status(snapshot(**changes), self.client.scope, "workflow")
        for changes in ({"activation_id": "active"}, {"terminal_at": None}, {"terminal_at": 0}):
            with self.assertRaises(ProtocolError):
                parse_workflow_status(snapshot("succeeded", **changes), self.client.scope, "workflow")

    async def test_result_validates_state_output_bounds_and_error_shape(self):
        pending = parse_workflow_result(completed("waiting"), self.client.scope, "workflow")
        self.assertIsNone(pending.outcome)
        invalid = [completed(output="x" * (256 * 1024))]
        value = completed(); value["outcome"]["kind"] = "failed"; invalid.append(value)
        value = completed("waiting"); value["outcome"] = {"kind": "cancelled"}; invalid.append(value)
        value = completed("failed"); value["outcome"]["error"]["kind"] = ""; invalid.append(value)
        value = completed("failed"); value["outcome"]["error"]["message"] = "x" * 4097; invalid.append(value)
        value = completed("cancelled"); value["outcome"]["output"] = None; invalid.append(value)
        for value in invalid:
            with self.assertRaises(ProtocolError):
                parse_workflow_result(value, self.client.scope, "workflow")
        nested = None
        for _ in range(64): nested = [nested]
        self.assertEqual(parse_workflow_result(completed(output=nested), self.client.scope,
                                               "workflow").outcome.output, nested)
        with self.assertRaises(ProtocolError):
            parse_workflow_result(completed(output=[nested]), self.client.scope, "workflow")

    async def test_root_and_nested_results_accept_rust_float_boundary_over_http(self):
        from ledgence.client import codec
        limit = 256 * 1024
        count = (limit - 1) // 5
        output = b"[" + b",".join([b"1e-8"] * count) + b"]"
        self.assertLessEqual(len(output), limit)
        expected = [1e-8] * count
        with self.assertRaises(InputError): codec.encode(expected, limit)
        for metadata in ({}, {"parent_workflow_id": "parent", "root_workflow_id": "root"}):
            current = snapshot("succeeded", **metadata)
            async def handle(request, body):
                if request.path.endswith("/status"):
                    return response(current)
                body = (b'{"workflow":' + json.dumps(current).encode()
                        + b',"outcome":{"kind":"succeeded","output":' + output + b'}}')
                return web.Response(body=body, content_type="application/json")
            self.callback = handle
            with self.subTest(metadata=metadata):
                received = await self.client.workflows.handle("workflow").result(timeout=5)
                self.assertEqual(received, expected)
        self.assertEqual(len(self.requests), 4)
        self.assertTrue(all(request[0] == "GET" for request in self.requests))

    async def test_task_observations_link_to_workflow_and_allow_controller_envelopes(self):
        from ledgence.client.models import parse_status, parse_result
        from support import status, result
        ordinary = parse_status(status(), self.client.scope, "task")
        self.assertIsNone(ordinary.workflow_id)
        linked = dict(status(), workflow_id="workflow")
        self.assertEqual(parse_status(linked, self.client.scope, "task").workflow_id, "workflow")
        for changes in ({"workflow_activation_id": "task"},
                        {"workflow_id": "workflow", "workflow_activation_id": "other"},
                        {"workflow_id": ""}):
            with self.assertRaises(ProtocolError):
                parse_status(dict(status(), **changes), self.client.scope, "task")
        nested = None
        for _ in range(64): nested = [nested]
        envelope = {"v": 1, "activation_id": "task", "revision": 0,
                    "kind": "complete", "output": nested}
        controller = result(output=envelope)
        with self.assertRaises(ProtocolError):
            parse_result(controller, self.client.scope, "task")
        controller["task"].update(workflow_id="workflow", workflow_activation_id="task")
        self.assertEqual(parse_result(controller, self.client.scope, "task").outcome.output, envelope)

    async def test_controller_depth_allowance_survives_actual_http_decode(self):
        from support import result
        nested = None
        for _ in range(96): nested = [nested]
        reply = result(output=nested)
        reply["task"].update(workflow_id="workflow", workflow_activation_id="task")
        async def handle(request, body):
            return response(reply)
        self.callback = handle
        task = self.client.tasks.handle("task")
        self.assertEqual((await task.outcome()).outcome.output, nested)
        reply["outcome"]["output"] = [nested]
        with self.assertRaises(ProtocolError):
            await task.outcome()
        reply["outcome"]["output"] = nested
        reply["task"].pop("workflow_activation_id")
        with self.assertRaises(ProtocolError):
            await task.outcome()

    async def test_caller_cancelled_wait_never_cancels_remote_workflow(self):
        entered = asyncio.Event()
        async def handle(request, body):
            entered.set()
            await asyncio.sleep(.2)
            return response(snapshot("waiting"))
        self.callback = handle
        waiting = asyncio.create_task(self.client.workflows.handle("workflow").wait())
        await entered.wait()
        waiting.cancel()
        with self.assertRaises(asyncio.CancelledError): await waiting
        self.assertTrue(all(r[0] == "GET" for r in self.requests))

    async def test_lineage_preserves_legacy_roots_and_owned_child_identities(self):
        for metadata in ({}, {"parent_workflow_id": None, "root_workflow_id": None}):
            status = parse_workflow_status(snapshot(**metadata), self.client.scope, "workflow")
            self.assertIsNone(status.parent_workflow_id)
            self.assertEqual(status.root_workflow_id, "workflow")
        for parent, root in (("root", "root"), ("parent", "root")):
            status = parse_workflow_status(snapshot(parent_workflow_id=parent,
                                           root_workflow_id=root), self.client.scope, "workflow")
            self.assertEqual((status.parent_workflow_id, status.root_workflow_id), (parent, root))
            with self.assertRaises(AttributeError): status.parent_workflow_id = "changed"
        for parent, root in ((None, "root"), ("parent", None), ("workflow", "root"),
                             ("parent", "workflow"), ("", "root"), ("parent", "x" * 129),
                             (1, "root"), ("parent", "bad\nroot")):
            with self.subTest(parent=parent, root=root), self.assertRaises(ProtocolError):
                parse_workflow_status(snapshot(parent_workflow_id=parent, root_workflow_id=root),
                                      self.client.scope, "workflow")

    async def test_root_submission_cannot_resolve_to_an_owned_child(self):
        async def handle(request, body):
            return response(snapshot(parent_workflow_id="parent", root_workflow_id="root"))
        self.callback = handle
        with self.assertRaises(SubmissionUncertain) as raised:
            await self.client.workflows.submit(**self.args())
        self.assertIsInstance(raised.exception.cause, ProtocolError)
        self.assertEqual(len(self.requests), 1)

    async def test_nested_result_and_independent_cancel_use_the_child_identity(self):
        metadata = dict(parent_workflow_id="parent", root_workflow_id="root")
        async def handle(request, body):
            if request.path.endswith("/cancel"):
                self.assertEqual(json.loads(body), {"scope": SCOPE, "workflow_id": "workflow"})
                return response(snapshot("cancelling", **metadata))
            self.assertEqual(dict(request.query), {"tenant_id": SCOPE["tenant_id"],
                             "namespace": SCOPE["namespace"], "workflow_id": "workflow"})
            if request.path.endswith("/status"):
                return response(snapshot("succeeded", **metadata))
            value = completed(output={"nested": True})
            value["workflow"].update(metadata)
            return response(value)
        self.callback = handle
        handle = self.client.workflows.handle("workflow")
        self.assertEqual(await handle.result(timeout=1), {"nested": True})
        status = await handle.cancel()
        self.assertEqual(status.state, "cancelling")
        self.assertEqual(status.parent_workflow_id, "parent")
        self.assertEqual(status.root_workflow_id, "root")

    async def test_wait_rejects_changed_parent_root_and_root_to_child_transitions(self):
        cases = [({}, {"parent_workflow_id": "parent", "root_workflow_id": "root"}),
                 ({"parent_workflow_id": "parent", "root_workflow_id": "root"}, {}),
                 ({"parent_workflow_id": "parent", "root_workflow_id": "root"},
                  {"parent_workflow_id": "other", "root_workflow_id": "root"}),
                 ({"parent_workflow_id": "parent", "root_workflow_id": "root"},
                  {"parent_workflow_id": "parent", "root_workflow_id": "other"})]
        for first, second in cases:
            count = 0
            async def handle(request, body):
                nonlocal count
                self.assertTrue(request.path.endswith("/status"))
                count += 1
                return response(snapshot("waiting", **(first if count == 1 else second)))
            self.callback = handle
            with self.subTest(first=first, second=second), self.assertRaises(ProtocolError):
                await self.client.workflows.handle("workflow").wait(timeout=2)
            self.assertEqual(count, 2)
        self.assertTrue(all(request[0] == "GET" for request in self.requests))

    async def test_wait_rejects_changed_lineage_in_terminal_result(self):
        async def handle(request, body):
            if request.path.endswith("/status"):
                return response(snapshot("succeeded", parent_workflow_id="parent",
                                         root_workflow_id="root"))
            value = completed()
            value["workflow"].update(parent_workflow_id="other", root_workflow_id="root")
            return response(value)
        self.callback = handle
        with self.assertRaises(ProtocolError):
            await self.client.workflows.handle("workflow").wait(timeout=1)
        self.assertEqual(len(self.requests), 2)
