"""External event and durable timer workflow decisions (MIT)."""
import asyncio
import copy
import json
from pathlib import Path
import unittest

import test_workflow
from ledgence_worker.workflow import (
    MAX_DECISION_BYTES, MAX_EVENT_BYTES, MAX_WAIT_MS, WorkflowContext, WorkflowError,
    _encode,
)


def event(**changes):
    return {"specversion": "1.0", "id": "approval-1", "source": "/external",
            "type": "invoice.approved", "datacontenttype": "application/json",
            "data": {"approved": True}, **changes}


class WorkflowEventTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.contexts = []

    async def asyncTearDown(self):
        for context in self.contexts:
            await context._finish(cancel=True)

    def context(self, **changes):
        async def no_rpc(*args):
            self.fail("a wait must not issue a local RPC")
        context = WorkflowContext(test_workflow.payload(**changes), no_rpc)
        self.contexts.append(context)
        return context

    async def test_wake_is_optional_copied_and_separate_from_child_inputs(self):
        self.assertIsNone(self.context().wake)
        self.assertIsNone(self.context(wake=None).wake)
        for wake in ({"kind": "event", "key": "approval", "event": event(), "accepted_at": 7},
                     {"kind": "timeout", "key": "approval", "deadline": 8},
                     {"kind": "timer", "key": "retry:1", "deadline": 9}):
            original = copy.deepcopy(wake)
            context = self.context(wake=wake)
            wake["key"] = "changed"
            returned = context.wake
            returned["key"] = "also-changed"
            if returned["kind"] == "event":
                returned["event"]["data"]["approved"] = False
            self.assertEqual(context.wake, original)
            self.assertEqual(context.inputs, {})

    async def test_wait_decisions_capture_commands_state_and_zero_or_max_durations(self):
        context = self.context()
        context.task("audit", program="audit", version="1", queue="q", data={"a": 1})
        state = {"invoice": [1]}
        for duration in (None, 0, MAX_WAIT_MS):
            decision = context.wait_event("approval", continuation="approved", state=state,
                                          timeout_ms=duration)
            self.assertEqual(decision["kind"], "wait")
            self.assertEqual(decision["wait"], {"kind": "event", "key": "approval", "timeout_ms": duration})
            self.assertEqual(context._validate_decision(decision), decision)
            self.assertEqual(len(decision["commands"]), 1)
        decision = context.sleep("retry:1", 0, continuation="retry", state=state)
        state["invoice"].append(2)
        self.assertEqual(decision["state"], {"invoice": [1]})
        self.assertEqual(decision["wait"], {"kind": "timer", "key": "retry:1", "delay_ms": 0})
        self.assertEqual(context._validate_decision(decision), decision)
        self.assertEqual(context.sleep("year", MAX_WAIT_MS, continuation="next", state=None)["wait"]["delay_ms"], MAX_WAIT_MS)

    async def test_duration_key_shape_and_forged_decision_rejections(self):
        context = self.context()
        for duration in (True, False, -1, 0.0, "1", MAX_WAIT_MS + 1, 1 << 64):
            with self.subTest(duration=duration):
                with self.assertRaises(WorkflowError):
                    context.wait_event("key", continuation="next", state=None, timeout_ms=duration)
                with self.assertRaises(WorkflowError):
                    context.sleep("key", duration, continuation="next", state=None)
        for key in (None, "", "x" * 129, "bad\n", "\ufdd0", "\ud800"):
            with self.assertRaises(WorkflowError):
                context.wait_event(key, continuation="next", state=None)
        base = context.wait_event("key", continuation="next", state=None)
        for changed in ({"wait": {"kind": "event", "key": "key"}},
                        {"wait": {"kind": "event", "key": "key", "timeout_ms": None, "extra": 1}},
                        {"wait": {"kind": "timer", "key": "key", "delay_ms": None}},
                        {"wait": {"kind": "children", "key": "key"}},
                        {"continuation": ""}, {"revision": True}, {"activation_id": "other"},
                        {"state": "x" * (64 * 1024)}):
            with self.assertRaises(WorkflowError):
                context._validate_decision(dict(base, **changed))

    async def test_wait_cannot_drop_later_commands_or_run_inside_local_callable(self):
        context = self.context()
        decision = context.wait_event("approval", continuation="next", state=None)
        context.task("late", program="audit", version="1", queue="q", data=None)
        with self.assertRaisesRegex(WorkflowError, "staged child commands"):
            context._validate_decision(decision)
        for key, operation in (("event", lambda: context.wait_event("key", continuation="next", state=None)),
                               ("timer", lambda: context.sleep("key", 1, continuation="next", state=None))):
            with self.assertRaisesRegex(WorkflowError, "use the controller"):
                await context.local(key, operation)
        await context._finish()
        with self.assertRaisesRegex(WorkflowError, "closed"):
            context.sleep("closed", 0, continuation="next", state=None)

    async def test_wake_strict_shape_and_shared_input_budget(self):
        good = {"kind": "event", "key": "approval", "event": event(), "accepted_at": 1}
        for wake in ([], {}, {**good, "key": ""}, {**good, "accepted_at": True},
                     {**good, "accepted_at": -1}, {**good, "extra": 1},
                     {"kind": "timer", "key": "key", "deadline": 1.0},
                     {"kind": "timeout", "key": "key", "deadline": None},
                     {"kind": "timer", "key": "key", "deadline": 1, "event": event()}):
            with self.assertRaises(WorkflowError): self.context(wake=wake)
        child = {"task_id": "child", "state": "succeeded", "outcome": {
            "kind": "succeeded", "attempt_id": "attempt", "quiescence": "confirmed",
            "execution_may_have_started": True, "output": ""}}
        inputs = {"child": child}
        overhead = len(_encode(inputs, MAX_DECISION_BYTES, 96))
        child["outcome"]["output"] = "x" * (MAX_DECISION_BYTES - overhead)
        self.context(inputs=inputs)
        with self.assertRaises(WorkflowError): self.context(inputs=inputs, wake=good)
        nested = None
        for _ in range(64): nested = [nested]
        self.assertEqual(self.context(wake={**good, "event": event(data=nested)}).wake["event"]["data"], nested)
        with self.assertRaises(WorkflowError): self.context(wake={**good, "event": event(data=[nested])})

    async def test_event_time_parity_includes_offsets_rollover_and_year_zero(self):
        vectors = json.loads(Path(__file__).with_name("workflow-event-times.json").read_text())
        for timestamp in vectors["accepted"]:
            with self.subTest(timestamp=timestamp):
                wake = {"kind": "event", "key": "approval", "event": event(time=timestamp), "accepted_at": 1}
                self.assertEqual(self.context(wake=wake).wake["event"]["time"], timestamp)
        for timestamp in vectors["rejected"]:
            with self.subTest(timestamp=timestamp), self.assertRaises(WorkflowError):
                self.context(wake={"kind": "event", "key": "approval", "event": event(time=timestamp), "accepted_at": 1})

    async def test_event_common_profile_and_size_validation(self):
        rich = event(id="é" * 64, source="/" + "a" * 2047, subject="invoice/1", time="2024-02-29t23:59:60.5z",
                     dataschema="urn:invoice:schema", customflag=True, count=-2147483648,
                     traceparent="00-" + "1" * 32 + "-" + "2" * 16 + "-01",
                     tracestate="vendor=one", ldgtaskid="original-task")
        wake = {"kind": "event", "key": "key", "event": rich, "accepted_at": 1}
        self.assertEqual(self.context(wake=wake).wake["event"], rich)
        bad = [event(specversion="0.3"), event(id=""), event(id="é" * 65), event(source="/" + "a" * 2048), event(source="http://bad host"),
               event(source="/bad%xx"), event(dataschema="relative"), event(dataschema="https://x#f"),
               event(time="2023-02-29T00:00:00Z"), event(subject=True), event(subject=""),
               event(datacontenttype="text/plain"), event(custom=None), event(custom=[]),
               event(custom=1.5), event(custom=2147483648), event(**{"Bad": "name"}),
               event(tracestate="a=b"), event(data="x" * MAX_EVENT_BYTES)]
        missing = event(); del missing["data"]; bad.append(missing)
        for item in bad:
            with self.subTest(item=list(item)), self.assertRaises(WorkflowError):
                self.context(wake={**wake, "event": item})


class WorkflowWaitProtocolTests(unittest.TestCase):
    launch = test_workflow.WorkflowProtocolTests.launch
    invocation = test_workflow.WorkflowProtocolTests.invocation

    def test_old_protocol_user_results_named_wait_are_unchanged(self):
        source = 'def handle(event): return {"kind": "wait", "wait": event["data"]}'
        for version in (1, 2):
            message = test_workflow.test_bootstrap.BootstrapTests.invocation(self)
            message["v"] = version
            if version == 2:
                message["processing_context"] = None
            process, frames = self.launch(source, [message], protocol=version)
            self.assertEqual(process.returncode, 0, process.stderr)
            self.assertEqual(frames[-1]["output"], {"kind": "wait", "wait": message["event"]["data"]})

    def test_wait_decision_ends_invocation_and_wake_does_not_leak_on_reuse(self):
        source = '''from ledgence_worker.workflow import workflow_context
def handle(event):
    ctx = workflow_context()
    if event["id"] == "evt-1":
        return ctx.sleep("timer:1", 0, continuation="after", state={"saved": 1})
    return ctx.complete({"wake": ctx.wake, "inputs": ctx.inputs})
'''
        first = self.invocation()
        second = self.invocation("2", test_workflow.payload(wake={"kind": "timer", "key": "timer:1", "deadline": 1}))
        third = self.invocation("3")
        process, frames = self.launch(source, [first, second, third, {"v": 3, "type": "shutdown"}], protocol=3)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual([x["type"] for x in frames], ["ready", "result", "result", "result", "closing"])
        self.assertEqual(frames[1]["output"]["kind"], "wait")
        self.assertEqual(frames[2]["output"]["output"]["wake"]["kind"], "timer")
        self.assertIsNone(frames[3]["output"]["output"]["wake"])
        self.assertTrue(all(x["status"] == "success" for x in frames if x["type"] == "result"))
