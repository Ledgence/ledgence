"""Registered workflow entrypoints and real bootstrap routing (MIT)."""
import unittest
from enum import Enum, StrEnum

import test_workflow
from ledgence.worker.workflow import Workflow, WorkflowContext, WorkflowError, _workflow


class Entry(StrEnum):
    BEGIN = "begin"
    NEXT = "next"


class Other(StrEnum):
    BEGIN = "begin"
    NEXT = "next"


class RegistryTests(unittest.TestCase):
    def test_enum_family_alias_and_invalid_wire_id_validation(self):
        class Aliased(StrEnum):
            ONE = "one"
            TWO = "one"
        class Empty(StrEnum):
            pass
        class Invalid(StrEnum):
            BAD = "bad\n"
        class Plain(Enum):
            ONE = "one"
        for value in (None, str, Plain, Empty, Aliased, Invalid):
            with self.subTest(value=value), self.assertRaises(WorkflowError):
                Workflow(value)
        workflow = Workflow(Entry)
        for entry in (Other.BEGIN, "begin", None):
            with self.assertRaisesRegex(WorkflowError, "enum family"):
                workflow.entrypoint(entry)
        with self.assertRaises(WorkflowError):
            workflow.entrypoint(Entry.BEGIN, default=1)

    def test_missing_handlers_defaults_duplicates_and_freezing(self):
        for defaults in ((), (Entry.BEGIN, Entry.NEXT)):
            workflow = Workflow(Entry)
            for entry in Entry:
                workflow.entrypoint(entry, default=entry in defaults)(lambda event, ctx: None)
            with self.assertRaisesRegex(WorkflowError, "exactly one default"):
                workflow.build()
        workflow = Workflow(Entry)
        workflow.entrypoint(Entry.BEGIN, default=True)(lambda event, ctx: None)
        with self.assertRaisesRegex(WorkflowError, "requires a handler"):
            workflow.build()
        with self.assertRaisesRegex(WorkflowError, "duplicate"):
            workflow.entrypoint(Entry.BEGIN)(lambda event, ctx: None)
        with self.assertRaisesRegex(WorkflowError, "callable"):
            workflow.entrypoint(Entry.NEXT)(None)
        delayed = workflow.entrypoint(Entry.NEXT)
        delayed(lambda event, ctx: None)
        self.assertIs(workflow.build(), workflow.build())
        with self.assertRaisesRegex(WorkflowError, "frozen"):
            workflow.entrypoint(Entry.BEGIN)
        with self.assertRaisesRegex(WorkflowError, "frozen"):
            delayed(lambda event, ctx: None)

    def test_literal_start_must_be_default(self):
        class HasStart(StrEnum):
            START = "start"
            NEXT = "next"
        workflow = Workflow(HasStart)
        workflow.entrypoint(HasStart.START)(lambda event, ctx: None)
        workflow.entrypoint(HasStart.NEXT, default=True)(lambda event, ctx: None)
        with self.assertRaisesRegex(WorkflowError, "start entrypoint must be the default"):
            workflow.build()


class BoundContextTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.contexts = []

    async def asyncTearDown(self):
        for context in self.contexts:
            await context._finish(cancel=True)

    def context(self, **changes):
        async def no_rpc(*args):
            self.fail("routing must not perform an RPC")
        context = WorkflowContext(test_workflow.payload(**changes), no_rpc)
        self.contexts.append(context)
        return context

    async def invoke(self, workflow, ctx, event=None):
        token = _workflow.set(ctx)
        try:
            return await workflow.build()(event)
        finally:
            _workflow.reset(token)

    async def test_sync_and_async_handlers_receive_original_event_and_enum(self):
        workflow = Workflow(Entry)
        seen = []
        @workflow.entrypoint(Entry.BEGIN, default=True)
        def begin(event, ctx):
            seen.append((event, ctx.entrypoint))
            return ctx.continue_(continuation=Entry.NEXT, state={"count": 1})
        @workflow.entrypoint(Entry.NEXT)
        async def next_(event, ctx):
            seen.append((event, ctx.entrypoint))
            return ctx.complete(ctx.state)
        event = {"data": {"entrypoint": "user-owned"}}
        first = self.context()
        decision = await self.invoke(workflow, first, event)
        self.assertEqual(first._validate_decision(decision)["continuation"], "next")
        second = self.context(continuation="next", revision=1, state={"count": 1})
        self.assertEqual((await self.invoke(workflow, second, event))["output"], {"count": 1})
        self.assertIs(seen[0][0], event)
        self.assertEqual([entry for _, entry in seen], [Entry.BEGIN, Entry.NEXT])
        with self.assertRaisesRegex(WorkflowError, "unknown workflow entrypoint"):
            await self.invoke(workflow, self.context(continuation="missing"))

    async def test_all_transitions_validate_exact_enum_family_before_encoding(self):
        workflow = Workflow(Entry)
        workflow.entrypoint(Entry.BEGIN, default=True)(lambda event, ctx: ctx.complete(None))
        workflow.entrypoint(Entry.NEXT)(lambda event, ctx: ctx.complete(None))
        ctx = self.context()
        await self.invoke(workflow, ctx)
        transitions = [lambda entry: ctx.continue_(continuation=entry, state=None),
                       lambda entry: ctx.suspend(continuation=entry, state=None),
                       lambda entry: ctx.wait_event("event", continuation=entry, state=None),
                       lambda entry: ctx.sleep("timer", 1, continuation=entry, state=None)]
        self.assertEqual(Entry.NEXT, Other.NEXT)  # StrEnum equality alone is insufficient.
        for transition in transitions:
            self.assertEqual(transition(Entry.NEXT)["continuation"], "next")
            for wrong in (Other.NEXT, "next", 1):
                with self.assertRaisesRegex(WorkflowError, "enum family"):
                    transition(wrong)
        forged = dict(ctx.continue_(continuation=Entry.NEXT, state=None), continuation="missing")
        with self.assertRaisesRegex(WorkflowError, "unknown workflow entrypoint"):
            ctx._validate_decision(forged)
        raw = self.context()
        self.assertEqual(raw.continue_(continuation="unregistered", state=None)["continuation"],
                         "unregistered")
        self.assertEqual(raw.entrypoint, "start")


class RegistryProtocolTests(unittest.TestCase):
    launch = test_workflow.WorkflowProtocolTests.launch
    invocation = test_workflow.WorkflowProtocolTests.invocation

    def test_real_bootstrap_routing_and_registry_isolation_in_one_warm_process(self):
        source = '''import os
from enum import StrEnum
from ledgence.worker.workflow import Workflow, WorkflowError, workflow_context
class A(StrEnum):
    FIRST = "first"
    NEXT = "next"
class B(StrEnum):
    FIRST = "first"
    NEXT = "next"
a, b = Workflow(A), Workflow(B)
@a.entrypoint(A.FIRST, default=True)
def first_a(event, ctx):
    assert ctx.entrypoint is A.FIRST
    return ctx.continue_(continuation=A.NEXT, state={"pid": os.getpid()})
@a.entrypoint(A.NEXT)
async def next_a(event, ctx):
    assert ctx.entrypoint is A.NEXT
    return ctx.complete({"pid": os.getpid(), "event": event})
@b.entrypoint(B.FIRST, default=True)
def first_b(event, ctx):
    assert ctx.entrypoint is B.FIRST
    try: ctx.continue_(continuation=A.NEXT, state=None)
    except WorkflowError: pass
    else: raise AssertionError("registry leaked")
    return ctx.continue_(continuation=B.NEXT, state={"pid": os.getpid()})
@b.entrypoint(B.NEXT)
def next_b(event, ctx): return ctx.complete(None)
ah, bh = a.build(), b.build()
async def handle(event):
    return await (bh if event["id"] == "evt-3" else ah)(event)
'''
        resumed = test_workflow.payload(continuation="next", revision=1)
        messages = [self.invocation(), self.invocation("2", resumed), self.invocation("3")]
        process, frames = self.launch(source, messages, protocol=3)
        self.assertEqual(process.returncode, 0, process.stderr)
        results = [frame for frame in frames if frame["type"] == "result"]
        self.assertEqual([frame["status"] for frame in results], ["success"] * 3)
        self.assertEqual(results[0]["output"]["state"]["pid"], results[1]["output"]["output"]["pid"])
        self.assertEqual(results[2]["output"]["state"]["pid"], results[1]["output"]["output"]["pid"])
        self.assertEqual(results[1]["output"]["output"]["event"], messages[1]["event"])

    def test_registered_handler_error_and_fail_keep_existing_retry_classification(self):
        source = '''from enum import StrEnum
from ledgence.worker.workflow import Workflow
class Entry(StrEnum): START = "start"
workflow = Workflow(Entry)
@workflow.entrypoint(Entry.START, default=True)
def start(event, ctx):
    if event["id"] == "evt-1": raise ValueError("retry activation")
    return ctx.fail("declined", "intentional failure")
handle = workflow.build()
'''
        process, frames = self.launch(source, [self.invocation(), self.invocation("2")], protocol=3)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(frames[1]["status"], "runtime_error")
        self.assertEqual(frames[2]["status"], "success")
        self.assertEqual(frames[2]["output"]["kind"], "fail")
