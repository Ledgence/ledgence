"""Owned child workflow contract and reused-process regressions (MIT)."""
import copy
import inspect
import unittest

import test_workflow
import test_workflow_inbound
from ledgence_worker.workflow import (
    MAX_COMMANDS, MAX_DECISION_BYTES, TaskRef, WorkflowContext, WorkflowError, WorkflowRef,
    _encode,
)

ARGS = dict(program="child", version="1.0.0", queue="queue", data={"invoice": 42})


def child(kind="workflow", state="succeeded", output=None):
    outcome = {"kind": state}
    result = {"state": state, "outcome": outcome}
    if kind == "workflow":
        result.update(kind=kind, workflow_id="child-workflow")
        if state == "failed":
            outcome["error"] = {"kind": "business", "message": "declined"}
    else:
        result["task_id"] = "child-task"
        if state != "cancelled":
            outcome.update(attempt_id="attempt", quiescence="confirmed",
                           execution_may_have_started=True)
        if state == "failed":
            outcome["failure"] = {"kind": "application", "error": {
                "kind": "business", "message": "declined"}}
    if state == "succeeded":
        outcome["output"] = output
    return result


class SubworkflowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.contexts = []

    async def asyncTearDown(self):
        for context in self.contexts:
            await context._finish(cancel=True)

    def context(self, **changes):
        async def no_rpc(*args):
            self.fail("staging and observing children must not perform an RPC")
        context = WorkflowContext(test_workflow.payload(**changes), no_rpc)
        self.contexts.append(context)
        return context

    async def test_mixed_children_stage_atomically_with_legacy_task_shape(self):
        ctx = self.context()
        args = copy.deepcopy(ARGS)
        task = ctx.task("task", **args)
        flow = ctx.workflow("workflow", **args)
        args["data"]["invoice"] = 99
        self.assertIs(type(task), TaskRef)
        self.assertIs(type(flow), WorkflowRef)
        self.assertFalse(inspect.isawaitable(task))
        self.assertFalse(inspect.isawaitable(flow))
        decision = ctx.suspend(continuation="collect", state=None,
                               until=[task, flow, "earlier-child"])
        self.assertEqual(decision["until"], ["task", "workflow", "earlier-child"])
        legacy, nested = decision["commands"]
        self.assertNotIn("kind", legacy)
        self.assertEqual(nested["kind"], "workflow")
        self.assertEqual(legacy["data"], nested["data"])
        self.assertEqual(nested["data"], ARGS["data"])
        self.assertEqual(ctx._validate_decision(decision), decision)
        with self.assertRaises(WorkflowError):
            ctx.complete(None)

    async def test_keys_share_one_namespace_and_exact_binding_is_reusable(self):
        for first_kind, second_kind in (("task", "workflow"), ("workflow", "task")):
            ctx = self.context()
            first = getattr(ctx, first_kind)("same", **ARGS)
            repeated = getattr(ctx, first_kind)("same", **ARGS)
            self.assertEqual(first.key, repeated.key)
            with self.assertRaisesRegex(WorkflowError, "different binding"):
                getattr(ctx, second_kind)("same", **ARGS)
            for field, value in (("data", {"invoice": 42.0}), ("program", "other"),
                                 ("version", "2"), ("queue", "other"),
                                 ("attempt_timeout_ms", 60000),
                                 ("retry_policy", {"max_attempts": 1, "retry_delay_ms": 0})):
                with self.subTest(field=field), self.assertRaises(WorkflowError):
                    getattr(ctx, first_kind)("same", **dict(ARGS, **{field: value}))
            self.assertEqual(len(ctx.continue_(continuation="next", state=None)["commands"]), 1)
            with self.assertRaisesRegex(WorkflowError, "another activation"):
                self.context().suspend(continuation="next", state=None, until=[first])

    async def test_command_and_wait_limits_are_combined_across_kinds(self):
        ctx = self.context()
        refs = [getattr(ctx, "workflow" if i % 2 else "task")(str(i), **ARGS)
                for i in range(MAX_COMMANDS)]
        self.assertEqual(len(ctx.suspend(continuation="next", state=None,
                                         until=refs)["until"]), MAX_COMMANDS)
        for method in (ctx.workflow, ctx.task):
            with self.assertRaisesRegex(WorkflowError, "too many"):
                method("extra", **ARGS)
        with self.assertRaises(WorkflowError):
            ctx.suspend(continuation="next", state=None, until=[*refs, "earlier"])
        with self.assertRaises(WorkflowError):
            ctx.suspend(continuation="next", state=None, until=[refs[0], refs[0].key])

    async def test_both_terminal_kinds_are_inspectable_and_outputs_are_copied(self):
        inputs = {f"{kind}-{state}": child(kind, state, {"items": [1]})
                  for kind in ("task", "workflow")
                  for state in ("succeeded", "failed", "cancelled")}
        ctx = self.context(inputs=inputs)
        for kind in ("task", "workflow"):
            key = kind + "-succeeded"
            ref = getattr(ctx, kind)(key, **ARGS)
            output = ctx.get_result(ref)
            output["items"].append(2)
            self.assertEqual(ctx.get_result(key), {"items": [1]})
            for state in ("failed", "cancelled"):
                self.assertEqual(ctx.inputs[kind + "-" + state]["outcome"]["kind"], state)
                with self.assertRaisesRegex(WorkflowError, "did not succeed"):
                    ctx.get_result(kind + "-" + state)
        ctx.inputs["workflow-succeeded"]["outcome"]["output"]["items"].clear()
        self.assertEqual(ctx.get_result("workflow-succeeded"), {"items": [1]})
        with self.assertRaisesRegex(WorkflowError, "kind does not match"):
            ctx.get_result(TaskRef("workflow-succeeded", ctx))
        with self.assertRaisesRegex(WorkflowError, "kind does not match"):
            ctx.get_result(WorkflowRef("task-succeeded", ctx))

    async def test_malformed_child_identities_and_terminal_evidence_are_rejected(self):
        invalid = [None, [], {}, child(state="waiting")]
        for kind in ("workflow", "task"):
            identity = "workflow_id" if kind == "workflow" else "task_id"
            for field, value in ((identity, ""), (identity, True), ("state", "failed"),
                                 ("outcome", None), ("unexpected", None)):
                invalid.append(dict(child(kind), **{field: value}))
            mixed = child(kind)
            mixed["task_id" if kind == "workflow" else "workflow_id"] = "other"
            invalid.append(mixed)
            bad = child(kind); bad["outcome"].pop("output"); invalid.append(bad)
            bad = child(kind, "cancelled"); bad["outcome"]["output"] = None; invalid.append(bad)
        for value in (None, "task", "unknown", 1):
            invalid.append(dict(child(), kind=value))
        bad = child(); bad.pop("kind"); invalid.append(bad)
        for field, value in (("attempt_id", ""), ("quiescence", "unknown"),
                             ("execution_may_have_started", False),
                             ("execution_may_have_started", 1)):
            bad = child("task"); bad["outcome"][field] = value; invalid.append(bad)
        for field, value in (("kind", ""), ("message", "x" * 4097), ("message", 1)):
            bad = child(state="failed"); bad["outcome"]["error"][field] = value; invalid.append(bad)
        for index, value in enumerate(invalid):
            with self.subTest(case=index), self.assertRaises(WorkflowError):
                self.context(inputs={"child": value})

    async def test_task_failure_variants_preserve_existing_wire_shapes(self):
        failures = [{"kind": "attempt_lost"}, {"kind": "application", "error": {
            "kind": "custom", "message": "application failure"}}]
        failures.extend({"kind": "execution", "phase": phase,
                         "error": {"kind": "unavailable", "message": "failure"},
                         "cleanup_error": cleanup}
                        for phase in ("admission", "preparation", "startup", "execution", "cleanup")
                        for cleanup in (None, {"kind": "io", "message": "cleanup"}))
        for failure in failures:
            value = child("task", "failed")
            value["outcome"]["failure"] = failure
            if failure["kind"] == "attempt_lost":
                value["outcome"]["quiescence"] = "unconfirmed"
            self.assertEqual(self.context(inputs={"task": value}).inputs["task"], value)
        invalid_evidence = child("task", "failed")
        invalid_evidence["outcome"]["execution_may_have_started"] = False
        with self.assertRaises(WorkflowError): self.context(inputs={"task": invalid_evidence})
        invalid_evidence = child("task", "failed")
        invalid_evidence["outcome"]["failure"] = {"kind": "attempt_lost"}
        with self.assertRaises(WorkflowError): self.context(inputs={"task": invalid_evidence})
        for failure in ({"kind": "execution", "phase": "unknown", "error": {}, "cleanup_error": None},
                        {"kind": "unknown"}, {"kind": "attempt_lost", "extra": 1}):
            value = child("task", "failed"); value["outcome"]["failure"] = failure
            with self.assertRaises(WorkflowError): self.context(inputs={"task": value})

    async def test_lineage_is_paired_bounded_and_root_compatible(self):
        for metadata in ({}, {"parent_workflow_id": None, "root_workflow_id": None}):
            ctx = self.context(**metadata)
            self.assertIsNone(ctx.parent_workflow_id)
            self.assertEqual(ctx.root_workflow_id, ctx.workflow_id)
        for parent, root in (("root", "root"), ("parent", "root")):
            ctx = self.context(parent_workflow_id=parent, root_workflow_id=root)
            self.assertEqual((ctx.parent_workflow_id, ctx.root_workflow_id), (parent, root))
        for parent, root in (("parent", None), (None, "root"), ("workflow-1", "root"),
                             ("parent", "workflow-1"), ("", "root"), ("parent", "x" * 129),
                             (False, "root"), ("parent", "bad\nroot")):
            with self.subTest(parent=parent, root=root), self.assertRaises(WorkflowError):
                self.context(parent_workflow_id=parent, root_workflow_id=root)

    async def test_subworkflow_input_boundary_accepts_rust_float_expansion(self):
        inputs = test_workflow_inbound.full_array(
            lambda value: {"flow": child(output=value)}, MAX_DECISION_BYTES)
        with self.assertRaises(WorkflowError):
            _encode(inputs, MAX_DECISION_BYTES, 96)
        ctx = self.context(inputs=inputs)
        self.assertEqual(ctx.get_result("flow"), inputs["flow"]["outcome"]["output"])
        with self.assertRaises(WorkflowError): ctx.complete(ctx.get_result("flow"))
        with self.assertRaises(WorkflowError):
            self.context(inputs={str(i): child("workflow" if i % 2 else "task")
                                 for i in range(MAX_COMMANDS + 1)})


class SubworkflowProtocolTests(unittest.TestCase):
    launch = test_workflow.WorkflowProtocolTests.launch
    invocation = test_workflow.WorkflowProtocolTests.invocation
    ordinary = test_workflow.WorkflowProtocolTests.ordinary

    def test_nested_root_and_plain_invocations_clear_lineage_and_log_fields(self):
        nested = self.invocation(activation=test_workflow.payload(
            parent_workflow_id="parent", root_workflow_id="root"))
        nested["event"].update(ldgparentworkflowid="parent", ldgrootworkflowid="root")
        root, plain = self.invocation("2"), self.invocation("3")
        self.ordinary(plain)
        source = """import os, time
from ledgence_worker import current_invocation, get_logger
from ledgence_worker.workflow import workflow_context
async def handle(event):
    invocation = current_invocation()
    result = {"parent": invocation.parent_workflow_id, "root": invocation.root_workflow_id,
              "pid": os.getpid()}
    get_logger("owned").info(event["id"])
    time.sleep(.02)
    if invocation.activation_id is None: return result
    ctx = workflow_context()
    assert ctx.parent_workflow_id == invocation.parent_workflow_id
    assert ctx.root_workflow_id == invocation.root_workflow_id
    return ctx.complete(result)
"""
        process, frames = self.launch(source, [nested, root, plain,
                                              {"v": 3, "type": "shutdown"}], protocol=3)
        self.assertEqual(process.returncode, 0, process.stderr)
        results = [f["output"] for f in frames if f["type"] == "result"]
        values = [results[0]["output"], results[1]["output"], results[2]]
        self.assertEqual([(v["parent"], v["root"]) for v in values],
                         [("parent", "root"), (None, "workflow-1"), (None, None)])
        self.assertEqual(len({v["pid"] for v in values}), 1)
        logs = {f["message"]: f["invocation"] for f in frames if f["type"] == "log"}
        self.assertEqual(logs["evt-1"]["parent_workflow_id"], "parent")
        self.assertEqual(logs["evt-1"]["root_workflow_id"], "root")
        for event in ("evt-2", "evt-3"):
            self.assertNotIn("parent_workflow_id", logs[event])
            self.assertNotIn("root_workflow_id", logs[event])

    def test_nested_context_must_match_both_event_ancestors(self):
        source = "def handle(event): raise AssertionError('controller ran')"
        for field in ("ldgparentworkflowid", "ldgrootworkflowid"):
            invocation = self.invocation(activation=test_workflow.payload(
                parent_workflow_id="parent", root_workflow_id="root"))
            invocation["event"].update(ldgparentworkflowid="parent", ldgrootworkflowid="root")
            invocation["event"][field] = "different"
            process, frames = self.launch(source, [invocation], protocol=3)
            self.assertEqual(process.returncode, 0, process.stderr)
            self.assertEqual(frames[-1]["status"], "runtime_error")
            self.assertIn("does not match", frames[-1]["error"]["message"])
            self.assertNotIn(b"controller ran", process.stderr)

    def test_local_step_cannot_hide_subworkflow_command_on_replay(self):
        source = """from ledgence_worker.workflow import workflow_context
async def hidden():
    workflow_context().workflow("hidden", program="child", version="1", queue="q", data=None)
async def handle(event):
    ctx = workflow_context()
    await ctx.local("local", hidden)
    return ctx.complete(None)
"""
        process, frames = self.launch(source, [self.invocation()], protocol=3)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertFalse(any(f["type"] == "runtime_request" for f in frames))
        self.assertEqual(frames[-1]["status"], "runtime_error")
        self.assertIn("use the controller", frames[-1]["error"]["message"])
