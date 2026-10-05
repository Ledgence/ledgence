"""Run the real SDK decisions with simulated transport; no database or model (MIT)."""
import copy
from functools import wraps
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parents[1]
ROOT = HERE.parents[1]
sys.path[:0] = [str(HERE), str(ROOT / "sdk/python")]
from ledgence.worker.workflow import WorkflowContext, _workflow
from fulfillment.config import APPROVAL_KEY, SOURCE_WAIT
from fulfillment.data import prepare_data
from fulfillment.storage import read_json
from client import source_event

spec = importlib.util.spec_from_file_location("fulfillment_program", HERE / "program.py")
program = importlib.util.module_from_spec(spec)
spec.loader.exec_module(program)


class LostAck(RuntimeError):
    pass


class Simulator:
    """Minimal test transport, deliberately separate from the production example."""
    def __init__(self, data, *, decision="approved", source_timeout=False, failed_branch=None, lose_key=None):
        self.data = data
        self.decision = decision
        self.source_timeout = source_timeout
        self.failed_branch = failed_branch
        self.lose_key = lose_key
        self.lost = False
        self.forks, self.journals, self.waits, self.states = {}, {}, [], []
        self.serial = 0

    async def activation(self, workflow_id, data, continuation="start", *, state=None, inputs=None, wake=None, activation=None):
        self.serial += 1
        activation = activation or f"activation-{self.serial}"
        self.states.append(copy.deepcopy(state))
        records = self.journals.setdefault(activation, [])
        async def rpc(operation, request):
            if operation == "workflow.fork":
                key = (workflow_id, request["key"])
                if key in self.forks:
                    assert self.forks[key] == request, "fork binding changed on retry"
                self.forks[key] = copy.deepcopy(request)
                return {"committed": True, "key": request["key"],
                        "branch_keys": [branch["key"] for branch in request["branches"]]}
            assert operation == "local_step.commit", operation
            records.append(copy.deepcopy(request))
            if request["key"] == self.lose_key and not self.lost:
                self.lost = True
                raise LostAck("accepted record reply lost")
            return {"committed": True}
        context = WorkflowContext({"v": 1, "workflow_id": workflow_id, "activation_id": activation,
            "revision": 1 if wake and wake["kind"] == "approval" else 0, "continuation": continuation, "state": state, "inputs": inputs or {},
            "wake": wake, "local_steps": copy.deepcopy(records)}, rpc)
        token = _workflow.set(context)
        try:
            result = context._validate_decision(await program.handle({"data": data}))
            await context._finish()
            return result
        except LostAck:
            await context._finish(cancel=True)
        finally:
            _workflow.reset(token)
        return await self.activation(workflow_id, data, continuation, state=state, inputs=inputs,
                                     wake=wake, activation=activation)

    async def run(self, workflow_id="workflow-parent", data=None, entry="start"):
        data = self.data if data is None else data
        decision = await self.activation(workflow_id, data, entry)
        for _ in range(30):
            kind = decision["kind"]
            if kind in ("complete", "fail"):
                return decision
            continuation, state = decision["continuation"], decision["state"]
            inputs, wake = {}, None
            if kind == "suspend":
                specs = {branch["key"]: branch for (owner, _), request in self.forks.items()
                         if owner == workflow_id for branch in request["branches"]}
                for key in decision["until"]:
                    child_id = workflow_id + ":" + key
                    branch = specs[key]
                    if key == self.failed_branch:
                        outcome = {"kind": "failed", "error": {"kind": "fixture_failure", "message": "source failed"}}
                    else:
                        result = await self.run(child_id, branch["data"], branch["entrypoint"])
                        outcome = ({"kind": "succeeded", "output": result["output"]} if result["kind"] == "complete"
                                   else {"kind": "failed", "error": result["error"]})
                    inputs[key] = {"kind": "workflow", "workflow_id": child_id,
                                   "state": outcome["kind"], "outcome": outcome}
            elif kind == "wait":
                self.waits.append(copy.deepcopy(decision))
                wait = decision["wait"]
                if wait["kind"] == "event":
                    wake = ({"kind": "timeout", "key": SOURCE_WAIT, "deadline": 100} if self.source_timeout else
                            {"kind": "event", "key": SOURCE_WAIT, "accepted_at": 2,
                             "event": source_event(workflow_id, "warehouse:1")})
                else:
                    resumed = f"activation-{self.serial + 1}"
                    wake = {"kind": "approval", "approval": {
                        "scope": {"tenant_id": "acme", "namespace": "demo"}, "workflow_id": workflow_id,
                        "key": APPROVAL_KEY, "activation_id": decision["activation_id"], "revision": 0,
                        "action": wait["action"], "proposed_arguments": wait.get("proposed_arguments"),
                        "created_at": 1, "deadline": 100, "status": self.decision,
                        "decision": None if self.decision == "expired" else {
                            "decision_id": "review:1", "decision": "approve" if self.decision == "approved" else "reject",
                            "reviewer": "fixture", "reason": None, "decided_at": 2},
                        "resumed_activation_id": resumed}}
            else:
                assert kind == "continue", kind
            decision = await self.activation(workflow_id, data, continuation, state=state, inputs=inputs, wake=wake)
        raise AssertionError("workflow exceeded simulator budget")


class WorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = str(Path(self.directory.name).resolve())
        prepare_data(self.store)

    def runner(self, scenario="data-gap", **options):
        return Simulator({"store": self.store, "scenario": scenario}, **options)

    async def test_gap_waits_reprocesses_only_warehouse_and_publishes_consistent_evidence(self):
        run = self.runner()
        result = await run.run()
        self.assertEqual(result["kind"], "complete", result)
        output = result["output"]
        self.assertEqual(output["status"], "published")
        self.assertEqual(output["corrected_sources"], ["warehouse"])
        self.assertEqual(output["initial_quality"]["missing_dispatches"], 12)
        self.assertEqual(output["quality"]["missing_dispatches"], 0)
        self.assertEqual([wait["wait"]["kind"] for wait in run.waits], ["event", "approval"])
        branches = [branch["key"] for request in run.forks.values() for branch in request["branches"]]
        self.assertEqual(len(branches), 9)
        self.assertEqual([key for key in branches if key.endswith(":corrected")], ["source:warehouse:corrected"])
        candidate = read_json(self.store, output["candidate_ref"])
        self.assertIn("data_gap", json.dumps(candidate))
        self.assertLess(max(len(json.dumps(s).encode()) for s in run.states), 56 * 1024)
        for state in run.states:
            if state and "receipts" in state:
                self.assertLess(len(json.dumps(state)), 12 * 1024)

    async def test_real_delay_is_not_dismissed_as_bad_data(self):
        run = self.runner("real-delay")
        result = await run.run()
        self.assertEqual(result["output"]["status"], "published", result)
        self.assertEqual([wait["wait"]["kind"] for wait in run.waits], ["approval"])
        self.assertEqual(result["output"]["corrected_sources"], [])
        candidate = read_json(self.store, result["output"]["candidate_ref"])
        self.assertIn("delivery_delay", json.dumps(candidate))

    async def test_refusal_expiry_and_source_timeout_do_not_publish(self):
        for status in ("rejected", "expired"):
            run = self.runner("real-delay", decision=status)
            result = await run.run()
            self.assertEqual(result["output"]["status"], status)
            self.assertFalse(result["output"]["published"])
            self.assertNotIn("publication", result["output"])
        run = self.runner(source_timeout=True)
        result = await run.run()
        self.assertEqual(result["output"]["status"], "incomplete")
        self.assertEqual(len(run.forks), 1)

    async def test_failed_branch_cannot_reach_agent_or_approval(self):
        run = self.runner(failed_branch="source:carrier:initial")
        result = await run.run()
        self.assertEqual(result["error"]["kind"], "source_failed")
        self.assertEqual(run.waits, [])
        run = self.runner("real-delay", failed_branch="analysis:carrier")
        result = await run.run()
        self.assertEqual(result["error"]["kind"], "analysis_failed")
        self.assertEqual(run.waits, [])

    async def test_accepted_model_result_replayed_after_lost_ack(self):
        original = program.model_turn
        calls = []
        @wraps(original)
        async def counted(**arguments):
            calls.append(arguments)
            return await original(**arguments)
        run = self.runner(lose_key="model:0")
        with patch.object(program, "model_turn", counted):
            result = await run.run()
        self.assertTrue(run.lost)
        self.assertEqual(result["output"]["status"], "published")
        self.assertEqual(len(calls), 5, "four evidence turns and final answer, no repeated accepted call")

    async def test_crash_after_report_write_before_journal_reconciles_artifacts(self):
        original = program.publish_report
        receipts = []
        @wraps(original)
        def interrupted(**arguments):
            receipt = original(**arguments)
            receipts.append(receipt)
            if len(receipts) == 1:
                raise LostAck("simulated process loss after application write, before journal commit")
            return receipt
        with patch.object(program, "publish_report", interrupted):
            result = await self.runner("real-delay").run()
        self.assertEqual(result["output"]["status"], "published")
        self.assertEqual(len(receipts), 2)
        self.assertEqual(receipts[0], receipts[1], "repeated external effect reconciles the same visible artifacts")

    async def test_invalid_input_and_early_answer_fail_without_publication(self):
        run = self.runner()
        for bad in (None, {}, {"store": "relative"}, {"store": self.store, "source_timeout_ms": True}):
            result = await run.activation("wf", bad)
            self.assertEqual(result["error"]["kind"], "invalid_input")
        async def premature(**_):
            return {"kind": "answer"}
        with patch.object(program, "model_turn", premature):
            result = await self.runner("real-delay").run()
        self.assertEqual(result["error"]["kind"], "incomplete_investigation")


if __name__ == "__main__":
    unittest.main()
