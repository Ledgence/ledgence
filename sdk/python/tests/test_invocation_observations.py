"""Optional lifecycle measurements do not control execution or replay (MIT)."""
import asyncio
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

import test_bootstrap
from test_workflow import payload

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ledgence.worker._observations import Observations, _current
from ledgence.worker.workflow import WorkflowContext


class RuntimeObservations(unittest.TestCase):
    launch = test_bootstrap.BootstrapTests.launch
    invocation = test_bootstrap.BootstrapTests.invocation
    def test_optional_measurements_follow_each_warm_invocation(self):
        messages = [dict(self.invocation(), observe=True),
                    dict(self.invocation("evt-2", "attempt-2"), observe=True),
                    self.invocation("evt-3", "attempt-3"), {"v": 1, "type": "shutdown"}]
        result, frames = self.launch("def handle(event):\n    return sum(range(10000))\n", messages)
        self.assertEqual(result.returncode, 0, result.stderr)
        for frame in frames[1:3]:
            self.assertEqual(frame["status"], "success")
            observations = frame["observations"]
            self.assertGreater(observations["runtime_started_at_ms"], 0)
            self.assertGreater(observations["runtime_elapsed_us"], 0)
            self.assertEqual(observations["local_steps"], [])
            self.assertFalse(observations["local_steps_truncated"])
            self.assertGreaterEqual(observations["process_cpu_user_us"], 0)
        self.assertNotIn("observations", frames[3])

    def test_measurements_never_displace_near_limit_output(self):
        message = dict(self.invocation(), observe=True)
        result, frames = self.launch("def handle(event):\n    return 'x'*300\n",
                                     [message], output_limit=450)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(frames[1]["output"], "x" * 300)
        self.assertNotIn("observations", frames[1])


class LocalObservations(unittest.IsolatedAsyncioTestCase):
    async def test_overlap_failure_and_replay_preserve_local_authority(self):
        records = []
        async def commit(_, value):
            records.append(value)
            return {"committed": True}
        async def succeeds(value):
            await asyncio.sleep(.005)
            return value
        async def fails():
            await asyncio.sleep(0)
            raise ValueError("private error detail")
        observations = Observations()
        token = _current.set(observations)
        context = WorkflowContext(payload(), commit)
        try:
            with self.assertRaises(ValueError):
                await context.gather(context.local("ok", succeeds, value=42), context.local("bad", fails))
            await context._finish(cancel=True)
            by_key = {row["key"]: row for row in observations.snapshot()["local_steps"]}
            self.assertEqual(by_key["ok"]["state"], "returned")
            self.assertEqual(by_key["bad"]["state"], "failed")
            self.assertEqual([row["key"] for row in records], ["ok"])
            self.assertNotIn("private", json.dumps(observations.snapshot()))
        finally:
            _current.reset(token)
        replay = Observations()
        token = _current.set(replay)
        context = WorkflowContext(payload(local_steps=records), commit)
        try:
            self.assertEqual(await context.local("ok", succeeds, value=42), 42)
            self.assertEqual(await context.local("ok", succeeds, value=42), 42)
            await context._finish()
            self.assertEqual(len(records), 1)
            self.assertEqual(len(replay.locals), 1)
            self.assertEqual(replay.locals[0]["state"], "replayed")
        finally:
            _current.reset(token)

    async def test_measurement_failure_does_not_fail_local_result(self):
        async def commit(_, value):
            return {"committed": True}
        observation = Observations()
        token = _current.set(observation)
        context = WorkflowContext(payload(), commit)
        try:
            with patch.object(observation, "begin", side_effect=RuntimeError("sink unavailable")):
                self.assertEqual(await context.local("ok", lambda: 42), 42)
            self.assertTrue(observation.truncated)
            await context._finish()
        finally:
            _current.reset(token)

    async def test_cancelled_local_is_recorded_without_accepted_result(self):
        async def commit(_, value):
            self.fail("cancelled step cannot commit")
        started = asyncio.Event()
        async def waits():
            started.set()
            await asyncio.Event().wait()
        observation = Observations()
        token = _current.set(observation)
        context = WorkflowContext(payload(), commit)
        try:
            context.local("wait", waits)
            await started.wait()
            await context._finish(cancel=True)
            self.assertEqual(observation.locals[0]["state"], "cancelled")
        finally:
            _current.reset(token)

    def test_buffer_is_bounded_and_unavailable_is_not_zero(self):
        with patch("ledgence.worker._observations._usage", return_value=(None, None, None)):
            observation = Observations()
            for n in range(200):
                observation.begin({"key": str(n), "callable": "module:function"})
            value = observation.snapshot()
            self.assertEqual(len(value["local_steps"]), 128)
            self.assertTrue(value["local_steps_truncated"])
            self.assertIsNone(value["process_cpu_user_us"])
            self.assertIsNone(value["process_lifetime_peak_rss_bytes"])


if __name__ == "__main__":
    unittest.main()
