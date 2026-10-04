"""Tests of evidence grounding, turn bounds and optional Codex parsing (MIT)."""
import asyncio
import copy
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fulfillment.agent import MAX_TURNS, TOPICS, TOOLS, WORDING, model_turn, verify_answer
from fulfillment.codex import CodexError, parse_events

SNAPSHOT = "a" * 64


def records(delay=False):
    metrics = {
        "carrier": {"baseline_orders": 40, "current_orders": 40, "baseline_late": 4, "current_late": 16 if delay else 4},
        "warehouse": {"missing_dispatches": 0},
        "source_health": {"missing_sources": [], "missing_dispatches": 0, "corrected_sources": [] if delay else ["warehouse"]},
        "support": {"current_tickets": 20 if delay else 8},
    }
    return [{"topic": topic, "snapshot_id": SNAPSHOT, "evidence_id": f"{topic}:{SNAPSHOT}", "metrics": metrics[topic],
             "claims": [{"id": f"{topic}.verified", "text": f"Verified {topic} fact."}]} for topic in TOPICS]


async def answer_for(evidence):
    return await model_turn(mode="fixture", model="unused", snapshot_id=SNAPSHOT,
                            messages=[{"role": "tool", "content": item} for item in evidence], tools=TOOLS)


class AgentTests(unittest.IsolatedAsyncioTestCase):
    async def test_scripted_agent_reads_each_topic_then_derives_both_answers(self):
        for delay in (False, True):
            evidence = {item["topic"]: item for item in records(delay)}
            messages = []
            for _ in range(4):
                response = await model_turn(mode="fixture", model="unused", snapshot_id=SNAPSHOT, messages=messages, tools=TOOLS)
                self.assertEqual(response["kind"], "tool")
                self.assertTrue(response["execution"]["simulated"])
                messages.extend([{"role": "assistant", "content": response}, {"role": "tool", "content": evidence[response["arguments"]["topic"]]}])
            answer = await model_turn(mode="fixture", model="unused", snapshot_id=SNAPSHOT, messages=messages, tools=TOOLS)
            self.assertEqual(answer["conclusion"], "delivery_delay" if delay else "data_gap")
            self.assertEqual(verify_answer(answer, snapshot_id=SNAPSHOT, evidence=evidence), answer)

    async def test_fabricated_evidence_claim_or_snapshot_rejected(self):
        evidence = records()
        answer = await answer_for(evidence)
        for field, value in (("evidence_ids", ["invented"]), ("claim_ids", ["carrier.invented"]), ("snapshot_id", "b" * 64)):
            changed = copy.deepcopy(answer)
            changed[field] = value
            with self.assertRaises(ValueError):
                verify_answer(changed, snapshot_id=SNAPSHOT, evidence=evidence)

    async def test_generated_numerical_claims_are_not_accepted(self):
        evidence = records()
        answer = await answer_for(evidence)
        answer["summary"] = "The late-delivery rate is 99 percent."
        with self.assertRaisesRegex(ValueError, "verified conclusion wording"):
            verify_answer(answer, snapshot_id=SNAPSHOT, evidence=evidence)

    async def test_conclusion_must_follow_complete_metrics(self):
        evidence = records()
        answer = await answer_for(evidence)
        answer.update(conclusion="delivery_delay", **WORDING["delivery_delay"])
        with self.assertRaisesRegex(ValueError, "contradicts"):
            verify_answer(answer, snapshot_id=SNAPSHOT, evidence=evidence)
        evidence[2]["metrics"]["missing_dispatches"] = 12
        with self.assertRaisesRegex(ValueError, "incomplete"):
            await answer_for(evidence)

    async def test_corrected_source_is_not_proof_metrics_returned_to_baseline(self):
        evidence = records()
        evidence[0]["metrics"]["current_late"] = 5
        with self.assertRaisesRegex(ValueError, "does not support"):
            await answer_for(evidence)

    async def test_wrong_tool_snapshot_and_turn_budget(self):
        evidence = records()
        evidence[0]["snapshot_id"] = "b" * 64
        with self.assertRaises(ValueError):
            await answer_for(evidence)
        with self.assertRaisesRegex(ValueError, "budget"):
            await model_turn(mode="fixture", model="unused", snapshot_id=SNAPSHOT,
                             messages=[{"role": "assistant", "content": {}}] * MAX_TURNS, tools=TOOLS)

    async def test_live_adapter_output_rejected_before_tool_can_run(self):
        fake = {"output": {"response": {"kind": "tool", "call_id": "1", "name": "shell", "arguments": {"topic": "carrier"}}}, "execution": {}}
        with patch("fulfillment.codex.run_codex", return_value=fake):
            with self.assertRaisesRegex(ValueError, "Unsupported agent tool"):
                await model_turn(mode="codex", model="test-model", snapshot_id=SNAPSHOT, messages=[], tools=TOOLS)


class CodexParsingTests(unittest.TestCase):
    def stream(self, output, extra=None):
        events = [{"type": "thread.started", "thread_id": "thread-1"}, {"type": "turn.started"}]
        if extra:
            events.append(extra)
        events.extend([{"type": "item.completed", "item": {"type": "agent_message", "text": json.dumps(output)}},
                       {"type": "turn.completed", "usage": {"input_tokens": 30, "cached_input_tokens": 0, "output_tokens": 5}}])
        return "\n".join(json.dumps(item) for item in events).encode()

    def test_accepts_complete_structured_turn(self):
        output, thread, usage = parse_events(self.stream({"response": {"kind": "tool"}}))
        self.assertEqual(output["response"]["kind"], "tool")
        self.assertEqual(thread, "thread-1")
        self.assertEqual(usage["input_tokens"], 30)

    def test_rejects_cli_tools_and_incomplete_duplicate_or_nonfinite_json(self):
        command = {"type": "item.completed", "item": {"type": "command_execution", "command": "echo unexpected"}}
        for raw in (self.stream({}, command), b'{"type":"turn.started"}', b'{"type":"thread.started","type":"turn.completed"}', self.stream({}) + b'\n{"type":"error"}'):
            with self.assertRaises(CodexError):
                parse_events(raw)
        raw = self.stream({}).replace(b'"input_tokens": 30', b'"input_tokens": NaN')
        with self.assertRaises(CodexError):
            parse_events(raw)


if __name__ == "__main__":
    unittest.main()
