"""Process death and uncertain per-call acknowledgment without a model provider (MIT)."""
import asyncio
import json
import os
import signal

from http_acceptance.harness import eventually
from .owned_scenarios import quote


SCENARIOS = ("agent-crash-model", "agent-crash-tool", "agent-lost-ack", "agent-effect-gap", "agent-binding", "agent-example")
MODEL_KEY = "turn:0:model"
TOOL_KEY = "turn:0:tool:lookup:0"


def journal(d, activation):
    return json.loads(d.sql("SELECT coalesce(json_agg(convert_from(record_bytes,'UTF8')::json "
                           "ORDER BY step_key),'[]'::json) FROM workflow_local_results "
                           "WHERE activation_id=" + quote(activation)))


async def run(d, names, record, records):
    from ledgence.client import AsyncClient, RetryPolicy
    options = dict(tenant=d.scope["tenant_id"], namespace=d.scope["namespace"])
    faults = {"agent-crash-model": "model", "agent-crash-tool": "tool",
              "agent-lost-ack": "none", "agent-effect-gap": "effect", "agent-binding": "binding"}
    phases = {"model": "agent_tool_unfinished", "tool": "agent_tool_ack",
              "effect": "agent_effect_before_commit"}
    for tag in faults:
        if tag not in names:
            continue
        fault = faults[tag]
        gate = d.directory / (tag + "-release")
        d.gates.add(gate)
        proxy = d.proxy() if tag == "agent-lost-ack" else None
        lost = proxy.lose_once("/v1/workflows/local-results",
                               lambda body: body["record"]["key"] == MODEL_KEY) if proxy else None
        server = proxy.url if proxy else d.server_url
        worker = d.start_worker(server=server, concurrency=1)
        async with AsyncClient(server, **options) as client:
            handle = await client.workflows.submit(program="agent-recovery-controller", version="1.0.0",
                queue=d.queue, data={"tag": tag, "marker": str(d.directory / "workflow-markers.jsonl"),
                    "fault": fault, "gate": str(gate), "receipt": str(d.directory / (tag + "-provider.json"))},
                idempotency_key=tag, retry_policy=RetryPolicy(max_attempts=2, retry_delay_ms=0))
            before_death = None
            if fault in phases:
                phase = await asyncio.to_thread(eventually, lambda: records(d, tag, phases[fault]),
                    timeout=max(30, getattr(d, "terminal_timeout_floor", 30)),
                    description="agent phase ready for owned process death")
                first = records(d, tag, "agent_activation")[0]
                assert first["pid"] == phase[0]["pid"], phase
                before_death = await asyncio.to_thread(journal, d, first["activation"])
                expected = {MODEL_KEY, TOOL_KEY} if fault == "tool" else {MODEL_KEY}
                assert {row["key"] for row in before_death} == expected, before_death
                assert len(records(d, tag, "agent_model_called")) == 1
                # The PID is from this unique run's fixture handshake. Kill the
                # Python runtime, leaving its worker to detect death, clean up,
                # report failure and acquire a new attempt under normal policy.
                os.kill(first["pid"], signal.SIGKILL)
                gate.touch()
            if fault == "binding":
                outcome = await handle.wait(timeout=130)
                assert outcome.workflow.state.value == "failed", outcome
                assert len(records(d, tag, "agent_model_called")) == 1
                assert not records(d, tag, "agent_tool_called")
                assert not records(d, tag, "agent_finish")
                output = None
            else:
                output = await handle.result(timeout=130)
            attempts = records(d, tag, "agent_activation")
            expected_attempts = 1 if proxy else 2
            assert [row["attempt"] for row in attempts] == list(range(1, expected_attempts + 1)), attempts
            assert len({row["activation"] for row in attempts}) == 1, attempts
            assert len({row["attempt_id"] for row in attempts}) == expected_attempts, attempts
            if fault in phases:
                assert len({row["pid"] for row in attempts}) == 2, attempts
            accepted = await asyncio.to_thread(journal, d, attempts[0]["activation"])
            assert len(accepted) == (1 if fault == "binding" else 2), accepted
            assert {row["input"]["kind"] for row in accepted} == (
                {"model"} if fault == "binding" else {"model", "tool"}), accepted
            model_calls = records(d, tag, "agent_model_called")
            tool_calls = records(d, tag, "agent_tool_called")
            effects = records(d, tag, "agent_effect_created")
            assert len(model_calls) == 1, model_calls
            last = await asyncio.to_thread(d.attempt, attempts[-1]["activation"], attempts[-1]["attempt_id"])
            report = last["settlement"]["command"]["report"]["report"]
            observed = {row["key"]: row["state"] for row in report["observations"]["local_steps"]}
            if fault == "binding":
                assert "different binding" in json.dumps(last["settlement"]), last
                assert observed == {}, observed
            else:
                assert len(tool_calls) == (2 if fault in ("model", "effect") else 1), tool_calls
                assert len(effects) == 1, effects
                assert output["model"]["token"] == output["tool"]["model_token"] == model_calls[0]["token"], output
                assert output["tool"] == effects[0]["receipt"], (output, effects)
                assert len(records(d, tag, "agent_finish")) == 1
                assert records(d, tag, "agent_finish")[0]["activation"] != attempts[0]["activation"]
                acknowledgments = records(d, tag, "agent_model_ack")
                assert len(acknowledgments) == expected_attempts and {
                    row["token"] for row in acknowledgments} == {model_calls[0]["token"]}, acknowledgments
                assert observed == {MODEL_KEY: "returned" if proxy else "replayed",
                                    TOOL_KEY: "replayed" if fault == "tool" else "returned"}, observed
            if fault == "effect":
                reconciled = records(d, tag, "agent_effect_reconciled")
                assert len(reconciled) == 1 and reconciled[0]["receipt"] == effects[0]["receipt"]
                assert len({row["idempotency_key"] for row in tool_calls}) == 1
            commit_requests = None
            if proxy:
                calls = proxy.commands("/v1/workflows/local-results", lambda body: body["record"]["key"] == MODEL_KEY)
                assert lost.is_set() and len(calls) >= 2, calls
                assert all(row["body"] == calls[0]["body"] and row["status"] == 200 for row in calls), calls
                assert json.loads(calls[0]["response"])["already_accepted"] is False
                assert any(json.loads(row["response"])["already_accepted"] is True for row in calls[1:])
                commit_requests = len(calls)
            record(tag, dict(workflow_id=handle.id, concurrency=1, attempts=attempts,
                model_calls=len(model_calls), tool_calls=len(tool_calls), synthetic_effects=len(effects),
                journal_keys=[row["key"] for row in accepted],
                journal_before_death=None if before_death is None else [row["key"] for row in before_death],
                model_commit_requests=commit_requests, last_attempt_observations=observed, output=output))
        await asyncio.to_thread(worker.stop)
    if "agent-example" in names:
        worker = d.start_worker(concurrency=1)
        async with AsyncClient(d.server_url, **options) as client:
            handle = await client.workflows.submit(program="agent-recovery", version="1.0.0", queue=d.queue,
                data={"order_id": "ORD-1042"}, idempotency_key="agent-example")
            output = await handle.result(timeout=130)
            assert output == {"answer": "Order ORD-1042 is shipped; estimated delivery in 2 days.",
                              "turns": 2, "model": "scripted-order-assistant-v1", "simulated": True}, output
            activations = json.loads(await asyncio.to_thread(d.sql,
                "SELECT json_agg(json_build_object('activation',activation_id,'revision',revision::text,"
                "'context',convert_from(context_bytes,'UTF8')::json) ORDER BY revision) "
                "FROM workflow_activations WHERE workflow_id=" + quote(handle.id)))
            assert len(activations) == 2 and activations[0]["context"]["state"] is None, activations
            saved = activations[1]["context"]["state"]
            assert saved["round"] == 1 and len(saved["messages"]) == 3, saved
            assert saved["messages"][-1] == {"role": "tool", "call_id": "lookup:0", "result": {
                "order_id": "ORD-1042", "region": "us-east", "status": "shipped", "delivery_days": 2}}, saved
            first = await asyncio.to_thread(journal, d, activations[0]["activation"])
            second = await asyncio.to_thread(journal, d, activations[1]["activation"])
            assert [row["key"] for row in first] == ["model:turn:0", "tool:turn:0:lookup:0"], first
            assert [row["key"] for row in second] == ["model:turn:1"], second
            assert first[0]["input"]["arguments"]["temperature"] == 0
            assert first[1]["input"]["arguments"]["region"] == "us-east"
            assert second[0]["input"]["arguments"]["messages"] == saved["messages"]
            record("agent-example", dict(workflow_id=handle.id, concurrency=1, activation_count=2,
                journal_keys=[[row["key"] for row in rows] for rows in (first, second)],
                checkpoint_round=saved["round"], output=output, source="examples/agent-recovery/program.py"))
        await asyncio.to_thread(worker.stop)
