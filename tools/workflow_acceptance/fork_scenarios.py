"""Fork registration, local overlap, restart and reconciliation acceptance."""
import asyncio
import contextlib
import json
import os
import signal
import urllib.parse

from http_acceptance.harness import eventually, exchange
from .owned_scenarios import quote, send, tree_rows


SCENARIOS = ("fork-example", "fork-mixed", "fork-lost-ack", "fork-crash", "fork-failure", "fork-cancellation")


def branch_ids(d, workflow_id):
    query = ("SELECT coalesce(json_object_agg(command_key,child_workflow_id),'{}'::json) "
             "FROM owned_workflow_links WHERE parent_workflow_id=" + quote(workflow_id))
    return json.loads(d.sql(query))


async def run(d, names, record, records, snapshot):
    from ledgence.client import AsyncClient, RetryPolicy
    options = dict(tenant=d.scope["tenant_id"], namespace=d.scope["namespace"])

    async def submit(client, tag, **extra):
        return await client.workflows.submit(
            program="fork-controller", version="1.0.0", queue=d.queue,
            data=dict(tag=tag, marker=str(d.directory / "workflow-markers.jsonl"), queue=d.queue, **extra),
            idempotency_key=tag, retry_policy=RetryPolicy(max_attempts=3, retry_delay_ms=0))

    async def wait_root(workflow_id):
        await asyncio.to_thread(eventually, lambda: snapshot(d, workflow_id)["state"] == "waiting",
                                timeout=110, description="parent durable join")

    def tree_in_state(workflow_id, state):
        rows = tree_rows(d, workflow_id)
        return rows if len(rows) == 3 and all(row["state"] == state for row in rows) else None

    async def release(workflow_id, tag):
        children = await asyncio.to_thread(branch_ids, d, workflow_id)
        assert set(children) == {"left", "right"}, children
        for side, child in children.items():
            receipt = await send(d, child, "release", f"evt-{tag}-{side}", approved=True)
            duplicate = await send(d, child, "release", f"evt-{tag}-{side}", approved=True)
            assert not receipt["already_accepted"] and duplicate["already_accepted"]
        return children

    async def verify(handle, tag, failed=False):
        result = await handle.result(timeout=130)
        expected = {"left": "succeeded", "right": "failed" if failed else "succeeded"}
        assert result["states"] == expected and result["local"]["value"] == 11, result
        assert set(result["values"]) == ({"left"} if failed else {"left", "right"}), result
        assert len(records(d, tag, "fork_join")) == 1
        assert len(records(d, tag, "fork_local_started")) == 1
        rows = await asyncio.to_thread(tree_rows, d, handle.id)
        assert len(rows) == 3, rows
        assert len({json.dumps(row["controller"], sort_keys=True) for row in rows}) == 1, rows
        return result

    if "fork-example" in names:
        worker = d.start_worker(concurrency=1)
        async with AsyncClient(d.server_url, **options) as client:
            handle = await client.workflows.submit(
                program="mixed-workflow", version="1.0.0", queue=d.queue,
                data={"values": [1, 2, 3], "queue": d.queue}, idempotency_key="fork-example")
            result = await handle.result(timeout=110)
            assert result == {"local": {"count": 3, "sum": 6}, "double": 12, "triple": 18}, result
            record("fork-example", dict(workflow_id=handle.id, concurrency=1, output=result))
        await asyncio.to_thread(worker.stop)


    if "fork-mixed" in names:
        tag = "fork-mixed"
        worker = d.start_worker(concurrency=3)
        async with AsyncClient(d.server_url, **options) as client:
            handle = await submit(client, tag, wait_remote=True, require_overlap=True)
            await wait_root(handle.id)
            parents = records(d, tag, "fork_parent_started")
            starts = records(d, tag, "fork_remote_started")
            local = records(d, tag, "fork_local_finished")
            assert len(parents) == 1 and len(starts) == 2 and len(local) == 1
            assert records(d, tag, "fork_local_started")[0]["at"] < min(row["at"] for row in starts)
            assert max(row["at"] for row in starts) < local[0]["at"]
            assert local[0]["pid"] == parents[0]["pid"] == records(d, tag, "fork_ack")[0]["pid"]
            # Every workflow is suspended, including both independently resumable branches.
            await asyncio.to_thread(eventually, lambda: tree_in_state(handle.id, "waiting"),
                                    timeout=110, description="all fork branches durably waiting")
            await asyncio.to_thread(worker.stop)
            await asyncio.to_thread(d.server.stop)
            d.server, _ = await asyncio.to_thread(d.start_server)
            worker = d.start_worker(concurrency=1)
            # One slot can execute unrelated work while parent and branches wait.
            probe = await client.tasks.submit(program="invoice", version="1.0.0", queue=d.queue,
                                               data={"mode": "normal", "marker": str(d.directory / "probe.jsonl")},
                                               idempotency_key=tag + ":probe")
            await probe.result(timeout=110)
            assert (await asyncio.to_thread(snapshot, d, handle.id))["state"] == "waiting"
            children = await release(handle.id, tag)
            result = await verify(handle, tag)
            record(tag, dict(workflow_id=handle.id, branches=children, overlap=True,
                             original_parent_pid=parents[0]["pid"], restart=True,
                             resumed_concurrency=1, duplicate_events=True, output=result))
        await asyncio.to_thread(worker.stop)

    if "fork-lost-ack" in names:
        tag = "fork-lost-ack"
        proxy = d.proxy()
        lost = proxy.lose_once("/v1/workflows/forks")
        worker = d.start_worker(server=proxy.url, concurrency=1)
        async with AsyncClient(proxy.url, **options) as client:
            handle = await submit(client, tag)
            result = await verify(handle, tag)
            calls = proxy.commands("/v1/workflows/forks")
            assert lost.is_set() and len(calls) >= 2
            assert all(call["body"] == calls[0]["body"] for call in calls)
            receipt = {"key": "analysis", "branch_keys": ["left", "right"], "already_accepted": False}
            assert calls[0]["status"] == 200 and json.loads(calls[0]["response"]) == receipt, calls
            receipt["already_accepted"] = True
            assert any(call["status"] == 200 and json.loads(call["response"]) == receipt
                       for call in calls[1:]), calls
            assert len(records(d, tag, "fork_parent_started")) == 1
            assert len(records(d, tag, "fork_remote_started")) == 2
            record(tag, dict(workflow_id=handle.id, fork_requests=len(calls),
                             branch_count=2, unchanged_replay=True, output=result))
        await asyncio.to_thread(worker.stop)

    if "fork-crash" in names:
        tag = "fork-crash"
        gate = d.directory / "fork-crash-gate"
        d.gates.add(gate)
        worker = d.start_worker(concurrency=1)
        async with AsyncClient(d.server_url, **options) as client:
            handle = await submit(client, tag, crash_gate=str(gate))
            acknowledged = await asyncio.to_thread(
                eventually, lambda: records(d, tag, "fork_local_ack"),
                timeout=max(30, getattr(d, "terminal_timeout_floor", 30)),
                description="fork and local result acknowledged before crash")
            await asyncio.to_thread(worker.kill)
            # PID came from this unique fixture's acknowledged local invocation.
            with contextlib.suppress(ProcessLookupError):
                os.kill(acknowledged[0]["pid"], signal.SIGKILL)
            worker = d.start_worker(concurrency=3)
            result = await verify(handle, tag)
            starts = records(d, tag, "fork_parent_started")
            assert len(starts) == 2 and [row["attempt"] for row in starts] == [1, 2], starts
            assert len({row["activation"] for row in starts}) == 1
            assert len(records(d, tag, "fork_remote_started")) == 2
            record(tag, dict(workflow_id=handle.id, activation_attempts=2,
                             branch_count=2, local_execution_count=1, output=result))
        await asyncio.to_thread(worker.stop)

    if "fork-failure" in names:
        tag = "fork-failure"
        worker = d.start_worker(concurrency=1)
        async with AsyncClient(d.server_url, **options) as client:
            handle = await submit(client, tag, fail_right=True)
            result = await verify(handle, tag, failed=True)
            record(tag, dict(workflow_id=handle.id, failure_observed_by_join=True, output=result))
        await asyncio.to_thread(worker.stop)

    if "fork-cancellation" in names:
        tag = "fork-cancellation"
        worker = d.start_worker(concurrency=3)
        async with AsyncClient(d.server_url, **options) as client:
            handle = await submit(client, tag, wait_remote=True, require_overlap=True)
            await wait_root(handle.id)
            status, _, reply = await asyncio.to_thread(exchange, d.server_url, "POST", "/v1/workflows/cancel",
                                                       dict(scope=d.scope, workflow_id=handle.id))
            assert status == 200, reply
            rows = await asyncio.to_thread(eventually, lambda: tree_in_state(handle.id, "cancelled"),
                timeout=110, description="cancelled fork and owned branches")
            assert len(rows) == 3 and not records(d, tag, "fork_join"), rows
            record(tag, dict(workflow_id=handle.id, cancelled_workflows=3, join_scheduled=False))
        await asyncio.to_thread(worker.stop)


def verify_traces(d, capture, results, records, trace_rows):
    """Prove the interactive fork preserves the actual spawning process span."""
    detail = next(row["detail"] for row in results if row["scenario"] == "fork-mixed")
    parent = records(d, "fork-mixed", "fork_parent_started")[0]
    spans = trace_rows(capture)

    def attempt(marker):
        attempt_id = d.sql("SELECT attempt_id FROM attempts WHERE task_id=" + quote(marker["activation"]))
        query = urllib.parse.urlencode(dict(d.scope, task_id=marker["activation"], attempt_id=attempt_id))
        status, _, value = exchange(d.server_url, "GET", "/v1/attempts/inspect?" + query)
        assert status == 200, value
        return attempt_id, value

    parent_attempt, parent_value = attempt(parent)
    origin = parent_value["settlement"]["command"]["processing_trace"]
    trace = origin["traceparent"].split("-")
    parents = [span for span in spans if span["name"] == "ledgence.attempt.process"
               and span["attributes"].get("ledgence.attempt.id") == parent_attempt]
    assert len(parents) == 1 and (parents[0]["trace_id"], parents[0]["span_id"]) == (trace[1], trace[2])
    inspected = []
    for marker in records(d, "fork-mixed", "fork_remote_started"):
        branch_origin = json.loads(d.sql("SELECT convert_from(submission_bytes,'UTF8')::json->'origin_trace' "
                                        "FROM workflow_runs WHERE workflow_id=" + quote(marker["workflow"])))
        assert branch_origin == origin, branch_origin
        attempt_id, value = attempt(marker)
        event = value["event"]
        assert event["ldgparentworkflowid"] == event["ldgrootworkflowid"] == detail["workflow_id"]
        propagated = event["traceparent"].split("-")
        producer = [span for span in spans if span["trace_id"] == propagated[1]
                    and span["span_id"] == propagated[2]]
        process = [span for span in spans if span["name"] == "ledgence.attempt.process"
                   and span["attributes"].get("ledgence.attempt.id") == attempt_id]
        assert len(producer) == len(process) == 1, (producer, process)
        assert (producer[0]["trace_id"], producer[0]["parent_span_id"]) == (trace[1], trace[2])
        assert (process[0]["trace_id"], process[0]["parent_span_id"]) == (propagated[1], propagated[2])
        assert process[0]["attributes"]["ledgence.workflow.parent.id"] == detail["workflow_id"]
        assert process[0]["attributes"]["ledgence.workflow.root.id"] == detail["workflow_id"]
        inspected.append(dict(workflow_id=marker["workflow"], processing_span=process[0]["span_id"],
                              producer_span=producer[0]["span_id"]))
    assert len(inspected) == 2, inspected
    return dict(workflow_id=detail["workflow_id"], parent_processing_span=trace[2], branches=inspected)
