#!/usr/bin/env python3
"""Exercise real MCP stdio -> HTTP -> PostgreSQL -> Python worker execution.

Requires an explicitly disposable PostgreSQL server in LEDGENCE_POSTGRES_URL,
psql, CPython >=3.11 and a built ledgence executable with the mcp feature. Owns
and removes a unique database and all subprocesses; never restarts user services.
Uses no external models, credentials, broker, or Python third-party packages.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import traceback
import uuid

from http_acceptance.harness import eventually
from mcp_acceptance.harness import HeldReplyProxy, McpDeployment, McpSession
from postgres_fixture import owned_database_url


WORKFLOW = '''from ledgence.worker.workflow import ApprovalAction, workflow_context
def handle(event):
    ctx = workflow_context()
    if ctx.continuation == "start":
        if event["data"].get("approval"):
            return ctx.request_approval("review", action=ApprovalAction(
                "fixture:review", version="1", arguments={"value": event["data"]["value"]}),
                continuation="resume", state={"saved": True}, timeout_ms=120000)
        return ctx.wait_event("resume", continuation="resume", state={"saved": True}, timeout_ms=120000)
    return ctx.complete({"input": event["data"], "checkpoint": ctx.state, "wake": ctx.wake})
'''
WRITE_TOOLS = {"ledgence_task_submit", "ledgence_task_cancel", "ledgence_workflow_submit",
               "ledgence_workflow_cancel", "ledgence_workflow_send_event"}
READ_TOOLS = {"ledgence_program_list", "ledgence_program_versions", "ledgence_program_inspect",
              "ledgence_task_list", "ledgence_task_status", "ledgence_task_result",
              "ledgence_workflow_status", "ledgence_workflow_result", "ledgence_approval_list",
              "ledgence_approval_inspect"}


def task_arguments(d, key, **data):
    return {"program": "invoice", "version": "1.0.0", "queue": d.queue, "idempotency_key": key,
            "data": {"mode": "success", "marker": str(d.marker), **data}}


def workflow_arguments(d, key, **data):
    return {"program": "mcp-workflow", "version": "1.0.0", "queue": d.queue,
            "idempotency_key": key, "data": {"value": 7, **data}}


def finished(session, kind, identity):
    def read():
        value = session.tool("ledgence_" + kind + "_result", {kind + "_id": identity})
        return value if value["outcome"] is not None else None
    value = eventually(read, timeout=60, description=f"MCP {kind} result")
    assert value["outcome"]["kind"] == "succeeded", value
    return value["outcome"]["output"]


def wait_workflow(session, identity):
    return eventually(lambda: (value if (value := session.tool("ledgence_workflow_status",
        {"workflow_id": identity}))["state"] == "waiting" else None),
        timeout=60, description="MCP workflow checkpointed event/approval wait")


def scenarios(d, record, sessions):
    def session(label, **kwargs):
        value = McpSession(d, label, **kwargs)
        sessions.append(value)
        return value
    main = session("mcp-main")
    tools = main.request("tools/list", {})["result"]["tools"]
    assert {item["name"] for item in tools} == READ_TOOLS | WRITE_TOOLS, tools
    for item in tools:
        assert item["inputSchema"]["additionalProperties"] is False, item
        assert not {"scope", "tenant_id", "namespace"} & item["inputSchema"]["properties"].keys(), item
        assert item["annotations"]["readOnlyHint"] is (item["name"] in READ_TOOLS), item
    assert "ledgence_approval_decide" not in {item["name"] for item in tools}
    programs, cursor = [], None
    for _ in range(4):
        page = main.tool("ledgence_program_list", {"limit": 1, **({"cursor": cursor} if cursor else {})})
        programs.extend(row["program"]["program_id"] for row in page["items"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert cursor is None and programs == ["invoice", "mcp-workflow"], programs
    versions = main.tool("ledgence_program_versions", {"program": "invoice"})
    assert [item["descriptor"]["program"] for item in versions["items"]] == [{"id": "invoice", "version": "1.0.0"}]
    detail = main.tool("ledgence_program_inspect", {"program": "mcp-workflow", "version": "1.0.0"})
    assert detail["version"]["manifest"]["runtime"]["protocol"] == 3, detail
    assert d.sql("SELECT count(*) FROM tasks") == "0", "discovery executed code"
    record("discovery", {"tools": len(tools), "registered_programs": programs, "execution_count": 0})

    args = task_arguments(d, "mcp-task", payload={"tenant_id": "user-owned",
        "value": [1, None], "numbers": [(1 << 64) - 1, -(1 << 63), -0.0]})
    task = main.tool("ledgence_task_submit", args)
    assert task["scope"] == d.scope and task["idempotency_key"] == args["idempotency_key"], task
    replay = main.tool("ledgence_task_submit", args)
    assert replay["task_id"] == task["task_id"], (task, replay)
    changed = dict(args, data={**args["data"], "changed": True})
    error = main.tool("ledgence_task_submit", changed, error="conflict")
    assert error["outcome_unknown"] is False, error
    pending = main.tool("ledgence_task_result", {"task_id": task["task_id"]})
    assert pending["task"]["state"] == "queued" and pending["outcome"] is None, pending
    status = main.tool("ledgence_task_status", {"task_id": task["task_id"]})
    assert status["scope"] == d.scope and status["state"] == "queued", status
    page = main.tool("ledgence_task_list", {"filters": {"queue": d.queue}, "limit": 2})
    assert [row["task_id"] for row in page["items"]] == [task["task_id"]], page
    queued = main.tool("ledgence_task_submit", task_arguments(d, "mcp-cancel-task"))
    cancelled = main.tool("ledgence_task_cancel", {"task_id": queued["task_id"]})
    assert cancelled["state"] == "cancelled", cancelled
    assert main.tool("ledgence_task_result", {"task_id": queued["task_id"]})["outcome"] == {"kind": "cancelled"}
    worker = d.start_worker(concurrency=1)
    result = finished(main, "task", task["task_id"])
    assert result["event"]["data"] == args["data"] and result["dependency"] == "packaged", result
    assert json.dumps(result["event"]["data"]["payload"]["numbers"]) == "[18446744073709551615, -9223372036854775808, -0.0]", result
    assert len(d.invocations(task["task_id"])) == 1 and not d.invocations(queued["task_id"])
    record("task-execution-and-idempotency", {"task_id": task["task_id"], "replayed_id": replay["task_id"],
        "worker_invocations": 1, "queued_cancelled_task": queued["task_id"],
        "exact_integer_and_negative_zero_roundtrip": True})

    wrong = session("mcp-wrong-scope", scope={**d.scope, "namespace": "other"})
    for name, arguments in (("ledgence_task_status", {"task_id": task["task_id"]}),
                           ("ledgence_program_list", {}),
                           ("ledgence_program_versions", {"program": "invoice"}),
                           ("ledgence_program_inspect", {"program": "invoice", "version": "1.0.0"})):
        error = wrong.tool(name, arguments, error="not_found")
        assert error["outcome_unknown"] is False, error
    wrong.close()
    for overrides in ({"tenant_id": "other"}, {"namespace": "other"}, {"scope": d.scope}):
        main.invalid("ledgence_task_submit", {**args, **overrides})
        main.invalid("ledgence_program_list", overrides)
    proxy = d.proxy()
    readonly = session("mcp-read-only", read_only=True, server=proxy.url)
    exposed = readonly.request("tools/list", {})["result"]["tools"]
    assert {item["name"] for item in exposed} == READ_TOOLS, exposed
    for name in WRITE_TOOLS:
        readonly.invalid(name, {})
    assert proxy.records == [], "read-only denial contacted the backend"
    assert readonly.tool("ledgence_task_status", {"task_id": task["task_id"]})["state"] == "succeeded"
    readonly.close()
    record("fixed-scope-and-read-only", {"rejected_scope_reads": 4, "read_only_tools": len(exposed),
                                        "mutation_attempts_blocked": len(WRITE_TOOLS)})

    args = workflow_arguments(d, "mcp-workflow", original={"workflow_id": "application-value"})
    workflow = main.tool("ledgence_workflow_submit", args)
    workflow_id = workflow["workflow_id"]
    assert main.tool("ledgence_workflow_submit", args)["workflow_id"] == workflow_id
    main.tool("ledgence_workflow_submit", dict(args, data={"changed": True}), error="conflict")
    wait_workflow(main, workflow_id)
    assert main.tool("ledgence_workflow_result", {"workflow_id": workflow_id})["outcome"] is None
    for mode in ("cancel", "disconnect"):
        held = HeldReplyProxy(d.server_url, "/v1/workflows/status")
        d.proxies.append(held)
        observer = session("mcp-observer-" + mode, server=held.url)
        request_id = observer.begin("tools/call", {"name": "ledgence_workflow_status",
                                                  "arguments": {"workflow_id": workflow_id}})
        assert held.reached.wait(timeout=10), "observer read did not reach its deterministic hold"
        if mode == "cancel":
            observer.notify("notifications/cancelled", {"requestId": request_id, "reason": "stop observing"})
            assert observer.request("ping", {})["result"] == {}
            held.release.set()
            observer.close()
        else:
            observer.close()
            held.release.set()
        assert main.tool("ledgence_workflow_status", {"workflow_id": workflow_id})["state"] == "waiting"
        assert all(path.split("?", 1)[0] == "/v1/workflows/status" for path in held.paths), held.paths
    event = {"specversion": "1.0", "id": "mcp-resume-1", "source": "urn:ledgence:mcp:acceptance",
             "type": "fixture.resume", "datacontenttype": "application/json", "custom": "preserved",
             "data": {"tenant_id": "application-owned", "value": 42}}
    send = {"workflow_id": workflow_id, "key": "resume", "event": event}
    receipt = main.tool("ledgence_workflow_send_event", send)
    duplicate = main.tool("ledgence_workflow_send_event", send)
    assert receipt["already_accepted"] is False and duplicate["already_accepted"] is True
    assert receipt["accepted_at"] == duplicate["accepted_at"]
    main.tool("ledgence_workflow_send_event", {**send, "event": {**event, "data": {"changed": True}}}, error="conflict")
    output = finished(main, "workflow", workflow_id)
    assert output["input"] == args["data"] and output["checkpoint"] == {"saved": True}, output
    assert output["wake"]["event"] == event, output
    record("workflow-resume-and-client-lifecycle", {"workflow_id": workflow_id,
        "event_reconciled": True, "cancelled_request_preserved_workflow": True,
        "disconnected_client_preserved_workflow": True, "output": output})

    for kind in ("task", "workflow"):
        path = "/v1/tasks" if kind == "task" else "/v1/workflows"
        proxy = d.proxy()
        dropped = proxy.lose_once(path)
        caller = session("mcp-lost-" + kind, server=proxy.url)
        arguments = task_arguments(d, "lost-task") if kind == "task" else workflow_arguments(d, "lost-workflow")
        error = caller.tool("ledgence_" + kind + "_submit", arguments, error="unavailable")
        assert error["outcome_unknown"] is True and dropped.is_set(), error
        calls = proxy.commands(path)
        assert len(calls) == 1 and calls[0]["status"] == 200, "submission retried automatically"
        accepted_id = json.loads(calls[0]["response"])[kind + "_id"]
        reconciled = caller.tool("ledgence_" + kind + "_submit", arguments)
        assert reconciled[kind + "_id"] == accepted_id, reconciled
        calls = proxy.commands(path)
        assert len(calls) == 2 and calls[0]["body"] == calls[1]["body"], calls
        if kind == "task":
            finished(main, kind, accepted_id)
            assert len(d.invocations(accepted_id)) == 1
        else:
            wait_workflow(main, accepted_id)
            main.tool("ledgence_workflow_cancel", {"workflow_id": accepted_id})
            eventually(lambda: main.tool("ledgence_workflow_result", {"workflow_id": accepted_id})["outcome"] == {"kind": "cancelled"},
                       timeout=30, description="explicit workflow cancellation")
        caller.close()
        record("lost-" + kind + "-submission-reply", {kind + "_id": accepted_id,
            "automatic_retries": 0, "identical_manual_reconciliation": True})

    # Once the real service commits, cancelling its still-pending MCP request
    # or ending the client must not turn into execution cancellation or a retry.
    for kind, mode in (("task", "cancel"), ("workflow", "disconnect")):
        label = kind + "-submit-" + mode
        path = "/v1/tasks" if kind == "task" else "/v1/workflows"
        held = HeldReplyProxy(d.server_url, path, method="POST")
        d.proxies.append(held)
        caller = session("mcp-" + label, server=held.url)
        arguments = (task_arguments(d, label, business_id="BUSINESS-1042") if kind == "task"
                     else workflow_arguments(d, label, business_id="BUSINESS-1042"))
        trace = {"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                 "tracestate": "mcp=acceptance"}
        arguments.update(correlation_key="BUSINESS-1042:" + label, origin_trace=trace)
        request_id = caller.begin("tools/call", {"name": "ledgence_" + kind + "_submit",
                                                "arguments": arguments})
        assert held.reached.wait(timeout=10), "submission did not reach its committed reply hold"
        calls = held.commands()
        assert len(calls) == 1 and calls[0]["status"] == 200, calls
        accepted = json.loads(calls[0]["response"])
        accepted_id = accepted[kind + "_id"]
        original_body = calls[0]["body"]
        command = json.loads(original_body)
        assert command["idempotency_key"] == arguments["idempotency_key"]
        assert command["origin_trace"] == trace
        assert command["input"]["data"] == arguments["data"]
        assert command["input"]["correlation_key"] == arguments["correlation_key"]
        if mode == "cancel":
            caller.notify("notifications/cancelled", {"requestId": request_id, "reason": "stop waiting for submission"})
            assert caller.request("ping", {})["result"] == {}
        else:
            caller.close()
        assert len(held.commands()) == 1, "MCP retried a cancelled or disconnected submission"
        held.release.set()
        # A new session makes the caller's explicit reconciliation visible as
        # the only second POST. Reusing its original key/input recovers the ID.
        fresh = session("mcp-reconcile-" + label, server=held.url)
        reconciled = fresh.tool("ledgence_" + kind + "_submit", arguments)
        assert reconciled[kind + "_id"] == accepted_id, (accepted, reconciled)
        assert reconciled["idempotency_key"] == arguments["idempotency_key"]
        calls = held.commands()
        assert len(calls) == 2 and all(row["body"] == original_body and row["status"] == 200 for row in calls), calls
        assert held.paths == [path, path], "client lifetime triggered another HTTP operation"
        activation_id = accepted_id if kind == "task" else accepted["activation_id"]
        stored = d.task(activation_id)
        assert stored["origin_trace"] == trace, stored
        assert stored["input"]["data"] == arguments["data"], stored
        assert stored["input"]["correlation_key"] == arguments["correlation_key"], stored
        if kind == "task":
            output = finished(main, "task", accepted_id)
            assert output["event"]["data"] == arguments["data"]
            assert {name: output["event"][name] for name in trace} == trace, output
            assert len(d.invocations(accepted_id)) == 1
            assert main.tool("ledgence_task_status", {"task_id": accepted_id})["correlation_key"] == arguments["correlation_key"]
            assert caller.request("ping", {})["result"] == {}
            caller.close()
            replies = [json.loads(line) for line in caller.stdout_path.read_text().splitlines()]
            assert not any(row.get("id") == request_id for row in replies), "cancelled MCP result was delivered"
        else:
            waiting = wait_workflow(main, accepted_id)
            assert waiting["correlation_key"] == arguments["correlation_key"], waiting
            event = {"specversion": "1.0", "id": "resume-disconnected-submission", "source": "urn:ledgence:mcp:acceptance",
                     "type": "fixture.resume", "datacontenttype": "application/json", "data": {"business_id": "BUSINESS-1042"}}
            main.tool("ledgence_workflow_send_event", {"workflow_id": accepted_id, "key": "resume", "event": event})
            output = finished(main, "workflow", accepted_id)
            assert output["input"] == arguments["data"] and output["wake"]["event"] == event, output
        fresh.close()
        assert len(held.commands()) == 2 and held.paths == [path, path]
        record("committed-" + label, {kind + "_id": accepted_id, "automatic_retries": 0,
            "identical_fresh_session_reconciliation": True, "business_identity_preserved": True,
            "origin_trace_preserved": True, "execution_cancelled": False})

    approved = main.tool("ledgence_workflow_submit", workflow_arguments(d, "mcp-approval", approval=True))
    wait_workflow(main, approved["workflow_id"])
    approvals = main.tool("ledgence_approval_list", {"workflow_id": approved["workflow_id"]})
    inspection = main.tool("ledgence_approval_inspect", {"workflow_id": approved["workflow_id"], "key": "review"})
    assert inspection["status"] == "pending" and inspection["action"]["arguments"] == {"value": 7}, inspection
    assert any(item["key"] == "review" for item in approvals["items"]), approvals
    main.invalid("ledgence_approval_decide", {"workflow_id": approved["workflow_id"], "key": "review", "decision": "approve"})
    main.tool("ledgence_workflow_cancel", {"workflow_id": approved["workflow_id"]})
    eventually(lambda: main.tool("ledgence_workflow_result", {"workflow_id": approved["workflow_id"]})["outcome"] == {"kind": "cancelled"},
               timeout=30, description="approval fixture cleanup")
    record("approval-observation-only", {"workflow_id": approved["workflow_id"], "pending_action_inspected": True,
                                        "approval_decision_unavailable": True})
    main.close()
    before_init = session("mcp-before-initialize", initialize=False)
    before_init.close()
    signal_session = session("mcp-signal")
    signal_session.close(terminate=True)
    record("bounded-clean-exit", {"initialized_eof": True, "uninitialized_eof": True, "sigterm": True})
    worker.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--binaries", type=Path)
    parser.add_argument("--evidence", type=Path, help="new directory for retained protocol and process evidence")
    args = parser.parse_args()
    parent_url = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not parent_url:
        parser.error("LEDGENCE_POSTGRES_URL must name a disposable test PostgreSQL server")
    database = "ledgence_mcp_" + uuid.uuid4().hex
    try:
        database_url = owned_database_url(parent_url, database)
    except ValueError as error:
        parser.error(str(error))
    root = Path(__file__).resolve().parents[1]
    binaries = args.binaries
    if binaries is None:
        subprocess.run(["cargo", "build", "-p", "ledgence-cli", "--bin", "ledgence", "--all-features", "--locked"], cwd=root, check=True)
        metadata = json.loads(subprocess.check_output(["cargo", "metadata", "--no-deps", "--format-version", "1", "--locked"], cwd=root))
        binaries = Path(metadata["target_directory"]) / "debug"
    binaries = binaries.resolve()
    if not (binaries / "ledgence").is_file():
        parser.error("missing built ledgence executable")
    directory = args.evidence.resolve() if args.evidence else Path(tempfile.mkdtemp(prefix="ledgence-mcp-"))
    if args.evidence:
        directory.mkdir(parents=True, exist_ok=False)
    python = os.environ.get("LEDGENCE_PYTHON", sys.executable)
    def admin(statement):
        result = subprocess.run([args.psql, "--dbname", parent_url, "-X", "--set", "ON_ERROR_STOP=1", "--command", statement],
                                capture_output=True, timeout=40)
        if result.returncode:
            raise RuntimeError("owned MCP test database setup/cleanup failed")
    results = []
    def record(name, detail):
        results.append({"scenario": name, "result": "passed", "detail": detail})
        (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        print("PASS " + name + ": " + json.dumps(detail), flush=True)
    deployment, created, succeeded, sessions = None, False, False, []
    try:
        admin(f'CREATE DATABASE "{database}"')
        created = True
        deployment = McpDeployment(root, directory, binaries, python, database_url, args.psql)
        deployment.publish("mcp-workflow", "1.0.0", program_source=WORKFLOW, runtime_protocol=3)
        migrated = subprocess.run([str(binaries / "ledgence"), "orchestrator", "migrate"],
                                  env=deployment.environment, capture_output=True, timeout=40)
        assert migrated.returncode == 0, migrated.stderr.decode(errors="replace")[-3000:]
        deployment.server, _ = deployment.start_server()
        for program, kind in (("invoice", "task"), ("mcp-workflow", "workflow")):
            deployment.command("ledgence", ["program", "register", "--server", deployment.server_url,
                "--program", program, "--version", "1.0.0", "--kind", kind])
        (directory / "resources.json").write_text(json.dumps({"database": database, "scope": deployment.scope,
            "binary": str(binaries / "ledgence"), "binary_sha256": hashlib.sha256((binaries / "ledgence").read_bytes()).hexdigest(),
            "python": python, "transport": "stdio", "external_model": False, "broker": False}, indent=2) + "\n")
        scenarios(deployment, record, sessions)
        deployment.server.stop()
        succeeded = True
        print(f"MCP acceptance passed: {len(results)} scenarios; evidence {directory}", flush=True)
        return 0
    except Exception as error:
        # These credentials come from the fixture environment, not MCP inputs.
        failure = traceback.format_exc().replace(parent_url, "[test database URL]").replace(database_url, "[owned database URL]")
        (directory / "failure.txt").write_text(failure)
        print(f"MCP acceptance failed: {type(error).__name__}; evidence {directory}\n{failure}", file=sys.stderr)
        return 1
    finally:
        try:
            try:
                failures = []
                for session in reversed(sessions):
                    try:
                        session.cleanup()
                    except Exception as error:
                        failures.append(type(error).__name__)
                if failures:
                    raise RuntimeError("MCP subprocess cleanup failed: " + ", ".join(failures))
            finally:
                if deployment:
                    deployment.close()
        finally:
            if created:
                admin(f'DROP DATABASE "{database}" WITH (FORCE)')
            if not args.evidence and succeeded:
                shutil.rmtree(directory)


if __name__ == "__main__":
    sys.exit(main())
