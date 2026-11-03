"""Actual worker execution, durable attribution and fail-closed payload delivery."""
import json
import subprocess
import urllib.parse

from http_acceptance.harness import eventually
from mcp_acceptance.harness import McpSession

CANARY = "operator-audit-private-application-canary"
WORKFLOW = f'''from ledgence.worker.workflow import ApprovalAction, workflow_context
def memo():
    return {{"private": {CANARY!r}}}
async def handle(event):
    ctx = workflow_context()
    if ctx.continuation == "start":
        await ctx.local("memo", memo)
        return ctx.request_approval("review", action=ApprovalAction(
            "fixture:review", version="1", arguments={{"private": {CANARY!r}}}),
            continuation="resume", state={{}}, timeout_ms=300000)
    return ctx.complete({{"private": {CANARY!r}}})
'''


def count(d, kind=None):
    suffix = " WHERE event_kind='" + kind + "'" if kind else ""
    return int(d.sql("SELECT count(*) FROM operator_audit_events" + suffix))


def query(d, path, **values):
    if path.startswith("/v1/console/"):
        return path + "?" + urllib.parse.urlencode(values)
    return d.query(path, **values)


def execute(d):
    d.publish("audit-workflow", "1.0.0", program_source=WORKFLOW, runtime_protocol=3)
    d.publish("audit-task", "1.0.0", program_source="def handle(event):\n    return event['data']\n")
    task = d.submit(d.submission("audit-task", program="audit-task", private=CANARY))["task_id"]
    workflow = d.checked("POST", "/v1/workflows",
        d.submission("audit-workflow", program="audit-workflow", private=CANARY))["workflow_id"]
    paths = [("/v1/tasks/result", {"task_id": task}),
             ("/v1/console/tasks/result", {"task_id": task}),
             ("/v1/workflows/result", {"workflow_id": workflow}),
             ("/v1/console/workflows/result", {"workflow_id": workflow})]
    before = count(d)
    for path, identity in paths:
        for _ in range(3):
            assert d.checked("GET", query(d, path, **identity))["outcome"] is None
        d.checked("GET", query(d, path, **identity), credentials=d.reader, expected=403)
    assert count(d) == before, "pending metadata-only results wrote audit events"
    worker = d.start_worker()
    eventually(lambda: d.checked("GET", d.query("/v1/tasks/status", task_id=task))["state"] == "succeeded",
               timeout=60, description="actual worker task completion")
    eventually(lambda: d.checked("GET", d.query("/v1/workflows/status", workflow_id=workflow))["state"] == "waiting",
               timeout=60, description="actual workflow local step and approval wait")
    assert count(d) == before, "worker execution or metadata status polling wrote operator audit events"
    approval = d.checked("POST", "/v1/workflows/approvals/inspect",
                         {"scope": d.scope, "workflow_id": workflow, "key": "review"})
    assert approval["action"]["arguments"]["private"] == CANARY
    assert "protection" not in approval, "legacy approval snapshot changed"
    decision = {key: approval[key] for key in ("workflow_id", "key", "activation_id", "revision", "action")}
    decision.update(scope=d.scope, decision_id="audit-accept", decision="approve",
                    reviewer="untrusted-reviewer-label", reason=None)
    return worker, task, workflow, decision


def install_fault(d):
    # Owned disposable DB only: acceptance rows may commit, response observations
    # deliberately cannot. This is below the real HTTP/application/store stack.
    d.sql("CREATE FUNCTION audit_fixture_reject_read() RETURNS trigger LANGUAGE plpgsql AS $$ "
          "BEGIN IF NEW.event_kind='sensitive_response' THEN RETURN NULL; END IF; RETURN NEW; END $$; "
          "CREATE TRIGGER audit_fixture_reject_read BEFORE INSERT ON operator_audit_events "
          "FOR EACH ROW EXECUTE FUNCTION audit_fixture_reject_read()")


def remove_fault(d):
    d.sql("DROP TRIGGER IF EXISTS audit_fixture_reject_read ON operator_audit_events; "
          "DROP FUNCTION IF EXISTS audit_fixture_reject_read()")


def failed_response_and_aliases(d, task, workflow, decision):
    install_fault(d)
    try:
        status, _, value = d.request("POST", "/v1/workflows/approvals/decide", decision, credentials=d.operator)
        assert status == 503 and CANARY not in json.dumps(value), "accepted decision leaked/was reported definitive"
        assert count(d, "approval_accepted") == 1, "acceptance did not commit independently of response"
        eventually(lambda: d.checked("GET", d.query("/v1/workflows/status", workflow_id=workflow))["state"] == "succeeded",
                   timeout=60, description="accepted workflow resumes despite lost response")
        routes = [("GET", query(d, path, task_id=task), None) for path in (
            "/v1/tasks/inspect", "/v1/console/tasks/inspect", "/v1/tasks/result", "/v1/console/tasks/result")]
        routes += [("GET", query(d, path, workflow_id=workflow), None) for path in (
            "/v1/workflows/result", "/v1/console/workflows/result", "/v1/console/workflows/input")]
        routes.append(("GET", query(d, "/v1/console/workflows/local-steps/inspect", workflow_id=workflow,
                       activation_id=decision["activation_id"], step_key="memo"), None))
        for prefix, console in (("/v1/workflows", False), ("/v1/console", True)):
            reference = {"workflow_id": workflow, "key": "review"}
            page = {"workflow_id": workflow, "after_key": None, "limit": 10}
            replay = dict(decision)
            if console:
                replay.pop("scope")
                replay["revision"] = str(replay["revision"])
            else:
                reference["scope"] = page["scope"] = d.scope
            routes.extend((
                ("POST", prefix + "/approvals/inspect", reference),
                ("POST", prefix + "/approvals/list", page),
                ("POST", prefix + "/approvals/decide", replay),
            ))
        for method, path, body in routes:
            status, _, value = d.request(method, path, body, credentials=d.operator)
            assert status == 503 and CANARY not in json.dumps(value), path.split("?")[0]
        assert len(routes) == 14 and count(d, "approval_accepted") == 1
    finally:
        remove_fault(d)
    replay = d.checked("POST", "/v1/workflows/approvals/decide", decision)
    assert replay["already_accepted"] and replay["approval"]["decision"]["reviewer"] == "untrusted-reviewer-label"
    assert count(d, "approval_accepted") == 1
    # Caller correlation is never the deduplication authority for response reads.
    before = count(d)
    for _ in range(2):
        status, _, value = d.request("GET", d.query("/v1/tasks/result", task_id=task), credentials=d.operator,
                                    headers={"Request-Id": "caller-reused-correlation"})
        assert status == 200 and CANARY in json.dumps(value)
    assert count(d) == before + 2
    assert d.sql("SELECT count(*)=count(DISTINCT event_id) FROM operator_audit_events") == "t"
    return {"payload_aliases_denied_before_bytes": 14, "decision_commit_response_failure": 503,
            "exact_replay": "accepted_once", "pending_result_poll_audits": 0}


def exact_filter(decision):
    return dict(kind="approval_acceptance", **{key: decision[key] for key in (
        "workflow_id", "key", "activation_id", "revision", "decision_id")})


def assert_proof(page, decision):
    assert len(page["items"]) == 1 and page["next_cursor"] is None
    event = page["items"][0]
    assert event["actor"]["subject"] == "operator"
    assert event["operation"]["decision_id"] == decision["decision_id"]
    assert "untrusted-reviewer-label" not in json.dumps(page) and CANARY not in json.dumps(page)
    return event


def clients(d, decision, interpreter):
    before = count(d)
    args = ["audit", "approval", "--server", d.server_url, "--credentials-file", str(d.reader),
            "--tenant", d.scope["tenant_id"], "--namespace", d.scope["namespace"],
            "--workflow", decision["workflow_id"], "--key", decision["key"],
            "--activation", decision["activation_id"], "--revision", str(decision["revision"]),
            "--decision", decision["decision_id"]]
    event = assert_proof(d.command("ledgence", args), decision)
    assert event["recorded_at"] == int(d.sql("SELECT convert_from(snapshot_bytes,'UTF8')::jsonb"
        "->'decision'->>'decided_at' FROM workflow_approvals WHERE wait_key='review'"))
    listed = d.command("ledgence", ["audit", "list", "--server", d.server_url,
        "--credentials-file", str(d.reader), "--tenant", d.scope["tenant_id"],
        "--namespace", d.scope["namespace"], "--workflow", decision["workflow_id"]])
    assert any(item["event_id"] == event["event_id"] for item in listed["items"])
    session = McpSession(d, "audit-mcp", credentials_file=d.reader)
    d.sessions.append(session)
    assert_proof(session.tool("ledgence_audit_query", {"filter": exact_filter(decision), "limit": 2}), decision)
    resource = session.tool("ledgence_audit_query", {"filter": {"kind": "resource",
        "resource_kind": "workflow", "resource_id": decision["workflow_id"]}, "limit": 100})
    assert any(item["event_id"] == event["event_id"] for item in resource["items"])
    session.close()
    generic = {"scope": d.scope, "filter": exact_filter(decision), "cursor": None, "limit": 2}
    assert_proof(d.checked("POST", "/v1/audit/events", generic, credentials=d.reader), decision)
    console = {key: value for key, value in generic.items() if key != "scope"}
    console["filter"] = dict(console["filter"], revision=str(decision["revision"]))
    projected = d.checked("POST", "/v1/console/audit/events", console, credentials=d.reader)
    assert projected["items"][0]["operation"]["revision"] == str(decision["revision"])
    assert_proof(projected, decision)
    if interpreter:
        script = d.directory / "installed-audit-client.py"
        script.write_text('''import asyncio,json,sys
from ledgence.client import AsyncClient, AuditFilter
async def main():
    server,credentials,tenant,namespace,query_path=sys.argv[1:]
    query=json.load(open(query_path))
    async with AsyncClient(server,tenant=tenant,namespace=namespace,credentials_file=credentials) as client:
        filter=AuditFilter.approval_acceptance(**{k:v for k,v in query.items() if k!="kind"})
        page=await client.audit.list(filter=filter,limit=2)
        assert len(page.items)==1 and page.items[0].actor.subject=="operator"
        assert page.items[0].operation.decision_id==query["decision_id"]
    print(json.dumps({"installed_wheel":True,"verified_actor":"operator"}))
asyncio.run(main())
''')
        query_file = d.directory / "audit-query.json"
        query_file.write_text(json.dumps(exact_filter(decision)))
        result = subprocess.run([str(interpreter), "-I", str(script), d.server_url, str(d.reader),
            d.scope["tenant_id"], d.scope["namespace"], str(query_file)], env=d.environment,
            capture_output=True, timeout=30)
        (d.directory / "installed-audit-client.stdout").write_bytes(result.stdout)
        (d.directory / "installed-audit-client.stderr").write_bytes(result.stderr)
        assert result.returncode == 0, "installed Python audit client failed"
    assert count(d) == before, "audit queries recursively recorded observations"
    audit_bytes = d.sql("SELECT convert_from(event_bytes,'UTF8') FROM operator_audit_events")
    assert CANARY not in audit_bytes and "untrusted-reviewer-label" not in audit_bytes
    return {"verified_actor": event["actor"]["subject"], "cli": True, "stdio_mcp": True,
            "python_installed_wheel": bool(interpreter), "recursive_audit_writes": 0}


def rotation(d, worker, decision):
    original_actor = d.checked("GET", "/v1/access")["actor_key"]
    original_bytes = d.sql("SELECT encode(decision_bytes,'hex') FROM workflow_approvals WHERE wait_key='review'")
    rotated, d.config, _ = d.issue("rotated-operator", "operator", ["metadata_read", "sensitive_read", "approval_decide"])
    outsider, d.config, _ = d.issue("other-operator", "another-operator", ["metadata_read", "sensitive_read", "approval_decide"])
    worker.stop()
    d.server.stop()
    d.start_server()
    assert d.checked("GET", "/v1/access", credentials=rotated)["actor_key"] == original_actor
    assert d.checked("POST", "/v1/workflows/approvals/decide", decision, credentials=rotated)["already_accepted"]
    d.checked("POST", "/v1/workflows/approvals/decide", decision, credentials=outsider, expected=409)
    assert count(d, "approval_accepted") == 1
    assert d.sql("SELECT encode(decision_bytes,'hex') FROM workflow_approvals WHERE wait_key='review'") == original_bytes
    return {"same_actor_credential_rotation": "reconciled", "different_actor_replay": 409,
            "legacy_decision_bytes_unchanged": True}
