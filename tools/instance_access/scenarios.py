"""Production entry points, scoped denial, restart and credential rotation."""
import json
import subprocess

from http_acceptance.harness import eventually
from mcp_acceptance.harness import McpSession
from program_fixture import prepare_package


APPROVAL = '''from ledgence.worker.workflow import ApprovalAction, workflow_context
def handle(event):
    ctx = workflow_context()
    if ctx.continuation == "start":
        return ctx.request_approval("review", action=ApprovalAction(
            "fixture:review", version="1", arguments={"private": "approval-canary"}),
            continuation="resume", state={"saved": True}, timeout_ms=300000)
    return ctx.complete({"approved": True})
'''

# These paths supplement the exhaustive Rust route/method network test. Their
# bodies are deliberately invalid: authorization must precede typed decoding.
ANONYMOUS_ROUTES = (
    ("GET", "/v1/access"), ("GET", "/v1/console/config"),
    ("POST", "/v1/tasks"), ("GET", "/v1/tasks/inspect"),
    ("GET", "/v1/tasks/result"), ("GET", "/v1/attempts/inspect"),
    ("POST", "/v1/worker-sessions"), ("POST", "/v1/worker-observations"),
    ("POST", "/v1/workflows"), ("GET", "/v1/console/workflows/inspect"),
    ("GET", "/v1/console/workflows/local-steps/inspect"),
    ("POST", "/v1/workflows/approvals/inspect"), ("POST", "/v1/workflows/approvals/decide"),
    ("POST", "/v1/console/approvals/inspect"), ("POST", "/v1/console/approvals/decide"),
    ("GET", "/v1/console/workflows/input"), ("GET", "/v1/console/tasks/inspect"),
    ("GET", "/v1/console/tasks/result"), ("GET", "/v1/console/workflows/result"),
    ("GET", "/v1/console/programs/catalog"), ("POST", "/v1/console/programs/register"),
    ("GET", "/v1/programs/publication-capabilities"),
)


def anonymous_denial(d):
    before = d.row_counts(), d.store_hashes()
    for method, path in ANONYMOUS_ROUTES:
        status, _, _ = d.request(method, path, {} if method == "POST" else None)
        assert status == 401, f"anonymous {method} {path}: {status}"
    assert before == (d.row_counts(), d.store_hashes()), "anonymous denial mutated durable state"
    return {"denied_routes": len(ANONYMOUS_ROUTES), "durable_mutations": 0}


def publication(d):
    source = prepare_package(d.directory / "remote-program", "access-program", "1.0.0",
                             "def handle(event):\n    return event['data']\n", python=d.python)
    before = d.row_counts(), d.store_hashes()
    rejected = d.command("ledgence", ["program", "publish", "--source", str(source),
        "--server", d.server_url, "--credentials-file", str(d.reader), "--register"], expected=1)
    assert rejected["publication_outcome"] == "unconfirmed", "403 reported as uncertain publication"
    assert rejected["phase"] == "prepared", "denied upload advanced receipt"
    assert before == (d.row_counts(), d.store_hashes()), "denied publication wrote state"
    receipt = rejected["receipt"]
    arguments = ["program", "publish", "--resume", receipt, "--credentials-file", str(d.operator)]
    accepted = d.command("ledgence", arguments)
    assert accepted["phase"] == "registered" and accepted["publication_outcome"] == "confirmed"
    counts, hashes = d.row_counts(), d.store_hashes()
    assert hashes != before[1], "authorized publication wrote no artifact"
    replay = d.command("ledgence", arguments)
    assert replay["phase"] == "registered"
    assert (counts, hashes) == (d.row_counts(), d.store_hashes()), "receipt resume duplicated registration"
    return {"denied_upload": "unconfirmed", "resume": "registered", "replay_duplicate_writes": 0}


def console_session(d):
    origin = {"X-Ledgence-Console-Origin": d.server_url, "Origin": d.server_url,
              "Sec-Fetch-Site": "same-origin"}

    def login(credentials, cookie=None):
        headers = dict(origin)
        if cookie:
            headers["Cookie"] = cookie
        token = json.loads(credentials.read_text())["token"]
        status, response, actor = d.request("POST", "/v1/console/access/login",
            {"credential": token}, headers=headers)
        assert status == 200, "Console login failed"
        encoded = next(value for name, value in response.items() if name.lower() == "set-cookie")
        assert "HttpOnly" in encoded and "SameSite=Strict" in encoded and "Path=/" in encoded
        return encoded.split(";", 1)[0], actor

    cookie, actor = login(d.reader)
    headers = dict(origin, Cookie=cookie)
    status, _, bootstrap = d.request("GET", "/v1/console/access", headers=headers)
    assert status == 200 and bootstrap["actor_key"] == actor["actor_key"]
    headers["X-Ledgence-Actor"] = actor["actor_key"]
    assert d.request("GET", "/v1/console/config", headers=headers)[0] == 200
    assert d.request("GET", "/v1/console/tasks/result", headers=headers)[0] == 403
    assert d.request("GET", "/v1/tasks/result", headers=headers)[0] == 401, "cookie authorized raw API"
    assert d.request("GET", "/v1/console/config", credentials=d.operator, headers=headers)[0] == 401
    wrong = dict(headers, Origin="https://untrusted.example")
    assert d.request("GET", "/v1/console/config", headers=wrong)[0] == 403
    missing_actor = {key: value for key, value in headers.items() if key != "X-Ledgence-Actor"}
    assert d.request("GET", "/v1/console/config", headers=missing_actor)[0] == 403
    # A second tab switches identities. The first tab must not be able to send
    # a request or logout under the new cookie with its old expected actor.
    replacement, replacement_actor = login(d.operator, cookie)
    headers["Cookie"] = replacement
    before = d.row_counts()
    assert d.request("POST", "/v1/console/tasks", {}, headers=headers)[0] == 403
    status, response, _ = d.request("POST", "/v1/console/access/logout", {}, headers=headers)
    assert status == 403 and not any(name.lower() == "set-cookie" for name in response)
    assert d.row_counts() == before, "stale-actor mutation reached persistence"
    headers["X-Ledgence-Actor"] = replacement_actor["actor_key"]
    missing_origin = {key: value for key, value in headers.items() if key != "Origin"}
    assert d.request("POST", "/v1/console/access/logout", {}, headers=missing_origin)[0] == 403
    status, response, value = d.request("POST", "/v1/console/access/logout", {}, headers=headers)
    assert status == 200 and value == {"signed_out": True}
    assert any(name.lower() == "set-cookie" and "Max-Age=0" in value for name, value in response.items())
    # Dedicated login can recover from an invalid old cookie; ordinary calls
    # never accept mixed or stale credentials.
    stale = cookie.split("=", 1)[0] + "=expired-test-value"
    _, recovered = login(d.operator, stale)
    assert recovered["actor_key"] == replacement_actor["actor_key"]
    token = json.loads(d.worker_credentials.read_text())["token"]
    assert d.request("POST", "/v1/console/access/login", {"credential": token}, headers=origin)[0] == 403
    return {"http_only_cookie": True, "stale_actor_mutation": 403,
            "stale_actor_logout": 403, "cross_origin": 403, "worker_login": 403}


def worker_and_cli(d):
    worker = d.start_worker()
    submitted = d.submit(d.submission("access-cli"))
    task_id = submitted["task_id"]
    eventually(lambda: d.task(task_id)["state"] == "succeeded", timeout=60, description="authenticated worker task")
    arguments = ["--server", d.server_url, "--credentials-file", str(d.operator),
                 "--tenant", d.scope["tenant_id"], "--namespace", d.scope["namespace"], "--task", task_id]
    assert d.command("ledgence", ["task", "status"] + arguments)["state"] == "succeeded"
    result = d.command("ledgence", ["task", "result"] + arguments)
    assert result["outcome"]["kind"] == "succeeded"
    assert len(d.invocations(task_id)) == 1
    # The reporter uses its separate HTTP client. A working acquire/settle path
    # alone would not establish that credentials were wired into observations.
    eventually(lambda: int(d.sql("SELECT count(*) FROM worker_observations")) > 0,
               timeout=30, description="authenticated worker observation reporter")
    history = d.checked("GET", d.query("/v1/tasks/history", task_id=task_id))
    attempt = next(item["event"]["attempt_id"] for item in history if item["event"]["reason"] == "claimed")
    path = d.query("/v1/attempts/inspect", task_id=task_id, attempt_id=attempt)
    d.checked("GET", path, expected=403)
    raw = d.checked("GET", path, credentials=d.worker_credentials)
    assert raw["event"]["ldgattemptid"] == attempt and "lease" in raw
    d.checked("GET", d.query("/v1/tasks/result", task_id=task_id), credentials=d.worker_credentials, expected=403)
    assert d.artifacts.downloads() > 0, "no real artifact HTTP download"
    assert not d.artifact_credentials_seen.is_set(), "API credential leaked to artifact origin"
    return worker, task_id, {"task_id": task_id, "worker_observation_persisted": True,
                             "operator_raw_attempt": 403, "artifact_credential_forwarded": False}


def mcp_and_approval(d, task_id):
    admin = McpSession(d, "mcp-operator", credentials_file=d.operator)
    d.sessions.append(admin)
    reader = McpSession(d, "mcp-metadata", credentials_file=d.reader)
    d.sessions.append(reader)
    assert admin.tool("ledgence_task_result", {"task_id": task_id})["outcome"]["kind"] == "succeeded"
    assert reader.tool("ledgence_task_status", {"task_id": task_id})["state"] == "succeeded"
    denied = reader.tool("ledgence_task_result", {"task_id": task_id}, error="forbidden")
    assert denied["outcome_unknown"] is False, "definitive MCP denial reported uncertain"
    d.publish("access-approval", "1.0.0", program_source=APPROVAL, runtime_protocol=3)
    accepted = admin.tool("ledgence_workflow_submit", {
        "program": "access-approval", "version": "1.0.0", "queue": d.queue,
        "idempotency_key": "access-approval", "data": {"private": "workflow-canary"},
    })
    workflow_id = accepted["workflow_id"]
    eventually(lambda: admin.tool("ledgence_workflow_status", {"workflow_id": workflow_id})["state"] == "waiting",
               timeout=60, description="real workflow durable approval")
    inspection = admin.tool("ledgence_approval_inspect", {"workflow_id": workflow_id, "key": "review"})
    assert inspection["action"]["arguments"]["private"] == "approval-canary"
    assert "protection" not in inspection, "legacy approval wire unexpectedly changed"
    for name in ("ledgence_approval_list", "ledgence_approval_inspect"):
        arguments = {"workflow_id": workflow_id}
        if name.endswith("inspect"):
            arguments["key"] = "review"
        assert reader.tool(name, arguments, error="forbidden")["outcome_unknown"] is False
    for prefix in ("/v1/workflows", "/v1/console"):
        body = {"scope": d.scope, "workflow_id": workflow_id, "key": "review"}
        d.checked("POST", prefix + "/approvals/inspect", body, credentials=d.reader, expected=403)
        d.checked("POST", prefix + "/approvals/list", {"scope": d.scope, "workflow_id": workflow_id},
                  credentials=d.reader, expected=403)
    d.checked("GET", "/v1/console/workflows/input?workflow_id=" + workflow_id, credentials=d.reader, expected=403)
    admin.tool("ledgence_workflow_cancel", {"workflow_id": workflow_id})
    admin.close()
    reader.close()
    return {"workflow_id": workflow_id, "mcp_definitive_denial": True, "generic_approval_aliases_denied": True}


PYTHON_SCENARIO = '''import asyncio, json, sys
from ledgence.client import AsyncClient, Forbidden
async def main():
    server, operator, reader, tenant, namespace, queue, marker = sys.argv[1:]
    async with AsyncClient(server, tenant=tenant, namespace=namespace, credentials_file=operator) as client:
        task = await client.tasks.submit(program="invoice", version="1.0.0", queue=queue,
            data={"mode":"success", "marker":marker}, idempotency_key="access-python")
        output = await task.result(timeout=60)
        assert output["dependency"] == "packaged"
    async with AsyncClient(server, tenant=tenant, namespace=namespace, credentials_file=reader) as client:
        try:
            await client.tasks.submit(program="invoice", version="1.0.0", queue=queue,
                data={}, idempotency_key="access-python-denied")
        except Forbidden as error:
            assert error.code == "forbidden"
        else:
            raise AssertionError("metadata-only Python client submitted work")
    print(json.dumps({"task_id":task.id,"definitive_denial":True}))
asyncio.run(main())
'''


def python_client(d, interpreter):
    script = d.directory / "python-client.py"
    script.write_text(PYTHON_SCENARIO)
    # -I proves imports come from this interpreter's installation, not cwd or
    # a caller's PYTHONPATH. The gate never installs dependencies implicitly.
    result = subprocess.run([str(interpreter), "-I", str(script), d.server_url,
        str(d.operator), str(d.reader), d.scope["tenant_id"], d.scope["namespace"],
        d.queue, str(d.marker)], env=d.environment, capture_output=True, timeout=90)
    (d.directory / "python-client.stdout").write_bytes(result.stdout)
    (d.directory / "python-client.stderr").write_bytes(result.stderr)
    assert result.returncode == 0, "installed Python client scenario failed"
    assert d.sql("SELECT count(*) FROM tasks WHERE idempotency_key='access-python-denied'") == "0"
    return json.loads(result.stdout)


def restart_and_rotation(d):
    original = d.checked("GET", "/v1/access")
    d.server.stop()
    binding = d.sql("SELECT access_authority_id FROM self_hosted_instance")
    d.rejected_start(None)
    wrong = json.loads(d.config.read_text())
    wrong["authority_id"] = "unrelated-test-authority"
    path = d.directory / "wrong-authority.json"
    path.write_text(json.dumps(wrong))
    d.rejected_start(path)
    assert d.sql("SELECT access_authority_id FROM self_hosted_instance") == binding
    d.start_server()
    assert d.checked("GET", "/v1/access")["actor_key"] == original["actor_key"]
    d.server.stop()
    rotated, config, credential_id = d.issue("rotated-credentials", "operator", original["capabilities"])
    d.config = config
    d.start_server()
    rotated_identity = d.checked("GET", "/v1/access", credentials=rotated)
    assert rotated_identity["actor_key"] == original["actor_key"], "rotation changed stable actor"
    headers = {"Origin": d.server_url, "X-Ledgence-Console-Origin": d.server_url}
    status, response, _ = d.request("POST", "/v1/console/access/login",
        {"credential": json.loads(rotated.read_text())["token"]}, headers=headers)
    assert status == 200
    headers["Cookie"] = next(value for name, value in response.items()
                              if name.lower() == "set-cookie").split(";", 1)[0]
    headers["X-Ledgence-Actor"] = rotated_identity["actor_key"]
    old_operator = d.operator
    d.operator = rotated
    d.server.stop()
    # Test-owned verifier copies simulate the documented coordinated reload.
    # Only the new credential remains. No live/running config is edited.
    config = json.loads(config.read_text())
    config["credentials"] = [row for row in config["credentials"] if row["credential_id"] == credential_id]
    d.config = d.directory / "revoked-config.json"
    d.config.write_text(json.dumps(config))
    d.start_server()
    d.checked("GET", "/v1/access", credentials=old_operator, expected=401)
    assert d.checked("GET", "/v1/access")["actor_key"] == original["actor_key"]
    d.server.stop()
    config["credentials"][0]["expires_at_ms"] = 1
    d.config = d.directory / "expired-config.json"
    d.config.write_text(json.dumps(config))
    d.start_server()
    d.checked("GET", "/v1/access", expected=401)
    status, response, value = d.request("POST", "/v1/console/access/logout", {}, headers=headers)
    assert status == 200 and value == {"signed_out": True}, "expired cookie could not be cleared"
    assert any(name.lower() == "set-cookie" and "Max-Age=0" in value for name, value in response.items())
    return {"omission_rejected": True, "authority_change_rejected": True,
            "same_actor_rotation": True, "removed_token": 401, "expired_token": 401,
            "expired_cookie_logout": True}
