"""Installed-client, CLI, and MCP contracts against actual task admission (MIT)."""

import ast
import json
import subprocess

from mcp_acceptance.harness import McpSession


PYTHON_SCENARIO = """import asyncio
import json
from pathlib import Path
import sys

import ledgence.client
from ledgence.client import AsyncClient, TaskAdmission, TaskAdmissionState


async def main():
    server, credentials, tenant, namespace, queue, resource, marker = sys.argv[1:]
    assert Path(ledgence.client.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
    async with AsyncClient(server, tenant=tenant, namespace=namespace,
                           credentials_file=credentials) as client:
        data = {"mode":"success", "marker":marker, "ticket":"SDK-1042"}
        frozen = client.tasks.prepare(program="invoice", version="1.0.0", queue=queue,
                                    data=data, idempotency_key="admission-sdk-1042",
                                    admission=TaskAdmission(resource=resource))
        handle = await client.tasks.submit(frozen)
        output = await handle.result(timeout=45)
        replay = await client.tasks.submit(frozen)
        assert replay.id == handle.id and output["event"]["data"] == data
        status = await handle.status()
        assert status.attempt_count == 1 and status.admission_deferrals == 0
        # Result delivery and source-bound capacity cleanup are independent.
        async with asyncio.timeout(30):
            while True:
                snapshot = await client.task_admissions.inspect(handle.id, generation=1)
                if snapshot.state is TaskAdmissionState.FINISHED:
                    break
                await asyncio.sleep(0.05)
        assert snapshot.source["owner"]["attempt_id"] == output["event"]["ldgattemptid"]
        print(json.dumps({"task_id":handle.id,"snapshot":snapshot.to_dict()}))


asyncio.run(main())
"""


def retained_source_surfaces(d, task):
    """An unresolved real task should have the same occurrence in all public clients."""
    query = {"name":d.resource_name,"state":"unknown_occupied","cursor":None,"limit":5}
    expected = d.checked("POST", "/v1/resources/sources", {"scope":d.scope, **query})
    assert expected["next_cursor"] is None and len(expected["items"]) == 1
    row = expected["items"][0]
    assert row["subject"] == {"kind":"task","task_id":task,"generation":1}
    assert set(row) == {"scope","resource","sequence","revision","state","subject","updated_at"}
    args = ["--server", d.server_url, "--credentials-file", str(d.operator),
            "--tenant",d.scope["tenant_id"],"--namespace",d.scope["namespace"],
            "--name",d.resource_name,"--state","unknown_occupied","--limit","5"]
    assert d.command("ledgence", ["resource","sources",*args]) == expected
    mcp = McpSession(d, "retained-resource-sources", credentials_file=d.operator)
    d.sessions.append(mcp)
    assert mcp.tool("ledgence_resource_sources", query) == expected
    mcp.close()
    client = """import asyncio, json, sys
from dataclasses import asdict
from pathlib import Path
import ledgence.client
from ledgence.client import AsyncClient, AuditFilter, AuditReadKind, AuditResourceKind, AuditSensitiveResponse
async def main():
    server, credential, tenant, namespace, name = sys.argv[1:]
    assert Path(ledgence.client.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
    async with AsyncClient(server, tenant=tenant, namespace=namespace, credentials_file=credential) as c:
        page = await c.resources.sources(name,state="unknown_occupied",limit=5)
        history = await c.audit.list(filter=AuditFilter.resource(AuditResourceKind.RESOURCE,name),limit=100)
        reads = [event.operation for event in history.items if isinstance(event.operation,AuditSensitiveResponse)
                 and event.operation.read is AuditReadKind.RESOURCE_SOURCES]
        assert len(reads) == 4 and all(entry.resource.name == name for entry in reads)
        items = [asdict(i) for i in page.items]
        for item in items:
            item["revision"] = str(item["revision"])
            item["sequence"] = str(item["sequence"])
        print(json.dumps({"items":items,"next_cursor":page.next_cursor}))
asyncio.run(main())
"""
    out = subprocess.run([d.python_client,"-I","-c",client,d.server_url,str(d.operator),
        d.scope["tenant_id"],d.scope["namespace"],d.resource_name],
        env=d.environment, capture_output=True, timeout=45)
    (d.directory / "retained-source-sdk.stderr").write_bytes(out.stderr)
    assert out.returncode == 0 and json.loads(out.stdout) == expected


def run_documented_task(d):
    """Execute the public source file twice using the installed candidate client."""
    script = d.root / "examples/resource-admission/submit_task.py"
    command = [
        d.python_client,
        "-I",
        str(script),
        "--server",
        d.server_url,
        "--credentials-file",
        str(d.operator),
        "--tenant",
        d.scope["tenant_id"],
        "--namespace",
        d.scope["namespace"],
        "--queue",
        d.queue,
        "--resource",
        d.resource_name,
        "--key",
        "documented-task-hello-ada",
    ]
    task = None
    for number in (1, 2):
        run = subprocess.run(command, capture_output=True, timeout=80)
        (d.directory / f"public-task-example-{number}.stderr").write_bytes(run.stderr)
        assert run.returncode == 0, "documented task example failed; inspect its stderr"
        output = dict(line.split(": ", 1) for line in run.stdout.decode().splitlines())
        assert ast.literal_eval(output["Result"]) == {"message": "Hello, Ada!"}
        if number == 1:
            task = output["Task"]
        assert output["Task"] == task, "frozen example submission did not replay"
    assert d.outcome(task)["outcome"]["output"] == {"message": "Hello, Ada!"}
    snapshot = d.ledger_state(task, "finished")
    assert snapshot["program"]["program"] == {
        "id": "hello-ledgence",
        "version": "0.1.0",
    }
    assert (
        snapshot["resource"] == d.resource_name and d.status(task)["attempt_count"] == 1
    )
    return task


def public_surfaces(d, _):
    d.policy(offer_ms=30_000)
    worker = d.worker()
    documented = run_documented_task(d)
    run = subprocess.run(
        [
            d.python_client,
            "-I",
            "-c",
            PYTHON_SCENARIO,
            d.server_url,
            str(d.operator),
            d.scope["tenant_id"],
            d.scope["namespace"],
            d.queue,
            d.resource_name,
            str(d.marker),
        ],
        capture_output=True,
        timeout=60,
    )
    (d.directory / "task-admission-client.stderr").write_bytes(run.stderr)
    assert run.returncode == 0, (
        "installed client task admission failed; inspect task-admission-client.stderr"
    )
    reply = json.loads(run.stdout)
    task = reply["task_id"]
    snapshot = reply["snapshot"]
    assert snapshot == d.ledger(task)
    d.assert_marker(task, [1])
    cli = d.command(
        "ledgence",
        [
            "admission",
            "inspect",
            "--task",
            task,
            "--generation",
            "1",
            "--server",
            d.server_url,
            "--tenant",
            d.scope["tenant_id"],
            "--namespace",
            d.scope["namespace"],
            "--credentials-file",
            str(d.operator),
        ],
    )
    assert cli == snapshot
    config = d.checked("GET", "/v1/console/config", credentials=d.operator)
    assert config["contract_version"] == 16 and config["capabilities"]["task_admission"]
    address = {"task_id": task, "generation": 1}
    status, headers, inspected = d.request(
        "POST", "/v1/console/task-admissions/inspect", address, credentials=d.operator
    )
    assert status == 200 and inspected == snapshot
    assert {key.lower(): value for key, value in headers.items()}["ledgence-console-contract"] == "16"
    d.checked("POST", "/v1/console/task-admissions/inspect",
              {**address, "scope": d.scope}, credentials=d.operator, expected=400)

    mcp = McpSession(d, "admission-public-task", credentials_file=d.operator)
    d.sessions.append(mcp)
    catalog = {
        item["name"] for item in mcp.request("tools/list", {})["result"]["tools"]
    }
    assert "ledgence_task_admission_inspect" in catalog
    assert "ledgence_task_admission_release" not in catalog
    assert (
        mcp.tool("ledgence_task_admission_inspect", {"task_id": task, "generation": 1})
        == snapshot
    )
    command = {
        "program": "invoice",
        "version": "1.0.0",
        "queue": d.queue,
        "data": {"mode": "success", "marker": str(d.marker), "ticket": "MCP-1042"},
        "idempotency_key": "admission-mcp-1042",
        "admission": {"resource": d.resource_name},
    }
    submitted = mcp.tool("ledgence_task_submit", command)
    second = submitted["task_id"]
    assert mcp.tool("ledgence_task_submit", command)["task_id"] == second
    outcome = d.outcome(second)
    assert outcome["outcome"]["output"]["event"]["data"] == command["data"]
    d.ledger_state(second, "finished")
    assert mcp.tool(
        "ledgence_task_admission_inspect", {"task_id": second, "generation": 1}
    ) == d.ledger(second)
    d.assert_marker(second, [1])
    mcp.close()
    worker.stop()
    return {
        "documented_hello_task": documented,
        "installed_client_task": task,
        "mcp_task": second,
        "cli_and_mcp_agree_with_durable_snapshot": True,
    }
