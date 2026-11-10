"""Execution artifact acceptance through published entry points (MIT)."""
import hashlib
import json
from pathlib import Path

from mcp_acceptance.harness import McpSession
from .harness import LostUploadAck


def transfer_retry_restart(d):
    writer, reader = d.worker("artifact-producer"), d.worker("artifact-consumer")
    outputs = []
    for size in (0, 300_007):
        workflow = d.begin(size)
        output = d.settled(workflow)
        expected_digest = "sha256:" + hashlib.sha256(b"x" * size).hexdigest()
        assert output["verified"] == {"bytes": size, "digest": expected_digest}
        assert output["report"]["size"] == size
        page = d.artifact_command("list", ["--workflow", workflow])
        assert len(page["items"]) == 2 and page["next_cursor"] is None
        producers = {item["producer"]["kind"] for item in page["items"]}
        assert producers == {"workflow_activation", "task"}
        assert all(item["root"] == {"kind": "workflow", "id": workflow} for item in page["items"])
        records = [json.loads(line) for line in (d.directory / "producer-attempts.jsonl").read_text().splitlines()]
        records = [row for row in records if row["artifact"]["id"] == output["report"]["id"]]
        assert len(records) == 2 and [row["attempt"] for row in records] == [1, 2]
        assert records[0]["artifact"] == records[1]["artifact"]
        task = d.task(records[0]["task"])
        assert task["attempt_count"] == 2 and task["state"] == "succeeded"
        outputs.append({"workflow_id": workflow, "output": output})
    # Four logical artifacts produce exactly four immutable objects/generations,
    # despite two failed task attempts followed by retry.
    assert d.sql("SELECT count(*) FROM execution_artifacts") == "4"
    assert d.sql("SELECT count(*) FROM artifact_uploads") == "4"
    assert d.sql("SELECT count(*) FROM artifact_uploads WHERE stage='ready' AND verified") == "4"
    writer.stop(); reader.stop(); d.server.stop()
    if hasattr(d, "s3_fixture"):
        d.s3_fixture.stop()
        d.s3_fixture.start()
    d.start_server()
    for index, entry in enumerate(outputs):
        reference = entry["output"]["report"]
        inspection = d.artifact_command("inspect", ["--artifact", reference["id"]])
        assert inspection["reference"] == reference
        destination = d.directory / f"restart-download-{index}.bin"
        d.artifact_command("download", ["--artifact", reference["id"], "--file", str(destination)])
        assert destination.read_bytes() == b"x" * reference["size"]
    d.outputs = outputs
    return {"workflows": outputs, "durable_objects": 4, "producer_attempts": 4, "workers": 2, "restart_verified": True}


def permissions_and_public_clients(d):
    entry = d.outputs[-1]
    reference, workflow = entry["output"]["report"], entry["workflow_id"]
    # Metadata-only authority can inspect/list through MCP but cannot download
    # bytes, publish, or recover somebody else's generation.
    session = McpSession(d, "artifact-metadata", credentials_file=d.reader, read_only=True)
    d.sessions.append(session)
    tools = session.request("tools/list", {})["result"]["tools"]
    names = {tool["name"] for tool in tools}
    assert {"ledgence_artifact_list", "ledgence_artifact_inspect"} <= names
    assert not {"ledgence_artifact_put", "ledgence_artifact_download", "ledgence_artifact_replace"} & names
    assert session.tool("ledgence_artifact_inspect", {"artifact_id": reference["id"]})["reference"] == reference
    page = session.tool("ledgence_artifact_list", {"target": {"kind": "workflow", "id": workflow}, "limit": 100})
    assert len(page["items"]) == 2
    session.close()
    requester = {"kind": "external", "scope": d.scope}
    d.checked("POST", "/v1/artifacts/download", {"requester": requester, "command": reference}, credentials=d.reader, expected=403)
    d.checked("POST", "/v1/artifacts/prepare", {"requester": requester, "command": {
        "scope": d.scope, "target": {"kind": "workflow", "id": workflow}, "key": "forbidden",
        "metadata": {key: reference[key] for key in ("digest", "size", "media_type")} | {"filename": None}}}, credentials=d.reader, expected=403)
    # Same tenant but a distinct standalone execution root still cannot read the
    # workflow's file through a worker lease.
    worker = d.worker("artifact-cross-root")
    command = d.submission("cross-root", program="artifact-consumer", artifact=reference)
    command["input"]["queue"] = "artifact-cross-root"
    command["input"]["retry_policy"]["max_attempts"] = 1
    task_id = d.checked("POST", "/v1/tasks", command)["task_id"]
    d.terminal(task_id, state="failed", timeout=45)
    worker.stop()
    source = d.directory / "external-report.bin"
    source.write_bytes(b"external report")
    result = d.installed('''import asyncio,json,sys
from pathlib import Path
from ledgence.client import AsyncClient, AuditArtifact, AuditFilter, AuditReadKind
async def main():
    async with AsyncClient(sys.argv[1], tenant=sys.argv[3], namespace=sys.argv[4], credentials_file=sys.argv[2]) as client:
        ref=await client.artifacts.put_file(sys.argv[6],workflow_id=sys.argv[5],key="external-report")
        replay=await client.artifacts.put_file(sys.argv[6],workflow_id=sys.argv[5],key="external-report")
        assert replay==ref
        manifest=await client.artifacts.inspect(ref.id)
        assert manifest.producer.kind=="verified_actor"
        page=await client.artifacts.list(workflow_id=sys.argv[5])
        assert ref in [item.reference for item in page.items]
        await client.artifacts.handle(ref.id).download(sys.argv[7])
        assert Path(sys.argv[7]).read_bytes()==Path(sys.argv[6]).read_bytes()
        cursor=None
        found=False
        for _ in range(10):
            page=await client.audit.list(filter=AuditFilter.resource("workflow",sys.argv[5]),cursor=cursor)
            for event in page.items:
                operation=event.operation
                if getattr(operation,"read",None)==AuditReadKind.ARTIFACT_DOWNLOAD:
                    assert isinstance(operation.resource,AuditArtifact)
                    assert operation.resource.root.id==sys.argv[5]
                    found=found or operation.resource.artifact_id==ref.id
            cursor=page.next_cursor
            if cursor is None:
                break
        assert found
        print(json.dumps(ref.to_dict()))
asyncio.run(main())
''', workflow, source, d.directory / "installed-download.bin")
    assert result["size"] == len(b"external report")
    # Exercise CLI publication independently, reusing this external actor's
    # canonical key returns the installed client's same accepted reference.
    cli_ref = d.artifact_command("put", ["--workflow", workflow, "--key", "external-report", "--file", str(source)])
    assert cli_ref == result
    assert d.sql("SELECT count(*) FROM artifact_uploads") == "5"
    return {"mcp_metadata": True, "forbidden_reader": True, "forbidden_cross_root_task": task_id,
            "installed_client_and_cli_reference": result}


def lost_upload_ack(d):
    source = '''from pathlib import Path
from tempfile import TemporaryDirectory
from ledgence.worker import artifacts
async def handle(event):
    recovered=False
    with TemporaryDirectory() as directory:
        path=Path(directory)/"report.txt"
        path.write_bytes(b"recover explicitly")
        try:
            ref=await artifacts.put_file(path,key="recover")
        except artifacts.ArtifactUploadUncertain as error:
            assert error.selection is not None
            observed=await artifacts.upload_status(error.selection["upload_id"])
            status=await artifacts.reconcile(error.selection)
            assert observed["selection"]==status["selection"]
            ref=status["reference"]
            assert ref is not None
            recovered=True
    return {"artifact":ref,"recovered":recovered}
'''
    d.publish("artifact-recovery", "1.0.0", program_source=source, runtime_protocol=3)
    proxy = LostUploadAck(d.server_url)
    d.proxies.append(proxy)
    credentials = json.loads(d.worker_credentials.read_text())
    credentials["server"] = proxy.url
    path = d.directory / "worker-artifact-proxy.json"
    path.write_text(json.dumps(credentials)); path.chmod(0o600)
    d.secret_files.append(path)
    worker = d.worker("artifact-recovery", server=proxy.url, credentials=path)
    command = d.submission("recover-lost-ack", program="artifact-recovery")
    command["input"]["queue"] = "artifact-recovery"
    command["input"]["retry_policy"]["max_attempts"] = 1
    task_id = d.checked("POST", "/v1/tasks", command)["task_id"]
    d.terminal(task_id, timeout=45)
    output = d.checked("GET", d.query("/v1/tasks/result", task_id=task_id))["outcome"]["output"]
    assert output["recovered"] is True and proxy.dropped.is_set() and proxy.puts == 1
    assert d.task(task_id)["attempt_count"] == 1
    worker.stop()
    return {"task_id": task_id, "puts": proxy.puts, "explicit_status_and_reconcile": True, "reference": output["artifact"]}


CASES = [transfer_retry_restart, permissions_and_public_clients, lost_upload_ack]
