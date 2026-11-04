"""Inspect the real composed source through CLI, MCP, installed Python and Console (MIT)."""
import json
import subprocess
from urllib.parse import urlencode

from mcp_acceptance.harness import McpSession


PYTHON_CHECK = '''import asyncio,json,sys
from pathlib import Path
import ledgence.client
from ledgence.client import AdmissionOccurrence,AsyncClient
async def main():
    server,credentials,tenant,namespace,filename=sys.argv[1:]
    expected=json.loads(Path(filename).read_text())
    assert Path(ledgence.client.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
    async with AsyncClient(server,tenant=tenant,namespace=namespace,credentials_file=credentials) as client:
        value=expected["occurrence"]
        identity=AdmissionOccurrence(client.scope,value["workflow_id"],value["origin_activation_id"],value["key"])
        actual=await client.admissions.inspect(identity)
        assert actual.to_dict()==expected
    print(json.dumps({"same_snapshot":True,"generic_finish_absent":expected["finish"] is None}))
asyncio.run(main())
'''


def public_surfaces(d, occurrence):
    snapshot = d.assert_original(occurrence)
    identity = {k: v for k, v in snapshot["occurrence"].items() if k != "scope"}
    expected = d.checked("POST", "/v1/admissions/inspect", {"occurrence": snapshot["occurrence"]})
    assert expected == snapshot
    args = ["--server", d.server_url, "--tenant", d.scope["tenant_id"], "--namespace", d.scope["namespace"],
            "--credentials-file", str(d.operator)]
    actual = d.command("ledgence", ["admission", "inspect", *args, "--workflow", identity["workflow_id"],
                                   "--activation", identity["origin_activation_id"], "--key", identity["key"]])
    assert actual == expected
    mcp = McpSession(d, "protected-admission-inspect", credentials_file=d.operator)
    d.sessions.append(mcp)
    assert mcp.tool("ledgence_admission_inspect", identity) == expected
    mcp.close()
    script = d.directory / "installed-provider-admission-client.py"
    script.write_text(PYTHON_CHECK)
    evidence = d.directory / "protected-public-snapshot.json"
    evidence.write_text(json.dumps(expected))
    result = subprocess.run([str(d.python_client), "-I", str(script), d.server_url, str(d.operator),
                             d.scope["tenant_id"], d.scope["namespace"], str(evidence)],
                            env=d.environment, capture_output=True, timeout=60)
    (d.directory / "installed-provider-admission-client.stdout").write_bytes(result.stdout)
    (d.directory / "installed-provider-admission-client.stderr").write_bytes(result.stderr)
    assert result.returncode == 0, "installed client failed to inspect typed protected admission"
    query = urlencode({"workflow_id": occurrence["workflow_id"], "limit": 50})
    status, headers, graph = d.request("GET", "/v1/console/workflows/explorer?" + query, credentials=d.operator)
    assert status == 200 and {k.lower(): v for k, v in headers.items()}["ledgence-console-contract"] == "11"
    nodes = graph["page"]["items"]
    assert graph["page"]["next_cursor"] is None
    assert any(node["kind"] == "admission_wait" for node in nodes)
    assert any(node["kind"] == "provider_wait" for node in nodes)
    assert not any(node["kind"] == "admission_operation" for node in nodes), "provider call fabricated a generic business success"
    assert d.checked("POST", "/v1/console/admissions/inspect", identity) == expected
    (d.directory / "console-admission-graph.json").write_text(json.dumps(graph, indent=2) + "\n")
    return {"cli_mcp_installed_python_same_snapshot": True, "console_c9_separates_waits": True}
