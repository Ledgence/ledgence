"""Cross-check native CLI, MCP and a wheel-installed Python client on one immutable journal (MIT)."""

import json
import subprocess
from pathlib import Path

from mcp_acceptance.harness import McpSession

from .scenario import GROUPS


def surfaces(d, python_client):
    name = d.resource_name  # The completed mixed scope has workflow and task decisions.
    query = {"scope": d.scope, "name": name, "limit": 2, "before": None}
    first = d.checked("POST", "/v1/resources/decisions", query, credentials=d.reader)
    query["before"] = first["next"]
    second = d.checked("POST", "/v1/resources/decisions", query, credentials=d.reader)
    assert first["next"] and second["items"]
    d.checked(
        "POST",
        "/v1/resources/decisions",
        query,
        credentials=d.worker_credentials,
        expected=403,
    )
    args = [
        "--server",
        d.server_url,
        "--tenant",
        d.scope["tenant_id"],
        "--namespace",
        d.scope["namespace"],
        "--credentials-file",
        str(d.reader),
    ]
    cli = ["resource", "decisions", *args, "--name", name, "--limit", "2"]
    assert d.command("ledgence", cli) == first
    assert d.command("ledgence", [*cli, "--before", first["next"]]) == second
    d.command("ledgence", [*cli, "--before", "01"], expected=2)

    mcp = McpSession(d, "fair-reader", credentials_file=d.reader, read_only=True)
    d.sessions.append(mcp)
    catalog = {
        tool["name"] for tool in mcp.request("tools/list", {})["result"]["tools"]
    }
    assert (
        "ledgence_resource_decisions" in catalog
        and "ledgence_resource_put" not in catalog
    )
    for before, expected in ((None, first), (first["next"], second)):
        assert (
            mcp.tool(
                "ledgence_resource_decisions",
                {"name": name, "before": before, "limit": 2},
            )
            == expected
        )
    mcp.invalid("ledgence_resource_decisions", {"name": name, "scope": d.scope})
    mcp.close()

    command = {
        "name": "surface-policy",
        "scope": d.scope,
        "request_id": "cli-surface",
        "expected_revision": None,
        "reason": "configure public fair sharing",
        "policy": {
            "enabled": True,
            "max_occupancy": 2,
            "rate": None,
            "offer_timeout_ms": 30_000,
            "start_timeout_ms": 5_000,
            "fair_sharing": GROUPS,
        },
    }
    saved = d.directory / "surface-policy.json"
    saved.write_text(
        json.dumps(
            {
                "format": "ledgence.admission.command.v1",
                "base_url": d.server_url,
                "body": json.dumps(command),
            }
        )
    )
    ctl = [*args[:-1], str(d.operator)]
    accepted = d.command("ledgence", ["resource", "put", *ctl, "--file", str(saved)])
    replay = d.command("ledgence", ["resource", "put", *ctl, "--file", str(saved)])
    assert replay["already_accepted"] and replay["resource"] == accepted["resource"]
    assert accepted["resource"]["policy"] == command["policy"]
    write = McpSession(
        d, "fair-control", credentials_file=d.operator, allow_resource_control=True
    )
    d.sessions.append(write)
    update = {k: v for k, v in command.items() if k != "scope"}
    update.update(
        expected_revision=1,
        request_id="mcp-surface",
        policy={**command["policy"], "max_occupancy": 3},
    )
    result = write.tool("ledgence_resource_put", update)
    assert (
        result["resource"]["revision"] == 2
        and result["resource"]["policy"] == update["policy"]
    )
    write.close()

    expected = d.directory / "surface-pages.json"
    expected.write_text(json.dumps([first, second]))
    script = Path(__file__).with_name("installed_client.py")
    proc = subprocess.run(
        [
            str(python_client),
            "-I",
            str(script),
            d.server_url,
            str(d.operator),
            d.scope["tenant_id"],
            d.scope["namespace"],
            name,
            str(expected),
        ],
        env=d.environment,
        check=False,
        capture_output=True,
        timeout=45,
    )
    (d.directory / "surface-client.stderr").write_bytes(proc.stderr)
    assert proc.returncode == 0, (
        "installed client probe failed; inspect surface-client.stderr"
    )
    return {
        "cli_pages": 2,
        "mcp_pages": 2,
        "worker_read_denied": True,
        "client": json.loads(proc.stdout),
    }
