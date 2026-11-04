"""Exercise installed public clients against the owned admission deployment (MIT)."""

import json
import subprocess
import uuid
from urllib.parse import urlencode

from mcp_acceptance.harness import McpSession

PYTHON_SCENARIO = """import asyncio
from dataclasses import asdict
import json
from pathlib import Path
import sys

import ledgence.client
from ledgence.client import (
    AdmissionOccurrence, AdmissionState, AsyncClient, AuditFilter, AuditResourceKind,
    AuditResourcePolicyAccepted, InputError, ResourcePolicy, Scope,
)


async def main():
    server, credentials, tenant, namespace, name, expected_file = sys.argv[1:]
    evidence = json.loads(Path(expected_file).read_text())
    expected = evidence["snapshot"]
    # -I excludes cwd/PYTHONPATH; also reject editable/source-tree installations.
    assert Path(ledgence.client.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
    async with AsyncClient(
        server, tenant=tenant, namespace=namespace, credentials_file=credentials
    ) as client:
        before = await client.resources.get(name)
        assert before.resource.revision == 2 and not before.resource.policy.enabled
        assert before.resource.scope == client.scope
        policy = ResourcePolicy(max_occupancy=3, enabled=False,
                                offer_timeout_ms=1000, start_timeout_ms=1000)
        command = dict(policy=policy, request_id="python-" + name,
                       expected_revision=2, reason="installed client acceptance")
        first = await client.resources.put(name, **command)
        replay = await client.resources.put(name, **command)
        assert not first.already_accepted and replay.already_accepted
        assert first.resource == replay.resource
        assert first.resource.revision == 3 and first.resource.policy == policy
        resource = await client.resources.get(name)
        assert resource.resource == first.resource
        assert resource.offered == resource.occupied == resource.unknown == 0
        page = await client.resources.list(limit=100)
        assert any(item == resource for item in page.items)
        identity = expected["occurrence"]
        occurrence = AdmissionOccurrence(client.scope, identity["workflow_id"],
                                         identity["origin_activation_id"], identity["key"])
        snapshot = await client.admissions.inspect(occurrence)
        assert snapshot.state is AdmissionState.FINISHED
        assert snapshot.to_dict() == expected
        assert snapshot.source == snapshot.finish["source"]
        audit = await client.audit.list(filter=AuditFilter.resource(AuditResourceKind.RESOURCE, name))
        assert audit.next_cursor is None and len(audit.items) == 3
        previous = {row["event_id"]: row for row in evidence["audit"]["items"]}
        actor = evidence["audit"]["items"][0]["actor"]
        revisions = set()
        for row in audit.items:
            assert isinstance(row.operation, AuditResourcePolicyAccepted)
            revisions.add(row.operation.revision)
            assert row.operation.name == name
            assert asdict(row.audience) == evidence["audit"]["items"][0]["audience"]
            assert {"issuer": row.actor.issuer, "subject": row.actor.subject,
                    "class": row.actor.principal_class.value,
                    "credential_id": row.actor.credential_id} == actor
            if row.event_id in previous:
                saved = previous.pop(row.event_id)
                assert asdict(row.operation) == saved["operation"]
                assert row.recorded_at == saved["recorded_at"]
            else:
                assert row.operation.revision == 3
                assert row.operation.request_id == command["request_id"]
                assert row.operation.reason == command["reason"]
        assert not previous and revisions == {1, 2, 3}
        # Context identities cannot silently be rebound to this client's scope.
        foreign = AdmissionOccurrence(Scope(tenant, namespace + "-other"),
                                      occurrence.workflow_id, occurrence.origin_activation_id,
                                      occurrence.key)
        try:
            await client.admissions.inspect(foreign)
        except InputError:
            pass
        else:
            raise AssertionError("foreign admission scope was accepted")
    print(json.dumps({"resource_revision": 3, "put_replayed": True,
                      "accepted_callback": snapshot.finish["output"], "policy_audit_events": 3,
                      "foreign_scope_rejected": True}))


asyncio.run(main())
"""


def _saved(path, base_url, body):
    # Preserve the actual body string. Reconciliation submits this same identity
    # and content, never a freshly generated command after an uncertain reply.
    path.write_text(
        json.dumps(
            {
                "format": "ledgence.admission.command.v1",
                "base_url": base_url,
                "body": json.dumps(body),
            }
        )
    )


def _audit(d, name, page, revisions):
    assert page["next_cursor"] is None and len(page["items"]) == len(revisions)
    authenticated = d.checked("GET", "/v1/access")
    assert authenticated["operator_audit"] is True
    authority = json.loads(d.config.read_text())["authority_id"]
    accepted = {}
    for row in page["items"]:
        operation = row["operation"]
        assert (
            operation["kind"] == "resource_policy_accepted"
            and operation["name"] == name
        )
        revision = operation["revision"]
        assert revision not in accepted
        accepted[revision] = row
        assert (
            operation["request_id"]
            == {1: "cli-", 2: "mcp-", 3: "python-"}[revision] + name
        )
        assert operation["policy_digest"].startswith("sha256:")
        assert row["audience"] == {
            "authority_id": authority,
            "instance_id": authenticated["instance_id"],
            "scope": d.scope,
        }
        for field in ("issuer", "subject", "class"):
            assert row["actor"][field] == authenticated[field]
        assert row["actor"]["credential_id"]
    assert set(accepted) == set(revisions)
    return page


def _cli(d, name, expected):
    args = [
        "--server",
        d.server_url,
        "--tenant",
        d.scope["tenant_id"],
        "--namespace",
        d.scope["namespace"],
        "--credentials-file",
        str(d.operator),
    ]
    occurrence = expected["occurrence"]
    inspected = d.command(
        "ledgence",
        [
            "admission",
            "inspect",
            *args,
            "--workflow",
            occurrence["workflow_id"],
            "--activation",
            occurrence["origin_activation_id"],
            "--key",
            occurrence["key"],
        ],
    )
    assert inspected == expected
    body = {
        "scope": d.scope,
        "name": name,
        "request_id": "cli-" + name,
        "expected_revision": None,
        "reason": "saved CLI acceptance",
        "policy": {
            "enabled": False,
            "max_occupancy": 1,
            "rate": None,
            "offer_timeout_ms": 1000,
            "start_timeout_ms": 1000,
        },
    }
    saved = d.directory / "resource-public-command.json"
    command = ["resource", "put", *args, "--file", str(saved)]
    # Both refusals precede creation; neither can create even the harmless policy.
    _saved(saved, d.server_url + "/different-destination", body)
    d.command("ledgence", command, expected=2)
    _saved(saved, d.server_url, {**body, "scope": {**d.scope, "namespace": "other"}})
    d.command("ledgence", command, expected=2)
    d.checked(
        "POST", "/v1/resources/query", {"scope": d.scope, "name": name}, expected=404
    )
    _saved(saved, d.server_url, body)
    first = d.command("ledgence", command)
    replay = d.command("ledgence", command)
    assert first["already_accepted"] is False and replay["already_accepted"] is True
    assert first["resource"] == replay["resource"]
    assert first["resource"]["revision"] == 1
    assert (
        first["resource"]["scope"] == d.scope
        and first["resource"]["policy"] == body["policy"]
    )
    resource = d.command("ledgence", ["resource", "inspect", *args, "--name", name])
    assert resource["resource"] == first["resource"]
    assert resource["offered"] == resource["occupied"] == resource["unknown"] == 0
    audit = d.command("ledgence", ["audit", "list", *args, "--resource", name])
    _audit(d, name, audit, [1])
    return resource


def _mcp(d, resource, expected):
    ordinary = McpSession(d, "admission-public-default", credentials_file=d.operator)
    d.sessions.append(ordinary)
    catalog = {
        tool["name"] for tool in ordinary.request("tools/list", {})["result"]["tools"]
    }
    mutations = {"ledgence_resource_put", "ledgence_admission_release", "ledgence_task_admission_release"}
    assert mutations.isdisjoint(catalog)
    for mutation in mutations:
        ordinary.invalid(mutation, {})
    identity = {
        key: value for key, value in expected["occurrence"].items() if key != "scope"
    }
    assert ordinary.tool("ledgence_admission_inspect", identity) == expected
    assert (
        ordinary.tool(
            "ledgence_resource_inspect", {"name": resource["resource"]["name"]}
        )
        == resource
    )
    page = ordinary.tool("ledgence_resource_list", {"limit": 100})
    assert resource in page["items"]
    ordinary.invalid("ledgence_admission_inspect", {**identity, "scope": d.scope})
    ordinary.close()

    enabled = McpSession(
        d,
        "admission-public-control",
        credentials_file=d.operator,
        allow_resource_control=True,
    )
    d.sessions.append(enabled)
    catalog = {
        tool["name"] for tool in enabled.request("tools/list", {})["result"]["tools"]
    }
    assert mutations.issubset(catalog)
    name = resource["resource"]["name"]
    command = {
        "name": name,
        "request_id": "mcp-" + name,
        "expected_revision": 1,
        "reason": "explicit MCP control acceptance",
        "policy": {**resource["resource"]["policy"], "max_occupancy": 2},
    }
    first = enabled.tool("ledgence_resource_put", command)
    replay = enabled.tool("ledgence_resource_put", command)
    assert first["already_accepted"] is False and replay["already_accepted"] is True
    assert first["resource"] == replay["resource"]
    assert first["resource"]["revision"] == 2 and first["resource"]["scope"] == d.scope
    assert first["resource"]["policy"] == command["policy"]
    audit = enabled.tool(
        "ledgence_audit_query",
        {
            "filter": {
                "kind": "resource",
                "resource_kind": "resource",
                "resource_id": name,
            }
        },
    )
    _audit(d, name, audit, [1, 2])
    enabled.close()
    return audit


def _python(d, name, expected, audit):
    script = d.directory / "installed-admission-client.py"
    script.write_text(PYTHON_SCENARIO)
    snapshot = d.directory / "public-admission-expected.json"
    snapshot.write_text(json.dumps({"snapshot": expected, "audit": audit}))
    result = subprocess.run(
        [
            str(d.python_client),
            "-I",
            str(script),
            d.server_url,
            str(d.operator),
            d.scope["tenant_id"],
            d.scope["namespace"],
            name,
            str(snapshot),
        ],
        env=d.environment,
        capture_output=True,
        check=False,
        timeout=60,
    )
    (d.directory / "installed-admission-client.stdout").write_bytes(result.stdout)
    (d.directory / "installed-admission-client.stderr").write_bytes(result.stderr)
    assert result.returncode == 0, (
        "installed admission client failed; inspect retained diagnostics"
    )
    return json.loads(result.stdout)


def _console(d, expected):
    occurrence = expected["occurrence"]
    query = urlencode({"workflow_id": occurrence["workflow_id"], "limit": 50})
    status, headers, graph = d.request("GET", "/v1/console/workflows/explorer?" + query,
                                       credentials=d.operator)
    assert status == 200
    assert {name.lower(): value for name, value in headers.items()}["ledgence-console-contract"] == "14"
    assert graph["page"]["next_cursor"] is None
    nodes = graph["page"]["items"]
    waits = [node for node in nodes if node["kind"] == "admission_wait"]
    operations = [node for node in nodes if node["kind"] == "admission_operation"]
    assert len(waits) == len(operations) == 1
    wait, operation = waits[0], operations[0]
    assert wait["occurrence"] == operation["occurrence"] == occurrence
    assert wait["activation_id"] == occurrence["origin_activation_id"]
    assert wait["resumed_activation_id"] == operation["activation_id"] == expected["source"]["owner"]["task_id"]
    assert operation["source_id"] == expected["source"]["source_id"]
    assert operation["accepting_attempt_id"] == expected["source"]["owner"]["attempt_id"]
    assert operation["accepted_at"] == expected["finish"]["accepted_at"]
    assert {edge["kind"] for edge in wait["relations"]} == {"registers", "resumes"}
    assert [edge["kind"] for edge in operation["relations"]] == ["invokes"]
    assert all("arguments" not in node and "output" not in node for node in nodes)
    inspected = d.checked("POST", "/v1/console/admissions/inspect",
                          {key: value for key, value in occurrence.items() if key != "scope"})
    assert inspected == expected
    (d.directory / "console-admission-graph.json").write_text(json.dumps(graph, indent=2) + "\n")


def public_surfaces(d, workflow_id):
    """Use a completed real callback as the inspection oracle; no additional work."""
    expected = d.admission(workflow_id)
    assert d.status(workflow_id)["state"] == "succeeded"
    assert expected["state"] == "finished" and expected["finish"] is not None
    assert expected["source"] == expected["finish"]["source"]
    _console(d, expected)
    # All mutations are confined to an unused, disabled policy in this owned DB.
    name = "acceptance-public-" + uuid.uuid4().hex
    resource = _cli(d, name, expected)
    audit = _mcp(d, resource, expected)
    python = _python(d, name, expected, audit)
    assert d.admission(workflow_id) == expected, (
        "inspection changed accepted callback facts"
    )
    return {
        "workflow_id": workflow_id,
        "resource": name,
        "console_c9_occurrence_and_relations": True,
        "cli_saved_replay": True,
        "cli_destination_and_scope_rejected": True,
        "mcp_default_writes_hidden": True,
        "mcp_explicit_put_replayed": True,
        "installed_client": python,
    }
