"""Real provider operations with independent HTTP/effect evidence (MIT)."""

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import subprocess

from http_acceptance.harness import eventually
from mcp_acceptance.harness import McpSession
from operator_audit.scenarios import install_fault, remove_fault
from workflow_acceptance.owned_scenarios import quote
from .harness import CLAIM, REPORT, PROBE_REPORT, OBSERVE, provider_class


INBOX = "/v1/workflows/approved-effects/inbox"
INSPECT = "/v1/workflows/approved-effects/inspect"

CANCELLED_PUBLIC_FIELDS = (
    "occurrence",
    "provider",
    "database",
    "late_ingress",
    "caller",
    "no_managed_late_report",
    "no_revival",
)


def public_cancelled(value):
    return {name: value[name] for name in CANCELLED_PUBLIC_FIELDS}


def census(provider, *, posts=1, effects=1):
    value = provider.census()
    assert value["mutation_http_requests"] == posts, (
        "reference adapter repeated a mutation HTTP request"
    )
    assert value["committed_effects"] == effects, (
        "provider committed an unexpected number of effects"
    )
    return value


def public_surfaces(d, occurrence, interpreter):
    identity = {
        key: occurrence[key] for key in ("workflow_id", "activation_id", "approval_key")
    }
    query = {
        "scope": d.scope,
        "filter": "all",
        "provider_filter": "succeeded",
        "workflow_id": occurrence["workflow_id"],
        "cursor": None,
        "limit": 50,
    }
    metadata = d.checked("POST", INBOX, query, credentials=d.reader)
    assert (
        len(metadata["items"]) == 1
        and metadata["items"][0]["provider"]["knowledge"] == "succeeded"
    )
    assert not {"target", "result", "arguments", "credential_reference"}.intersection(
        metadata["items"][0]["provider"]
    )
    inspect = {"occurrence": occurrence, "cursor": None, "limit": 4}
    assert d.request("POST", INSPECT, inspect)[0] == 401
    d.checked("POST", INSPECT, inspect, credentials=d.reader, expected=403)
    install_fault(d)
    try:
        status, _, denied = d.request("POST", INSPECT, inspect, credentials=d.operator)
        assert status == 503 and "binding" not in denied
    finally:
        remove_fault(d)
    expected = d.inspect(occurrence)
    console = d.checked(
        "POST",
        "/v1/console/workflows/approved-effects/inspect",
        {**identity, "cursor": None, "limit": 4},
    )
    assert console["summary"]["effect_revision"] == str(
        expected["summary"]["effect_revision"]
    )
    assert console["summary"]["provider"]["reconciliation"]["generation"] == str(
        expected["summary"]["provider"]["reconciliation"]["generation"]
    )
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
    cli = d.command(
        "ledgence",
        [
            "effect",
            "inspect",
            *args,
            "--workflow",
            identity["workflow_id"],
            "--activation",
            identity["activation_id"],
            "--key",
            identity["approval_key"],
        ],
    )
    assert cli == expected
    assert (
        d.command(
            "ledgence",
            [
                "effect",
                "inbox",
                *args,
                "--workflow",
                identity["workflow_id"],
                "--provider-filter",
                "succeeded",
            ],
        )
        == metadata
    )
    session = McpSession(
        d, "provider-inspection", credentials_file=d.operator, read_only=True
    )
    d.sessions.append(session)
    catalog = session.request("tools/list", {})["result"]["tools"]
    assert len(catalog) == 14 and "ledgence_approval_decide" not in {
        item["name"] for item in catalog
    }
    view = session.tool("ledgence_effect_inspect", identity)
    assert (
        view["provider"]["current_report"]["report"]
        == expected["provider"]["current_report"]["report"]
    )
    assert view["provider"]["current_report"]["recorded_revision"] == str(
        expected["provider"]["current_report"]["recorded_revision"]
    )
    assert (
        session.tool(
            "ledgence_effect_inbox",
            {"workflow_id": identity["workflow_id"], "provider_filter": "succeeded"},
        )["items"][0]["provider"]["knowledge"]
        == "succeeded"
    )
    approved = session.tool(
        "ledgence_approval_inspect",
        {"workflow_id": identity["workflow_id"], "key": identity["approval_key"]},
    )
    reviewed_binding = expected["binding"]["approval"]["provider_binding"]
    assert approved["provider_binding"]["program"]["size"] == str(
        reviewed_binding["program"]["size"]
    )
    assert (
        approved["action"]["arguments"]
        == expected["binding"]["approval"]["action"]["arguments"]
    )
    _, all_entries = d.all_entries(occurrence)
    scheduled = next(
        entry for entry in all_entries if entry["kind"] == "provider_probe_scheduled"
    )
    probe_status = session.tool(
        "ledgence_task_status", {"task_id": scheduled["task_id"]}
    )
    assert probe_status["provider_probe"] == {
        "occurrence": occurrence,
        "probe_id": scheduled["probe_id"],
        "generation": str(scheduled["generation"]),
    }
    session.close()
    script = d.directory / "installed-provider-client.py"
    script.write_text("""import asyncio
import json
import sys

from ledgence.client import AsyncClient, ProviderFilter, ProviderReportEntry, ProviderSucceeded


async def main():
    server, credentials, tenant, namespace, workflow, activation, key = sys.argv[1:]
    async with AsyncClient(
        server, tenant=tenant, namespace=namespace, credentials_file=credentials
    ) as client:
        page = await client.effects.inbox(
            provider_filter=ProviderFilter.SUCCEEDED, workflow_id=workflow
        )
        assert len(page.items) == 1
        detail = await client.effects.inspect(
            workflow_id=workflow, activation_id=activation, approval_key=key
        )
        report = detail.provider.current_report
        assert isinstance(report, ProviderReportEntry)
        assert isinstance(report.report.knowledge, ProviderSucceeded)
        print(json.dumps({
            "knowledge": report.report.knowledge.kind,
            "result": report.report.knowledge.result,
        }))


asyncio.run(main())
""")
    result = subprocess.run(
        [
            str(interpreter),
            "-I",
            str(script),
            d.server_url,
            str(d.operator),
            d.scope["tenant_id"],
            d.scope["namespace"],
            identity["workflow_id"],
            identity["activation_id"],
            identity["approval_key"],
        ],
        env=d.environment,
        capture_output=True,
        timeout=60,
    )
    (d.directory / "installed-provider-client.stdout").write_bytes(result.stdout)
    (d.directory / "installed-provider-client.stderr").write_bytes(result.stderr)
    assert result.returncode == 0, "installed provider client failed"
    return {
        "cli": True,
        "mcp": True,
        "installed_client": json.loads(result.stdout),
        "sensitive_audit_failure_closed": True,
    }


def pending_capacity(d, interpreter):
    Provider = provider_class(d.root)
    with Provider(d.directory / "pending-provider", completion_delay=5) as provider:
        occurrence, paths = d.begin("provider-pending", provider)
        d.parked(occurrence)
        witnesses = [
            d.witness("provider-capacity-main", d.queue),
            d.witness("provider-capacity-probe", d.probe_queue),
        ]
        wake = d.finished(occurrence, paths, "provider_succeeded")
        page, entries = d.all_entries(occurrence)
        assert any(entry["kind"] == "provider_probe_source" for entry in entries)
        assert page["summary"]["provider"]["handling"] == "provider_terminal"
        assert d.counts(occurrence)["local_records"] == 0
        output = wake["selection"]["result"]
        artifact = provider.directory / output["artifact"]
        import hashlib

        assert hashlib.sha256(artifact.read_bytes()).hexdigest() == output["sha256"]
        surfaces = public_surfaces(d, occurrence, interpreter)
        return {
            "occurrence": occurrence,
            "witness_tasks": witnesses,
            "provider": census(provider),
            "database": d.counts(occurrence),
            "public_surfaces": surfaces,
            "wake_count": 1,
        }


def loss_pending_expiry(d, flow_proxy, probe_proxy):
    Provider = provider_class(d.root)
    with Provider(
        d.directory / "lost-response-provider", completion_delay=3
    ) as provider:
        key = "provider-lost-response"
        provider.set_fault(key, post_response="drop", hidden_gets=1, key_lifetime=0.1)
        lost_report = probe_proxy.lose_once(
            PROBE_REPORT,
            lambda command: command["ticket"]["occurrence"]["approval_key"] == "export",
        )
        occurrence, paths = d.begin(key, provider)
        wake = d.finished(occurrence, paths, "provider_succeeded")
        assert lost_report.is_set(), "report reply loss was never exercised"
        commands = probe_proxy.commands(
            PROBE_REPORT, lambda command: command["ticket"]["occurrence"] == occurrence
        )
        duplicates = [
            (first, second)
            for first, second in zip(commands, commands[1:])
            if first["json"]["report_id"] == second["json"]["report_id"]
        ]
        assert len(duplicates) == 1, (
            "report response loss did not cause one frozen replay"
        )
        first, second = duplicates[0]
        assert (
            first["body"] == second["body"]
            and first["status"] == second["status"] == 200
        )
        page, entries = d.all_entries(occurrence)
        probe_sources = [
            entry for entry in entries if entry["kind"] == "provider_probe_source"
        ]
        probe_reports = [
            entry
            for entry in entries
            if entry["kind"] == "provider_report" and entry["source"]["kind"] == "probe"
        ]
        counts = census(provider)
        assert counts["status_gets"] == len(probe_sources) == len(probe_reports), (
            "report retry reinvoked GET or fabricated a source"
        )
        assert any(
            entry["report"]["knowledge"]["kind"] == "unknown" for entry in probe_reports
        )
        assert wake["selection"]["result"]["artifact"]
        assert all(provider.now() > op["dedup_until"] for op in counts["operations"]), (
            "provider key did not actually expire"
        )
        return {
            "occurrence": occurrence,
            "provider": counts,
            "database": d.counts(occurrence),
            "probe_report_requests": len(commands),
            "accepted_probe_reports": len(probe_reports),
            "frozen_report_retry": True,
            "provider_key_expired_without_resend": True,
        }


def lost_claim_and_no_lookup(d, flow_proxy):
    Provider = provider_class(d.root)
    results = []
    for lookup in (True, False):
        key = "provider-lost-claim" if lookup else "provider-no-lookup"
        with Provider(d.directory / (key + "-service")) as provider:
            lost = flow_proxy.lose_once(
                CLAIM,
                lambda command, key=key: command["binding"]["business"]["key"] == key,
            )
            occurrence, paths = d.begin(key, provider, lookup=lookup, attempts=2)
            wake = d.finished(occurrence, paths, "needs_review")
            assert lost.is_set()
            claims = flow_proxy.commands(
                CLAIM,
                lambda command: command["workflow_id"] == occurrence["workflow_id"],
            )
            assert (
                len(claims) >= 2
                and json.loads(claims[0]["response"])["kind"] == "dispatch"
            )
            assert all(
                json.loads(record["response"])["kind"] == "existing"
                for record in claims[1:]
            )
            counts = census(provider, posts=0, effects=0)
            state = d.inspect(occurrence)["summary"]["provider"]
            if lookup:
                assert (
                    state["reconciliation"]["checks_used"] == 4
                    and counts["status_gets"] == 4
                )
                assert wake["selection"]["reason"] == "budget_exhausted"
            else:
                assert (
                    state["reconciliation"]["checks_used"] == counts["status_gets"] == 0
                )
                assert wake["selection"]["reason"] == "lookup_unavailable"
            results.append(
                {
                    "occurrence": occurrence,
                    "provider": counts,
                    "database": d.counts(occurrence),
                    "reason": wake["selection"]["reason"],
                }
            )
    return {"cases": results}


def concurrent_business(d):
    Provider = provider_class(d.root)
    with Provider(d.directory / "concurrent-provider", completion_delay=2) as provider:
        approvals = [
            d.request_approval(
                "provider-concurrent-" + str(n),
                provider,
                business_key="shared-business",
            )
            for n in range(2)
        ]
        # Two real workflow workers can race the bind-and-grant transaction.
        with ThreadPoolExecutor(max_workers=2) as pool:
            occurrences = list(
                pool.map(
                    lambda item: d.approve(item[1][0], "concurrent-" + str(item[0])),
                    enumerate(approvals),
                )
            )
        for occurrence in occurrences:
            eventually(
                lambda occurrence=occurrence: (
                    d.status(occurrence["workflow_id"])["state"] == "succeeded"
                ),
                timeout=90,
                description="duplicate owner or explicit existing-operation reference",
            )
        outcomes = [
            d.checked(
                "GET",
                d.query("/v1/workflows/result", workflow_id=occurrence["workflow_id"]),
            )["outcome"]["output"]
            for occurrence in occurrences
        ]
        assert sorted(value["status"] for value in outcomes) == [
            "existing_operation",
            "succeeded",
        ]
        assert census(provider)["committed_effects"] == 1
        changed, changed_paths = d.begin(
            "provider-conflicting-binding",
            provider,
            business_key="shared-business",
            dataset="different",
        )
        eventually(
            lambda: d.status(changed["workflow_id"])["state"] == "failed",
            timeout=60,
            description="changed business binding rejected",
        )
        assert not d.wakes(changed_paths)
        separate, separate_paths = d.begin(
            "provider-distinct-business", provider, dataset="customers"
        )
        d.finished(separate, separate_paths, "provider_succeeded")
        reused, reused_paths = d.begin(
            "provider-reused-domain-key",
            provider,
            overrides={"provider_key": "shared-business"},
        )
        eventually(
            lambda: d.status(reused["workflow_id"])["state"] == "failed",
            timeout=60,
            description="distinct business cannot reuse the same provider account key",
        )
        assert not d.wakes(reused_paths)
        independent, independent_paths = d.begin(
            "provider-distinct-account",
            provider,
            overrides={"provider_key": "shared-business", "provider_account": "other"},
        )
        d.finished(independent, independent_paths, "provider_succeeded")
        return {
            "workflows": [item["workflow_id"] for item in occurrences],
            "changed_binding": changed,
            "domain_key_reuse_rejected": reused,
            "distinct_provider_account": independent,
            "distinct_business": separate,
            "provider": census(provider, posts=3, effects=3),
        }


def typed_dispute_before_selection(d, flow_proxy):
    Provider = provider_class(d.root)
    with Provider(d.directory / "dispute-provider", completion_delay=0.1) as provider:
        key = "provider-typed-dispute"
        provider.set_fault(
            key,
            post_terminal_reply=True,
            lookup_result={
                "artifact": "contradictory.csv",
                "sha256": "0" * 64,
                "rows": 3,
            },
        )
        provider.release_reply.clear()
        occurrence, paths = d.begin(key, provider, before=True)
        eventually(
            lambda: d.counts(occurrence)["reports"] >= 1,
            timeout=4,
            description="read-only probe reports a terminal fact while original dispatch waits",
        )
        provider.release_reply.set()
        eventually(
            lambda: Path(paths["before_park"]).exists(),
            timeout=20,
            description="original callback completed before park",
        )
        state = d.inspect(occurrence)["summary"]["provider"]
        assert (
            state["knowledge"] == "disputed" and len(state["conflicting_reports"]) == 2
        )
        assert not d.wakes(paths)
        Path(paths["release_park"]).touch()
        wake = d.finished(occurrence, paths, "disputed")
        # Existing B assessment records handling; it cannot rewrite that wake.
        detail = d.inspect(occurrence)
        command = {
            "occurrence": occurrence,
            "resolution_id": "provider-dispute-assessment",
            "expected_effect_revision": detail["summary"]["effect_revision"],
            "supersedes_resolution_id": None,
            "action": detail["binding"]["approval"]["action"],
            "assessment": {"kind": "close_unknown"},
            "reason": "Preserved both contradictory synthetic provider facts",
            "evidence_references": [],
        }
        accepted = d.checked("POST", "/v1/workflows/approved-effects/resolve", command)
        assert d.wakes(paths) == [wake]
        after = d.inspect(occurrence)["summary"]["provider"]
        assert (
            after["handling"] == "operator_closed" and after["knowledge"] == "disputed"
        )
        return {
            "occurrence": occurrence,
            "provider": census(provider),
            "database": d.counts(occurrence),
            "conflicting_reports": state["conflicting_reports"],
            "wake_count": 1,
            "assessment_revision": accepted["accepted_revision"],
        }


def cancelled_retained_source(d, flow_proxy):
    from .harness import fixture_source

    Provider = provider_class(d.root)
    retained = d.directory / "cancelled-frozen-provider-report.json"
    release = d.directory / "cancelled-release-report"
    d.gates.add(release)
    d.publish(
        "provider-held-report",
        "1.0.0",
        runtime_protocol=3,
        program_source=fixture_source(
            d.root, retained_report=retained, release_report=release
        ),
    )
    with Provider(d.directory / "cancel-provider", completion_delay=0.1) as provider:
        key = "provider-cancelled-report"
        occurrence, paths = d.begin(key, provider, program="provider-held-report")
        eventually(
            retained.exists,
            timeout=30,
            description="actual registered callback retained its frozen classified report",
        )
        (claim,) = flow_proxy.commands(
            CLAIM, lambda command: command["workflow_id"] == occurrence["workflow_id"]
        )
        source = json.loads(claim["response"])["source"]
        d.cancel_workflow(occurrence)
        eventually(
            lambda: d.status(occurrence["workflow_id"])["state"] == "cancelled",
            timeout=90,
            description="cancelled provider workflow drained owned callback/probe",
        )
        assert not flow_proxy.commands(
            REPORT, lambda command: command["ticket"]["occurrence"] == occurrence
        )
        report = json.loads(retained.read_text())
        command = {
            "ticket": source["ticket"],
            "source_id": source["source_id"],
            "report_id": source["report_id"],
            "report": report,
        }
        before = d.status(occurrence["workflow_id"])
        count = d.counts(occurrence)["scheduled"]
        subject = source["worker"]["subject"]
        d.checked("POST", OBSERVE, command, credentials=d.reader, expected=403)
        d.checked(
            "POST",
            OBSERVE,
            command,
            credentials=d.workers["late-outsider"],
            expected=403,
        )
        accepted = d.checked("POST", OBSERVE, command, credentials=d.workers[subject])
        receipt = accepted["receipt"]
        assert (
            receipt["accepted_via"] == "evidence_only"
            and not receipt["already_accepted"]
        )
        replay = d.checked("POST", OBSERVE, command, credentials=d.workers[subject])
        assert replay["receipt"]["already_accepted"]
        assert d.status(occurrence["workflow_id"]) == before and not d.wakes(paths)
        assert (
            d.counts(occurrence)["scheduled"] == count and d.journal(occurrence) == ""
        )
        return {
            "occurrence": occurrence,
            "paths": paths,
            "command": command,
            "subject": subject,
            "provider": census(provider),
            "database": d.counts(occurrence),
            "late_ingress": receipt["accepted_via"],
            "caller": "independent delivery of the actual retained callback result",
            "no_managed_late_report": True,
            "no_revival": True,
        }


def worker_replacement(d):
    """Kill the real dispatch worker after the provider commits; replacement only looks up."""
    d.stop_workers()
    Provider = provider_class(d.root)
    proxy = d.authenticated_proxy()
    original = d.worker("crash-original", proxy)
    with Provider(d.directory / "replacement-provider", completion_delay=2) as provider:
        provider.release_reply.clear()
        occurrence, paths = d.begin("provider-worker-replacement", provider, attempts=2)
        assert provider.accepted.wait(30), "provider never committed before worker kill"
        original.kill()
        provider.release_reply.set()
        replacement = d.worker("crash-replacement", proxy)
        probe_proxy = d.authenticated_proxy()
        probe = d.worker("late-origin", probe_proxy, queue=d.probe_queue)
        d.finished(occurrence, paths, "provider_succeeded")
        _, entries = d.all_entries(occurrence)
        claims = proxy.commands(
            CLAIM, lambda command: command["workflow_id"] == occurrence["workflow_id"]
        )
        assert (
            len(claims) >= 2 and json.loads(claims[0]["response"])["kind"] == "dispatch"
        )
        assert all(
            json.loads(item["response"])["kind"] == "existing" for item in claims[1:]
        )
        dispatch = next(
            entry for entry in entries if entry["kind"] == "provider_dispatch"
        )
        probes = [
            entry for entry in entries if entry["kind"] == "provider_probe_source"
        ]
        assert dispatch["worker"]["subject"] == "crash-original" and probes
        assert all(entry["worker"]["subject"] != "crash-original" for entry in probes)
        census_value = census(provider)
        original_id = claims[0]["json"]["owner"]["attempt_id"]
        assert all(
            item["json"]["owner"]["attempt_id"] != original_id for item in claims[1:]
        )
        return {
            "occurrence": occurrence,
            "provider": census_value,
            "database": d.counts(occurrence),
            "real_worker_killed_after_provider_commit": True,
            "replacement_only_recovered": True,
        }


def retained_after_collection(d, retained):
    occurrence, paths = retained["occurrence"], retained["paths"]
    command = d.resolution(
        occurrence,
        "provider-retention-assessment",
        {"kind": "close_unknown"},
        references=(),
    )
    accepted = d.resolve(command)
    before, entries = d.all_entries(occurrence)
    d.stop_workers()
    d.server.stop()
    workflow = quote(occurrence["workflow_id"])
    age = "(extract(epoch FROM clock_timestamp())*1000)::bigint-91*86400000::bigint"
    d.sql(
        "UPDATE workflow_runs SET terminal_at_ms="
        + age
        + " WHERE workflow_id="
        + workflow
    )
    d.sql(
        "UPDATE tasks SET terminal_at_ms="
        + age
        + " WHERE workflow_id="
        + workflow
        + " AND terminal_at_ms IS NOT NULL"
    )
    d.sql(
        "UPDATE worker_sessions SET expires_at_ms="
        + age
        + " WHERE NOT EXISTS(SELECT 1 FROM attempts WHERE worker_session_id=worker_sessions.session_id AND state='active')"
    )
    for _ in range(3):
        d.command(
            "ledgence",
            [
                "orchestrator",
                "retain",
                "--tenant",
                d.scope["tenant_id"],
                "--namespace",
                d.scope["namespace"],
                "--retain-days",
                "90",
                "--batch-size",
                "128",
                "--batches",
                "100",
                "--apply",
            ],
        )
        if (
            d.sql("SELECT count(*) FROM workflow_runs WHERE workflow_id=" + workflow)
            == "0"
        ):
            break
    if d.sql("SELECT count(*) FROM workflow_runs WHERE workflow_id=" + workflow) != "0":
        diagnostics = {
            name: int(
                d.sql("SELECT count(*) FROM " + name + " WHERE workflow_id=" + workflow)
            )
            for name in (
                "workflow_runs",
                "workflow_provider_waits",
                "workflow_work",
                "workflow_activations",
                "tasks",
            )
        }
        (d.directory / "provider-retention-blockers.json").write_text(
            json.dumps(diagnostics, indent=2) + "\n"
        )
        raise AssertionError("provider work prevented ordinary execution collection")
    assert (
        d.sql(
            "SELECT count(*) FROM workflow_provider_waits WHERE workflow_id=" + workflow
        )
        == "0"
    )
    d.start_server()
    after, retained_entries = d.all_entries(occurrence)
    assert after == before and retained_entries == entries
    assert d.resolve(command) == dict(accepted, already_accepted=True)
    assert d.checked(
        "POST", OBSERVE, retained["command"], credentials=d.workers[retained["subject"]]
    )["receipt"]["already_accepted"]
    assert not d.wakes(paths) and d.journal(occurrence) == ""
    return {
        "occurrence": occurrence,
        "execution_collected": True,
        "retained_evidence_unchanged": True,
        "assessment_and_observation_replay": True,
    }


def snapshot_database(executable, database_url, destination):
    """Stream bytes across local or container CLI boundaries, never container paths."""
    with destination.open("wb") as stream:
        result = subprocess.run(
            [str(executable), "--dbname", database_url, "--format=custom"],
            stdout=stream,
            stderr=subprocess.PIPE,
            timeout=60,
        )
    assert result.returncode == 0, "owned database snapshot failed"


def restore_database(executable, database_url, source):
    with source.open("rb") as stream:
        result = subprocess.run(
            [
                str(executable),
                "--dbname",
                database_url,
                "--clean",
                "--if-exists",
                "--exit-on-error",
            ],
            stdin=stream,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=60,
        )
    assert result.returncode == 0, "owned database restore failed"


def declared_restore(d):
    """Rollback this owned DB only; provider effects remain outside that snapshot."""
    Provider = provider_class(d.root)
    dump = Path(d.psql).with_name("pg_dump")
    restore = Path(d.psql).with_name("pg_restore")
    assert dump.is_file() and restore.is_file(), (
        "pg_dump and pg_restore must accompany --psql"
    )
    snapshot = d.directory / "declared-restore.dump"
    d.stop_workers()
    d.pair()
    with (
        Provider(
            d.directory / "restore-existing-provider", completion_delay=0.1
        ) as existing_provider,
        Provider(
            d.directory / "restore-missing-provider", completion_delay=0.1
        ) as missing_provider,
    ):
        existing_provider.set_fault("restore-existing", never_complete=True)
        existing, existing_paths = d.begin("restore-existing", existing_provider)
        d.parked(existing)
        d.stop_workers()
        d.server.stop()
        snapshot_database(dump, d.database_url, snapshot)
        d.start_server()
        d.pair()
        missing, missing_paths = d.begin("restore-missing", missing_provider)
        d.finished(missing, missing_paths, "provider_succeeded")
        d.stop_workers()
        d.server.stop()
        restore_database(restore, d.database_url, snapshot)
        assert (
            d.sql(
                "SELECT count(*) FROM workflow_runs WHERE workflow_id="
                + quote(missing["workflow_id"])
            )
            == "0"
        )
        d.set_policy("declared_restore_quarantine")
        existing_provider.set_fault("restore-existing", never_complete=False)
        d.start_server()
        d.pair()
        d.finished(existing, existing_paths, "provider_succeeded")
        blocked, blocked_paths = d.begin(
            "restore-new-absent-key", missing_provider, business_key="restore-missing"
        )
        eventually(
            lambda: d.status(blocked["workflow_id"])["state"] == "failed",
            timeout=60,
            description="quarantine denies a dispatch grant absent from restored ledger",
        )
        assert not d.wakes(blocked_paths)
        assert (
            d.sql(
                "SELECT count(*) FROM provider_operations WHERE workflow_id="
                + quote(blocked["workflow_id"])
            )
            == "0"
        )
        return {
            "restored_existing": existing,
            "absent_after_restore": missing,
            "quarantine_blocked": blocked,
            "existing_provider": census(existing_provider),
            "missing_provider": census(missing_provider),
            "read_only_recovery_under_quarantine": True,
            "snapshot_restored": True,
        }


def typed_outcomes(d):
    Provider = provider_class(d.root)
    outcomes = []
    for terminal, selection in (
        ("failed", "provider_failed"),
        ("cancelled", "provider_cancelled"),
        ("requires_action", "needs_review"),
    ):
        key = "provider-terminal-" + terminal
        with Provider(
            d.directory / (key + "-service"), completion_delay=0.05
        ) as provider:
            provider.set_fault(key, terminal=terminal)
            occurrence, paths = d.begin(key, provider)
            wake = d.finished(occurrence, paths, selection)
            state = d.inspect(occurrence)["summary"]["provider"]
            assert state["knowledge"] == terminal
            if terminal == "requires_action":
                assert wake["selection"]["reason"] == "action_required"
            outcomes.append(
                {
                    "occurrence": occurrence,
                    "knowledge": terminal,
                    "selection": selection,
                    "provider": census(provider),
                }
            )
    return {"outcomes": outcomes}


def old_worker_before_provider_callback(d, old_binary, old_runner):
    """An actual prior binary/helper receives a marked check, never an application call."""
    d.stop_workers()
    flow = d.worker("late-origin", d.authenticated_proxy())
    Provider = provider_class(d.root)
    with Provider(
        d.directory / "old-worker-provider", completion_delay=0.05
    ) as provider:
        occurrence, paths = d.begin("provider-old-worker", provider)
        d.parked(occurrence)
        eventually(
            lambda: d.counts(occurrence)["scheduled"] >= 1,
            timeout=15,
            description="dedicated provider check queued",
        )
        old_proxy = d.authenticated_proxy()
        old = d.worker(
            "late-outsider",
            old_proxy,
            queue=d.probe_queue,
            binaries=old_binary.parent,
            runner=old_runner,
        )

        def assigned():
            records = [
                json.loads(record["response"])
                for record in old_proxy.commands("/v1/acquisitions")
            ]
            matches = [
                record["assignment"]
                for record in records
                if record.get("disposition") == "assigned"
            ]
            if matches:
                return matches[0]
            assert old.process.poll() is None, (
                "old worker failed discovery before reaching the marked task"
            )

        assignment = eventually(
            assigned,
            timeout=30,
            description="actual old worker received the marked provider check",
        )
        assert assignment["provider_probe"]["occurrence"] == occurrence
        task_id = assignment["lease"]["owner"]["task_id"]
        eventually(
            lambda: d.task(task_id)["state"] == "failed",
            timeout=90,
            description="old runtime rejected the marked task before provider callback",
        )
        if old.process.poll() is None:
            old.stop()
        assert provider.census()["status_gets"] == 0
        assert not old_proxy.commands("/v1/provider-probes/context")
        assert not [
            e
            for e in d.all_entries(occurrence)[1]
            if e["kind"] == "provider_probe_source"
        ]
        new = d.worker(
            "crash-replacement", d.authenticated_proxy(), queue=d.probe_queue
        )
        d.finished(occurrence, paths, "provider_succeeded")
        return {
            "occurrence": occurrence,
            "provider": census(provider),
            "old_binary_rejected_before_callback": True,
        }


def bounded_probe_timeout(d):
    Provider = provider_class(d.root)
    with Provider(
        d.directory / "probe-timeout-provider", completion_delay=0.05
    ) as provider:
        key = "provider-probe-timeout"
        provider.set_fault(key, lookup_delay=3)
        occurrence, paths = d.begin(key, provider)
        wake = d.finished(occurrence, paths, "needs_review")
        assert wake["selection"]["reason"] == "budget_exhausted"
        _, entries = d.all_entries(occurrence)
        finished = [
            entry for entry in entries if entry["kind"] == "provider_probe_finished"
        ]
        assert len(finished) == 4
        assert all(
            entry["completion"]
            == {"kind": "unreported", "failure": "invocation_timeout"}
            for entry in finished
        )
        counts = census(provider)
        assert counts["status_gets"] == 4
        witness = d.witness("after-provider-probe-timeout", d.probe_queue)
        return {
            "occurrence": occurrence,
            "provider": counts,
            "bounded_checks": 4,
            "ordinary_witness_after_cleanup": witness,
        }
