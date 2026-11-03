"""Real native worker recovery and a separate historical authenticated caller (MIT)."""
import json
import subprocess
from urllib.parse import urlencode

from http_acceptance.harness import eventually, exchange
from mcp_acceptance.harness import McpSession
from operator_audit.scenarios import install_fault, remove_fault
from workflow_acceptance.owned_scenarios import quote
from workflow_acceptance.refund_provider import RefundProvider
from workflow_acceptance.refund_scenarios import parked_state, runtime_alive
from .harness import CLAIM, CONFIRM, OBSERVE, INBOX, INSPECT


RECOVERY_REPORT_FIELDS = ("occurrence", "provider", "database", "worker_subjects", "apply_attempts",
                          "lost_confirmation_recovery", "confirmation_requests", "receipt")


def public_recovery(recovery):
    """Project an explicit report; frozen lease commands stay in process memory."""
    return {name: recovery[name] for name in RECOVERY_REPORT_FIELDS}


def lost_claim(d):
    key = "evidence-lost-claim"
    with RefundProvider(d.directory / (key + "-provider.jsonl"), cooperative=True) as provider:
        proxy = d.authenticated_proxy()
        lost = proxy.lose_once(CLAIM)
        worker = d.worker("lost-grant", proxy)
        occurrence = d.start_workflow(key, provider)
        assert lost.wait(60), "first v2 grant never reached proxy"
        parked = eventually(lambda: parked_state(d, occurrence["workflow_id"]), timeout=130,
                            description="lost claim retries as lookup and parks unknown")
        worker.stop()
        claims = proxy.commands(CLAIM)
        assert len(claims) == 2 and all(record["status"] == 200 for record in claims)
        replies = [json.loads(record["response"]) for record in claims]
        assert [value["kind"] for value in replies] == ["dispatch", "lookup_only"]
        assert replies[0]["claim"]["attempt_id"] != replies[1]["claim"]["attempt_id"]
        assert not proxy.commands(CONFIRM)
        counts = d.counts(occurrence)
        assert counts == {"claims": 2, "observations": 0, "local_records": 0, "state": "unconfirmed", "revision": 3}, counts
        census = provider.census()
        assert census == {"post_requests": 0, "lookup_requests": 1, "refunds": []}, census
        assert parked["checkpoint"] == {"state": "unknown", "effect_key": key}
        page, entries = d.all_entries(occurrence)
        assert [entry["worker"]["subject"] for entry in entries] == ["lost-grant"] * 2
        d.no_private_fields(page)
        d.cancel_workflow(occurrence)
        eventually(lambda: d.status(occurrence["workflow_id"])["state"] == "cancelled", description="cancel unresolved fixture")
        return {"occurrence": occurrence, "provider": census, "database": counts,
                "claim_reply_kinds": [value["kind"] for value in replies]}


def worker_replacement(d):
    key = "evidence-replacement"
    with RefundProvider(d.directory / (key + "-provider.jsonl"), cooperative=True) as provider:
        original_proxy = d.authenticated_proxy()
        original = d.worker("crash-original", original_proxy)
        occurrence = d.start_workflow(key, provider)
        assert provider.accepted.wait(60), "provider did not durably accept the first POST"
        first, = d.apply_markers(key)
        assert d.counts(occurrence) == {"claims": 1, "observations": 0, "local_records": 0, "state": "unconfirmed", "revision": 2}
        session = original.process.pid
        assert original.process.poll() is None and runtime_alive(first["pid"], session)
        original.kill()  # Only the owned, unreaped Popen worker is signalled.
        provider.drop_reply()
        eventually(lambda: not runtime_alive(first["pid"], session), timeout=20,
                   description="owned Python runtime exits after worker transport closes")
        recovery_proxy = d.authenticated_proxy()
        lost = recovery_proxy.lose_once(CONFIRM)
        replacement = d.worker("crash-replacement", recovery_proxy)
        eventually(lambda: d.status(occurrence["workflow_id"])["state"] == "succeeded", timeout=160,
                   description="distinct replacement reconciles and accepted confirmation replays")
        assert lost.is_set(), "the confirmation response fault was not exercised"
        replacement.stop()
        first_claim, = original_proxy.commands(CLAIM)
        recovery_claim, = recovery_proxy.commands(CLAIM)
        confirmed, replayed = recovery_proxy.commands(CONFIRM)
        original_reply = json.loads(first_claim["response"])
        recovery_reply = json.loads(recovery_claim["response"])
        assert original_reply["kind"] == "dispatch" and recovery_reply["kind"] == "lookup_only"
        assert original_reply["claim"]["worker"]["subject"] == "crash-original"
        assert recovery_reply["claim"]["worker"]["subject"] == "crash-replacement"
        assert original_reply["claim"]["attempt_id"] != recovery_reply["claim"]["attempt_id"]
        assert original_reply["claim"]["worker"]["credential_id"] != recovery_reply["claim"]["worker"]["credential_id"]
        assert confirmed["status"] == replayed["status"] == 200
        assert confirmed["json"] == replayed["json"], "confirmation retry changed its frozen command"
        assert json.loads(confirmed["response"])["already_accepted"] is False
        assert json.loads(replayed["response"])["already_accepted"] is True
        markers = d.apply_markers(key)
        assert len(markers) == 2, "confirmation receipt replay must not rerun the application callback"
        counts = d.counts(occurrence)
        assert counts == {"claims": 2, "observations": 1, "local_records": 1, "state": "confirmed", "revision": 4}, counts
        census = provider.census()
        assert census["post_requests"] == census["lookup_requests"] == len(census["refunds"]) == 1, census
        page, entries = d.all_entries(occurrence)
        assert [entry["kind"] for entry in entries] == ["claim", "claim", "observation"]
        source = entries[-1]["source"]
        assert source["worker"]["subject"] == "crash-replacement"
        assert source["reported_callback"] == "lookup" and source["ingress"] == "live_confirm"
        assert entries[-1]["output"]["receipt"] == census["refunds"][0]
        # Read the standalone confirm receipt again with the original retained
        # command and still-current worker identity; it is not execution permission.
        receipt = d.checked("POST", CONFIRM, confirmed["json"], credentials=d.workers["crash-replacement"])
        assert receipt["already_accepted"] is True
        assert d.counts(occurrence) == counts
        d.no_private_fields(page)
        return {"occurrence": occurrence, "provider": census, "database": counts,
                "worker_subjects": ["crash-original", "crash-replacement"],
                "apply_attempts": len(markers), "confirmation_requests": 2,
                "lost_confirmation_recovery": "same_attempt_exact_confirm_receipt_replay",
                "retained_confirm_command": confirmed["json"], "retained_claim_command": recovery_claim["json"],
                "receipt": receipt}


def late_observation(d):
    key = "evidence-late-observe"
    with RefundProvider(d.directory / (key + "-provider.jsonl"), cooperative=True) as provider:
        proxy = d.authenticated_proxy()
        worker = d.worker("late-origin", proxy)
        occurrence = d.start_workflow(key, provider)
        assert provider.accepted.wait(60)
        claim_record, = proxy.commands(CLAIM)
        claim = json.loads(claim_record["response"])["claim"]
        assert claim["worker"]["subject"] == "late-origin"
        d.cancel_workflow(occurrence)
        provider.drop_reply()
        eventually(lambda: d.status(occurrence["workflow_id"])["state"] == "cancelled", timeout=90,
                   description="managed worker cancellation settles before independent observation")
        worker.stop()
        assert not proxy.commands(CONFIRM), "a cancelled runtime unexpectedly confirmed evidence"
        before = d.counts(occurrence)
        assert before == {"claims": 1, "observations": 0, "local_records": 0, "state": "unconfirmed", "revision": 2}
        workflow_before = d.status(occurrence["workflow_id"])
        work_before = d.sql("SELECT count(*) FROM workflow_work WHERE workflow_id=" + quote(occurrence["workflow_id"]))
        # This is a different real HTTP caller using the same verified stable
        # worker subject. It performs its own provider lookup after cancellation;
        # it does not claim the cancelled subprocess delivered a callback.
        status, _, receipt = exchange(provider.url, "GET", "/refunds?" + urlencode({"effect_key": key}))
        assert status == 200
        observed = {"occurrence": occurrence, "claim_id": claim["claim_id"],
                    "observation_id": "external-lookup-after-cancel", "reported_callback": "lookup",
                    "output": {"state": "confirmed", "receipt": receipt}}
        d.checked("POST", OBSERVE, observed, credentials=d.workers["late-outsider"], expected=409)
        d.checked("POST", OBSERVE, observed, credentials=d.reader, expected=403)
        accepted = d.checked("POST", OBSERVE, observed, credentials=d.workers["late-origin"])
        assert accepted["disposition"] == "evidence_only" and accepted["already_accepted"] is False
        assert accepted["worker"]["subject"] == "late-origin"
        again = d.checked("POST", OBSERVE, observed, credentials=d.workers["late-origin"])
        assert again == dict(accepted, already_accepted=True)
        d.checked("POST", OBSERVE, dict(observed, output={"different": True}), credentials=d.workers["late-origin"], expected=409)
        different = dict(observed, observation_id="external-differing-report", output={"unverified": "different"})
        d.checked("POST", OBSERVE, different, credentials=d.workers["late-origin"])
        counts = d.counts(occurrence)
        assert counts == {"claims": 1, "observations": 2, "local_records": 0, "state": "unconfirmed", "revision": 4}, counts
        assert d.status(occurrence["workflow_id"]) == workflow_before
        assert d.sql("SELECT count(*) FROM workflow_work WHERE workflow_id=" + quote(occurrence["workflow_id"])) == work_before
        assert d.journal(occurrence) == ""
        page, entries = d.all_entries(occurrence)
        assert page["summary"]["has_discrepancy"] is True
        assert [row["source"]["ingress"] for row in entries[1:]] == ["late_observe"] * 2
        assert [row["source"]["worker"]["subject"] for row in entries[1:]] == ["late-origin"] * 2
        assert provider.census()["post_requests"] == 1
        return {"occurrence": occurrence, "database": counts, "provider": provider.census(),
                "caller": "independent authenticated historical worker subject", "workflow_state": "cancelled",
                "journal_confirmed": False, "discrepancy": True, "foreign_subject_status": 409}


def read_surfaces(d, occurrence, interpreter):
    identity = {key: occurrence[key] for key in ("workflow_id", "activation_id", "approval_key")}
    inspect = {"occurrence": occurrence, "cursor": None, "limit": 4}
    metadata = {"scope": d.scope, "filter": "confirmed", "workflow_id": occurrence["workflow_id"], "cursor": None, "limit": 50}
    summary = d.checked("POST", INBOX, metadata, credentials=d.reader)
    assert len(summary["items"]) == 1
    d.no_private_fields(summary)
    assert "output" not in json.dumps(summary) and "arguments" not in json.dumps(summary)
    assert d.request("POST", INSPECT, inspect)[0] == 401
    d.checked("POST", INSPECT, inspect, credentials=d.reader, expected=403)
    console = dict(identity, cursor=None, limit=4)
    d.checked("POST", "/v1/console/workflows/approved-effects/inspect", console, credentials=d.reader, expected=403)
    install_fault(d)
    try:
        for path, body in ((INSPECT, inspect), ("/v1/console/workflows/approved-effects/inspect", console)):
            status, _, value = d.request("POST", path, body, credentials=d.operator)
            assert status == 503 and "binding" not in value and "entries" not in value
    finally:
        remove_fault(d)
    before = int(d.sql("SELECT count(*) FROM operator_audit_events WHERE event_kind='sensitive_response'"))
    expected = d.inspect(occurrence)
    d.no_private_fields(expected)
    console_result = d.checked("POST", "/v1/console/workflows/approved-effects/inspect", console)
    assert console_result["summary"]["effect_revision"] == str(expected["summary"]["effect_revision"])
    assert console_result["entries"][0]["sequence"] == "1"
    assert int(d.sql("SELECT count(*) FROM operator_audit_events WHERE event_kind='sensitive_response'")) == before + 2
    audit = d.checked("POST", "/v1/audit/events", {"scope": d.scope, "filter": {"kind": "resource",
        "resource_kind": "workflow", "resource_id": occurrence["workflow_id"]}, "cursor": None, "limit": 100}, credentials=d.reader)
    assert any(row["operation"].get("read") == "effect_inspect" and row["operation"]["resource"] == dict(kind="effect", **identity) for row in audit["items"])
    args = ["--server", d.server_url, "--tenant", d.scope["tenant_id"], "--namespace", d.scope["namespace"],
            "--credentials-file", str(d.operator)]
    cli = d.command("ledgence", ["effect", "inspect", *args, "--workflow", identity["workflow_id"],
        "--activation", identity["activation_id"], "--key", identity["approval_key"]])
    assert cli == expected
    inbox = d.command("ledgence", ["effect", "inbox", *args, "--workflow", identity["workflow_id"], "--filter", "confirmed"])
    assert inbox == summary
    session = McpSession(d, "effect-read-mcp", credentials_file=d.operator, read_only=True)
    d.sessions.append(session)
    assert session.tool("ledgence_effect_inspect", identity) == expected
    assert session.tool("ledgence_effect_inbox", {"workflow_id": identity["workflow_id"], "filter": "confirmed"}) == summary
    session.close()
    reader = McpSession(d, "effect-metadata-mcp", credentials_file=d.reader, read_only=True)
    d.sessions.append(reader)
    reader.tool("ledgence_effect_inspect", identity, error="forbidden")
    reader.close()
    script = d.directory / "installed-effect-client.py"
    script.write_text('''import asyncio,json,sys
from ledgence.client import AsyncClient, EffectClaimEntry, EffectObservationEntry
async def main():
    server,credential,tenant,namespace,query=sys.argv[1:]
    identity=json.load(open(query))
    async with AsyncClient(server,tenant=tenant,namespace=namespace,credentials_file=credential) as client:
        summaries=await client.effects.inbox(filter="confirmed",workflow_id=identity["workflow_id"])
        assert len(summaries.items)==1
        entries=[]; cursor=None
        while True:
            page=await client.effects.inspect(**identity,cursor=cursor,limit=1)
            entries.extend(page.entries); cursor=page.next_cursor
            if cursor is None:break
        assert len(entries)==3 and isinstance(entries[0],EffectClaimEntry) and isinstance(entries[-1],EffectObservationEntry)
        assert entries[-1].source.worker.subject=="crash-replacement"
        assert entries[-1].source.reported_callback=="lookup"
    print(json.dumps({"installed_wheel":True,"whole_entry_pages":3,"source":"crash-replacement"}))
asyncio.run(main())
''')
    query = d.directory / "effect-client-query.json"
    query.write_text(json.dumps(identity))
    result = subprocess.run([str(interpreter), "-I", str(script), d.server_url, str(d.operator),
        d.scope["tenant_id"], d.scope["namespace"], str(query)], env=d.environment, capture_output=True, timeout=30)
    (d.directory / "installed-effect-client.stdout").write_bytes(result.stdout)
    (d.directory / "installed-effect-client.stderr").write_bytes(result.stderr)
    assert result.returncode == 0, "installed effect client failed"
    return {"generic_api": True, "console_alias": True, "cli": True, "stdio_mcp": True,
            "installed_python": True, "metadata_denied_sensitive": True, "durable_audit_failure_status": 503}


def retention(d, recovery):
    occurrence = recovery["occurrence"]
    saved, entries = d.all_entries(occurrence)
    journal = d.journal(occurrence)
    workflow = quote(occurrence["workflow_id"])
    # Only the owned disposable database is altered to advance the retention
    # clock. Real collection still runs through the production retain command.
    d.server.stop()
    age = "(extract(epoch FROM clock_timestamp())*1000)::bigint-91*86400000::bigint"
    d.sql("UPDATE workflow_runs SET terminal_at_ms=" + age + " WHERE workflow_id=" + workflow)
    d.sql("UPDATE tasks SET terminal_at_ms=" + age + " WHERE task_id IN "
          "(SELECT activation_id FROM workflow_activations WHERE workflow_id=" + workflow + ") AND terminal_at_ms IS NOT NULL")
    # The killed worker's persisted consumer cursor legitimately protects its
    # last task until session expiry. Advance these stopped sessions' clock as
    # well; the production collector, not the fixture, removes their cursors.
    sessions = "SELECT DISTINCT a.worker_session_id FROM attempts a JOIN tasks t ON t.task_id=a.task_id WHERE t.workflow_id=" + workflow
    d.sql("UPDATE worker_sessions s SET expires_at_ms=" + age + " WHERE s.session_id IN (" + sessions
          + ") AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.worker_session_id=s.session_id AND a.state='active')")
    for _ in range(3):
        d.command("ledgence", ["orchestrator", "retain", "--tenant", d.scope["tenant_id"],
            "--namespace", d.scope["namespace"], "--retain-days", "90", "--batch-size", "128", "--batches", "100", "--apply"])
        if d.sql("SELECT count(*) FROM workflow_runs WHERE workflow_id=" + workflow) == "0": break
    if d.sql("SELECT count(*) FROM workflow_runs WHERE workflow_id=" + workflow) != "0":
        # Keep only row identities/states and reference counts for diagnosing
        # protective collection fences; no application values or private leases.
        diagnostics = {"workflow": json.loads(d.sql("SELECT json_build_object('state',state,'terminal_at_ms',terminal_at_ms,'retiring_at_ms',retiring_at_ms) FROM workflow_runs WHERE workflow_id=" + workflow)),
            "tasks": json.loads(d.sql("SELECT coalesce(json_agg(json_build_object('task_id',task_id,'state',state,'terminal_at_ms',terminal_at_ms,'retiring_at_ms',retiring_at_ms)),'[]'::json) FROM tasks WHERE workflow_id=" + workflow))}
        for table in ("workflow_activations", "workflow_local_results", "workflow_waits", "workflow_task_links",
                      "workflow_work", "workflow_forks", "workflow_approvals", "owned_workflow_links"):
            clause = ("activation_id IN (SELECT activation_id FROM workflow_activations WHERE workflow_id=" + workflow + ")"
                      if table == "workflow_local_results" else "workflow_id=" + workflow
                      if table != "owned_workflow_links" else "child_workflow_id=" + workflow)
            diagnostics[table] = int(d.sql("SELECT count(*) FROM " + table + " WHERE " + clause))
        task_ids = "SELECT task_id FROM tasks WHERE workflow_id=" + workflow
        diagnostics["consumer_cursors"] = int(d.sql("SELECT count(*) FROM consumer_cursors WHERE task_id IN (" + task_ids + ")"))
        diagnostics["active_attempts"] = int(d.sql("SELECT count(*) FROM attempts WHERE state='active' AND task_id IN (" + task_ids + ")"))
        diagnostics["dispatch_intents"] = int(d.sql("SELECT count(*) FROM dispatch_intents WHERE task_id IN (" + task_ids + ")"))
        (d.directory / "retention-blockers.json").write_text(json.dumps(diagnostics, indent=2) + "\n")
        raise AssertionError("real collector did not remove execution")
    assert d.sql("SELECT count(*) FROM workflow_approvals WHERE workflow_id=" + workflow) == "0"
    d.start_server()
    after, retained = d.all_entries(occurrence)
    assert after == saved and retained == entries and d.journal(occurrence) == journal
    receipt = d.checked("POST", CONFIRM, recovery["retained_confirm_command"], credentials=d.workers["crash-replacement"])
    assert receipt == recovery["receipt"] and receipt["already_accepted"]
    before = d.counts(occurrence)
    d.checked("POST", CLAIM, dict(recovery["retained_claim_command"], claim_id="new-after-collection"),
              credentials=d.workers["crash-replacement"], expected=409)
    assert d.counts(occurrence) == before, "collected execution granted new effect authority"
    return {"workflow_collected": True, "approval_collected": True, "retained_entries": len(retained),
            "journal_bytes_unchanged": True, "exact_confirm_replay_after_collection": True}
