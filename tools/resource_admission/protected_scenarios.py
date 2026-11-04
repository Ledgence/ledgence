"""Composed authority checked against independent provider requests and durable state (MIT)."""
import json

from http_acceptance.harness import eventually

from .protected import REPORT, START
from .protected_surfaces import public_surfaces


def finish(d, occurrence, paths, *, selection="provider_succeeded"):
    wake = d.finished(occurrence, paths, selection)
    snapshot = d.assert_original(occurrence)
    assert snapshot["provider"]["ticket"]["occurrence"] == occurrence
    return wake


def parked_restart(d, provider):
    key = "protected-restart"
    d.policy(enabled=False)
    flow = d.worker()
    occurrence, paths = d.begin(key, provider)
    snapshot = d.admitted(occurrence)
    assert snapshot["state"] == "waiting" and snapshot["source"] is None
    assert snapshot["binding"]["provider"]["ticket"]["occurrence"] == occurrence
    assert d.provider_census(provider, key)["posts"] == 0
    witness = d.witness("ordinary-while-protected-parked", d.queue)
    d.restart()
    assert d.status(occurrence["workflow_id"])["state"] == "waiting"
    d.policy()
    # No probe worker yet: an accepted HTTP response releases call occupancy,
    # while the business-result wait remains genuinely unresolved.
    eventually(lambda: (d.admission(occurrence["workflow_id"]) or {}).get("state") == "finished",
               description="completed original HTTP call releases capacity")
    d.parked(occurrence)
    assert d.resource()["occupied"] == 0
    snapshot = d.assert_original(occurrence)
    assert snapshot["provider"]["report"]["call_completion"] == "completed"
    assert d.inspect(occurrence)["summary"]["provider"]["knowledge"] in ("unknown", "pending")
    probe = d.worker("crash-replacement", queue=d.probe_queue)
    finish(d, occurrence, paths)
    assert d.provider_census(provider, key)["posts"] == 1
    surfaces = public_surfaces(d, occurrence)
    flow.stop(); probe.stop()
    return {"occurrence": occurrence, "ordinary_task": witness,
            "original_identity_preserved": True, "call_finished_before_business": True,
            "public_surfaces": surfaces}


def competing_capacity(d, provider):
    d.policy(capacity=1, offer_ms=30000)
    first, second = d.worker(), d.worker("crash-replacement")
    provider.release_reply.clear()
    try:
        one, one_paths = d.begin("protected-held", provider)
        eventually(lambda: d.provider_census(provider, "protected-held")["posts"] == 1,
                   description="first protected dispatch reached provider")
        two, two_paths = d.begin("protected-queued", provider)
        second_snapshot = d.admitted(two)
        assert second_snapshot["state"] == "waiting"
        assert d.resource()["occupied"] == 1
        assert d.provider_census(provider, "protected-queued")["posts"] == 0
    finally:
        provider.release_reply.set()
    probe = d.worker("late-origin", queue=d.probe_queue)
    finish(d, one, one_paths); finish(d, two, two_paths)
    assert d.provider_census(provider, "protected-held")["posts"] == 1
    assert d.provider_census(provider, "protected-queued")["posts"] == 1
    assert d.resource()["occupied"] == 0
    first.stop(); second.stop(); probe.stop()
    return {"occurrences": [one, two], "capacity_enforced_before_dispatch": True}


def lost_report(d, provider):
    d.policy()
    proxy = d.authenticated_proxy(); lost = proxy.lose_once(REPORT)
    flow = d.worker(proxy=proxy); probe = d.worker("crash-replacement", queue=d.probe_queue)
    occurrence, paths = d.begin("protected-lost-report", provider)
    finish(d, occurrence, paths)
    assert lost.is_set()
    commands = eventually(lambda: proxy.commands(REPORT) if len(proxy.commands(REPORT)) >= 2 else None,
                          description="frozen composed report retried after lost acknowledgement")
    assert all(command["body"] == commands[0]["body"] for command in commands)
    assert d.provider_census(provider, "protected-lost-report")["posts"] == 1
    assert d.resource()["occupied"] == 0
    flow.stop(); probe.stop()
    return {"occurrence": occurrence, "identical_report_exchanges": len(commands), "posts": 1}


def lost_start(d, provider):
    d.policy()
    proxy = d.authenticated_proxy(); lost = proxy.lose_once(START)
    flow = d.worker(proxy=proxy)
    occurrence, _ = d.begin("protected-lost-start", provider, attempts=1)
    eventually(lost.is_set, description="composed Start committed but response lost")
    eventually(lambda: d.status(occurrence["workflow_id"])["state"] == "failed",
               description="uncertain Start fails closed without retrying permission")
    assert len(proxy.commands(START)) == 1
    assert d.provider_census(provider, "protected-lost-start")["posts"] == 0
    snapshot = d.assert_original(occurrence)
    assert snapshot["source"] is not None and snapshot["provider"]["source"] is not None
    assert d.resource()["occupied"] == 1
    d.release(occurrence["workflow_id"])
    assert d.resource()["occupied"] == 0
    flow.stop()
    return {"occurrence": occurrence, "start_exchanges": 1, "posts": 0, "release_replayed": True}


def expired_start_window(d, provider):
    from workflow_acceptance.owned_scenarios import quote

    key = "protected-expired-send-window"
    d.policy(capacity=1, offer_ms=30_000, rate={"numerator": 1, "period_ms": 1000, "burst": 1})
    proxy = d.authenticated_proxy()
    gate = d.directory / "release-retry-start"
    d.gates.add(gate)
    flow = d.worker(proxy=proxy)
    probe = None
    try:
        approval, paths = d.request_approval(key, provider, attempts=2, budget={"max_calls": 1},
                                              overrides={"retry_start_gate": str(gate)})
        workflow = quote(approval["workflow_id"])
        resource = quote(d.resource_name)
        credit = d.sql("SELECT credit FROM admission_resources WHERE resource_name=" + resource)
        # PostgreSQL sequences retain the timestamp witnesses even if Start rolls back.
        # Only the first tentative grant waits, under the normal owner/resource locks.
        d.sql(f"""
            CREATE SEQUENCE held_start_deadline;
            CREATE SEQUENCE held_start_released;
            CREATE FUNCTION delay_first_start() RETURNS trigger LANGUAGE plpgsql AS $$
            DECLARE deadline bigint;
            BEGIN
                IF NOT (SELECT is_called FROM held_start_deadline) THEN
                    SELECT (convert_from(snapshot_bytes,'UTF8')::jsonb #>> '{{source,start_deadline_at}}')::bigint
                      INTO STRICT deadline FROM admission_requests WHERE workflow_id={workflow};
                    PERFORM setval('held_start_deadline', deadline);
                    PERFORM pg_sleep(GREATEST(0, (deadline-extract(epoch FROM clock_timestamp())*1000)/1000.0)+0.03);
                    PERFORM setval('held_start_released', floor(extract(epoch FROM clock_timestamp())*1000)::bigint);
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER delay_first_start BEFORE UPDATE ON admission_resources FOR EACH ROW
                WHEN (NEW.resource_name={resource} AND NEW.started > OLD.started)
                EXECUTE FUNCTION delay_first_start();
        """)
        occurrence = d.approve(approval, key)
        first = eventually(lambda: proxy.commands(START), timeout=60,
                           description="original external worker receives the first protected Start reply")[0]
        assert first["status"] == 409 and json.loads(first["response"])["code"] == "ownership_lost"
        eventually(gate.with_suffix(".retry").exists, timeout=60, description="replacement activation enters dispatch")
        assert len(proxy.commands(START)) == 1 and not proxy.commands(REPORT)
        assert d.provider_census(provider, key)["posts"] == 0
        snapshot = d.assert_original(occurrence)
        assert snapshot["state"] == "offered" and snapshot["source"] is None and snapshot.get("provider") is None
        due = int(d.sql("SELECT last_value FROM held_start_deadline WHERE is_called"))
        released = int(d.sql("SELECT last_value FROM held_start_released WHERE is_called"))
        original = first["json"]["admission"]
        attempt = quote(original["owner"]["attempt_id"])
        lease = int(d.sql("SELECT least(expires_at_ms, authority_deadline_ms, deadline_ms) FROM attempts WHERE attempt_id=" + attempt))
        assert due <= released < min(snapshot["offer"]["expires_at"], snapshot["deadline_at"], lease)
        assert d.resource()["offered"] == d.resource()["occupied"] == 1
        assert d.sql("SELECT credit FROM admission_resources WHERE resource_name=" + resource) == credit
        assert d.sql(f"SELECT effect_dispatch_claimed FROM workflow_approvals WHERE workflow_id={workflow}") == "f"
        assert d.sql(f"SELECT has_provider_operations FROM workflow_runs WHERE workflow_id={workflow}") == "f"
        for table in ("provider_operations", "provider_invocations", "provider_probe_tasks", "approved_effect_occurrences"):
            assert d.sql(f"SELECT count(*) FROM {table} WHERE workflow_id={workflow}") == "0"
        query = {"scope": d.scope, "subject": {"kind": "workflow", "workflow_id": occurrence["workflow_id"]}}
        assert d.checked("POST", "/v1/budgets/inspect", query)["calls"] == "0"
        assert not d.checked("POST", "/v1/budgets/entries", {**query, "before": None, "limit": 10})["items"]

        probe = d.worker("crash-replacement", queue=d.probe_queue)
        gate.touch()
        finish(d, occurrence, paths)
        _, accepted = proxy.commands(START)
        assert accepted["status"] == 200 and json.loads(accepted["response"])["kind"] == "granted"
        fresh = accepted["json"]["admission"]
        assert fresh["owner"]["attempt_id"] != original["owner"]["attempt_id"]
        assert fresh["owner"]["task_id"] == original["owner"]["task_id"]
        assert fresh["offer_generation"] == original["offer_generation"] and fresh["start_id"] != original["start_id"]
        assert d.provider_census(provider, key)["posts"] == 1 and d.resource()["occupied"] == 0
        assert d.checked("POST", "/v1/budgets/inspect", query)["calls"] == "1"
        return {"occurrence": occurrence, "first_start_status": first["status"], "first_provider_posts": 0,
                "first_charged_calls": 0, "first_send_deadline_ms": due, "first_final_write_ms": released,
                "fresh_lease": True, "final_provider_posts": 1, "final_charged_calls": 1}
    finally:
        gate.touch()
        flow.stop()
        if probe:
            probe.stop()
        d.sql("DROP TRIGGER IF EXISTS delay_first_start ON admission_resources;"
              "DROP FUNCTION IF EXISTS delay_first_start();"
              "DROP SEQUENCE IF EXISTS held_start_deadline, held_start_released;")


def unknown_call_successful_probe(d, provider):
    key = "protected-unknown-call"
    d.policy()
    provider.set_fault(key, post_response="drop")
    flow = d.worker(); probe = d.worker("crash-replacement", queue=d.probe_queue)
    occurrence, paths = d.begin(key, provider)
    finish(d, occurrence, paths)
    snapshot = d.assert_original(occurrence)
    assert snapshot["provider"]["report"]["call_completion"] == "unknown"
    assert snapshot["state"] == "unknown_occupied"
    assert snapshot["finish"] is None and d.resource()["occupied"] == 1
    census = d.provider_census(provider, key)
    assert census["posts"] == census["effects"] == 1 and census["gets"] >= 1
    d.release(occurrence["workflow_id"])
    assert d.resource()["occupied"] == 0
    assert d.status(occurrence["workflow_id"])["state"] == "succeeded"
    flow.stop(); probe.stop()
    return {"occurrence": occurrence, "provider_census": census,
            "business_success_did_not_release_unknown_call": True}


def duplicate_business(d, provider):
    key = "protected-shared-business"
    d.policy(capacity=2, offer_ms=30000)
    first, second = d.worker(), d.worker("crash-replacement")
    probe = d.worker("late-origin", queue=d.probe_queue)
    one_approval, _ = d.request_approval("protected-duplicate-a", provider, business_key=key)
    two_approval, _ = d.request_approval("protected-duplicate-b", provider, business_key=key)
    one = d.approve(one_approval, "duplicate-a")
    two = d.approve(two_approval, "duplicate-b")
    for occurrence in (one, two):
        eventually(lambda occurrence=occurrence: d.status(occurrence["workflow_id"])["state"] == "succeeded",
                   timeout=90, description="duplicate governed workflows finish without repeating mutation")
    snapshots = [d.assert_original(occurrence) for occurrence in (one, two)]
    records = [snapshot["provider"] for snapshot in snapshots]
    assert records[0]["ticket"] == records[1]["ticket"]
    assert sum(record["source"] is not None for record in records) == 1
    assert sum(snapshot["source"] is not None for snapshot in snapshots) == 1
    assert sum(snapshot["state"] == "closed_before_start" for snapshot in snapshots) == 1
    assert d.provider_census(provider, key)["posts"] == 1
    assert d.resource()["occupied"] == 0
    duplicate = snapshots[0] if records[0]["source"] is None else snapshots[1]
    result = d.checked("GET", d.query("/v1/workflows/result", workflow_id=duplicate["occurrence"]["workflow_id"]))
    assert result["outcome"]["output"]["status"] == "existing_operation"
    first.stop(); second.stop(); probe.stop()
    return {"occurrences": [one, two], "one_ticket": records[0]["ticket"], "posts": 1,
            "unused_duplicate_offer_released": True}


def retained_report(d, provider):
    d.policy()
    proxy = d.authenticated_proxy(); committed = proxy.lose_once(REPORT, block_after=True)
    flow = d.worker(proxy=proxy)
    occurrence, paths = d.begin("protected-report-before-crash", provider, attempts=3)
    eventually(committed.is_set, description="composed Report persisted before worker loss")
    assert d.admission(occurrence["workflow_id"])["state"] == "finished"
    flow.kill()
    replacement_proxy = d.authenticated_proxy()
    replacement = d.worker(proxy=replacement_proxy)
    probe = d.worker("crash-replacement", queue=d.probe_queue)
    finish(d, occurrence, paths)
    assert not replacement_proxy.commands(START) and not replacement_proxy.commands(REPORT), (
        "replacement failed to use the preloaded original provider record"
    )
    assert d.provider_census(provider, "protected-report-before-crash")["posts"] == 1
    assert d.resource()["occupied"] == 0
    replacement.stop(); probe.stop()
    return {"occurrence": occurrence, "accepted_ticket_replayed_after_crash": True, "replacement_start_and_report_rpcs": 0, "posts": 1}


def quarantines(d, provider):
    results = []
    for fence in ("admission", "provider"):
        key = "protected-quarantine-" + fence
        d.policy(enabled=False, offer_ms=30000)
        flow = d.worker()
        occurrence, _ = d.begin(key, provider, attempts=1)
        d.admitted(occurrence); flow.stop()
        d.policy(offer_ms=30000)
        eventually(lambda occurrence=occurrence: d.admission(occurrence["workflow_id"])["state"] == "offered",
                   description="preexisting protected offer before quarantine")
        d.restart(admission="quarantined" if fence == "admission" else "enabled",
                  provider="declared_restore_quarantine" if fence == "provider" else "enabled")
        proxy = d.authenticated_proxy(); flow = d.worker(proxy=proxy)
        commands = eventually(lambda proxy=proxy: proxy.commands(START), description="combined Start checks " + fence + " quarantine")
        assert all(command["status"] == 503 for command in commands)
        eventually(lambda occurrence=occurrence: d.status(occurrence["workflow_id"])["state"] == "failed",
                   description="quarantined protected activation fails closed")
        snapshot = d.assert_original(occurrence)
        assert snapshot["source"] is None and snapshot.get("provider") is None
        assert d.provider_census(provider, key)["posts"] == 0
        flow.stop(); d.restart()
        eventually(lambda: d.resource()["occupied"] == 0, description="unused quarantined offer cleanup")
        results.append({"fence": fence, "occurrence": occurrence, "no_dispatch": True})
    return {"independent_fences": results}


CASES = [expired_start_window, parked_restart, competing_capacity, lost_report, lost_start,
         unknown_call_successful_probe, duplicate_business, retained_report, quarantines]


def late_report_after_retirement(d, provider):
    """A frozen original call fact survives cancellation, explicit release and collection."""
    from workflow_acceptance.owned_scenarios import quote

    d.policy()
    retained = d.retained_outcome
    proxy = d.authenticated_proxy(); d.worker(proxy=proxy)
    occurrence, paths = d.begin("protected-late-retained", provider, program="provider-admission-held")
    eventually(retained.exists, description="registered dispatch froze its actual outcome before reporting")
    (start,) = proxy.commands(START)
    source = json.loads(start["response"])["source"]
    outcome = json.loads(retained.read_text())
    assert outcome["call_completion"] == "completed"
    command = {"source": source, "outcome": outcome}
    d.cancel_workflow(occurrence)
    eventually(lambda: d.status(occurrence["workflow_id"])["state"] == "cancelled", timeout=90,
               description="managed callback terminated before reporting")
    assert not proxy.commands(REPORT)
    assert d.resource()["occupied"] == 1
    assessment = d.resolution(occurrence, "protected-retention-close",
                             {"kind": "close_unknown"}, references=())
    d.resolve(assessment)
    d.release(occurrence["workflow_id"])
    assert d.resource()["occupied"] == 0
    d.stop_workers(); d.stop_servers()
    workflow = quote(occurrence["workflow_id"])
    age = "(extract(epoch FROM clock_timestamp())*1000)::bigint-91*86400000::bigint"
    d.sql("UPDATE workflow_runs SET terminal_at_ms=" + age + " WHERE workflow_id=" + workflow)
    d.sql("UPDATE tasks SET terminal_at_ms=" + age + " WHERE workflow_id=" + workflow + " AND terminal_at_ms IS NOT NULL")
    d.sql("UPDATE worker_sessions SET expires_at_ms=" + age + " WHERE NOT EXISTS(SELECT 1 FROM attempts WHERE worker_session_id=worker_sessions.session_id AND state='active')")
    for _ in range(3):
        d.command("ledgence", ["orchestrator", "retain", "--tenant", d.scope["tenant_id"],
                               "--namespace", d.scope["namespace"], "--retain-days", "90",
                               "--batch-size", "128", "--batches", "100", "--apply"])
        if d.sql("SELECT count(*) FROM workflow_runs WHERE workflow_id=" + workflow) == "0":
            break
    assert d.sql("SELECT count(*) FROM workflow_runs WHERE workflow_id=" + workflow) == "0", "real collector retained cancelled protected execution"
    d.start_server()
    d.checked("POST", REPORT, command, credentials=d.reader, expected=403)
    d.checked("POST", REPORT, command, credentials=d.workers["late-outsider"], expected=403)
    accepted = d.checked("POST", REPORT, command, credentials=d.workers["late-origin"])
    replay = d.checked("POST", REPORT, command, credentials=d.workers["late-origin"])
    assert not accepted["receipt"]["already_accepted"] and replay["receipt"]["already_accepted"]
    assert accepted["call_completion"] == replay["call_completion"] == "completed"
    assert d.resource()["occupied"] == 0
    altered = {**command, "outcome": {**outcome, "call_completion": "unknown"}}
    d.checked("POST", REPORT, altered, credentials=d.workers["late-origin"], expected=409)
    assert d.sql("SELECT count(*) FROM workflow_runs WHERE workflow_id=" + workflow) == "0"
    assert not d.wakes(paths) and d.provider_census(provider, "protected-late-retained")["posts"] == 1
    snapshot = d.admission(occurrence["workflow_id"])
    assert snapshot["state"] == "administratively_released" and snapshot["provider"]["report"] is not None
    return {"occurrence": occurrence, "execution_collected": True, "original_report_accepted_late": True,
            "explicit_release_not_decremented_twice": True, "identity_change_rejected": True}


def exhausted_budget_after_approval(d, provider):
    key = "approved-without-call-allowance"
    d.policy(capacity=1)
    worker = d.worker()
    try:
        occurrence, _ = d.begin(key, provider, budget={"max_calls": 0})
        workflow = occurrence["workflow_id"]
        eventually(lambda: d.status(workflow)["state"] == "failed", timeout=90,
                   description="approved mutation closes on exhausted root allowance")
        admitted = d.admission(workflow)
        assert admitted["state"] == "closed_before_start" and admitted["source"] is None
        assert admitted["budget_denial"] == "calls_exhausted"
        assert d.provider_census(provider, key)["posts"] == 0 and d.resource()["occupied"] == 0
        query = {"scope": d.scope, "subject": {"kind":"workflow", "workflow_id":workflow}}
        assert d.checked("POST", "/v1/budgets/inspect", query)["calls"] == "0"
        page = d.checked("POST", "/v1/budgets/entries", {**query,"before":None,"limit":10})
        assert len(page["items"]) == 1 and page["items"][0]["reason"] == "calls_exhausted"
        return {"workflow_id":workflow,"provider_posts":0,"charged_calls":0}
    finally:
        worker.stop()


CASES.extend([late_report_after_retirement, exhausted_budget_after_approval])
