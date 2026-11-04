"""Native acceptance scenarios; no provider truth is inferred from reported JSON (MIT)."""
import json
from pathlib import Path
import subprocess

from http_acceptance.harness import eventually
from mcp_acceptance.harness import McpSession
from workflow_acceptance.owned_scenarios import external, quote
from workflow_acceptance.refund_provider import RefundProvider
from .harness import RESOLVE


def one_provider_request(provider):
    census = provider.census()
    assert census["post_requests"] == 1 and census["lookup_requests"] == 0 and len(census["refunds"]) == 1
    return census


def installed_client(d, interpreter, occurrence):
    script = d.directory / "installed-resolution-client.py"
    script.write_text('''import asyncio,json,sys
from pathlib import Path
from ledgence.client import AsyncClient,EffectResolutionAssessment,EffectResolutionReceipt,EffectAssessmentEntry,AuditFilter,AuditEffectResolved
async def main():
    url,credentials,tenant,namespace,identity,saved=sys.argv[1:]
    async with AsyncClient(url,tenant=tenant,namespace=namespace,credentials_file=credentials) as client:
        occurrence=json.loads(Path(identity).read_text())
        detail=await client.effects.inspect(**{k:occurrence[k] for k in ("workflow_id","activation_id","approval_key")})
        command=client.effects.prepare_resolution(detail,resolution_id="installed-success",assessment=EffectResolutionAssessment.record_success({"selected":True,"large":2**64-1}),reason="Reviewed owned provider ledger",evidence_references=["fixture:receipt"])
        Path(saved).write_text(command.to_json())
        restored=client.effects.restore_resolution(Path(saved).read_text())
        receipt=await client.effects.resolve(restored)
        assert isinstance(receipt,EffectResolutionReceipt) and not receipt.already_accepted
        after=await client.effects.inspect(**{k:occurrence[k] for k in ("workflow_id","activation_id","approval_key")})
        assert isinstance(after.current_assessment,EffectAssessmentEntry)
        assert after.current_assessment.assessment.result=={"selected":True,"large":2**64-1}
        audit=await client.audit.list(filter=AuditFilter.resource("workflow",occurrence["workflow_id"]),limit=100)
        assert any(isinstance(event.operation,AuditEffectResolved) and event.operation.resolution_id==receipt.resolution_id for event in audit.items)
        print(json.dumps({"resolution_id":receipt.resolution_id,"accepted_revision":receipt.accepted_revision}))
asyncio.run(main())
''')
    identity = d.directory / "installed-occurrence.json"
    identity.write_text(json.dumps(occurrence))
    saved = d.directory / "installed-resolution.json"
    result = subprocess.run([str(interpreter), "-I", str(script), d.server_url, str(d.operator),
        d.scope["tenant_id"], d.scope["namespace"], str(identity), str(saved)], env=d.environment, capture_output=True, timeout=60)
    (d.directory / "installed-resolution.stdout").write_bytes(result.stdout)
    (d.directory / "installed-resolution.stderr").write_bytes(result.stderr)
    assert result.returncode == 0, "installed client assessment failed"
    return json.loads(saved.read_text())


def mcp_resolution_arguments(command):
    arguments = {key: value for key, value in command.items() if key != "occurrence"}
    arguments.update({key: command["occurrence"][key] for key in ("workflow_id", "activation_id", "approval_key")})
    arguments["expected_effect_revision"] = str(command["expected_effect_revision"])
    return arguments


def after_park_capacity_surfaces(d, interpreter):
    with RefundProvider(d.directory / "after-park-provider.jsonl", cooperative=True) as provider:
        occurrence, paths = d.begin("after-park", provider)
        d.parked(occurrence)
        # The single global consumer/runtime slot is free while this workflow waits.
        task = d.submit(d.submission("capacity-witness", program="invoice"))["task_id"]
        eventually(lambda: d.task(task)["state"] == "succeeded", timeout=60, description="single-slot worker executes witness while native workflow parked")
        before = d.status(occurrence["workflow_id"])
        event = external(d, occurrence["workflow_id"], "operator:0", "generic-collision", result={"selected":True})
        d.checked("POST", "/v1/workflows/events", event)
        assert d.status(occurrence["workflow_id"]) == before and not d.wakes(paths)
        command = d.resolution(occurrence, "permission-test", {"kind":"close_unknown"}, references=())
        assert d.request("POST", RESOLVE, command)[0] == 401
        for credential in (d.reader,d.reviewer,d.workers["late-origin"]):
            d.resolve(command,credential,expected=403)
        assert d.assessments(occurrence)==0
        # Even the resolver credential needs an explicit process opt-in.
        session = McpSession(d, "default-no-resolution", credentials_file=d.resolver)
        d.sessions.append(session)
        catalog = {row["name"]: row["annotations"] for row in session.request("tools/list", {})["result"]["tools"]}
        assert catalog["ledgence_effect_inspect"]["readOnlyHint"] is True
        assert "ledgence_effect_resolve" not in catalog, catalog.keys()
        session.invalid("ledgence_effect_resolve", mcp_resolution_arguments(command))
        assert d.assessments(occurrence) == 0
        session.close()
        saved=installed_client(d,interpreter,occurrence)
        wakes=d.finished(occurrence,paths,"recorded_success")
        assert wakes[-1]["outcome"]["result"]=={"selected":True,"large":2**64-1}
        command=json.loads(saved["body"])
        # Same stable actor is required; use original operator credential for replay.
        replay=d.cli_resolve(command,credentials=d.operator,label="installed-replay")
        assert replay["already_accepted"] and d.assessments(occurrence)==1
        read=McpSession(d,"assessment-read",credentials_file=d.operator,read_only=True)
        d.sessions.append(read)
        identity={key:occurrence[key] for key in ("workflow_id","activation_id","approval_key")}
        detail=read.tool("ledgence_effect_inspect",identity)
        assert detail["current_assessment"]["recorded_revision"]==str(replay["accepted_revision"])
        assert detail["current_assessment"]["assessment"]["result"]["large"]==2**64-1
        read.close()
        return {"occurrence":occurrence,"single_slot_witness":task,"native_wakes":len(wakes),
                "journal_confirmed":False,"installed_python":True,"cli_replay":True,"mcp_read":True,"provider":one_provider_request(provider)}


def before_park_disputed_followup(d, proxy):
    with RefundProvider(d.directory / "before-park-provider.jsonl",cooperative=True) as provider:
        occurrence,paths=d.begin("before-park",provider,before=True,follow_up=True)
        command=d.resolution(occurrence,"before-success",{"kind":"record_success","result":{"operator_selected":True}})
        session=McpSession(d,"resolve-opt-in",credentials_file=d.resolver,allow_effect_resolution=True)
        d.sessions.append(session)
        catalog = {row["name"]: row["annotations"] for row in session.request("tools/list", {})["result"]["tools"]}
        assert catalog["ledgence_effect_inspect"]["readOnlyHint"] is True
        assert catalog["ledgence_effect_resolve"]["readOnlyHint"] is False
        assert {"ledgence_approval_decide", "ledgence_resource_put", "ledgence_admission_release",
                "ledgence_task_admission_release"}.isdisjoint(catalog), catalog.keys()
        arguments = mcp_resolution_arguments(command)
        receipt = session.tool("ledgence_effect_resolve", arguments)
        assert receipt["accepted_revision"]==str(command["expected_effect_revision"]+1)
        assert not d.wakes(paths)
        assert int(d.sql("SELECT count(*) FROM workflow_effect_waits WHERE workflow_id="+quote(occurrence["workflow_id"])))==0
        # A distinct authenticated historical report is evidence, not a managed late callback.
        d.late_report(occurrence,proxy,"unequal-before-selection",{"different":True})
        detail=d.inspect(occurrence)
        assert detail["summary"]["operator_resolution"]["first_conflicting_entry_sequence"] is not None
        Path(paths["release_park"]).touch()
        d.parked(occurrence,count=2)
        first,=d.wakes(paths)
        assert first["outcome"]["kind"]=="disputed" and first["resolution_id"]=="before-success"
        second=d.resolution(occurrence,"follow-up-failure",{"kind":"record_failure","failure":{"kind":"declined","message":"Operator documented rejection"}})
        accepted=d.resolve(second)
        assert accepted["supersedes_resolution_id"]=="before-success"
        wakes=d.finished(occurrence,paths,"recorded_failure")
        assert wakes[0]==first and len(wakes)==2
        # Historical discrepancy persists, but bound review of it closes current dispute.
        detail=d.inspect(occurrence)
        assert detail["summary"]["operator_resolution"]["first_conflicting_entry_sequence"] is None
        original = session.tool("ledgence_effect_resolve", arguments)
        assert original==dict(receipt,already_accepted=True)
        session.close()
        return {"occurrence":occurrence,"before_park_acceptance":True,"disputed_wake":True,"follow_up_wake":True,
                "old_wake_immutable":True,"old_receipt_replay":True,"provider":one_provider_request(provider)}


def lost_reply_rotation(d):
    with RefundProvider(d.directory / "lost-reply-provider.jsonl",cooperative=True) as provider:
        occurrence,paths=d.begin("lost-reply",provider)
        d.parked(occurrence)
        command=d.resolution(occurrence,"lost-response",{"kind":"close_unknown"},references=())
        proxy=d.authenticated_proxy()
        lost=proxy.unavailable_once(RESOLVE)
        def proxy_credential(source,name):
            data=json.loads(source.read_text()); data["server"]=proxy.url
            path=d.directory/(name+".json");path.write_text(json.dumps(data));path.chmod(0o600);d.secret_files.append(path)
            return path
        original=proxy_credential(d.resolver,"resolver-proxy")
        rotated=proxy_credential(d.resolver_rotated,"resolver-rotated-proxy")
        d.cli_resolve(command,credentials=original,server=proxy.url,expected=1,label="lost-response")
        assert lost.is_set() and len(proxy.commands(RESOLVE))==1 and d.assessments(occurrence)==1
        receipt=d.cli_resolve(command,credentials=rotated,server=proxy.url,label="lost-response-replay")
        assert receipt["already_accepted"] and len(proxy.commands(RESOLVE))==2 and d.assessments(occurrence)==1
        records=proxy.commands(RESOLVE)
        assert records[0]["body"]==records[1]["body"]
        d.resolve(command,d.other_resolver,expected=409)
        d.resolve(dict(command,reason="changed"),expected=409)
        d.resolve(dict(command,resolution_id="stale-command"),expected=409)
        wakes=d.finished(occurrence,paths,"closed_unknown")
        assert len(wakes)==1
        return {"occurrence":occurrence,"lost_reply_exercised":True,"same_actor_rotated_replay":True,
                "assessments":1,"native_wakes":1,"provider":one_provider_request(provider)}


def cancelled_retained(d,proxy,worker):
    with RefundProvider(d.directory / "cancelled-provider.jsonl",cooperative=True) as provider:
        occurrence,paths=d.begin("cancelled",provider)
        d.parked(occurrence)
        d.cancel_workflow(occurrence)
        eventually(lambda:d.status(occurrence["workflow_id"])["state"]=="cancelled",timeout=60,description="cancel native wait")
        command=d.resolution(occurrence,"after-cancel",{"kind":"close_unknown"},references=())
        receipt=d.resolve(command)
        d.late_report(occurrence,proxy,"after-cancel-report",{"state":"succeeded","opaque":True})
        assert d.status(occurrence["workflow_id"])["state"]=="cancelled" and not d.wakes(paths)
        saved,entries=d.all_entries(occurrence)
        assert saved["summary"]["operator_resolution"]["first_conflicting_entry_sequence"] is None
        assert d.journal(occurrence)==""
        worker.stop()
        d.server.stop()
        workflow=quote(occurrence["workflow_id"])
        age="(extract(epoch FROM clock_timestamp())*1000)::bigint-91*86400000::bigint"
        d.sql("UPDATE workflow_runs SET terminal_at_ms="+age+" WHERE workflow_id="+workflow)
        d.sql("UPDATE tasks SET terminal_at_ms="+age+" WHERE workflow_id="+workflow+" AND terminal_at_ms IS NOT NULL")
        d.sql("UPDATE worker_sessions SET expires_at_ms="+age+" WHERE NOT EXISTS(SELECT 1 FROM attempts WHERE worker_session_id=worker_sessions.session_id AND state='active')")
        for _ in range(3):
            d.command("ledgence",["orchestrator","retain","--tenant",d.scope["tenant_id"],"--namespace",d.scope["namespace"],"--retain-days","90","--batch-size","128","--batches","100","--apply"])
            if d.sql("SELECT count(*) FROM workflow_runs WHERE workflow_id="+workflow)=="0":break
        if d.sql("SELECT count(*) FROM workflow_runs WHERE workflow_id="+workflow)!="0":
            diagnostics = {"workflow_id": occurrence["workflow_id"]}
            for table in ("workflow_runs", "workflow_effect_waits", "workflow_work", "workflow_activations", "tasks"):
                diagnostics[table] = int(d.sql("SELECT count(*) FROM " + table + " WHERE workflow_id=" + workflow))
            diagnostics["active_attempts"] = int(d.sql("SELECT count(*) FROM attempts WHERE state='active' AND task_id IN (SELECT task_id FROM tasks WHERE workflow_id=" + workflow + ")"))
            (d.directory / "native-retention-blockers.json").write_text(json.dumps(diagnostics, indent=2) + "\n")
            raise AssertionError("native wait prevented collection")
        assert d.sql("SELECT count(*) FROM workflow_effect_waits WHERE workflow_id="+workflow)=="0"
        d.start_server()
        after,retained=d.all_entries(occurrence)
        assert after==saved and retained==entries
        assert d.resolve(command)==dict(receipt,already_accepted=True)
        superseding=d.resolution(occurrence,"after-collection",{"kind":"close_unknown"},references=())
        d.resolve(superseding)
        assert d.sql("SELECT count(*) FROM workflow_runs WHERE workflow_id="+workflow)=="0"
        assert not d.wakes(paths) and d.journal(occurrence)==""
        return {"occurrence":occurrence,"cancelled_wait_not_revived":True,"opaque_report_not_interpreted":True,
                "workflow_and_wait_collected":True,"retained_assessment_replay":True,"post_collection_supersession":True,
                "provider":one_provider_request(provider)}
