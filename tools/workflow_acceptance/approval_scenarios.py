"""Real subprocess approval recovery, exact binding, and one-slot acceptance."""
import asyncio
import copy
import json
from http_acceptance.harness import eventually, exchange

SCENARIOS = ("approvals", "approval-example")

async def run(d, names, record, snapshot):
    from ledgence.client import AsyncClient, RetryPolicy, ApprovalDecisionUncertain, ApprovalStatus
    options = dict(tenant=d.scope["tenant_id"], namespace=d.scope["namespace"])
    if "approvals" in names:
        proxy = d.proxy()
        worker = d.start_worker(server=proxy.url, concurrency=1)
        marker = d.directory / "approval-effects.jsonl"
        async with AsyncClient(proxy.url, **options) as client:
            async def submit(tag, **extra):
                return await client.workflows.submit(program="approval-controller",version="1.0.0",queue=d.queue,
                    data={"amount":100,"marker":str(marker),**extra},idempotency_key=tag,
                    retry_policy=RetryPolicy(max_attempts=3,retry_delay_ms=0))
            async def waiting(handle):
                await asyncio.to_thread(eventually, lambda: snapshot(d,handle.id)["state"] == "waiting",
                    timeout=110, description="durable approval proposal")
                proposal = await handle.approval("refund")
                assert proposal.status == ApprovalStatus.PENDING, proposal
                return proposal
            handle = await submit("approved-recovery",fail_after_commit=True)
            proposal = await waiting(handle)
            assert proposal.proposed_arguments == {"amount":100}
            assert proposal.action.arguments["amount"] == 50 and proposal.action.arguments["currency"] == "USD"
            decision = handle.prepare_approval_decision(proposal,decision_id="review:1",decision="approve",reviewer="test-operator")
            original = decision.to_dict()
            wrong = copy.deepcopy(original);wrong["action"]["arguments"]["amount"] = 100
            code,_,_ = await asyncio.to_thread(exchange,d.server_url,"POST","/v1/workflows/approvals/decide",wrong)
            assert code == 409, code
            missing=copy.deepcopy(original);missing["key"]="not-requested"
            code,_,_ = await asyncio.to_thread(exchange,d.server_url,"POST","/v1/workflows/approvals/decide",missing)
            assert code == 404, code
            code,_,_ = await asyncio.to_thread(exchange,d.server_url,"POST","/v1/workflows/events",{
                "scope":d.scope,"workflow_id":handle.id,"key":"refund","event":{
                    "specversion":"1.0","id":"not-an-approval","source":"urn:ledgence:test","type":"test.approved",
                    "datacontenttype":"application/json","data":{"approved":True}}})
            assert code == 409, code
            probe = await client.tasks.submit(program="invoice",version="1.0.0",queue=d.queue,
                data={"mode":"normal","marker":str(d.directory/"approval-probe.jsonl")},idempotency_key="approval-probe")
            await probe.result(timeout=110)
            assert not marker.exists(), "approval waiting did not execute its effect"
            await asyncio.to_thread(worker.stop)
            await asyncio.to_thread(d.server.stop)
            d.server,_ = await asyncio.to_thread(d.start_server)
            assert (await handle.approval("refund")).to_dict() == proposal.to_dict(), "proposal survives restart unchanged"
            saved = d.directory / "approval-decision.json"
            saved.write_text(json.dumps(original))
            lost = proxy.lose_once("/v1/workflows/approvals/decide")
            try:
                await handle.decide_approval(decision)
                raise AssertionError("fault proxy must lose the committed decision response")
            except ApprovalDecisionUncertain as error:
                assert error.command.to_dict() == original
            assert lost.is_set()
            # New client process/session can restore the command from durable storage.
            async with AsyncClient(proxy.url, **options) as restored_client:
                restored_handle=restored_client.workflows.handle(handle.id)
                restored=restored_handle.restore_approval_decision(json.loads(saved.read_text()))
                receipt=await restored_handle.decide_approval(restored)
                assert receipt.already_accepted and receipt.approval.status == ApprovalStatus.APPROVED
            worker = d.start_worker(server=proxy.url,concurrency=1)
            result=await handle.result(timeout=110)
            assert result == {"status":"approved","result":{"simulated":True,"amount":50,"currency":"USD"}}, result
            rows=[json.loads(line) for line in marker.read_text().splitlines()]
            assert len([r for r in rows if r["kind"]=="effect"]) == 1, rows
            resumes=[r for r in rows if r["kind"]=="resume"]
            assert len(resumes)==2 and {r["attempt"] for r in resumes} == {1,2}, rows
            assert len({r["activation"] for r in resumes})==1
            current=await handle.approval("refund")
            assert current.resumed_activation_id == resumes[0]["activation"]
            assert (await handle.decide_approval(decision)).already_accepted
            page=await handle.approvals(limit=1)
            assert len(page.items)==1 and page.items[0].to_dict()==current.to_dict() and page.next_cursor is None
            # Exercise public CLI against the real service and saved decision file.
            cli = await asyncio.to_thread(d.command,"ledgence",["approval","decide","--server",d.server_url,"--file",str(saved)])
            assert cli["already_accepted"]
            cli = await asyncio.to_thread(d.command,"ledgence",["approval","inspect","--server",d.server_url,
                "--tenant",options["tenant"],"--namespace",options["namespace"],"--workflow",handle.id,"--key","refund"])
            assert cli["action"]["arguments"]["amount"]==50
            calls=proxy.commands("/v1/workflows/approvals/decide")
            assert len(calls)==3 and all(c["body"]==calls[0]["body"] for c in calls)
            rejected=await submit("rejected")
            p=await waiting(rejected)
            await rejected.decide_approval(rejected.prepare_approval_decision(p,decision_id="reject:1",decision="reject",reviewer="test-operator"))
            assert await rejected.result(timeout=110) == {"status":"rejected","executed":False}
            expired=await submit("expired",timeout_ms=0)
            assert await expired.result(timeout=110) == {"status":"expired","executed":False}
            assert (await expired.approval("refund")).status == ApprovalStatus.EXPIRED
            cancelled=await submit("cancelled")
            p=await waiting(cancelled)
            await cancelled.cancel()
            await asyncio.to_thread(eventually,lambda:snapshot(d,cancelled.id)["state"]=="cancelled",timeout=110,description="cancelled approval workflow")
            assert (await cancelled.approval("refund")).status == ApprovalStatus.CANCELLED
            rows=[json.loads(line) for line in marker.read_text().splitlines()]
            assert len([r for r in rows if r["kind"]=="effect"]) == 1
            record("approvals",dict(workflow_id=handle.id,approved_arguments={"amount":50,"currency":"USD"},
                rejected_workflow=rejected.id,expired_workflow=expired.id,cancelled_workflow=cancelled.id,
                restart=True,lost_response=True,changed_action_rejected=True,event_not_approval=True,
                resumed_attempts=2,effect_calls=1,concurrency=1,cli_reconciliation=True))
        await asyncio.to_thread(worker.stop)
    if "approval-example" in names:
        worker=d.start_worker(concurrency=1)
        async with AsyncClient(d.server_url,**options) as client:
            handle=await client.workflows.submit(program="durable-approval",version="1.0.0",queue=d.queue,
                data={"amount":100},idempotency_key="public-approval-example")
            await asyncio.to_thread(eventually,lambda:snapshot(d,handle.id)["state"]=="waiting",timeout=110,description="example approval")
            proposal=await handle.approval("refund")
            assert proposal.proposed_arguments=={"amount":100} and proposal.action.arguments=={"amount":50,"currency":"USD"}
            await handle.decide_approval(handle.prepare_approval_decision(proposal,decision_id="public-example-review",decision="approve",reviewer="test-operator"))
            output=await handle.result(timeout=110)
            assert output=={"status":"approved","result":{"simulated":True,"amount":50,"currency":"USD"},
                "proposed_arguments":{"amount":100},"approved_arguments":{"amount":50,"currency":"USD"}},output
            record("approval-example",dict(workflow_id=handle.id,output=output,source="examples/durable-approval/program.py"))
        await asyncio.to_thread(worker.stop)
