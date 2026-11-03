"""Independent request counts and durable authority, including lost replies (MIT)."""
import json
from http_acceptance.harness import eventually
from .harness import START, FINISH
from .public_surfaces import public_surfaces


def complete(d, provider, key):
    workflow = d.begin(key, provider)
    d.settled(workflow)
    assert provider.census()["requests"][key] == 1
    assert d.admission(workflow)["state"] == "finished"
    return workflow


def parked_restart(d, provider):
    d.policy(enabled=False)
    worker = d.worker()
    workflow = d.begin("restart-invoice", provider)
    eventually(lambda: d.admission(workflow), description="admission persisted before restart")
    assert d.status(workflow)["state"] == "waiting"
    assert not provider.census()["requests"]
    # An unrelated ordinary task proves the only worker process is reusable.
    task = d.submit(d.submission("ordinary-while-parked"))
    eventually(lambda: d.task(task["task_id"])["state"] == "succeeded", description="ordinary work while admission parked")
    d.server.stop(); d.start_server()
    assert d.status(workflow)["state"] == "waiting"
    d.policy()
    d.settled(workflow)
    assert provider.census()["requests"]["restart-invoice"] == 1
    worker.stop()
    surfaces = public_surfaces(d, workflow)
    return {"workflow": workflow, "parked_after_restart": True, "ordinary_work_completed": True, "public_surfaces": surfaces}


def competing_capacity(d, provider):
    d.policy(capacity=1)
    first, second = d.worker(), d.worker()
    provider.hold("held-invoice")
    workflow = d.begin("held-invoice", provider)
    eventually(lambda: provider.census()["active"] == 1, description="first callback occupies capacity")
    waiting = d.begin("waiting-invoice", provider)
    eventually(lambda: d.admission(waiting), description="second callback waits durably")
    assert d.resource()["occupied"] == 1 and "waiting-invoice" not in provider.census()["requests"]
    provider.release("held-invoice")
    d.settled(workflow); d.settled(waiting)
    assert provider.census()["peak"] == 1
    assert d.resource()["occupied"] == 0
    first.stop(); second.stop()
    return {"workflows": [workflow, waiting], "peak_http_requests": 1}


def lost_finish(d, provider):
    d.policy()
    proxy = d.authenticated_proxy()
    lost = proxy.lose_once(FINISH)
    worker = d.worker(proxy)
    workflow = complete(d, provider, "lost-finish")
    assert lost.is_set()
    commands = proxy.commands(FINISH)
    assert len(commands) >= 2 and all(row["body"] == commands[0]["body"] for row in commands)
    assert d.resource()["occupied"] == 0
    worker.stop()
    return {"workflow": workflow, "finish_exchanges": len(commands), "callback_requests": 1}


def lost_start(d, provider):
    d.policy()
    proxy = d.authenticated_proxy()
    lost = proxy.lose_once(START)
    worker = d.worker(proxy)
    workflow = d.begin("lost-start", provider, attempts=1)
    eventually(lost.is_set, description="Start committed but response lost")
    eventually(lambda: d.status(workflow)["state"] in ("failed", "cancelled"), description="uncertain activation fails closed")
    assert "lost-start" not in provider.census()["requests"]
    assert len(proxy.commands(START)) == 1
    assert d.resource()["occupied"] == 1
    d.release(workflow)
    assert d.resource()["occupied"] == 0
    worker.stop()
    return {"workflow": workflow, "start_exchanges": 1, "callback_requests": 0, "operator_release_replayed": True}


def lost_provider_response(d, provider):
    d.policy()
    provider.drop_response("lost-provider-response")
    worker = d.worker()
    workflow = d.begin("lost-provider-response", provider)
    eventually(lambda: provider.census()["completed"].get("lost-provider-response") == 1,
               description="provider completed work before losing its response")
    eventually(lambda: d.status(workflow)["state"] == "waiting" and d.admission(workflow)["source"] is not None,
               description="retry parks the same uncertain operation without another call")
    census = provider.census()
    assert census["requests"]["lost-provider-response"] == 1
    assert census["completed"]["lost-provider-response"] == 1
    snapshot = d.admission(workflow)
    assert snapshot["source"] is not None and snapshot["finish"] is None
    assert d.resource()["occupied"] == 1
    d.release(workflow)
    assert d.resource()["occupied"] == 0
    eventually(lambda: d.status(workflow)["state"] == "failed", description="operator release closes unknown work without inventing success")
    assert provider.census()["requests"]["lost-provider-response"] == 1
    worker.stop()
    return {"workflow": workflow, "callback_requests": 1, "provider_completed": True,
            "unknown_completion_retained_until_operator_release": True}


def expired_offer(d, provider):
    d.policy(enabled=False)
    worker = d.worker()
    workflow = d.begin("expired-offer", provider)
    eventually(lambda: d.admission(workflow), description="initial durable wait")
    worker.stop()
    d.policy(offer_ms=1000)
    eventually(lambda: d.admission(workflow)["state"] == "awaiting_rewait", timeout=15, description="unstarted offer expires without a worker")
    assert d.resource()["occupied"] == 0
    worker = d.worker()
    d.settled(workflow)
    assert provider.census()["requests"]["expired-offer"] == 1
    worker.stop()
    return {"workflow": workflow, "old_offer_did_not_authorize": True}


def retained_result(d, provider):
    d.policy()
    proxy = d.authenticated_proxy()
    lost = proxy.lose_once(FINISH, block_after=True)
    worker = d.worker(proxy)
    workflow = d.begin("accepted-before-crash", provider)
    eventually(lost.is_set, description="Finish committed before worker crash")
    assert d.admission(workflow)["state"] == "finished"
    worker.kill()
    replacement = d.worker()
    d.settled(workflow)
    assert provider.census()["requests"]["accepted-before-crash"] == 1
    assert d.resource()["occupied"] == 0
    replacement.stop()
    return {"workflow": workflow, "accepted_result_reused_after_worker_loss": True}


def quarantine(d, provider):
    # Establish an actual offer before changing deployment policy. Observing a
    # denied Start proves quarantine is checked at authorization, not just offer.
    d.policy(enabled=False)
    worker = d.worker()
    offered = d.begin("quarantined-offer", provider, attempts=1)
    eventually(lambda: d.admission(offered), description="pre-restore wait")
    worker.stop()
    d.policy(offer_ms=30000)
    eventually(lambda: d.admission(offered)["state"] == "offered", description="pre-restore live offer")
    d.server.stop(); d.dispatch_policy("quarantined"); d.start_server()
    proxy = d.authenticated_proxy()
    worker = d.worker(proxy)
    denied = eventually(lambda: proxy.commands(START), description="worker attempts old offer under quarantine")
    assert all(row["status"] == 503 for row in denied)
    eventually(lambda: d.status(offered)["state"] == "failed", description="quarantined old offer fails closed")
    assert d.admission(offered)["source"] is None
    assert "quarantined-offer" not in provider.census()["requests"]
    workflow = d.begin("quarantined-new", provider)
    eventually(lambda: d.admission(workflow), description="new occurrence in restored quarantine")
    # Confirm quarantine maintenance deferred cleanup to the original
    # deadline while retaining normal scheduling readiness; an immediate no-call assertion would race the scheduler.
    eventually(lambda: d.sql("SELECT r.quarantined_due_ms >= a.deadline_at_ms FROM admission_resources r JOIN admission_requests a USING(tenant_id,namespace,resource_name) WHERE a.workflow_id='" + workflow + "'") == "t", description="quarantined scheduler observed new demand")
    assert d.admission(workflow)["source"] is None
    assert "quarantined-new" not in provider.census()["requests"]
    d.server.stop(); d.dispatch_policy("enabled"); d.start_server()
    d.settled(workflow)
    worker.stop()
    return {"workflow": workflow, "preexisting_offer": offered, "old_start_denied": True, "new_demand_processed_while_quarantined": True}


CASES = [parked_restart, competing_capacity, lost_finish, lost_start, lost_provider_response, expired_offer, retained_result, quarantine]
