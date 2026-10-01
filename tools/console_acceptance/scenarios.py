"""Console API acceptance with real packages, workers and authoritative storage."""

import http.client
import json
import math
import threading
import urllib.parse
from html.parser import HTMLParser

from http_acceptance.harness import PROGRAM, eventually, exchange
from .harness import ObservationProxy, raw_exchange
from .workflows import workflow_scenarios
from .explorer import run as explorer_scenario
from .fork4 import run as fork4_scenario


def equivalent(left, right):
    assert type(left) is type(right), (type(left), type(right))
    if isinstance(left, dict):
        assert left.keys() == right.keys()
        for key in left:
            equivalent(left[key], right[key])
    elif isinstance(left, list):
        assert len(left) == len(right)
        for first, second in zip(left, right):
            equivalent(first, second)
    else:
        assert left == right, (left, right)
        if isinstance(left, float) and left == 0:
            assert math.copysign(1, left) == math.copysign(1, right)


def static_and_contract(d, record):
    deep_links = ("/console/", "/console/executions/unknown-id", "/console/workflows/unknown-id",
                  "/console/programs", "/console/programs/invoice", "/console/programs/invoice/versions/1.0.0",
                  "/console/workers/unknown-id", "/console/agents/invoice/versions/1.0.0",
                  "/console/agents/invoice", "/console/executions/task%2F%20%25%C3%A9",
                  "/console/workflows/literal%252F", "/console/workers/worker%2B%25")
    for path in deep_links:
        status, headers, body = raw_exchange(d.server_url, "GET", path)
        assert status == 200 and b"<html" in body.lower(), (path, status)
        cache = {k.lower(): v for k, v in headers.items()}["cache-control"]
        assert cache == "no-store", cache
    for path in ("/console/assets/missing.js", "/console/not-a-route", "/v1/not-a-route"):
        status, _, _ = raw_exchange(d.server_url, "GET", path)
        assert status == 404, (path, status)
    manifest = json.loads((d.console_dist / "console-manifest.json").read_text())
    assert manifest["console_contract_version"] == 4
    for asset in manifest["assets"]:
        status, _, body = raw_exchange(d.server_url, "GET", "/console/" + asset["path"])
        assert status == 200 and body == (d.console_dist / asset["path"]).read_bytes(), asset["path"]
    status, _, notice_page = raw_exchange(d.server_url, "GET", "/console/notices/")
    assert status == 200 and notice_page == (d.console_dist / "notices/index.html").read_bytes()

    class NoticeLinks(HTMLParser):
        def __init__(self):
            super().__init__()
            self.links = set()

        def handle_starttag(self, tag, attributes):
            if tag in ("a", "link"):
                self.links.update(value for name, value in attributes
                                  if name == "href" and value.startswith("/console/notices/"))

    notices = NoticeLinks()
    notices.feed(notice_page.decode("utf-8"))
    assert notices.links and any("%40" in link for link in notices.links)
    for link in sorted(notices.links):
        relative = urllib.parse.unquote(link.removeprefix("/console/"))
        status, _, body = raw_exchange(d.server_url, "GET", link)
        assert status == 200 and body == (d.console_dist / relative).read_bytes(), link
    config = d.api("GET", "config")
    assert config["contract_version"] == 4
    assert config["instance_id"] == d.instance["instance_id"]
    assert all(config["capabilities"].values()), config
    assert "scope" not in config and "tenant_id" not in config and "namespace" not in config
    for path in ("config?tenant_id=other", "tasks?limit=1&limit=2", "tasks?tenant_id=other",
                 "tasks?limit=0", "tasks?limit=101", "tasks?cursor=corrupt", "tasks?%GG=value"):
        d.api("GET", path, expected=400)
    d.api("POST", "tasks", b'{"idempotency_key":"duplicate","idempotency_key":"changed","input":{}}', expected=400)
    foreign = d.submission("foreign")
    foreign["input"]["namespace"] = "foreign"
    assert exchange(d.server_url, "POST", "/v1/tasks", foreign)[0] in (400, 404)
    assert d.sql("SELECT count(*) FROM tasks") == "0"
    record("static-and-strict-contract", {"assets": len(manifest["assets"]), "deep_links": len(deep_links),
                                         "notice_links": len(notices.links), "instance_id": config["instance_id"], "strict_queries": True})


def register_packages(d, workflow_gate, record):
    alternate = PROGRAM + "\n_original_handle = handle\ndef handle(event):\n    result = _original_handle(event)\n    return None if event['data'].get('mode') == 'null' else result\n"
    d.publish("alternate", "1.0.0", program_source=alternate)
    controller = (d.root / "examples/checkpoint-workflow/controller/program.py").read_text()
    publish = workflow_gate["publish"]
    publish(d, "workflow-controller", controller + "\n" + workflow_gate["FIXTURE"])
    publish(d, "workflow-io", workflow_gate["CHILD"])
    publish(d, "owned-controller", (d.root / "tools/workflow_acceptance/owned_program.py").read_text())
    programs = {"invoice": "task", "alternate": "task", "workflow-io": "task",
                "workflow-controller": "workflow", "owned-controller": "workflow"}
    for name, kind in programs.items():
        command = {"program": {"id": name, "version": "1.0.0"},
                   "metadata": {"display_name": name, "kind": kind}}
        first = d.api("POST", "programs/register", command)
        assert first["already_registered"] is False
        replay = d.api("POST", "programs/register", command)
        assert replay["already_registered"] is True
        assert replay["version"]["descriptor"] == first["version"]["descriptor"]
        detail = d.api("GET", "programs/inspect", program_id=name, version="1.0.0")
        assert detail["version"]["descriptor"]["program"] == command["program"]
    catalog = d.rows("programs", limit=2)
    assert {row["program_id"] for row in catalog} == programs.keys()
    assert d.sql("SELECT count(*) FROM tasks") == "0", "registration executed a program"
    assert not d.marker.exists(), "registration ran fixture code"
    record("catalog-before-execution", {"programs": list(programs), "tasks_created": 0,
                                        "registration_replayed": True, "catalog_page_size": 2})


def uncertain_submission_and_queued_cancel(d, record):
    proxy = d.proxy()
    command = d.console_submission("uncertain-submit")
    lost = proxy.lose_once("/v1/console/tasks")
    try:
        d.api("POST", "tasks", command, base=proxy.url)
        raise AssertionError("expected a response lost after commit")
    except (OSError, http.client.HTTPException):
        pass
    assert lost.is_set()
    accepted = d.api("POST", "tasks", command)
    replay = d.api("POST", "tasks", command)
    assert accepted["task_id"] == replay["task_id"]
    changed = json.loads(json.dumps(command))
    changed["input"]["data"]["changed"] = True
    d.api("POST", "tasks", changed, expected=409)
    run_again = dict(command, idempotency_key="run-again-explicit-new-key")
    another = d.api("POST", "tasks", run_again)
    assert another["task_id"] != accepted["task_id"]
    assert d.sql("SELECT count(*) FROM tasks") == "2"
    lost_cancel = proxy.lose_once("/v1/console/tasks/cancel")
    try:
        d.api("POST", "tasks/cancel", {"task_id": accepted["task_id"]}, base=proxy.url)
        raise AssertionError("expected a committed cancellation response loss")
    except (OSError, http.client.HTTPException):
        pass
    assert lost_cancel.is_set()
    status = d.task_status(accepted["task_id"])
    assert status["state"] == "cancelled" and status["attempt_count"] == 0
    d.api("POST", "tasks/cancel", {"task_id": another["task_id"]})
    d.task_result(another["task_id"], "cancelled")
    page = d.api("GET", "tasks", limit=1)
    assert page["next_cursor"]
    d.api("GET", "tasks", limit=1, state="cancelled", cursor=page["next_cursor"], expected=400)
    record("uncertain-commands-and-queued-cancel", {"original_task": accepted["task_id"],
        "run_again_task": another["task_id"], "single_key_one_task": True, "lost_cancel_reconciled_by_get": True})


def wait_slot(d, session_id, predicate, description):
    def inspect():
        detail = d.worker_detail(session_id)
        return detail if predicate(detail) else None
    return eventually(inspect, timeout=15, description=description)


def execution_and_reporting(d, record):
    proxy = ObservationProxy(d.server_url)
    d.proxies.append(proxy)
    worker = d.start_worker(proxy.url)
    session = eventually(lambda: next((row for row in d.rows("workers") if row["snapshot_sequence"]), None),
                         description="initial connected observation")["worker_session_id"]
    exact = {"u64": 18446744073709551615, "above_js_integer": 9007199254740993, "signed_zero": -0.0,
             "float": 1.0, "null": None, "list": [True, "0007", "18446744073709551615"],
             "html": "<script>not executable</script>", "unicode": "Café 東京", "tenant_id": "user-owned"}
    command = d.console_submission("lossless-and-warm", exact=exact)
    first = d.api("POST", "tasks", command)
    result = d.task_result(first["task_id"])
    equivalent(result["outcome"]["output"]["event"]["data"], command["input"]["data"])
    detail = d.api("GET", "tasks/inspect", task_id=first["task_id"])
    equivalent(detail["input"]["data"], command["input"]["data"])
    first_slot = wait_slot(d, session, lambda value: value["slots"]["items"][0]["state"] == "warm", "warm process observation")
    identity = first_slot["slots"]["items"][0]["process_instance_id"]
    pid = result["outcome"]["output"]["pid"]
    second = d.api("POST", "tasks", d.console_submission("warm-reuse"))
    second_result = d.task_result(second["task_id"])
    assert second_result["outcome"]["output"]["pid"] == pid
    baseline = d.worker_detail(session)["worker"]["snapshot_sequence"]
    reused = wait_slot(d, session, lambda value: int(value["worker"]["snapshot_sequence"]) > int(baseline), "new warm reuse observation")
    assert reused["slots"]["items"][0]["process_instance_id"] == identity
    replacement = d.api("POST", "tasks", d.console_submission("replace-program", program="alternate", mode="null"))
    null_result = d.task_result(replacement["task_id"])
    assert null_result["outcome"]["output"] is None
    replaced = wait_slot(d, session, lambda value: value["slots"]["items"][0]["state"] == "warm"
                        and value["slots"]["items"][0]["process_instance_id"] != identity, "actual program replacement")
    assert replaced["slots"]["items"][0]["slot_id"] == 0
    for mode, field in (("business", "application_error"), ("interrupt", "error")):
        failed = d.api("POST", "tasks", d.console_submission(f"failed-{mode}", mode=mode))
        d.task_result(failed["task_id"], "failed")
        attempts = d.rows("tasks/attempts", task_id=failed["task_id"], limit=1)
        assert len(attempts) == 1
        attempt = d.api("GET", "attempts/inspect", attempt_id=attempts[0]["attempt_id"])
        assert attempt[field] is not None, attempt
        assert d.rows("tasks/history", task_id=failed["task_id"], limit=1)
        assert "lease_id" not in json.dumps(attempt)
    gate = d.directory / "reporting-task-gate"
    active = d.api("POST", "tasks", d.console_submission("active-reporting", mode="gate", gate=str(gate)))
    d.started(active["task_id"])
    executing = wait_slot(d, session, lambda value: value["slots"]["items"][0]["task_id"] == active["task_id"], "current attempt link")
    slot = executing["slots"]["items"][0]
    assert slot["state"] == "executing" and slot["attempt_id"] and slot["consumer_id"] == 0
    proxy.block_reporting.set()
    stale = eventually(lambda: (value if (value := d.worker_detail(session))["worker"]["freshness"] == "stale" else None),
                       timeout=23, description="server-clock stale observation")
    assert stale["worker"]["occupied_process_slots"] == 1
    assert stale["slots"]["items"][0]["state"] == "executing"
    gate.touch()
    d.task_result(active["task_id"])
    assert proxy.failed_reports >= 2
    after = d.worker_detail(session)
    assert after["worker"]["snapshot_sequence"] == stale["worker"]["snapshot_sequence"]
    assert after["worker"]["occupied_process_slots"] == 1, "stale telemetry fabricated free capacity"
    assert d.task_status(active["task_id"])["state"] == "succeeded"
    proxy.block_reporting.clear()
    wait_slot(d, session, lambda value: value["worker"]["freshness"] == "fresh"
              and int(value["worker"]["snapshot_sequence"]) > int(stale["worker"]["snapshot_sequence"]), "reporting recovery")
    record("execution-and-reporting", {"worker_session_id": session, "first_process_instance_id": identity,
        "replacement_process_instance_id": replaced["slots"]["items"][0]["process_instance_id"],
        "u64_exact": exact["u64"], "null_output": True, "failure_kinds": ["application", "execution"],
        "slow_failed_reports": proxy.failed_reports, "settled_while_reporting_failed": True})
    return worker, session, first["task_id"]


def active_cancel_and_race(d, record):
    cancelled = []
    for race in (False, True):
        gate = d.directory / f"cancel-gate-{race}"
        task = d.api("POST", "tasks", d.console_submission(f"cancel-active-{race}", mode="gate", gate=str(gate)))
        d.started(task["task_id"])
        assert d.task_status(task["task_id"])["state"] == "active"
        if race:
            release = threading.Thread(target=gate.touch)
            release.start()
        d.api("POST", "tasks/cancel", {"task_id": task["task_id"]})
        if race:
            release.join(timeout=5)
        final = eventually(lambda: (value if (value := d.task_status(task["task_id"]))["state"] in
                           ("cancelled", "succeeded") else None), description="active cancellation reconciliation")
        assert final["state"] == "cancelled" or race
        assert len(d.invocations(task["task_id"])) == 1
        cancelled.append({"task_id": task["task_id"], "race": race, "state": final["state"]})
    record("active-cancellation-and-completion-race", cancelled)


def run(d, delay, workflow_gate, record):
    static_and_contract(d, record)
    register_packages(d, workflow_gate, record)
    uncertain_submission_and_queued_cancel(d, record)
    worker, old_session, task_id = execution_and_reporting(d, record)
    active_cancel_and_race(d, record)
    workflow_id = workflow_scenarios(d, delay, record)
    explorer_scenario(d, workflow_gate["publish"], record)
    fork4_scenario(d, workflow_gate["publish"], record)
    old_result = d.api("GET", "tasks/result", task_id=task_id)
    worker.stop()
    d.server.stop()
    d.server, _ = d.start_server()
    equivalent(d.api("GET", "tasks/result", task_id=task_id)["outcome"], old_result["outcome"])
    assert d.workflow_result(workflow_id)["workflow"]["state"] == "succeeded"
    assert len(d.rows("programs")) == 5
    assert d.rows("tasks/history", task_id=task_id)
    replacement = d.start_worker()
    newer = eventually(lambda: next((row for row in d.rows("workers") if row["worker_session_id"] != old_session
                      and row["snapshot_sequence"]), None), description="replacement connected session")
    assert d.worker_detail(old_session)["worker"]["worker_session_id"] == old_session
    assert newer["worker_session_id"] != old_session
    replacement.stop()
    record("native-restart-retains-authoritative-state", {"task_id": task_id, "workflow_id": workflow_id,
        "old_session": old_session, "new_session": newer["worker_session_id"], "catalog_programs": 5,
        "database_preserved": True, "containers_recreated": False})
