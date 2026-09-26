"""Console projections over the existing checkpoint and owned-tree fixtures."""

import json

from http_acceptance.harness import eventually


def workflow_command(d, key, program="owned-controller", mode="tree", **extra):
    command = d.console_submission(key, program=program)
    command["input"]["correlation_key"] = "same-business-correlation"
    command["input"]["data"] = dict(tag=key, mode=mode, role="parent", queue=d.queue,
        marker=str(d.directory / "workflow-markers.jsonl"), **extra)
    return command


def submit(d, command):
    return d.api("POST", "workflows", command)["workflow"]["workflow_id"]


def children(d, workflow_id):
    return d.rows("workflows/children", workflow_id=workflow_id, limit=1)


def tree_ids(d, root):
    child = eventually(lambda: next((row["target_id"] for row in children(d, root)
                                    if row["kind"] == "workflow"), None), description="owned child workflow")
    grandchild = eventually(lambda: next((row["target_id"] for row in children(d, child)
                                         if row["kind"] == "workflow"), None), description="owned grandchild workflow")
    return {"parent": root, "child": child, "grandchild": grandchild}


def waiting(d, workflow_id):
    eventually(lambda: d.workflow_status(workflow_id)["state"] == "waiting",
               description="durably suspended workflow")


def event(workflow_id, key, identifier, **data):
    return {"workflow_id": workflow_id, "key": key, "event": {
        "specversion": "1.0", "id": identifier, "source": "urn:console-acceptance",
        "type": "approval.received.v1", "datacontenttype": "application/json", "data": data,
    }}


def workflow_scenarios(d, delay, record):
    local = submit(d, workflow_command(d, "local-steps", program="workflow-controller",
                                     mode="local", count=3, url=delay.url))
    result = d.workflow_result(local)
    assert result["outcome"]["output"] == {"count": 3, "total": 3, "overlap": True}
    activations = d.rows("workflows/activations", workflow_id=local, limit=1)
    assert len(activations) == 1
    steps = d.rows("workflows/local-steps", workflow_id=local,
                   activation_id=activations[0]["activation_id"], limit=1)
    assert {step["step_key"] for step in steps} == {"fetch-0", "fetch-1", "fetch-2"}
    assert not children(d, local)
    d.api("GET", "workflows/local-steps", workflow_id=local,
          activation_id="wrong-activation", expected=404)
    record("workflow-recorded-local-steps", {"workflow_id": local, "steps": len(steps),
                                             "distributed_children": 0})

    root = submit(d, workflow_command(d, "owned-console-tree", url=delay.url))
    ids = tree_ids(d, root)
    for identifier in ids.values():
        waiting(d, identifier)
    unrelated = d.console_submission("same-correlation-no-ownership")
    unrelated["input"]["correlation_key"] = "same-business-correlation"
    unrelated = d.api("POST", "tasks", unrelated)
    d.task_result(unrelated["task_id"])
    parent_children = children(d, root)
    assert len(parent_children) == 2
    assert {row["kind"] for row in parent_children} == {"task", "workflow"}
    assert unrelated["task_id"] not in {row["target_id"] for row in parent_children}
    assert d.api("GET", "workflows/inspect", workflow_id=root)["child_wait"] is not None
    branch_status = d.workflow_status(ids["child"])
    assert branch_status["parent_workflow_id"] == root and branch_status["root_workflow_id"] == root
    pending = d.rows("workflows/waits", workflow_id=ids["grandchild"])
    assert any(row["kind"] == "event" and row["wait_key"] == "approve" for row in pending)
    command = event(ids["grandchild"], "approve", "evt-console-approval", approved=True,
                    exact=9007199254740993)
    accepted = d.api("POST", "workflows/events", command)
    duplicate = d.api("POST", "workflows/events", command)
    assert accepted["already_accepted"] is False and duplicate["already_accepted"] is True
    assert accepted["accepted_at"] == duplicate["accepted_at"]
    conflicting = json.loads(json.dumps(command))
    conflicting["event"]["data"]["approved"] = False
    d.api("POST", "workflows/events", conflicting, expected=409)
    eventually(lambda: any(row["kind"] == "timer" for row in d.rows("workflows/waits", workflow_id=ids["grandchild"])),
               description="event resume installs durable timer")
    result = d.workflow_result(root)
    assert result["outcome"]["output"]["branch"]["leaf"] == {"approved": True, "value": 7}
    for identifier in ids.values():
        assert d.workflow_status(identifier)["state"] == "succeeded"
        assert d.rows("workflows/history", workflow_id=identifier, limit=1)
    assert all(row["consumed"] for row in children(d, root))
    assert len(d.rows("workflows/activations", workflow_id=root, limit=1)) >= 2
    record("workflow-owned-tree-events-and-timer", {"ids": ids, "event_duplicate": True,
        "event_conflict": True, "unrelated_same_correlation_task": unrelated["task_id"],
        "false_correlation_link_excluded": True, "consumed_parent_children": 2})

    gate = d.directory / "cascade-child-gate"
    d.gates.add(gate)
    cancelled_root = submit(d, workflow_command(d, "owned-console-cancel", mode="cancel",
                                                url=delay.url, gate=str(gate)))
    cancelled = tree_ids(d, cancelled_root)
    for identifier in cancelled.values():
        waiting(d, identifier)
    d.api("POST", "workflows/events", event(cancelled["child"], "block-task", "evt-block-child"))
    blocking = eventually(lambda: next((row["target_id"] for row in children(d, cancelled["child"])
                                       if row["kind"] == "task" and row["command_key"] == "blocking"), None),
                          description="owned blocking child registered")
    eventually(lambda: d.task_status(blocking)["state"] == "active", description="owned task active")
    d.api("POST", "workflows/cancel", {"workflow_id": cancelled_root})
    for identifier in cancelled.values():
        d.workflow_result(identifier, "cancelled")
    d.task_result(blocking, "cancelled")
    gate.touch()
    record("workflow-cancellation-cascade", {"ids": cancelled, "active_child_task": blocking,
                                            "all_terminal_cancelled": True})
    return root
