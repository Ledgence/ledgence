"""Inspect the real fork4 lifecycle through bounded Console HTTP reads (MIT)."""
from pathlib import Path

from http_acceptance.harness import eventually
from .workflows import event
from .explorer import relation_map, require_relation


PROGRAM = "explorer-release-workflow"
TASK_PROGRAM = "explorer-release-task"
CORRELATION = "explorer-native-fork4"
BRANCH_ENTRIES = {
    "security:0": "security_review",
    "tests:0": "test_suite",
    "dependencies:0": "dependency_audit",
    "docs:0": "docs_review",
}
BRANCH_KEYS = list(BRANCH_ENTRIES)
ENTRYPOINTS = ["start", "after_prepare", "publish_draft", "review", "publish_report", "finish"]
TASK_CREATORS = {"prepare:0": "start", "publish-draft:0": "publish_draft",
                 "review-draft:0": "review", "publish-final:0": "publish_report"}
SEQUENCE = 9007199254740993


def paginated(d, workflow_id):
    pages = [d.rows("workflows/explorer", workflow_id=workflow_id, limit=limit) for limit in (1, 2)]
    for rows in pages:
        assert len({row["id"] for row in rows}) == len(rows), "duplicate node across pages"
        assert all(row["kind"] != "phase" for row in rows), "legacy Console node kind"
        assert all(type(row["revision"]) is str and row["revision"].isdigit() for row in rows)
    assert [row["id"] for row in pages[0]] == [row["id"] for row in pages[1]]
    assert relation_map(pages[0]) == relation_map(pages[1]), "page size changed graph relationships"
    return pages[0]


def attempt(d, task_id):
    attempts = d.rows("tasks/attempts", task_id=task_id, limit=1)
    assert len(attempts) == 1, (task_id, attempts)
    identifier = attempts[0]["attempt_id"]
    observed = d.api("GET", "attempts/inspect", attempt_id=identifier)
    return completed_attempt(observed, task_id, identifier)


def completed_attempt(observed, task_id, attempt_id):
    summary = observed["attempt"]
    assert summary["task_id"] == task_id and summary["attempt_id"] == attempt_id
    assert summary["state"] == "succeeded"
    assert summary["finished_at"] is not None
    assert summary["execution_may_have_started"] is True
    assert summary["worker_session_id"]
    # attempts/inspect derives PID/reuse/elapsed from the accepted settlement.
    # It does not join the optional invocation-observation process identity;
    # process_instance_id may therefore be null. A PID is not that identity.
    assert type(observed["process_id"]) is int and observed["process_id"] > 0
    assert type(observed["reused_process"]) is bool
    assert observed["worker_elapsed_ms"] is not None and int(observed["worker_elapsed_ms"]) >= 0
    return summary["attempt_id"]


def submit(d, mode):
    correlation = CORRELATION if mode == "approved" else CORRELATION + "-" + mode
    command = d.console_submission(correlation, program=PROGRAM)
    command["input"]["data"] = {
        "queue": d.queue, "release": "2026.09", "sequence": SEQUENCE, "mode": mode,
    }
    command["input"]["correlation_key"] = correlation
    accepted = d.api("POST", "workflows", command)
    root = accepted["workflow"]["workflow_id"]
    replay = d.api("POST", "workflows", command)
    assert replay["workflow"]["workflow_id"] == root, "idempotent submit duplicated root"
    return root, correlation, command["input"]["data"]


def cancel_branch(d, root):
    child = eventually(lambda: next((row for row in d.rows("workflows/children", workflow_id=root, limit=1)
                                    if row["command_key"] == "security:0"), None),
                       description="fork4 security branch registered")
    identifier = child["target_id"]
    eventually(lambda: d.workflow_status(identifier)["state"] == "waiting",
               description="fork4 security branch persisted external wait")
    d.api("POST", "workflows/cancel", {"workflow_id": identifier})
    d.workflow_result(identifier, "cancelled")


def release_satisfied_join(d, root):
    eventually(lambda: d.api("GET", "workflows/inspect", workflow_id=root)["external_wait_key"] == "join-ready:0",
               description="fork4 parent persisted join gate")
    children = d.rows("workflows/children", workflow_id=root, limit=1)
    branches = [child for child in children if child["command_key"] in BRANCH_KEYS]
    assert len(branches) == 4
    for branch in branches:
        d.workflow_result(branch["target_id"])
    command = event(root, "join-ready:0", "evt-fork4-join-ready")
    first = d.api("POST", "workflows/events", command)
    duplicate = d.api("POST", "workflows/events", command)
    assert first["already_accepted"] is False and duplicate["already_accepted"] is True


def assert_structure(d, root, mode):
    nodes = paginated(d, root)
    entry_nodes = [node for node in nodes if node["kind"] == "entrypoint"]
    entries = {node["entrypoint"]: node for node in entry_nodes}
    successful = mode in ("approved", "join_satisfied")
    names = ENTRYPOINTS if successful else ENTRYPOINTS[:5] if mode == "rejected" else ENTRYPOINTS[:3]
    assert len(entries) == len(names) and set(entries) == set(names)
    assert len(entry_nodes) == len(names) + int(mode == "join_satisfied")
    assert len({node["activation_id"] for node in entry_nodes}) == len(entry_nodes)
    fork, = [node for node in nodes if node["kind"] == "fork"]
    assert fork["key"] == "release-checks:0" and fork["branch_keys"] == BRANCH_KEYS
    assert fork["activation_id"] == entries["start"]["activation_id"]
    assert attempt(d, entries["start"]["activation_id"]) == fork["accepting_attempt_id"]
    children = {node["key"]: node for node in nodes if node["kind"] == "child"}
    expected_tasks = list(TASK_CREATORS)[:4 if successful else 3 if mode == "rejected" else 1]
    assert set(children) == set(BRANCH_KEYS + expected_tasks)
    assert len([node for node in nodes if node["kind"] == "child"]) == len(children)
    task_outputs = {}
    for key, child in children.items():
        identity = child["execution"]
        assert child["availability"] == "available"
        ancestry = d.api("GET", "executions/ancestry", kind=identity["kind"], id=identity["id"])
        assert [row["execution"]["id"] for row in ancestry["path"]] == [root, identity["id"]]
        if key in BRANCH_ENTRIES:
            assert identity["kind"] == "workflow" and child["fork_key"] == fork["key"]
            assert child["program"] == {"id": PROGRAM, "version": "1.0.0"}
            assert child["activation_id"] == entries["start"]["activation_id"]
            branch_relation = require_relation(child, "branch", {"kind": "fork", "key": fork["key"]},
                                               {"kind": "child", "key": key})
            assert branch_relation in fork["relations"], "fork/child evidence has different relation identities"
            branch_state = mode.removeprefix("branch_") if key == "security:0" and mode.startswith("branch_") else "succeeded"
            assert child["state"] == branch_state
            outcome = d.workflow_result(identity["id"], branch_state)["outcome"]
            if branch_state == "succeeded":
                assert outcome["output"]["passed"] is True
            nested = paginated(d, identity["id"])
            nested_entry, = [node for node in nested if node["kind"] == "entrypoint"]
            assert nested_entry["entrypoint"] == BRANCH_ENTRIES[key]
            attempt(d, nested_entry["activation_id"])
            if branch_state == "cancelled":
                wait, = [node for node in nested if node["kind"] == "external_wait"]
                assert wait["closed_at"] is not None
                assert wait["wake_reason"] is None and wait["resumed_activation_id"] is None
                assert not any(relation["kind"] == "resumes" for relation in wait["relations"])
        else:
            assert identity["kind"] == "task" and child["fork_key"] is None
            assert child["state"] == "succeeded"
            assert child["activation_id"] == entries[TASK_CREATORS[key]]["activation_id"]
            task_outputs[key] = d.task_result(identity["id"])["outcome"]["output"]
            attempt(d, identity["id"])
    joins = [node for node in nodes if node["kind"] == "child_wait"]
    assert len(joins) == len(names) - 1
    waits = [("start", ["prepare:0"], "after_prepare"),
             ("after_prepare", BRANCH_KEYS, "publish_draft"),
             ("publish_draft", ["publish-draft:0"], "review"),
             ("review", ["review-draft:0"], "publish_report"),
             ("publish_report", ["publish-final:0"], "finish")]
    for entry, keys, resume in waits[:len(joins)]:
        current = entries[entry]
        target = next(node for node in entry_nodes if node["entrypoint"] == resume)
        join, = [node for node in joins if node["activation_id"] == current["activation_id"]]
        assert set(join["member_keys"]) == set(keys)
        assert join["resume"] == resume
        assert join["resumed_activation_id"] == target["activation_id"]
        assert {relation["source"]["key"] for relation in join["relations"]
                if relation["kind"] == "awaits_terminal"} == set(keys)
        require_relation(join, "resumes", {"kind": "child_wait", "activation_id": join["activation_id"]},
                         {"kind": "entrypoint", "activation_id": target["activation_id"]})
        assert current["decision_kind"] == "suspend" and current["applied_at"] is not None
        assert current["resumed_activation_id"] == target["activation_id"]
    assert entries[names[-1]]["decision_kind"] == ("complete" if successful else "fail")
    # The four-key join is installed in a later activation, after the separate
    # prepare task. Its members retain their original creation activation.
    assert entries["start"]["activation_id"] != entries["after_prepare"]["activation_id"]
    for entry in entry_nodes:
        attempt(d, entry["activation_id"])
    if mode == "join_satisfied":
        before, after = [node for node in entry_nodes if node["entrypoint"] == "after_prepare"]
        assert before["activation_id"] != after["activation_id"]
        assert before["decision_kind"] == "wait" and after["decision_kind"] == "suspend"
        assert before["resumed_activation_id"] == after["activation_id"]
        wake, = [node for node in nodes if node["kind"] == "external_wait"]
        assert wake["key"] == "join-ready:0" and wake["wake_reason"] == "event"
        assert wake["activation_id"] == before["activation_id"]
        assert wake["resumed_activation_id"] == after["activation_id"]
        join, = [node for node in joins if node["activation_id"] == after["activation_id"]]
        assert all(children[key]["terminal_at"] <= join["applied_at"] for key in BRANCH_KEYS)
    assert task_outputs["prepare:0"]["sequence"] == SEQUENCE
    return nodes, children, task_outputs


def run(d, publish, record):
    directory = Path(__file__).parent
    publish(d, PROGRAM, (directory / "fork4_program.py").read_text())
    publish(d, TASK_PROGRAM, (directory / "fork4_task.py").read_text())
    roots = {}
    for mode in ("approved", "rejected", "branch_failed", "branch_cancelled", "join_satisfied"):
        root, correlation, data = submit(d, mode)
        roots[mode] = root
        if mode == "branch_cancelled":
            cancel_branch(d, root)
        if mode == "join_satisfied":
            release_satisfied_join(d, root)
        successful = mode in ("approved", "join_satisfied")
        result = d.workflow_result(root, "succeeded" if successful else "failed")
        nodes, children, outputs = assert_structure(d, root, mode)
        assert d.api("GET", "workflows/input", workflow_id=root)["data"] == data
        discovered = d.rows("executions", correlation_key=correlation, limit=1)
        assert [row["id"] for row in discovered] == [root]
        if successful:
            final = result["outcome"]["output"]
            assert final == outputs["publish-final:0"]
            assert final["draft"] == outputs["publish-draft:0"]
            assert final["review"] == outputs["review-draft:0"]
            assert final["review"]["approved"] is True
            assert final["review"]["draft_id"] == final["draft"]["draft_id"]
            assert set(final["draft"]["checks"]) == set(BRANCH_KEYS)
            assert final["draft"]["prepared"] == outputs["prepare:0"]
            assert final["report_id"] == "report-2026.09"
        else:
            expected_error = "review_rejected" if mode == "rejected" else mode
            assert result["outcome"]["error"]["kind"] == expected_error
            assert "publish-final:0" not in children
            if mode == "rejected":
                assert outputs["review-draft:0"]["approved"] is False
            else:
                assert "publish-draft:0" not in children
        record("explorer-fork4-" + mode, {
            "workflow_id": root, "correlation_key": correlation, "nodes": len(nodes),
            "children": {key: child["execution"] for key, child in children.items()},
            "branch_keys": BRANCH_KEYS, "prepare_is_fork_member": False,
            "persisted_keys_joined_from_later_activation": True,
            "join_already_satisfied_verified": mode == "join_satisfied",
            "pagination_limits": [1, 2], "exact_sequence": SEQUENCE,
            "state": result["workflow"]["state"],
        })
    return roots
