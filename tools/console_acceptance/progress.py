"""Real CLI progress recovery across subscriber and owned-service restarts (MIT)."""

import json
import os
from pathlib import Path

from http_acceptance.harness import Process, eventually, exchange
from .fork4 import BRANCH_KEYS, ENTRYPOINTS, PROGRAM, SEQUENCE, TASK_PROGRAM, paginated
from .workflows import event


PROGRESS_PATH = "/v1/executions/progress"
MAX_OUTPUT_BYTES = 4 * 1024 * 1024


def pages(observer, *, final=False):
    with observer.stdout_path.open("rb") as output:
        raw = output.read(MAX_OUTPUT_BYTES + 1)
    assert len(raw) <= MAX_OUTPUT_BYTES, "progress observer exceeded fixture output bound"
    lines = raw.splitlines(keepends=True)
    if final:
        assert not lines or lines[-1].endswith(b"\n"), "partial progress page delivered"
    return [json.loads(line) for line in lines if line.endswith(b"\n")]


def observe(d, kind, identifier, label, *, server=None, cursor=None):
    args = [str(d.binaries / "ledgence"), kind, "progress", "--server", server or d.server_url,
            "--tenant", d.scope["tenant_id"], "--namespace", d.scope["namespace"],
            "--" + kind, identifier, "--follow", "--limit", "1", "--wait-ms", "1000",
            "--poll-interval-ms", "250", "--timeout-ms", "90000"]
    if cursor is not None:
        args += ["--cursor", cursor]
    process = Process(args, d.directory, label, d.environment)
    d.processes.append(process)
    return process


def finish(observer, expected=0):
    code = observer.process.wait(timeout=60)
    assert code == expected, (observer.label, code, str(observer.stderr_path))
    diagnostic = json.loads(observer.stderr_path.read_text())
    if expected:
        assert diagnostic["error"]["code"] == "unavailable", diagnostic
        assert diagnostic["outcome_may_be_unknown"] is False, diagnostic
    return pages(observer, final=True)


def events(observed, scope, target):
    result = []
    for page in observed:
        assert page["scope"] == scope and page["target"] == target
        assert type(page["high_watermark"]) is str
        assert len(page["events"]) <= 100
        for fact in page["events"]:
            assert type(fact["sequence"]) is str
            assert int(fact["sequence"]) <= int(page["high_watermark"])
            result.append(fact)
    return result


def read_all(d, target):
    query = {"scope": d.scope, "target": target, "limit": 2}
    result = []
    for _ in range(200):
        status, _, page = exchange(d.server_url, "POST", PROGRESS_PATH, query)
        assert status == 200, page
        result.append(page)
        if not page["has_more"]:
            return result
        assert page["next_cursor"] != query.get("cursor"), "progress cursor did not advance"
        query["cursor"] = page["next_cursor"]
    raise AssertionError("progress history exceeded fixture page bound")


def checkpoint(path, value):
    # Model a caller-owned durable acknowledgement, not a hidden SDK checkpoint.
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as output:
        json.dump(value, output, indent=2)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def pending_fork(d, root):
    children = {row["command_key"]: row["target_id"]
                for row in d.rows("workflows/children", workflow_id=root, limit=2)}
    if set(children) != set(BRANCH_KEYS + ["prepare:0"]):
        return None
    security = d.api("GET", "workflows/inspect", workflow_id=children["security:0"])
    if security["external_wait_key"] != "progress-gate:0":
        return None
    if d.workflow_status(root)["state"] != "waiting":
        return None
    if any(d.workflow_status(children[key])["state"] != "succeeded"
           for key in BRANCH_KEYS if key != "security:0"):
        return None
    if d.task_status(children["prepare:0"])["state"] != "succeeded":
        return None
    nodes = d.rows("workflows/explorer", workflow_id=root, limit=2)
    joins = [node for node in nodes if node["kind"] == "child_wait"
             and set(node["member_keys"]) == set(BRANCH_KEYS)
             and node["resumed_activation_id"] is None]
    return children if len(joins) == 1 else None


def worker_session(d, activation_id):
    # A workflow decision is durable before its controller attempt settles.
    # Wait for that distinct boundary instead of assuming both writes coincide.
    def settled():
        attempts = d.rows("tasks/attempts", task_id=activation_id, limit=1)
        assert len(attempts) <= 1, "the fixture must not retry an activation"
        if not attempts:
            return None
        attempt = d.api("GET", "attempts/inspect", attempt_id=attempts[0]["attempt_id"])["attempt"]
        if attempt["finished_at"] is None:
            return None
        assert attempt["state"] == "succeeded" and attempt["execution_may_have_started"] is True
        return attempt["worker_session_id"]
    return eventually(settled, timeout=30, description="accepted terminal controller attempt")


def run(d, worker, record, *, publish=None):
    """Return the live replacement worker; caller retains lifecycle ownership."""
    if publish is not None:
        source = Path(__file__).parent
        publish(d, PROGRAM, (source / "fork4_program.py").read_text())
        publish(d, TASK_PROGRAM, (source / "fork4_task.py").read_text())
    command = d.console_submission("progress-fork-recovery", program=PROGRAM)
    command["input"]["correlation_key"] = "progress-fork-recovery"
    command["input"]["data"] = {
        "queue": d.queue, "release": "2026.09", "sequence": SEQUENCE,
        "mode": "approved", "progress_gate": True,
    }
    root = d.api("POST", "workflows", command)["workflow"]["workflow_id"]
    target = {"kind": "workflow", "id": root}
    children = eventually(lambda: pending_fork(d, root), timeout=60,
                          description="four-branch fork durably pending on Security gate")
    security = children["security:0"]
    first_entry, = [node for node in paginated(d, security) if node["kind"] == "entrypoint"]
    old_session = worker_session(d, first_entry["activation_id"])
    old_sessions = {row["worker_session_id"] for row in d.rows("workers")}
    old_pids = {"worker": worker.process.pid, "orchestrator": d.server.process.pid}
    observers = []
    try:
        # Also exercise the real task follower against the fork's completed
        # preparation task. Neither observer is allowed to submit or cancel work.
        task = observe(d, "task", children["prepare:0"], "progress-task")
        observers.append(task)
        task_pages = finish(task)
        task_target = {"kind": "task", "id": children["prepare:0"]}
        assert task_pages[-1]["terminal"] and not task_pages[-1]["has_more"]
        assert events(task_pages, d.scope, task_target) == events(read_all(d, task_target), d.scope, task_target)

        proxy = d.proxy()
        first = observe(d, "workflow", root, "progress-before-disconnect", server=proxy.url)
        observers.append(first)
        positive_wait = lambda body: body.get("wait_ms", 0) > 0
        eventually(lambda: proxy.commands(PROGRESS_PATH, positive_wait), timeout=30,
                   description="observer caught up and completed a real positive-wait request")
        accepted = pages(first)
        accepted_events = events(accepted, d.scope, target)
        event_pages = [page for page in accepted if page["events"]]
        assert len(event_pages) >= 2 and not accepted[-1]["terminal"]
        acknowledged = event_pages[-2]
        saved = {
            "scope": d.scope, "target": target, "cursor": acknowledged["next_cursor"],
            "acknowledged_sequence": acknowledged["events"][-1]["sequence"],
            "observed_sequence": accepted_events[-1]["sequence"],
            "purpose": "intentional caller acknowledgement gap to exercise real redelivery",
        }
        assert int(saved["acknowledged_sequence"]) < int(saved["observed_sequence"])
        checkpoint_path = d.directory / "progress-checkpoint.json"
        checkpoint(checkpoint_path, saved)
        # Only now sever a subsequent real response. No synthetic event/page is
        # injected, and the CLI must stop on this error rather than resubmit.
        disconnected = proxy.lose_once(PROGRESS_PATH, positive_wait, block_after=True)
        assert disconnected.wait(timeout=10), "progress fault was not reached"
        before = finish(first, expected=1)
        assert pending_fork(d, root) == children, "observer failure changed workflow ownership"
        assert all(item["path"] == PROGRESS_PATH and item["status"] == 200 for item in proxy.records)
        dropped = proxy.commands(PROGRESS_PATH, positive_wait)[-1]
        dropped_page = json.loads(dropped["response"])
        assert not dropped_page["events"] and not dropped_page["terminal"]
        (d.directory / "progress-lost-response.json").write_text(json.dumps(dropped_page, indent=2) + "\n")

        worker.stop()
        d.server.stop()
        assert worker.process.poll() is not None and d.server.process.poll() is not None
        d.server, _ = d.start_server()
        worker = d.start_worker()
        newer = eventually(lambda: next((row for row in d.rows("workers")
                           if row["worker_session_id"] not in old_sessions and row["snapshot_sequence"]), None),
                           description="new worker session after both owned services restart")
        new_session = newer["worker_session_id"]
        new_pids = {"worker": worker.process.pid, "orchestrator": d.server.process.pid}
        assert all(old_pids[kind] != new_pids[kind] for kind in old_pids)
        assert new_session != old_session
        assert pending_fork(d, root) == children, "restart duplicated or replaced pending branches"

        recovered = json.loads(checkpoint_path.read_text())
        assert recovered == saved
        resumed_proxy = d.proxy()
        resumed = observe(d, "workflow", root, "progress-after-restart", server=resumed_proxy.url,
                          cursor=recovered["cursor"])
        observers.append(resumed)
        eventually(lambda: resumed_proxy.commands(PROGRESS_PATH, positive_wait), timeout=30,
                   description="resumed observer caught up while the fork is still pending")
        assert pending_fork(d, root) == children
        signal = event(security, "progress-gate:0", "evt-progress-recovery-approved")
        receipt = d.api("POST", "workflows/events", signal)
        duplicate = d.api("POST", "workflows/events", signal)
        assert receipt["already_accepted"] is False and duplicate["already_accepted"] is True
        result = d.workflow_result(root)
        after = finish(resumed)
        assert after[-1]["terminal"] and after[-1]["state"] == "succeeded" and not after[-1]["has_more"]

        # Deduplication belongs to this test consumer, using the public identity
        # contract. The real resumed page must overlap the previously seen facts.
        before_events, after_events = (events(observed, d.scope, target) for observed in (before, after))
        assert [int(fact["sequence"]) for fact in before_events] == list(range(1, int(before_events[-1]["sequence"]) + 1))
        assert [int(fact["sequence"]) for fact in after_events] == list(range(
            int(saved["acknowledged_sequence"]) + 1, int(after_events[-1]["sequence"]) + 1))
        view, redeliveries = {}, 0
        for fact in before_events + after_events:
            key = (d.scope["tenant_id"], d.scope["namespace"], target["kind"], root, fact["sequence"])
            if key in view:
                assert view[key] == fact, "redelivery mutated a durable fact"
                redeliveries += 1
            else:
                view[key] = fact
        assert redeliveries > 0, "the intentional acknowledgement gap did not replay a real fact"
        reference = read_all(d, target)
        authoritative = events(reference, d.scope, target)
        assert list(view.values()) == authoritative, "resumed progress lost, reordered or invented facts"
        assert [int(fact["sequence"]) for fact in authoritative] == list(range(1, int(reference[-1]["high_watermark"]) + 1))
        assert reference[-1]["terminal"] and not reference[-1]["has_more"]
        (d.directory / "progress-reference-pages.json").write_text(json.dumps(reference, indent=2) + "\n")
        (d.directory / "progress-deduplicated-events.json").write_text(json.dumps(list(view.values()), indent=2) + "\n")

        root_nodes = paginated(d, root)
        root_entries = [node for node in root_nodes if node["kind"] == "entrypoint"]
        assert len(root_entries) == len(ENTRYPOINTS)
        assert {node["entrypoint"] for node in root_entries} == set(ENTRYPOINTS)
        fork, = [node for node in root_nodes if node["kind"] == "fork"]
        assert fork["branch_keys"] == BRANCH_KEYS
        final_children = {node["key"]: node["execution"]["id"] for node in root_nodes if node["kind"] == "child"}
        assert all(final_children[key] == identifier for key, identifier in children.items())
        security_nodes = paginated(d, security)
        entries = [node for node in security_nodes if node["kind"] == "entrypoint"]
        assert len(entries) == 2 and all(node["entrypoint"] == "security_review" for node in entries)
        resumed_entry, = [node for node in entries if node["activation_id"] != first_entry["activation_id"]]
        wait, = [node for node in security_nodes if node["kind"] == "external_wait"]
        assert wait["activation_id"] == first_entry["activation_id"]
        assert wait["wake_reason"] == "event" and wait["resumed_activation_id"] == resumed_entry["activation_id"]
        assert worker_session(d, resumed_entry["activation_id"]) == new_session
        output = result["outcome"]["output"]
        assert output["review"]["approved"] is True
        assert output["review"]["draft_id"] == output["draft"]["draft_id"]
        assert set(output["draft"]["checks"]) == set(BRANCH_KEYS)
        assert output["draft"]["prepared"]["sequence"] == SEQUENCE
        assert output["report_id"] == "report-2026.09"
        record("progress-fork-disconnect-restart-resume", {
            "scope": d.scope, "target": target, "pending_children": children,
            "old_pids": old_pids, "new_pids": new_pids, "old_session": old_session, "new_session": new_session,
            "checkpoint": checkpoint_path.name, "acknowledged_sequence": saved["acknowledged_sequence"],
            "observed_sequence_before_disconnect": saved["observed_sequence"],
            "retained_events": len(authoritative), "actual_redeliveries": redeliveries,
            "root_entrypoints": len(root_entries), "forks": 1, "fork_branch_keys": fork["branch_keys"],
            "deduplication_owner": "fixture consumer", "same_pending_fork_after_restart": True,
            "security_activations": [first_entry["activation_id"], resumed_entry["activation_id"]],
            "resumed_activation_worker_session": new_session, "terminal_state": after[-1]["state"],
            "task_observer_events": len(events(task_pages, d.scope, task_target)),
            "observer_exit_codes": [item.process.returncode for item in observers],
            "old_services_exited": True, "database_preserved": True,
        })
        return worker
    finally:
        for observer in reversed(observers):
            observer.cleanup()
        (d.directory / "progress-observer-cleanup.json").write_text(json.dumps({
            "observers": [{"pid": item.process.pid, "exit_code": item.process.returncode,
                           "running": item.process.poll() is None} for item in observers],
            "services_cleanup_owner": "ConsoleDeployment.close",
        }, indent=2) + "\n")
