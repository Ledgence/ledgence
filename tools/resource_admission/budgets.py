"""Qualify shared run budgets at the worker-ready and child-creation boundaries (MIT)."""

import json
import os
import subprocess
from pathlib import Path

from http_acceptance.harness import eventually
from mcp_acceptance.harness import McpSession
from workflow_acceptance.owned_scenarios import quote

from .harness import AdmissionDeployment


class BudgetDeployment(AdmissionDeployment):
    def server_args(self, config):
        args = super().server_args(config)
        if hasattr(self, "console_dist"):
            args += ["--console-dir", str(self.console_dist)]
        return args

    def verify_browser(self, child, root, provider):
        env = dict(
            os.environ,
            LEDGENCE_CONSOLE_URL=self.server_url + "/console/",
            LEDGENCE_CONSOLE_LIVE_SUITE="run-budgets",
            LEDGENCE_BUDGET_CHILD=child,
            LEDGENCE_BUDGET_ROOT=root,
            LEDGENCE_BUDGET_QUEUE=self.queue,
            LEDGENCE_BUDGET_READER_FILE=str(self.reader),
            LEDGENCE_BUDGET_OPERATOR_FILE=str(self.operator),
            LEDGENCE_BUDGET_PROVIDER_URL=provider.url,
        )
        subprocess.run(
            self.browser_command, cwd=self.root, env=env, check=True, timeout=240
        )

    def publish_example(self):
        super().publish_example()
        self.publish(
            "shared-run-budget",
            "1.0.0",
            runtime_protocol=3,
            program_source=(
                self.root / "examples/shared-run-budget/program.py"
            ).read_text(),
        )

        self.publish(
            "budget-task",
            "1.0.0",
            program_source=(
                self.root / "tools/worker_poll_plan/program.py"
            ).read_text(),
        )
        self.publish(
            "budget-usage",
            "1.0.0",
            runtime_protocol=3,
            program_source=Path(__file__)
            .with_name("budget_usage_program.py")
            .read_text(),
        )

    def submit_review(self, key, provider, policy):
        command = self.submission(
            key,
            program="shared-run-budget",
            endpoint=provider.url,
            tickets=[key + "-invoice", key + "-address"],
            queue=self.queue,
        )
        command["input"]["budget"] = policy
        root = self.checked("POST", "/v1/workflows", command)["workflow_id"]
        return root, command

    def budget_query(self, workflow):
        return {
            "scope": self.scope,
            "subject": {"kind": "workflow", "workflow_id": workflow},
        }

    def snapshot(self, workflow):
        return self.checked("POST", "/v1/budgets/inspect", self.budget_query(workflow))

    def entries(self, workflow):
        rows, before = [], None
        while True:
            page = self.checked(
                "POST",
                "/v1/budgets/entries",
                {
                    **self.budget_query(workflow),
                    "before": before,
                    "limit": 1,
                },
            )
            assert len(page["items"]) <= 1
            rows.extend(page["items"])
            before = page["next"]
            if before is None:
                break
            assert page["items"] and before == page["items"][-1]["sequence"]
        sequences = [int(row["sequence"]) for row in rows]
        assert sequences == sorted(set(sequences), reverse=True)
        return rows

    def terminal(self, workflow):
        return eventually(
            lambda: (
                self.status(workflow)["state"] in ("succeeded", "failed", "cancelled")
            ),
            timeout=90,
            description="budgeted workflow settles",
        )

    def child_ids(self, root):
        return json.loads(
            self.sql(
                "SELECT coalesce(json_agg(workflow_id ORDER BY workflow_id),'[]') "
                "FROM workflow_runs WHERE parent_workflow_id=" + quote(root)
            )
        )


def last_call(d, provider):
    d.policy(enabled=False, capacity=2)
    workers = [d.worker() for _ in range(3)]
    root, command = d.submit_review(
        "last-call", provider, {"max_calls": 1, "max_delegations": 2}
    )

    def both_parked():
        return (
            d.sql(
                "SELECT count(*) FROM admission_requests a JOIN workflow_runs w USING(workflow_id) "
                "WHERE w.parent_workflow_id=" + quote(root) + " AND a.state='waiting'"
            )
            == "2"
        )

    eventually(
        both_parked,
        timeout=60,
        description="two descendants parked with no call permission",
    )
    assert d.snapshot(root)["calls"] == "0" and d.snapshot(root)["delegations"] == "2"
    assert not provider.census()["requests"]
    for worker in workers:
        worker.stop()
    d.server.stop()
    d.start_server()
    for _ in range(3):
        d.worker()
    tickets = command["input"]["data"]["tickets"]
    for ticket in tickets:
        provider.hold(ticket)
    try:
        d.policy(capacity=2)
        eventually(
            lambda: (
                provider.census()["active"] == 1
                and any(e["reason"] == "calls_exhausted" for e in d.entries(root))
            ),
            timeout=20,
            description="one external request in flight and competing branch refused",
        )
        d.server.kill()
        d.start_server()
        assert d.snapshot(root)["calls"] == "1"
    finally:
        for ticket in tickets:
            provider.release(ticket)
    d.terminal(root)
    assert d.status(root)["state"] == "succeeded"
    winner_and_denial = d.entries(root)
    calls = [e for e in winner_and_denial if e["action"] == "call"]
    assert sorted(e["outcome"] for e in calls) == ["authorized", "denied"], calls
    assert [e["reason"] for e in calls if e["outcome"] == "denied"] == [
        "calls_exhausted"
    ]
    assert (
        len(
            [
                e
                for e in winner_and_denial
                if e["action"] == "delegation" and e["outcome"] == "authorized"
            ]
        )
        == 2
    )
    assert sum(provider.census()["requests"].values()) == 1
    ids = d.child_ids(root)
    assert len(ids) == 2
    for child in ids:
        view = d.snapshot(child)
        assert view["root"] == {"kind": "workflow", "workflow_id": root}
        assert view["calls"] == "1" and view["delegations"] == "2"
        assert d.entries(child) == winner_and_denial
    assert d.checked("POST", "/v1/workflows", command)["workflow_id"] == root
    assert (
        d.entries(root) == winner_and_denial
        and sum(provider.census()["requests"].values()) == 1
    )
    d.checked(
        "POST",
        "/v1/budgets/inspect",
        d.budget_query(root),
        credentials=d.worker_credentials,
        expected=403,
    )
    # Exercise the installed wheel, independently of the machine's Python import path.
    installed = Path(__file__).with_name("budget_client.py")
    subprocess.run(
        [
            d.python_client,
            "-I",
            str(installed),
            d.server_url,
            str(d.operator),
            d.scope["tenant_id"],
            d.scope["namespace"],
            root,
        ],
        check=True,
        timeout=30,
    )
    first_page = d.checked(
        "POST",
        "/v1/budgets/entries",
        {**d.budget_query(root), "before": None, "limit": 1},
    )
    cli = [
        "budget",
        "entries",
        "--server",
        d.server_url,
        "--tenant",
        d.scope["tenant_id"],
        "--namespace",
        d.scope["namespace"],
        "--workflow",
        root,
        "--limit",
        "1",
        "--credentials-file",
        str(d.operator),
    ]
    assert d.command("ledgence", cli) == first_page
    session = McpSession(
        d, "budget-reader", credentials_file=d.operator, read_only=True
    )
    d.sessions.append(session)
    try:
        assert (
            session.tool(
                "ledgence_budget_entries",
                {"subject": {"kind": "workflow", "workflow_id": root}, "limit": 1},
            )
            == first_page
        )
    finally:
        session.close()
    if getattr(d, "browser_command", None):
        d.verify_browser(ids[0], root, provider)
    return {
        "root": root,
        "children": ids,
        "calls": 1,
        "delegations": 2,
        "entries": winner_and_denial,
    }


def fanout_refusal(d, provider):
    before = sum(provider.census()["requests"].values())
    root, command = d.submit_review(
        "too-many-delegations", provider, {"max_delegations": 1}
    )
    d.terminal(root)
    assert d.status(root)["state"] == "failed"
    assert not d.child_ids(root) and d.snapshot(root)["delegations"] == "0"
    rows = d.entries(root)
    assert any(row["reason"] == "delegations_exhausted" for row in rows), rows
    assert sum(provider.census()["requests"].values()) == before
    assert d.checked("POST", "/v1/workflows", command)["workflow_id"] == root
    assert d.entries(root) == rows
    return {"root": root, "children": 0, "refusals": len(rows)}


def deadline_refusal(d, provider):
    # Use the database clock, so this covers late processing even on skewed hosts.
    observed = int(
        d.sql("SELECT floor(extract(epoch from clock_timestamp())*1000)::bigint")
    )
    calls = sum(provider.census()["requests"].values())
    root, _ = d.submit_review(
        "expired-review", provider, {"deadline_at_ms": observed - 1}
    )
    d.terminal(root)
    assert d.status(root)["state"] == "failed"
    assert (
        not d.child_ids(root) and sum(provider.census()["requests"].values()) == calls
    )
    rows = d.entries(root)
    assert any(row["reason"] == "deadline_exceeded" for row in rows)
    return {"root": root, "refusals": len(rows)}


def settle_usage(d, provider):
    verified = {}
    for provenance in ("reported", "estimated"):
        command = d.submission(
            "usage-" + provenance, program="budget-usage", provenance=provenance
        )
        command["input"]["budget"] = {"max_calls": 1}
        root = d.checked("POST", "/v1/workflows", command)["workflow_id"]
        d.terminal(root)
        assert d.status(root)["state"] == "succeeded"
        rows = d.entries(root)
        assert len(rows) == 1 and rows[0]["outcome"] == "authorized", rows
        assert rows[0]["usage"] == {
            "kind": provenance,
            "input_tokens": 7,
            "output_tokens": 3,
        }
        # The token observation is metadata; the user only sees the operation output.
        output = d.checked("GET", d.query("/v1/workflows/result", workflow_id=root))
        assert output["outcome"] == {
            "kind": "succeeded",
            "output": {"review": "complete"},
        }
        assert d.checked("POST", "/v1/workflows", command)["workflow_id"] == root
        assert d.entries(root) == rows and d.snapshot(root)["calls"] == "1"
        verified[provenance] = {"root": root, "charged_calls": 1}
    return verified


def standalone_roots(d, provider):
    marker = d.directory / "budget-task-starts.jsonl"
    details = []
    for limit in (0, 1):
        command = d.submission(
            f"standalone-{limit}",
            program="budget-task",
            queue=d.queue,
            duration_ms=0,
            marker=str(marker),
        )
        command["input"]["data"].update(gate=None, marker=str(marker))
        command["input"].update(
            budget={"max_calls": limit}, admission={"resource": d.resource_name}
        )
        receipt = d.checked("POST", "/v1/tasks", command)
        task_id = receipt["task_id"]
        eventually(
            lambda task_id=task_id: d.task(task_id)["state"] in ("succeeded", "failed"),
            timeout=60,
            description="budgeted standalone root settles without a retry loop",
        )
        state = d.task(task_id)
        assert (
            state["state"] == ("succeeded" if limit else "failed")
            and state["attempt_count"] == 1
        )
        subject = {"kind": "task", "task_id": task_id}
        q = {"scope": d.scope, "subject": subject}
        snap = d.checked("POST", "/v1/budgets/inspect", q)
        assert (
            snap["root"] == subject
            and snap["calls"] == str(limit)
            and snap["delegations"] == "0"
        )
        rows = d.checked(
            "POST", "/v1/budgets/entries", {**q, "limit": 10, "before": None}
        )["items"]
        assert len(rows) == 1 and rows[0]["outcome"] == (
            "authorized" if limit else "denied"
        )
        assert d.checked("POST", "/v1/tasks", command)["task_id"] == task_id
        entries = (
            [json.loads(row) for row in marker.read_text().splitlines()]
            if marker.exists()
            else []
        )
        assert sum(row["task"] == task_id for row in entries) == limit
        details.append(
            {
                "task_id": task_id,
                "actual_starts": limit,
                "attempt_count": state["attempt_count"],
            }
        )
    return details


def documented_submission(d, provider):
    script = d.root / "examples/shared-run-budget/submit.py"
    args = [
        d.python_client,
        "-I",
        str(script),
        "--server",
        d.server_url,
        "--credentials",
        str(d.operator),
        "--tenant",
        d.scope["tenant_id"],
        "--namespace",
        d.scope["namespace"],
        "--queue",
        d.queue,
        "--classifier",
        provider.url,
        "--request-id",
        "documented-budget-example",
    ]
    before = sum(provider.census()["requests"].values())
    roots = []
    for _ in range(2):
        output = subprocess.run(
            args, check=True, capture_output=True, text=True, timeout=100
        ).stdout
        roots.append(
            next(
                line.removeprefix("Workflow: ")
                for line in output.splitlines()
                if line.startswith("Workflow: ")
            )
        )
    assert roots[0] == roots[1]
    snapshot = d.snapshot(roots[0])
    assert (snapshot["calls"], snapshot["delegations"]) == ("1", "2")
    assert sum(provider.census()["requests"].values()) == before + 1
    return {"workflow_id": roots[0], "submissions": 2, "physical_calls": 1}


CASES = (
    last_call,
    fanout_refusal,
    deadline_refusal,
    settle_usage,
    standalone_roots,
    documented_submission,
)
