#!/usr/bin/env python3
"""Qualify an offline upgrade from the verified 0.6.1 native bundle on an owned PostgreSQL server."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

from console_acceptance.harness import ConsoleDeployment
from http_acceptance.harness import eventually
from postgres_fixture import create_owned_database, owned_database_url


def migrate(d):
    subprocess.run([d.binaries / "ledgence", "orchestrator", "migrate"],
                   env=d.environment, check=True, timeout=60)


def submit(d, name, data):
    command = d.console_submission(name, program=name)
    command["input"]["data"] = data
    return d.api("POST", "workflows", command)["workflow"]["workflow_id"]


def exercise(d, old_bundle, binaries, new_console, new_runner):
    migrate(d)
    d.server, _ = d.start_server()
    worker = d.start_worker(runner=old_bundle / "runtime/ledgence/worker/bootstrap.py")
    for program, source in [
        ("approval-controller", d.root / "tools/workflow_acceptance/approval_program.py"),
        ("agent-recovery", d.root / "examples/agent-recovery/program.py"),
    ]:
        d.publish(program, "1.0.0", program_source=source.read_text(), runtime_protocol=3)

    marker = d.directory / "effect.jsonl"
    wf = submit(d, "approval-controller", {"marker": str(marker), "amount": 100, "timeout_ms": 180_000})
    eventually(lambda: d.workflow_status(wf)["state"] == "waiting", description="0.6.1 approval is pending")
    identity = {"workflow_id": wf, "key": "refund"}
    saved = d.api("POST", "approvals/inspect", identity)
    assert saved["status"] == "pending" and not marker.exists()
    agent = submit(d, "agent-recovery", {"order_id": "ORD-42"})
    d.workflow_result(agent)
    old_step, = [node for node in d.rows("workflows/explorer", workflow_id=agent)
                 if node["kind"] == "local" and node["key"].startswith("tool:")]
    step_id = {"workflow_id": agent, "activation_id": old_step["activation_id"], "step_key": old_step["key"]}
    summaries = d.rows("workflows/local-steps", workflow_id=agent, activation_id=old_step["activation_id"])
    old_summary, = [row for row in summaries if row["step_key"] == old_step["key"]]

    # The DB, instance binding and program store survive the upgrade. Stop every writer first.
    worker.stop()
    d.server.stop()
    previous_schema = d.sql("SELECT version FROM _sqlx_migrations ORDER BY version")
    d.binaries, d.console_dist = binaries, new_console
    migrate(d)
    applied = set(d.sql("SELECT version FROM _sqlx_migrations ORDER BY version").splitlines()) - set(previous_schema.splitlines())
    assert {"20261102000000", "20261102010000"} <= applied, applied
    d.server, _ = d.start_server()
    d.start_worker(runner=new_runner)
    assert d.api("POST", "approvals/inspect", identity) == saved
    filters = {"program_id": "approval-controller", "status": "pending", "limit": 1}
    deadline = saved["deadline"]
    assert d.api("POST", "approvals/inbox", dict(filters, due_before=deadline))["items"] == []
    row, = d.api("POST", "approvals/inbox", dict(filters, due_before=deadline + 1))["items"]
    assert row["workflow_id"] == wf and row["activation_id"] == saved["activation_id"]
    detail = d.api("GET", "workflows/local-steps/inspect", **step_id)
    assert detail["step"] == old_summary and detail["kind"] == "tool" and detail["version"] == "1"
    assert detail["arguments"] == {"order_id": "ORD-42", "region": "us-east"}
    assert detail["result"] == {"order_id": "ORD-42", "region": "us-east", "status": "shipped", "delivery_days": 2}

    decision = {key: saved[key] for key in ("workflow_id", "key", "activation_id", "revision", "action")}
    decision.update(decision_id="upgrade-review", decision="approve", reviewer="upgrade-test", reason=None)
    assert not d.api("POST", "approvals/decide", decision)["already_accepted"]
    assert d.api("POST", "approvals/decide", decision)["already_accepted"]
    outcome = d.workflow_result(wf)["outcome"]["output"]
    assert outcome == {"status": "approved", "result": {"simulated": True, "amount": 50, "currency": "USD"}}
    settled = d.api("POST", "approvals/inspect", identity)
    rows = [json.loads(line) for line in marker.read_text().splitlines()]
    assert [row["activation"] for row in rows if row["kind"] == "resume"] == [settled["resumed_activation_id"]]
    assert [row["amount"] for row in rows if row["kind"] == "effect"] == [50]
    return {"workflow_id": wf, "resumed_activation_id": settled["resumed_activation_id"],
            "accepted_operation": step_id, "migrations": sorted(applied)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--previous-bundle", type=Path, required=True, help="extracted, checksum-verified 0.6.1 bundle")
    parser.add_argument("--binaries", type=Path, required=True, help="directory containing the candidate ledgence")
    parser.add_argument("--console-dist", type=Path, required=True, help="Console assets for the candidate")
    parser.add_argument("--candidate-runner", type=Path, help="bundled runtime/ledgence/worker/bootstrap.py; defaults to the source checkout")
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--evidence", type=Path, required=True, help="new directory for process logs and results")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    previous, binaries, dist = (p.resolve() for p in (args.previous_bundle, args.binaries, args.console_dist))
    old, new = previous / "bin/ledgence", binaries / "ledgence"
    if subprocess.check_output([old, "--version"], text=True).strip() != "ledgence 0.6.1":
        parser.error("the previous bundle must contain Ledgence 0.6.1")
    hashes = {}
    for name, binary in (("previous", old), ("candidate", new)):
        with binary.open("rb") as stream:
            hashes[name] = hashlib.file_digest(stream, "sha256").hexdigest()
    if hashes["previous"] == hashes["candidate"]:
        parser.error("the candidate executable must differ from the previous one")
    parent = os.environ.get("LEDGENCE_POSTGRES_URL")
    database = "ledgence_upgrade_" + uuid.uuid4().hex
    try:
        url = owned_database_url(parent, database)
    except ValueError as exc:
        parser.error(str(exc))
    def admin(sql):
        proc = subprocess.run([args.psql, "--dbname", parent, "-X", "--set", "ON_ERROR_STOP=1", "-c", sql],
                              capture_output=True, timeout=60)
        if proc.returncode:
            raise RuntimeError("owned upgrade database administration failed")
    candidate_runner = args.candidate_runner.resolve() if args.candidate_runner else root / "sdk/python/ledgence/worker/bootstrap.py"
    if not candidate_runner.is_file():
        parser.error("the candidate Python runner must exist")
    directory = args.evidence.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    create_owned_database(admin, database)
    try:
        d = ConsoleDeployment(root, directory, old.parent, os.getenv("LEDGENCE_PYTHON", sys.executable),
                              url, args.psql, console_dist=previous / "console")
        try:
            result = exercise(d, previous, binaries, dist, candidate_runner)
        finally:
            d.close()
    finally:
        admin(f'DROP DATABASE "{database}" WITH (FORCE)')
    result["executable_sha256"] = hashes
    result["candidate_version"] = subprocess.check_output([new, "--version"], text=True).strip()
    (directory / "results.json").write_text(json.dumps(result, indent=2) + "\n")
    print("PASS 0.6.1 upgrade, approval resumption and accepted operation inspection")


if __name__ == "__main__":
    main()
