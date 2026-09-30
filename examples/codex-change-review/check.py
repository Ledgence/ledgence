#!/usr/bin/env python3
"""Native acceptance with owned databases; offline unless --live-codex is explicit (MIT).

Requires CPython 3.13, a built ledgence executable, the installed Python client, and an existing
PostgreSQL server allowing CREATE DATABASE through LEDGENCE_POSTGRES_URL. The gate
never restarts that server. Each deployment owns a unique database and processes.
"""

import argparse
import contextlib
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.parse
import uuid

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT / "tools"))

from http_acceptance.harness import Deployment, Process, eventually, exchange


def dump(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def quote(value):
    return "'" + value.replace("'", "''") + "'"


def descendant_running(markers):
    marker = markers / "descendant.json"
    if not marker.exists():
        return False
    pid = json.loads(marker.read_text())["pid"]
    result = subprocess.run(["ps", "-p", str(pid), "-o", "args="], capture_output=True, text=True, timeout=5)
    # Match the exact unique marker before treating a PID as ours; zombies have
    # no running command, and a reused PID must never be signalled.
    return str(marker) in result.stdout


def fixture_wrapper(config_path, *, one_slot=False):
    """Only appended to explicitly labelled, test-owned package copies."""
    return f'''
# Native acceptance instrumentation, absent from the published example package.
import asyncio as _acceptance_asyncio
import json as _acceptance_json
import os as _acceptance_os
from pathlib import Path as _AcceptancePath
import time as _acceptance_time
_acceptance_config = _AcceptancePath({str(config_path)!r})
_acceptance_tests_original = run_tests
async def _acceptance_tests(*, candidate):
    config = _acceptance_json.loads(_acceptance_config.read_text())
    markers = _AcceptancePath(config["markers"])
    with (markers / "local-calls.jsonl").open("a") as output:
        output.write(_acceptance_json.dumps({{"pid": _acceptance_os.getpid(), "sha256": candidate["sha256"]}}) + "\\n")
    (markers / "tests-started").touch()
    if config.get("synchronize"):
        deadline = _acceptance_time.monotonic() + 45
        while not (markers / "review-started").exists():
            if _acceptance_time.monotonic() >= deadline:
                raise RuntimeError("offline review rendezvous was not reached")
            await _acceptance_asyncio.sleep(.01)
    try:
        return await _acceptance_tests_original(candidate=candidate)
    finally:
        (markers / "tests-finished").touch()
run_tests = _acceptance_tests
''' + ("\nAGENT_QUEUE = CONTROL_QUEUE\n" if one_slot else "")


class ChangeDeployment(Deployment):
    def __init__(self, directory, args, database_url, prepared, config_path, *, variant):
        super().__init__(ROOT, directory, args.binaries, sys.executable, database_url, args.psql)
        self.scope = {"tenant_id": "acme", "namespace": "demo"}
        self.queue = "change-review"
        self.variant = variant
        self.instance_file = directory / "instance.json"
        shutil.copyfile(prepared / "instance.json", self.instance_file)
        self.descriptors = {}
        for kind in ("workflow", "implement", "finalize"):
            package = directory / "packages" / kind
            shutil.copytree(prepared / "packages" / kind, package)
            if kind == "workflow" and variant != "production":
                with (package / "program.py").open("a") as output:
                    output.write(fixture_wrapper(config_path, one_slot=variant == "one-slot-fixture"))
            self.descriptors[kind] = self.command("ledgence", [
                "program", "publish", "--source", str(package), "--store", str(self.store)])
        self.environment.update(PYTHONDONTWRITEBYTECODE="1", LEDGENCE_CODEX_BIN=str(args.codex_bin))
        self.server = None

    def start_server(self, port=None):
        self.counter += 1
        process = Process([str(self.binaries / "ledgence"), "orchestrator", "serve", "--bind",
                           f"127.0.0.1:{port or self.server_port}", "--store", self.artifacts.url,
                           "--instance-config", str(self.instance_file)],
                          self.directory, f"server-{self.counter}", self.environment)
        self.processes.append(process)

        def ready():
            assert process.process.poll() is None, "owned orchestrator exited before readiness"
            try:
                return exchange(self.server_url, "GET", "/health/ready", timeout=1)[0] == 200
            except (OSError, http.client.HTTPException):
                return False

        eventually(ready, description="change-review orchestrator readiness")
        self.server = process
        return process, self.server_url

    def worker(self, queue, server=None):
        previous = self.queue
        try:
            self.queue = queue
            return self.start_worker(server=server, concurrency=1, cache="cache-" + queue)
        finally:
            self.queue = previous

    def workflow(self, identity, resource="status"):
        query = urllib.parse.urlencode(dict(self.scope, workflow_id=identity))
        status, _, result = exchange(self.server_url, "GET", f"/v1/workflows/{resource}?{query}")
        assert status == 200, (status, result)
        return result

    def result(self, identity, timeout=120):
        eventually(lambda: self.workflow(identity)["state"] in ("succeeded", "failed", "cancelled"),
                   timeout=timeout, description="change-review terminal outcome")
        return self.workflow(identity, "result")

    def companion(self, arguments, label):
        completed = subprocess.run([sys.executable, "-B", str(HERE / "client.py"),
                                    "--server", self.server_url, *arguments],
                                   env=self.environment, capture_output=True, text=True, timeout=480)
        (self.directory / (label + ".stdout")).write_text(completed.stdout)
        (self.directory / (label + ".stderr")).write_text(completed.stderr)
        assert completed.returncode == 0, f"client {label} failed; inspect its evidence logs"
        return json.loads(completed.stdout)

    def topology(self, identity):
        parent = quote(identity)
        children = json.loads(self.sql("SELECT coalesce(json_agg(json_build_object('key',l.command_key,"
            "'task_id',l.task_id,'attempts',t.attempt_count,'state',t.state)), '[]'::json) "
            "FROM workflow_task_links l JOIN tasks t USING(task_id) WHERE NOT l.is_activation "
            "AND l.workflow_id=" + parent))
        branches = json.loads(self.sql("SELECT coalesce(json_agg(json_build_object('key',l.command_key,"
            "'workflow_id',l.child_workflow_id,'terminal',l.terminal,'fork_key',l.fork_key,"
            "'same_descriptor',c.controller_bytes=p.controller_bytes)), '[]'::json) "
            "FROM owned_workflow_links l JOIN workflow_runs c ON c.workflow_id=l.child_workflow_id "
            "JOIN workflow_runs p ON p.workflow_id=l.parent_workflow_id WHERE l.parent_workflow_id=" + parent))
        journals = json.loads(self.sql("SELECT coalesce(json_agg(json_build_object('workflow_id',a.workflow_id,"
            "'key',r.step_key)), '[]'::json) FROM workflow_local_results r JOIN workflow_activations a "
            "USING(activation_id) JOIN workflow_runs w USING(workflow_id) WHERE w.workflow_id=" + parent +
            " OR w.parent_workflow_id=" + parent))
        forks = int(self.sql("SELECT count(*) FROM workflow_forks WHERE workflow_id=" + parent))
        return {"children": children, "branches": branches, "local_results": journals, "forks": forks}


@contextlib.contextmanager
def deployment(args, evidence, prepared, config_path, variant):
    parent_url = os.environ["LEDGENCE_POSTGRES_URL"]
    database = "ledgence_change_" + uuid.uuid4().hex
    url = urllib.parse.urlunsplit(urllib.parse.urlsplit(parent_url)._replace(path="/" + database))
    directory = evidence / variant
    directory.mkdir()
    owned = None

    def admin(statement):
        result = subprocess.run([args.psql, "--dbname", parent_url, "-X", "--set", "ON_ERROR_STOP=1",
                                 "--command", statement], capture_output=True, timeout=40)
        assert result.returncode == 0, "owned acceptance database administration failed"

    admin(f'CREATE DATABASE "{database}"')
    try:
        owned = ChangeDeployment(directory, args, url, prepared, config_path, variant=variant)
        owned.command("ledgence", ["orchestrator", "migrate"])
        dump(directory / "resources.json", {"database": database, "package_variant": variant,
             "packages": owned.descriptors, "worker_capacity_each": 1, "live_codex": args.live_codex})
        owned.start_server()
        yield owned
    finally:
        try:
            if owned is not None:
                owned.close()
        finally:
            admin(f'DROP DATABASE "{database}" WITH (FORCE)')


def verify_bundle(bundle, topology, expected, *, physical_separation):
    from change_review.candidate import source_digest, validate_candidate
    candidate = validate_candidate(bundle["candidate"])
    digest = source_digest(candidate["source"])
    assert bundle["status"] == expected, bundle["status"]
    assert bundle["tests"]["candidate_sha256"] == bundle["review"]["candidate_sha256"] == digest
    assert candidate["execution"]["cli_invocations"] == bundle["review"]["execution"]["cli_invocations"] == 1
    assert bundle["pull_request"]["url"] is None, "the acceptance gate must not publish to GitHub"
    assert bundle["pull_request"]["title"] and bundle["pull_request"]["body"]
    assert {child["key"] for child in topology["children"]} == {"implement:0", "finalize:0"}
    assert all(child["state"] == "succeeded" and child["attempts"] == 1 for child in topology["children"])
    assert len(topology["branches"]) == topology["forks"] == 1
    branch = topology["branches"][0]
    assert branch["key"] == "review:0" and branch["fork_key"] == "validate:0"
    assert branch["same_descriptor"] and branch["terminal"], branch
    assert sorted(record["key"] for record in topology["local_results"]) == ["review:0", "tests:0"]
    if physical_separation:
        assert bundle["tests"]["pid"] != bundle["review"]["pid"], "review and tests must execute in separate Python processes"


def scenario(d, args, config_path, evidence, tag, *, synchronize=False, bad_patch=False,
             findings=False, review_failure=False, restart=False, lost_ack=False, export=None):
    from change_review.candidate import BASE_SOURCE
    markers = evidence / (tag + "-markers")
    markers.mkdir()
    source = BASE_SOURCE if bad_patch else BASE_SOURCE.replace("total_cents > 10000", "total_cents >= 10000")
    assert source != BASE_SOURCE or bad_patch, "offline repair no longer matches the bundled exercise"
    dump(config_path, {"markers": str(markers), "source": source, "synchronize": synchronize,
                       "hold_review": restart, "findings": findings, "review_failure": review_failure})
    d.gates.add(markers / "release-review")
    d.gates.add(markers / "tests-finished")
    d.gates.add(markers / "review-started")
    proxy = d.proxy() if lost_ack else None
    lost = (proxy.lose_once("/v1/workflows/forks") if proxy else None)
    control = d.worker("change-review", proxy.url if proxy else None)
    agent = None if d.variant == "one-slot-fixture" else d.worker("change-review-agents")
    try:
        accepted = d.companion(["submit", "--change-id", tag, "--idempotency-key", tag], tag + "-submit")
        identity = accepted["workflow_id"]
        if restart:
            eventually(lambda: (markers / "review-started").exists() and d.workflow(identity)["state"] == "waiting",
                       timeout=45, description="parent durably joined while its review is active")
            d.server.stop()
            d.start_server()
            assert d.workflow(identity)["state"] == "waiting"
            (markers / "release-review").touch()
        result = d.result(identity, timeout=420 if args.live_codex else 120)
        topology = d.topology(identity)
        if review_failure:
            assert result["workflow"]["state"] == "failed", result
            assert result["outcome"]["error"]["kind"] == "review_failed", result
            assert [child["key"] for child in topology["children"]] == ["implement:0"]
            assert topology["forks"] == len(topology["branches"]) == 1
            assert (markers / "descendant.json").is_file(), "the descendant cleanup fixture did not run"
            eventually(lambda: not descendant_running(markers), timeout=20,
                       description="Rust worker drains failed Codex descendants")
        else:
            assert result["workflow"]["state"] == "succeeded", result
            bundle = result["outcome"]["output"]
            expected = ("ready_for_review" if bundle["tests"]["passed"] and bundle["review"]["verdict"] == "approve"
                        else "needs_changes") if args.live_codex else ("needs_changes" if bad_patch or findings else "ready_for_review")
            verify_bundle(bundle, topology, expected, physical_separation=d.variant != "one-slot-fixture")
            if synchronize:
                tests, review = bundle["tests"], bundle["review"]
                assert review["started_at_ms"] <= tests["started_at_ms"] <= tests["finished_at_ms"] <= review["finished_at_ms"]
                assert len((markers / "local-calls.jsonl").read_text().splitlines()) == 1
            if bad_patch:
                assert not bundle["tests"]["passed"] and bundle["tests"]["failures"] > 0
            if findings:
                assert bundle["tests"]["passed"] and bundle["review"]["findings"]
            if export:
                exported = d.companion(["result", "--workflow", identity, "--timeout", "30",
                                        "--output", str(export)], tag + "-result")
                assert exported["candidate_sha256"] == bundle["candidate"]["sha256"]
                assert (export / "shipping.py").read_text() == bundle["candidate"]["source"]
                assert json.loads((export / "review.json").read_text()) == bundle
        if lost:
            assert lost.is_set(), "fork response-loss fault was not reached"
            requests = proxy.commands("/v1/workflows/forks")
            assert len(requests) >= 2, "fork registration was not reconciled after response loss"
            assert all(item["json"]["fork"] == requests[0]["json"]["fork"] for item in requests)
        if not args.live_codex:
            cli_calls = [json.loads(line) for line in (markers / "codex.jsonl").read_text().splitlines()]
            assert [item["phase"] for item in cli_calls] == ["implement", "review"], cli_calls
            assert all(item["offline_fixture"] for item in cli_calls)
        record = {"scenario": tag, "passed": True, "live_codex": args.live_codex,
                  "package_variant": d.variant, "workflow_id": identity, "result": result,
                  "topology": topology, "deterministic_overlap": synchronize,
                  "orchestrator_restarted_while_waiting": restart, "lost_fork_ack_reconciled": lost_ack,
                  "failed_review_descendant_drained": review_failure}
        dump(evidence / (tag + ".json"), record)
        print("PASS " + tag, flush=True)
        return record
    finally:
        (markers / "release-review").touch()
        (markers / "tests-finished").touch()
        (markers / "review-started").touch()
        try:
            for worker in (agent, control):
                if worker is not None and worker.process.poll() is None:
                    worker.stop()
        finally:
            if descendant_running(markers):
                import signal
                with contextlib.suppress(ProcessLookupError):
                    os.kill(json.loads((markers / "descendant.json").read_text())["pid"], signal.SIGKILL)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binaries", type=Path, default=ROOT / "target/debug")
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--evidence", type=Path, help="new output directory outside the repository")
    parser.add_argument("--live-codex", action="store_true", help="explicitly make two real Codex CLI generations")
    parser.add_argument("--codex-bin", type=Path, help="absolute executable for explicitly enabled live Codex")
    args = parser.parse_args()
    if sys.version_info[:2] != (3, 13):
        parser.error("run with CPython 3.13")
    if not os.environ.get("LEDGENCE_POSTGRES_URL"):
        parser.error("set LEDGENCE_POSTGRES_URL to the existing local acceptance PostgreSQL server")
    args.binaries = args.binaries.resolve()
    if args.live_codex:
        if (not args.codex_bin or not args.codex_bin.is_absolute() or not args.codex_bin.is_file()
                or not os.access(args.codex_bin, os.X_OK)):
            parser.error("--live-codex requires --codex-bin naming an absolute executable")
    elif args.codex_bin:
        parser.error("--codex-bin is only used with explicit --live-codex")
    evidence = args.evidence.resolve() if args.evidence else Path(tempfile.mkdtemp(prefix="ledgence-change-review-"))
    if evidence.is_relative_to(ROOT):
        parser.error("evidence must be outside the repository")
    if args.evidence:
        evidence.mkdir(parents=True, exist_ok=False)
    config_path = evidence / "offline-fixture.json"
    if not args.live_codex:
        executable = evidence / "offline-codex"
        executable.write_text(f"#!{sys.executable}\nimport runpy\nrunpy.run_path({str(HERE / 'tests/fake_codex.py')!r}, "
                              f"run_name='__main__', init_globals={{'FIXTURE_CONFIG_PATH': {str(config_path)!r}}})\n")
        executable.chmod(0o700)
        args.codex_bin = executable
    records = []
    try:
        from ledgence.client import AsyncClient  # Fail before starting services if the client is missing.
        binaries = {name: hashlib.sha256((args.binaries / name).read_bytes()).hexdigest()
                    for name in ("ledgence",)}
        fixture_sources = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                           for path in (Path(__file__), HERE / "tests/fake_codex.py",
                                        ROOT / "tools/http_acceptance/harness.py")}
        import prepare
        prepared = evidence / "prepared"
        prepared_info = prepare.prepare(prepared, args.binaries)
        with deployment(args, evidence, prepared, config_path, "production") as d:
            assert d.descriptors == prepared_info["packages"], "production package bytes changed in the gate"
            records.append(scenario(d, args, config_path, evidence, "live" if args.live_codex else "offline-production",
                                    export=evidence / "bundle"))
        if not args.live_codex:
            with deployment(args, evidence, prepared, config_path, "one-slot-fixture") as d:
                records.append(scenario(d, args, config_path, evidence, "offline-one-slot"))
            with deployment(args, evidence, prepared, config_path, "fault-fixture") as d:
                for tag, options in (
                    ("offline-overlap", {"synchronize": True}),
                    ("offline-bad-patch", {"bad_patch": True}),
                    ("offline-review-findings", {"findings": True}),
                    ("offline-review-failure", {"review_failure": True}),
                    ("offline-restart-lost-ack", {"restart": True, "lost_ack": True}),
                ):
                    records.append(scenario(d, args, config_path, evidence, tag, **options))
        dump(evidence / "result.json", {"passed": True, "live_codex": args.live_codex,
             "provider_called_by_gate": args.live_codex, "scenarios": [item["scenario"] for item in records],
             "bundle": str(evidence / "bundle"), "source_sha256": prepared_info["source_sha256"],
             "binaries": binaries, "fixture_sha256": fixture_sources})
        print(f"Change review acceptance passed: {len(records)} scenarios; evidence {evidence}")
        return 0
    except Exception:
        diagnostic = traceback.format_exc().replace(os.environ["LEDGENCE_POSTGRES_URL"], "[local PostgreSQL URL]")
        (evidence / "failure.txt").write_text(diagnostic)
        print(f"Change review acceptance failed; inspect {evidence / 'failure.txt'}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
