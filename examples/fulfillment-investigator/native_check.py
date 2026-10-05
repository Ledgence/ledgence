"""Real workflow acceptance in an owned database; all model responses are fixtures (MIT)."""
import asyncio
import hashlib
import http.client
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from http_acceptance.harness import Deployment, Process, eventually, exchange
from postgres_fixture import create_owned_database, owned_database_url
from fulfillment.config import APPROVAL_KEY, PROGRAM, QUEUE, SOURCE_WAIT, VERSION
from fulfillment.report import load_candidate
from fulfillment.storage import read_json
from prepare import prepare


class ExampleDeployment(Deployment):
    def __init__(self, directory, args, database_url, prepared):
        super().__init__(ROOT, directory, args.binaries.resolve(), sys.executable, database_url, args.psql)
        self.scope = {"tenant_id": "acme", "namespace": "demo"}
        self.queue = QUEUE
        self.instance = prepared / "instance.json"
        self.data = str(prepared / "data")
        self.descriptor = self.command("ledgence", ["program", "publish", "--source",
            str(prepared / "packages/workflow"), "--store", str(self.store)])
        self.environment.update(PYTHONDONTWRITEBYTECODE="1")

    def start_server(self):
        self.counter += 1
        process = Process([str(self.binaries / "ledgence"), "orchestrator", "serve", "--bind",
            f"127.0.0.1:{self.server_port}", "--store", self.artifacts.url,
            "--instance-config", str(self.instance)], self.directory, f"server-{self.counter}", self.environment)
        self.processes.append(process)
        def ready():
            assert process.process.poll() is None, "owned orchestrator exited; inspect its logs"
            try:
                return exchange(self.server_url, "GET", "/health/ready", timeout=1)[0] == 200
            except (OSError, http.client.HTTPException):
                return False
        eventually(ready, description="fulfillment orchestrator readiness")
        return process

    def companion(self, arguments):
        result = subprocess.run([sys.executable, "-B", str(HERE / "client.py"),
            "--server", self.server_url, *arguments], env=self.environment, capture_output=True, text=True, timeout=180)
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)

    def scalar(self, query):
        return self.sql(query)


def quote(value):
    return "'" + value.replace("'", "''") + "'"


async def scenarios(d, evidence):
    from ledgence.client import AsyncClient, ApprovalStatus, RetryPolicy
    proxy = d.proxy()
    # Real network loss after model commit and after approved local publication commit.
    from ledgence.worker.workflow import _APPROVAL_LOCAL_PREFIX
    approval_local_key = _APPROVAL_LOCAL_PREFIX + hashlib.sha256(APPROVAL_KEY.encode()).hexdigest()
    lost_publish = proxy.lose_once("/v1/workflows/local-results", lambda body: body["record"]["key"] == approval_local_key)
    lost_model = proxy.lose_once("/v1/workflows/local-results", lambda body: body["record"]["key"] == "model:0")
    worker = d.start_worker(server=proxy.url, concurrency=1)
    server = d.server
    async with AsyncClient(d.server_url, tenant="acme", namespace="demo") as client:
        async def submit(tag, scenario="data-gap", **extra):
            return await client.workflows.submit(program=PROGRAM, version=VERSION, queue=QUEUE,
                data={"store": d.data, "scenario": scenario, **extra}, idempotency_key=tag,
                retry_policy=RetryPolicy(max_attempts=3, retry_delay_ms=100), attempt_timeout_ms=180_000)
        async def wait_until(predicate, description):
            for _ in range(600):
                if await predicate():
                    return
                await asyncio.sleep(.1)
            raise AssertionError("timed out: " + description)
        async def waiting_for_source(handle):
            # A child join is also 'waiting'; verify the external event wait was committed.
            key = await asyncio.to_thread(d.sql,
                "SELECT coalesce(external_wait_key,'') FROM workflow_runs WHERE state='waiting' AND workflow_id=" + quote(handle.id))
            return key == SOURCE_WAIT
        async def pending(handle):
            page = await handle.approvals()
            return len(page.items) == 1 and page.items[0].status == ApprovalStatus.PENDING
        async def restart():
            nonlocal worker, server
            await asyncio.to_thread(worker.stop)
            await asyncio.to_thread(server.stop)
            server = await asyncio.to_thread(d.start_server)
            worker = d.start_worker(server=proxy.url, concurrency=1)
        async def topology(handle):
            rows = json.loads(await asyncio.to_thread(d.sql,
                "SELECT coalesce(json_agg(json_build_object('key',command_key,'fork',fork_key,'terminal',terminal)), '[]'::json) "
                "FROM owned_workflow_links WHERE parent_workflow_id=" + quote(handle.id)))
            return rows
        async def approve(handle, label, decision="approve"):
            preview = await asyncio.to_thread(d.companion, ["review", "--workflow", handle.id,
                "--output", str(evidence / (label + "-review"))])
            args = ["decide", "--workflow", handle.id, "--decision", decision,
                "--decision-id", label + ":decision", "--reviewer", "native-fixture-reviewer",
                "--candidate-sha256", preview["candidate_sha256"], "--file", str(evidence / (label + "-decision.json"))]
            receipt = await asyncio.to_thread(d.companion, args)
            again = await asyncio.to_thread(d.companion, args)
            assert again["already_accepted"] is True
            return receipt

        gap = await submit("gap:1")
        duplicate = await submit("gap:1")
        assert gap.id == duplicate.id
        await wait_until(lambda: waiting_for_source(gap), "incomplete warehouse wait")
        assert len(await topology(gap)) == 4
        assert len((await gap.approvals()).items) == 0
        await restart()
        assert await waiting_for_source(gap)
        source_args = ["source-ready", "--workflow", gap.id, "--event-id", "warehouse:1"]
        await asyncio.to_thread(d.companion, source_args)
        await asyncio.to_thread(d.companion, source_args)
        await wait_until(lambda: pending(gap), "verified gap report approval")
        proposal = (await gap.approval(APPROVAL_KEY)).to_dict()
        await restart()
        assert (await gap.approval(APPROVAL_KEY)).to_dict() == proposal
        await approve(gap, "gap")
        output = await gap.result(timeout=120)
        assert output["status"] == "published" and output["corrected_sources"] == ["warehouse"], output
        candidate = load_candidate(store=d.data, candidate_ref=output["candidate_ref"])
        assert candidate["conclusion"] == "data_gap"
        assert candidate["metrics"]["carrier"]["current_late"] == 4
        assert candidate["metrics"]["carrier"]["baseline_late"] == 4
        tree = await topology(gap)
        assert len(tree) == 9 and all(row["terminal"] for row in tree), tree
        assert [row["key"] for row in tree if row["fork"] == "ingest:corrected"] == ["source:warehouse:corrected"]
        assert lost_model.is_set(), "model journal fault not reached"
        commits = proxy.commands("/v1/workflows/local-results", lambda b: b["record"]["key"] == "model:0")
        assert len(commits) >= 2 and commits[0]["body"] == commits[1]["body"]
        assert json.loads(commits[1]["response"])["already_accepted"]
        gap_summary = {"workflow_id": gap.id, "source_restart": True, "approval_restart": True,
            "duplicate_submission": True, "duplicate_event": True, "duplicate_decision": True,
            "lost_model_commit_reply": True, "lost_publication_commit_reply": lost_publish.is_set(),
            "branches": tree, "result": output}
        assert lost_publish.is_set(), "publication journal fault not reached"
        publication_commits = proxy.commands("/v1/workflows/local-results", lambda b: b["record"]["key"] == approval_local_key)
        assert len(publication_commits) >= 2 and publication_commits[0]["body"] == publication_commits[1]["body"]
        assert json.loads(publication_commits[1]["response"])["already_accepted"]
        observed = await asyncio.to_thread(d.companion, ["result", "--workflow", gap.id])
        assert Path(observed["publication"]["html_path"]).is_file()

        incident = await submit("delay:1", "real-delay")
        await wait_until(lambda: pending(incident), "real delay review")
        await approve(incident, "delay")
        incident_output = await incident.result(timeout=120)
        candidate = read_json(d.data, incident_output["candidate_ref"])
        assert candidate["conclusion"] == "delivery_delay"
        assert candidate["metrics"]["carrier"]["current_late"] == 16
        assert incident_output["corrected_sources"] == []
        assert len(await topology(incident)) == 8

        rejected = await submit("reject:1", "real-delay")
        await wait_until(lambda: pending(rejected), "rejection review")
        await approve(rejected, "rejected", "reject")
        rejection = await rejected.result(timeout=120)
        assert rejection["status"] == "rejected" and rejection["published"] is False
        expired = await submit("expire:1", "real-delay", approval_timeout_ms=0)
        expiry = await expired.result(timeout=120)
        assert expiry["status"] == "expired" and expiry["published"] is False
        incomplete = await submit("incomplete:1", source_timeout_ms=0)
        timeout = await incomplete.result(timeout=120)
        assert timeout["status"] == "incomplete" and timeout["published"] is False
        summary = {"mode": "fixture", "synthetic": True, "worker_concurrency": 1,
            "gap": gap_summary, "real_delay": {"workflow_id": incident.id, "result": incident_output},
            "rejected": rejection["status"], "expired": expiry["status"], "missing_source": timeout["status"],
            "package": d.descriptor}
        (evidence / "acceptance.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps({"status": "passed", "evidence": str(evidence / "acceptance.json"),
            "gap_report": str(Path(d.data) / output["publication"]["html_ref"]["path"]),
            "incident_report": str(Path(d.data) / incident_output["publication"]["html_ref"]["path"])}))
    await asyncio.to_thread(worker.stop)
    await asyncio.to_thread(server.stop)


def run(args):
    evidence = args.directory.resolve()
    if evidence.is_relative_to(ROOT):
        raise ValueError("use a new evidence directory outside the checkout")
    evidence.mkdir(parents=True, exist_ok=False)
    prepared = evidence / "prepared"
    prepare(prepared, args.binaries)
    database = "ledgence_fulfillment_" + uuid.uuid4().hex
    parent = os.environ["LEDGENCE_POSTGRES_URL"]
    database_url = owned_database_url(parent, database)
    def admin(statement):
        result = subprocess.run([args.psql, "--dbname", parent, "-X", "--set", "ON_ERROR_STOP=1", "--command", statement],
                                capture_output=True, timeout=40)
        assert result.returncode == 0, "owned test database administration failed"
    create_owned_database(admin, database)
    deployment = None
    try:
        directory = evidence / "deployment"
        directory.mkdir()
        deployment = ExampleDeployment(directory, args, database_url, prepared)
        deployment.command("ledgence", ["orchestrator", "migrate"])
        deployment.server = deployment.start_server()
        asyncio.run(scenarios(deployment, evidence))
    finally:
        try:
            if deployment is not None:
                deployment.close()
        finally:
            admin(f'DROP DATABASE "{database}" WITH (FORCE)')
