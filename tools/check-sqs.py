"""Exercise SQS-compatible delivery through real PostgreSQL, HTTP and Python programs.

Local mode requires an explicit owned loopback ElasticMQ endpoint. Real AWS mode
requires AWS CLI, an explicit --allow-aws-test-resources flag and a test queue
prefix; it creates/deletes only a unique test queue. Both modes create/drop one
unique PostgreSQL database using LEDGENCE_POSTGRES_URL. This is functionality
acceptance, not a throughput qualification or an exactly-once-effects claim.
"""

import argparse
import http.client
import ipaddress
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid

from http_acceptance.harness import eventually
from http_acceptance.sqs import SqsDeployment
from http_acceptance.sqs_startup import failed_startup_preserves_integrated_delivery
from http_acceptance.scenarios import (
    cancellation_and_failures, warm_cache_and_cli, worker_crash_recovery,
)


class QueueAdmin:
    def __init__(self, endpoint, region, aws_cli):
        self.endpoint, self.region, self.aws_cli = endpoint, region, aws_cli
        self.calls = []

    def call(self, operation, payload):
        self.calls.append(operation)
        if self.endpoint:
            request = urllib.request.Request(
                self.endpoint, data=json.dumps(payload).encode(), method="POST",
                headers={"Content-Type": "application/x-amz-json-1.0",
                         "X-Amz-Target": "AmazonSQS." + operation},
            )
            with urllib.request.urlopen(request, timeout=15) as response:
                body = response.read(1024 * 1024 + 1)
                assert len(body) <= 1024 * 1024, "oversized test queue administration response"
                return json.loads(body) if body else {}
        action = re.sub(r"(?<!^)(?=[A-Z])", "-", operation).lower()
        command = [self.aws_cli, "sqs", action, "--region", self.region,
                   "--cli-input-json", json.dumps(payload), "--output", "json",
                   "--no-cli-pager", "--cli-connect-timeout", "5", "--cli-read-timeout", "15"]
        result = subprocess.run(command, capture_output=True, timeout=25)
        if result.returncode:
            raise RuntimeError(f"AWS test queue {operation} failed; inspect AWS CLI configuration")
        return json.loads(result.stdout) if result.stdout.strip() else {}

    def queue_url(self, created):
        value = created["QueueUrl"]
        if self.endpoint:
            original = urllib.parse.urlsplit(value)
            endpoint = urllib.parse.urlsplit(self.endpoint)
            value = urllib.parse.urlunsplit(original._replace(scheme=endpoint.scheme, netloc=endpoint.netloc))
        return value



def sql_text(value):
    return "'" + value.replace("'", "''") + "'"


def duplicates_and_unknown_claim(d, admin, queue_url):
    submitted = d.submit(d.submission("duplicate-dispatch"))
    task_id = submitted["task_id"]
    statement = (
        "SELECT json_build_object('dispatch',json_build_object('scope',"
        "json_build_object('tenant_id',t.tenant_id,'namespace',t.namespace),"
        "'queue',t.queue,'task_id',t.task_id,'generation',i.generation),"
        "'publication_id',i.publication_id) FROM dispatch_intents i JOIN tasks t USING(task_id) "
        f"WHERE i.task_id={sql_text(task_id)} AND i.last_confirmed_at_ms IS NOT NULL"
    )
    published = json.loads(eventually(lambda: d.sql(statement), description="confirmed dispatch intent"))
    response = admin.call("SendMessageBatch", {
        "QueueUrl": queue_url,
        "Entries": [{"Id": str(index), "MessageBody": json.dumps(published)} for index in range(2)],
    })
    assert len(response.get("Successful", [])) == 2 and not response.get("Failed"), response
    proxy = d.proxy()
    matching_task = lambda command: command["dispatch"]["task_id"] == task_id
    lost = proxy.lose_once("/v1/dispatch/claim", matching_task)
    worker = d.start_worker(proxy.url)
    task, _ = d.terminal(task_id)
    assert task["attempt_count"] == 1, task
    assert lost.is_set(), "committed claim reply was not intercepted"
    def operation_identity(record):
        acquisition = record["json"]["acquisition"]
        return (acquisition["worker_session_id"], acquisition["consumer_id"], acquisition["sequence"])

    def matching_claims():
        return proxy.commands("/v1/dispatch/claim", matching_task)

    eventually(lambda: len({operation_identity(record) for record in matching_claims()}) >= 3,
               description="all three dispatch records durably reconciled")
    worker.stop()
    claims = matching_claims()
    first = claims[0]
    proof = {
        "task_id": task_id,
        "publication_id": published["publication_id"],
        "distinct_operations": sorted({operation_identity(item) for item in claims}),
        "claims": [{
            "operation": operation_identity(item),
            "request_id": item["request_id"],
            "status": item["status"],
            "response_kind": json.loads(item["response"]).get("disposition", {}).get("kind"),
            "authority_disposition": json.loads(item["response"]).get("disposition", {}).get("reply", {}).get("disposition"),
            "replays_first_operation": operation_identity(item) == operation_identity(first),
            "exact_first_command": item["body"] == first["body"],
        } for item in claims],
    }
    (d.directory / "duplicate-claim-proof.json").write_text(json.dumps(proof, indent=2) + "\n")
    assert first["status"] == 200, first
    assert sum(item["body"] == first["body"] for item in claims) >= 2, "unknown claim was not replayed exactly"
    first_reply = json.loads(first["response"])
    assert first_reply["command"] == first["json"]
    assert first_reply["disposition"]["kind"] == "claimed"
    assert first_reply["disposition"]["reply"]["disposition"] == "assigned"
    first_operation = operation_identity(first)
    for item in claims:
        if operation_identity(item) == first_operation:
            assert item["body"] == first["body"], "claim replay changed its dispatch binding"
            continue
        assert item["status"] == 200, item
        reply = json.loads(item["response"])
        assert reply["command"] == item["json"]
        assert reply["disposition"]["kind"] in ("already_handed_off", "terminal_or_superseded"), reply
    assert len(d.invocations(task_id)) == 1
    assert sum(event["event"]["reason"] == "claimed" for event in d.history(task_id)) == 1
    return "duplicate publications and lost durable claim response produce one authority and one observed invocation"


def warm_without_abandoned_receive(d):
    # Healthy graceful stop must reconcile the already-issued ReceiveMessage,
    # rather than abandon it and hide the next task for the 60s visibility time.
    original = d.terminal_timeout_floor
    d.terminal_timeout_floor = 35
    try:
        return warm_cache_and_cli(d)
    finally:
        d.terminal_timeout_floor = original


def completed_obligations(d):
    eventually(lambda: d.sql("SELECT count(*) FROM dispatch_intents") == "0",
               description="terminal task dispatch intents removed")
    assert d.sql("SELECT count(*) FROM tasks WHERE dispatch_destination IS NULL") == "0"
    assert d.sql("SELECT count(*) FROM tasks WHERE state NOT IN ('succeeded','failed','cancelled')") == "0"
    assert d.sql("SELECT count(*) FROM dispatch_claim_receipts") != "0"
    return "all external tasks terminal, no orphan dispatch intents, durable claim receipts retained"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--endpoint", help="owned loopback ElasticMQ endpoint; never inferred")
    mode.add_argument("--aws", action="store_true", help="explicit real-AWS acceptance mode")
    parser.add_argument("--allow-aws-test-resources", action="store_true")
    parser.add_argument("--aws-queue-prefix", help="explicit ledgence-test- prefix for owned AWS queue")
    parser.add_argument("--aws-cli", default="aws")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--binaries", type=Path)
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--scenario", action="append", choices=["duplicates", "warm", "retry", "crash", "obligations", "startup"])
    args = parser.parse_args()
    if args.endpoint:
        parsed = urllib.parse.urlsplit(args.endpoint)
        host = parsed.hostname
        try:
            loopback = host == "localhost" or ipaddress.ip_address(host or "").is_loopback
        except ValueError:
            loopback = False
        if parsed.scheme not in ("http", "https") or not loopback or parsed.username or parsed.password or parsed.query or parsed.fragment:
            parser.error("--endpoint must be an explicit loopback HTTP(S) URL without credentials/query/fragment")
        if args.allow_aws_test_resources or args.aws_queue_prefix:
            parser.error("AWS authorization options apply only to --aws")
    elif not args.allow_aws_test_resources or not args.aws_queue_prefix or not re.fullmatch(r"ledgence-test-[A-Za-z0-9_-]{1,30}", args.aws_queue_prefix):
        parser.error("--aws requires --allow-aws-test-resources and an explicit --aws-queue-prefix ledgence-test-NAME")
    parent_url = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not parent_url:
        parser.error("LEDGENCE_POSTGRES_URL must name an owned disposable test PostgreSQL server")
    root = Path(__file__).resolve().parents[1]
    binaries = args.binaries
    if binaries is None:
        subprocess.run(["cargo", "build", "--workspace", "--bins", "--all-features", "--locked"], cwd=root, check=True)
        metadata = json.loads(subprocess.check_output(["cargo", "metadata", "--no-deps", "--format-version", "1", "--locked"], cwd=root))
        binaries = Path(metadata["target_directory"]) / "debug"
    binaries = binaries.resolve()
    for name in ("ledgence", "ledgence-worker", "ledgence-orchestrator"):
        if not (binaries / name).is_file():
            parser.error(f"missing executable {binaries / name}")
    temporary = None
    if args.evidence:
        args.evidence.mkdir(parents=True, exist_ok=False)
        directory = args.evidence.resolve()
    else:
        temporary = tempfile.mkdtemp(prefix="ledgence-sqs-acceptance-")
        directory = Path(temporary)
    parsed = urllib.parse.urlsplit(parent_url)
    database = "ledgence_sqs_" + uuid.uuid4().hex
    database_url = urllib.parse.urlunsplit(parsed._replace(path="/" + database))
    admin = QueueAdmin(args.endpoint, args.region, args.aws_cli)
    queue_name = (args.aws_queue_prefix or "ledgence-test-local") + "-" + uuid.uuid4().hex
    queue_url = None
    queue_create_started = False
    created = False
    succeeded = False
    deployment = None

    def database_admin(statement):
        result = subprocess.run([args.psql, "--dbname", parent_url, "-X", "--set", "ON_ERROR_STOP=1", "--command", statement], capture_output=True, timeout=40)
        if result.returncode:
            raise RuntimeError("owned test database setup/cleanup failed")

    try:
        queue_create_started = True
        queue_url = admin.queue_url(admin.call("CreateQueue", {"QueueName": queue_name,
            "Attributes": {"DelaySeconds": "0", "VisibilityTimeout": "60", "MessageRetentionPeriod": "3600"}}))
        (directory / "resources.json").write_text(json.dumps({"mode": "aws" if args.aws else "elasticmq", "queue_name": queue_name,
            "queue_url": queue_url, "database_name": database, "real_aws": args.aws}, indent=2) + "\n")
        database_admin(f'CREATE DATABASE "{database}"')
        created = True
        deployment = SqsDeployment(root, directory, binaries, os.environ.get("LEDGENCE_PYTHON", sys.executable), database_url, args.psql,
            queue_url=queue_url, endpoint=args.endpoint, region=args.region)
        result = subprocess.run([str(binaries / "ledgence-orchestrator"), "migrate"], env=deployment.environment, capture_output=True, timeout=40)
        if result.returncode:
            raise RuntimeError("explicit migration failed: " + result.stderr.decode(errors="replace")[-3000:])
        deployment.server, _ = deployment.start_server()
        cases = [
            ("warm", lambda: warm_without_abandoned_receive(deployment)),
            ("duplicates", lambda: duplicates_and_unknown_claim(deployment, admin, queue_url)),
            ("retry", lambda: cancellation_and_failures(deployment)),
            ("crash", lambda: worker_crash_recovery(deployment)),
            ("obligations", lambda: completed_obligations(deployment)),
            # This isolated namespace intentionally creates integrated tasks;
            # run it after the external-only task binding assertions above.
            ("startup", lambda: failed_startup_preserves_integrated_delivery(deployment)),
        ]
        results = []
        for name, scenario in cases:
            if args.scenario and name not in args.scenario:
                continue
            print(f"RUN {name}", flush=True)
            started = time.monotonic()
            detail = scenario()
            results.append({"scenario": name, "result": "passed", "detail": detail,
                            "elapsed_seconds": round(time.monotonic() - started, 3)})
            (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
            print(f"PASS {name}: {detail}", flush=True)
        deployment.server.stop()
        succeeded = True
        print(f"SQS separate-process acceptance passed: {len(results)} scenarios ({'AWS' if args.aws else 'ElasticMQ'})", flush=True)
        return 0
    except (AssertionError, OSError, RuntimeError, subprocess.SubprocessError) as error:
        (directory / "failure-traceback.txt").write_text(traceback.format_exc())
        if deployment is not None:
            diagnostics = {}
            for name, statement in {
                "tasks": "SELECT coalesce(json_agg(row_to_json(t)),'[]'::json) FROM (SELECT task_id,state,attempt_count,dispatch_destination FROM tasks) t",
                "intents": "SELECT coalesce(json_agg(row_to_json(t)),'[]'::json) FROM dispatch_intents t",
            }.items():
                try:
                    diagnostics[name] = json.loads(deployment.sql(statement))
                except (AssertionError, OSError, subprocess.SubprocessError):
                    diagnostics[name] = "unavailable"
            try:
                diagnostics["queue"] = admin.call("GetQueueAttributes", {"QueueUrl":queue_url,"AttributeNames":["All"]})
            except (OSError, RuntimeError, subprocess.SubprocessError):
                diagnostics["queue"] = "unavailable"
            (directory / "failure-diagnostics.json").write_text(json.dumps(diagnostics, indent=2) + "\n")
        print(f"SQS acceptance failed: {error}\nEvidence: {directory}", file=sys.stderr)
        return 1
    finally:
        try:
            if deployment:
                deployment.close()
        finally:
            try:
                if created:
                    database_admin(f'DROP DATABASE "{database}" WITH (FORCE)')
            finally:
                if queue_create_started:
                    # The CreateQueue response can be lost after remote success.
                    # Reconcile the same unique owned name before cleanup.
                    if queue_url is None:
                        queue_url = admin.queue_url(admin.call("GetQueueUrl", {"QueueName": queue_name}))
                    admin.call("DeleteQueue", {"QueueUrl": queue_url})
                (directory / "queue-api-calls.json").write_text(json.dumps(admin.calls, indent=2) + "\n")
                if temporary and succeeded:
                    shutil.rmtree(temporary)


if __name__ == "__main__":
    sys.exit(main())
