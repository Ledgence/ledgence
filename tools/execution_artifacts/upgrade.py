"""Published 0.9 native upgrade, interrupted DDL and fresh restore rehearsal (MIT)."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import socket

from http_acceptance.harness import Deployment, Process, eventually
from instance_access.harness import AccessDeployment
from provider_operations.scenarios import snapshot_database, restore_database
from resource_admission.common import ResourceControls
from workflow_acceptance.owned_scenarios import quote, external

ARTIFACT_MIGRATION = "20261110000000"


class ArtifactUpgradeDeployment(ResourceControls, AccessDeployment):
    def __init__(self, *args):
        super().__init__(*args)
        self.environment["PYTHONDONTWRITEBYTECODE"] = "1"
        self.artifact_config = None

    def provision(self):
        super().provision()
        self.operator, self.config, _ = self.issue("upgrade-operator", "upgrade-operator", [
            "metadata_read", "sensitive_read", "execution_submit", "execution_control",
            "program_publish", "approval_decide", "effect_resolve", "resource_control"])
        access = json.loads(self.config.read_text())
        self.admission_config = self.directory / "admission.json"
        self.admission_config.write_text(json.dumps({"v": 1, "dispatch": "enabled", "audience": {
            key: access[key] for key in ("authority_id", "instance_id", "scope")}}))

    def server_args(self, config):
        args = super().server_args(config) + ["--console-dir", str(self.console_dist),
                "--admission-config", str(self.admission_config),
                "--completion-config", str(self.completion_config)]
        if self.artifact_config is not None:
            args += ["--artifact-config", str(self.artifact_config)]
        return args

    def worker(self, queue=None):
        previous = self.queue
        try:
            self.queue = queue or previous
            return Deployment.start_worker(self, runner=self.runner, credentials_file=self.worker_credentials,
                                           cache="worker-cache-" + str(self.counter))
        finally:
            self.queue = previous

    def offline(self):
        for process in self.processes:
            if process.label.startswith("worker-") and process.process.poll() is None:
                process.stop()
        if self.server.process.poll() is None:
            self.server.stop()

    def migration_rows(self):
        return json.loads(self.sql("SELECT coalesce(json_object_agg(version,encode(checksum,'hex')),'{}') "
                                   "FROM _sqlx_migrations WHERE success"))

    def workflow(self, workflow_id):
        return self.checked("GET", self.query("/v1/workflows/status", workflow_id=workflow_id))


def fingerprint(d, columns=None):
    """Freeze every predecessor column and row, including bytea journal/receipt values."""
    if columns is None:
        columns = json.loads(d.sql("SELECT json_object_agg(table_name,cols) FROM (SELECT table_name,"
            "json_agg(column_name ORDER BY ordinal_position) cols FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name<>'_sqlx_migrations' GROUP BY table_name) q"))
    hashes, counts = {}, {}
    for table, names in sorted(columns.items()):
        # Both identifiers are selected from our owned PostgreSQL catalog.
        selected = ",".join('"' + name.replace('"', '""') + '"' for name in names)
        quoted = '"' + table.replace('"', '""') + '"'
        rows = d.sql(f"SELECT row_to_json(q)::text FROM (SELECT {selected} FROM {quoted}) q ORDER BY 1")
        hashes[table] = hashlib.sha256(rows.encode()).hexdigest()
        counts[table] = int(d.sql(f"SELECT count(*) FROM {quoted}"))
    return {"columns": columns, "sha256": hashes, "counts": counts}


def migration_manifest(d):
    return {path.name.split("_", 1)[0]: hashlib.sha384(path.read_bytes()).hexdigest()
            for path in (d.root / "crates/ledgence-adapter-postgres/migrations").glob("*.sql")
            if not path.name.endswith(".down.sql")}


def migrate_twice(d, original, before):
    expected = migration_manifest(d)
    assert ARTIFACT_MIGRATION in expected and ARTIFACT_MIGRATION not in original
    assert all(expected.get(version) == checksum for version, checksum in original.items())
    for _ in range(2):
        d.command("ledgence", ["orchestrator", "migrate"])
        assert d.migration_rows() == expected
        assert fingerprint(d, before["columns"]) == before
    return sorted(set(expected) - set(original))


def interrupt_migration(d, original, before):
    """Interrupt actual candidate DDL after its first table, inside its transaction."""
    d.sql("CREATE FUNCTION ldg_upgrade_pause() RETURNS event_trigger LANGUAGE plpgsql AS $$ "
          "BEGIN IF to_regclass('public.artifact_stores') IS NOT NULL THEN PERFORM pg_sleep(600); END IF; END $$; "
          "CREATE EVENT TRIGGER ldg_upgrade_pause ON ddl_command_end WHEN TAG IN ('CREATE TABLE') "
          "EXECUTE FUNCTION ldg_upgrade_pause()")
    process = Process([str(d.binaries / "ledgence"), "orchestrator", "migrate", "--timeout-ms", "10000"],
                      d.directory, "interrupted-migration", d.environment)
    d.processes.append(process)
    try:
        # Observe the executing DDL, not merely a client that failed to start.
        eventually(lambda: int(d.sql("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
            "AND pid<>pg_backend_pid() AND wait_event='PgSleep' AND query LIKE '%CREATE TABLE artifact_stores%'")) > 0,
            timeout=8, description="candidate artifact DDL suspended inside transaction")
        process.process.wait(timeout=12)
        assert process.process.returncode != 0
        combined = process.stderr_path.read_text() + process.stdout_path.read_text()
        assert "migration" in combined.lower() and ("timed out" in combined.lower() or "timeout" in combined.lower())
        eventually(lambda: int(d.sql("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
            "AND pid<>pg_backend_pid() AND wait_event='PgSleep'")) == 0, timeout=12,
            description="timed-out migration connection closes and PostgreSQL rolls back")
        assert d.sql("SELECT to_regclass('public.artifact_stores') IS NULL") == "t"
        applied = d.migration_rows()
        assert ARTIFACT_MIGRATION not in applied and all(applied[v] == c for v, c in original.items())
        assert fingerprint(d, before["columns"]) == before
        return {"observed_transactional_ddl": True, "nonzero_exit": True,
                "artifact_migration_rolled_back": True, "historical_rows_unchanged": True}
    finally:
        process.cleanup()
        d.sql("DROP EVENT TRIGGER ldg_upgrade_pause; DROP FUNCTION ldg_upgrade_pause()")


def reject_predecessor(d, previous, before):
    # Exact arguments already used successfully by the predecessor, no new flags.
    saved, saved_console = d.binaries, d.console_dist
    d.binaries, d.console_dist = previous / "bin", previous / "console"
    try:
        process = Process(d.server_args(d.config), d.directory, "old-schema-rejection", d.environment)
        d.processes.append(process)
        def exited_without_readiness():
            with socket.socket() as probe:
                probe.settimeout(0.1)
                assert probe.connect_ex(("127.0.0.1", d.server_port)) != 0, "old binary exposed a listener on the new schema"
            return process.process.poll() is not None
        eventually(exited_without_readiness, timeout=20, description="old binary rejects upgraded schema before readiness")
        assert process.process.returncode != 0
        output = process.stderr_path.read_text() + process.stdout_path.read_text()
        assert "database schema does not match this orchestrator's migrations" in output
        assert "unexpected argument" not in output and "unknown argument" not in output
        assert fingerprint(d, before["columns"]) == before
        return {"nonzero_exit": True, "schema_error_verified": True, "readiness_never_opened": True, "historical_rows_unchanged": True}
    finally:
        d.binaries, d.console_dist = saved, saved_console


def seed(d, receiver):
    d.command("ledgence", ["orchestrator", "migrate"])
    d.provision()
    d.start_server()
    d.policy(enabled=False)
    queue = {"scope": d.scope, "queue": "upgrade-paused", "request_id": "old-queue-limit",
             "expected_revision": None, "policy": {"max_concurrency": 0, "timeout": None},
             "reason": "Published predecessor upgrade fixture"}
    d.checked("POST", "/v1/queue-policies/put", queue)
    source = (d.root / "tools/workflow_acceptance/fork_program.py").read_text().replace("timeout_ms=120_000", "timeout_ms=3_600_000")
    d.publish("upgrade-fork", "1.0.0", program_source=source, runtime_protocol=3)
    command = d.submission("old-fork", program="upgrade-fork", tag="upgrade-fork",
                           marker=str(d.directory / "fork.jsonl"), queue=d.queue, wait_remote=True)
    command["input"]["data"]["marker"] = str(d.directory / "fork.jsonl")
    workflow = d.checked("POST", "/v1/workflows", command)["workflow_id"]
    terminal = d.checked("POST", "/v1/tasks", d.submission("old-terminal"))["task_id"]
    subscription = d.checked("POST", "/v1/completion-subscriptions", {"scope": d.scope,
        "target": {"kind": "task", "id": terminal}, "destination": "upgrade-results", "idempotency_key": "old-callback"})
    governed = d.submission("old-resource-wait")
    governed["input"]["admission"] = {"resource": d.resource_name}
    parked = d.checked("POST", "/v1/tasks", governed)["task_id"]
    worker = d.worker()
    eventually(lambda: d.workflow(workflow)["state"] == "waiting"
               and int(d.sql("SELECT count(*) FROM workflow_runs WHERE root_workflow_id=" + quote(workflow) + " AND state='waiting'")) == 2,
               timeout=60, description="predecessor parent local step and two durable fork waits")
    eventually(lambda: d.task(terminal)["state"] == "succeeded", description="predecessor terminal task")
    occurrence = {"scope": d.scope, "task_id": parked, "generation": 1}
    def waiting():
        status, _, value = d.request("POST", "/v1/task-admissions/inspect", {"occurrence": occurrence}, credentials=d.operator)
        assert status in (200, 404)
        return status == 200 and value["state"] == "waiting"
    eventually(waiting, description="predecessor durable task admission wait")
    eventually(lambda: receiver.for_subscription(subscription["subscription_id"]),
               description="predecessor callback attempts fail while terminal task stays retained")
    worker.stop()
    queued_command = d.submission("old-ordinary-queued")
    queued_command["input"]["queue"] = "upgrade-queued"
    queued = d.checked("POST", "/v1/tasks", queued_command)["task_id"]
    d.offline()
    forks = json.loads(d.sql("SELECT json_agg(workflow_id ORDER BY workflow_id) FROM workflow_runs WHERE root_workflow_id=" + quote(workflow)))
    assert len(forks) == 2
    assert len(receiver.for_subscription(subscription["subscription_id"])) >= 1
    return {"workflow": workflow, "branches": forks, "terminal": terminal, "subscription": subscription["subscription_id"],
            "parked": parked, "queued": queued, "queued_command": queued_command, "queue_policy": queue}


def resume(d, seeded, receiver):
    d.start_server()
    config = d.checked("GET", "/v1/console/config")
    assert config["contract_version"] == json.loads((d.console_dist / "console-manifest.json").read_text())["console_contract_version"]
    assert d.checked("POST", "/v1/tasks", seeded["queued_command"])["task_id"] == seeded["queued"]
    replay = d.checked("POST", "/v1/queue-policies/put", seeded["queue_policy"])
    assert replay["already_accepted"]
    d.policy(enabled=True)
    receiver.set("accept")
    worker, ordinary = d.worker(), d.worker("upgrade-queued")
    for index, branch in enumerate(seeded["branches"]):
        event = external(d, branch, "release", "upgrade-release-" + str(index), approved=True)
        d.checked("POST", "/v1/workflows/events", event)
        d.checked("POST", "/v1/workflows/events", event)
    eventually(lambda: d.workflow(seeded["workflow"])["state"] == "succeeded", timeout=60,
               description="upgraded fork joins preserved local result")
    for task in (seeded["queued"], seeded["parked"]):
        eventually(lambda: d.task(task)["state"] == "succeeded", description="predecessor work executes after candidate restore")
        assert d.task(task)["attempt_count"] == 1
    eventually(lambda: d.sql("SELECT state FROM completion_subscriptions WHERE subscription_id=" + quote(seeded["subscription"])) == "delivered",
               timeout=45, description="restored durable callback acknowledged")
    markers = [json.loads(line) for line in (d.directory / "fork.jsonl").read_text().splitlines()]
    assert sum(row["kind"] == "fork_local_finished" for row in markers) == 1
    assert sum(row["kind"] == "fork_join" for row in markers) == 1
    worker.stop(); ordinary.stop()
    d.offline()
    return {"queued_task_completed_once": True, "parked_admission_completed_once": True,
            "fork_join_completed_once": True, "local_step_not_reexecuted": True, "callback_delivered": True,
            "submission_and_queue_receipts_replayed": True}



def artifacts_after_restore(d, seeded):
    """Enable the new feature on a preserved predecessor root, then restart it."""
    from execution_artifacts.harness import ArtifactDeployment
    writer, d.config, _ = d.issue("artifact-writer", "upgrade-artifact-writer", [
        "artifact_write", "metadata_read", "sensitive_read"])
    objects, spool = d.directory / "upgrade-objects", d.directory / "upgrade-spool"
    objects.mkdir(); spool.mkdir()
    d.artifact_config = d.directory / "upgrade-artifacts.json"
    d.artifact_config.write_text(json.dumps({"schema": "ledgence.artifact-store.v1", "store_id": "upgrade-artifacts",
        "backend": {"kind": "filesystem", "directory": str(objects)}, "spool_directory": str(spool)}))
    d.start_server()
    payload = bytes(range(256)) * 4097
    source = d.directory / "upgrade-input.bin"; source.write_bytes(payload)
    d.checked("POST", "/v1/artifacts/prepare", {"requester": {"kind": "external", "scope": d.scope}, "command": {
        "scope": d.scope, "target": {"kind": "task", "id": seeded["terminal"]}, "key": "upgrade-report",
        "metadata": {"digest": "sha256:" + hashlib.sha256(payload).hexdigest(), "size": len(payload),
                     "media_type": "application/octet-stream", "filename": source.name}}}, expected=403)
    command = ["--task", seeded["terminal"], "--key", "upgrade-report", "--file", str(source)]
    reference = ArtifactDeployment.artifact_command(d, "put", command, credentials=writer)
    assert ArtifactDeployment.artifact_command(d, "put", command, credentials=writer) == reference
    d.offline(); d.start_server()
    manifest = ArtifactDeployment.artifact_command(d, "inspect", ["--artifact", reference["id"]], credentials=writer)
    assert manifest["reference"] == reference and manifest["root"] == {"kind": "task", "id": seeded["terminal"]}
    destination = d.directory / "upgrade-download.bin"
    ArtifactDeployment.artifact_command(d, "download", ["--artifact", reference["id"], "--file", str(destination)], credentials=writer)
    assert destination.read_bytes() == payload
    assert d.sql("SELECT count(*) FROM execution_artifacts") == d.sql("SELECT count(*) FROM artifact_uploads") == "1"
    d.offline()
    return {"old_credential_denied_artifact_write": True, "preserved_root_accepts_artifact": True,
            "same_key_replays_reference": True, "restart_verified_bytes": True, "reference": reference}


def exercise(d, previous, bins, console, candidate_runner, restore_url, backup, pgtools):
    spec = importlib.util.spec_from_file_location("artifact_upgrade_receiver", d.root / "tools/check-completions.py")
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    receiver = module.Receiver(); receiver.set("fail")
    d.console_dist, d.runner = previous / "console", previous / "runtime/ledgence/worker/bootstrap.py"
    d.completion_config = d.directory / "completion.json"
    d.completion_config.write_text(json.dumps({"destinations": [{"scope": d.scope,
        "destination": "upgrade-results", "url": receiver.url}]}))
    try:
        seeded = seed(d, receiver)
        original, before = d.migration_rows(), fingerprint(d)
        assert max(original) == "20261104030000" and ARTIFACT_MIGRATION not in original
        snapshot_database(pgtools["pg_dump"], d.database_url, backup)
        with backup.open("rb") as stream:
            backup_sha = hashlib.file_digest(stream, "sha256").hexdigest()
        d.binaries, d.runner, d.console_dist = bins, candidate_runner, console
        interrupted = interrupt_migration(d, original, before)
        applied = migrate_twice(d, original, before)
        rejected = reject_predecessor(d, previous, before)
        # A new empty database is mandatory. --clean cannot remove candidate-only tables.
        d.database_url = restore_url; d.environment["DATABASE_URL"] = restore_url
        assert d.sql("SELECT count(*) FROM information_schema.tables WHERE table_schema='public'") == "0"
        restore_database(pgtools["pg_restore"], restore_url, backup)
        assert d.migration_rows() == original and fingerprint(d, before["columns"]) == before
        assert d.sql("SELECT to_regclass('public.artifact_stores') IS NULL") == "t"
        assert migrate_twice(d, original, before) == applied
        resumed = resume(d, seeded, receiver)
        artifacts = artifacts_after_restore(d, seeded)
        receiver.assert_healthy()
        return {"predecessor_migrations": original, "applied_migrations": applied, "seeded": seeded,
                "old_rows": before, "backup_sha256": backup_sha, "interrupted_migration": interrupted,
                "old_binary_rejection": rejected, "double_migration": True, "fresh_restore": True,
                "restored_rows_unchanged": True, "resumed": resumed, "artifacts": artifacts}
    finally:
        receiver.close()
