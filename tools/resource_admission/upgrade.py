"""A genuine native predecessor plus an independent provider that survives PostgreSQL restore (MIT)."""

import hashlib
import json

from http_acceptance.harness import eventually
from provider_operations.harness import CLAIM, ProviderDeployment, provider_class
from provider_operations.scenarios import restore_database, snapshot_database
from resource_admission.protected import fixture_source
from workflow_acceptance.owned_scenarios import quote


class UpgradeDeployment(ProviderDeployment):
    def server_args(self, config):
        args = super().server_args(config) + ["--console-dir", str(self.console_dist)]
        if self.admission_config:
            args += ["--admission-config", str(self.admission_config)]
        return args

    def start_workers(self, probes=True, proxy=None):
        self.worker("late-origin", proxy=proxy, runner=self.runner)
        if probes:
            return self.worker(
                "crash-replacement", runner=self.runner, queue=self.probe_queue
            )
        return None

    def offline(self):
        self.stop_workers()
        self.server.stop()

    def migration_rows(self):
        return json.loads(
            self.sql(
                "SELECT coalesce(json_object_agg(version, encode(checksum,'hex')),'{}') "
                "FROM _sqlx_migrations WHERE success"
            )
        )

    def check_console(self, version):
        status, headers, config = self.request(
            "GET", "/v1/console/config", credentials=self.operator
        )
        assert status == 200 and config["contract_version"] == version
        assert {k.lower(): v for k, v in headers.items()}[
            "ledgence-console-contract"
        ] == str(version)

    def admit_mode(self, mode):
        self.admission_config = self.directory / "admission.json"
        access = json.loads(self.config.read_text())
        self.admission_config.write_text(
            json.dumps(
                {
                    "v": 1,
                    "dispatch": mode,
                    "audience": {
                        k: access[k] for k in ("authority_id", "instance_id", "scope")
                    },
                }
            )
        )


def counts(provider, key=None):
    with provider.lock:
        events = (
            [json.loads(row) for row in provider.log_path.read_text().splitlines()]
            if provider.log_path.is_file()
            else []
        )
    return {
        "posts": sum(
            e["kind"] == "request"
            and e.get("method") == "POST"
            and (key is None or e.get("key") == key)
            for e in events
        ),
        "effects": sum(
            e["kind"] == "created" and (key is None or e["operation"]["key"] == key)
            for e in events
        ),
        "gets": sum(
            e["kind"] == "request"
            and e.get("method") == "GET"
            and (key is None or e.get("key") == key)
            for e in events
        ),
    }


def digest_rows(d):
    # Capture committed historical bytes before starting either new server or backfill.
    queries = {
        "task_inputs": "SELECT task_id,encode(input_bytes,'hex') FROM tasks ORDER BY task_id",
        "approvals": "SELECT workflow_id,wait_key,encode(snapshot_bytes,'hex'),encode(decision_bytes,'hex') "
        "FROM workflow_approvals ORDER BY workflow_id,wait_key",
        "provider_reports": "SELECT workflow_id,encode(report_bytes,'hex'),encode(receipt_bytes,'hex') "
        "FROM provider_reports ORDER BY workflow_id,report_id",
    }
    result = {}
    for key, query in queries.items():
        data = d.sql(query)
        assert data
        result[key] = hashlib.sha256(data.encode()).hexdigest()
    return result


def migrate_candidate(d, original, receipts):
    d.command("ledgence", ["orchestrator", "migrate"])
    first = d.migration_rows()
    expected = {
        p.name.split("_", 1)[0]: hashlib.sha384(p.read_bytes()).hexdigest()
        for p in (d.root / "crates/ledgence-adapter-postgres/migrations").glob("*.sql")
        if not p.name.endswith(".down.sql")
    }
    assert first == expected and all(
        first[v] == checksum for v, checksum in original.items()
    )
    applied = sorted(set(first) - set(original))
    assert "20261104010000" in applied
    d.command("ledgence", ["orchestrator", "migrate"])
    assert d.migration_rows() == first and digest_rows(d) == receipts
    return applied


def seed(d, provider):
    # All these writes are done by the actual predecessor executable and helper.
    d.command("ledgence", ["orchestrator", "migrate"])
    d.provision()
    d.start_server()
    d.check_console(7)
    d.publish_reference()
    d.publish("upgrade-task", "1.0.0")
    probe = d.start_workers()
    known, paths = d.begin("old-known", provider)
    d.finished(known, paths, "provider_succeeded")
    probe.stop()
    provider.set_fault("old-unknown", post_response="drop")
    unknown, unknown_paths = d.begin("old-unknown", provider)
    d.parked(unknown)
    assert d.inspect(unknown)["summary"]["provider"]["knowledge"] == "unknown"
    pending, _ = d.request_approval("old-pending", provider)
    d.stop_workers()
    command = d.submission("old-queued", program="upgrade-task")
    command["input"]["queue"] = "upgrade-ordinary"
    task = d.checked("POST", "/v1/tasks", command)["task_id"]
    assert d.task(task)["state"] == "queued"
    assert (
        counts(provider, "old-known")["effects"]
        == counts(provider, "old-unknown")["effects"]
        == 1
    )
    assert counts(provider, "old-pending")["posts"] == 0
    d.server.stop()
    assert d.sql("SELECT to_regclass('public.run_budgets') IS NULL") == "t"
    decision = json.loads(
        d.sql(
            "SELECT convert_from(decision_bytes,'UTF8') FROM workflow_approvals WHERE workflow_id="
            + quote(known["workflow_id"])
        )
    )
    return {
        "known": known,
        "decision": decision,
        "unknown": unknown,
        "unknown_paths": unknown_paths,
        "pending": pending,
        "command": command,
        "task": task,
    }


def replay(d, seeded):
    assert (
        d.checked("POST", "/v1/tasks", seeded["command"])["task_id"] == seeded["task"]
    )
    assert d.checked("POST", "/v1/workflows/approvals/decide", seeded["decision"])[
        "already_accepted"
    ]
    pending = seeded["pending"]
    query = {"scope": d.scope, "workflow_id": pending["workflow_id"], "key": "export"}
    assert d.checked("POST", "/v1/workflows/approvals/inspect", query) == pending
    d.checked(
        "POST",
        "/v1/budgets/inspect",
        {"scope": d.scope, "subject": {"kind": "task", "task_id": seeded["task"]}},
        expected=404,
    )


def exercise(
    d, previous, bins, console, candidate_runner, restore_url, backup, pgtools
):
    d.console_dist = previous / "console"
    d.runner = previous / "runtime/ledgence/worker/bootstrap.py"
    d.admission_config = None
    with provider_class(d.root)(
        d.directory / "external-provider", completion_delay=0.05
    ) as provider:
        seeded = seed(d, provider)
        original, receipts = d.migration_rows(), digest_rows(d)
        assert max(original) == "20261103060000"
        snapshot_database(pgtools["pg_dump"], d.database_url, backup)
        with backup.open("rb") as stream:
            backup_sha = hashlib.file_digest(stream, "sha256").hexdigest()

        d.binaries, d.runner, d.console_dist = bins, candidate_runner, console
        applied = migrate_candidate(d, original, receipts)
        d.admit_mode("enabled")
        d.start_server()
        d.check_console(14)
        replay(d, seeded)
        old_worker = d.worker(
            "late-outsider",
            queue="upgrade-ordinary",
            binaries=previous / "bin",
            runner=previous / "runtime/ledgence/worker/bootstrap.py",
        )
        eventually(
            lambda: d.task(seeded["task"])["state"] == "succeeded",
            description="predecessor worker executes ordinary queued work after migration",
        )
        old_worker.stop()
        assert json.loads(d.marker.read_text().strip())["task"] == seeded["task"]
        d.start_workers()
        d.finished(seeded["unknown"], seeded["unknown_paths"], "provider_succeeded")
        assert d.task(seeded["task"])["attempt_count"] == 1
        assert counts(provider, "old-unknown")["posts"] == 1
        # Old credentials do not acquire the new resource-control permission.
        resource = {
            "scope": d.scope,
            "name": "export-dispatch",
            "expected_revision": None,
            "request_id": "create-export-limit",
            "reason": "Upgrade qualification",
            "policy": {
                "enabled": True,
                "max_occupancy": 1,
                "rate": None,
                "offer_timeout_ms": 30_000,
                "start_timeout_ms": 5_000,
            },
        }
        d.checked("POST", "/v1/resources/put", resource, expected=403)
        d.offline()
        controller, d.config, _ = d.issue(
            "capacity-control", "resource-owner", ["resource_control", "metadata_read"]
        )
        d.start_server()
        d.checked("POST", "/v1/resources/put", resource, credentials=controller)
        d.publish(
            "provider-admission",
            "1.0.0",
            program_source=fixture_source(d.root),
            runtime_protocol=3,
        )
        d.start_workers()
        later, paths = d.begin(
            "post-backup",
            provider,
            program="provider-admission",
            budget={"max_calls": 1},
        )
        d.finished(later, paths, "provider_succeeded")
        query = {
            "scope": d.scope,
            "subject": {"kind": "workflow", "workflow_id": later["workflow_id"]},
        }
        budget = d.checked("POST", "/v1/budgets/inspect", query)
        assert budget["calls"] == "1" and counts(provider, "post-backup")["posts"] == 1
        d.offline()
        prior_unknown_gets = counts(provider, "old-unknown")["gets"]
        external_before_restore = counts(provider)
        before_restore = counts(provider, "post-backup")
        assert before_restore["effects"] == 1

        # Restore to a *fresh* database. pg_restore --clean alone does not discard
        # post-backup tables that the predecessor archive has never heard of.
        d.database_url = restore_url
        d.environment["DATABASE_URL"] = restore_url
        restore_database(pgtools["pg_restore"], restore_url, backup)
        assert (
            d.migration_rows() == original
            and d.sql("SELECT to_regclass('public.run_budgets') IS NULL") == "t"
        )
        assert migrate_candidate(d, original, receipts) == applied
        assert (
            d.sql(
                "SELECT count(*) FROM workflow_runs WHERE workflow_id="
                + quote(later["workflow_id"])
            )
            == "0"
        )
        assert d.sql("SELECT count(*) FROM run_budget_members") == "0"
        # Isolate admission quarantine: ordinary provider dispatch remains enabled.
        # A second production deployment would also set declared_restore_quarantine.
        d.admit_mode("quarantined")
        d.start_server()
        replay(d, seeded)
        d.checked("POST", "/v1/budgets/inspect", query, expected=404)
        proxy = d.authenticated_proxy()
        d.start_workers(proxy=proxy)
        # The SQL rollback re-executes the continuation, whose diagnostic file
        # is deliberately outside the restored DB just like the real provider.
        eventually(
            lambda: d.status(seeded["unknown"]["workflow_id"])["state"] == "succeeded",
            timeout=90,
            description="predecessor unknown operation recovered under quarantine",
        )
        wakes = d.wakes(seeded["unknown_paths"])
        assert (
            len(wakes) == 2
            and wakes[0]["occurrence"] == wakes[1]["occurrence"] == seeded["unknown"]
        )
        assert all(wake["selection"]["kind"] == "provider_succeeded" for wake in wakes)
        assert counts(provider, "old-unknown")["gets"] > prior_unknown_gets
        blocked, _ = d.begin(
            "restored-new-request", provider, business_key="post-backup"
        )
        eventually(
            lambda: d.status(blocked["workflow_id"])["state"] == "failed",
            timeout=60,
            description="fresh unbudgeted claim refused despite absent root membership",
        )
        assert (
            d.sql(
                "SELECT count(*) FROM provider_operations WHERE workflow_id="
                + quote(blocked["workflow_id"])
            )
            == "0"
        )
        refused_claims = proxy.commands(
            CLAIM, lambda body: body["workflow_id"] == blocked["workflow_id"]
        )
        assert refused_claims and all(row["status"] == 403 for row in refused_claims)
        after_quarantine = counts(provider)
        assert all(
            after_quarantine[k] == external_before_restore[k]
            for k in ("posts", "effects")
        )
        assert counts(provider, "post-backup") == before_restore
        assert (
            counts(provider, "old-unknown")["posts"]
            == counts(provider, "old-known")["posts"]
            == 1
        )
        # This is a disposable recovery drill. Use a new business key for the
        # positive control so enabling dispatch can never repeat the old effect.
        d.offline()
        d.admit_mode("enabled")
        d.start_server()
        d.start_workers()
        control, paths = d.begin("enabled-after-reconcile", provider)
        d.finished(control, paths, "provider_succeeded")
        control_counts = counts(provider, "enabled-after-reconcile")
        assert control_counts["posts"] == control_counts["effects"] == 1
        after_control = counts(provider)
        assert all(
            after_control[k] == after_quarantine[k] + 1 for k in ("posts", "effects")
        )
        assert counts(provider, "post-backup") == before_restore
        return {
            "quarantined_claim_statuses": [row["status"] for row in refused_claims],
            "external_before_restore": external_before_restore,
            "external_under_quarantine": after_quarantine,
            "enabled_control": control_counts,
            "applied_migrations": applied,
            "old_rows_sha256": receipts,
            "backup_sha256": backup_sha,
            "old_queued_task": seeded["task"],
            "old_pending_approval": seeded["pending"]["workflow_id"],
            "upgraded_unknown_operation": seeded["unknown"],
            "post_backup_ledger": budget,
            "same_effect_after_restore": before_restore,
            "refused_workflow_id": blocked["workflow_id"],
            "restored_read_only_recovery": counts(provider, "old-unknown"),
            "original_operator_denied_resource_control": True,
        }
