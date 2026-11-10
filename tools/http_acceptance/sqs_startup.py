"""Failed SQS process startup must preserve an existing integrated deployment."""

import json
import subprocess
import uuid

from instance_access.harness import AccessDeployment
from postgres_fixture import create_owned_database, owned_database_url

from .harness import Deployment, exchange, unused_port, eventually


def failed_startup_preserves_integrated_delivery(external):
    directory = external.directory / "startup"
    directory.mkdir()
    integrated = Deployment(
        external.root, directory, external.binaries, external.python,
        external.database_url, external.psql,
    )
    integrated.scope = dict(external.scope, namespace="startup-regression")
    integrated.environment.update(
        LEDGENCE_POSTGRES_NOTIFICATIONS="on",
        LEDGENCE_POSTGRES_NOTIFICATION_URL=external.database_url,
    )
    scope_sql = " AND ".join(
        key + "='" + integrated.scope[key].replace("'", "''") + "'"
        for key in ("tenant_id", "namespace")
    )
    results = []
    try:
        integrated.server, base = integrated.start_server()
        not_directory = directory / "not-a-program-store"
        not_directory.write_text("deliberately not a directory\n")
        cases = [
            ("occupied-port", integrated.server_port, integrated.artifacts.url, {},
             "could not bind HTTP listener"),
            ("invalid-program-store", None, str(not_directory), {},
             "program store root must be a directory"),
            ("invalid-notification-toggle", None, integrated.artifacts.url,
             {"LEDGENCE_POSTGRES_NOTIFICATIONS": "invalid"},
             "LEDGENCE_POSTGRES_NOTIFICATIONS must be on or off"),
            ("invalid-notification-url", None, integrated.artifacts.url,
             {"LEDGENCE_POSTGRES_NOTIFICATION_URL": "postgres://[invalid"},
             "database operation failed; reconcile using the same command"),
            ("activation-rejected", None, integrated.artifacts.url, {},
             "external routing requires an empty logical queue"),
        ]
        for name, port, store, overrides, diagnostic in cases:
            integrated.queue = "startup-" + name
            config = json.loads(external.delivery_config.read_text())
            config["route"] = {
                "scope": integrated.scope, "queue": integrated.queue,
                "destination": "startup-" + name,
            }
            config_path = directory / f"{name}.json"
            config_path.write_text(json.dumps(config, indent=2) + "\n")
            # Prove this same process served the queue before the failed start.
            status, _, session = exchange(base, "POST", "/v1/worker-sessions", {
                "scope": integrated.scope, "queue": integrated.queue, "concurrency": 1,
            })
            assert status == 200, session
            operation = {
                "scope": integrated.scope, "queue": integrated.queue,
                "worker_session_id": session["id"], "consumer_id": 0, "sequence": 1,
            }
            before = exchange(base, "POST", "/v1/acquisitions", operation)
            assert before[0] == 200 and before[2]["disposition"] == "empty", before

            # A real activation rejection happens after notifications start.
            # Keep its pre-existing task queued until the failed process exits.
            preexisting = None
            if name == "activation-rejected":
                preexisting = integrated.submit(integrated.submission(name + "-before"))["task_id"]
            failed = subprocess.run([
                str(integrated.binaries / "ledgence"), "orchestrator", "serve",
                "--bind", f"127.0.0.1:{port or unused_port()}", "--store", store,
                "--delivery-config", str(config_path),
            ], env=dict(integrated.environment, **overrides), capture_output=True, timeout=40)
            (directory / f"{name}.stdout").write_bytes(failed.stdout)
            (directory / f"{name}.stderr").write_bytes(failed.stderr)
            stderr = failed.stderr.decode(errors="replace")
            assert failed.returncode == 1, (name, failed.returncode, stderr)
            assert diagnostic in stderr, (name, stderr)
            if preexisting:
                assert "acquisition notifications stopped" in stderr, stderr
            else:
                operation["sequence"] = 2
                after = exchange(base, "POST", "/v1/acquisitions", operation)
                assert after[0] == 200 and after[2]["disposition"] == "empty", after

            # SQL is scoped to this isolated test namespace, never the external
            # route exercised by the rest of check-sqs.py.
            assert integrated.sql(
                f"SELECT count(*) FROM dispatch_routes WHERE {scope_sql} AND destination IS NOT NULL"
            ) == "0", f"{name} persisted an external route"
            task_id = integrated.submit(integrated.submission(name + "-after"))["task_id"]
            assert integrated.sql(
                "SELECT count(*) FROM dispatch_intents i JOIN tasks t USING(task_id) "
                f"WHERE {scope_sql}"
            ) == "0", f"{name} left an external delivery intent"
            worker = integrated.start_worker()
            task_ids = [preexisting, task_id] if preexisting else [task_id]
            for accepted in task_ids:
                task, _ = integrated.terminal(accepted)
                assert task["attempt_count"] == 1, task
                assert len(integrated.invocations(accepted)) == 1
            worker.stop()
            assert exchange(base, "GET", "/health/ready")[0] == 200
            results.append({"case": name, "failed_server_exit": failed.returncode,
                            "task_ids": task_ids, "result": "passed"})
            (directory / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        assert integrated.sql(
            f"SELECT count(*) FROM tasks WHERE {scope_sql} AND state <> 'succeeded'"
        ) == "0", "startup regressions left unfinished integrated tasks"
        integrated.server.stop()
        artifact_startup_preserves_integrated_delivery(external)
        return "local startup failures, including artifact configuration, preserve integrated execution; rejected activation drains notifications"
    finally:
        integrated.close()


def artifact_startup_preserves_integrated_delivery(external):
    """A bad artifact prerequisite must not activate routes or callback bindings.

    The artifact profile requires a fixed instance, so use an owned child DB.
    Binding the existing broad SQS test database would affect unrelated suites.
    The external deployment supplies only its preflighted local queue config.
    """
    directory = external.directory / "artifact-startup"
    directory.mkdir()
    parent = owned_database_url(external.database_url, "postgres")
    database = "ldg_artifact_startup_" + uuid.uuid4().hex
    url = owned_database_url(parent, database)
    created, passed, clean = False, False, True
    deployment = None

    def admin(sql):
        result = subprocess.run([external.psql, "--dbname", parent, "-X", "--set", "ON_ERROR_STOP=1", "--command", sql],
                                capture_output=True, timeout=40)
        assert result.returncode == 0, "owned startup database setup/cleanup failed"

    try:
        create_owned_database(admin, database); created = True
        deployment = AccessDeployment(external.root, directory, external.binaries, external.python, url, external.psql)
        deployment.command("ledgence", ["orchestrator", "migrate"])
        deployment.provision()
        deployment.start_server()
        route = json.loads(external.delivery_config.read_text())
        route["route"] = {"scope": deployment.scope, "queue": deployment.queue, "destination": "artifact-startup"}
        route_path = directory / "delivery.json"
        route_path.write_text(json.dumps(route))
        invalid = directory / "invalid-artifacts.json"
        invalid.write_text('{"schema":"ledgence.artifact-store.v1","unexpected":true}')
        completion = directory / "completion.json"
        completion.write_text(json.dumps({"destinations": [{"scope": deployment.scope,
            "destination": "artifact-startup", "url": deployment.server_url + "/unused-callback"}]}))
        # Before the failed start, a real worker session can consume this empty
        # integrated queue. The same session must remain usable afterwards.
        session = deployment.checked("POST", "/v1/worker-sessions", {
            "scope": deployment.scope, "queue": deployment.queue, "concurrency": 1,
        }, credentials=deployment.worker_credentials)
        acquisition = {"scope": deployment.scope, "queue": deployment.queue,
                       "worker_session_id": session["id"], "consumer_id": 0, "sequence": 1}
        assert deployment.checked("POST", "/v1/acquisitions", acquisition,
                                  credentials=deployment.worker_credentials)["disposition"] == "empty"
        args = deployment.server_args(deployment.config)
        args[args.index("--bind") + 1] = f"127.0.0.1:{unused_port()}"
        args += ["--delivery-config", str(route_path), "--completion-config", str(completion),
                 "--artifact-config", str(invalid)]
        failed = subprocess.run(args, env=deployment.environment, capture_output=True, timeout=40)
        (directory / "failed-server.stdout").write_bytes(failed.stdout)
        (directory / "failed-server.stderr").write_bytes(failed.stderr)
        assert failed.returncode == 1
        assert b"invalid artifact configuration JSON" in failed.stderr
        assert deployment.sql("SELECT count(*) FROM dispatch_routes WHERE destination IS NOT NULL") == "0", "invalid artifact config activated a durable external route"
        assert deployment.sql("SELECT count(*) FROM completion_destinations") == "0", "invalid artifact config persisted a callback binding"
        acquisition["sequence"] = 2
        assert deployment.checked("POST", "/v1/acquisitions", acquisition,
                                  credentials=deployment.worker_credentials)["disposition"] == "empty"
        accepted = deployment.submit(deployment.submission("after-artifact-startup-failure"))["task_id"]
        assert deployment.sql("SELECT count(*) FROM dispatch_intents") == "0"
        worker = deployment.start_worker()
        task = eventually(lambda: (value if (value := deployment.task(accepted))["state"] in
                                   ("succeeded", "failed", "cancelled") else None), timeout=40,
                          description="integrated task after artifact startup refusal")
        assert task["state"] == "succeeded" and task["attempt_count"] == 1
        assert len(deployment.invocations(accepted)) == 1
        worker.stop(); deployment.server.stop()
        passed = True
    finally:
        try:
            if deployment:
                try:
                    deployment.close()
                finally:
                    clean = deployment.remove_credentials_and_scan()
        finally:
            if created:
                admin(f'DROP DATABASE "{database}" WITH (FORCE)')
            (directory / "summary.json").write_text(json.dumps({"passed": passed and clean,
                "credential_scan_clean": clean, "database": database}, indent=2) + "\n")
        assert clean, "startup fixture exposed a credential"
    return "artifact configuration failure preserved integrated delivery and left no external intents"
