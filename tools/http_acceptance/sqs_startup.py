"""Failed SQS process startup must preserve an existing integrated deployment."""

import json
import subprocess

from .harness import Deployment, exchange, unused_port


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
                str(integrated.binaries / "ledgence-orchestrator"), "serve",
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
        return "four local startup failures preserve integrated execution; rejected activation drains notifications"
    finally:
        integrated.close()
