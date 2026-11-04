"""Check exported admission metrics against an independent provider and durable ledger (MIT)."""

import json
import os

from http_acceptance.harness import Process, eventually

from .harness import START
from .scenarios import lost_start

START_RESULT = "ledgence.admission.start.result"
REFUSAL = "ledgence.run_budget.refusal"


def stop_workers(deployment):
    for process in deployment.processes:
        if process.label.startswith("worker-") and process.process.poll() is None:
            process.stop()


def exercise(deployment, provider):
    # A lost Granted response produces no physical request. An explicit replay
    # of the identical Start returns the source retained by that first commit.
    original = lost_start(deployment, provider)
    [exchange] = deployment.proxies[-1].commands(START)
    replay = deployment.checked(
        "POST", START, exchange["json"], credentials=deployment.worker_credentials
    )
    assert replay["kind"] == "existing" and replay["snapshot"]["source"]
    assert "lost-start" not in provider.census()["requests"]

    worker = deployment.worker()
    try:
        command = deployment.submission(
            "metric-call",
            program="resource-admission",
            endpoint=provider.url,
            text="metric-call",
        )
        command["input"]["budget"] = {"max_calls": 0}
        workflow = deployment.checked("POST", "/v1/workflows", command)["workflow_id"]
        deployment.terminal(workflow)
        assert (
            deployment.snapshot(workflow)["calls"] == "0"
            and "metric-call" not in provider.census()["requests"]
        )
        [call] = deployment.entries(workflow)
        assert call["reason"] == "calls_exhausted"

        # Fail the first write at COMMIT, after the scheduler has staged its
        # refusal. Sequence increments survive rollback, so the retry is observable.
        deployment.sql("""
            CREATE SEQUENCE fixture_refusal_commits;
            CREATE FUNCTION fixture_fail_first_refusal() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.action='delegation' AND nextval('fixture_refusal_commits')=1 THEN
                    RAISE EXCEPTION 'fixture rollback of first delegation refusal';
                END IF;
                RETURN NEW;
            END $$;
            CREATE CONSTRAINT TRIGGER fixture_commit
              AFTER INSERT ON run_budget_entries DEFERRABLE INITIALLY DEFERRED
              FOR EACH ROW EXECUTE FUNCTION fixture_fail_first_refusal()
        """)
        root, _ = deployment.submit_review(
            "metric-fanout", provider, {"max_delegations": 1}
        )
        deployment.terminal(root)
        [refusal] = deployment.entries(root)
        assert refusal["reason"] == "delegations_exhausted"
        assert not deployment.child_ids(root)
        assert deployment.sql("SELECT last_value FROM fixture_refusal_commits") == "2"

        expired, _ = deployment.submit_review(
            "metric-deadline", provider, {"deadline_at_ms": 1}
        )
        deployment.terminal(expired)
        [deadline] = deployment.entries(expired)
        assert deadline["reason"] == "deadline_exceeded"
        return {"lost_start": original, "rollback_attempts": 1, "retained_refusals": 3}
    finally:
        worker.stop()


def samples(path):
    expected = {
        START_RESULT: {"granted": 1, "existing_source": 1, "budget_denied": 1},
        REFUSAL: {
            "calls_exhausted": 1,
            "delegations_exhausted": 1,
            "deadline_exceeded": 1,
        },
    }
    actual = {}
    instance = None
    for line in path.read_text().splitlines():
        row = json.loads(line)
        name = row.get("name")
        if name not in expected:
            continue
        assert (
            row["signal"] == "metrics" and row["kind"] == "sum" and row["unit"] == "1"
        )
        assert row["monotonic"] and row["temporality"] == 2
        resource = row["resource"]
        assert (
            row["scope"] == "ledgence"
            and resource["service.name"] == "ledgence-orchestrator"
        )
        if instance is None:
            instance = resource["service.instance.id"]
        assert instance and resource["service.instance.id"] == instance
        values = {}
        for point in row["points"]:
            attributes = point["attributes"]
            assert (
                set(attributes) == {"outcome"} and attributes["outcome"] not in values
            )
            values[attributes["outcome"]] = point["value"]
        # Periodic exports from this single orchestrator are cumulative. The
        # last export follows graceful shutdown and includes the whole scenario.
        for outcome, previous in actual.get(name, {}).items():
            assert values[outcome] >= previous
        actual[name] = values
    assert actual == expected, actual
    return actual


def verify_metrics(deployment, provider, executable):
    # Give this epoch exactly one instrumented server. Earlier scenario restarts
    # and their unfinished telemetry cannot affect its expected cumulative sums.
    stop_workers(deployment)
    deployment.server.stop()
    capture = Process(
        [str(executable), "127.0.0.1:0"],
        deployment.directory,
        "capture-budget-metrics",
        dict(os.environ),
    )
    try:

        def endpoint():
            assert capture.process.poll() is None, "local metrics capture exited"
            return next(
                (
                    line.split("=", 1)[1]
                    for line in capture.stderr_path.read_text().splitlines()
                    if line.startswith("OTLP_CAPTURE_METRICS_ENDPOINT=")
                ),
                None,
            )

        deployment.environment["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] = eventually(
            endpoint
        )
        deployment.start_server()
        detail = exercise(deployment, provider)
        deployment.server.stop()
        detail["series"] = samples(capture.stdout_path)
        return detail
    finally:
        deployment.environment.pop("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", None)
        capture.cleanup()
