"""Verify actual OTLP bytes and durable identities across separate platform processes.

Requires the same disposable PostgreSQL server and psql wrapper as check-http.py.
The capture fixture decodes real OTLP protobuf; it is not a production collector.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import urllib.parse
import uuid

from http_acceptance.harness import Deployment, Process, eventually
from http_acceptance.scenarios import report


def carrier(value):
    if value is None:
        return None
    parent = value["traceparent"] if isinstance(value, dict) else value
    _, trace_id, span_id, flags = parent.split("-")[:4]
    return trace_id, span_id, flags


def records(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def spans_for(rows, name, task):
    return [span for span in rows if span["name"] == name and
            span["attributes"].get("ledgence.task.id") == task]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    parent_url = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not parent_url:
        parser.error("LEDGENCE_POSTGRES_URL must name a disposable PostgreSQL server")
    root = Path(__file__).resolve().parents[1]
    directory = args.evidence.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    binaries = args.binaries.resolve()
    database = "ledgence_otel_" + uuid.uuid4().hex
    database_url = urllib.parse.urlunsplit(urllib.parse.urlsplit(parent_url)._replace(path="/" + database))
    def admin(sql):
        result = subprocess.run([args.psql, "--dbname", parent_url, "-X", "--set", "ON_ERROR_STOP=1", "--command", sql], capture_output=True, timeout=40)
        assert result.returncode == 0, "owned database administration failed"
    capture = Process([str(args.capture.resolve()), "127.0.0.1:0"], directory, "capture", dict(os.environ))
    d = None
    created = False
    observations = []
    try:
        line = eventually(lambda: next((line for line in capture.stderr_path.read_text().splitlines()
                                        if line.startswith("OTLP_CAPTURE_ENDPOINT=")), None), description="capture receiver")
        endpoint = line.split("=", 1)[1]
        admin(f'CREATE DATABASE "{database}"')
        created = True
        d = Deployment(root, directory, binaries, os.environ.get("LEDGENCE_PYTHON", sys.executable), database_url, args.psql)
        d.environment["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = endpoint
        d.command("ledgence-orchestrator", ["migrate"])
        for orchestrator_on, worker_on in ((True, True), (False, True), (True, False), (False, False)):
            d.environment["OTEL_SDK_DISABLED"] = str(not orchestrator_on).lower()
            d.server, _ = d.start_server()
            d.environment["OTEL_SDK_DISABLED"] = str(not worker_on).lower()
            worker = d.start_worker()
            command = d.submission(f"matrix-{orchestrator_on}-{worker_on}")
            task = d.submit(command)
            _, attempt = d.terminal(task["task_id"])
            worker.stop()
            d.server.stop()
            origin = carrier(command["origin_trace"])
            creation = carrier(attempt["event"].get("traceparent"))
            processing = carrier(attempt["settlement"]["command"].get("processing_trace"))
            assert (creation != origin) == orchestrator_on
            assert (processing is not None) == worker_on
            assert report(attempt)["outcome"]["output"]["event"] == attempt["event"]
            rows = records(capture.stdout_path)
            producers = spans_for(rows, "ledgence.invocation.create", task["task_id"])
            workers = spans_for(rows, "ledgence.attempt.process", task["task_id"])
            assert len(producers) == int(orchestrator_on), producers
            assert len(workers) == int(worker_on), workers
            if producers:
                assert producers[0]["parent_span_id"] == origin[1]
                assert (producers[0]["trace_id"], producers[0]["span_id"]) == creation[:2]
                links = producers[0]["links"]
                assert len(links) == 1, links
                linked = [row for row in rows if row["trace_id"] == links[0]["trace_id"] and row["span_id"] == links[0]["span_id"]]
                assert len(linked) == 1 and linked[0]["kind"] == 2, linked
                assert "acquisitions" in linked[0]["name"]
                if worker_on:
                    clients = [row for row in rows if row["span_id"] == linked[0]["parent_span_id"] and row["trace_id"] == linked[0]["trace_id"]]
                    assert len(clients) == 1 and clients[0]["kind"] == 3, clients
            if workers:
                assert workers[0]["parent_span_id"] == creation[1]
                assert (workers[0]["trace_id"], workers[0]["span_id"]) == processing[:2]
                executions = [span for span in rows if span["name"] == "ledgence.program.execute"
                              and span["parent_span_id"] == processing[1]]
                assert len(executions) == 1, executions
                for span in [workers[0], executions[0]]:
                    assert type(span["attributes"]["ledgence.duration_ms"]) is int, span
                assert type(workers[0]["attributes"]["ledgence.attempt.number"]) is int
                assert type(executions[0]["attributes"]["process.pid"]) is int
                logs = [row for row in records(worker.stderr_path) if row.get("fields", {}).get("text") == "accepted invocation"]
                assert any(row["fields"].get("span_id") == executions[0]["span_id"] for row in logs), logs
            observations.append({"matrix": [orchestrator_on, worker_on], "task_id": task["task_id"], "result": "passed"})
        d.environment["OTEL_SDK_DISABLED"] = "false"
        d.server, _ = d.start_server()
        proxy = d.proxy()
        lost_acquire = proxy.lose_once("/v1/acquisitions")
        lost_settle = proxy.lose_once("/v1/settlements")
        accepted = d.submit(d.submission("replayed-trace"))
        worker = d.start_worker(proxy.url)
        d.terminal(accepted["task_id"])
        assert lost_acquire.is_set() and lost_settle.is_set()
        # Reuse the same pool after success, failure and a flood of optional logs.
        cases = []
        for label, origin, flags, mode, extra in (
            ("retry", True, "01", "interrupt_once", {}),
            ("root-retry", False, "01", "interrupt_once", {}),
            ("unsampled", True, "00", "success", {}),
            ("flood", True, "01", "success", {"log_count": 10000}),
        ):
            command = d.submission(label, mode, **extra)
            if not origin: command.pop("origin_trace")
            else: command["origin_trace"]["traceparent"] = command["origin_trace"]["traceparent"][:-2] + flags
            task = d.submit(command)
            _, attempt = d.terminal(task["task_id"])
            cases.append((label, command, task, attempt))
        worker.stop()
        d.server.stop()
        rows = records(capture.stdout_path)
        assert len(spans_for(rows, "ledgence.invocation.create", accepted["task_id"])) == 1
        processing = spans_for(rows, "ledgence.attempt.process", accepted["task_id"])
        assert len(processing) == 1
        assert len([row for row in rows if row["name"] == "ledgence.program.execute" and row["parent_span_id"] == processing[0]["span_id"]]) == 1
        assert len(d.invocations(accepted["task_id"])) == 1
        for path in ("/v1/acquisitions", "/v1/settlements"):
            exchanges = proxy.commands(path)
            same = [row for row in exchanges if row["body"] == exchanges[0]["body"]]
            assert len(same) >= 2
            assert len({row["traceparent"] for row in same}) == len(same), "each retry needs a new HTTP span"
            if path.endswith("acquisitions"):
                responses = [json.loads(row["response"]) for row in same]
                assert all(row.get("disposition") == "assigned" for row in responses), responses
                events = [row["assignment"]["event"] for row in responses]
                assert all(event == events[0] for event in events), "replayed event must remain immutable"
        for label, command, task, attempt in cases:
            producers = spans_for(rows, "ledgence.invocation.create", task["task_id"])
            expected = 0 if label == "unsampled" else (2 if "retry" in label else 1)
            assert len(producers) == expected, (label, producers)
            if label == "root-retry":
                assert len({span["trace_id"] for span in producers}) == 2
                assert all(not span["parent_span_id"] for span in producers)
            elif label == "retry":
                assert all(span["parent_span_id"] == carrier(command["origin_trace"])[1] for span in producers)
                assert len({span["span_id"] for span in producers}) == 2
                processing_spans = spans_for(rows, "ledgence.attempt.process", task["task_id"])
                assert len(processing_spans) == 2
                cleanup = [span for span in rows if span["name"] == "ledgence.attempt.cleanup" and span["parent_span_id"] in {worker["span_id"] for worker in processing_spans}]
                assert cleanup, "failed execution cleanup must parent to W"
            elif label == "unsampled":
                assert carrier(attempt["event"]["traceparent"])[2] == "00"
                assert carrier(attempt["settlement"]["command"]["processing_trace"])[2] == "00"
                assert not spans_for(rows, "ledgence.attempt.process", task["task_id"])
            observations.append({"case": label, "task_id": task["task_id"], "result": "passed"})
        # Endpoint failure leaves enabled instrumentation and task outcomes intact.
        capture.kill()
        d.environment["RUST_LOG"] = "error"
        d.server, _ = d.start_server()
        worker = d.start_worker()
        task = d.submit(d.submission("collector-outage"))
        _, attempt = d.terminal(task["task_id"])
        assert attempt["settlement"]["command"]["processing_trace"] is not None
        assert carrier(attempt["event"]["traceparent"]) != carrier(task["origin_trace"])
        worker.stop()
        d.server.stop()
        observations.append({"case": "collector-outage-with-error-only-logs", "task_id": task["task_id"], "result": "passed"})
        (directory / "results.json").write_text(json.dumps(observations, indent=2) + "\n")
        print(f"Observability acceptance passed: {len(observations)} checks; {len(rows)} decoded spans")
    finally:
        if d: d.close()
        capture.cleanup()
        if created: admin(f'DROP DATABASE "{database}" WITH (FORCE)')


if __name__ == "__main__":
    main()
