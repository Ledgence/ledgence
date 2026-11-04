"""Run the genuine authenticated worker and capture queue-bound acquisitions. MIT."""
import json
import os

from http_acceptance.harness import FaultProxy, Process, eventually
from instance_access.harness import AccessDeployment
from resource_admission.common import ResourceControls


class PollDeployment(ResourceControls, AccessDeployment):
    def enable_admission(self):
        config = json.loads(self.config.read_text())
        self.admission_config = self.directory / "worker-poll-admission.json"
        self.admission_config.write_text(json.dumps({"v": 1, "dispatch": "enabled", "audience":
            {key: config[key] for key in ("authority_id", "instance_id", "scope")}}))

    def server_args(self, config):
        args = super().server_args(config)
        if hasattr(self, "admission_config"):
            args.extend(["--admission-config", str(self.admission_config)])
        return args

    def worker(self, name, plan, concurrency, proxy):
        self.counter += 1
        credentials = self.directory / f"{name}-worker.json"
        bound = json.loads(self.worker_credentials.read_text())
        bound["server"] = proxy.url
        with os.fdopen(os.open(credentials, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as output:
            json.dump(bound, output)
        self.secret_files.append(credentials)
        args = [
            str(self.binaries / "ledgence"), "worker", "connect", "--server", proxy.url,
            "--credentials-file", str(credentials), "--tenant", self.scope["tenant_id"],
            "--namespace", self.scope["namespace"], "--poll-plan", str(plan),
            "--store", self.artifacts.url, "--cache", str(self.directory / f"{name}-cache"),
            "--python", self.python, "--runner", str(self.root / "sdk/python/ledgence/worker/bootstrap.py"),
            "--concurrency", str(concurrency), "--acquire-wait-ms", "0",
        ]
        proc = Process(args, self.directory, f"worker-{name}", self.environment)
        self.processes.append(proc)
        return proc

    def observing_proxy(self):
        proxy = FaultProxy(self.server_url, forward_authorization=True)
        self.proxies.append(proxy)
        return proxy

    def submit_to(self, queue, key, *, duration_ms=0, gate=None, resource=None):
        command = self.submission(key, program="poll-fixture")
        if gate is not None:
            self.gates.add(gate)
        command["input"].update(queue=queue, data={
            "queue": queue,
            "duration_ms": duration_ms,
            "gate": str(gate) if gate else None,
            "marker": str(self.marker),
        })
        if resource is not None:
            command["input"]["admission"] = {"resource": resource}
        return self.checked("POST", "/v1/tasks", command)["task_id"]

    def output_for(self, task, queue, duration_ms=0):
        record = self.task(task)
        assert record["state"] == "succeeded" and record["attempt_count"] == 1, (task, record["state"])
        result = self.checked("GET", self.query("/v1/tasks/result", task_id=task))
        assert result["outcome"]["kind"] == "succeeded"
        output = result["outcome"]["output"]
        assert output["queue"] == queue and output["task"] == task and output["pid"] > 0, output
        assert output["finished_ns"] - output["started_ns"] >= duration_ms * 1_000_000, output
        return output


def publish_plan(path, revision, strategy, queues):
    document = {"schema_version": 1, "revision": revision, "strategy": strategy,
                "queues": [{"queue": queue, "weight": weight} for queue, weight in queues.items()]}
    temporary = path.with_suffix(".next")
    temporary.write_text(json.dumps(document))
    temporary.replace(path)


def wait_worker(proc, predicate, description, timeout=30):
    def observed():
        assert proc.process.poll() is None, f"worker exited: inspect {proc.stderr_path.name}"
        return predicate()
    return eventually(observed, timeout=timeout, description=description)


def plan_log(proc, message, *, revision=None, after=0):
    rows = []
    for line in proc.stderr_path.read_bytes().splitlines(keepends=True):
        # read only complete structured log lines
        if line.endswith(b"\n"):
            try:
                row = json.loads(line)
            except ValueError:
                continue
            fields = row.get("fields", {})
            if fields.get("message") == message and (revision is None or fields.get("revision") == revision):
                rows.append(row)
    return len(rows) > after


def acquired(proxy):
    return proxy.commands("/v1/acquisitions")


def identity(record):
    body = record["json"]
    return body["worker_session_id"], body["consumer_id"], body["sequence"]


def trace(d, proxy, channels, concurrency, phase):
    sessions = {}
    for record in proxy.commands("/v1/worker-sessions") + proxy.commands("/v1/task-admissions/sessions"):
        assert record["status"] == 200
        body, response = record["json"], json.loads(record["response"])
        assert body["scope"] == d.scope and body["queue"] in channels
        assert body["concurrency"] == concurrency
        session_id = response["id"]
        assert session_id not in sessions
        sessions[session_id] = body["queue"]
    assert sorted(sessions.values()) == sorted(channels), sessions
    rows, seen, previous_seq = [], {}, {}
    for record in acquired(proxy):
        body, response = record["json"], json.loads(record["response"])
        session, consumer, sequence = key = identity(record)
        assert body["scope"] == d.scope and sessions[session] == body["queue"]
        assert 0 <= consumer < concurrency and record["status"] == 200
        assert response["sequence"] == sequence
        result = response["disposition"]
        assert result in ("assigned", "empty"), result
        owner = response.get("assignment", {}).get("lease", {}).get("owner", {})
        task, attempt = owner.get("task_id"), owner.get("attempt_id")
        if result == "assigned":
            assert task and attempt
            assert owner["worker_session_id"] == session and owner["consumer_id"] == consumer
        binding = (result, task, attempt, body["queue"])
        replay = key in seen
        if replay:
            assert seen[key] == binding, "acquisition replay changed route, outcome or task/attempt"
        else:
            lane = session, consumer
            assert sequence == previous_seq.get(lane, 0) + 1, "queue-bound durable sequence gap"
            previous_seq[lane] = sequence
            seen[key] = binding
        rows.append({"session": session, "consumer": consumer, "sequence": sequence,
                     "request_id": record["request_id"], "queue": body["queue"], "result": result,
                     "task": task, "attempt": attempt, "replay": replay})
    assert all(isinstance(r["request_id"], str) and r["request_id"] for r in rows)
    assert len({r["request_id"] for r in rows}) == len(rows)
    (d.directory / f"{phase}-trace.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    return rows
