"""Owned PostgreSQL/HTTP/worker admission acceptance deployment (MIT)."""
import importlib.util
import json
import runpy
import shutil
import uuid

from instance_access.harness import AccessDeployment
from http_acceptance.harness import Process, FaultProxy, eventually
from provider_operations.sqs import delivery_config, loopback_endpoint
from workflow_acceptance.owned_scenarios import quote

START = "/v1/admissions/start"
FINISH = "/v1/admissions/finish"


def classifier(root):
    spec = importlib.util.spec_from_file_location("admission_example_provider", root / "examples/resource-admission/provider.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.Classifier()


class AdmissionDeployment(AccessDeployment):
    def provision(self):
        super().provision()
        self.runner = self.directory / "runtime/ledgence/worker/bootstrap.py"
        shutil.copytree(self.root / "sdk/python/ledgence/worker", self.runner.parent,
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        config = json.loads(self.config.read_text())
        self.admission_config = self.directory / "admission.json"
        self.admission_config.write_text(json.dumps({"v": 1, "dispatch": "enabled", "audience": {
            key: config[key] for key in ("authority_id", "instance_id", "scope")}}))

    def server_args(self, config):
        args = super().server_args(config)
        if hasattr(self, "admission_config"):
            args += ["--admission-config", str(self.admission_config)]
        if hasattr(self, "delivery"):
            args += ["--delivery-config", str(self.delivery)]
        return args

    def dispatch_policy(self, value):
        data = json.loads(self.admission_config.read_text())
        data["dispatch"] = value
        self.admission_config.write_text(json.dumps(data))

    def configure_delivery(self, endpoint):
        endpoint = loopback_endpoint(endpoint)
        admin_type = runpy.run_path(str(self.root / "tools/check-sqs.py"))["QueueAdmin"]
        self.queue_admin = admin_type(endpoint, "us-east-1", "aws")
        self.queue_name = "ledgence-admission-" + uuid.uuid4().hex
        self.queue_url = None
        response = self.queue_admin.call("CreateQueue", {"QueueName": self.queue_name,
            "Attributes": {"VisibilityTimeout": "60", "MessageRetentionPeriod": "3600"}})
        self.queue_url = self.queue_admin.queue_url(response)
        self.delivery = self.directory / "delivery.json"
        self.delivery.write_text(json.dumps(delivery_config(self.scope, self.queue, "admission-sqs", self.queue_url, endpoint)))

    def authenticated_proxy(self):
        proxy = FaultProxy(self.server_url, forward_authorization=True)
        self.proxies.append(proxy)
        return proxy

    def worker(self, proxy=None):
        credentials = self.worker_credentials
        self.counter += 1
        if proxy is not None:
            value = json.loads(credentials.read_text())
            value["server"] = proxy.url
            credentials = self.directory / f"worker-proxy-{self.counter}.json"
            credentials.write_text(json.dumps(value)); credentials.chmod(0o600)
            self.secret_files.append(credentials)
        args = [str(self.binaries / "ledgence"), "worker", "connect", "--server", proxy.url if proxy else self.server_url,
                "--credentials-file", str(credentials), "--tenant", self.scope["tenant_id"], "--namespace", self.scope["namespace"],
                "--queue", self.queue, "--store", self.artifacts.url, "--cache", str(self.directory / f"worker-cache-{self.counter}"),
                "--python", self.python, "--runner", str(self.runner), "--concurrency", "1"]
        if hasattr(self, "delivery"):
            args += ["--delivery-config", str(self.delivery), "--acquire-wait-ms", "1000"]
        process = Process(args, self.directory, f"worker-{self.counter}", self.environment)
        self.processes.append(process)
        return process

    def policy(self, *, enabled=True, capacity=1, rate=None, offer_ms=1000):
        query = {"scope": self.scope, "name": "support-model"}
        status, _, old = self.request("POST", "/v1/resources/query", query, credentials=self.operator)
        assert status in (200, 404)
        command = {**query, "expected_revision": old["resource"]["revision"] if status == 200 else None,
                   "request_id": "policy-" + uuid.uuid4().hex, "reason": "local acceptance scenario",
                   "policy": {"enabled": enabled, "max_occupancy": capacity, "rate": rate,
                              "offer_timeout_ms": offer_ms, "start_timeout_ms": 1000}}
        return self.checked("POST", "/v1/resources/put", command)

    def resource(self):
        return self.checked("POST", "/v1/resources/query", {"scope": self.scope, "name": "support-model"})

    def publish_example(self):
        self.publish("resource-admission", "1.0.0", program_source=(self.root / "examples/resource-admission/program.py").read_text(), runtime_protocol=3)

    def begin(self, text, provider, *, attempts=3):
        command = self.submission(text, program="resource-admission", endpoint=provider.url, text=text)
        command["input"]["retry_policy"]["max_attempts"] = attempts
        command["input"]["attempt_timeout_ms"] = 60_000
        return self.checked("POST", "/v1/workflows", command)["workflow_id"]

    def status(self, workflow):
        return self.checked("GET", self.query("/v1/workflows/status", workflow_id=workflow))

    def admission(self, workflow):
        value = self.sql("SELECT convert_from(snapshot_bytes,'UTF8') FROM admission_requests WHERE workflow_id=" + quote(workflow))
        return json.loads(value) if value else None

    def settled(self, workflow):
        def terminal():
            value = self.status(workflow)
            assert value["state"] not in ("failed", "cancelled"), "admitted workflow failed before its expected result"
            return value if value["state"] == "succeeded" else None
        status = eventually(terminal, timeout=70, description="admitted workflow completes")
        result = self.checked("GET", self.query("/v1/workflows/result", workflow_id=workflow))
        text = self.admission(workflow)["binding"]["operation"]["arguments"]["text"]
        expected = {"category": "billing" if "invoice" in text.lower() else "general"}
        assert result["outcome"] == {"kind": "succeeded", "output": expected}
        assert self.admission(workflow)["finish"]["output"] == expected
        return status

    def release(self, workflow):
        value = self.admission(workflow)
        command = {"occurrence": value["occurrence"], "source_id": value["source"]["source_id"],
                   "expected_revision": value["revision"], "request_id": "release-" + uuid.uuid4().hex,
                   "reason": "synthetic provider verified; explicitly release uncertain capacity"}
        first = self.checked("POST", "/v1/admissions/release", command)
        replay = self.checked("POST", "/v1/admissions/release", command)
        assert replay["already_accepted"] and replay["revision"] == first["revision"]
        return command

    def delivery_census(self):
        rows = json.loads(self.sql("SELECT coalesce(json_agg(json_build_object('task_id',t.task_id,'destination',t.dispatch_destination,'attempts',t.attempt_count,'claims',(SELECT count(*) FROM dispatch_claim_receipts c WHERE c.task_id=t.task_id))),'[]'::json) FROM tasks t"))
        assert rows and any(row["attempts"] > 0 for row in rows)
        for row in rows:
            assert row["destination"] == ("admission-sqs" if hasattr(self, "delivery") else None)
            if row["attempts"] > 0 and hasattr(self, "delivery"):
                assert row["claims"] > 0, "executed external task lacks a durable broker claim"
        return rows

    def close(self):
        try:
            super().close()
        finally:
            if hasattr(self, "queue_admin"):
                if self.queue_url is None:
                    self.queue_url = self.queue_admin.queue_url(self.queue_admin.call("GetQueueUrl", {"QueueName": self.queue_name}))
                self.queue_admin.call("DeleteQueue", {"QueueUrl": self.queue_url})
