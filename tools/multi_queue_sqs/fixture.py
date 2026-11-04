"""Owned queue routes, one scoped PostgreSQL instance and native workers (MIT)."""

import json
import os
import runpy
import subprocess
import uuid
from urllib.parse import urlsplit

from http_acceptance.harness import Process
from provider_operations.sqs import delivery_config, loopback_endpoint
from queue_capacity.fixture import CapacityDeployment
from worker_poll_plan.fixture import publish_plan
from .broker import BrokerMeter
from .invalid_routes import aliased_routes

PAY = "payments"
REPORT = "reports"
QUEUES = (PAY, REPORT)


class MultiDeployment(CapacityDeployment):
    def provision_routes(self, endpoint):
        endpoint = loopback_endpoint(endpoint)
        self.admin = runpy.run_path(str(self.root / "tools/check-sqs.py"))[
            "QueueAdmin"
        ](endpoint, "us-east-1", "aws")
        self.owned = {}
        self.meter = BrokerMeter(endpoint)
        self.routes = {}
        self.single_configs = {}
        for logical in QUEUES:
            name = "ledgence-test-multi-" + uuid.uuid4().hex
            self.owned[name] = None
            url = self.admin.queue_url(
                self.admin.call(
                    "CreateQueue",
                    {
                        "QueueName": name,
                        "Attributes": {
                            "VisibilityTimeout": "60",
                            "MessageRetentionPeriod": "3600",
                        },
                    },
                )
            )
            self.owned[name] = url
            self.meter.route(name, logical)
            observed_url = self.meter.url + urlsplit(url).path
            document = delivery_config(
                self.scope, logical, "multi-" + logical, observed_url, self.meter.url
            )
            self.routes[logical] = document
            path = self.directory / f"delivery-{logical}.json"
            path.write_text(json.dumps(document))
            self.single_configs[logical] = path
        self.delivery = self.directory / "delivery-routes.json"
        # Use reversed route order to catch accidental positional bindings.
        self.delivery.write_text(
            json.dumps({"routes": [self.routes[q] for q in reversed(QUEUES)]})
        )

    def set_plan(self, path, revision, weights):
        publish_plan(path, revision, "stride", weights)

    def worker_args(
        self, name, n, *, plan=None, queue=None, wait=1000, proxy=None, config=None
    ):
        assert (queue is None) != (plan is None)
        origin = self.server_url if proxy is None else proxy.url
        credentials = self.worker_credentials
        if proxy is not None:
            credentials = self.directory / f"{name}-credential.json"
            value = json.loads(self.worker_credentials.read_text())
            value["server"] = origin
            with os.fdopen(
                os.open(credentials, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w"
            ) as out:
                json.dump(value, out)
            self.secret_files.append(credentials)
        args = [
            str(self.binaries / "ledgence"),
            "worker",
            "connect",
            "--server",
            origin,
            "--credentials-file",
            str(credentials),
            "--tenant",
            self.scope["tenant_id"],
            "--namespace",
            self.scope["namespace"],
            "--concurrency",
            str(n),
            "--store",
            self.artifacts.url,
            "--cache",
            str(self.directory / f"{name}-cache"),
            "--python",
            self.python,
            "--runner",
            str(self.root / "sdk/python/ledgence/worker/bootstrap.py"),
            "--delivery-config",
            str(config or (self.delivery if plan else self.single_configs[queue])),
        ]
        args += ["--poll-plan", str(plan)] if plan else ["--queue", queue]
        if wait is not None:
            args += ["--acquire-wait-ms", str(wait)]
        return args

    def start_consumer(self, name, n, **kwargs):
        environment = dict(
            self.environment, RUST_LOG="info,ledgence_worker_delivery::polls=debug"
        )
        p = Process(
            self.worker_args(name, n, **kwargs),
            self.directory,
            "worker-" + name,
            environment,
        )
        self.processes.append(p)
        return p

    def mismatched_startup(self):
        plan = self.directory / "unregistered-plan.json"
        self.set_plan(plan, 1, {PAY: 1, "orphan": 1})
        before = self.sql("SELECT count(*) FROM worker_sessions")
        result = subprocess.run(
            self.worker_args("invalid", 8, plan=plan),
            env=self.environment,
            capture_output=True,
            timeout=15,
        )
        assert result.returncode != 0 and before == self.sql(
            "SELECT count(*) FROM worker_sessions"
        )
        return {"startup_rejected_before_registration": True, **aliased_routes(self)}

    def close(self):
        try:
            super().close()
        finally:
            if hasattr(self, "meter"):
                self.meter.close()
            failures = []
            for name, url in getattr(self, "owned", {}).items():
                try:
                    if url is None:
                        url = self.admin.queue_url(
                            self.admin.call("GetQueueUrl", {"QueueName": name})
                        )
                    self.admin.call("DeleteQueue", {"QueueUrl": url})
                except Exception as exc:
                    failures.append(exc)
            if failures:
                raise RuntimeError("could not remove owned local queues") from failures[
                    0
                ]
