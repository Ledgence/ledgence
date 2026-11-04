"""Owned PostgreSQL + HTTP/ElasticMQ deployment for global queue policies (MIT)."""

import json
import runpy
import uuid

from http_acceptance.harness import Process
from provider_operations.sqs import delivery_config, loopback_endpoint
from worker_poll_plan.fixture import PollDeployment


class CapacityDeployment(PollDeployment):
    # Queue fault fixtures use short polling; other harnesses may defer to the CLI default.
    acquire_wait_ms = 0

    def configure_broker(self, endpoint):
        endpoint = loopback_endpoint(endpoint)
        self.admin = runpy.run_path(str(self.root / "tools/check-sqs.py"))[
            "QueueAdmin"
        ](endpoint, "us-east-1", "aws")
        self.broker_name = "ledgence-cap-" + uuid.uuid4().hex
        self.broker_url = self.admin.queue_url(
            self.admin.call(
                "CreateQueue",
                {
                    "QueueName": self.broker_name,
                    "Attributes": {"VisibilityTimeout": "60"},
                },
            )
        )
        self.delivery = self.directory / "delivery.json"
        self.delivery.write_text(
            json.dumps(
                delivery_config(
                    self.scope, self.queue, "cap-external", self.broker_url, endpoint
                )
            )
        )

    def server_args(self, config):
        args = super().server_args(config)
        if hasattr(self, "delivery"):
            args.extend(["--delivery-config", str(self.delivery)])
        if getattr(self, "console_dist", None):
            args.extend(["--console-dir", str(self.console_dist)])
        return args

    def consumer(self, name, queue, concurrency):
        arguments = [
            str(self.binaries / "ledgence"),
            "worker",
            "connect",
            "--server",
            self.server_url,
            "--credentials-file",
            str(self.worker_credentials),
            "--tenant",
            self.scope["tenant_id"],
            "--namespace",
            self.scope["namespace"],
            "--queue",
            queue,
            "--concurrency",
            str(concurrency),
            "--store",
            self.artifacts.url,
            "--cache",
            str(self.directory / (name + "-cache")),
            "--python",
            self.python,
            "--runner",
            str(self.root / "sdk/python/ledgence/worker/bootstrap.py"),
        ]
        if self.acquire_wait_ms is not None:
            arguments.extend(["--acquire-wait-ms", str(self.acquire_wait_ms)])
        if hasattr(self, "delivery") and queue == self.queue:
            arguments.extend(["--delivery-config", str(self.delivery)])
        p = Process(arguments, self.directory, "worker-" + name, self.environment)
        self.processes.append(p)
        return p

    def inspect(self, queue):
        return self.checked(
            "POST", "/v1/queue-policies/inspect", {"scope": self.scope, "queue": queue}
        )

    def holds(self, queue, *, released=False):
        return self.checked(
            "POST",
            "/v1/queue-policies/holds",
            {
                "scope": self.scope,
                "queue": queue,
                "include_released": released,
                "after": None,
                "limit": 100,
            },
        )["items"]

    def change(self, queue, maximum, revision, *, timeout=None):
        command = {
            "scope": self.scope,
            "queue": queue,
            "request_id": uuid.uuid4().hex,
            "expected_revision": revision,
            "policy": {"max_concurrency": maximum, "timeout": timeout},
            "reason": "native queue qualification",
        }
        first = self.checked("POST", "/v1/queue-policies/put", command)
        replay = self.checked("POST", "/v1/queue-policies/put", command)
        assert (
            not first["already_accepted"]
            and replay["already_accepted"]
            and replay["record"] == first["record"]
        )
        return first["record"]["revision"]

    def submission(self, *args, **kwargs):
        command = super().submission(*args, **kwargs)
        command["input"]["retry_policy"]["max_attempts"] = 1
        return command

    def close(self):
        try:
            super().close()
        finally:
            if getattr(self, "broker_name", None):
                url = getattr(self, "broker_url", None)
                if url is None:
                    url = self.admin.queue_url(
                        self.admin.call("GetQueueUrl", {"QueueName": self.broker_name})
                    )
                self.admin.call("DeleteQueue", {"QueueUrl": url})
