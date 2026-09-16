"""Shared explicit SQS composition for acceptance and performance harnesses."""

import http.client
import json
from pathlib import Path

from .harness import Deployment, Process, eventually, exchange


class SqsDeployment(Deployment):
    def __init__(self, *args, queue_url=None, endpoint=None, region=None, delivery_config=None, **kwargs):
        provided = None
        if delivery_config is not None:
            path = Path(delivery_config).resolve()
            with path.open("rb") as source:
                body = source.read(16 * 1024 + 1)
            assert len(body) <= 16 * 1024, "delivery config exceeds 16 KiB"
            config = json.loads(body)
            scope, queue = config["route"]["scope"], config["route"]["queue"]
            assert isinstance(scope, dict) and isinstance(queue, str), "invalid delivery route"
            assert all(isinstance(scope[key], str) and scope[key] for key in ("tenant_id", "namespace")), "invalid delivery scope"
            assert queue, "empty delivery queue"
            provided = path, scope, queue
        # Parse the optional file before creating any artifact server or process.
        super().__init__(*args, **kwargs)
        self.terminal_timeout_floor = 95
        if provided is not None:
            self.delivery_config, self.scope, self.queue = provided
            return
        self.delivery_config = self.directory / "delivery.json"
        self.delivery_config.write_text(json.dumps({
            "route": {"scope": self.scope, "queue": self.queue, "destination": "acceptance-sqs"},
            "sqs": {"region": region, "queue_url": queue_url, "endpoint_url": endpoint,
                    "local_credentials": endpoint is not None, "operation_timeout_ms": 5000,
                    "visibility_timeout_seconds": 60},
        }, indent=2) + "\n")

    def start_server(self, port=None):
        port = port or self.server_port
        self.counter += 1
        process = Process([
            str(self.binaries / "ledgence-orchestrator"), "serve", "--bind", f"127.0.0.1:{port}",
            "--store", self.artifacts.url, "--delivery-config", str(self.delivery_config),
        ], self.directory, f"server-{self.counter}", self.environment)
        self.processes.append(process)
        base = f"http://127.0.0.1:{port}"

        def ready():
            assert process.process.poll() is None, f"SQS server exited: {process.stderr_path}"
            try:
                return exchange(base, "GET", "/health/ready", timeout=1)[0] == 200
            except (OSError, http.client.HTTPException):
                return False

        eventually(ready, description="SQS orchestrator readiness")
        return process, base

    def terminal(self, task_id, state="succeeded", timeout=35):
        # A canceled ReceiveMessage may still make a record invisible remotely.
        # This gate uses 60s visibility, unlike the integrated HTTP queue; allow
        # expiry/repair and a subsequent long poll rather than assume cancellation
        # proves the broker did not receive a later publication.
        return super().terminal(task_id, state, timeout=max(timeout, self.terminal_timeout_floor))

    def start_worker(self, server=None, concurrency=1, cache="cache"):
        self.counter += 1
        args = [
            str(self.binaries / "ledgence-worker"), "connect", "--server", server or self.server_url,
            "--tenant", self.scope["tenant_id"], "--namespace", self.scope["namespace"],
            "--queue", self.queue, "--store", self.artifacts.url,
            "--cache", str(self.directory / cache), "--python", self.python,
            "--runner", str(self.root / "sdk/python/ledgence/worker/bootstrap.py"),
            "--concurrency", str(concurrency), "--delivery-config", str(self.delivery_config),
        ]
        process = Process(args, self.directory, f"worker-{self.counter}", self.environment)
        self.processes.append(process)
        return process
