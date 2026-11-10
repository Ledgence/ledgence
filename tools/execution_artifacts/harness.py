"""Real filesystem blob store, PostgreSQL, scoped HTTP and isolated worker processes (MIT)."""
import json
import shutil
import subprocess
from pathlib import Path

from http_acceptance.harness import Process, eventually
from instance_access.harness import AccessDeployment


class ArtifactDeployment(AccessDeployment):
    def provision(self):
        super().provision()
        self.runner = self.directory / "runtime/ledgence/worker/bootstrap.py"
        shutil.copytree(self.root / "sdk/python/ledgence/worker", self.runner.parent,
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        self.objects = self.directory / "objects"
        self.spool = self.directory / "spool"
        self.objects.mkdir(); self.spool.mkdir()
        self.artifacts_config = self.directory / "artifacts.json"
        self.artifacts_config.write_text(json.dumps({"schema": "ledgence.artifact-store.v1", "store_id": "acceptance",
            "backend": {"kind": "filesystem", "directory": str(self.objects)}, "spool_directory": str(self.spool)}))

    def configure_s3(self, endpoint, bucket, fixture):
        config = json.loads(self.artifacts_config.read_text())
        config["backend"] = {"kind": "s3", "bucket": bucket, "prefix": "execution", "region": "us-east-1",
                             "endpoint_url": endpoint, "force_path_style": True, "local_credentials": True}
        self.artifacts_config.write_text(json.dumps(config))
        self.s3_fixture = fixture
        self.environment = {key: value for key, value in self.environment.items() if not key.startswith("AWS_")}
        self.environment.update(AWS_EC2_METADATA_DISABLED="true", AWS_MAX_ATTEMPTS="7")

    def server_args(self, config):
        return super().server_args(config) + ["--artifact-config", str(self.artifacts_config)]

    def worker(self, queue, *, server=None, credentials=None):
        self.counter += 1
        process = Process([str(self.binaries / "ledgence"), "worker", "connect", "--server", server or self.server_url,
            "--credentials-file", str(credentials or self.worker_credentials), "--tenant", self.scope["tenant_id"],
            "--namespace", self.scope["namespace"], "--queue", queue, "--store", self.artifacts.url,
            "--cache", str(self.directory / f"worker-cache-{self.counter}"), "--python", self.python,
            "--runner", str(self.runner), "--concurrency", "1"], self.directory, f"worker-{self.counter}", self.environment)
        self.processes.append(process)
        return process

    def publish_example(self):
        for suffix in ("producer", "consumer", "workflow"):
            source = (self.root / f"examples/execution-artifacts/{suffix}.py").read_text()
            if suffix == "producer":
                # Exercise replay after durable publication but before the task
                # result is delivered. The public example stays free of faults.
                source += '''\nimport os, json\n_original_handle = handle\nasync def handle(event):\n    result = await _original_handle(event)\n    with open(''' + repr(str(self.directory / "producer-attempts.jsonl")) + ''', 'a') as stream:\n        stream.write(json.dumps({"task":event["ldgtaskid"], "attempt":event["ldgattemptno"], "artifact":result["artifact"]})+'\\n')\n    if event["ldgattemptno"] == 1:\n        os._exit(19)\n    return result\n'''
            self.publish("artifact-" + suffix, "1.0.0", program_source=source, runtime_protocol=3)

    def begin(self, size, *, producer_queue="artifact-producer", consumer_queue="artifact-consumer"):
        command = self.submission(f"artifact-{size}", program="artifact-workflow", size=size,
                                  producer_queue=producer_queue, consumer_queue=consumer_queue)
        command["input"]["queue"] = producer_queue
        return self.checked("POST", "/v1/workflows", command)["workflow_id"]

    def settled(self, workflow):
        def terminal():
            value = self.checked("GET", self.query("/v1/workflows/status", workflow_id=workflow))
            return value if value["state"] in ("succeeded", "failed", "cancelled") else None
        status = eventually(terminal, timeout=90, description="artifact workflow terminal outcome")
        assert status["state"] == "succeeded", "artifact workflow did not succeed; inspect retained task/worker logs"
        return self.checked("GET", self.query("/v1/workflows/result", workflow_id=workflow))["outcome"]["output"]

    def history(self, task_id, after=0):
        return self.checked("GET", self.query("/v1/tasks/history", task_id=task_id, after_sequence=after))

    def attempt(self, task_id, attempt_id=None):
        if attempt_id is None:
            claims, after = [], 0
            while page := self.history(task_id, after):
                claims.extend(row["event"]["attempt_id"] for row in page if row["event"]["reason"] == "claimed")
                after = page[-1]["sequence"]
            assert claims, "task has no retained attempt"
            attempt_id = claims[-1]
        return self.checked("GET", self.query("/v1/attempts/inspect", task_id=task_id, attempt_id=attempt_id), credentials=self.worker_credentials)

    def artifact_command(self, command, options, *, credentials=None):
        return self.command("ledgence", ["artifacts", command, "--server", self.server_url,
            "--credentials-file", str(credentials or self.operator), "--tenant", self.scope["tenant_id"],
            "--namespace", self.scope["namespace"], *options])

    def installed(self, script, *args):
        self.counter += 1
        path = self.directory / f"installed-client-{self.counter}.py"
        path.write_text(script)
        result = subprocess.run([self.python_client, "-I", str(path), self.server_url, str(self.operator),
                                 self.scope["tenant_id"], self.scope["namespace"], *map(str,args)],
                                env=self.environment, capture_output=True, timeout=150)
        (self.directory / f"installed-client-{self.counter}.stdout").write_bytes(result.stdout)
        (self.directory / f"installed-client-{self.counter}.stderr").write_bytes(result.stderr)
        assert result.returncode == 0, "installed artifact client scenario failed"
        return json.loads(result.stdout)


class LostUploadAck:
    """Forward one complete upload, then lose its ACK; never record credentials or bytes."""
    def __init__(self, upstream):
        import http.client
        import http.server
        import threading
        from urllib.parse import urlsplit
        origin = urlsplit(upstream)
        self.puts = 0
        self.dropped = threading.Event()
        owner = self
        lock = threading.Lock()
        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            def handle_request(self):
                body = bytearray()
                if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                    while True:
                        line = self.rfile.readline(100)
                        size = int(line.split(b";", 1)[0], 16)
                        if size == 0:
                            assert self.rfile.readline(100) == b"\r\n"
                            break
                        assert len(body) + size <= 4 * 1024 * 1024
                        chunk = self.rfile.read(size)
                        assert len(chunk) == size and self.rfile.read(2) == b"\r\n"
                        body.extend(chunk)
                else:
                    size = int(self.headers.get("Content-Length", "0"))
                    assert 0 <= size <= 4 * 1024 * 1024
                    body.extend(self.rfile.read(size))
                connection = http.client.HTTPConnection(origin.hostname, origin.port, timeout=35)
                try:
                    headers = {key: value for key, value in self.headers.items()
                               if key.lower() not in {"host", "content-length", "transfer-encoding", "connection"}}
                    headers["Content-Length"] = str(len(body))
                    connection.request(self.command, self.path, body, headers)
                    response = connection.getresponse()
                    payload = response.read(4 * 1024 * 1024 + 1)
                    assert len(payload) <= 4 * 1024 * 1024
                    with lock:
                        upload = self.command == "PUT" and self.path.endswith("/content")
                        if upload:
                            owner.puts += 1
                        drop = upload and not owner.dropped.is_set() and response.status == 200
                        if drop:
                            owner.dropped.set()
                    if drop:
                        self.close_connection = True
                        return
                    self.send_response(response.status)
                    for key, value in response.getheaders():
                        if key.lower() not in {"content-length", "transfer-encoding", "connection"}:
                            self.send_header(key, value)
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except (OSError, http.client.HTTPException):
                    self.close_connection = True
                finally:
                    connection.close()
            do_POST = do_GET = do_PUT = handle_request
            def log_message(self, *_):
                pass
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(timeout=5)
