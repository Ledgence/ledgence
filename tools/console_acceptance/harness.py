"""Real Console deployment and bounded HTTP faults for the acceptance gate."""

import http.client
import http.server
import json
import threading
import time
import urllib.parse

from http_acceptance.harness import Deployment, Process, eventually, exchange


def raw_exchange(base, method, path, body=None, headers=None):
    url = urllib.parse.urlsplit(base)
    connection = http.client.HTTPConnection(url.hostname, url.port, timeout=35)
    try:
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        payload = response.read(16 * 1024 * 1024 + 1)
        assert len(payload) <= 16 * 1024 * 1024, "oversized fixture response"
        return response.status, dict(response.getheaders()), payload
    finally:
        connection.close()


class ConsoleDeployment(Deployment):
    def __init__(self, *args, console_dist):
        super().__init__(*args)
        self.console_dist = console_dist
        self.instance = {"instance_id": "console-acceptance", "name": "Console acceptance",
                         "scope": self.scope, "suggested_queues": [self.queue]}
        self.instance_file = self.directory / "instance.json"
        self.instance_file.write_text(json.dumps(self.instance))

    def start_server(self, port=None):
        port = port or self.server_port
        self.counter += 1
        process = Process([
            str(self.binaries / "ledgence-orchestrator"), "serve", "--bind", f"127.0.0.1:{port}",
            "--store", self.artifacts.url, "--instance-config", str(self.instance_file),
            "--console-dir", str(self.console_dist),
        ], self.directory, f"server-{self.counter}", self.environment)
        self.processes.append(process)
        base = f"http://127.0.0.1:{port}"

        def ready():
            assert process.process.poll() is None, f"server exited: {process.stderr_path}"
            try:
                return exchange(base, "GET", "/health/ready", timeout=1)[0] == 200
            except (OSError, http.client.HTTPException):
                return False

        eventually(ready, description="Console orchestrator readiness")
        return process, base

    def api(self, method, path, body=None, expected=200, base=None, **query):
        if query:
            path += "?" + urllib.parse.urlencode(query)
        status, headers, result = exchange(base or self.server_url, method, "/v1/console/" + path, body)
        assert status == expected, (method, path, status, result)
        lower = {key.lower(): value for key, value in headers.items()}
        assert lower.get("request-id"), headers
        assert lower.get("cache-control") == "no-store", headers
        if status == 200:
            assert lower.get("ledgence-console-contract") == "3", headers
            assert lower.get("ledgence-instance-id") == self.instance["instance_id"], headers
        return result

    def rows(self, path, **query):
        cursor, rows, pages = None, [], 0
        while True:
            fields = dict(query)
            if cursor:
                fields["cursor"] = cursor
            page = self.api("GET", path, **fields)
            page = page.get("page", page)
            assert isinstance(page["observed_at"], int)
            rows.extend(page["items"])
            pages += 1
            assert pages <= 200, "pagination did not terminate"
            cursor = page["next_cursor"]
            if cursor is None:
                return rows

    def task_status(self, task_id):
        return self.api("GET", "tasks/status", task_id=task_id)["task"]

    def task_result(self, task_id, state="succeeded"):
        eventually(lambda: self.task_status(task_id)["state"] in ("succeeded", "failed", "cancelled"),
                   timeout=40, description=f"task {task_id} terminal")
        result = self.api("GET", "tasks/result", task_id=task_id)
        assert result["task"]["state"] == state, result
        return result

    def workflow_status(self, workflow_id):
        return self.api("GET", "workflows/status", workflow_id=workflow_id)["workflow"]

    def workflow_result(self, workflow_id, state="succeeded"):
        eventually(lambda: self.workflow_status(workflow_id)["state"] in
                   ("succeeded", "failed", "cancelled"), timeout=60,
                   description=f"workflow {workflow_id} terminal")
        result = self.api("GET", "workflows/result", workflow_id=workflow_id)
        assert result["workflow"]["state"] == state, result
        return result

    def console_submission(self, key, **options):
        command = self.submission(key, **options)
        del command["input"]["tenant_id"]
        del command["input"]["namespace"]
        command["input"]["retry_policy"] = {"max_attempts": 1, "retry_delay_ms": 0}
        return command

    def worker_detail(self, session_id):
        return self.api("GET", "workers/inspect", worker_session_id=session_id)


class ObservationProxy:
    """Only reporting can stall; execution requests still reach the real service."""

    def __init__(self, upstream):
        self.upstream = upstream
        self.block_reporting = threading.Event()
        self.failed_reports = 0
        self.lock = threading.Lock()
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def handle_request(self):
                size = int(self.headers.get("Content-Length", "0"))
                if size > 8 * 1024 * 1024:
                    self.send_error(413)
                    return
                body = self.rfile.read(size)
                try:
                    if self.path == "/v1/worker-observations" and owner.block_reporting.is_set():
                        with owner.lock:
                            owner.failed_reports += 1
                        # Exceeds the worker's report deadline without reaching
                        # persistence. Other handler threads forward normally.
                        time.sleep(2.25)
                        status, headers, response = 503, {"Content-Type": "application/json"}, b'{"code":"unavailable","message":"owned reporting outage"}'
                    else:
                        headers = {"Content-Type": "application/json", "Accept": "application/json"}
                        status, headers, response = raw_exchange(owner.upstream, self.command, self.path, body or None, headers)
                    self.send_response(status)
                    for key, value in headers.items():
                        if key.lower() not in ("content-length", "transfer-encoding", "connection"):
                            self.send_header(key, value)
                    self.send_header("Content-Length", str(len(response)))
                    self.end_headers()
                    self.wfile.write(response)
                except (OSError, http.client.HTTPException):
                    self.close_connection = True

            do_POST = handle_request
            do_GET = handle_request

            def log_message(self, *_):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
