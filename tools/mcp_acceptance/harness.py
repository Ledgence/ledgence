"""Owned MCP subprocesses and a bounded held HTTP reply for acceptance (MIT)."""
import http.client
import http.server
import json
import queue
import signal
import subprocess
import threading
import time
import urllib.parse

from http_acceptance.harness import Deployment, Process, eventually, exchange


class McpDeployment(Deployment):
    def start_server(self):
        instance = self.directory / "instance.json"
        instance.write_text(json.dumps({"instance_id": "mcp-acceptance", "name": "MCP acceptance",
                                        "scope": self.scope, "suggested_queues": [self.queue]}))
        self.counter += 1
        process = Process([str(self.binaries / "ledgence"), "orchestrator", "serve", "--bind",
                           f"127.0.0.1:{self.server_port}", "--store", self.artifacts.url,
                           "--instance-config", str(instance)], self.directory,
                          f"server-{self.counter}", self.environment)
        self.processes.append(process)
        def ready():
            assert process.process.poll() is None, f"server exited: {process.stderr_path}"
            try:
                return exchange(self.server_url, "GET", "/health/ready", timeout=1)[0] == 200
            except (OSError, http.client.HTTPException):
                return False
        eventually(ready, description="scope-bound orchestrator readiness")
        return process, self.server_url


class McpSession:
    """Minimal protocol client; stdout must contain only complete JSON-RPC lines."""
    def __init__(self, d, label, *, server=None, scope=None, read_only=False, initialize=True):
        scope = scope or d.scope
        args = [str(d.binaries / "ledgence"), "mcp", "serve", "--server", server or d.server_url,
                "--tenant", scope["tenant_id"], "--namespace", scope["namespace"]]
        if read_only:
            args.append("--read-only")
        self.stdout_path = d.directory / (label + ".stdout")
        self.stderr_path = d.directory / (label + ".stderr")
        self.stderr = self.stderr_path.open("wb")
        self.process = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.stderr, env=d.environment, start_new_session=True)
        self.responses = queue.Queue()
        self.pending = {}
        self.next_id = 1
        self.notifications = []
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        if initialize:
            try:
                reply = self.request("initialize", {"protocolVersion": "2025-11-25", "capabilities": {},
                    "clientInfo": {"name": "ledgence-mcp-acceptance", "version": "1"}})
                assert reply["result"]["protocolVersion"] == "2025-11-25", reply
                assert reply["result"]["serverInfo"]["name"] == "ledgence", reply
                assert "tools" in reply["result"]["capabilities"], reply
                self.notify("notifications/initialized", {})
            except BaseException:
                self.cleanup()
                raise

    def _read(self):
        try:
            with self.stdout_path.open("wb") as output:
                while line := self.process.stdout.readline(8 * 1024 * 1024 + 1):
                    assert len(line) <= 8 * 1024 * 1024 and line.endswith(b"\n"), "invalid MCP frame size/newline"
                    output.write(line)
                    output.flush()
                    value = json.loads(line)
                    assert type(value) is dict and value.get("jsonrpc") == "2.0", "non-protocol stdout"
                    self.responses.put(value)
        except BaseException as error:
            self.responses.put(error)
        finally:
            self.responses.put(None)

    def send(self, value):
        self.process.stdin.write(json.dumps(value, separators=(",", ":")).encode() + b"\n")
        self.process.stdin.flush()

    def begin(self, method, params):
        identity = self.next_id
        self.next_id += 1
        self.send({"jsonrpc": "2.0", "id": identity, "method": method, "params": params})
        return identity

    def notify(self, method, params):
        self.send({"jsonrpc": "2.0", "method": method, "params": params})

    def receive(self, identity, timeout=40):
        deadline = time.monotonic() + timeout
        while identity not in self.pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(f"MCP response timed out; inspect {self.stderr_path}")
            try:
                value = self.responses.get(timeout=remaining)
            except queue.Empty:
                raise AssertionError(f"MCP response timed out; inspect {self.stderr_path}") from None
            if value is None:
                raise AssertionError(f"MCP stream ended before response; inspect {self.stderr_path}")
            if isinstance(value, BaseException):
                raise value
            if "id" not in value:
                self.notifications.append(value)
            else:
                assert value["id"] not in self.pending, "duplicate MCP response"
                self.pending[value["id"]] = value
        return self.pending.pop(identity)

    def request(self, method, params):
        return self.receive(self.begin(method, params))

    def tool(self, name, arguments, *, error=None):
        response = self.request("tools/call", {"name": name, "arguments": arguments})
        assert "error" not in response, response
        result = response["result"]
        value = result["structuredContent"]
        texts = [json.loads(item["text"]) for item in result["content"] if item["type"] == "text"]
        assert texts == [value], result
        assert result.get("isError", False) is (error is not None), result
        if error is not None:
            assert value["code"] == error, value
        return value

    def invalid(self, name, arguments):
        response = self.request("tools/call", {"name": name, "arguments": arguments})
        assert response["error"]["code"] == -32602, response

    def close(self, *, terminate=False):
        if terminate:
            self.process.send_signal(signal.SIGTERM)
        else:
            self.process.stdin.close()
        code = self.process.wait(timeout=10)
        assert code == 0, f"MCP exit {code}; inspect {self.stderr_path}"
        self.reader.join(timeout=3)
        assert not self.reader.is_alive(), "MCP stdout reader did not finish"
        self.process.stdin.close()
        self.process.stdout.close()
        self.stderr.close()

    def cleanup(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=10)
        self.process.stdin.close()
        self.reader.join(timeout=3)
        self.process.stdout.close()
        self.stderr.close()


class HeldReplyProxy:
    """Complete one real upstream request, then hold its first reply until released."""
    def __init__(self, upstream, path, *, method="GET"):
        self.reached, self.release = threading.Event(), threading.Event()
        self.paths, self.records = [], []
        self.lock = threading.Lock()
        owner = self
        class Handler(http.server.BaseHTTPRequestHandler):
            def handle_request(self):
                with owner.lock:
                    owner.paths.append(self.path)
                if self.command != method or self.path.split("?", 1)[0] != path:
                    self.send_error(405, "unexpected operation in held reply fixture")
                    return
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 <= size <= 2 * 1024 * 1024:
                    self.send_error(413)
                    return
                request_body = self.rfile.read(size)
                url = urllib.parse.urlsplit(upstream)
                connection = http.client.HTTPConnection(url.hostname, url.port, timeout=10)
                try:
                    headers = {"Accept": "application/json", "Content-Type": "application/json"}
                    for name in ("traceparent", "tracestate"):
                        if self.headers.get(name):
                            headers[name] = self.headers[name]
                    connection.request(self.command, self.path, body=request_body, headers=headers)
                    response = connection.getresponse()
                    body = response.read(2 * 1024 * 1024 + 1)
                    assert len(body) <= 2 * 1024 * 1024
                    with owner.lock:
                        owner.records.append({"path": self.path, "method": self.command,
                            "body": request_body, "status": response.status, "response": body})
                        hold = not owner.reached.is_set()
                        if hold:
                            owner.reached.set()
                    if hold:
                        owner.release.wait(timeout=35)
                    self.send_response(response.status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (OSError, http.client.HTTPException):
                    self.close_connection = True
                finally:
                    connection.close()
            do_GET = handle_request
            do_POST = handle_request
            def log_message(self, *_):
                pass
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def commands(self):
        with self.lock:
            return list(self.records)

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
