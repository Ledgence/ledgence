"""Owned subprocesses and deterministic HTTP faults for the network gate."""

import contextlib
import http.client
import http.server
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import time
import urllib.parse


def eventually(predicate, timeout=30, description="condition"):
    deadline = time.monotonic() + timeout
    while True:
        result = predicate()
        if result:
            return result
        if time.monotonic() >= deadline:
            raise AssertionError(f"Timed out waiting for {description}")
        time.sleep(0.05)


def unused_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def exchange(base, method, path, body=None, timeout=35):
    url = urllib.parse.urlsplit(base)
    connection = http.client.HTTPConnection(url.hostname, url.port, timeout=timeout)
    headers = {"Accept": "application/json"}
    if body is not None:
        body = body if isinstance(body, bytes) else json.dumps(body, ensure_ascii=False).encode()
        headers["Content-Type"] = "application/json"
    try:
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        data = response.read(16 * 1024 * 1024 + 1)
        assert len(data) <= 16 * 1024 * 1024, "oversized response"
        return response.status, dict(response.getheaders()), json.loads(data)
    finally:
        connection.close()


class Process:
    def __init__(self, args, directory, label, environment=None):
        self.label = label
        self.stdout_path = directory / f"{label}.stdout"
        self.stderr_path = directory / f"{label}.stderr"
        self.stdout = self.stdout_path.open("wb")
        self.stderr = self.stderr_path.open("wb")
        self.process = subprocess.Popen(
            args, stdout=self.stdout, stderr=self.stderr, env=environment,
            start_new_session=True,
        )

    def kill(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=10)

    def stop(self, timeout=40):
        assert self.process.poll() is None, f"{self.label} exited before shutdown: {self.stderr_path}"
        self.process.send_signal(signal.SIGTERM)
        try:
            code = self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            raise AssertionError(f"{self.label} did not drain; inspect {self.stderr_path}") from None
        expected = 1 if self.label.startswith("worker-") else 0
        assert code == expected, f"{self.label} exit {code}; inspect {self.stderr_path}"
        if self.label.startswith("worker-"):
            delivery = json.loads(self.stdout_path.read_text())["delivery"]
            assert delivery["finished"] is True, delivery
        return code

    def cleanup(self):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=10)
        self.stdout.close()
        self.stderr.close()


class ArtifactServer:
    def __init__(self, directory):
        self.counts = {}
        self.lock = threading.Lock()
        self.block = None
        owner = self

        class Handler(http.server.SimpleHTTPRequestHandler):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, directory=str(directory), **kwargs)

            def do_GET(self):
                with owner.lock:
                    owner.counts[self.path] = owner.counts.get(self.path, 0) + 1
                    blocker = owner.block
                if blocker and self.path.endswith(".zip"):
                    blocker[0].set()
                    blocker[1].wait(timeout=120)
                try:
                    super().do_GET()
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, *_):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def downloads(self):
        with self.lock:
            return sum(count for path, count in self.counts.items() if path.endswith(".zip"))

    def close(self):
        if self.block:
            self.block[1].set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class FaultProxy:
    """Consume a committed upstream reply, then deterministically lose it."""

    def __init__(self, upstream):
        self.upstream = upstream
        self.lock = threading.Lock()
        self.blocked = False
        self.rules = []
        self.records = []
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def handle_request(self):
                size = int(self.headers.get("Content-Length", "0"))
                if size > 8 * 1024 * 1024:
                    self.send_error(413)
                    return
                body = self.rfile.read(size)
                parsed = json.loads(body) if body else None
                with owner.lock:
                    blocked = owner.blocked
                    upstream = owner.upstream
                if blocked:
                    self.close_connection = True
                    return
                url = urllib.parse.urlsplit(upstream)
                connection = http.client.HTTPConnection(url.hostname, url.port, timeout=35)
                try:
                    headers = {"Accept": "application/json", "Content-Type": "application/json"}
                    connection.request(self.command, self.path, body=body, headers=headers)
                    response = connection.getresponse()
                    payload = response.read(16 * 1024 * 1024 + 1)
                    response_headers = dict(response.getheaders())
                    record = {"path": self.path, "body": body, "json": parsed,
                              "status": response.status, "response": payload,
                              "request_id": response_headers.get("request-id")}
                    with owner.lock:
                        owner.records.append(record)
                        rule = next((rule for rule in owner.rules if not rule[2].is_set()
                                     and rule[0] == self.path and rule[1](parsed)), None)
                        if rule is not None:
                            if rule[3]:
                                owner.blocked = True
                            rule[2].set()
                    if rule is not None:
                        self.close_connection = True
                        return
                    self.send_response(response.status)
                    for key, value in response_headers.items():
                        if key.lower() not in ("content-length", "transfer-encoding", "connection"):
                            self.send_header(key, value)
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except (OSError, http.client.HTTPException):
                    self.close_connection = True
                finally:
                    connection.close()

            do_POST = handle_request
            do_GET = handle_request

            def log_message(self, *_):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def lose_once(self, path, predicate=lambda _: True, block_after=False):
        event = threading.Event()
        with self.lock:
            self.rules.append((path, predicate, event, block_after))
        return event

    def commands(self, path, predicate=lambda _: True):
        with self.lock:
            return [record for record in self.records
                    if record["path"] == path and predicate(record["json"])]

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


PROGRAM = '''import json, os, time
from pathlib import Path
from prepared_dependency import VALUE

def handle(event):
    data = event['data']
    record = {'task': event['ldgtaskid'], 'attempt': event['ldgattemptid'],
              'number': event['ldgattemptno'], 'pid': os.getpid()}
    with open(data['marker'], 'a', encoding='utf-8') as output:
        output.write(json.dumps(record) + '\\n')
    mode = data.get('mode', 'success')
    if mode == 'business':
        raise RuntimeError('deliberate business failure')
    if (mode == 'gate' or (mode == 'first_gate' and event['ldgattemptno'] == 1)
            or data.get('interrupt_gate_at') == event['ldgattemptno']):
        parent = os.getppid()
        deadline = time.monotonic() + 150
        while not Path(data['gate']).exists():
            if os.getppid() != parent:
                raise RuntimeError('test worker process exited')
            if time.monotonic() > deadline:
                raise RuntimeError('test gate was not released')
            time.sleep(0.05)
    if mode == 'interrupt' or (mode == 'interrupt_once' and event['ldgattemptno'] == 1):
        os._exit(17)
    return {'pid': os.getpid(), 'dependency': VALUE, 'event': event}
'''


class Deployment:
    def __init__(self, root, directory, binaries, python, database_url, psql):
        self.root, self.directory, self.binaries = root, directory, binaries
        self.python, self.database_url, self.psql = python, database_url, psql
        self.processes, self.proxies = [], []
        self.gates = set()
        self.store = directory / "store"
        self.store.mkdir()
        self.marker = directory / "invocations.jsonl"
        self.counter = 0
        self.scope = {"tenant_id": "tenant_http", "namespace": "billing"}
        self.queue = "python-http"
        self.server_port = unused_port()
        self.server_url = f"http://127.0.0.1:{self.server_port}"
        self.environment = dict(os.environ, DATABASE_URL=database_url, RUST_LOG="info")
        self.publish("invoice", "1.0.0")
        self.artifacts = ArtifactServer(self.store)

    def command(self, binary, args, expected=0):
        result = subprocess.run([str(self.binaries / binary)] + args, env=self.environment,
                                capture_output=True, timeout=45)
        assert result.returncode == expected, (
            f"{binary} failed ({result.returncode}): {result.stderr.decode(errors='replace')[-4000:]}"
        )
        return json.loads(result.stdout) if result.stdout.strip() else None

    def publish(self, name, version, startup_gate=None):
        directory = self.directory / f"package-{name}-{version}"
        info = self.command("ledgence-worker", ["example", "--directory", str(directory),
                                               "--python", self.python])
        package = Path(info["program"])
        manifest_path = package / "ledgence-program.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["program"] = {"id": name, "version": version}
        manifest_path.write_text(json.dumps(manifest))
        program = PROGRAM
        if startup_gate is not None:
            self.gates.add(startup_gate)
            program = ("import os, time\nfrom pathlib import Path\n"
                       f"Path({str(startup_gate) + '.pid'!r}).write_text(str(os.getpid()))\n"
                       f"while not Path({str(startup_gate)!r}).exists():\n    time.sleep(0.05)\n" + program)
        (package / "program.py").write_text(program)
        (package / "prepared_dependency.py").write_text("VALUE = 'packaged'\n")
        return self.command("ledgence-worker", ["publish", "--source", str(package),
                                               "--store", str(self.store)])

    def start_server(self, port=None):
        port = port or self.server_port
        self.counter += 1
        process = Process([str(self.binaries / "ledgence-orchestrator"), "serve", "--bind",
                           f"127.0.0.1:{port}", "--store", self.artifacts.url],
                          self.directory, f"server-{self.counter}", self.environment)
        self.processes.append(process)
        base = f"http://127.0.0.1:{port}"

        def ready():
            assert process.process.poll() is None, f"server exited: {process.stderr_path}"
            try:
                return exchange(base, "GET", "/health/ready", timeout=1)[0] == 200
            except (OSError, http.client.HTTPException):
                return False

        eventually(ready, description="orchestrator readiness")
        return process, base

    def proxy(self, upstream=None):
        proxy = FaultProxy(upstream or self.server_url)
        self.proxies.append(proxy)
        return proxy

    def start_worker(self, server=None, concurrency=1, cache="cache"):
        self.counter += 1
        args = [str(self.binaries / "ledgence-worker"), "connect", "--server",
                server or self.server_url, "--tenant", self.scope["tenant_id"], "--namespace",
                self.scope["namespace"], "--queue", self.queue, "--store", self.artifacts.url,
                "--cache", str(self.directory / cache), "--python", self.python,
                "--runner", str(self.root / "sdk/python/ledgence_worker/bootstrap.py"),
                "--concurrency", str(concurrency)]
        process = Process(args, self.directory, f"worker-{self.counter}", self.environment)
        self.processes.append(process)
        return process

    def submission(self, key, mode="success", program="invoice", version="1.0.0", **data):
        if "gate" in data:
            self.gates.add(Path(data["gate"]))
        return {"idempotency_key": key,
                "origin_trace": {"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
                "input": dict(self.scope, queue=self.queue, program={"id": program, "version": version},
                              retry_policy={"max_attempts": 3, "retry_delay_ms": 0},
                              data=dict(data, mode=mode, marker=str(self.marker)))}

    def submit(self, command, server=None):
        self.counter += 1
        path = self.directory / f"submit-{self.counter}.json"
        path.write_text(json.dumps(command, ensure_ascii=False))
        return self.command("ledgence", ["task", "submit", "--server", server or self.server_url,
                                         "--file", str(path)])

    def task(self, task_id):
        query = urllib.parse.urlencode(dict(self.scope, task_id=task_id))
        status, _, result = exchange(self.server_url, "GET", f"/v1/tasks/inspect?{query}")
        assert status == 200, result
        return result

    def history(self, task_id, after=0):
        query = urllib.parse.urlencode(dict(self.scope, task_id=task_id, after_sequence=after))
        status, _, result = exchange(self.server_url, "GET", f"/v1/tasks/history?{query}")
        assert status == 200, result
        return result

    def attempt(self, task_id, attempt_id=None):
        if attempt_id is None:
            task = self.task(task_id)
            attempt_id = task["current_attempt_id"]
            if attempt_id is None:
                claims, after = [], 0
                while page := self.history(task_id, after):
                    claims.extend(x["event"]["attempt_id"] for x in page
                                  if x["event"]["reason"] == "claimed")
                    after = page[-1]["sequence"]
                assert len(claims) == task["attempt_count"], task
                attempt_id = claims[-1]
        query = urllib.parse.urlencode(dict(self.scope, task_id=task_id, attempt_id=attempt_id))
        status, _, result = exchange(self.server_url, "GET", f"/v1/attempts/inspect?{query}")
        assert status == 200, result
        return result

    def terminal(self, task_id, state="succeeded", timeout=35):
        task = eventually(lambda: (t if (t := self.task(task_id))["state"] in
                                   ("succeeded", "failed", "cancelled") else None),
                          timeout, f"task {task_id} to finish")
        assert task["state"] == state, task
        return task, self.attempt(task_id) if task["attempt_count"] else None

    def invocations(self, task_id=None):
        if not self.marker.exists():
            return []
        # A polling read can overlap one append. Every newline-terminated
        # record must decode; silently skipping bad complete lines hides runs.
        text = self.marker.read_text()
        lines = text.splitlines(keepends=True)
        records = [json.loads(line) for line in lines if line.endswith("\n")]
        return [r for r in records if task_id is None or r["task"] == task_id]

    def started(self, task_id, count=1):
        return eventually(lambda: (records if len(records := self.invocations(task_id)) >= count
                                   else None), description="Python invocation marker")

    def cancel(self, task_id):
        return self.command("ledgence", ["task", "cancel", "--server", self.server_url,
                                         "--tenant", self.scope["tenant_id"], "--namespace",
                                         self.scope["namespace"], "--task", task_id])

    def sql(self, statement, administrative=False):
        url = self.database_url
        if administrative:
            url = urllib.parse.urlunsplit(urllib.parse.urlsplit(url)._replace(path="/postgres"))
        result = subprocess.run([self.psql, "--dbname", url, "-X", "-A", "-t",
                                 "--set", "ON_ERROR_STOP=1", "--command", statement],
                                capture_output=True, timeout=35)
        assert result.returncode == 0, "owned test database command failed"
        return result.stdout.decode().strip()

    def close(self):
        # Release test gates first so deliberately orphaned handlers can exit.
        for gate in self.gates:
            gate.touch()
        for process in reversed(self.processes):
            process.cleanup()
        # The fixture can outlive a SIGKILLed worker. Match both the helper and
        # this unique run's package path; never signal unrelated Python programs.
        processes = subprocess.check_output(["ps", "-axo", "pid=,args="], text=True)
        for line in processes.splitlines():
            fields = line.strip().split(None, 1)
            if len(fields) == 2 and "--package-root" in fields[1] and str(self.directory) + "/" in fields[1]:
                with contextlib.suppress(ProcessLookupError):
                    os.kill(int(fields[0]), signal.SIGKILL)
        for proxy in self.proxies:
            proxy.close()
        self.artifacts.close()
