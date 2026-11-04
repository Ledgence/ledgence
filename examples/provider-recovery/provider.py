"""A local asynchronous CSV export service with an independent durable census (MIT).

This is an example provider, not a Ledgence plugin. It uses only the Python
standard library and binds loopback. The native acceptance harness can configure
faults directly; no HTTP endpoint can change its fault policy.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
from pathlib import Path
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit
import uuid


class ExportProvider:
    def __init__(self, directory, *, port=0, completion_delay=0.25, reply_timeout=5):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.log_path = self.directory / "provider.jsonl"
        self.lock = threading.RLock()
        self.stopping = threading.Event()
        self.accepted = threading.Event()
        self.release_reply = threading.Event()
        self.release_reply.set()
        if not 0 < reply_timeout <= 30:
            raise ValueError("reply timeout must be greater than zero and at most 30 seconds")
        self.reply_timeout = reply_timeout
        self.completion_delay = completion_delay
        self.clock_offset = 0.0
        self.operations = {}
        self.faults = {}
        self.server = ThreadingHTTPServer(("127.0.0.1", port), self.handler())
        self.server.daemon_threads = False
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.thread = threading.Thread(
            target=self.server.serve_forever, name="export-http"
        )
        self.jobs = threading.Thread(target=self.complete_jobs, name="export-jobs")
        self._restore()

    def _restore(self):
        if self.log_path.exists():
            for line in self.log_path.read_text().splitlines():
                event = json.loads(line)
                if event["kind"] == "created":
                    self.operations[event["operation"]["id"]] = event["operation"]
                elif event["kind"] == "completed":
                    self.operations[event["id"]].update(event["changes"])

    def now(self):
        return time.time() + self.clock_offset

    def record(self, kind, **fields):
        # Caller holds the lock. Keeping this outside Ledgence lets the gate
        # distinguish accepted claims, received HTTP requests and real effects.
        import os

        with self.log_path.open("a", encoding="utf-8") as stream:
            stream.write(
                json.dumps(
                    {"kind": kind, "at": self.now(), **fields}, separators=(",", ":")
                )
                + "\n"
            )
            stream.flush()
            os.fsync(stream.fileno())

    def set_fault(self, key, **values):
        with self.lock:
            self.faults[key] = dict(values)

    def advance_clock(self, seconds):
        with self.lock:
            self.clock_offset += seconds

    def complete_jobs(self):
        while not self.stopping.wait(0.01):
            with self.lock:
                for operation in self.operations.values():
                    fault = self.faults.get(operation["key"], {})
                    if (
                        operation["status"] != "pending"
                        or fault.get("never_complete")
                        or self.now() < operation["ready_at"]
                    ):
                        continue
                    state = fault.get("terminal", "succeeded")
                    changes = {"status": state}
                    if state == "succeeded":
                        content = io.StringIO(newline="")
                        writer = csv.writer(content)
                        writer.writerow(["dataset", "row", "value"])
                        for row in range(3):
                            writer.writerow(
                                [operation["arguments"]["dataset"], row, row * 10]
                            )
                        data = content.getvalue().encode()
                        name = operation["id"] + ".csv"
                        artifact = self.directory / name
                        with artifact.open("wb") as stream:
                            stream.write(data)
                            stream.flush()
                            import os

                            os.fsync(stream.fileno())
                        changes["result"] = {
                            "artifact": name,
                            "sha256": hashlib.sha256(data).hexdigest(),
                            "rows": 3,
                        }
                    elif state == "failed":
                        changes["failure"] = {
                            "kind": "dataset_unavailable",
                            "message": "Synthetic dataset is unavailable",
                        }
                    elif state == "cancelled":
                        changes["reason"] = "Synthetic provider cancelled the export"
                    elif state == "requires_action":
                        changes["action_kind"] = "confirm_dataset"
                    else:
                        raise ValueError("unsupported synthetic terminal state")
                    self.record("completed", id=operation["id"], changes=changes)
                    operation.update(changes)

    def census(self):
        with self.lock:
            events = (
                [json.loads(line) for line in self.log_path.read_text().splitlines()]
                if self.log_path.exists()
                else []
            )
            requests = [event for event in events if event["kind"] == "request"]
            return {
                "mutation_http_requests": sum(
                    event["method"] == "POST" for event in requests
                ),
                "status_gets": sum(
                    event["method"] == "GET" and event["path"] == "/exports"
                    for event in requests
                ),
                "identity_gets": sum(
                    event["method"] == "GET" and event["path"] == "/account"
                    for event in requests
                ),
                "artifact_gets": sum(
                    event["method"] == "GET" and event["path"].startswith("/artifacts/")
                    for event in requests
                ),
                "committed_effects": sum(
                    event["kind"] == "created" for event in events
                ),
                "completed_effects": sum(
                    event["kind"] == "completed" for event in events
                ),
                "operations": json.loads(json.dumps(list(self.operations.values()))),
            }

    def handler(self):
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_):
                pass

            def setup(self):
                super().setup()
                self.connection.settimeout(5)

            def send_json(self, status, value):
                data = json.dumps(value, separators=(",", ":")).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Connection", "close")
                self.end_headers()
                try:
                    self.wfile.write(data)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                self.close_connection = True

            def do_POST(self):
                path = urlsplit(self.path).path
                account, key = (
                    self.headers.get("X-Provider-Account", ""),
                    self.headers.get("Idempotency-Key", ""),
                )
                with owner.lock:
                    owner.record(
                        "request", method="POST", path=path, account=account, key=key
                    )
                try:
                    length = int(self.headers.get("Content-Length", "-1"))
                    if not 0 <= length <= 32768:
                        return self.send_json(400, {"error": "invalid_length"})
                    body = self.rfile.read(length)
                    arguments = json.loads(body)
                    if (
                        type(arguments) is not dict
                        or set(arguments) != {"dataset"}
                        or type(arguments["dataset"]) is not str
                        or not 1 <= len(arguments["dataset"].encode()) <= 128
                    ):
                        return self.send_json(400, {"error": "invalid_dataset"})
                except (ValueError, OSError):
                    return self.send_json(400, {"error": "invalid_request"})
                with owner.lock:
                    owner.record(
                        "request_body",
                        method="POST",
                        path=path,
                        account=account,
                        key=key,
                        body_sha256=hashlib.sha256(body).hexdigest(),
                    )
                    if path != "/exports" or not account or not key:
                        return self.send_json(400, {"error": "invalid_target"})
                    fault = owner.faults.get(key, {})
                    if fault.get("reject_before_commit"):
                        return self.send_json(500, {"error": "unavailable"})
                    operation = next(
                        (
                            op
                            for op in owner.operations.values()
                            if op["account"] == account
                            and op["key"] == key
                            and owner.now() < op["dedup_until"]
                        ),
                        None,
                    )
                    if operation is not None and operation["arguments"] != arguments:
                        return self.send_json(409, {"error": "key_conflict"})
                    if operation is None:
                        operation = {
                            "id": "export_" + uuid.uuid4().hex,
                            "account": account,
                            "key": key,
                            "arguments": arguments,
                            "status": "pending",
                            "ready_at": owner.now() + owner.completion_delay,
                            "dedup_until": owner.now()
                            + fault.get("key_lifetime", 3600),
                        }
                        owner.record("created", operation=operation)
                        owner.operations[operation["id"]] = operation
                    reply = {"id": operation["id"], "status": operation["status"]}
                    owner.accepted.set()
                owner.release_reply.wait(owner.reply_timeout)
                response_kind = fault.get("post_response", "normal")
                if response_kind == "drop":
                    self.close_connection = True
                    return
                if response_kind == "500":
                    return self.send_json(500, {"error": "uncertain"})
                if response_kind == "redirect":
                    return self.send_json(307, {"location": owner.url + "/exports"})
                if fault.get("post_terminal_reply"):
                    with owner.lock:
                        reply = {
                            name: value
                            for name, value in operation.items()
                            if name
                            in (
                                "id",
                                "status",
                                "result",
                                "failure",
                                "reason",
                                "action_kind",
                            )
                        }
                self.send_json(202, reply)

            def do_GET(self):
                parsed = urlsplit(self.path)
                account = self.headers.get("X-Provider-Account", "")
                with owner.lock:
                    key = parse_qs(parsed.query).get("key", [""])[0]
                    owner.record(
                        "request", method="GET", path=parsed.path, account=account, key=key
                    )
                    delay = owner.faults.get(key, {}).get("lookup_delay", 0)
                # Delay after counting receipt, outside the shared lock. It is a
                # bounded fixture fault; another request still reaches this service.
                if delay:
                    owner.stopping.wait(min(float(delay), 5))
                with owner.lock:
                    if parsed.path == "/account":
                        return self.send_json(200, {"account": account})
                    if parsed.path == "/exports":
                        key = parse_qs(parsed.query).get("key", [""])[0]
                        fault = owner.faults.get(key, {})
                        hidden = fault.get("hidden_gets", 0)
                        if hidden:
                            fault["hidden_gets"] -= 1
                            return self.send_json(404, {"error": "not_visible"})
                        operation = next(
                            (
                                op
                                for op in owner.operations.values()
                                if op["account"] == account and op["key"] == key
                            ),
                            None,
                        )
                        if operation is None:
                            return self.send_json(404, {"error": "not_visible"})
                        result = {
                            name: value
                            for name, value in operation.items()
                            if name
                            in (
                                "id",
                                "status",
                                "result",
                                "failure",
                                "reason",
                                "action_kind",
                            )
                        }
                        if "lookup_result" in fault and result["status"] == "succeeded":
                            result["result"] = fault["lookup_result"]
                        return self.send_json(200, result)
                    if parsed.path.startswith("/artifacts/"):
                        name = parsed.path.removeprefix("/artifacts/")
                        operation = next(
                            (
                                op
                                for op in owner.operations.values()
                                if op["account"] == account
                                and op.get("result", {}).get("artifact") == name
                            ),
                            None,
                        )
                        if operation is None:
                            return self.send_json(404, {"error": "missing_artifact"})
                        data = (owner.directory / name).read_bytes()
                        self.send_response(200)
                        self.send_header("Content-Type", "text/csv")
                        self.send_header("Content-Length", str(len(data)))
                        self.send_header("Connection", "close")
                        self.end_headers()
                        try:
                            self.wfile.write(data)
                        except (BrokenPipeError, ConnectionResetError):
                            pass
                        self.close_connection = True
                        return
                    self.send_json(404, {"error": "unknown_route"})

        return Handler

    def __enter__(self):
        self.thread.start()
        self.jobs.start()
        return self

    def __exit__(self, *_):
        self.stopping.set()
        self.release_reply.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)
        self.jobs.join(5)
        if self.thread.is_alive() or self.jobs.is_alive():
            raise RuntimeError("provider cleanup incomplete")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=Path(".provider"))
    parser.add_argument("--port", type=int, default=8092)
    args = parser.parse_args()
    with ExportProvider(args.directory, port=args.port, completion_delay=2) as provider:
        print(provider.url, flush=True)
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
