"""Loopback SQS wire observations. Measure only timing, queue and receive outcomes (MIT)."""

from collections import Counter
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from threading import Lock, Thread
import time
from urllib.parse import urlsplit


class BrokerMeter:
    def __init__(self, endpoint):
        target = urlsplit(endpoint)
        self._lock = Lock()
        self._receives = []
        self._publication_failure = None
        self._names = {}
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                size = int(self.headers["Content-Length"])
                if size > 8 * 1024 * 1024:
                    self.send_error(413)
                    return
                body = self.rfile.read(size)
                operation = self.headers["X-Amz-Target"]
                started = time.monotonic_ns()
                observed = None
                if operation in {
                    "AmazonSQS.SendMessageBatch",
                    "AmazonSQS.GetQueueAttributes",
                }:
                    physical = urlsplit(json.loads(body)["QueueUrl"]).path.rsplit(
                        "/", 1
                    )[-1]
                    with owner._lock:
                        failed = (
                            owner._publication_failure is not None
                            and owner._names.get(physical) == owner._publication_failure
                        )
                    if failed:
                        payload = b'{"__type":"ServiceUnavailable","message":"owned route outage"}'
                        self.send_response(503)
                        self.send_header("Content-Type", "application/x-amz-json-1.0")
                        self.send_header("Content-Length", str(len(payload)))
                        self.end_headers()
                        self.wfile.write(payload)
                        return
                if operation == "AmazonSQS.ReceiveMessage":
                    request = json.loads(body)
                    physical = urlsplit(request["QueueUrl"]).path.rsplit("/", 1)[-1]
                    with owner._lock:
                        observed = {
                            "start": started,
                            "queue": owner._names[physical],
                            "wait_seconds": request["WaitTimeSeconds"],
                            "end": None,
                            "messages": None,
                        }
                        owner._receives.append(observed)
                # The emulator accepts anonymous local JSON requests. Never
                # retain or forward signed credentials or application messages in evidence.
                conn = http.client.HTTPConnection(
                    target.hostname, target.port, timeout=32
                )
                try:
                    conn.request(
                        "POST",
                        self.path,
                        body,
                        {
                            "Content-Type": self.headers["Content-Type"],
                            "X-Amz-Target": operation,
                        },
                    )
                    response = conn.getresponse()
                    payload = response.read(8 * 1024 * 1024 + 1)
                    if len(payload) > 8 * 1024 * 1024:
                        raise ValueError("oversized broker response")
                    if observed is not None:
                        with owner._lock:
                            observed["end"] = time.monotonic_ns()
                            if response.status == 200:
                                observed["messages"] = len(
                                    json.loads(payload or b"{}").get("Messages", [])
                                )
                    self.send_response(response.status)
                    self.send_header(
                        "Content-Type",
                        response.getheader(
                            "Content-Type", "application/x-amz-json-1.0"
                        ),
                    )
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except (OSError, http.client.HTTPException):
                    self.close_connection = True
                finally:
                    conn.close()

            def log_message(self, *args):
                pass

        class LocalServer(ThreadingHTTPServer):
            request_queue_size = 64

        self.server = LocalServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def route(self, physical, logical):
        with self._lock:
            self._names[physical] = logical

    def fail_publication(self, logical):
        with self._lock:
            self._publication_failure = logical

    def snapshot(self):
        with self._lock:
            return [dict(row) for row in self._receives]

    def counts(self, start, end):
        rows = [row for row in self.snapshot() if start <= row["start"] <= end]
        counts = {}
        for row in rows:
            counter = counts.setdefault(row["queue"], Counter())
            counter["requests"] += 1
            if row["messages"] is None:
                counter["unfinished_or_failed"] += 1
            elif row["messages"] == 0:
                counter["empty"] += 1
            else:
                counter["nonempty"] += 1
        return counts

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
