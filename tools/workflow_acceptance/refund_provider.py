"""Loopback refund destination for workflow crash qualification (MIT)."""
import http.server
import json
import os
import threading
import uuid
from urllib.parse import parse_qs, urlsplit


class RefundProvider:
    """Withhold the first successful POST reply after committing the refund."""

    def __init__(self, ledger_path, *, cooperative):
        self.ledger_path = ledger_path
        self.ledger_path.touch(exist_ok=False)
        self.cooperative = cooperative
        self.lock = threading.Lock()
        self.accepted = threading.Event()
        self._release = threading.Event()
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def reply(self, status, value):
                payload = json.dumps(value).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_POST(self):
                if self.path != "/refunds":
                    return self.reply(404, {"error": "unknown route"})
                with owner.lock:
                    owner._record(kind="refund_request")
                self.connection.settimeout(5)
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    if not 0 < length <= 8192:
                        raise ValueError("invalid refund request length")
                    request = json.loads(self.rfile.read(length))
                    if (not isinstance(request, dict) or set(request) != {"effect_key", "arguments"}
                            or not isinstance(request["effect_key"], str) or not request["effect_key"]
                            or not isinstance(request["arguments"], dict)):
                        raise ValueError("expected an effect key and refund arguments")
                except (ValueError, TimeoutError):
                    return self.reply(400, {"error": "invalid refund request"})
                status, value, withhold = owner._submit(request)
                if withhold:
                    # No response byte is sent, even if the worker outlives the fault.
                    owner._release.wait()
                    self.close_connection = True
                    return
                self.reply(status, value)

            def do_GET(self):
                route = urlsplit(self.path)
                if route.path != "/refunds":
                    return self.reply(404, {"error": "unknown route"})
                keys = parse_qs(route.query).get("effect_key", [])
                if len(keys) != 1:
                    return self.reply(400, {"error": "expected one effect key"})
                status, value = owner._lookup(keys[0])
                self.reply(status, value)

            def log_message(self, *_args):
                pass

        class Server(http.server.ThreadingHTTPServer):
            daemon_threads = False

        self.server = Server(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_args):
        self.drop_reply()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def drop_reply(self):
        self._release.set()

    def _events(self):
        with self.ledger_path.open(encoding="utf-8") as log:
            return [json.loads(line) for line in log]

    def _record(self, **event):
        with self.ledger_path.open("a", encoding="utf-8") as log:
            log.write(json.dumps(event) + "\n")
            log.flush()
            os.fsync(log.fileno())

    def _receipt(self, key):
        return next((event["created"] for event in self._events()
                     if "created" in event and event["created"]["effect_key"] == key), None)

    def _submit(self, request):
        key = request["effect_key"]
        with self.lock:
            existing = self._receipt(key) if self.cooperative else None
            if existing is not None:
                if json.dumps(existing["arguments"], sort_keys=True) != json.dumps(request["arguments"], sort_keys=True):
                    return 409, {"error": "refund arguments conflict with the original request"}, False
                return 200, existing, False
            receipt = {"refund_id": "ref_" + uuid.uuid4().hex, **request}
            self._record(kind="refund_created", created=receipt)
            first = not self.accepted.is_set()
            self.accepted.set()
            return 201, receipt, first

    def _lookup(self, key):
        with self.lock:
            self._record(kind="lookup_request", effect_key=key)
            if not self.cooperative:
                return 501, {"error": "refund lookup unsupported"}
            receipt = self._receipt(key)
            if receipt is None:
                return 404, {"error": "unknown refund"}
            return 200, receipt

    def census(self):
        """Read the destination's own requests and committed refunds under one lock."""
        with self.lock:
            events = self._events()
            return {
                "post_requests": sum(event["kind"] == "refund_request" for event in events),
                "lookup_requests": sum(event["kind"] == "lookup_request" for event in events),
                "refunds": [event["created"] for event in events if "created" in event],
            }
