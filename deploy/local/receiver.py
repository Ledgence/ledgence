"""Small local demo receiver: bounded, persisted source/id deduplication before ACK.

This is an example, not a general webhook service. At 256 unique events it
returns 503 until the local demo data is reset. No application data is logged.
"""
import http.server
import json
import os
from pathlib import Path
import threading

STORE = Path("/demo-state/events.json")
LOCK = threading.Lock()
EVENTS = json.loads(STORE.read_text()) if STORE.exists() else {}


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.reply(200, b"ok")
        elif self.path == "/page.txt":
            self.reply(200, b"Hello from Ledgence!\n")
        elif self.path == "/events":
            with LOCK:
                body = json.dumps(list(EVENTS.values())).encode()
            self.reply(200, body, "application/json")
        else:
            self.reply(404, b"")

    def do_POST(self):
        self.connection.settimeout(10)
        if self.path != "/completion":
            return self.reply(404, b"")
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 16384:
                return self.reply(413, b"")
            body = self.rfile.read(length)
            if len(body) != length:
                return self.reply(400, b"")
            event = json.loads(body)
            if not isinstance(event, dict) or event.get("specversion") != "1.0" or not isinstance(event.get("id"), str) or event.get("source") != "urn:ledgence:orchestrator":
                return self.reply(400, b"")
            key = event["source"] + "\n" + event["id"]
            with LOCK:
                if key not in EVENTS:
                    if len(EVENTS) >= 256:
                        return self.reply(503, b"")
                    updated = dict(EVENTS, **{key: event})
                    temporary = STORE.with_suffix(".tmp")
                    with temporary.open("w") as output:
                        json.dump(updated, output)
                        output.flush()
                        os.fsync(output.fileno())
                    temporary.replace(STORE)
                    directory = os.open(STORE.parent, os.O_RDONLY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
                    EVENTS.update(updated)
            self.reply(204, b"")
        except (ValueError, KeyError, OSError):
            self.reply(400, b"")

    def reply(self, status, body, kind="text/plain"):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Type", kind)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    http.server.HTTPServer(("0.0.0.0", 8091), Handler).serve_forever()
