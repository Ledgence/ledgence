"""Independent loopback request, effect and occupancy witness (MIT)."""

import json
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Provider:
    def __init__(self):
        self.lock = threading.Lock()
        self.rows = []
        self.effects = set()
        self.counts = Counter()
        self.warm = threading.Event()
        self.held = {}
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                try:
                    n = int(self.headers.get("Content-Length", "0"))
                    if self.path != "/work" or not 0 < n <= 8192:
                        raise ValueError
                    data = json.loads(self.rfile.read(n))
                    key, delay = data["key"], data["ms"]
                    if not isinstance(key, str) or not key or not 0 <= delay <= 1500:
                        raise ValueError
                except (ValueError, KeyError, TypeError):
                    self.send_error(400)
                    return
                with owner.lock:
                    owner.counts[key] += 1
                    charge = owner.counts[key]
                    status = 429 if data.get("throttle", 0) >= charge else 200
                    row = {
                        "key": key,
                        "task": data["task"],
                        "attempt": data["attempt"],
                        "workflow": data["workflow"],
                        "ordinal": charge,
                        "start_ns": time.monotonic_ns(),
                        "status": status,
                    }
                    owner.rows.append(row)
                if key.startswith("warm-") and not owner.warm.wait(30):
                    self.send_error(503)
                    return
                gate = owner.held.get(key)
                if gate is not None and not gate.wait(45):
                    self.send_error(503)
                    return
                time.sleep(delay / 1000)
                with owner.lock:
                    row["new_effect"] = status == 200 and key not in owner.effects
                    if status == 200:
                        owner.effects.add(key)
                    row["end_ns"] = time.monotonic_ns()
                payload = json.dumps({"key": key}).encode()
                self.send_response(status)
                if status == 429:
                    self.send_header("Retry-After", "1")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_port
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def records(self, prefix):
        with self.lock:
            return [r.copy() for r in self.rows if r["key"].startswith(prefix + "-")]

    def close(self):
        self.warm.set()
        for gate in self.held.values():
            gate.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
