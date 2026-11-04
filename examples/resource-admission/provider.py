"""Local deterministic classifier; no model account or API key is used (MIT)."""
import argparse
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import time


class Classifier:
    """A synthetic endpoint with independently observable HTTP activity."""
    def __init__(self, port=0):
        self.lock = threading.Lock()
        self.calls, self.completed, self.active, self.peak = [], [], 0, 0
        self.gates, self.dropped = {}, set()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    if self.path != "/classify" or not 0 < length <= 8192:
                        self.send_error(400)
                        return
                    value = json.loads(self.rfile.read(length))
                    text = value["text"]
                    if type(text) is not str:
                        raise ValueError("text must be a string")
                except (ValueError, KeyError, TypeError):
                    self.send_error(400)
                    return
                with owner.lock:
                    owner.calls.append({"text": text, "at": time.monotonic()})
                    owner.active += 1
                    owner.peak = max(owner.peak, owner.active)
                    gate = owner.gates.get(text)
                try:
                    if gate is not None and not gate.wait(30):
                        self.close_connection = True
                        return
                    result = {"category": "billing" if "invoice" in text.lower() else "general"}
                    with owner.lock:
                        owner.completed.append(text)
                        drop = text in owner.dropped
                    if drop:
                        self.close_connection = True
                        return
                    body = json.dumps(result).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (OSError, BrokenPipeError):
                    pass
                finally:
                    with owner.lock:
                        owner.active -= 1

            def log_message(self, *_):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def hold(self, text):
        with self.lock:
            self.gates[text] = threading.Event()

    def release(self, text):
        with self.lock:
            self.gates[text].set()

    def drop_response(self, text):
        with self.lock:
            self.dropped.add(text)

    def census(self):
        with self.lock:
            return {"requests": dict(Counter(row["text"] for row in self.calls)),
                    "completed": dict(Counter(self.completed)), "active": self.active,
                    "peak": self.peak, "calls": list(self.calls)}

    def close(self):
        with self.lock:
            for gate in self.gates.values():
                gate.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8099)
    provider = Classifier(parser.parse_args().port)
    print(provider.url, flush=True)
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        pass
    finally:
        provider.close()
