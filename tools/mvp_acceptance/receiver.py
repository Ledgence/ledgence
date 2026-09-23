"""Bounded local webhook receiver; duplicates must preserve the immutable event."""

import hashlib
import http.server
import json
import threading
import traceback


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


class Receiver:
    def __init__(self, path, limit):
        self.lock = threading.Lock()
        self.events = {}
        self.received = self.duplicates = 0
        self.errors = []
        self.limit = limit
        self.stream = path.open('w')
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                self.connection.settimeout(5)
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 16384:
                    raise ValueError('callback exceeds envelope limit')
                body = self.rfile.read(length)
                if len(body) != length:
                    raise ValueError('truncated callback')
                event = json.loads(body)
                identity = self.headers.get('ledgence-subscription-id')
                if not identity or event.get('specversion') != '1.0' or 'data' in event:
                    raise ValueError('invalid completion envelope')
                digest = fingerprint(event)
                with owner.lock:
                    previous = owner.events.get(identity)
                    if previous is not None and previous != digest:
                        raise ValueError('callback event changed across deliveries')
                    if previous is None and len(owner.events) >= owner.limit:
                        raise ValueError('callback bound exceeded')
                    owner.events[identity] = digest
                    owner.received += 1
                    owner.duplicates += previous is not None
                    owner.stream.write(json.dumps(dict(subscription_id=identity, event=event)) + '\n')
                    owner.stream.flush()
                self.send_response(204)
                self.send_header('Content-Length', '0')
                self.end_headers()

            def log_message(self, *_):
                pass

        class Server(http.server.ThreadingHTTPServer):
            daemon_threads = False

            def handle_error(self, _request, _address):
                with owner.lock:
                    if len(owner.errors) < 16:
                        owner.errors.append(traceback.format_exc())

        self.server = Server(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}/completed'

    def check(self):
        with self.lock:
            if self.errors:
                raise AssertionError(self.errors)

    def verify(self, subscription):
        self.check()
        with self.lock:
            assert self.events.get(subscription.subscription_id) == fingerprint(subscription.event), 'callback differs from durable event'

    def summary(self):
        self.check()
        with self.lock:
            return dict(unique=len(self.events), received=self.received, duplicates=self.duplicates)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise RuntimeError('callback listener failed to stop')
        self.stream.close()
