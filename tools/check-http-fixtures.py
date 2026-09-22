"""Bounded regressions for the local HTTP acceptance fixtures; no database needed."""
from concurrent.futures import ThreadPoolExecutor
import http.client
import http.server
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
import urllib.parse

from http_acceptance.harness import ArtifactServer


class ArtifactServerTests(unittest.TestCase):
    def test_cold_descriptor_burst_survives_delayed_acceptance(self):
        # Hold the accept loop briefly to model a cold fixture being descheduled
        # while the release harness opens its concurrent descriptor requests.
        count = 32
        accepting = threading.Event()
        all_connected = threading.Event()
        start = threading.Barrier(count + 1, timeout=10)
        lock = threading.Lock()
        connections = 0
        serve = http.server.ThreadingHTTPServer.serve_forever

        def delayed_accept(server, *args, **kwargs):
            accepting.wait(timeout=10)
            return serve(server, *args, **kwargs)

        with tempfile.TemporaryDirectory(prefix='ledgence-artifact-fixture-') as temporary:
            store = Path(temporary)
            descriptor = store / 'programs/performance/1.0.0/descriptor.json'
            descriptor.parent.mkdir(parents=True)
            content = b'{"program":{"id":"performance","version":"1.0.0"}}'
            descriptor.write_bytes(content)
            with patch.object(http.server.ThreadingHTTPServer, 'serve_forever', delayed_accept):
                fixture = ArtifactServer(store)
                address = urllib.parse.urlsplit(fixture.url)

                def request():
                    nonlocal connections
                    connection = http.client.HTTPConnection(address.hostname, address.port, timeout=10)
                    try:
                        start.wait()
                        connection.connect()
                        with lock:
                            connections += 1
                            if connections == count:
                                all_connected.set()
                        connection.request('GET', '/programs/performance/1.0.0/descriptor.json')
                        response = connection.getresponse()
                        return response.status, response.read(len(content) + 1)
                    finally:
                        connection.close()

                try:
                    with ThreadPoolExecutor(max_workers=count) as clients:
                        requests = [clients.submit(request) for _ in range(count)]
                        start.wait()
                        try:
                            self.assertTrue(all_connected.wait(timeout=5),
                                'cold artifact listener rejected its concurrent descriptor burst')
                        finally:
                            accepting.set()
                        for future in requests:
                            self.assertEqual(future.result(timeout=15), (200, content))
                    self.assertEqual(fixture.counts['/programs/performance/1.0.0/descriptor.json'], count)
                finally:
                    accepting.set()
                    fixture.close()


if __name__ == '__main__':
    unittest.main(verbosity=2)
