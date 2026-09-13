"""Bounded real-process acquisition resources and retained shutdown ownership.

Each scenario uses its Deployment's disposable database. These fixtures never
restart PostgreSQL, and they make no whole-process memory or latency SLA claim.
"""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import http.client
import json
import re
import signal
import socket
import struct
import subprocess
import threading
import time
import urllib.parse
import uuid

from .harness import Process, eventually, exchange
from .longpoll import acquisition, poll, servers, session, waiting


class AuxiliaryProxy:
    """Forward only the optional pool; retain a deliberately half-open cleanup.

    The local test URI disables TLS so the fixture can identify UNLISTEN and
    count PostgreSQL NotificationResponse frames. Production URI handling is
    unchanged. No credentials or authentication frames are written to evidence.
    """

    def __init__(self, database_url):
        parsed = urllib.parse.urlsplit(database_url)
        self.upstream = (parsed.hostname, parsed.port or 5432)
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.1)
        credentials = parsed.netloc.rsplit("@", 1)[0]
        query = dict(urllib.parse.parse_qsl(parsed.query))
        query["sslmode"] = "disable"
        self.url = urllib.parse.urlunsplit(parsed._replace(
            netloc=f"{credentials}@127.0.0.1:{self.listener.getsockname()[1]}",
            query=urllib.parse.urlencode(query)))
        self.stop = threading.Event()
        self.arm_cleanup = threading.Event()
        self.cleanup_blocked = threading.Event()
        self.lock = threading.Lock()
        self.sockets, self.threads = [], []
        self.notifications = self.notification_bytes = 0
        self.errors = []
        self.acceptor = threading.Thread(target=self._accept, daemon=True)
        self.acceptor.start()

    def _accept(self):
        while not self.stop.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                upstream = socket.create_connection(self.upstream, timeout=2)
            except OSError as error:
                client.close()
                self.errors.append(type(error).__name__)
                continue
            client.settimeout(0.1)
            upstream.settimeout(0.1)
            blocked = threading.Event()
            with self.lock:
                self.sockets.extend((client, upstream))
            for source, target, outgoing in ((client, upstream, True), (upstream, client, False)):
                relay = threading.Thread(target=self._relay,
                                         args=(source, target, outgoing, blocked), daemon=True)
                self.threads.append(relay)
                relay.start()

    def _relay(self, source, target, outgoing, blocked):
        suffix, frames = b"", bytearray()
        try:
            while not self.stop.is_set():
                try:
                    data = source.recv(64 * 1024)
                except socket.timeout:
                    continue
                if not data:
                    return
                if outgoing:
                    observed = suffix + data
                    if self.arm_cleanup.is_set() and b"UNLISTEN *" in observed:
                        blocked.set()
                        self.cleanup_blocked.set()
                    suffix = observed[-32:]
                received, received_bytes = 0, 0
                if not outgoing and not blocked.is_set():
                    frames.extend(data)
                    while len(frames) >= 5:
                        size = struct.unpack("!I", frames[1:5])[0]
                        if not 4 <= size <= 16 * 1024 * 1024:
                            raise AssertionError("unexpected PostgreSQL auxiliary frame")
                        if len(frames) < size + 1:
                            break
                        if frames[0] == ord("A"):
                            received += 1
                            received_bytes += size + 1
                        del frames[:size + 1]
                if not blocked.is_set():
                    remaining = memoryview(data)
                    while remaining and not self.stop.is_set():
                        try:
                            written = target.send(remaining)
                        except socket.timeout:
                            continue
                        if written == 0:
                            return
                        remaining = remaining[written:]
                    if not remaining:
                        with self.lock:
                            self.notifications += received
                            self.notification_bytes += received_bytes
        except OSError:
            pass  # Expected when an owned process closes or force-exits.
        except AssertionError as error:
            self.errors.append(str(error))
        finally:
            for endpoint in (source, target):
                try:
                    endpoint.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def counters(self):
        with self.lock:
            return dict(notifications=self.notifications, notification_bytes=self.notification_bytes)

    def close(self):
        self.stop.set()
        self.listener.close()
        with self.lock:
            sockets = list(self.sockets)
        for endpoint in sockets:
            try:
                endpoint.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            endpoint.close()
        self.acceptor.join(timeout=2)
        for thread in self.threads:
            thread.join(timeout=2)
        assert not self.acceptor.is_alive() and not any(t.is_alive() for t in self.threads)
        assert not self.errors, self.errors

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


@contextmanager
def held_sql(d, statement):
    """Hold a known lock, proving acquisition through PostgreSQL wait state."""
    tag = "ledgence_resource_" + uuid.uuid4().hex
    process = Process([d.psql, "--dbname", d.database_url, "-X", "-A", "-t",
                       "--set", "ON_ERROR_STOP=1", "--command",
                       f"SET application_name='{tag}'; BEGIN; {statement}; SELECT pg_sleep(90); ROLLBACK"],
                      d.directory, tag, d.environment)
    d.processes.append(process)
    try:
        eventually(lambda: d.sql("SELECT count(*) FROM pg_stat_activity "
                                 f"WHERE application_name='{tag}' AND wait_event='PgSleep'") == "1",
                   timeout=10, description="owned database lock")
        yield
    finally:
        d.sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
              f"WHERE application_name='{tag}' AND datname=current_database()")
        process.process.wait(timeout=10)


def assert_disconnected(future):
    try:
        future.result(timeout=5)
        raise AssertionError("force-exited request unexpectedly returned a completion")
    except (OSError, http.client.HTTPException):
        pass


def force(server):
    began = time.monotonic()
    server.process.send_signal(signal.SIGINT)
    assert server.process.wait(timeout=3) == 1, server.stderr_path
    elapsed = time.monotonic() - began
    assert "forced exit requested" in server.stderr_path.read_text()
    return elapsed


def longpoll_force_during_blocked_finalization(d):
    with servers(d, count=1) as ((server, base),), ThreadPoolExecutor(max_workers=1) as pool:
        opened = session(d, base)
        command = acquisition(d, opened)
        request = pool.submit(poll, base, command)
        waiting(server, opened)
        suffix = uuid.uuid4().hex
        trigger, function = "resource_block_" + suffix, "resource_block_" + suffix
        lock_key = (uuid.uuid4().int % 2_000_000_000) + 1
        # Pending does not UPDATE the cursor. This disposable trigger blocks only
        # the finalization UPDATE, removing any race with periodic Pending probes.
        d.sql(f"""CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.session_id='{opened['id']}' THEN
                PERFORM pg_advisory_xact_lock({lock_key}); END IF; RETURN NEW; END $$;
            CREATE TRIGGER {trigger} BEFORE UPDATE ON consumer_cursors
                FOR EACH ROW EXECUTE FUNCTION {function}()""")
        try:
            with held_sql(d, f"SELECT pg_advisory_xact_lock({lock_key})"):
                server.process.send_signal(signal.SIGTERM)
                eventually(lambda: d.sql("SELECT count(*) FROM pg_locks "
                                         f"WHERE locktype='advisory' AND objid={lock_key} AND NOT granted") == "1",
                           timeout=4, description="accepted finalization blocked in its transaction")
                assert server.process.poll() is None, "first signal abandoned the accepted finalization"
                elapsed = force(server)
                assert_disconnected(request)
            assert d.sql(f"SELECT count(*) FROM consumer_cursors WHERE session_id='{opened['id']}'") == "0"
            replay = poll(d.server_url, dict(command, wait_ms=0))
            assert replay == {"disposition": "empty", "sequence": 1}, replay
        finally:
            d.sql(f"DROP TRIGGER IF EXISTS {trigger} ON consumer_cursors; DROP FUNCTION IF EXISTS {function}()")
    return f"first signal retained a trigger-blocked final cursor transaction; second signal exited in {elapsed:.3f}s; same identity reconciled"


def longpoll_force_during_half_open_auxiliary_cleanup(d):
    before = eventually(lambda: set(d.sql(
        "SELECT pid FROM pg_stat_activity WHERE datname=current_database() "
        "AND application_name='ledgence-wake' AND query LIKE 'LISTEN%'"
    ).splitlines()), description="baseline listener subscription")
    with AuxiliaryProxy(d.database_url) as proxy, servers(
            d, count=1, notification_url=proxy.url) as ((server, base),):
        def listener():
            pids = set(d.sql("SELECT pid FROM pg_stat_activity WHERE datname=current_database() "
                             "AND application_name='ledgence-wake' AND query LIKE 'LISTEN%'").splitlines())
            return pids - before
        listeners = eventually(listener, description="real auxiliary LISTEN subscription")
        assert exchange(base, "GET", "/health/ready")[0] == 200
        proxy.arm_cleanup.set()
        server.process.send_signal(signal.SIGTERM)
        assert proxy.cleanup_blocked.wait(timeout=4), "listener cleanup did not issue UNLISTEN"
        eventually(lambda: "shutdown_still_pending" in server.stderr_path.read_text(),
                   timeout=5, description="retained auxiliary cleanup observation")
        assert server.process.poll() is None, "cleanup observation abandoned its half-open connection"
        elapsed = force(server)
        # The proxy observes the forced process's EOF and releases its owned backend.
        identities = ",".join(sorted(listeners))
        eventually(lambda: d.sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid IN ({identities})") == "0",
                   timeout=5, description="forced auxiliary connection closure")
    return f"real LISTEN connection stalled during UNLISTEN beyond the 3s observation; second signal exited in {elapsed:.3f}s and released backend"


def rss_bytes(pid):
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True).strip()) * 1024


def longpoll_listener_flood_with_stalled_probes(d):
    details = dict(samples=[], notification_target=40_000, memory_growth_limit_bytes=128 * 1024 * 1024)
    with AuxiliaryProxy(d.database_url) as proxy, servers(
            d, count=1, notification_url=proxy.url) as ((server, base),), ThreadPoolExecutor(max_workers=32) as pool:
        queue_prefix = d.queue
        registrations, requests = [], []
        for number in range(4):
            d.queue = f"{queue_prefix}-{number}"
            opened = session(d, base, concurrency=8)
            commands = [acquisition(d, opened, consumer=i) for i in range(8)]
            registrations.append((d.queue, opened, commands))
            requests.extend(pool.submit(exchange, base, "POST", "/v1/acquisitions", command)
                            for command in commands)
            waiting(server, opened, 8)
        control = d.submission("resource-control-" + uuid.uuid4().hex)
        control["input"]["queue"] = queue_prefix + "-control"
        task = d.submit(control, server=base)
        query = urllib.parse.urlencode(dict(d.scope, task_id=task["task_id"]))
        identities = ",".join(f"'{opened['id']}'" for _, opened, _ in registrations)
        listener_count = lambda: d.sql("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                                       "AND application_name='ledgence-wake' AND query LIKE 'LISTEN%'")
        eventually(lambda: int(listener_count()) >= 2, description="real listener before flood")
        with held_sql(d, f"SELECT session_id FROM worker_sessions WHERE session_id IN ({identities}) FOR UPDATE"):
            payloads = [json.dumps(dict(version=1, hint=dict(kind="queue_changed", key=dict(scope=d.scope, queue=queue))))
                        for queue, _, _ in registrations]
            for payload in payloads:
                d.sql(f"SELECT pg_notify('ledgence_acquisition_v1','{payload}')")
            eventually(lambda: d.sql("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                                     "AND application_name='ledgence' AND wait_event_type='Lock' "
                                     "AND query LIKE 'SELECT * FROM worker_sessions%'") == "4",
                       timeout=4, description="four acquisition probes blocked on owned session locks")
            details["rss_before_bytes"] = rss_bytes(server.process.pid)
            before = proxy.counters()
            # Distinct trailing JSON whitespace prevents same-transaction NOTIFY
            # coalescing while every payload still identifies an active queue.
            statements = []
            for batch in range(40):
                payload = payloads[batch % len(payloads)]
                statements.append("BEGIN; DO $$ BEGIN FOR i IN 1..1000 LOOP "
                                  f"PERFORM pg_notify('ledgence_acquisition_v1','{payload}' || repeat(' ',i)); "
                                  "END LOOP; END $$; COMMIT;")
            # This first invalid hint is an end-of-stream receive barrier: the
            # listener diagnoses it only after parsing the preceding valid flood.
            statements.append("SELECT pg_notify('ledgence_acquisition_v1','resource-flood-finished');")
            script = d.directory / "notification-flood.sql"
            script.write_text("\n".join(statements) + "\n")
            flood = Process([d.psql, "--dbname", d.database_url, "-X", "--set", "ON_ERROR_STOP=1",
                             "--command", script.read_text()], d.directory, "notification-flood", d.environment)
            d.processes.append(flood)
            began = time.monotonic()
            try:
                while (flood.process.poll() is None
                       or proxy.counters()["notifications"] - before["notifications"] < details["notification_target"] + 1
                       or "invalid_remote_hint" not in server.stderr_path.read_text()):
                    started = time.monotonic()
                    live = exchange(base, "GET", "/health/live", timeout=1)
                    inspected = exchange(base, "GET", f"/v1/tasks/inspect?{query}", timeout=1)
                    assert live[0] == 200 and inspected[0] == 200
                    assert inspected[2]["task_id"] == task["task_id"]
                    elapsed = time.monotonic() - started
                    details["samples"].append(dict(elapsed_s=time.monotonic() - began,
                                                   rss_bytes=rss_bytes(server.process.pid),
                                                   live_and_inspect_s=elapsed,
                                                   **proxy.counters()))
                    assert time.monotonic() - began < 12, "listener did not consume the bounded notification flood"
                    time.sleep(0.02)
                assert flood.process.wait(timeout=2) == 0, flood.stderr_path
            finally:
                if flood.process.poll() is None:
                    flood.kill()
            details["flood_elapsed_s"] = time.monotonic() - began
            details["receive_barrier_observed"] = "invalid_remote_hint" in server.stderr_path.read_text()
            details["forwarded"] = {key: value - before[key] for key, value in proxy.counters().items()}
            details["peak_rss_bytes"] = max(sample["rss_bytes"] for sample in details["samples"])
            details["rss_growth_bytes"] = details["peak_rss_bytes"] - details["rss_before_bytes"]
            assert details["rss_growth_bytes"] < details["memory_growth_limit_bytes"], details
            assert d.sql(f"SELECT count(*) FROM consumer_cursors WHERE session_id IN ({identities})") == "0"
        server.process.send_signal(signal.SIGTERM)
        replies = [request.result(timeout=8) for request in requests]
        assert all(status == 200 and reply == {"disposition": "empty", "sequence": 1}
                   or status == 503 and reply.get("code") == "unavailable"
                   for status, _, reply in replies), replies
        assert server.process.wait(timeout=8) == 0
        entries = [json.loads(line)["fields"] for line in server.stderr_path.read_text().splitlines()]
        def statistics(message):
            values = [entry["statistics"] for entry in entries if entry.get("message") == message]
            assert len(values) == 1, (message, values)
            return {key: json.loads(value) for key, value in re.findall(
                r"(\w+): (\d+|true|false)", values[0])}
        drained = statistics("acquisition coordinator drained")
        assert all(drained[key] == 0 for key in ("waiters", "keys", "queues", "nominated", "probes")), drained
        assert drained["peak_waiters"] == drained["peak_keys"] == 32, drained
        assert drained["peak_queues"] == drained["peak_probes"] == 4, drained
        assert drained["peak_nominated"] <= 4, drained
        notifications = statistics("acquisition notifications stopped")
        assert notifications["queued"] == 0 and not notifications["listener_connected"], notifications
        assert notifications["malformed"] == 1, notifications
        details["coordinator_drained"] = drained
        details["notifications_stopped"] = notifications
        d.cancel(task["task_id"])
    (d.directory / "notification-flood-resources.json").write_text(json.dumps(details, indent=2) + "\n")
    return (f"40,000 actual notifications across 4 active queues while probes stalled; "
            f"controls responsive, RSS growth {details['rss_growth_bytes']} bytes, notification forwarding {details['flood_elapsed_s']:.3f}s")


SCENARIOS = [longpoll_force_during_blocked_finalization,
             longpoll_force_during_half_open_auxiliary_cleanup,
             longpoll_listener_flood_with_stalled_probes]
