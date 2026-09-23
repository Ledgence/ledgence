"""Real HTTP/database acceptance for acquisition waits and advisory notifications."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import http.client
import signal
import time
import uuid

from .harness import eventually, exchange, unused_port


@contextmanager
def servers(d, count=2, notifications="on", notification_url=None):
    old_environment, old_queue = d.environment, d.queue
    d.environment = dict(d.environment, RUST_LOG="info,ledgence_orchestration_service=debug",
                         LEDGENCE_POSTGRES_NOTIFICATIONS=notifications)
    if notification_url:
        d.environment["LEDGENCE_POSTGRES_NOTIFICATION_URL"] = notification_url
    d.queue = "longpoll-" + uuid.uuid4().hex
    started = []
    try:
        for _ in range(count):
            started.append(d.start_server(unused_port()))
        yield started
    finally:
        for process, _ in reversed(started):
            if process.process.poll() is None:
                process.stop()
        d.environment, d.queue = old_environment, old_queue


def session(d, base, concurrency=1):
    status, _, reply = exchange(base, "POST", "/v1/worker-sessions",
                                dict(scope=d.scope, queue=d.queue, concurrency=concurrency))
    assert status == 200, reply
    return reply


def acquisition(d, opened, consumer=0, sequence=1, wait_ms=20_000):
    return dict(scope=d.scope, queue=d.queue, worker_session_id=opened["id"],
                consumer_id=consumer, sequence=sequence, wait_ms=wait_ms)


def poll(base, command):
    status, _, reply = exchange(base, "POST", "/v1/acquisitions", command)
    assert status == 200, reply
    return reply


def waiting(process, opened, count=1):
    # The first Pending debug event follows the rollback, so this is an actual
    # server-side barrier rather than a client send or a guessed sleep.
    eventually(lambda: sum("acquisition entered wait" in line and opened["id"] in line
                           for line in process.stderr_path.read_text().splitlines()) >= count,
               timeout=10, description="authoritative Pending rollback")


def cancel_all(d, *tasks):
    for task in tasks:
        d.cancel(task["task_id"])


def longpoll_empty_replay_and_remote_completion(d):
    with servers(d) as ((first, a), (_, b)), ThreadPoolExecutor(max_workers=2) as pool:
        opened = session(d, a)
        command = acquisition(d, opened)
        request = pool.submit(poll, a, command)
        waiting(first, opened)
        assert d.sql(f"SELECT count(*) FROM consumer_cursors WHERE session_id='{opened['id']}'") == "0"
        assert d.sql(f"SELECT count(*) FROM tasks WHERE queue='{d.queue}'") == "0"
        began = time.monotonic()
        completed = poll(b, dict(command, wait_ms=0))
        assert completed == {"disposition": "empty", "sequence": 1}
        assert request.result(timeout=5) == completed
        assert time.monotonic() - began < 5, "cross-server completion was deferred to the20s wait deadline"
        task = d.submit(d.submission("longpoll-after-empty"), server=b)
        assert poll(a, command) == completed
        successor = poll(a, dict(command, sequence=2, wait_ms=0))
        assert successor["disposition"] == "assigned", successor
        assert successor["assignment"]["event"]["data"] == task["input"]["data"]
        status, _, error = exchange(a, "POST", "/v1/acquisitions", command)
        assert status == 409 and error["code"] == "obsolete_operation", error
        cancel_all(d, task)
    return "Pending leaves no cursor; remote Empty wakes duplicate; changed wait replays; successor claims and old sequence is obsolete"


def longpoll_two_servers_share_first_assignment(d):
    with servers(d) as ((_, a), (_, b)), ThreadPoolExecutor(max_workers=2) as pool:
        opened = session(d, a)
        tasks = [d.submit(d.submission("longpoll-first-" + str(i)), server=a) for i in range(2)]
        command = acquisition(d, opened)
        futures = [pool.submit(poll, base, command) for base in (a, b)]
        replies = [future.result(timeout=10) for future in futures]
        assert all(reply["disposition"] == "assigned" for reply in replies), replies
        assignments = [reply["assignment"] for reply in replies]
        assert assignments[0]["event"] == assignments[1]["event"]
        assert assignments[0]["lease"] == assignments[1]["lease"]
        assert d.sql(f"SELECT sum(attempt_count) FROM tasks WHERE queue='{d.queue}'") == "1"
        first_remaining = assignments[1]["authority"]["remaining_ms"]
        # Waiting here intentionally measures a newly sampled authority value.
        time.sleep(0.05)
        replay = poll(b, dict(command, wait_ms=0))["assignment"]
        assert replay["event"] == assignments[0]["event"]
        assert replay["authority"]["remaining_ms"] < first_remaining
        cancel_all(d, *tasks)
    return "two replicas racing a brand-new cursor commit one attempt and replay immutable event with fresh authority"


def longpoll_fallback_and_listener_startup_failure(d):
    # An external submission reaches a different server. With no receive path,
    # only this server's periodic nominated probe can discover it.
    details = []
    for notifications, url in [("off", None), ("on", "postgres://postgres:ledgence-test@127.0.0.1:1/absent")]:
        with servers(d, count=1, notifications=notifications, notification_url=url) as ((server, base),), ThreadPoolExecutor(max_workers=1) as pool:
            opened = session(d, base)
            request = pool.submit(poll, base, acquisition(d, opened))
            waiting(server, opened)
            began = time.monotonic()
            task = d.submit(d.submission("longpoll-fallback-" + notifications))
            reply = request.result(timeout=3)
            elapsed = time.monotonic() - began
            assert reply["disposition"] == "assigned", reply
            assert reply["assignment"]["event"]["ldgtaskid"] == task["task_id"]
            assert elapsed < 2, elapsed
            cancel_all(d, task)
            details.append(round(elapsed, 3))
    return f"periodic-only and unavailable listener remain ready; external submissions picked up in {details}s"


def released_transactions(d, requests, timeout=3):
    """Observe a parked cohort without confusing a transient probe with a leak."""
    count = None

    def released():
        nonlocal count
        assert all(not request.done() for request in requests), "acquisition completed during transaction-release observation"
        count = d.sql("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                      "AND application_name='ledgence' AND state='idle in transaction'")
        assert all(not request.done() for request in requests), "acquisition completed during transaction-release observation"
        return count == "0"

    try:
        eventually(released, timeout=timeout, description="all parked acquisitions to release transactions")
    except AssertionError as error:
        raise AssertionError(f"{error}; last idle transaction count: {count}") from error


def longpoll_many_sleepers_release_connections_and_drain(d):
    auxiliary_query = ("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                       "AND application_name='ledgence-wake'")
    # The original server's publisher connects lazily; a selected scenario may
    # start with only its listener, while earlier scenarios warm both connections.
    auxiliary_before = d.sql(auxiliary_query)
    with servers(d, count=1) as ((server, base),), ThreadPoolExecutor(max_workers=16) as pool:
        opened = session(d, base, 16)
        started = time.monotonic()
        requests = [pool.submit(poll, base, acquisition(d, opened, consumer=i)) for i in range(16)]
        waiting(server, opened, 16)
        cursors = d.sql(f"SELECT count(*) FROM consumer_cursors WHERE session_id='{opened['id']}'")
        assert cursors == "0", f"parked acquisitions persisted {cursors} consumer cursors"
        # A new periodic probe or the independent expiry scanner can briefly be
        # between SQL statements after the Pending barrier. A persistent held
        # transaction must still fail while every acquisition remains parked.
        released_transactions(d, requests)
        extension = exchange(base, "POST", "/v1/worker-sessions/extend", {"worker_session_id": opened["id"]})
        assert extension[0] == 200, extension
        assert all(not request.done() for request in requests), "acquisition completed before shutdown"
        # Reserve the full shutdown allowance before the earliest possible 20s
        # poll deadline, so deadline expiry cannot masquerade as signal wakeup.
        assert time.monotonic() - started < 12, "transaction observation exhausted the shutdown test budget"
        began = time.monotonic()
        server.process.send_signal(signal.SIGTERM)
        replies = [request.result(timeout=8) for request in requests]
        assert all(reply == {"disposition": "empty", "sequence": 1} for reply in replies), replies
        code = server.process.wait(timeout=8)
        assert code == 0, f"long-poll server exited with {code} during graceful shutdown"
        assert time.monotonic() - began < 8, "accepted idle waits did not wake into finalization"
        finalized = d.sql(f"SELECT count(*) FROM consumer_cursors WHERE session_id='{opened['id']}' AND sequence=1")
        assert finalized == "16", f"shutdown finalized {finalized} of 16 consumer cursors"
        # Owned listener/publisher connections have closed before process exit.
        auxiliary_after = d.sql(auxiliary_query)
        assert auxiliary_after == auxiliary_before, (f"auxiliary connections after shutdown: {auxiliary_after}; "
                                                    f"original server baseline: {auxiliary_before}")
    return "16 sleepers hold no transaction; control progresses; first signal finalizes all cursors and closes auxiliary connections"


def longpoll_lost_empty_response_reconciles(d):
    with servers(d, count=1) as ((server, base),), ThreadPoolExecutor(max_workers=1) as pool:
        opened = session(d, base)
        proxy = d.proxy(base)
        lost = proxy.lose_once("/v1/acquisitions")
        command = acquisition(d, opened, wait_ms=250)
        # The fault proxy consumes the full committed response before closing.
        future = pool.submit(exchange, proxy.url, "POST", "/v1/acquisitions", command)
        waiting(server, opened)
        try:
            future.result(timeout=5)
            raise AssertionError("expected a dropped committed Empty reply")
        except (OSError, http.client.HTTPException):
            pass
        assert lost.is_set()
        task = d.submit(d.submission("longpoll-empty-lost"), server=base)
        assert poll(base, dict(command, wait_ms=0)) == {"disposition": "empty", "sequence": 1}
        successor = poll(base, dict(command, sequence=2, wait_ms=0))
        assert successor["assignment"]["event"]["ldgtaskid"] == task["task_id"]
        cancel_all(d, task)
    return "committed Empty survives response loss and later submission; same sequence reconciles before successor"


SCENARIOS = [longpoll_empty_replay_and_remote_completion, longpoll_two_servers_share_first_assignment,
             longpoll_fallback_and_listener_startup_failure,
             longpoll_many_sleepers_release_connections_and_drain, longpoll_lost_empty_response_reconciles]
