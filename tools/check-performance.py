"""Bounded local HTTP/Python performance experiment; never a daily capacity qualification."""

import argparse
from bisect import bisect_left
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse
import uuid

from http_acceptance.harness import Deployment, eventually
from http_acceptance.sqs import SqsDeployment

MAX_TASKS = 250_000
MAX_INPUT_BYTES = 2 * 1024**3
RESPONSE_LIMIT = 1024 * 1024
STATEMENT_LIMIT = 256
DELIVERY_CONFIG_LIMIT = 16 * 1024
PROGRAM = '''import os, time
from pathlib import Path

def handle(event):
    data = event['data']
    if 'warm_directory' in data:
        directory = Path(data['warm_directory'])
        (directory / str(os.getpid())).touch()
        deadline = time.monotonic() + 60
        while not (directory / 'release').exists():
            if time.monotonic() >= deadline:
                raise RuntimeError('warmup barrier expired')
            time.sleep(0.01)
    start = time.monotonic_ns()
    time.sleep(data['work_ms'] / 1000)
    return {'pid': os.getpid(), 'work_elapsed_ms': (time.monotonic_ns() - start) / 1000000}
'''


class Histogram:
    """Constant-memory latency buckets. Quantiles are upper bounds, not interpolations."""
    def __init__(self):
        self.bounds = [0.0] + [0.01 * 1.1**i for i in range(200)]
        self.counts = [0] * (len(self.bounds) + 1)
        self.count, self.total, self.minimum, self.maximum = 0, 0.0, None, None

    def add(self, value):
        if not math.isfinite(value) or value < 0:
            raise ValueError('latency must be finite and nonnegative')
        self.counts[bisect_left(self.bounds, value)] += 1
        self.count += 1
        self.total += value
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)

    def summary(self):
        def quantile(fraction):
            target, total = math.ceil(self.count * fraction), 0
            for index, count in enumerate(self.counts):
                total += count
                if target and total >= target:
                    return self.bounds[index] if index < len(self.bounds) else self.maximum
            return None
        return {'count': self.count, 'min': self.minimum, 'max': self.maximum,
                'mean': self.total / self.count if self.count else None,
                'p50_upper': quantile(.5), 'p95_upper': quantile(.95), 'p99_upper': quantile(.99),
                'unit': 'ms', 'quantile_method': 'fixed buckets, 10% relative spacing'}


def phases(args):
    """Piecewise-constant offered rate; the runner accounts for lateness explicitly."""
    if not args.burst_duration:
        return [(0.0, args.duration, args.rate)]
    return [(0.0, args.burst_at, args.rate),
            (args.burst_at, args.burst_at + args.burst_duration, args.rate * args.burst_multiplier),
            (args.burst_at + args.burst_duration, args.duration, args.rate)]


def arrivals(args):
    for start, end, rate in phases(args):
        for index in range(math.ceil(max(0.0, end - start) * rate)):
            offset = start + index / rate
            if offset < end:
                yield offset


def validate(args):
    limits = {'rate': (.1, 5000), 'duration': (.1, 3600), 'burst_multiplier': (1, 20),
              'burst_duration': (0, 120), 'burst_at': (0, 3600), 'work_ms': (0, 60_000),
              'concurrency': (1, 128), 'submitters': (1, 64), 'payload_bytes': (0, 256 * 1024),
              'drain_timeout': (1, 600), 'request_timeout': (1, 30),
              'sample_interval': (1, 60), 'max_lateness_ms': (1, 1000)}
    for name, (low, high) in limits.items():
        value = getattr(args, name)
        if not math.isfinite(value) or not low <= value <= high:
            raise ValueError(f'{name} must be finite and between {low} and {high}')
    if args.burst_duration and args.burst_at + args.burst_duration > args.duration:
        raise ValueError('burst must fit entirely inside the measurement duration')
    if args.burst_duration and args.rate * args.burst_multiplier > 5000:
        raise ValueError('peak offered rate exceeds 5000/s cap')
    count = sum(math.ceil(max(0.0, end - start) * rate) for start, end, rate in phases(args))
    if count > MAX_TASKS:
        raise ValueError(f'offered arrivals exceed {MAX_TASKS} cap')
    if count * (args.payload_bytes + 1024) > MAX_INPUT_BYTES:
        raise ValueError('estimated aggregate submission input exceeds 2 GiB cap')
    if args.delivery_config and not args.disposable_queue:
        raise ValueError('--delivery-config requires --disposable-queue for a fresh, dedicated test queue')
    if args.disposable_queue and not args.delivery_config:
        raise ValueError('--disposable-queue requires --delivery-config')
    return count


def inspect_delivery_config(path):
    """Bound the read before acquiring owned resources; Rust validates the full schema."""
    with path.open('rb') as source:
        body = source.read(DELIVERY_CONFIG_LIMIT + 1)
    if len(body) > DELIVERY_CONFIG_LIMIT:
        raise ValueError('delivery config exceeds 16 KiB')
    def unique_fields(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError('delivery config contains duplicate fields')
            value[key] = item
        return value
    value = json.loads(body, object_pairs_hook=unique_fields)
    try:
        route = value['route']
        fields = [route['queue'], route['scope']['tenant_id'], route['scope']['namespace']]
        if not all(isinstance(item, str) and item for item in fields):
            raise ValueError('invalid delivery route')
    except (KeyError, TypeError) as error:
        raise ValueError('invalid delivery route') from error
    return hashlib.sha256(body).hexdigest()


class Submitter:
    """One reusable connection per executor thread, with no mutation retries."""
    def __init__(self, base, timeout):
        parsed = urllib.parse.urlsplit(base)
        self.host, self.port, self.timeout = parsed.hostname, parsed.port, timeout
        self.local, self.connections, self.lock = threading.local(), [], threading.Lock()

    def __call__(self, command):
        start = time.monotonic()
        connection = getattr(self.local, 'connection', None)
        if connection is None:
            connection = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
            self.local.connection = connection
            with self.lock:
                self.connections.append(connection)
        body = json.dumps(command, separators=(',', ':')).encode()
        try:
            connection.request('POST', '/v1/tasks', body,
                               {'Content-Type': 'application/json', 'Accept': 'application/json'})
            response = connection.getresponse()
            payload = response.read(RESPONSE_LIMIT + 1)
            if len(payload) > RESPONSE_LIMIT:
                raise ValueError('submission response exceeds harness limit')
            if response.status != 200:
                return {'kind': 'http_error', 'status': response.status,
                        'latency_ms': (time.monotonic() - start) * 1000}
            value = json.loads(payload)
            if not isinstance(value, dict) or not isinstance(value.get('task_id'), str):
                raise ValueError('invalid successful submission response')
            if value.get('idempotency_key') != command['idempotency_key']:
                raise ValueError('submission response identity mismatch')
            return {'kind': 'accepted', 'task_id': value['task_id'],
                    'latency_ms': (time.monotonic() - start) * 1000}
        except (OSError, http.client.HTTPException, ValueError) as error:
            connection.close()
            # Reuse this closed object; HTTPConnection reconnects lazily. The
            # connection registry therefore stays bounded by submitter threads.
            return {'kind': 'uncertain', 'error_type': type(error).__name__,
                    'latency_ms': (time.monotonic() - start) * 1000}

    def close(self):
        for connection in self.connections:
            connection.close()


def command(deployment, index, args, warm_directory=None):
    data = {'work_ms': args.work_ms if warm_directory is None else 0,
            'padding': 'x' * args.payload_bytes if warm_directory is None else ''}
    if warm_directory is not None:
        data['warm_directory'] = str(warm_directory)
    return {'idempotency_key': ('measure-' if warm_directory is None else 'warm-') + str(index),
            'input': dict(deployment.scope, queue=deployment.queue,
                          program={'id': 'performance', 'version': '1.0.0'},
                          correlation_key='measurement' if warm_directory is None else 'warmup',
                          retry_policy={'max_attempts': 1, 'retry_delay_ms': 0}, data=data)}


def run_load(args, submit, build_command, check_health, journal):
    counters = {'offered': 0, 'sent': 0, 'http_accepted': 0, 'http_error': 0,
                'uncertain': 0, 'dropped_capacity': 0, 'dropped_late': 0, 'max_in_flight': 0}
    response_latency, lateness = Histogram(), Histogram()
    pending = set()
    start = time.monotonic()

    def collect(done):
        for future in done:
            # Unexpected background exceptions must abort instead of silently
            # becoming a successful low-throughput result.
            result = future.result()
            kind = result['kind']
            counters['http_accepted' if kind == 'accepted' else kind] += 1
            response_latency.add(result['latency_ms'])
            journal.write(json.dumps(result, separators=(',', ':')) + '\n')
            pending.remove(future)

    with ThreadPoolExecutor(max_workers=args.submitters) as pool:
        for index, offset in enumerate(arrivals(args)):
            check_health()
            while (remaining := start + offset - time.monotonic()) > 0:
                time.sleep(min(remaining, .05))
                check_health()
                collect({f for f in pending if f.done()})
            collect({f for f in pending if f.done()})
            counters['offered'] += 1
            delay = max(0.0, time.monotonic() - start - offset) * 1000
            lateness.add(delay)
            if delay > args.max_lateness_ms:
                counters['dropped_late'] += 1
            elif len(pending) >= args.submitters:
                counters['dropped_capacity'] += 1
            else:
                future = pool.submit(submit, build_command(index))
                pending.add(future)
                counters['sent'] += 1
                counters['max_in_flight'] = max(counters['max_in_flight'], len(pending))
        # Preserve the configured measurement window even if its final arrival
        # occurs early (for example a low-rate short run).
        while time.monotonic() < start + args.duration:
            check_health()
            collect({f for f in pending if f.done()})
            time.sleep(max(0.0, min(.05, start + args.duration - time.monotonic())))
        while pending:
            check_health()
            done, _ = wait(pending, timeout=.05, return_when=FIRST_COMPLETED)
            collect(done)
    return dict(counters, configured_duration_seconds=args.duration,
                submission_and_response_seconds=time.monotonic() - start,
                response_latency_ms=response_latency.summary(), generator_lateness_ms=lateness.summary())


def sql_json(deployment, statement, timeout=5):
    environment = dict(deployment.environment, PGCONNECT_TIMEOUT='2',
                       PGOPTIONS=f'-c statement_timeout={int((timeout - 1) * 1000)} -c lock_timeout=1000')
    result = subprocess.run([deployment.psql, '--dbname', deployment.database_url, '-X', '-A', '-t', '-q',
                             '--set', 'ON_ERROR_STOP=1', '--command', statement],
                            env=environment, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError('performance observation SQL failed')
    return json.loads(result.stdout)


CENSUS = """SELECT json_build_object(
    'database_at_ms',floor(extract(epoch FROM statement_timestamp())*1000),
    'durable_accepted',count(*),
    'succeeded',count(*) FILTER (WHERE state='succeeded'),
    'failed',count(*) FILTER (WHERE state='failed'),
    'cancelled',count(*) FILTER (WHERE state='cancelled'),
    'pending',count(*) FILTER (WHERE state IN ('queued','active')),
    'attempts',coalesce(sum(attempt_count),0))
    FROM tasks WHERE correlation_key='measurement'"""

LATENCIES = """WITH history AS (
    SELECT h.task_id,
      min(h.at_ms) FILTER (WHERE h.reason='claimed') AS claimed,
      min(h.at_ms) FILTER (WHERE h.reason='dispatch_authorized') AS dispatched
    FROM task_history h JOIN tasks t USING(task_id)
    WHERE t.correlation_key='measurement' GROUP BY h.task_id
), observations AS (
    SELECT t.terminal_at_ms-t.submitted_at_ms AS accepted_to_terminal,
      h.claimed-t.submitted_at_ms AS accepted_to_first_claim,
      h.dispatched-h.claimed AS first_claim_to_dispatch_authorized
    FROM tasks t LEFT JOIN history h USING(task_id)
    WHERE t.correlation_key='measurement'
), metrics AS (
    SELECT metric, json_build_object('count',count(value),'min',min(value),'max',max(value),
      'mean',avg(value),'p50',percentile_cont(.50) WITHIN GROUP (ORDER BY value),
      'p95',percentile_cont(.95) WITHIN GROUP (ORDER BY value),
      'p99',percentile_cont(.99) WITHIN GROUP (ORDER BY value),
      'negative_samples',count(*) FILTER (WHERE value < 0),'unit','ms') AS summary
    FROM observations CROSS JOIN LATERAL (VALUES
      ('accepted_to_terminal', accepted_to_terminal),
      ('accepted_to_first_claim', accepted_to_first_claim),
      ('first_claim_to_dispatch_authorized', first_claim_to_dispatch_authorized)) v(metric,value)
    GROUP BY metric
) SELECT coalesce(json_object_agg(metric,summary),'{}'::json) FROM metrics"""


# The two snapshots never reset shared statistics. They filter this experiment's
# unique database and bound both query count and normalized query text in evidence.
STATEMENTS = f"""WITH entries AS MATERIALIZED (
    SELECT userid::text AS userid,queryid::text AS queryid,toplevel,calls,rows,
      wal_bytes,total_exec_time,stats_since,left(query,2048) AS query
    FROM pg_stat_statements
    WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
) SELECT json_build_object(
    'observed_at',statement_timestamp(),
    'stats_reset',(SELECT stats_reset FROM pg_stat_statements_info),
    'deallocations',(SELECT dealloc FROM pg_stat_statements_info),
    'entry_count',(SELECT count(*) FROM entries),
    'entry_limit',{STATEMENT_LIMIT},
    'entries',coalesce((SELECT json_agg(e) FROM (
      SELECT * FROM entries ORDER BY total_exec_time DESC,userid,queryid,toplevel
      LIMIT {STATEMENT_LIMIT}) e),'[]'::json))"""


def statement_delta(before, after):
    counters = ('calls', 'rows', 'wal_bytes', 'total_exec_time')
    def key(entry):
        return entry['userid'], entry['queryid'], entry['toplevel']
    prior = {key(entry): entry for entry in before['entries']}
    current = {key(entry): entry for entry in after['entries']}
    reasons = []
    if before['stats_reset'] != after['stats_reset']:
        reasons.append('shared_statistics_reset')
    if before['deallocations'] != after['deallocations']:
        reasons.append('shared_statistics_eviction')
    if any(snapshot['entry_count'] != len(snapshot['entries']) for snapshot in (before, after)):
        reasons.append('query_entry_limit_exceeded')
    if len(prior) != len(before['entries']) or len(current) != len(after['entries']):
        reasons.append('duplicate_query_identity')
    if prior.keys() - current.keys():
        reasons.append('query_entries_disappeared')
    entries = []
    for identity, entry in current.items():
        previous = prior.get(identity, {})
        if previous and previous['stats_since'] != entry['stats_since']:
            reasons.append('query_statistics_reset')
        delta = {counter: entry[counter] - previous.get(counter, 0) for counter in counters}
        if any(value < 0 for value in delta.values()):
            reasons.append('query_counters_decreased')
        if any(delta.values()):
            entries.append(dict(delta, userid=entry['userid'], queryid=entry['queryid'],
                                toplevel=entry['toplevel'], query=entry['query']))
    entries.sort(key=lambda entry: entry['total_exec_time'], reverse=True)
    complete = not reasons
    return {'complete': complete, 'invalidity_reasons': sorted(set(reasons)),
            'start': before['observed_at'], 'end': after['observed_at'],
            'entry_limit': STATEMENT_LIMIT, 'query_text_limit': 2048,
            'totals': {counter: sum(entry[counter] for entry in entries)
                       for counter in counters} if complete else None,
            'entries': entries,
            'note': 'Deltas include this database only, from after warmup until load replies finish. '
                    'They include census and the first snapshot query, plus idle worker/publisher queries. '
                    'The final snapshot records its own cost after reading these counters. '
                    'No global statistics reset is issued; incomplete deltas are not valid totals.'}


class Sampler:
    def __init__(self, deployment, interval, path):
        self.deployment, self.interval, self.path = deployment, interval, path
        self.stop = threading.Event()
        self.error = None
        self.latest = None
        self.start = time.monotonic()
        self.count = 0
        self.thread = threading.Thread(target=self.run, name='performance-census')

    def run(self):
        try:
            with self.path.open('w') as output:
                while not self.stop.is_set():
                    begin = time.monotonic()
                    value = sql_json(self.deployment, CENSUS)
                    value.update(elapsed_seconds=time.monotonic() - self.start,
                                 observation_seconds=time.monotonic() - begin)
                    self.latest = value
                    self.count += 1
                    output.write(json.dumps(value) + '\n')
                    output.flush()
                    self.stop.wait(self.interval)
        except BaseException as error:
            self.error = error

    def close(self):
        self.stop.set()
        self.thread.join(timeout=7)
        if self.thread.is_alive():
            raise RuntimeError('performance sampler did not stop')
        self.check()

    def check(self):
        if self.error is not None:
            raise RuntimeError('background performance sampler failed') from self.error


def warm(deployment, args, submit):
    directory = deployment.directory / 'warm-starts'
    directory.mkdir()
    try:
        with ThreadPoolExecutor(max_workers=args.submitters) as pool:
            # N is capped at 128: the warmup future list has a fixed upper bound.
            futures = [pool.submit(submit, command(deployment, i, args, directory))
                       for i in range(args.concurrency)]
            for future in futures:
                if future.result()['kind'] != 'accepted':
                    raise RuntimeError('warmup submission failed')
        eventually(lambda: len(list(directory.iterdir())) == args.concurrency,
                   timeout=45, description='all N Python subprocesses at warmup barrier')
    finally:
        (directory / 'release').touch()
    pids = sorted(int(p.name) for p in directory.iterdir() if p.name != 'release')
    def completed():
        result = sql_json(deployment, "SELECT json_build_object('succeeded',count(*) FILTER "
                          "(WHERE state='succeeded'),'terminal',count(*) FILTER "
                          "(WHERE state IN ('succeeded','failed','cancelled'))) FROM tasks "
                          "WHERE correlation_key='warmup'")
        if result['terminal'] == args.concurrency and result['succeeded'] != args.concurrency:
            raise RuntimeError('warmup program failed')
        return result['succeeded'] == args.concurrency
    eventually(completed, timeout=45, description='warmup completion')
    return {'subprocess_pids': pids, 'concurrency': args.concurrency,
            'artifact_downloads': deployment.artifacts.downloads()}


def digest(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def metadata(root, args, binaries):
    def output(command):
        return subprocess.check_output(command, cwd=root, text=True, timeout=15).strip()
    settings = {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()}
    memory = None
    if hasattr(os, 'sysconf'):
        try:
            memory = os.sysconf('SC_PHYS_PAGES') * os.sysconf('SC_PAGE_SIZE')
        except (ValueError, OSError):
            pass
    return {'schema_version': 1, 'experiment': 'local HTTP + real reusable Python worker',
            'delivery_mode': 'sqs' if args.delivery_config else 'integrated-postgres',
            'daily_capacity_qualified': False,
            'workspace_commit': output(['git', 'rev-parse', 'HEAD']),
            'binary_provenance': 'externally built; source commit/profile not verified' if args.binaries else 'built from this workspace by this invocation',
            'working_tree_dirty': bool(output(['git', 'status', '--porcelain'])),
            'platform': platform.platform(), 'architecture': platform.machine(),
            'logical_cpus': os.cpu_count(), 'physical_memory_bytes': memory,
            'python_harness': sys.version, 'settings': settings,
            'binaries': {name: {'path': str(binaries / name), 'sha256': digest(binaries / name)}
                         for name in ('ledgence', 'ledgence-worker', 'ledgence-orchestrator')},
            'caps': {'arrivals': MAX_TASKS, 'estimated_total_input_bytes': MAX_INPUT_BYTES,
                     'pending_http': args.submitters, 'response_bytes': RESPONSE_LIMIT},
            'limitations': ['Fresh local database; no retention, replication, or failover qualification.',
                            'HTTP responses use bounded socket waits; no submission retries.',
                            'SQL census shares the tested database and adds observer load.',
                            'Database wall-clock intervals differ from monotonic client latency.',
                            'Dispatch authorization is not actual Python start.',
                            'Sleeping work models waiting, not CPU-bound Python computation.']}


def make_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--self-test', action='store_true')
    parser.add_argument('--disposable-postgres', action='store_true',
                        help='confirm LEDGENCE_POSTGRES_URL is an explicitly disposable test server')
    parser.add_argument('--delivery-config', type=Path,
                        help='shared SQS JSON configuration passed unchanged to server and worker')
    parser.add_argument('--disposable-queue', action='store_true',
                        help='confirm the configured queue is fresh, empty, dedicated, and disposable')
    parser.add_argument('--pg-stat-statements', action='store_true',
                        help='create extension in the owned DB and record bounded query counter deltas')
    parser.add_argument('--psql', default='psql')
    parser.add_argument('--binaries', type=Path, help='prebuilt binaries; --profile describes their build')
    parser.add_argument('--profile', choices=['debug', 'release'], default='release')
    parser.add_argument('--evidence', type=Path, help='new directory; default retained temporary directory')
    parser.add_argument('--rate', type=float, default=5_000_000 / 86400)
    parser.add_argument('--duration', type=float, default=60)
    parser.add_argument('--burst-at', type=float, default=20)
    parser.add_argument('--burst-duration', type=float, default=0)
    parser.add_argument('--burst-multiplier', type=float, default=10)
    parser.add_argument('--work-ms', type=float, default=100)
    parser.add_argument('--payload-bytes', type=int, default=1024)
    parser.add_argument('--concurrency', type=int, default=16)
    parser.add_argument('--submitters', type=int, default=16)
    parser.add_argument('--request-timeout', type=float, default=10)
    parser.add_argument('--drain-timeout', type=float, default=120)
    parser.add_argument('--sample-interval', type=float, default=5)
    parser.add_argument('--max-lateness-ms', type=float, default=50)
    return parser


def main():
    if sys.version_info < (3, 11):
        raise SystemExit('CPython >=3.11 is required')
    parser = make_parser()
    args = parser.parse_args()
    if args.self_test:
        return 0 if unittest.TextTestRunner(verbosity=2).run(
            unittest.defaultTestLoader.loadTestsFromTestCase(SelfTests)).wasSuccessful() else 1
    try:
        expected = validate(args)
        config_digest = inspect_delivery_config(args.delivery_config) if args.delivery_config else None
    except (ValueError, OSError) as error:
        parser.error(str(error))
    parent_url = os.environ.get('LEDGENCE_POSTGRES_URL')
    if not args.disposable_postgres or not parent_url:
        parser.error('--disposable-postgres and LEDGENCE_POSTGRES_URL are both required')
    parsed = urllib.parse.urlsplit(parent_url)
    if parsed.scheme not in ('postgres', 'postgresql') or not parsed.hostname:
        parser.error('LEDGENCE_POSTGRES_URL must be a PostgreSQL URL with a hostname')
    # A query-string dbname could override the owned path. Reject it before any DDL.
    if any(key.lower() in ('dbname', 'database') for key, _ in urllib.parse.parse_qsl(parsed.query)):
        parser.error('database overrides in PostgreSQL URL query parameters are not allowed')
    root = Path(__file__).resolve().parents[1]
    binaries = args.binaries
    if binaries is None:
        build = ['cargo', 'build', '--workspace', '--bins', '--locked']
        if args.delivery_config:
            build.extend(['--features', 'ledgence-worker/sqs,ledgence-orchestrator/sqs'])
        if args.profile == 'release':
            build.append('--release')
        subprocess.run(build, cwd=root, check=True)
        cargo_metadata = json.loads(subprocess.check_output(
            ['cargo', 'metadata', '--no-deps', '--format-version', '1', '--locked'], cwd=root))
        binaries = Path(cargo_metadata['target_directory']) / args.profile
    binaries = binaries.resolve()
    for name in ('ledgence', 'ledgence-worker', 'ledgence-orchestrator'):
        if not (binaries / name).is_file():
            parser.error(f'missing binary {binaries / name}')
    if args.evidence:
        args.evidence.mkdir(parents=True, exist_ok=False)
        directory = args.evidence.resolve()
    else:
        directory = Path(tempfile.mkdtemp(prefix='ledgence-performance-'))
    print(f'Evidence: {directory}', flush=True)
    report = metadata(root, args, binaries)
    report.update(result='failed', expected_arrivals=expected, delivery_config_sha256=config_digest)
    (directory / 'metadata.json').write_text(json.dumps(report, indent=2) + '\n')
    database = 'ledgence_perf_' + uuid.uuid4().hex
    database_url = urllib.parse.urlunsplit(parsed._replace(path='/' + database))
    deployment, sampler, submitter, created = None, None, None, False
    failures = []
    interrupted = False

    def admin(statement):
        environment = dict(os.environ, PGCONNECT_TIMEOUT='3', PGOPTIONS='-c statement_timeout=30000')
        result = subprocess.run([args.psql, '--dbname', parent_url, '-X', '--set', 'ON_ERROR_STOP=1',
                                 '--command', statement], env=environment, capture_output=True, timeout=35)
        if result.returncode:
            raise RuntimeError('owned performance database setup/cleanup failed')

    def interrupt(_signum, _frame):
        nonlocal interrupted
        interrupted = True

    stage = 'database_setup'
    old_handlers = {sig: signal.signal(sig, interrupt) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        # Cleanup owns this unique name even if CREATE commits but its reply is lost.
        created = True
        admin(f'CREATE DATABASE "{database}"')
        stage = 'deployment_setup'
        deployment_type = SqsDeployment if args.delivery_config else Deployment
        extra = {'delivery_config': args.delivery_config} if args.delivery_config else {}
        deployment = deployment_type(root, directory, binaries,
                                     os.environ.get('LEDGENCE_PYTHON', sys.executable),
                                     database_url, args.psql, **extra)
        deployment.environment = {key: value for key, value in deployment.environment.items()
                                  if not key.startswith(('OTEL_', 'LEDGENCE_POSTGRES_NOTIFICATION'))}
        deployment.environment['RUST_LOG'] = 'warn'
        deployment.publish('performance', '1.0.0', program_source=PROGRAM)
        stage = 'migration'
        with (directory / 'migration.stdout').open('wb') as out, (directory / 'migration.stderr').open('wb') as err:
            subprocess.run([str(binaries / 'ledgence-orchestrator'), 'migrate'],
                           env=deployment.environment, stdout=out, stderr=err, check=True, timeout=650)
        report['postgres'] = sql_json(deployment, "SELECT json_build_object('version',version(),"
                                    "'max_connections',current_setting('max_connections'),"
                                    "'shared_buffers',current_setting('shared_buffers'),"
                                    "'fsync',current_setting('fsync'),'synchronous_commit',"
                                    "current_setting('synchronous_commit'),'full_page_writes',"
                                    "current_setting('full_page_writes'))")
        if args.pg_stat_statements:
            stage = 'query_statistics_setup'
            sql_json(deployment, 'CREATE EXTENSION pg_stat_statements; SELECT to_json(true)')
        stage = 'server_and_worker_startup'
        deployment.server, _ = deployment.start_server()
        worker = deployment.start_worker(concurrency=args.concurrency)
        submitter = Submitter(deployment.server_url, args.request_timeout)
        stage = 'warmup'
        report['warmup'] = warm(deployment, args, submitter)
        submitter.close()
        submitter = Submitter(deployment.server_url, args.request_timeout)
        print(f'Warmup complete: {args.concurrency} Python subprocesses', flush=True)
        stage = 'measurement'
        statement_before = sql_json(deployment, STATEMENTS) if args.pg_stat_statements else None
        if statement_before:
            (directory / 'pg-stat-statements-before.json').write_text(json.dumps(statement_before, indent=2) + '\n')
        sampler = Sampler(deployment, args.sample_interval, directory / 'census.jsonl')
        sampler.thread.start()
        def health():
            if interrupted:
                raise KeyboardInterrupt
            sampler.check()
            for process in deployment.processes:
                if process.process.poll() is not None:
                    raise RuntimeError(f'{process.label} exited during measurement')
        database_start = sql_json(deployment, "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint")
        measurement_start = time.monotonic()
        report['database_completion_window'] = {'start_ms': database_start,
            'end_ms': database_start + round(args.duration * 1000),
            'note': 'Database clock sampled immediately before starting the arrival generator.'}
        with (directory / 'submissions.jsonl').open('w') as journal:
            report['load'] = run_load(args, submitter, lambda i: command(deployment, i, args), health, journal)
        if args.pg_stat_statements:
            statement_after = sql_json(deployment, STATEMENTS)
            (directory / 'pg-stat-statements-after.json').write_text(json.dumps(statement_after, indent=2) + '\n')
            report['pg_stat_statements'] = statement_delta(statement_before, statement_after)
            if not report['pg_stat_statements']['complete']:
                failures.append('query_statistics_incomplete')
        stage = 'drain'
        deadline = time.monotonic() + args.drain_timeout
        while True:
            health()
            census = sql_json(deployment, CENSUS)
            if 'load_end_census' not in report:
                report['load_end_census'] = census
                report['capacity_observation'] = ('backlog_exceeds_worker_concurrency'
                    if census['pending'] > args.concurrency else 'no_large_backlog_observed_after_arrivals')
            if census['pending'] == 0:
                break
            if time.monotonic() >= deadline:
                failures.append('drain_timeout')
                break
            time.sleep(.5)
        report['post_load_drain_seconds'] = time.monotonic() - measurement_start - report['load']['submission_and_response_seconds']
        sampler.close()
        report['census_samples'] = sampler.count
        sampler = None
        stage = 'service_shutdown'
        worker.stop()
        deployment.server.stop()
        stage = 'final_observations'
        # Read after owned services stop, including mutations whose HTTP reply
        # was lost. An early empty census is not proof an uncertain POST cannot commit.
        census = sql_json(deployment, CENSUS)
        report['final_census'] = census
        report['database_latency_ms'] = sql_json(deployment, LATENCIES, timeout=30)
        window = report['database_completion_window']
        window['succeeded'] = sql_json(deployment, "SELECT count(*) FROM tasks WHERE "
            "correlation_key='measurement' AND state='succeeded' AND terminal_at_ms >= "
            f"{window['start_ms']} AND terminal_at_ms < {window['end_ms']}")
        window['succeeded_per_second'] = window['succeeded'] / args.duration
        report['process_reuse'] = sql_json(deployment, "SELECT json_build_object("
            "'reported',count(*),'reused',count(*) FILTER (WHERE "
            "(convert_from(s.accepted_command,'UTF8')::jsonb #>> '{report,report,reused_process}')::boolean),"
            "'pids',coalesce(json_agg(DISTINCT (convert_from(s.accepted_command,'UTF8')::jsonb "
            "#>> '{report,report,outcome,output,pid}')::bigint),'[]'::json)) "
            "FROM accepted_settlements s JOIN attempts a USING(attempt_id) JOIN tasks t USING(task_id) "
            "WHERE t.correlation_key='measurement' AND t.state='succeeded'", timeout=30)
        reuse = report['process_reuse']
        if (reuse['reported'] != census['succeeded'] or reuse['reused'] != reuse['reported']
                or not set(reuse['pids']).issubset(report['warmup']['subprocess_pids'])):
            failures.append('warm_process_reuse_not_preserved')
        report['artifact_downloads'] = deployment.artifacts.downloads()
        report['load']['http_accepted_per_configured_second'] = report['load']['http_accepted'] / args.duration
        if report['load']['dropped_capacity'] or report['load']['dropped_late']:
            failures.append('generator_saturated_or_late')
        if report['load']['http_error'] or report['load']['uncertain']:
            failures.append('submission_errors_or_uncertain_outcomes')
        if census['durable_accepted'] != report['load']['http_accepted']:
            failures.append('http_and_durable_acceptance_mismatch')
        if census['succeeded'] != census['durable_accepted'] or census['attempts'] != census['durable_accepted']:
            failures.append('unfinished_failed_cancelled_or_retried_tasks')
        if any(value['negative_samples'] for value in report['database_latency_ms'].values()):
            failures.append('database_clock_moved_backwards')
    except KeyboardInterrupt:
        failures.append('interrupted')
    except Exception as error:
        # Subprocess exception text may contain a database URL. Keep credentials
        # out of durable evidence; stage and owned logs identify the failing operation.
        failures.append(f'{stage}: {type(error).__name__}')
    finally:
        for label, cleanup in [('sampler', lambda: sampler.close() if sampler else None),
                               ('submitter', lambda: submitter.close() if submitter else None),
                               ('deployment', lambda: deployment.close() if deployment else None),
                               ('database', lambda: admin(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)') if created else None)]:
            try:
                cleanup()
            except BaseException as error:
                failures.append(f'{label}_cleanup_failed: {type(error).__name__}')
        if args.delivery_config:
            try:
                if inspect_delivery_config(args.delivery_config) != config_digest:
                    failures.append('delivery_config_changed_during_experiment')
            except (ValueError, OSError):
                failures.append('delivery_config_unreadable_after_experiment')
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)
        report.update(result='passed_bounded_experiment' if not failures else 'failed', failures=failures)
        (directory / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'result': report['result'], 'failures': failures, 'evidence': str(directory)}), flush=True)
    return 1 if failures else 0


class SelfTests(unittest.TestCase):
    def test_rate_and_burst_have_no_extra_boundary_arrivals(self):
        args = make_parser().parse_args(['--rate', '2', '--duration', '4', '--burst-at', '1',
                                         '--burst-duration', '1', '--burst-multiplier', '2'])
        self.assertEqual(list(arrivals(args)), [0, .5, 1, 1.25, 1.5, 1.75, 2, 2.5, 3, 3.5])
        self.assertEqual(validate(args), 10)

    def test_caps_and_nonfinite_values(self):
        for options in [ ['--rate', 'nan'], ['--rate', '5001'], ['--duration', '3601'],
                         ['--rate', '5000', '--duration', '60'],
                         ['--payload-bytes', '262144', '--duration', '600'],
                         ['--duration', '1', '--burst-duration', '2'] ]:
            with self.subTest(options=options), self.assertRaises(ValueError):
                validate(make_parser().parse_args(options))

    def test_delivery_requires_disposable_queue_and_config_is_bounded(self):
        for options in (['--delivery-config', 'unused.json'], ['--disposable-queue']):
            with self.assertRaises(ValueError):
                validate(make_parser().parse_args(options))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'delivery.json'
            body = b'{"route":{"scope":{"tenant_id":"t","namespace":"n"},"queue":"q"}}'
            path.write_bytes(body)
            self.assertEqual(inspect_delivery_config(path), hashlib.sha256(body).hexdigest())
            for invalid in (b'x' * (DELIVERY_CONFIG_LIMIT + 1), b'{"route":{},"route":{}}',
                            b'{"route":{}}', b'{"route":null}'):
                path.write_bytes(invalid)
                with self.assertRaises(ValueError):
                    inspect_delivery_config(path)

    def test_query_statistics_delta_detects_reset_eviction_and_missing_entries(self):
        from copy import deepcopy
        entry = {'userid': '1', 'queryid': '2', 'toplevel': True, 'query': 'select $1',
                 'calls': 10, 'rows': 3, 'wal_bytes': 100, 'total_exec_time': 2.5, 'stats_since': 'same'}
        before = {'observed_at': 'before', 'stats_reset': 'reset', 'deallocations': 0,
                  'entry_count': 1, 'entries': [entry]}
        after = deepcopy(before)
        after['observed_at'] = 'after'
        after['entries'][0].update(calls=12, rows=4, wal_bytes=150, total_exec_time=4)
        after['entries'].append(dict(entry, queryid='new', calls=1, rows=0, wal_bytes=0, total_exec_time=.1))
        after['entry_count'] = 2
        delta = statement_delta(before, after)
        self.assertTrue(delta['complete'])
        self.assertEqual(delta['totals'], {'calls': 3, 'rows': 1, 'wal_bytes': 50, 'total_exec_time': 1.6})
        variants = [('stats_reset', 'different'), ('deallocations', 1), ('entry_count', 1000)]
        for field, value in variants:
            invalid = deepcopy(after)
            invalid[field] = value
            delta = statement_delta(before, invalid)
            self.assertFalse(delta['complete'])
            self.assertIsNone(delta['totals'])
        for entries in ([], [dict(entry, calls=9)], [entry, entry],
                        [dict(entry, calls=100, stats_since='new')]):
            invalid = dict(after, entries=entries, entry_count=len(entries))
            self.assertFalse(statement_delta(before, invalid)['complete'])

    def test_histogram_bounds_empty_and_overflow(self):
        histogram = Histogram()
        self.assertIsNone(histogram.summary()['p99_upper'])
        for value in (0, 1, 2, 10, 1e10):
            histogram.add(value)
        self.assertEqual(histogram.summary()['count'], 5)
        self.assertGreaterEqual(histogram.summary()['p50_upper'], 2)
        self.assertLessEqual(histogram.summary()['p50_upper'], 2.2)
        self.assertEqual(histogram.summary()['p99_upper'], 1e10)
        with self.assertRaises(ValueError):
            histogram.add(float('nan'))

    def test_load_drops_instead_of_building_hidden_backlog(self):
        import io
        args = make_parser().parse_args(['--rate', '1000', '--duration', '.1', '--submitters', '1', '--max-lateness-ms', '1000'])
        def slow(_command):
            time.sleep(.15)
            return {'kind': 'accepted', 'latency_ms': 150}
        result = run_load(args, slow, lambda i: i, lambda: None, io.StringIO())
        self.assertEqual(result['max_in_flight'], 1)
        self.assertEqual(result['sent'], 1)
        self.assertEqual(result['http_accepted'], 1)
        self.assertEqual(result['offered'], result['sent'] + result['dropped_capacity'] + result['dropped_late'])

    def test_background_submit_exception_is_not_lost(self):
        import io
        args = make_parser().parse_args(['--rate', '10', '--duration', '.1'])
        def broken(_command):
            raise RuntimeError('injected failure')
        with self.assertRaisesRegex(RuntimeError, 'injected failure'):
            run_load(args, broken, lambda i: i, lambda: None, io.StringIO())

    def test_submitter_distinguishes_rejection_uncertainty_and_acceptance(self):
        import http.server
        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                key = request['idempotency_key']
                status = 503 if key == 'reject' else 200
                payload = json.dumps({'task_id': 'task_test',
                    'idempotency_key': 'incorrect' if key == 'bad_identity' else key}).encode()
                self.send_response(status)
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            def log_message(self, *_args):
                pass
        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        submit = Submitter(f'http://127.0.0.1:{server.server_port}', 1)
        try:
            self.assertEqual(submit({'idempotency_key': 'reject'})['kind'], 'http_error')
            self.assertEqual(submit({'idempotency_key': 'bad_identity'})['kind'], 'uncertain')
            self.assertEqual(submit({'idempotency_key': 'valid'})['kind'], 'accepted')
            self.assertEqual(len(submit.connections), 1)
        finally:
            submit.close()
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
        self.assertFalse(thread.is_alive())

    def test_sampler_exception_is_not_lost(self):
        with tempfile.TemporaryDirectory() as directory:
            sampler = Sampler(None, 1, Path(directory) / 'samples')
            sampler.thread.start()
            sampler.thread.join(timeout=2)
            with self.assertRaisesRegex(RuntimeError, 'sampler failed'):
                sampler.close()


if __name__ == '__main__':
    sys.exit(main())
