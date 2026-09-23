# Performance experiments

`tools/check-performance.py` runs an explicitly bounded experiment against a fresh, uniquely named PostgreSQL database, one owned HTTP orchestrator, and one real Python worker. The worker downloads a published program and warms all N reusable subprocesses before measurement. The program sleeps for the requested work duration and returns a small result. It does not model CPU-bound computation, external services, dependency-heavy preparation, or application side effects.

This is a reproducible baseline and regression tool. Passing a short run does **not** qualify millions of daily executions, establish a production SLO, or validate retention, replication, failover, or a broker's capacity. The default mode is integrated PostgreSQL delivery. Supply `--delivery-config` to run the same experiment with SQS delivery; PostgreSQL still stores task state, claims, publication intents, history, and settlement.

## Running a bounded experiment

Use CPython 3.11 or newer, Cargo, and `psql`. The supplied PostgreSQL 18 server must be disposable, and its role must be allowed to create databases. The harness creates/drops only its own `ledgence_perf_<random>` database; it never creates or restarts the PostgreSQL server. Both the explicit flag and environment variable below are required. Never use a production or shared deployment.

```sh
export LEDGENCE_POSTGRES_URL='postgres://postgres:password@127.0.0.1:5432/postgres'
export LEDGENCE_PYTHON=/path/to/python3.12
python3 tools/check-performance.py --self-test
python3 tools/check-performance.py --disposable-postgres --psql /path/to/psql \
  --evidence /absolute/path/to/new-evidence-directory
```

By default, it builds the workspace binaries with the release profile, offers 5,000,000 / 86,400 = **57.87037 submissions/second** for 60 seconds, uses N=16 subprocesses, 16 HTTP submitter threads, 1,024 padding bytes, and 100 milliseconds of sleeping work. Startup and warmup are excluded. A ten-minute migration budget and a 120-second post-load drain are separate from the arrival window. Evidence is always retained, including on failure. Without `--evidence`, the tool prints a newly created temporary directory.

A tiny functional smoke run:

```sh
python3 tools/check-performance.py --disposable-postgres --psql /path/to/psql \
  --rate 10 --duration 2 --concurrency 2 --submitters 2 --work-ms 10 \
  --drain-timeout 10 --sample-interval 1 \
  --evidence /absolute/path/to/new-smoke-directory
```

A brief 10× arrival burst within a longer observation window:

```sh
python3 tools/check-performance.py --disposable-postgres --psql /path/to/psql \
  --duration 120 --burst-at 30 --burst-duration 10 --burst-multiplier 10 \
  --concurrency 96 --submitters 32 --work-ms 100 \
  --evidence /absolute/path/to/new-burst-directory
```

N remains Ledgence's single worker concurrency setting. `--submitters` bounds the external benchmark client, not the worker. Choose N from the workload and available hardware: roughly arrival rate × mean task duration slots are needed even before orchestration and headroom. The illustrative burst command may overload a small laptop, the load generator, or the database. Overload is a result to retain and inspect, not a reason to silently lower the offered rate.

Supply `--binaries /absolute/path/to/binaries --profile debug|release` to skip building. That profile is a declaration by the operator. The report hashes the actual executables and records the workspace commit/dirty status, but it cannot prove that externally built binaries came from that commit or profile. Preserve a clean build and its source revision when comparing changes. A debug run can validate the harness; use release builds for performance comparisons.

## Comparing SQS delivery

Provision a fresh, empty, disposable Standard queue dedicated to this experiment and prepare the shared [delivery configuration](dispatch-delivery.md#executable-sqs-configuration). The harness requires `--disposable-queue` in addition to `--disposable-postgres`; it sends, receives, and acknowledges messages on that queue. It does not provision, purge, or delete queues. Never reuse a production queue or a queue from a previous experiment: retained messages can refer to the previous experiment's deleted task database.

```sh
python3 tools/check-performance.py --disposable-postgres --psql /path/to/psql \
  --delivery-config /absolute/path/to/disposable-delivery.json --disposable-queue \
  --rate 57.87037 --duration 60 --concurrency 16 --submitters 16 \
  --evidence /absolute/path/to/new-sqs-evidence-directory
```

The configuration is read with a 16 KiB cap before database creation and passed unchanged to both binaries. The worker's scope and queue come from its explicit route. The Rust adapter validates the complete schema, queue configuration, and access at startup. The harness records a SHA-256 fingerprint and fails if the file differs at the final check; retain the original configuration separately and keep it unchanged throughout the run. The file itself is not copied into evidence. AWS credentials remain in the normal external credential chain. For ElasticMQ, use the explicit local endpoint and local credential option described in the delivery guide.

Without `--binaries`, the harness builds with both executable `sqs` features when a configuration is present. Supplied binaries must already include those features. SQS results use the same task census, timing, and process-reuse checks as the integrated mode. Queue publication, receives, acknowledgements, and redelivery add work; record broker request counts and resource settings separately. Local ElasticMQ results describe that local experiment and do not establish AWS capacity or service behavior.

## Optional PostgreSQL query counters

Add `--pg-stat-statements` to either mode when the disposable PostgreSQL 18 server already preloads `pg_stat_statements`. The role must be able to create the extension. The harness creates it **only in its uniquely owned database** and fails if collection is unavailable. It never resets shared statistics.

A snapshot after warmup and a snapshot after all measured submission replies finish bracket the collection interval. The interval includes startup of the census sampler and the small database clock sample before arrivals; it excludes the later drain and post-run latency queries. Work still pending at its end will incur additional database work outside the reported counters.

`pg-stat-statements-before.json` and `pg-stat-statements-after.json` retain at most 256 query entries, filtered to the owned database. Each records query/user/top-level identity, cumulative calls, rows, WAL bytes, execution time in milliseconds, and up to 2,048 characters of normalized query text. `results.json` includes deltas and totals. This is server-side statement execution time; it does not measure connection wait, end-to-end transaction latency, total cluster WAL, or broker costs.

Statistics resets, evictions, disappearing entries, duplicate identities, counter decreases, or exceeding the query-entry cap make the collection incomplete and fail the experiment. Invalid deltas remain visible, but totals are withheld. A global eviction caused by another database is treated conservatively as incomplete. Observer overhead is included: census queries, the clock sample, the first statistics snapshot, and normal idle worker/publisher queries all share this database. The final snapshot records its own cost after it has read the counters, so that final query's cost is outside the delta. Capture sampling cadence and competing workloads when comparing runs.

## Arrival generation and bounds

Arrivals follow a deterministic, open-loop schedule. The optional burst replaces the base rate for its interval; it does not add a second stream of arrivals. The client uses persistent HTTP connections with at most one in-flight request per submitter thread. It never queues an unbounded list of futures or retains all task results in memory.

If every submitter is occupied, an arrival is counted as `dropped_capacity`. If the generator is more than `--max-lateness-ms` behind the scheduled arrival (default 50 ms), it records `dropped_late`. It does not backfill arrivals beyond that tolerance or extend the arrival window to make its target appear achieved. Arrivals within the tolerance can bunch together; inspect lateness and tighten the tolerance when that affects rate fidelity. Every submitted request has a unique idempotency key. Mutation retries are disabled: non-200 replies and uncertain transport/decoding/identity failures remain visible.

| Setting | Allowed range or cap |
| --- | --- |
| Base and peak offered rate | 0.1–5,000/second; peak also capped at 5,000 |
| Arrival duration | 0.1–3,600 seconds |
| Burst duration/multiplier | 0–120 seconds / 1–20; entirely inside the arrival window |
| Total offered arrivals | At most 250,000 |
| Estimated aggregate input | At most 2 GiB, using padding + 1,024 bytes per request |
| Padding per task | 0–256 KiB; actual user data includes control fields |
| Sleeping work duration | 0–60,000 ms |
| Worker N / HTTP submitter threads | 1–128 / 1–64 |
| Request socket timeout | 1–30 seconds |
| Post-load drain | 1–600 seconds |
| Census interval | 1–60 seconds, default 5 |
| Submission response | At most 1 MiB |

The input estimate is an admission cap for this fixture, not measured database/WAL usage. The database, history, logs, and evidence still grow with total tasks. They are bounded by the experiment's caps, not by a steady-state retention policy. Request timeouts are socket waits; the owned HTTP server also retains its normal exchange deadline. SQL observations have explicit connection, statement, lock, and process timeouts. Inherited `OTEL_` configuration is removed and the owned services log at `warn`. Exports are disabled by default; `--metrics-endpoint http://127.0.0.1:4318/v1/metrics` explicitly enables only OTLP metrics for paired measurements. Run a collector separately and verify its received points; this harness measures execution and does not itself attest to successful telemetry delivery.

## What is measured

`results.json` records configuration, executable hashes, workspace state, OS/architecture/CPU and available memory metadata, PostgreSQL version and durability settings, warmup PIDs, failures, counters, and distributions. `metadata.json` preserves initial metadata before database setup. `submissions.jsonl` streams bounded per-response records to disk. `census.jsonl` streams the scalar database census and its query duration; service logs remain alongside it. Database URLs and passwords are not copied into reports.

The quantities are deliberately separate:

- **Offered:** arrivals in the configured schedule, including generator drops.
- **Sent:** HTTP requests actually submitted.
- **HTTP accepted:** successful, identity-validated HTTP replies. A lost reply may still correspond to a durable submission.
- **Durable accepted:** measured task records present in the owned database after the services stop.
- **Succeeded / failed / cancelled / pending:** durable task state, including tasks whose HTTP reply was uncertain.
- **Completions in the database window:** successful terminal timestamps inside an interval of the configured duration, anchored by a database-clock sample immediately before the generator starts. It excludes completions during the later drain. This anchor precedes the first actual arrival by a small setup gap.

The final census is read after stopping the owned services, so an early empty queue cannot hide an uncertain POST committing later. A nonempty backlog after arrivals is reported separately; `backlog_exceeds_worker_concurrency` flags more pending tasks than N at that observation. Census points are periodic observations, not a continuous maximum or proof of sustainable throughput.

Latency distributions:

| Measurement | Meaning |
| --- | --- |
| HTTP response latency | Client monotonic time around submission, including local encoding, HTTP, and response validation; successful and unsuccessful responses are included. |
| Generator lateness | Actual scheduling time minus planned arrival time, including dropped arrivals. |
| Accepted → terminal | Durable database submission time to logical terminal time. Includes waiting, execution, reporting, and cleanup required for task finalization. |
| Accepted → first claim | First committed acquisition history time minus durable submission time. |
| First claim → dispatch authorization | Committed dispatch permission time minus first claim time. This is not actual Python start time. |

Client histograms use constant-memory buckets with 10% relative spacing and report quantile upper bounds plus exact count/min/max/mean. Database percentiles use PostgreSQL `percentile_cont` over complete measured rows after load. Null/unreached phases are excluded and each distribution reports its own sample count. Negative database-clock intervals fail the run; clock synchronization and drift still require operational control.

The initial fixture permits one attempt per task. All measured successes must retain the N warmed Python PIDs and report process reuse. Program outputs are small; submission padding therefore exercises input transport/storage without adding equally large results.

## Interpreting results

A zero exit status means this bounded experiment completed without generator drops, submission errors/uncertainty, missing durable acceptance, failed/cancelled/pending/retried work, lost process reuse, observed backward clock intervals, incomplete requested query statistics, configuration changes detected at the final check, or cleanup failures. It does not impose a latency SLO or prove that the configured rate can continue indefinitely. A workload can accumulate backlog and drain successfully: inspect `capacity_observation`, the completion-window rate, end-of-load census, latency tails, and census trend, even when the run passes.

Any failed or uncertain request makes the experiment fail, even if the later database census finds its successful task. Background submission and sampler exceptions also fail the run. A drain timeout preserves pending counts; stopping the worker may allow some in-flight tasks to finish before the final census. The timeout remains a failure rather than being erased by later cleanup.

The census reads the same PostgreSQL database every few seconds and scans measured task metadata. It adds observer load. Post-run percentile/result-validation queries may scan history and settlement data, but do not run during the arrival window. Per-task result polling is deliberately absent from that window. The artifact HTTP server and generator also share the local host, so CPU, memory, Docker limits, and competing workloads can affect results. Capture those deployment limits separately when they are unavailable from the harness's OS metadata.

For a useful comparison, preserve the same release build settings, hardware, PostgreSQL durability/configuration, worker N, program/package, payload, rate/burst profile, census interval, and telemetry settings. Repeat runs and retain unfavorable results. Compare both acceptance and completion rates, latency tails, backlog, resource use, WAL and storage growth. For a local SQS-compatible comparison, include broker request counts, batching, and latency, and identify the ElasticMQ version and deployment. Qualifying AWS capacity or behavior requires a separate experiment against AWS SQS.

Long-duration qualification remains a separate exercise: representative application runtimes and payloads, sustained peaks, retries and recovery, realistic retained history, storage growth, and resource headroom. The first product target is five million executions/day with representative bursts, not a claim already established by this tool.


## Workflow placement measurements

`tools/check-workflows.py` exercises checkpoint recovery and compares concurrent
local I/O with explicitly distributed tasks on a disposable PostgreSQL database:

```sh
python tools/check-workflows.py --self-test
python tools/check-workflows.py --binaries target/debug --psql /path/to/psql \
  --evidence /absolute/path/to/new-workflow-evidence-directory
python tools/check-workflows.py --endpoint http://127.0.0.1:9324 \
  --binaries target/debug --psql /path/to/psql \
  --evidence /absolute/path/to/new-workflow-elasticmq-evidence-directory
```

Provide `LEDGENCE_POSTGRES_URL`, a supported `LEDGENCE_PYTHON`, and the Python
client's dependencies. Supplied binaries must include the SQS feature for the
ElasticMQ mode. The harness imports the client from the checkout; the separate
installed-client gate verifies wheel packaging. It creates and drops a unique
database and, in ElasticMQ mode, a unique local queue. It restarts only its owned
orchestrator/worker processes, without restarting PostgreSQL. Evidence remains
outside the repository.

Use `--scenario placement --placement-iterations 10` for ten paired samples.
The fixture performs 100 loopback HTTP reads with a 30 ms server delay and a
17-byte response, using worker concurrency 1. Local mode overlaps those reads
inside one activation and commits 100 local records; distributed mode creates
100 separate task executions in batches of 50. It deliberately compares
placement choices with different execution parallelism, not equal-concurrency
queue throughput. Alternating order reduces simple warmup/order bias.

Evidence records client elapsed time, durable submission-to-terminal elapsed
time, task/attempt/activation/local-record/history counts, payload size, and
server-wide WAL deltas. Client elapsed time includes one-second result polling.
WAL deltas may include maintenance and other databases, so they are not isolated
per-workflow write costs. Build mode, machine, database configuration, competing
load, and warm-process replacement affect these results. These bounded paired
measurements do not establish sustained throughput, tail latency, or billion-
execution scale. Use the arrival-based task harness and representative long runs
for separate capacity qualification.


## Mixed task and workflow soak

`tools/check-workload-soak.py` exercises an installed Python client against fresh owned
services. Six repeating cases cover ordinary tasks, a retryable first-attempt
process loss, acknowledged local async steps, distributed tasks plus an owned
subworkflow, durable timers, and external events. Every seventh root execution
gets a completion subscription, so both tasks and each workflow case receive
callbacks. Repeated identical submissions must keep their execution identity.
Every returned value and callback is verified against its accepted identity, and
the final database census must exactly match all expected tasks, attempts,
workflows, local results, and subscriptions. Business exceptions remain terminal;
the retry fixture deliberately terminates its process to exercise process recovery.

Use release binaries from the same reviewed source and the Python interpreter
in an environment containing the installed client wheel:

```sh
/path/to/client-venv/bin/python tools/check-workload-soak.py --self-test
/path/to/client-venv/bin/python tools/check-workload-soak.py \
  --disposable-postgres --psql /path/to/psql \
  --binaries /absolute/path/to/release-binaries \
  --duration 900 --clients 16 --workers 2 --concurrency 8 \
  --evidence /absolute/path/to/new-soak-evidence
```

`LEDGENCE_POSTGRES_URL` and `LEDGENCE_PYTHON` have the same meaning as above.
The database must be disposable. The harness creates and drops its own uniquely
named database, owns its HTTP receiver/processes, and preserves evidence on
failure. Supply `--delivery-config` and `--disposable-queue` for a separate fresh
ElasticMQ queue using the same rules as the arrival-rate experiment.
`--metrics-endpoint` has the same explicit opt-in behavior.

The default is a 15-minute **closed-loop** stability experiment: 16 client lanes
submit new work only after their previous result/callback finishes. It therefore
does not measure sustained offered-rate capacity. Two workers each have their
own single `N=8` concurrency setting. The harness samples process-family RSS and
subprocess counts, requires every workload case to execute, verifies graceful
shutdown, and fails if a worker exceeds N observed subprocesses. Default resource
gates are 2 GiB combined service RSS and 128 MiB growth between mature early/late
median samples. These are configurable test bounds, not production memory SLOs.
Sampling can miss brief peaks; it complements the runtime's ownership tests.

The duration is capped at one hour, clients at 64, workers at four, N at 32 per
worker, input padding at 16 KiB, and root operations at 100,000. Hitting the
operation cap ends arrivals early and produces `passed_bounded_mixed_run` rather than
`passed_bounded_soak`. At least six mature resource samples are required for
disjoint memory-growth observation windows; an undersized run fails that gate. Each operation
has a separate bounded completion deadline. Database/history and evidence grow
with completed work; PostgreSQL/broker memory and OS page cache are outside the
process RSS observation. Retention, restart/fault recovery, and exporter outages
have separate correctness gates. Compare fresh release builds on the same
hardware and retain unsuccessful runs alongside successful evidence.

The separate arrival-rate and soak models follow the distinction documented in
[Grafana's arrival-rate executor](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/constant-arrival-rate/)
and [load-test guidance](https://grafana.com/docs/k6/latest/testing-guides/api-load-testing/).
Ledgence's harness uses Python's standard library; k6 is a research reference.
