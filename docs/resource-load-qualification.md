# Resource admission load qualification

This source harness qualifies governed calls on one Ledgence instance using an independent local HTTP provider. It launches real orchestrator and worker processes, publishes one task and one workflow, starts PostgreSQL 18.6 with `pg_stat_statements` in a new Docker container on an ephemeral loopback port, and removes the container and its data volume. It requires Python 3.11+, Docker, PostgreSQL's `psql` client, `ps`, and the candidate executable.

## Run the checks

From the repository root, build the executable and run the smaller functional trial with a fresh evidence directory:

```sh
cargo build --locked --all-features -p ledgence-cli --bin ledgence
python3 tools/check-resource-load.py --smoke --binaries target/debug \
  --evidence /tmp/ledgence-resource-load-smoke
```

For release measurements, use a clean checkout of the exact committed source, its matching Python worker helper and a verified optimized binary built from that commit. Supply its Git SHA and the SHA-256 recorded by the build. Run on a quiet host without overlapping builds or load tests. Each output path must be new. Use `--scenario timing` or `--scenario pressure` to run them in separate windows.

```sh
python3 tools/check-resource-load.py --binaries /path/to/optimized/bin \
  --candidate-sha VERIFIED_COMMIT --binary-sha256 VERIFIED_BINARY_SHA256 \
  --evidence /tmp/ledgence-resource-load-qualification
```

If `psql` is installed outside your PATH, add `--psql /path/to/psql`. By default the runner omits the worker's `--acquire-wait-ms` argument, which uses the single-queue HTTP worker default of 20,000 ms. To run a matched short-poll control, repeat the same command with `--acquire-wait-ms 0` and a different evidence directory. Values between 0 and 20,000 ms are accepted. The report records the queue, concurrency and poll flag from each launched worker, and identifies whether the effective wait comes from the worker CLI default or an explicit argument. It describes configuration rather than the observed duration of each poll. The runner creates its own database and does not need an existing server URL. `--smoke` marks its output as functional evidence, including when all requests complete.

## Matched timing trials

Every arm uses the same binary, warmed program, four consumer slots, four submitters and the same physical provider work. Each arm offers 96 tasks at 16 per second. The first scheduled task in each block of eight requests 150 ms of provider work. The other seven request 5 ms each. Physical requests can reach the provider in a different order. The generator reports actual arrival times and rejects a saturated submission pool or delay exceeding 125 ms. Three repetitions alternate forward, reverse and forward order. Smoke mode uses one repetition of 12 tasks per arm.

| Arm | Policy |
| --- | --- |
| Ordinary | The tasks omit resource admission and run budgets. The installation supports both. |
| Hot resource | Every task requests the same enabled FIFO resource, with capacity four. |
| Independent resources | Every block of eight visits all eight FIFO resources, each with capacity four. The first key rotates between blocks so long requests span the resources. The worker still has four slots total. |
| Hot resource with budget | Each standalone root requests the hot resource and permits one call. Its accepted Start charges its own root. |

The hot arm has four possible reservations. The independent arm has 32 in aggregate. Both have four worker slots. Their differences include the number of reservations that can be offered before earlier work closes, so the timing contrast cannot isolate row-lock contention. Each timing and pressure result records the relevant resource capacity and consumer slots.

The provider records a distinct key and task identity for every POST, the requested service duration, monotonic entry/finish interval, the returned status and whether the synthetic effect was created. The verifier checks each identity individually and requires all offered tasks to complete in one attempt with one physical request/effect and the expected charge. Each budgeted task has its own root and exactly one ledger entry for the admitted source. Ordinary and resource-only tasks have no root or charges. Throughput includes the fixed arrival interval and the final terminal observation. This measures the stated offered workload, not peak platform capacity.

`summary.json` contains nearest-rank p50/p95/p99 from database registration to first claim, governed Start and terminal completion, and the peak simultaneous provider calls. The interval to Start includes queue, offer and worker preparation. The provider busy fraction divides the sum of physical call intervals by four times the first-arrival-to-observed-drain window. Accepted Start and provider entry are distinct events. Each arm retains raw database rows, provider calls, client arrival timings and samples.

SQL calls, returned/affected rows, statement WAL bytes and execution time are per-database `pg_stat_statements` deltas from before arrivals through observed drain. They include fixture polling and background maintenance, and are invalid if source entries evict, reset, disappear or exceed the capture bound. The separate cluster WAL figure comes from WAL insert positions on the disposable server. Host CPU and peak RSS sample the orchestrator, worker and their live children, excluding Docker, PostgreSQL and the load driver. PostgreSQL lock observations count sampled waiting backends, not cumulative wait duration. Short-lived processes and brief locks can fall between samples. [PostgreSQL statement statistics](https://www.postgresql.org/docs/18/pgstatstatements.html) · [PostgreSQL activity and waits](https://www.postgresql.org/docs/18/monitoring-stats.html).

## Pressure and recovery

- **Parked population.** Submit 512 governed tasks to a disabled resource. Check that all are waiting with no owner attempt or provider request, complete an ordinary witness on the same worker, kill and restart the orchestrator, check the parked population again, enable the resource and drain all 512 exactly once. Smoke mode uses 16 roots.
- **Late interactive work.** Two batch requests occupy both slots of a shared resource. Twelve more batch requests (short and long) and then eight support requests arrive before the holders release. The recorded offers must grant a support turn within four opportunities under the 3:1 weights. The provider independently checks physical overlap and completion. Use the existing [FIFO and weighted resource comparison](resource-admission.md#weighted-offers) for the paired fairness control and the two-coordinator crash test.
- **Provider backoff.** A real workflow calls a throttled endpoint, receives HTTP 429 and parks on a durable timer for the provider's one-second Retry-After value. The application allows one further call under a two-call budget. One case succeeds on its second request and another stops on a second 429. The verifier matches every physical request to the persisted workflow activation, attempt, source and charge, checks the single successful external effect (zero effects for persistent 429s), measures actual request spacing and confirms that replaying the identical submission cannot add a third call.

An unavailable or uninstrumented external service, hidden vendor SDK retries, another database topology or a different runtime mix can produce different results. For each release, interpret the raw evidence with the recorded binary hash, source revision, host, runtime and input distribution.

## Measurements for Ledgence 0.9.0

These trials used source `6de107d0727b7164fbb264724b883216190139c3`, executable SHA-256 `00d7f8ddc2ab5f24f7e53a43e5e733ed100c681a4e42485b0cff59ec47fffd0e`, macOS-27.2-arm64-arm-64bit-Mach-O and PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2). The timing worker used one integrated HTTP queue and four slots. The default runs omitted the wait flag, selecting a maximum wait of 20,000 ms per acquisition. The matched short-poll control explicitly set `--acquire-wait-ms 0`. These are worker configurations. Receipt timings do not identify how long any individual poll waited.

Each timing arm offered 96 tasks at 16 per second to the local synthetic provider in each of three repetitions. Each eighth call requested 150 ms of work and the others requested 5 ms. The table shows the range of observed registration-to-terminal and registration-to-first-claim p95 values in each poll configuration.

| Arm | Default completion p95 (ms) | Zero-wait completion p95 (ms) | Default first-claim p95 (ms) | Zero-wait first-claim p95 (ms) |
| --- | --- | --- | --- | --- |
| Ordinary | 175–177 | 760–891 | 6–7 | 727–815 |
| Hot resource | 221–226 | 2239–4152 | 64–68 | 2116–4109 |
| Eight independent resources | 225–236 | 399–919 | 64–69 | 357–839 |
| Hot resource with a root budget | 224–228 | 2584–4771 | 65–66 | 2490–4719 |

The hot resource permitted four reservations. Eight independent resources permitted four each, or 32 reservations in aggregate. Both timing configurations ran on the same four worker slots. Each block of eight tasks visited all eight keys, rotating the first key across blocks so that slow requests reached every key. This contrast changes reservation capacity as well as contention and cannot isolate row-lock cost or establish a scaling law.

SQL includes fixture polling and background work in the observed database window; these counters do not isolate trigger-internal work. Throughput includes the offered-arrival interval and final drain. Peak physical overlap comes from the synthetic provider's independent intervals.

| Poll configuration | Arm | Completions/s | SQL calls/task | Statement WAL bytes/task | Peak physical calls |
| --- | --- | --- | --- | --- | --- |
| Default | Ordinary | 15.9–15.9 | 95.0–95.6 | 21336.3–22997.7 | 2–2 |
| Default | Hot resource | 15.9–15.9 | 182.1–182.9 | 43440.3–43808.6 | 2–2 |
| Default | Eight independent resources | 15.7–15.9 | 186.6–194.1 | 43991.1–46618.0 | 2–2 |
| Default | Hot resource with a root budget | 15.9–15.9 | 191.1–191.9 | 45476.8–45732.0 | 2–2 |
| Zero wait | Ordinary | 14.7–15.9 | 75.3–76.6 | 20251.8–21947.2 | 3–4 |
| Zero wait | Hot resource | 9.5–11.9 | 154.5–161.1 | 44337.4–44700.2 | 2–3 |
| Zero wait | Eight independent resources | 14.4–15.7 | 156.7–158.4 | 43350.5–43831.9 | 2–4 |
| Zero wait | Hot resource with a root budget | 8.3–11.1 | 164.5–173.3 | 46261.5–50212.4 | 3–3 |

The default-poll pressure run drained all 512 parked tasks after restart and enablement. It used four worker slots for that phase. The interactive phase added a two-slot support worker, held the shared resource at two reservations, and granted the first support turn at opportunity 1. The two backoff cases each made two physical calls, one ultimately successful and one persistently throttled.

These figures describe this offered workload and topology. They do not establish peak throughput, per-poll causality or third-party provider limits. The ordinary arm in these tables uses the same 0.9.0 binary with admission omitted. The separate version comparison follows.

### Ordinary tasks and workflows compared with 0.8.0

These are separate direct-path trials at 10 arrivals per second, 250 executions per run, four worker slots, a 1 KiB payload and 5 ms of program work. Four balanced pairs compared each version. The workflow case includes a local result. The table gives the observed p95 range across the four runs for each version and the median of the four per-pair differences.

| Workload | 0.8.0 p95 (ms) | 0.9.0 p95 (ms) | Median paired change (ms) |
| --- | --- | --- | --- |
| task | 33.00–34.00 | 34.00–34.55 | +0.77 |
| workflow-local | 96.00–100.00 | 98.00–100.00 | -0.27 |

The SQL census used PostgreSQL 18.6 with `pg_stat_statements.track=top`. It excludes the observer's own queries and includes background processing. It found no top-level statements mentioning the governed tables selected by the driver; this does not count statements executed inside functions or triggers. These repeated observations do not establish zero overhead, a capacity limit, or statistical equivalence.
