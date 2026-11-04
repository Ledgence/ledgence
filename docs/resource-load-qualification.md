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

If `psql` is installed outside your PATH, add `--psql /path/to/psql`. The runner creates its own database and does not need an existing server URL. `--smoke` marks its output as functional evidence, including when all requests complete.

## Matched timing trials

Every arm uses the same binary, warmed program, four consumer slots, four submitters and the same physical provider work. Each arm offers 96 tasks at 16 per second. Every eighth provider request takes 150 ms and the other requests take 5 ms. The generator reports actual arrival times and rejects a saturated submission pool or delay exceeding 125 ms. Three repetitions alternate forward, reverse and forward order. Smoke mode uses one repetition of 12 tasks per arm.

| Arm | Policy |
| --- | --- |
| Ordinary | The tasks omit resource admission and run budgets. The installation supports both. |
| Hot resource | Every task requests the same enabled FIFO resource, with capacity four. |
| Independent resources | Tasks are assigned round-robin to eight FIFO resources of capacity four. The worker still has four slots total. |
| Hot resource with budget | Each standalone root requests the hot resource and permits one call. Its accepted Start charges its own root. |

The provider records a distinct key and task identity for every POST, the monotonic entry/finish interval, the returned status and whether the synthetic effect was created. The verifier checks each identity individually and requires all offered tasks to complete in one attempt with one physical request/effect and the expected charge. Each budgeted task has its own root and exactly one ledger entry for the admitted source. Ordinary and resource-only tasks have no root or charges. Throughput includes the fixed arrival interval and the final terminal observation. This measures the stated offered workload, not peak platform capacity.

`summary.json` contains nearest-rank p50/p95/p99 from database registration to first claim, governed Start and terminal completion, and the peak simultaneous provider calls. The interval to Start includes queue, offer and worker preparation. The provider busy fraction divides the sum of physical call intervals by four times the first-arrival-to-observed-drain window. Accepted Start and provider entry are distinct events. Each arm retains raw database rows, provider calls, client arrival timings and samples.

SQL calls, returned/affected rows, statement WAL bytes and execution time are per-database `pg_stat_statements` deltas from before arrivals through observed drain. They include fixture polling and background maintenance, and are invalid if source entries evict, reset, disappear or exceed the capture bound. The separate cluster WAL figure comes from WAL insert positions on the disposable server. Host CPU and peak RSS sample the orchestrator, worker and their live children, excluding Docker, PostgreSQL and the load driver. PostgreSQL lock observations count sampled waiting backends, not cumulative wait duration. Short-lived processes and brief locks can fall between samples. [PostgreSQL statement statistics](https://www.postgresql.org/docs/18/pgstatstatements.html) · [PostgreSQL activity and waits](https://www.postgresql.org/docs/18/monitoring-stats.html).

## Pressure and recovery

- **Parked population.** Submit 512 governed tasks to a disabled resource. Check that all are waiting with no owner attempt or provider request, complete an ordinary witness on the same worker, kill and restart the orchestrator, check the parked population again, enable the resource and drain all 512 exactly once. Smoke mode uses 16 roots.
- **Late interactive work.** Two batch requests occupy both slots of a shared resource. Twelve more batch requests (short and long) and then eight support requests arrive before the holders release. The recorded offers must grant a support turn within four opportunities under the 3:1 weights. The provider independently checks physical overlap and completion. Use the existing [FIFO and weighted resource comparison](resource-admission.md#weighted-offers) for the paired fairness control and the two-coordinator crash test.
- **Provider backoff.** A real workflow calls a throttled endpoint, receives HTTP 429 and parks on a durable timer for the provider's one-second Retry-After value. The application allows one further call under a two-call budget. One case succeeds on its second request and another stops on a second 429. The verifier matches every physical request to the persisted workflow activation, attempt, source and charge, checks the single successful external effect (zero effects for persistent 429s), measures actual request spacing and confirms that replaying the identical submission cannot add a third call.

An unavailable or uninstrumented external service, hidden vendor SDK retries, another database topology or a different runtime mix can produce different results. For each release, interpret the raw evidence with the recorded binary hash, source revision, host, runtime and input distribution.
