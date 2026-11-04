---
title: Share one worker across queues
description: Use weighted poll shares for multiple integrated HTTP queues in a development build.
---

**Development feature.** Build the worker from the matching development source.
The released 0.8.0 worker connects to one queue.

Suppose you want seven of every ten polling opportunities to go to interactive
work and three to batch work when both queues are enabled. Save this as
`poll-plan.json` in your working directory:

```json
{
  "schema_version": 1,
  "revision": 1,
  "strategy": "stride",
  "queues": [
    {"queue": "interactive", "weight": 7},
    {"queue": "batch", "weight": 3}
  ]
}
```

Start your existing worker with `--poll-plan ./poll-plan.json` in place of
`--queue NAME`. Keep the same server, storage, Python runner and credentials.
For example, from a development checkout with the local server running and its
program store at `./store`, run:

```sh
./target/debug/ledgence worker connect \
  --server http://127.0.0.1:8080 --tenant acme --namespace demo \
  --poll-plan ./poll-plan.json --concurrency 4 \
  --store ./store --cache ./worker-cache --python python3 \
  --runner ./sdk/python/ledgence/worker/bootstrap.py
```

These four consumers share one process pool across both queues. Stride apportions
**polls**, including empty replies. To choose queues randomly with the same
relative weights, use `"strategy": "lottery"` when starting the worker. A long
batch task still holds its consumer until completion. The 70/30 target describes
which queue each new poll goes to.

## Change weights without interrupting tasks

Copy `poll-plan.json` to `poll-plan.next.json` in the same directory and edit
the copy. Increase `revision` to 2 and change the weights. Save the file and run:

```sh
mv poll-plan.next.json poll-plan.json
```

The worker checks the path about once per second and logs the loaded revision.
A running task or pending poll finishes on its original queue. All-zero weights
pause fresh polls; a higher revision with positive weights resumes them. An
invalid replacement leaves the previous plan active. Keep the same queue names
and strategy for live updates. Each worker has its own plan and physical
concurrency. The feature supports integrated HTTP queues; external SQS routes
continue to use the single-queue connection.

In Console, the first queue in the startup plan is the primary session. It reports
the shared physical pool once and stays primary if a later revision reorders the queues.
A secondary task still occupies a slot, which appears as `unknown` in the
primary session's slot details. Open **Executions** to see that task's history.
To see individual poll choices, start the worker with
`RUST_LOG=info,ledgence_worker_delivery::polls=debug`.
