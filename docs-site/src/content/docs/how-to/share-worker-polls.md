---
title: Share one worker across queues
description: Share a worker across HTTP or SQS Standard queues, update live poll weights and choose a receive wait.
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
concurrency. The commands above use integrated HTTP queues. For SQS, follow the setup below.

In Console, the first queue in the startup plan is the primary session. It reports
the shared physical pool once and stays primary if a later revision reorders the queues.
A secondary task still occupies a slot, which appears as `unknown` in the
primary session's slot details. Open **Executions** to see that task's history.
To see individual poll choices, start the worker with
`RUST_LOG=info,ledgence_worker_delivery::polls=debug`.

## Connect the same worker to two SQS queues

Keep the poll plan above. It says how often to poll `interactive` and `batch`. A second file tells Ledgence where those queues live. Build matching development binaries with SQS support:

```sh
cargo build --locked -p ledgence-cli --features sqs
```

For a local ElasticMQ server on port 9324, create two Standard queues named `interactive` and `batch`. Open `examples/delivery-sqs-multi-local.json` in your editor. Replace each `queue_url` with the URL returned by your broker. Each physical queue must serve just its named logical route. For AWS, supply the actual region and queue URLs and omit `endpoint_url` and `local_credentials`. [Example route file](https://github.com/Ledgence/ledgence/blob/develop/examples/delivery-sqs-multi-local.json).

Start the source-built orchestrator using your existing database and program store. Add the following option to its usual `serve` command:

```sh
./target/debug/ledgence orchestrator serve --bind 127.0.0.1:8080 \
  --store ./store --delivery-config examples/delivery-sqs-multi-local.json
```

Run the worker in a second terminal. If your installation uses credentials, also pass its worker credential file.

```sh
./target/debug/ledgence worker connect \
  --server http://127.0.0.1:8080 --tenant acme --namespace demo \
  --poll-plan ./poll-plan.json --delivery-config examples/delivery-sqs-multi-local.json \
  --concurrency 8 --store ./store --cache ./worker-cache --python python3 \
  --runner ./sdk/python/ledgence/worker/bootstrap.py
```

This creates eight consumers in total. You can change the weights while work is running. Each consumer applies the new revision to its next fresh poll. A pending SQS receive, including one waiting on an empty queue, must finish or reconcile on its original route first.

**Choose the receive wait for your workload.** With multiple SQS queues the default is one second. Short waits let a free consumer check another queue sooner but usually cause more empty receive requests. A longer wait can be appropriate for a quiet workload. Set `--acquire-wait-ms 4000` for a four-second wait, or `0` for short polling. SQS uses whole seconds and rounds fractional waits down. Dedicated workers remain useful when one queue must respond promptly even if every consumer of another queue is waiting. SQS receive requests target one physical queue at a time. [AWS long-polling guidance](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/best-practices-setting-up-long-polling.html).

## Queue concurrency in development

Limit outstanding attempts across worker replicas, independently of each worker’s local poll weights. Inspect occupied and uncertain slots and edit a limit from Console, CLI, HTTP, Python or MCP. [Limit a queue across workers](/how-to/limit-queue-concurrency).
