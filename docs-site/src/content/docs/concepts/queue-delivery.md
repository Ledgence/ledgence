---
title: Queue delivery and execution authority
description: Understand integrated HTTP acquisition, SQS-compatible delivery, worker concurrency, and the boundary for custom queue adapters.
---

Ledgence separates a message that announces ready work from permission to
execute it. **PostgreSQL remains the durable authority** for tasks, attempts,
leases, workflow checkpoints and results in both built-in delivery modes.

## Choose a built-in delivery mode

| Mode in 0.4.0 | How workers acquire work | Setup |
| --- | --- | --- |
| Integrated HTTP acquisition | Workers request assignments from the task service, using bounded waits. No external broker is needed. | Default in the [local source stack](/tutorials/run-locally). |
| SQS-compatible external delivery | Workers receive compact dispatch references from SQS Standard or ElasticMQ, then claim the exact work from the task service. | Enable the `sqs` build feature and provide matching server/worker delivery configuration. |

The SQS adapter has been validated against local ElasticMQ. That qualification
does not establish production AWS capacity. FIFO queues are not supported by
this adapter. The native release build includes optional integrations, while a
custom source build must select the features it needs.

## A broker message is not an execution lease

1. Task submission commits the task and its delivery intent together.
2. The orchestrator's publisher sends a dispatch reference and records the
   outcome. Uncertain publication remains recoverable.
3. A worker reserves capacity, receives a reference and claims its exact task
   and readiness generation through the task service.
4. After validating the durable handoff, the worker acknowledges the broker
   record and obtains dispatch authorization before executing the program.
5. Ledgence's attempt lease, settlement and expiry recovery govern execution
   from that point onward.

Broker acknowledgment therefore does not wait for program completion. Duplicate
messages can be reconciled without granting a second owner execution authority.
That coordination does not make external effects exactly once; an attempt lost
after an external effect can still need application reconciliation.

Programs receive the complete CloudEvent from Ledgence. Queue receipt handles
and broker-specific details are not inserted into the application's `data`.

## Capacity and placement

`--concurrency N` controls N consumers and at most N managed subprocess slots
per worker across all programs. Consumers reserve capacity before receiving
work. Starting, warm, running and retiring processes all count against the
bound; healthy matching processes are reused.

Workers attached to the same logical queue compete for eligible work. Queue
routing and available capacity determine placement. Priority scheduling,
cost-based placement and program-version eligibility policies are not provided
by this release. Increasing worker count does not remove database, broker,
artifact-store or application bottlenecks.

## Configure and operate an external route

Use `--delivery-config` on both `ledgence orchestrator serve` and
`ledgence worker connect`. The
[dispatch configuration guide](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/dispatch-delivery.md)
shows routing for an already-created broker and queue. To start those locally,
use the source stack's [ElasticMQ Compose override](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/local-deployment.md#elasticmq-instead-of-integrated-acquisition).
A physical Standard queue must be dedicated to
one logical route; Ledgence validates its required capabilities at startup.

An external route is durable and initially requires an empty logical queue.
Keep at least one configured orchestrator publishing that destination. Removing
the publisher configuration does not switch tasks back to integrated
acquisition; pending delivery intents wait for a matching publisher to return.

## Extend through adapters

The Rust `DispatchPublisher` and `AckQueue` ports support batch publication and
individual receipt acknowledgment without provider SDK types in the core API.
Custom adapters must preserve the handoff, identity, cancellation and uncertainty
contracts; compatibility is more than implementing similarly named methods.

RabbitMQ, Kafka and Kinesis are not bundled adapters. A stream's partition and
checkpoint semantics are not interchangeable with an individual-ack queue, and
the current worker composition does not expose a built-in stream consumer.
See the [adapter contracts](https://github.com/Ledgence/ledgence/blob/v0.4.0/docs/dispatch-delivery.md)
before selecting a new transport.
