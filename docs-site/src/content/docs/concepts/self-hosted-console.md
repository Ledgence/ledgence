---
title: One self-hosted instance
description: Understand the Console deployment boundary, program catalog, and distinction between durable execution state and worker observations.
---

A self-hosted Ledgence installation is one operating environment. Console organizes work into **Executions**, **Programs** and **Workers**. Executions combines task and workflow history, Programs holds registered immutable packages, and Workers shows process observations. There is no tenant administration, workspace switcher, or organization model to configure.

Console is included in Ledgence 0.3.0. The [Console tutorial](/tutorials/use-console) explains how to start a matching deployment; the historical 0.1 releases predate it.

## The interface belongs to the instance

The same Rust orchestrator serves the API and Console's static files. The browser talks to that origin and receives its instance name, capabilities, limits, and contract version. Node is a frontend build tool, not a production service.

This keeps the operator interface alongside the service it describes. There is no mandatory vendor account or separately hosted control plane. Deployments can still choose their own access-controlled reverse proxy when remote operator access is needed.

An instance is an installation boundary, not an authentication system. Configuring one instance does not create user roles or make arbitrary remote requests trustworthy.

## Why older APIs still contain scope fields

Existing SDK, CLI, and worker contracts use `tenant` and `namespace` fields. The self-hosted server binds those compatibility fields to one fixed value; Console neither sends nor selects them. They do not expose a multi-tenant product mode.

A new database defaults to `default/default`. The supplied Compose example uses `acme/demo` to match its existing programs, workers, and client examples. An application connecting through the older client supplies those same values because they identify the installation's existing data.

The instance ID and binding are persisted and cannot be changed by editing display settings. The human-readable name and queue suggestions can change. Separate operational installations use separate configured instances and databases.

Adopting existing data therefore requires an explicit binding decision. If a database contains records from different historical bindings, startup rejects it rather than choosing one or reassigning work. Stopping writers and planning migration is part of adoption, not a browser setting.

## Publication, registration, and execution are different

**Publication** makes immutable application bytes available in a program store. The package contains the application and its prepared dependencies, with requirements for the worker's Python runtime and platform.

**Registration** verifies a published reference and adds its metadata to **Programs**, the instance's registry of tasks and workflows. Registration is deliberate: it does not scan a private store or run code to discover what a program does.

**Execution** creates durable work that asks a worker to invoke an exact program version. Many executions can use the same registered package. Deleting old execution history does not remove that package from the catalog.

The separation lets operators know which code they are selecting before they submit work. A successful registration establishes verified package identity; it does not prove that a compatible worker is currently available.

## Durable state and observation answer different questions

The execution record answers: **what work was accepted, which attempt owns it, and what outcome was recorded?** PostgreSQL remains the authority for those facts.

The worker report answers: **what did this worker most recently observe about its process pool?** It can reveal a warm reusable process, an executing slot, or cleanup still occupying capacity. The report's timestamp and freshness matter because the process can change before the browser refreshes.

These are complementary views. A stale worker report does not change a durable outcome. An active task is not proof that its handler is running at this instant. Missing telemetry does not make capacity available.

This also explains why Console separates consumer counts from process slots. A consumer can wait for an assignment without a process, and a warm process can remain after its previous consumer is released. One concurrency limit bounds both execution consumers and managed process capacity, but their current counts need not be equal.

## A workflow view records decisions already made

A workflow can decide its next work dynamically. Console shows accepted activations, created children, recorded local steps, and actual waits. It links children using durable ownership and creation relationships, rather than guessing from matching correlation strings.

Each graph shows one workflow and its direct children. Opening a child workflow moves to its own graph, so a large subworkflow does not crowd its parent's canvas. Entrypoint invocations, local calls, fork members, joins and resumptions connect through typed relations recorded by Ledgence. The graph does not need an OpenTelemetry backend or infer dependencies from span parents.

This is a record of execution, not a workflow designer or future dependency graph. When a controller checkpoints and waits, its invocation ends and releases its worker reservation. A warm process can remain available in the ordinary pool while the workflow waits durably.

The result is a view that stays useful after the activity finishes: operators can follow the accepted decisions without needing to catch an animation or an intermediate live state.

For operating details, see [Console reference](/reference/console). For how application continuations resume, read [Checkpoints and recovery](/concepts/checkpoints-and-recovery).
