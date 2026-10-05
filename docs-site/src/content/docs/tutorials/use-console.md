---
title: Explore Ledgence Console
description: Start Ledgence 0.3.1 Console, run the example agents, and follow their executions and worker processes.
---

Run the local stack, then use Console to follow a real task from its input to its result, inspect a completed workflow, and explore the worker's process slots.

**Availability:** Console is included in Ledgence 0.3.1. This tutorial uses its source checkout and Compose deployment. Follow [Run Ledgence locally](/tutorials/run-locally#1-get-the-source) to obtain `v0.3.1`, or use the [native installation guide](/how-to/install-native) for prebuilt assets.

## 1. Check your source checkout

Open a terminal at the Ledgence repository root. Confirm that it contains `console/` and that `deploy/local/compose.yaml` starts the orchestrator with `--instance-config` and `--console-dir`.

```sh
test -f console/package.json
git rev-parse HEAD
git status --short
```

Use Git and Docker Engine or Docker Desktop with **Compose 2.23.1 or newer** and Linux containers on `amd64` or `arm64`. The first build downloads pinned images and Rust and frontend dependencies. Docker builds the Console assets; Node, Rust, and Python are not required on your host.

Use a fresh local deployment for this tutorial. If an earlier Ledgence deployment already uses the `ledgence-local` Compose project, follow the [upgrade guidance](/reference/console#upgrades) before changing its source or database. The example API is bound to `127.0.0.1:8080` and uses local demonstration credentials.

## 2. Start the stack

Record the source identity and build the local image:

```sh
export LEDGENCE_SOURCE_REVISION="$(git rev-parse HEAD)"
export LEDGENCE_SOURCE_DIRTY="$(test -z "$(git status --porcelain)" && echo false || echo true)"
docker compose -f deploy/local/compose.yaml build
docker compose -f deploy/local/compose.yaml up -d --wait --wait-timeout 120
```

Compose starts PostgreSQL, runs the explicit migration, then starts the orchestrator, a worker, and the example callback receiver. The Rust orchestrator serves the built Console; there is no separate frontend server to start.

Open [http://127.0.0.1:8080/console/](http://127.0.0.1:8080/console/). You should see **Local instance** with **Executions**, **Programs** and **Workers** in the navigation. This is one self-hosted instance, with no tenant or workspace selection.

The lists can be empty at this point. Leave the worker's default concurrency at **one** so the example can check process reuse.

On desktop, use **Collapse sidebar** in the sidebar header to give the content more room; the Ledgence mark expands it again. Choose **System**, **Light** or **Dark** from **Appearance** in the header.

## 3. Publish and register the examples

```sh
docker compose -f deploy/local/compose.yaml run --rm --no-deps publish
```

This command prepares three immutable packages for the container's runtime and publishes them into the shared program store. It then registers each exact reference in the instance catalog:

| Program | Version | Declared use |
| --- | --- | --- |
| `invoice-issuer` | `1.0.0` | Task |
| `workflow-example` | `1.0.1` | Workflow controller |
| `workflow-summary` | `1.0.0` | Task |

Open **Programs**, choose `invoice-issuer`, and open version `1.0.0`. The page shows the verified digest, Python version, operating system, architecture, and handler. This application version is independent of the Ledgence platform version.

## 4. Run and inspect the example

Back in your terminal:

```sh
docker compose -f deploy/local/compose.yaml run --rm --no-deps demo
```

The final JSON contains `passed: true`, two task IDs, a workflow ID, and a workflow output. Keep those IDs visible.

In **Executions**, open either invoice task by its ID. In **General**, open **Input** to see the invoice request and **Output** to inspect the returned invoice ID, process ID, and invocation counter. **Trace** contains the attempts and durable history.

As the history grows, scroll to load older rows. Filter by type, status and submitted date; **More filters** contains exact program/version and execution ID filters. The table updates automatically on its first page. Browsing older results pauses those updates; **Refresh executions** returns to the latest matching work with your filters retained.

In **Executions**, set **Type** to **Workflow**, select **Apply filters**, and open the workflow ID printed by the demo. Its detail offers **Graph**, **Trace** and **General**. Graph shows entrypoint invocation nodes, the recorded local page-fetch steps and the child task `summarize`. Select a node or Trace row to inspect it; select **Open task** on the child to inspect its input and output. Local records connect to their invoking entrypoint without claiming dependencies between the local calls. Use Back or Up to return and explore another part of the workflow. A subworkflow opens its own graph, one level at a time. Completed work stays visible; these views reflect work that happened and do not predict future steps.

Try **Full screen** in the explorer. Pan or zoom, use **Fit** to see the loaded graph, and choose **Reorganize** to restore its automatic layout after moving a card. These controls change only the view. Press Escape to return without losing the selected node or camera position.

Under **General → Output**, expect:

```json
{
  "page_count": 4,
  "summary": {
    "characters": 84,
    "pages": 4
  }
}
```

The example finishes quickly. A completed history is the expected outcome; you do not need to catch a waiting state on screen.

## 5. Submit an invoice from Console

Open **Executions → New execution** and enter:

| Field | Value |
| --- | --- |
| Program ID | `invoice-issuer` |
| Exact version | `1.0.0` |
| Queue | `demo` |
| JSON input | `{"invoice_id":"CONSOLE-1042"}` |

Select **Verify reference**, then **Submit execution**. Console opens the accepted execution. Its **Output** should contain `"invoice_id": "CONSOLE-1042"` once the worker completes it. The process ID and invocation counter depend on the worker's current reusable process.

Submitting is a write to your running instance. **Run again** creates new work with a new submission identity; it does not reconnect to the previous result.

## 6. Inspect the worker

Open **Workers** and select the worker session. Its configured capacity is one. The process grid may show a warm process after the invoice completes; that process is available for compatible future work while still occupying a process slot.

Select the slot to inspect its process identity, program, digest, and any validated execution link. The observation timestamp tells you how recent this information is. An old report does not prove that a process has stopped.

You have now inspected a package, submitted an execution, followed a durable workflow, and connected the result to worker observations. Continue with [Register an agent](/how-to/register-agent) for your own packages, or [Console reference](/reference/console) for exact behavior.

## Stop the local stack

```sh
docker compose -f deploy/local/compose.yaml down --timeout 65
```

This preserves the named database, program store, worker cache, and callback volumes. Start the same deployment with the earlier `up` command when you want to continue.

If startup or publication fails, inspect the services and logs:

```sh
docker compose -f deploy/local/compose.yaml ps
docker compose -f deploy/local/compose.yaml logs --tail 100 orchestrator worker
```

A blank program registry usually means publication or registration has not completed. Repeat `publish` to reconcile an interrupted registration; it is safe for identical package bytes. If Console returns 404, check that your checkout and running image include Console and that the orchestrator was started with `--console-dir`.
