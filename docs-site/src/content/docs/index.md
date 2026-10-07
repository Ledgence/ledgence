---
title: Build with Ledgence.
description: Run agents, coordinate workflows, and operate a self-hosted Ledgence instance with Console.
tableOfContents: false
---

<p class="ld-home-intro">Run your agents, follow their workflows, and understand what is happening on your infrastructure.</p>

<a class="ld-release-link" href="/reference/releases"><span>Ledgence 0.5</span> Public releases are here <span aria-hidden="true">→</span></a>

<div class="ld-start-panel">
  <div>
    <span class="ld-eyebrow">Ledgence 0.5.0 · durable agents and workflows</span>
    <h2>See your work in motion.</h2>
    <p>Start a self-hosted instance, inspect real executions, and explore the workflow graph, registered programs, and worker processes.</p>
    <div class="ld-start-actions"><a class="ld-start-link" href="/tutorials/use-console">Explore Console <span aria-hidden="true">↗</span></a><a class="ld-install-link" href="/how-to/install-native">Install and run locally <span aria-hidden="true">→</span></a></div>
  </div>
  <div class="ld-worker-card" aria-label="Illustration of a worker with six process slots">
    <span>Your worker</span>
    <div class="ld-process-grid"><span>✓</span><span></span><span></span><span></span><span></span><span></span></div>
  </div>
</div>

Explore a complete agent workflow: [fix an empty page with Codex](/tutorials/codex-change-review). Follow the problem, the animated workflow replay, and the code behind independent branches, measured checks, and a human decision.

## Choose your starting point

| Need | Guide |
| --- | --- |
| Install the CLI in one command and configure your shell. | [Installation](/how-to/install-native) |
| Start and manage the local stack from published images. | [Local distribution](/how-to/run-local-distribution) |
| Update an existing installation to 0.5.0. | [Upgrade guide](/how-to/upgrade-to-0-5) — update the CLI and running services separately. |
| Build the Docker Compose examples from source. | [Local source tutorial](/tutorials/run-locally) |
| Write Python programs with local imports and editor support. | [Python development setup](/how-to/develop-python-programs) — install the published worker helper. |
| Build for a worker runtime and upload an immutable program. | [Build and publish programs](/how-to/build-and-publish-programs) — explicit targets and saved recovery receipts. |
| Submit work and receive results after disconnecting. | [Result waiting and callbacks](/how-to/receive-results) |
| Connect an MCP client to an existing instance. | [MCP setup](/how-to/connect-mcp) |
| Operate workers and diagnose executions. | [Observability](/how-to/configure-observability), [queue delivery](/concepts/queue-delivery), and [Console](/reference/console) |

Use the [capability map](/reference/capabilities) to find every supported area,
its limits and release availability. Ledgence 0.5.0 adds explicit program builds,
HTTP publication and saved receipts for interrupted uploads. It also includes
the installable Python worker helper introduced in 0.4.1. The one-line installer,
container images and `ledgence local` remain available.

## Find your way

<div class="ld-doc-grid">
  <a class="ld-doc-card" href="/tutorials/use-console"><span class="ld-doc-card-title">Tutorials <span aria-hidden="true">→</span></span><span>Learn through complete, guided examples with a result you can check.</span></a>
  <a class="ld-doc-card" href="/how-to/register-agent"><span class="ld-doc-card-title">How-to guides <span aria-hidden="true">→</span></span><span>Register an agent, coordinate parallel work, or wait for an external event.</span></a>
  <a class="ld-doc-card" href="/reference/console"><span class="ld-doc-card-title">Reference <span aria-hidden="true">→</span></span><span>Look up methods, fields, limits, and precise behavior.</span></a>
  <a class="ld-doc-card" href="/concepts/self-hosted-console"><span class="ld-doc-card-title">Concepts <span aria-hidden="true">→</span></span><span>Understand the instance boundary, worker observations, checkpoints, and recovery.</span></a>
</div>

## A few names to know

Your **agent** is application code. You publish that code and its prepared dependencies as a **program package**. A **task** asks a worker to execute it. A **workflow** coordinates tasks and branches through registered **entrypoints**. Checkpoints save the state needed to resume at the next entrypoint.

Ledgence also runs data pipelines and other application code. The initial runtime is Python; the platform is built in Rust.

<p class="ld-home-status">Released guides target Ledgence 0.5.0: program builds and publication, durable approvals, model/tool recovery, MCP, and workflows with a self-hosted Console. Install the CLI and run the local stack from published images, or build from source. Check the <a href="/reference/releases">release reference</a> before installing or upgrading. Run code you trust on infrastructure you control.</p>
