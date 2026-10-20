---
title: Start with Ledgence
description: Choose a local installation, run your first task, then build a workflow that reuses it.
tableOfContents: false
---

<p class="ld-home-intro">Run Python programs on workers, combine them into workflows, and see each execution in your browser. Start with a greeting; no AI account or API key is needed.</p>

<a class="ld-release-link" href="/reference/releases"><span>Ledgence 0.6.0</span> Versions and downloads <span aria-hidden="true">→</span></a>

<div class="ld-start-panel">
  <div>
    <span class="ld-eyebrow">New to Ledgence?</span>
    <h2>One task. Then a workflow.</h2>
    <p>Choose how to install Ledgence once. Then follow two short lessons with real Python files you can open, edit, and run.</p>
    <div class="ld-start-actions"><a class="ld-start-link" href="/getting-started/installation">Choose an installation <span aria-hidden="true">→</span></a><a class="ld-install-link" href="/tutorials/first-task">Already running? Create your first task <span aria-hidden="true">→</span></a></div>
  </div>
  <div class="ld-worker-card" aria-label="Illustration of a worker with six process slots">
    <span>Your worker</span>
    <div class="ld-process-grid"><span>✓</span><span></span><span></span><span></span><span></span><span></span></div>
  </div>
</div>

## Follow the learning path

Keep your editor and a terminal open. Each lesson names the files to create,
explains the commands, and shows the result you should see.

<div class="ld-doc-grid">
  <a class="ld-doc-card" href="/getting-started/installation"><span class="ld-doc-card-title">1. Install locally <span aria-hidden="true">→</span></span><span>Choose automatic setup, direct Docker Compose, or a CLI built from source. Follow only your chosen route.</span></a>
  <a class="ld-doc-card" href="/tutorials/first-task"><span class="ld-doc-card-title">2. Hello, Ledgence! <span aria-hidden="true">→</span></span><span>Create a Python task, publish it, and receive its greeting from a real worker.</span></a>
  <a class="ld-doc-card" href="/tutorials/first-workflow"><span class="ld-doc-card-title">3. Your first workflow <span aria-hidden="true">→</span></span><span>Invoke the same greeting task from a workflow. Learn how it waits and resumes.</span></a>
  <a class="ld-doc-card" href="/tutorials/use-console"><span class="ld-doc-card-title">4. Explore your executions <span aria-hidden="true">→</span></span><span>Inspect input and output, follow the workflow graph, and open its child task.</span></a>
</div>

## Three names you will use

A **program** is your Python code packaged for a worker. You publish a program
once, then ask Ledgence to execute it as many times as you need.

A **task** is one execution of a program. It receives input and produces a result.

A **workflow** coordinates tasks and other workflows. When it needs to wait,
Ledgence saves its progress and releases the worker slot until it can continue.

The **CLI** is the `ledgence` command in your terminal. The **Console** is the web
interface for your running installation. Python packages help you write programs
and send requests; installing those packages alone does not start Ledgence.

## After the first lessons

| What you want to do | Next guide |
| --- | --- |
| Develop an existing Python project | [Program development](/how-to/develop-python-programs) |
| Package dependencies or publish a new version | [Build and publish programs](/how-to/build-and-publish-programs) |
| Run several tasks at once | [Parallel tasks](/how-to/parallel-tasks) |
| Add independent workflow branches | [Local work and distributed branches](/how-to/fork-workflow-branches) |
| Wait for a person or another system | [Action approvals](/how-to/require-approval) and [external events](/how-to/wait-for-event) |
| Build an agent that can recover completed calls | [Model and tool recovery](/how-to/recover-agent-calls) |
| Connect an MCP client | [MCP setup](/how-to/connect-mcp) |
| Run a complete agent example | [Code review with Codex](/tutorials/codex-change-review) |

Use [Manage your local stack](/how-to/run-local-distribution) for logs and
restarts, and [Upgrade to 0.6.0](/how-to/upgrade-to-0-6) for an existing
installation. Detailed contracts live in [Reference](/reference/capabilities);
[Concepts](/concepts/execution-model) explains how the pieces work together.

<p class="ld-home-status">These guides target the published Ledgence 0.6.0 release. Ledgence is open source and self-hosted. Run programs you trust; worker processes are not a sandbox for hostile code.</p>
