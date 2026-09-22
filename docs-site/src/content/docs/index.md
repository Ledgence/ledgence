---
title: Build with Ledgence.
description: Learn to run agents, coordinate workflows, and understand the details of execution on your infrastructure.
tableOfContents: false
---

<p class="ld-home-intro">From your first task to a working workflow. Learn by doing, solve a specific problem, or find the exact detail you need.</p>

<div class="ld-start-panel">
  <div>
    <span class="ld-eyebrow">Start on your machine</span>
    <h2>See Ledgence run.</h2>
    <p>Start a local stack, publish the example agents, and follow their work through to a result. No vendor account required.</p>
    <a class="ld-start-link" href="/tutorials/run-locally">Run Ledgence locally <span aria-hidden="true">↗</span></a>
  </div>
  <div class="ld-worker-card" aria-label="Illustration of a worker with six process slots">
    <span>Your worker</span>
    <div class="ld-process-grid"><span>✓</span><span></span><span></span><span></span><span></span><span></span></div>
  </div>
</div>

## Find your way

<div class="ld-doc-grid">
  <a class="ld-doc-card" href="/tutorials/first-workflow"><span class="ld-doc-card-title">Tutorials <span aria-hidden="true">→</span></span><span>Learn through complete, guided examples with a result you can check.</span></a>
  <a class="ld-doc-card" href="/how-to/parallel-tasks"><span class="ld-doc-card-title">How-to guides <span aria-hidden="true">→</span></span><span>Accomplish a task in your own application, from parallel work to event waits.</span></a>
  <a class="ld-doc-card" href="/reference/workflow-context"><span class="ld-doc-card-title">Reference <span aria-hidden="true">→</span></span><span>Look up methods, fields, limits, and precise behavior.</span></a>
  <a class="ld-doc-card" href="/concepts/execution-model"><span class="ld-doc-card-title">Concepts <span aria-hidden="true">→</span></span><span>Understand workers, checkpoints, recovery, and the choices behind them.</span></a>
</div>

## A few names to know

Your **agent** is application code. You publish that code and its prepared dependencies as a **program package**. A **task** asks a worker to execute it. A **workflow** coordinates work through explicit checkpoints and continuations.

Ledgence also runs data pipelines and other application code. The initial runtime is Python; the platform is built in Rust.

<p class="ld-home-status">These docs describe the development branch. Start with the local, trusted-code deployment; public APIs and release compatibility are still evolving.</p>
